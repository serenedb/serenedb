////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "search_engine.h"

#include <absl/flags/flag.h>
#include <absl/functional/any_invocable.h>
#include <absl/strings/escaping.h>
#include <absl/time/time.h>

ABSL_FLAG(bool, server_skip_search_recovery, false,
          "Skip the entire WAL replay phase for inverted indexes on startup. "
          "Diagnostic only -- data loss is permanent for the skipped delta.");
ABSL_FLAG(uint32_t, server_refresh_threads, 0,
          "Threads in the iresearch refresh pool (0 = auto-derive from cores, "
          "clamped to [1, 4 * cores]).");
ABSL_FLAG(uint32_t, server_compaction_threads, 0,
          "Threads in the iresearch compaction pool (0 = auto-derive from "
          "cores, clamped to [1, 4 * cores]).");

#include <iresearch/analysis/classification_tokenizer.hpp>
#include <iresearch/analysis/fast_text_model.hpp>
#include <iresearch/analysis/nearest_neighbors_tokenizer.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/formats/formats.hpp>

#include "app/app_server.h"
#include "basics/down_cast.h"
#include "basics/exceptions.h"
#include "basics/lifecycle.h"
#include "basics/log.h"
#include "basics/number_of_cores.h"
#include "basics/assert.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/view.h"
#include "rest_server/database_path_feature.h"
#include "search/inverted_index_storage.h"
#include "search/wal_recovery.h"
#include "storage_engine/search_engine.h"

using namespace std::chrono_literals;

namespace sdb::search {
namespace {

constexpr std::string_view kEngineDirRoot = "engine_search";

uint32_t ComputeThreadsCount(uint32_t threads, uint32_t threads_limit,
                             uint32_t div) noexcept {
  SDB_ASSERT(div);
  constexpr uint32_t kMaxThreads = 8;
  constexpr uint32_t kMinThreads = 1;

  return std::clamp(
    threads ? threads : uint32_t(number_of_cores::GetValue()) / div,
    kMinThreads, threads_limit ? threads_limit : kMaxThreads);
}

}  // namespace

class SearchThreadPools {
 public:
  using ThreadPool = irs::async_utils::ThreadPool<>;

  SearchThreadPools() = default;

  ~SearchThreadPools() { Stop(); }

  ThreadPool& Get(ThreadGroup id) noexcept {
    return ThreadGroup::Refresh == id ? _refresh_threads_pool
                                      : _compaction_threads_pool;
  }

  void Stop() noexcept {
    _refresh_threads_pool.stop(true);
    _compaction_threads_pool.stop(true);
  }

 private:
  ThreadPool _refresh_threads_pool;
  ThreadPool _compaction_threads_pool;
};

SearchEngine::SearchEngine()
  : _dir_feature{DatabasePathFeature::instance()},
    _thread_pools(std::make_shared<SearchThreadPools>()) {
  const uint32_t threads_limit =
    static_cast<uint32_t>(4 * number_of_cores::GetValue());
  _refresh_threads = ComputeThreadsCount(
    absl::GetFlag(FLAGS_server_refresh_threads), threads_limit, 6);
  _compaction_threads = ComputeThreadsCount(
    absl::GetFlag(FLAGS_server_compaction_threads), threads_limit, 6);

  ::irs::analysis::ClassificationTokenizer::set_model_provider(
    &fast_text::CreateModel<fasttext::FastText>);
  ::irs::analysis::NearestNeighborsTokenizer::set_model_provider(
    &fast_text::CreateModel<fasttext::ImmutableFastText>);

  irs::formats::Init();

  gInstance = this;
}

SearchEngine::~SearchEngine() { gInstance = nullptr; }

SearchEngine& GetSearchEngine() { return *SearchEngine::gInstance; }

void SearchEngine::start() {
  SDB_ASSERT(std::make_tuple(size_t(0), size_t(0), size_t(0)) ==
             stats(ThreadGroup::Refresh));
  SDB_ASSERT(std::make_tuple(size_t(0), size_t(0), size_t(0)) ==
             stats(ThreadGroup::Compaction));

  SDB_ASSERT(_refresh_threads);
  SDB_ASSERT(_compaction_threads);

  _thread_pools->Get(ThreadGroup::Refresh)
    .start(_refresh_threads, IR_NATIVE_STRING("search:refresh"));
  _thread_pools->Get(ThreadGroup::Compaction)
    .start(_compaction_threads, IR_NATIVE_STRING("search:compaction"));

  const bool skip_wal_recovery =
    absl::GetFlag(FLAGS_server_skip_search_recovery);
  InitInvertedIndexes(skip_wal_recovery);

  SDB_INFO(SEARCH, "Search maintenance: ", _refresh_threads,
           " refresh thread(s), ", _compaction_threads,
           " compaction thread(s)");
}

void SearchEngine::stop() {
  // Nudge both pools to drain before we block on Stop().
  _thread_pools->Get(ThreadGroup::Refresh).stop(false);
  _thread_pools->Get(ThreadGroup::Compaction).stop(false);
  _thread_pools->Stop();
}

bool SearchEngine::Queue(ThreadGroup id, absl::Duration delay,
                         absl::AnyInvocable<void()>&& fn) {
  auto r = basics::SafeCall([&]() {
    return _thread_pools->Get(id).run(std::move(fn), delay)
             ? Result{}
             : Result{ERROR_INTERNAL};
  });

  if (r.ok()) [[likely]] {
    return true;
  }

  if (!lifecycle::IsStopping()) {
    SDB_WARN(
      SEARCH, "Caught exception while sumbitting a task to thread group '",
      std::underlying_type_t<ThreadGroup>(id), "', error: ", r.errorMessage());
  }

  return false;
}

std::tuple<size_t, size_t, size_t> SearchEngine::stats(ThreadGroup id) const {
  return _thread_pools->Get(id).stats();
}

std::filesystem::path SearchEngine::GetPersistedPath(
  ObjectId database_id) const {
  std::filesystem::path path = _dir_feature.directory();
  path /= kEngineDirRoot;
  path /= absl::StrCat(database_id);
  return path;
}

}  // namespace sdb::search
