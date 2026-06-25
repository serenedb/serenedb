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

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>
#include <absl/strings/escaping.h>

#include <algorithm>
#include <iresearch/analysis/classification_tokenizer.hpp>
#include <iresearch/analysis/fast_text_model.hpp>
#include <iresearch/analysis/nearest_neighbors_tokenizer.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/search/filter_optimizer.hpp>
#include <utility>
#include <yaclib/async/wait.hpp>

#include "basics/assert.h"
#include "search/inverted_index_storage.h"
#include "search/task.h"
#include "search/wal_recovery.h"

ABSL_DECLARE_FLAG(uint64_t, background_threads);

namespace sdb::search {
namespace {

constexpr std::string_view kEngineDirRoot = "engine_search";

}  // namespace

SearchEngine::SearchEngine() : _dir_feature{DatabasePathFeature::instance()} {
  ::irs::analysis::ClassificationTokenizer::set_model_provider(
    &fast_text::CreateModel<fasttext::FastText>);
  ::irs::analysis::NearestNeighborsTokenizer::set_model_provider(
    &fast_text::CreateModel<fasttext::ImmutableFastText>);

  irs::formats::Init();
  irs::InitOptimizeRules();

  gInstance = this;
}

SearchEngine::~SearchEngine() { gInstance = nullptr; }

SearchEngine& GetSearchEngine() { return *SearchEngine::gInstance; }

int SearchEngine::MaxConcurrentCompactions() noexcept {
  // The background pool is max(logical/4, 2) threads (--background_threads,
  // resolved at startup). Merges may use all but one of them -- refresh,
  // cleanup, and drop are light and interleave on the single spare thread.
  return std::max<int>(
    1, static_cast<int>(absl::GetFlag(FLAGS_background_threads)) - 1);
}

void SearchEngine::start() {
  InitInvertedIndexes();
  SDB_INFO(SEARCH, "Search maintenance: per-index refresh/compaction loops");
}

void SearchEngine::stop() {
  _stopping.store(true, std::memory_order_release);

  std::vector<yaclib::Future<>> loops;
  {
    absl::MutexLock lock{&_loops_mutex};
    loops = std::move(_loops);
    _loops.clear();
  }
  if (!loops.empty()) {
    yaclib::Wait(loops.begin(), loops.end());
  }
}

void SearchEngine::StartTasks(
  const std::shared_ptr<InvertedIndexStorage>& storage) {
  SDB_ASSERT(storage);
  if (_stopping.load(std::memory_order_acquire)) {
    return;
  }
  absl::MutexLock lock{&_loops_mutex};
  // Drop loops whose index was already dropped (their coroutine returned) so
  // the registry stays bounded across many CREATE/DROP INDEX cycles.
  std::erase_if(
    _loops, [](const yaclib::Future<>& f) { return !f.Valid() || f.Ready(); });
  _loops.push_back(RefreshLoop(storage));
  _loops.push_back(CompactionCoordinator(storage));
}

std::filesystem::path SearchEngine::GetPersistedPath(
  ObjectId database_id) const {
  std::filesystem::path path = _dir_feature.directory();
  path /= kEngineDirRoot;
  path /= absl::StrCat(database_id);
  return path;
}

}  // namespace sdb::search
