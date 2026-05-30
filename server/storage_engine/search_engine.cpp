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

#include <absl/functional/any_invocable.h>
#include <absl/strings/escaping.h>
#include <absl/time/time.h>

#include <iresearch/analysis/classification_tokenizer.hpp>
#include <iresearch/analysis/fast_text_model.hpp>
#include <iresearch/analysis/nearest_neighbors_tokenizer.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/search/scorers.hpp>

#include "app/app_server.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "app/options/section.h"
#include "basics/down_cast.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/number_of_cores.h"
#include "catalog/catalog.h"
#include "catalog/identity_analyzer.h"
#include "catalog/index.h"
#include "catalog/search_common.h"
#include "catalog/view.h"
#include "general_server/state.h"
#include "metrics/gauge_builder.h"
#include "metrics/metrics_feature.h"
#include "rest_server/database_path_feature.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/inverted_index_shard.h"
#include "search/resource_manager.h"
#include "search/wal_recovery.h"
#include "storage_engine/search_engine.h"

using namespace std::chrono_literals;

namespace sdb::search {
namespace {

REGISTER_ANALYZER_VPACK(IdentityAnalyzer, IdentityAnalyzer::make,
                        IdentityAnalyzer::normalize);
REGISTER_ANALYZER_JSON(IdentityAnalyzer, IdentityAnalyzer::make_json,
                       IdentityAnalyzer::normalize_json);

DECLARE_GAUGE(serenedb_search_num_out_of_sync_links, uint64_t,
              "Number of inverted indexes currently out of sync");
DECLARE_GAUGE(serenedb_search_columns_cache_size, LimitedResourceManager,
              "Search columns cache usage in bytes");

const std::string kCommitThreadsParam("--search.commit-threads");
const std::string kCompactionThreadsParam("--search.compaction-threads");
const std::string kFailOnOutOfSync("--search.fail-queries-on-out-of-sync");
const std::string kSkipRecovery("--search.skip-recovery");
const std::string kSkipWalRecovery("--search.skip-wal-recovery");
const std::string kCacheLimit("--search.columns-cache-limit");
const std::string kCacheOnlyLeader("--search.columns-cache-only-leader");
const std::string kSearchThreadsLimit("--search.execution-threads-limit");
const std::string kSearchDefaultParallelism("--search.default-parallelism");

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

SearchEngine::SearchEngine(Server& server)
  : SerenedFeature{server, name()},
    _dir_feature{server.getFeature<DatabasePathFeature>()},
    _thread_pools(std::make_shared<SearchThreadPools>()),
    _out_of_sync_links(metrics::GetMetrics().add(
      serenedb_search_num_out_of_sync_links{})),
    _columns_cache_memory_used(metrics::GetMetrics().add(
      serenedb_search_columns_cache_size{})) {
  setOptional(true);
  static_assert(Server::isCreatedAfter<SearchEngine, DatabasePathFeature>());
}

void SearchEngine::collectOptions(
  std::shared_ptr<options::ProgramOptions> options) {
  options->addSection("search", absl::StrCat(name(), " feature"));

  // --search.compaction-threads / --search.commit-threads were upper limits
  // tuned per-cluster; defaults are auto-detected from cpu count.

  options->addOption(
    kSkipRecovery,
    "Skip the data recovery for the specified View link or inverted "
    "index on startup. The value for this option needs to have the "
    "format '<collection-name>/<index-id>' or "
    "'<collection-name>/<index-name>'. You can use the option multiple "
    "times, for each View link and inverted index to skip the recovery "
    "for. The pseudo-value 'all' disables the recovery for all View "
    "links and inverted indexes. The links/indexes skipped during the "
    "recovery are marked as out-of-sync when the recovery completes. You "
    "need to recreate them manually afterwards.\n"
    "WARNING: Using this option causes data of affected links/indexes to "
    "become incomplete or more incomplete until they have been manually "
    "recreated.",
    new options::VectorParameter<options::StringParameter>(
      &_skip_recovery_items));

  options->addOption(
    kSkipWalRecovery,
    "Skip the entire WAL replay phase for inverted indexes on startup. "
    "Lagging shards will start serving queries with stale content; the "
    "missing WAL delta will not be applied. Intended for diagnostics -- "
    "data loss is permanent for the skipped delta unless you crash again "
    "with a longer recovery window.",
    new options::BooleanParameter(&_skip_wal_recovery));

  // --search.fail-on-out-of-sync, --search.search-threads-limit:
  // fields stay at defaults.
  // --search.default-parallelism survives as a runtime SET LOCAL setting --
  // a follow-up will register it via DBConfig::AddExtensionOption.
  options->addOption(
    kSearchDefaultParallelism, "Default parallelism for Search queries",
    new options::UInt32Parameter(&_default_parallelism));
}

void SearchEngine::validateOptions(
  std::shared_ptr<options::ProgramOptions> options) {
  auto check_format = [](const auto& item) {
    auto r = item.find('/');
    if (r == std::string_view::npos) {
      return false;
    }
    r = item.find('/', r);
    if (r == std::string_view::npos) {
      return true;
    }
    return false;
  };
  for (const auto& item : _skip_recovery_items) {
    if (item != "all" && check_format(item)) {
      SDB_FATAL(SEARCH, "invalid format for '", kSkipRecovery,
                "' parameter. expecting '",
                "<collection-name>/<index-id>' or "
                "'<collection-name>/<index-name>' or ",
                "'all', got: '", item, "'");
    }
  }

  const auto& args = options->processingResult();

  uint32_t threads_limit =
    static_cast<uint32_t>(4 * number_of_cores::GetValue());

  _commit_threads = ComputeThreadsCount(_commit_threads, threads_limit, 6);
  _compaction_threads =
    ComputeThreadsCount(_compaction_threads, threads_limit, 6);

  if (!args.touched(kSearchThreadsLimit)) {
    _search_execution_threads_limit =
      static_cast<uint32_t>(number_of_cores::GetValue());
  }
}

SearchEngine& GetSearchEngine() {
  return SerenedServer::Instance().getFeature<SearchEngine>();
}

void SearchEngine::prepare() {
  SDB_ASSERT(isEnabled());

  ::irs::analysis::ClassificationTokenizer::set_model_provider(
    &fast_text::CreateModel<fasttext::FastText>);
  ::irs::analysis::NearestNeighborsTokenizer::set_model_provider(
    &fast_text::CreateModel<fasttext::ImmutableFastText>);

  irs::analysis::analyzers::Init();
  irs::formats::Init();
  irs::scorers::Init();
  irs::compression::Init();

  SDB_ASSERT(std::make_tuple(size_t(0), size_t(0), size_t(0)) ==
             stats(ThreadGroup::Refresh));
  SDB_ASSERT(std::make_tuple(size_t(0), size_t(0), size_t(0)) ==
             stats(ThreadGroup::Compaction));
}

void SearchEngine::start() {
  SDB_ASSERT(isEnabled());

  if (ServerState::instance()->IsDBServer() ||
      ServerState::instance()->IsSingle()) {
    SDB_ASSERT(_commit_threads);
    SDB_ASSERT(_compaction_threads);

    _thread_pools->Get(ThreadGroup::Refresh)
      .start(_commit_threads, IR_NATIVE_STRING("search:commit"));
    _thread_pools->Get(ThreadGroup::Compaction)
      .start(_compaction_threads, IR_NATIVE_STRING("search:compact"));

    InitInvertedIndexes(_skip_wal_recovery);

    SDB_INFO(SEARCH, "Search maintenance: [", _commit_threads,
             "..", _commit_threads, "] commit thread(s), [",
             _compaction_threads, "..", _compaction_threads,
             "] compaction thread(s). Search execution parallel threads "
             "limit: ",
             _search_execution_threads_limit);
  }
}

void SearchEngine::stop() {
  SDB_ASSERT(isEnabled());
  _thread_pools->Stop();
}

void SearchEngine::unprepare() { SDB_ASSERT(isEnabled()); }

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

  if (!server().isStopping()) {
    SDB_WARN(SEARCH,
             "Caught exception while sumbitting a task to thread group '",
             std::underlying_type_t<ThreadGroup>(id),
             "', error: ", r.errorMessage());
  }

  return false;
}

std::tuple<size_t, size_t, size_t> SearchEngine::stats(ThreadGroup id) const {
  return _thread_pools->Get(id).stats();
}

std::pair<size_t, size_t> SearchEngine::limits(ThreadGroup id) const {
  auto threads = _thread_pools->Get(id).threads();
  return {threads, threads};
}

void SearchEngine::trackOutOfSyncLink() noexcept { ++_out_of_sync_links; }

void SearchEngine::untrackOutOfSyncLink() noexcept {
  uint64_t previous = _out_of_sync_links.fetch_sub(1);
  SDB_ASSERT(previous > 0);
}

bool SearchEngine::failQueriesOnOutOfSync() const noexcept {
  SDB_IF_FAILURE("Search::FailQueriesOnOutOfSync") { return true; }
  return _fail_queries_on_out_of_sync;
}

std::filesystem::path SearchEngine::GetPersistedPath(
  ObjectId database_id) const {
  std::filesystem::path path = _dir_feature.directory();
  path /= StaticStrings::kEngineDirRoot;
  path /= absl::StrCat(database_id);
  return path;
}

void SearchEngine::beginShutdown() {
  _thread_pools->Get(ThreadGroup::Refresh).stop(false);
  _thread_pools->Get(ThreadGroup::Compaction).stop(false);
}

}  // namespace sdb::search
