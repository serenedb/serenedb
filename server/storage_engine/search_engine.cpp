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

#include <absl/strings/escaping.h>

#include <iresearch/analysis/classification_tokenizer.hpp>
#include <iresearch/analysis/fast_text_model.hpp>
#include <iresearch/analysis/nearest_neighbors_tokenizer.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/search/scorers.hpp>

#include "app/app_server.h"
#include "app/options/parameters.h"
#include "basics/down_cast.h"
#include "basics/logger/logger.h"
#include "basics/number_of_cores.h"
#include "catalog/analyzer.h"
#include "catalog/geo_analyzer.h"
#include "catalog/identity_analyzer.h"
#include "catalog/index.h"
#include "catalog/search_common.h"
#include "catalog/view.h"
#include "general_server/state.h"
#include "metrics/gauge_builder.h"
#include "metrics/metrics_feature.h"
#include "rest_server/database_path_feature.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/data_store.h"
#include "storage_engine/search_engine.h"

using namespace std::chrono_literals;

namespace sdb::search {
namespace {

REGISTER_ANALYZER_VPACK(IdentityAnalyzer, IdentityAnalyzer::make,
                        IdentityAnalyzer::normalize);
REGISTER_ANALYZER_JSON(IdentityAnalyzer, IdentityAnalyzer::make_json,
                       IdentityAnalyzer::normalize_json);
// REGISTER_ANALYZER_VPACK(GeoPointAnalyzer, GeoPointAnalyzer::make,
//                         GeoPointAnalyzer::normalize);
// REGISTER_ANALYZER_VPACK(GeoJsonAnalyzer, GeoJsonAnalyzer::make,
//                         GeoJsonAnalyzer::normalize);
REGISTER_ANALYZER_VPACK(wildcard::Analyzer, wildcard::Analyzer::make,
                        wildcard::Analyzer::normalize);

DECLARE_GAUGE(serenedb_search_num_out_of_sync_links, uint64_t,
              "Number of inverted indexes currently out of sync");


const std::string kCommitThreadsParam("--search.commit-threads");
const std::string kConsolidationThreadsParam("--search.consolidation-threads");
const std::string kFailOnOutOfSync("--search.fail-queries-on-out-of-sync");
const std::string kSkipRecovery("--search.skip-recovery");
const std::string kCacheLimit("--search.columns-cache-limit");
const std::string kCacheOnlyLeader("--search.columns-cache-only-leader");
const std::string kSearchThreadsLimit("--search.execution-threads-limit");
const std::string kSearchDefaultParallelism("--search.default-parallelism");

uint32_t ComputeThreadsCount(uint32_t threads, uint32_t threads_limit,
                             uint32_t div) noexcept {
  SDB_ASSERT(div);
  // arbitrary limit on the upper bound of threads in pool
  constexpr uint32_t kMaxThreads = 8;
  constexpr uint32_t kMinThreads = 1;  // at least one thread is required

  return std::max(
    kMinThreads,
    std::min(threads_limit ? threads_limit : kMaxThreads,
             threads ? threads : uint32_t(number_of_cores::GetValue()) / div));
}

std::filesystem::path GetPersistedPath(
  const DatabasePathFeature& db_path_feature, ObjectId database_id) {
  std::filesystem::path path = db_path_feature.directory();
  path /= StaticStrings::kEngineDirRoot;
  path /= absl::StrCat(StaticStrings::kDatabaseDirPrefix, database_id);
  return path;
}

}  // namespace

class SearchThreadPools {
 public:
  using ThreadPool = irs::async_utils::ThreadPool<>;

  SearchThreadPools() = default;

  ~SearchThreadPools() { Stop(); }

  ThreadPool& Get(ThreadGroup id) noexcept {
    return ThreadGroup::Commit == id ? _commit_threads_pool
                                     : _consolidation_threads_pool;
  }

  void Stop() noexcept {
    _commit_threads_pool.stop(true);
    _consolidation_threads_pool.stop(true);
  }

 private:
  ThreadPool _commit_threads_pool;
  ThreadPool _consolidation_threads_pool;
};

SearchEngine::SearchEngine(Server& server)
  : SerenedFeature{server, name()},
    _dir_feature{server.getFeature<DatabasePathFeature>()},
    _thread_pools(std::make_shared<SearchThreadPools>()),
    _out_of_sync_links(server.getFeature<metrics::MetricsFeature>().add(
      serenedb_search_num_out_of_sync_links{})) {
  setOptional(true);
  static_assert(Server::isCreatedAfter<SearchEngine, DatabasePathFeature>());
  static_assert(
    Server::isCreatedAfter<SearchEngine, metrics::MetricsFeature>());
}

void SearchEngine::collectOptions(
  std::shared_ptr<options::ProgramOptions> options) {
  options->addSection("search", absl::StrCat(name(), " feature"));

  options
    ->addOption(
      kConsolidationThreadsParam,
      "The upper limit to the allowed number of consolidation threads "
      "(0 = auto-detect).",
      new options::UInt32Parameter(&_consolidation_threads))
    .setLongDescription(R"(The option value must fall in the range
`[ 1..search.consolidation-threads ]`. Set it to `0` to automatically
choose a sensible number based on the number of cores in the system.)");

  options
    ->addOption(kCommitThreadsParam,
                "The upper limit to the allowed number of commit threads "
                "(0 = auto-detect).",
                new options::UInt32Parameter(&_commit_threads))
    .setLongDescription(R"(The option value must fall in the range
`[ 1..4 * NumberOfCores ]`. Set it to `0` to automatically choose a sensible
number based on the number of cores in the system.)");

  options->addOption(
    kSkipRecovery,  // TODO: Move parts of the descriptions to
                    // longDescription?
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

  options
    ->addOption(kFailOnOutOfSync,
                "Whether retrieval queries on out-of-sync "
                "View links and inverted indexes should fail.",
                new options::BooleanParameter(&_fail_queries_on_out_of_sync))

    .setLongDescription(R"(If set to `true`, any data retrieval queries on
out-of-sync links/indexes fail with the error 'collection/view is out of sync'
(error code 1481).

If set to `false`, queries on out-of-sync links/indexes are answered normally,
but the returned data may be incomplete.)");

  options->addOption(
    kSearchThreadsLimit,
    "The maximum number of threads that can be used to process "
    "Search indexes during a SEARCH operation of a query.",
    new options::UInt32Parameter(&_search_execution_threads_limit),
    options::MakeDefaultFlags(options::Flags::DefaultNoComponents,
                              options::Flags::OnDBServer,
                              options::Flags::OnSingle));

  options->addOption(
    kSearchDefaultParallelism, "Default parallelism for Search queries",
    new options::UInt32Parameter(&_default_parallelism),
    options::MakeDefaultFlags(options::Flags::DefaultNoComponents,
                              options::Flags::OnDBServer,
                              options::Flags::OnSingle));
}

void SearchEngine::validateOptions(
  std::shared_ptr<options::ProgramOptions> options) {
  // validate all entries in _skip_recovery_items for formal correctness
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
      SDB_FATAL("xxxxx", Logger::SEARCH, "invalid format for '", kSkipRecovery,
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
  _consolidation_threads =
    ComputeThreadsCount(_consolidation_threads, threads_limit, 6);

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

  // RegisterTransactionDataSourceRegistrationCallback();
  // RegisterRecoveryHelper();

  // RegisterFilters();
  // RegisterScorers();
  // RegisterFunctions();

  // ensure no tasks are scheduled and no threads are started
  SDB_ASSERT(std::make_tuple(size_t(0), size_t(0), size_t(0)) ==
             stats(ThreadGroup::Commit));
  SDB_ASSERT(std::make_tuple(size_t(0), size_t(0), size_t(0)) ==
             stats(ThreadGroup::Consolidation));
}

void SearchEngine::start() {
  SDB_ASSERT(isEnabled());

  // here can be registered upgrade tasks if needed

  if (ServerState::instance()->IsDBServer() ||
      ServerState::instance()->IsSingle()) {
    SDB_ASSERT(_commit_threads);
    SDB_ASSERT(_consolidation_threads);

    _thread_pools->Get(ThreadGroup::Commit)
      .start(_commit_threads, IR_NATIVE_STRING("search:commit"));
    _thread_pools->Get(ThreadGroup::Consolidation)
      .start(_consolidation_threads, IR_NATIVE_STRING("search:compact"));

    SDB_INFO("xxxxx", Logger::SEARCH, "Search maintenance: [", _commit_threads,
             "..", _commit_threads, "] commit thread(s), [",
             _consolidation_threads, "..", _consolidation_threads,
             "] consolidation thread(s). Search execution parallel threads "
             "limit: ",
             _search_execution_threads_limit);

  }
}

void SearchEngine::stop() {
  SDB_ASSERT(isEnabled());
  _thread_pools->Stop();
}

void SearchEngine::unprepare() { SDB_ASSERT(isEnabled()); }

void CleanupDatabase(ObjectId database_id) {
  const auto& feature =
    SerenedServer::Instance().getFeature<DatabasePathFeature>();
  auto path = GetPersistedPath(feature, database_id);
  std::error_code error;
  std::filesystem::remove_all(path, error);
  if (error) [[unlikely]] {
    SDB_ERROR("xxxxx", Logger::SEARCH,
              "Failed to remove search path for database '", database_id,
              "' with error '", error.message(), "'");
  }
}

bool SearchEngine::queue(ThreadGroup id, absl::Duration delay,
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
    SDB_WARN("xxxxx", Logger::SEARCH,
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
  SDB_IF_FAILURE("Search::FailQueriesOnOutOfSync") {
    // here to test --search.fail-queries-on-out-of-sync
    return true;
  }
  return _fail_queries_on_out_of_sync;
}

std::filesystem::path SearchEngine::GetPersistedPath(
  ObjectId database_id) const {
  return ::sdb::search::GetPersistedPath(_dir_feature, database_id);
}

ResultOr<std::shared_ptr<DataStore>> SearchEngine::CreateDataStore(
  const catalog::Index& index) {
  DataStoreOptions options;
  options.path = GetPersistedPath(index.GetDatabaseId());
  options.path /= absl::StrCat(index.GetId());
  // TODO(codeworse): setup format option properly
  options.codec = irs::formats::Get(search::GetFormat(LinkVersion::Max));
  if (!options.codec) {
    return std::unexpected<Result>{
      std::in_place,
      ERROR_INTERNAL,
      absl::StrCat("failed to get default codec for index '", index.GetName(),
                   "'"),
    };
  }
  return std::make_shared<DataStore>(options);
}

Result SearchEngine::DropDataStore(ObjectId database_id, ObjectId index_id) {
  return {ERROR_NOT_IMPLEMENTED};
}

}  // namespace sdb::search
