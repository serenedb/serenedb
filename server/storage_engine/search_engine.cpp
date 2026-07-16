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
#include <duckdb/common/file_system.hpp>
#include <iresearch/analysis/classification_tokenizer.hpp>
#include <iresearch/analysis/fast_text_model.hpp>
#include <iresearch/analysis/nearest_neighbors_tokenizer.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/search/filter_optimizer.hpp>
#include <utility>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/lifecycle.h"
#include "basics/log.h"
#include "basics/number_of_cores.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/view.h"
#include "pg/sql_exception_macro.h"
#include "rest_server/database_path_feature.h"
#include "search/inverted_index_storage.h"
#include "search/search_db_wal.h"
#include "search/search_table_recovery.h"
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
  // Replay each database's search-table WAL into iresearch (delta-based and
  // unconditional, mirroring inverted-index recovery).
  RunSearchTableRecovery(false);
  // Only now that every shard is fully replayed + committed do we start the
  // search-table background loops -- never while recovery is still rebuilding a
  // table, or a background commit's WAL GC could reclaim un-replayed chunks.
  StartSearchTableMaintenance();
  SDB_INFO(SEARCH, "Search maintenance: per-index refresh/compaction loops");
}

void SearchEngine::stop() {
  _stopping.store(true, std::memory_order_release);
  _loops.Done();
  _loops.Wait();
  // Close the per-database WALs (flush + release file handles) before shutdown.
  absl::MutexLock lock(&_db_wals_mu);
  _db_wals.clear();
}

template<class Storage>
void SearchEngine::StartTasks(const std::shared_ptr<Storage>& storage) {
  SDB_ASSERT(storage);
  if (_stopping.load(std::memory_order_acquire)) {
    return;
  }
  _loops.Consume(RefreshLoop<Storage>(storage),
                 CompactionCoordinator<Storage>(storage));
}

template void SearchEngine::StartTasks(
  const std::shared_ptr<InvertedIndexStorage>&);
template void SearchEngine::StartTasks(const std::shared_ptr<SearchTable>&);

std::filesystem::path SearchEngine::GetPersistedPath(
  ObjectId database_id) const {
  std::filesystem::path path = _dir_feature.directory();
  path /= kEngineDirRoot;
  path /= absl::StrCat(database_id);
  return path;
}

SearchDbWal& SearchEngine::GetDbWal(ObjectId database_id) {
  absl::MutexLock lock(&_db_wals_mu);
  auto it = _db_wals.find(database_id);
  if (it == _db_wals.end()) {
    // Borrow the process-wide FileSystem (owned by the DuckDB instance, which
    // outlives the engine). The WAL lives at GetPersistedPath(db)/wal/.
    auto& fs = duckdb::FileSystem::GetFileSystem(
      sdb::DuckDBEngine::Instance().instance());
    auto wal_dir = GetPersistedPath(database_id) / "wal";
    it = _db_wals
           .emplace(database_id,
                    std::make_unique<SearchDbWal>(fs, std::move(wal_dir)))
           .first;
  }
  return *it->second;
}

}  // namespace sdb::search
