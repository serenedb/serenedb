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
#include <iresearch/analysis/keyword_tokenizer.hpp>
#include <iresearch/analysis/nearest_neighbors_tokenizer.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "scheduler/background_scheduler.h"
#include "search/inverted_index_storage.h"
#include "search/search_table_recovery.h"
#include "search/task.h"
#include "search/wal_recovery.h"
#include "server/utils/lifecycle.h"
#include "server/utils/number_of_cores.h"

ABSL_DECLARE_FLAG(uint64_t, background_threads);
ABSL_DECLARE_FLAG(bool, skip_search_recovery);

namespace sdb::search {

SearchEngine::SearchEngine() { gInstance = this; }

int SearchEngine::MaxConcurrentCompactions() noexcept {
  // The background pool is max(logical/4, 2) threads (--background_threads,
  // resolved at startup). Merges may use all but one of them -- refresh,
  // cleanup, and drop are light and interleave on the single spare thread.
  return std::max<int>(
    1, static_cast<int>(absl::GetFlag(FLAGS_background_threads)) - 1);
}

uint32_t SearchEngine::MaxAnnBuildWorkers() noexcept {
  return static_cast<uint32_t>(BackgroundScheduler::AnnBuildBudget());
}

const irs::AnnBuildEnv& AnnBuildEnv() {
  static const irs::AnnBuildEnv env{
    .executor = &BackgroundScheduler::instance().annExecutor(),
    .acquire = AnnAcquireWorkers,
    .release = AnnReleaseWorkers};
  return env;
}

void SearchEngine::start() {
  StartInvertedIndexTasks();
  if (!absl::GetFlag(FLAGS_skip_search_recovery)) {
    RunSearchTableRecovery();
  }
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
}

template<class Storage>
void SearchEngine::StartTasks(const std::shared_ptr<Storage>& storage) {
  if (_stopping.load(std::memory_order_acquire)) {
    return;
  }
  _loops.Consume(RefreshLoop<Storage>(storage),
                 CompactionCoordinator<Storage>(storage));
}

template void SearchEngine::StartTasks(
  const std::shared_ptr<InvertedIndexStorage>&);
template void SearchEngine::StartTasks(const std::shared_ptr<SearchTable>&);

}  // namespace sdb::search
