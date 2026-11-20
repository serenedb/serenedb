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

#include "search/engine.h"

#include <absl/strings/escaping.h>

#include <algorithm>
#include <duckdb/common/fault_points.hpp>
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
#include "catalog/transaction.h"
#include "runtime/runtime.h"
#include "search/store.h"
#include "search/task.h"
#include "search/wal_recovery.h"
#include "server/utils/lifecycle.h"
#include "server/utils/number_of_cores.h"

namespace sdb::search {

int SearchEngine::MaxConcurrentCompactions() noexcept {
  return std::max<int>(1, static_cast<int>(CountLogicalCores()) / 4 - 1);
}

uint32_t SearchEngine::MaxAnnBuildWorkers() noexcept {
  return static_cast<uint32_t>(std::max<int64_t>(1, CountLogicalCores()));
}

const irs::AnnBuildEnv& AnnBuildEnv() { return GetSearchEngine().AnnEnv(); }

namespace {

uint32_t AnnAcquireAllWorkers(uint32_t want, uint32_t held) noexcept {
  return GetSearchEngine().AcquireAnnWorkers(
    want, held, SearchEngine::MaxAnnBuildWorkers());
}

}  // namespace

SearchEngine::SearchEngine(Runtime& runtime)
  : _runtime{runtime},
    _ann_env{.executor = &runtime.Background(),
             .acquire = AnnAcquireWorkers,
             .release = AnnReleaseWorkers},
    _manual_ann_env{.executor = &runtime.Background(),
                    .acquire = AnnAcquireAllWorkers,
                    .release = AnnReleaseWorkers},
    _snapshot_env{.executor = &runtime.Background(),
                  .acquire = SnapshotAcquireWorkers,
                  .release = SnapshotReleaseWorkers} {
  gInstance = this;
}

ManualCompactionSlot::ManualCompactionSlot() noexcept
  : _held{GetSearchEngine().TryAcquireCompaction()} {}

ManualCompactionSlot::~ManualCompactionSlot() {
  if (_held) {
    GetSearchEngine().ReleaseCompaction();
  }
}

const irs::AnnBuildEnv* ManualCompactionSlot::AnnEnv() const noexcept {
  return &GetSearchEngine().ManualAnnEnv();
}

void SearchEngine::Start() {
  _loops.Consume(MaintenanceLoop(_loops));
  StartSearchStorageTasks();
  catalog::Transactions::Get().SettleLog();
  SDB_IF_FAILURE("search_start_throws") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
}

bool SearchEngine::IsStopping() const noexcept { return _runtime.IsStopping(); }

void SearchEngine::Stop() {
  _loops.Done();
  _loops.Wait();
}

void SearchEngine::StartTasks(const std::shared_ptr<Store>& store) {
  if (IsStopping()) {
    return;
  }
  AddMaintenanceTarget(store);
}

}  // namespace sdb::search
