////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "search/wal_recovery.h"

#include <absl/time/time.h>

#include <chrono>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/index/bound_index.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/storage/data_table.hpp>
#include <iresearch/index/index_writer.hpp>
#include <limits>
#include <memory>
#include <ranges>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/log.h"
#include "catalog/catalog.h"
#include "catalog/duckdb_catalog_sets.h"
#include "catalog/duckdb_table_entry.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "connector/inverted_store_index.h"
#include "search/inverted_index_storage.h"
#include "search/tick_domain.h"

namespace sdb::search {
namespace {

// Collect the injected inverted indexes of one store table. The indexes were
// injected bound when the store DataTable came alive during attach, so this
// boot's WAL replay streamed the post-checkpoint delta straight into each
// index's replay session; FinishReplay commits it into the iresearch storage.
void CollectStoreTableReplays(duckdb::ClientContext& context,
                              ObjectId database_id, ObjectId table_id,
                              std::vector<duckdb::BoundIndex*>& out) {
  auto& entry =
    catalog::GetStoreTableEntry(context, database_id, table_id,
                                duckdb::OnEntryNotFound::THROW_EXCEPTION)
      ->Cast<duckdb::DuckTableEntry>();
  for (auto& index :
       entry.GetStorage().GetDataTableInfo()->GetIndexes().Indexes()) {
    if (index.IsBound() &&
        index.GetIndexType() == connector::InvertedStoreIndex::kTypeName) {
      out.push_back(&index.Cast<duckdb::BoundIndex>());
    }
  }
}

struct FinishReplayTask final : duckdb::BaseExecutorTask {
  FinishReplayTask(duckdb::TaskExecutor& executor_in,
                   duckdb::BoundIndex& index_in,
                   std::shared_ptr<InvertedIndexStorage> storage_in)
    : BaseExecutorTask{executor_in},
      index{index_in},
      storage{std::move(storage_in)} {}

  // Refresh right after this index's own replay rather than in a second stage:
  // a refresh depends only on the index it belongs to, so a global barrier
  // between the stages would make every index wait out the largest delta before
  // any of them becomes searchable.
  void ExecuteTask() override {
    index.FinishReplay();
    if (storage) {
      storage->Refresh();
    }
  }

  std::string TaskType() const override { return "InvertedFinishReplay"; }

  duckdb::BoundIndex& index;
  std::shared_ptr<InvertedIndexStorage> storage;
};

}  // namespace

void InitInvertedIndexes() {
  auto begin = std::chrono::steady_clock::now();

  // Recovery is delta-based: the indexes were injected bound before any of
  // their table's WAL operations replayed, so replay fed exactly the delta
  // since the last checkpoint. No table rebuild -- recovery cost is O(WAL),
  // not O(table).
  std::vector<std::pair<ObjectId, ObjectId>> tables_to_finish;
  containers::FlatHashSet<ObjectId> seen_tables;
  std::vector<std::shared_ptr<InvertedIndexStorage>> recovering_storages;
  std::vector<std::shared_ptr<InvertedIndexStorage>> static_storages;

  std::vector<ObjectId> database_ids;
  catalog::VisitDatabases(nullptr, [&](const catalog::DatabaseRef& db) {
    database_ids.push_back(db.Id());
  });
  for (const auto db_id : database_ids) {
    std::vector<catalog::IndexInfoRef> indexes;
    catalog::VisitIndexes(nullptr, db_id,
                          [&](const catalog::IndexInfoRef& index) {
                            if (index->IsInverted()) {
                              indexes.push_back(index);
                            }
                          });
    for (const auto& idx : indexes) {
      auto inv_storage = idx->GetData();
      SDB_ASSERT(inv_storage);
      // Keep ordinals monotone across restarts.
      TickDomain::Instance().SeedAtLeast(inv_storage->GetRecoveryTick());
      inv_storage->StartTasks();

      // View-backed indexes are static -- the view body doesn't change at
      // runtime, so the persisted index is already current. The relation is
      // asked of its entry rather than of the snapshot: a view has never been
      // in one, so a snapshot lookup can only ever answer for a table, and a
      // miss would silently demote a live index to static.
      auto* relation =
        catalog::FindTableEntryIn(nullptr, db_id, idx->GetRelationId());
      if (relation == nullptr) {
        static_storages.push_back(std::move(inv_storage));
        continue;
      }

      inv_storage->StartRecovery();
      recovering_storages.push_back(std::move(inv_storage));
      const auto table_id = catalog::IdOf(*relation);
      if (seen_tables.insert(table_id).second) {
        tables_to_finish.emplace_back(db_id, table_id);
      }
    }
  }

  irs::Finally finish_recovering = [&] noexcept {
    for (auto& storage : static_storages) {
      storage->FinishCreation();
    }
    for (auto& storage : recovering_storages) {
      storage->FinishCreation();
    }
  };

  if (tables_to_finish.empty()) {
    return;
  }

  // One scratch connection resolves the store entries; FinishReplay commits
  // each index's streamed delta into the storage. Entry resolution goes
  // through the connection's transaction, so an explicit one must be active.
  auto conn = DuckDBEngine::Instance().CreateConnection();
  conn->BeginTransaction();
  irs::Finally end_txn = [&] noexcept {
    try {
      conn->Commit();
    } catch (...) {  // NOLINT(bugprone-empty-catch)
    }
  };
  std::vector<duckdb::BoundIndex*> to_finish;
  for (const auto [database_id, table_id] : tables_to_finish) {
    CollectStoreTableReplays(*conn->context, database_id, table_id, to_finish);
  }
  // The replay commits each delta into the storage's writer, but the query
  // snapshot only advances on a refresh -- force one per index so recovered
  // rows are searchable the instant the server accepts queries.
  containers::FlatHashMap<ObjectId, std::shared_ptr<InvertedIndexStorage>>
    storage_by_index;
  storage_by_index.reserve(recovering_storages.size());
  for (const auto& storage : recovering_storages) {
    storage_by_index.emplace(storage->GetId(), storage);
  }
  duckdb::TaskExecutor executor{
    duckdb::TaskScheduler::GetScheduler(*conn->context)};
  for (auto* index : to_finish) {
    const auto index_id =
      index->Cast<connector::InvertedStoreIndex>().IndexId();
    auto it = storage_by_index.find(index_id);
    executor.ScheduleTask(duckdb::make_uniq<FinishReplayTask>(
      executor, *index, it == storage_by_index.end() ? nullptr : it->second));
  }
  executor.WorkOnTasks();

  const auto duration =
    absl::FromChrono(std::chrono::steady_clock::now() - begin);
  SDB_INFO(SEARCH, "search index recovery: replayed ", tables_to_finish.size(),
           " table(s), ", recovering_storages.size(), " inverted index(es) in ",
           absl::FormatDuration(duration));
}

}  // namespace sdb::search
