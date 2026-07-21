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
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/log.h"
#include "catalog/catalog.h"
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
                              ObjectId table_id,
                              std::vector<duckdb::BoundIndex*>& out) {
  const auto store_name = catalog::StoreTableName(table_id);
  auto& entry = duckdb::Catalog::GetEntry(
                  context, duckdb::CatalogType::TABLE_ENTRY,
                  duckdb::QualifiedName(
                    duckdb::Identifier{catalog::kStoreDatabaseName},
                    duckdb::Identifier{"main"}, duckdb::Identifier{store_name}))
                  .Cast<duckdb::DuckTableEntry>();
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
                   duckdb::BoundIndex& index_in)
    : BaseExecutorTask{executor_in}, index{index_in} {}

  void ExecuteTask() override { index.FinishReplay(); }

  std::string TaskType() const override { return "InvertedFinishReplay"; }

  duckdb::BoundIndex& index;
};

struct RefreshTask final : duckdb::BaseExecutorTask {
  RefreshTask(duckdb::TaskExecutor& executor_in,
              InvertedIndexStorage& storage_in)
    : BaseExecutorTask{executor_in}, storage{storage_in} {}

  void ExecuteTask() override { storage.Refresh(); }

  std::string TaskType() const override { return "InvertedRecoveryRefresh"; }

  InvertedIndexStorage& storage;
};

}  // namespace

void InitInvertedIndexes() {
  auto begin = std::chrono::steady_clock::now();

  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  SDB_ASSERT(snapshot);

  // Recovery is delta-based: duckdb's WAL replay buffered every store-table
  // insert/delete since the last checkpoint against the (unbound) inverted
  // index; binding the index now replays exactly that delta into the storage.
  // No table rebuild -- recovery cost is O(WAL), not O(table).
  std::vector<ObjectId> tables_to_bind;
  containers::FlatHashSet<ObjectId> seen_tables;
  std::vector<std::shared_ptr<InvertedIndexStorage>> recovering_storages;
  std::vector<std::shared_ptr<InvertedIndexStorage>> static_storages;

  for (const auto& database : snapshot->GetDatabases()) {
    for (const auto& schema : snapshot->GetSchemas(database->GetId())) {
      for (const auto& idx :
           snapshot->GetIndexes(database->GetId(), schema->GetName())) {
        if (idx->GetType() != catalog::ObjectType::InvertedIndex) {
          continue;
        }
        auto inv_storage =
          basics::downCast<const catalog::InvertedIndex>(*idx).GetData();
        SDB_ASSERT(inv_storage);
        // Keep ordinals monotone across restarts.
        TickDomain::Instance().SeedAtLeast(inv_storage->GetRecoveryTick());

        // View-backed indexes are static -- the view body doesn't change at
        // runtime, so the persisted index is already current.
        const auto relation = snapshot->GetObject(idx->GetRelationId());
        const bool table_backed =
          relation && relation->GetType() == catalog::ObjectType::Table;
        if (!table_backed) {
          inv_storage->StartTasks();
          static_storages.push_back(std::move(inv_storage));
          continue;
        }

        // Background refresh/compaction stay off until the feed is committed:
        // replay commits segments mid-feed, and a background writer commit
        // would make them durable ahead of the recorded cursor (a crash then
        // re-feeds those rows). StartTasks happens after the final refresh.
        inv_storage->StartRecovery();
        recovering_storages.push_back(std::move(inv_storage));
        const auto table_id = relation->GetId();
        if (seen_tables.insert(table_id).second) {
          tables_to_bind.push_back(table_id);
        }
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

  if (tables_to_bind.empty()) {
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
  for (const auto table_id : tables_to_bind) {
    CollectStoreTableReplays(*conn->context, table_id, to_finish);
  }
  duckdb::TaskExecutor executor{
    duckdb::TaskScheduler::GetScheduler(*conn->context)};
  for (auto* index : to_finish) {
    executor.ScheduleTask(duckdb::make_uniq<FinishReplayTask>(executor, *index));
  }
  executor.WorkOnTasks();

  // The replay committed the delta into each storage's writer, but the
  // storage's query snapshot is only refreshed by a background commit. Force it
  // now so the recovered rows are searchable the instant the server accepts
  // queries.
  for (auto& storage : recovering_storages) {
    executor.ScheduleTask(duckdb::make_uniq<RefreshTask>(executor, *storage));
  }
  executor.WorkOnTasks();

  for (auto& storage : recovering_storages) {
    storage->StartTasks();
  }

  const auto duration =
    absl::FromChrono(std::chrono::steady_clock::now() - begin);
  SDB_INFO(SEARCH, "search index recovery: bound ", tables_to_bind.size(),
           " table(s), ", recovering_storages.size(), " inverted index(es) in ",
           absl::FormatDuration(duration));
}

}  // namespace sdb::search
