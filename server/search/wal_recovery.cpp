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

#include <absl/container/flat_hash_set.h>
#include <absl/time/time.h>

#include <chrono>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/storage/data_table.hpp>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/errors.h"
#include "basics/log.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "connector/inverted_store_index.h"
#include "search/inverted_index_shard.h"
#include "search/tick_domain.h"

namespace sdb::search {
namespace {

// Trigger duckdb to bind the inverted indexes on one store table. Binding
// applies the operations buffered during this boot's WAL replay
// (InvertedStoreIndex::Append/Delete with no committing context), feeding the
// post-checkpoint delta into the iresearch shard. The shards must already be
// loaded (this runs after InitCatalog) so the index's replay path
// can resolve them.
void BindStoreTableIndexes(duckdb::ClientContext& context,
                           std::string_view database_name,
                           std::string_view schema_name,
                           std::string_view table_name) {
  const auto store_name =
    catalog::StoreTableName(database_name, schema_name, table_name);
  auto& entry = duckdb::Catalog::GetEntry(
                  context, duckdb::CatalogType::TABLE_ENTRY,
                  std::string{catalog::kStoreDatabaseName}, "main", store_name)
                  .Cast<duckdb::DuckTableEntry>();
  entry.GetStorage().GetDataTableInfo()->BindIndexes(
    context, connector::InvertedStoreIndex::kTypeName);
}

}  // namespace

void InitInvertedIndexes(bool skip_wal_recovery) {
  auto begin = std::chrono::steady_clock::now();

  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  SDB_ASSERT(snapshot);

  // Recovery is delta-based: duckdb's WAL replay buffered every store-table
  // insert/delete since the last checkpoint against the (unbound) inverted
  // index; binding the index now replays exactly that delta into the shard.
  // No table rebuild -- recovery cost is O(WAL), not O(table).
  struct TableCoord {
    std::string database_name;
    std::string schema_name;
    std::string table_name;
  };
  std::vector<TableCoord> tables_to_bind;
  absl::flat_hash_set<ObjectId> seen_tables;
  std::vector<std::shared_ptr<InvertedIndexShard>> recovering_shards;
  std::vector<std::shared_ptr<InvertedIndexShard>> static_shards;

  for (const auto& database : snapshot->GetDatabases()) {
    for (const auto& schema : snapshot->GetSchemas(database->GetId())) {
      for (const auto& idx :
           snapshot->GetIndexes(database->GetId(), schema->GetName())) {
        if (idx->GetType() != catalog::ObjectType::InvertedIndex) {
          continue;
        }
        auto inv_shard = basics::downCast<InvertedIndexShard>(
          snapshot->GetIndexShard(idx->GetId()));
        SDB_ASSERT(inv_shard);
        // Keep ordinals monotone across restarts.
        TickDomain::Instance().SeedAtLeast(inv_shard->GetRecoveryTick());
        inv_shard->StartTasks();

        // View-backed indexes are static -- the view body doesn't change at
        // runtime, so the persisted index is already current.
        const auto relation = snapshot->GetObject(idx->GetRelationId());
        const bool table_backed =
          relation && relation->GetType() == catalog::ObjectType::Table;
        if (skip_wal_recovery || !table_backed) {
          static_shards.push_back(std::move(inv_shard));
          continue;
        }

        inv_shard->StartRecovery();
        recovering_shards.push_back(std::move(inv_shard));
        const auto table_id = relation->GetId();
        if (seen_tables.insert(table_id).second) {
          tables_to_bind.push_back(TableCoord{
            std::string{database->GetName()}, std::string{schema->GetName()},
            std::string{relation->GetName()}});
        }
      }
    }
  }

  irs::Finally finish_recovering = [&] noexcept {
    for (auto& shard : static_shards) {
      shard->FinishCreation();
    }
    for (auto& shard : recovering_shards) {
      shard->FinishCreation();
    }
  };

  if (tables_to_bind.empty()) {
    return;
  }

  // One scratch connection drives the binds; BindIndexes applies the buffered
  // replays synchronously (InvertedStoreIndex::FinishReplay commits the delta
  // into the shard). The bind path resolves the catalog through the
  // connection's transaction, so an explicit transaction must be active.
  auto conn = DuckDBEngine::Instance().CreateConnection();
  conn->BeginTransaction();
  irs::Finally end_txn = [&] noexcept {
    try {
      conn->Commit();
    } catch (...) {  // NOLINT(bugprone-empty-catch)
    }
  };
  for (const auto& coord : tables_to_bind) {
    BindStoreTableIndexes(*conn->context, coord.database_name,
                          coord.schema_name, coord.table_name);
  }

  // The replay committed the delta into each shard's writer, but the shard's
  // query snapshot is only refreshed by a background commit. Force it now so
  // the recovered rows are searchable the instant the server accepts queries.
  for (auto& shard : recovering_shards) {
    shard->Refresh();
  }

  const auto duration =
    absl::FromChrono(std::chrono::steady_clock::now() - begin);
  SDB_INFO(SEARCH, "search index recovery: bound ", tables_to_bind.size(),
           " table(s), ", recovering_shards.size(), " inverted index(es) in ",
           absl::FormatDuration(duration));
}

}  // namespace sdb::search
