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
#include <duckdb/main/connection.hpp>
#include <duckdb/storage/data_table.hpp>

#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/errors.h"
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

// Trigger duckdb to bind the inverted indexes on one store table. Binding
// applies the operations buffered during this boot's WAL replay
// (InvertedStoreIndex::Append/Delete with no committing context), feeding the
// post-checkpoint delta into the iresearch storage. The storages must already
// be loaded (this runs after InitCatalog) so the index's replay path can
// resolve them.
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

void InitInvertedIndexes() {
  auto begin = std::chrono::steady_clock::now();

  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  SDB_ASSERT(snapshot);

  // Recovery is delta-based: duckdb's WAL replay buffered every store-table
  // insert/delete since the last checkpoint against the (unbound) inverted
  // index; binding the index now replays exactly that delta into the storage.
  // No table rebuild -- recovery cost is O(WAL), not O(table).
  struct TableCoord {
    std::string database_name;
    std::string schema_name;
    std::string table_name;
  };
  std::vector<TableCoord> tables_to_bind;
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
        inv_storage->StartTasks();

        // View-backed indexes are static -- the view body doesn't change at
        // runtime, so the persisted index is already current.
        const auto relation = snapshot->GetObject(idx->GetRelationId());
        const bool table_backed =
          relation && relation->GetType() == catalog::ObjectType::Table;
        if (!table_backed) {
          static_storages.push_back(std::move(inv_storage));
          continue;
        }

        inv_storage->StartRecovery();
        recovering_storages.push_back(std::move(inv_storage));
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

  // One scratch connection drives the binds; BindIndexes applies the buffered
  // replays synchronously (InvertedStoreIndex::FinishReplay commits the delta
  // into the storage). The bind path resolves the catalog through the
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

  // The replay committed the delta into each storage's writer, but the
  // storage's query snapshot is only refreshed by a background commit. Force it
  // now so the recovered rows are searchable the instant the server accepts
  // queries.
  for (auto& storage : recovering_storages) {
    storage->Refresh();
  }

  const auto duration =
    absl::FromChrono(std::chrono::steady_clock::now() - begin);
  SDB_INFO(SEARCH, "search index recovery: bound ", tables_to_bind.size(),
           " table(s), ", recovering_storages.size(), " inverted index(es) in ",
           absl::FormatDuration(duration));
}

}  // namespace sdb::search
