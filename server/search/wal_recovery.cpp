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
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database_manager.hpp>
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
#include "catalog1/catalog.h"
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
                           duckdb::DuckTableEntry& table) {
  table.GetStorage().GetDataTableInfo()->BindIndexes(
    context, connector::InvertedStoreIndex::kTypeName);
}

}  // namespace

void InitInvertedIndexes() {
  auto begin = std::chrono::steady_clock::now();

  // Recovery is delta-based: duckdb's WAL replay buffered every store-table
  // insert/delete since the last checkpoint against the (unbound) inverted
  // index; binding the index now replays exactly that delta into the storage.
  // No table rebuild -- recovery cost is O(WAL), not O(table).
  //
  // The storage does not exist until the bind builds the index, so unlike the
  // catalog-entry-owned storage this replaced, the walk collects tables first
  // and reads storages back off the bound indexes afterwards.
  std::vector<duckdb::reference<duckdb::DuckTableEntry>> tables_to_bind;
  auto& manager =
    duckdb::DatabaseManager::Get(DuckDBEngine::Instance().instance());
  for (auto& attached : manager.GetDatabases()) {
    auto& catalog = attached->GetCatalog();
    if (catalog.GetCatalogType() != catalog::SereneDBCatalog::kStorageType) {
      continue;
    }
    catalog.Cast<catalog::SereneDBCatalog>().ScanSchemas(
      [&](duckdb::SchemaCatalogEntry& schema) {
        schema.Scan(
          duckdb::CatalogType::TABLE_ENTRY, [&](duckdb::CatalogEntry& entry) {
            auto* table = dynamic_cast<duckdb::DuckTableEntry*>(&entry);
            if (table == nullptr) {
              return;
            }
            if (!table->GetStorage().GetDataTableInfo()->GetIndexes().Empty()) {
              tables_to_bind.emplace_back(*table);
            }
          });
      });
  }

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
  for (auto table : tables_to_bind) {
    BindStoreTableIndexes(*conn->context, table.get());
  }

  // Each bind built its index and opened its storage, so the storages exist
  // only now. Seeding keeps ordinals monotone across restarts; the refresh is
  // because the replay committed the delta into the writer but the query
  // snapshot only advances on a background commit, and the recovered rows must
  // be searchable the instant the server accepts queries.
  size_t recovered = 0;
  for (auto table : tables_to_bind) {
    for (auto& index :
         table.get().GetStorage().GetDataTableInfo()->GetIndexes().Indexes()) {
      if (!index.IsBound() ||
          index.GetIndexType() !=
            std::string{connector::InvertedStoreIndex::kTypeName}) {
        continue;
      }
      const auto& storage =
        index.Cast<connector::InvertedStoreIndex>().Storage();
      if (storage) {
        TickDomain::Instance().SeedAtLeast(storage->GetRecoveryTick());
        storage->StartTasks();
        storage->Refresh();
        ++recovered;
      }
    }
  }

  const auto duration =
    absl::FromChrono(std::chrono::steady_clock::now() - begin);
  SDB_INFO(SEARCH, "search index recovery: bound ", tables_to_bind.size(),
           " table(s), ", recovered, " inverted index(es) in ",
           absl::FormatDuration(duration));
}

}  // namespace sdb::search
