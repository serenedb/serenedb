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

#include <absl/cleanup/cleanup.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <chrono>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/table/data_table_info.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <memory>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "connector/inverted_store_index.h"
#include "search/inverted_index_storage.h"
#include "search/tick_domain.h"

namespace sdb::search {

void InitInvertedIndexes() {
  const auto begin = std::chrono::steady_clock::now();
  std::vector<std::shared_ptr<InvertedIndexStorage>> recovering;
  std::vector<std::shared_ptr<InvertedIndexStorage>> statics;
  std::vector<duckdb::DuckTableEntry*> tables;
  irs::containers::FlatHashSet<duckdb::idx_t> seen_tables;

  auto conn = irs::DuckDBEngine::Instance().CreateConnection();
  auto& context = *conn->context;
  context.RunFunctionInTransaction([&] {
    for (const auto& database :
         duckdb::DatabaseManager::Get(irs::DuckDBEngine::Instance().instance())
           .GetDatabases()) {
      auto& catalog = database->GetCatalog();
      if (catalog.GetCatalogType() != catalog::SereneDBCatalog::kStorageType) {
        continue;
      }
      const auto transaction = catalog.GetCatalogTransaction(context);
      catalog.Cast<catalog::SereneDBCatalog>().ScanSchemas(
        [&](duckdb::SchemaCatalogEntry& schema) {
          schema.Scan(
            duckdb::CatalogType::INDEX_ENTRY, [&](duckdb::CatalogEntry& entry) {
              if (!connector::IsInvertedIndex(
                    entry.Cast<duckdb::IndexCatalogEntry>())) {
                return;
              }
              auto& index = entry.Cast<catalog::InvertedIndexEntry>();
              const auto& storage = index.Storage();
              if (!storage) {
                return;
              }
              TickDomain::Instance().SeedAtLeast(storage->GetRecoveryTick());
              storage->StartTasks();
              auto relation =
                schema.GetEntry(transaction, duckdb::CatalogType::TABLE_ENTRY,
                                index.GetTableName());
              const bool table_backed =
                relation &&
                relation->type == duckdb::CatalogType::TABLE_ENTRY &&
                relation->Cast<duckdb::TableCatalogEntry>().IsDuckTable();
              if (!table_backed) {
                statics.push_back(storage);
                return;
              }
              storage->StartRecovery();
              recovering.push_back(storage);
              if (seen_tables.insert(relation->oid).second) {
                tables.push_back(&relation->Cast<duckdb::DuckTableEntry>());
              }
            });
        });
    }

    absl::Cleanup finish = [&] {
      for (auto& storage : statics) {
        storage->FinishCreation();
      }
      for (auto& storage : recovering) {
        storage->FinishCreation();
      }
    };

    auto& search_path = *duckdb::ClientData::Get(context).catalog_search_path;
    for (auto* table : tables) {
      search_path.Set(duckdb::CatalogSearchEntry{table->catalog.GetName(),
                                                 table->ParentSchemaName()},
                      duckdb::CatalogSetPathType::SET_SCHEMA);
      table->GetStorage().GetDataTableInfo()->BindIndexes(
        context, connector::InvertedStoreIndex::kTypeName);
    }
    search_path.Reset();
    for (auto& storage : recovering) {
      storage->Refresh();
    }
  });

  if (tables.empty()) {
    return;
  }
  const auto duration =
    absl::FromChrono(std::chrono::steady_clock::now() - begin);
  SDB_INFO(SEARCH, "search index recovery: bound ", tables.size(),
           " table(s), ", recovering.size(), " inverted index(es) in ",
           absl::FormatDuration(duration));
}

}  // namespace sdb::search
