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

#include "connector/duckdb_storage_extension.h"

#include <absl/container/flat_hash_map.h>

#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/storage_extension.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/data_table_info.hpp>
#include <duckdb/storage/table/index_entry.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <memory>
#include <vector>

#include "catalog/boot.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/foreign_server.h"
#include "connector/duckdb_client_state.h"
#include "connector/inverted_store_index.h"
#include "connector/optimizer/iresearch_plan.h"
#include "connector/optimizer/wrap_unsupported_types.h"
#include "pg/connection_context.h"
#include "pg/pg_types.h"
#include "pg/sql_utils.h"
#include "search/inverted_index_storage.h"
#include "server/utils/app_server.h"

namespace sdb::connector {
namespace {

duckdb::unique_ptr<duckdb::Catalog> AttachSereneDB(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::ClientContext& context, duckdb::AttachedDatabase& db,
  const duckdb::string& name, duckdb::AttachInfo& info,
  duckdb::AttachOptions& options) {
  if (!info.path.empty() &&
      (info.path != IN_MEMORY_PATH || options.original_path)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("cannot attach \"", info.path,
              "\" as a SereneDB database: a SereneDB database is created "
              "with CREATE DATABASE"));
  }
  if (info.on_conflict == duckdb::OnCreateConflict::ERROR_ON_CONFLICT &&
      duckdb::DatabaseManager::Get(context).GetDatabase(info.name)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_DATABASE),
                    ERR_MSG("database \"", info.name.GetIdentifierName(),
                            "\" already exists"));
  }
  auto& cluster = catalog::ClusterOf(context);
  const auto transaction = cluster.GetCatalogTransaction(context);
  auto entry = cluster.GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
                 .GetEntry(transaction, info.name);
  if (!entry) {
    duckdb::CreateDatabaseInfo database;
    database.SetName(info.name);
    for (const auto* key : {"block_size", "row_group_size"}) {
      if (auto option = options.options.find(key);
          option != options.options.end()) {
        database.options.emplace(key, option->second);
      }
    }
    auto* connection = GetSereneDBContextPtr(context);
    database.permissions.owner =
      connection ? connection->GetRoleId() : pg::kRootUser;
    entry = cluster.CreateDatabase(transaction, database);
    SDB_IF_FAILURE("unable_to_create") {
      THROW_SQL_ERROR(ERR_MSG("internal error"));
    }
  }
  db.oid = entry->oid;
  const auto& directory =
    entry->Cast<catalog::DatabaseCatalogEntry>().Directory();
  if (info.path.empty()) {
    info.path = directory->DataFile();
  }
  db.HoldUntilClosed(duckdb::shared_ptr<duckdb::StorageExtensionInfo>{
    std::shared_ptr<duckdb::StorageExtensionInfo>{directory}});
  // Every serenedb on-disk format sits behind our storage version, so a
  // duckdb-version database is unaffected by anything we change.
  catalog::RequestSereneDBStorageVersion(options);
  return duckdb::make_uniq<catalog::SereneDBCatalog>(db, directory);
}

duckdb::unique_ptr<duckdb::TransactionManager> CreateTransactionManager(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::AttachedDatabase& db, duckdb::Catalog& catalog) {
  return duckdb::make_uniq<duckdb::DuckTransactionManager>(db);
}

std::vector<std::shared_ptr<search::InvertedIndexStorage>> BoundStorages(
  duckdb::AttachedDatabase& db) {
  std::vector<std::shared_ptr<search::InvertedIndexStorage>> storages;
  if (!InvertedStoreIndex::AnyBound()) {
    return storages;
  }
  db.GetCatalog().Cast<duckdb::DuckCatalog>().ScanSchemas(
    [&](duckdb::SchemaCatalogEntry& schema) {
      schema.Scan(
        duckdb::CatalogType::TABLE_ENTRY, [&](duckdb::CatalogEntry& entry) {
          if (entry.type != duckdb::CatalogType::TABLE_ENTRY ||
              !entry.Cast<duckdb::TableCatalogEntry>().IsDuckTable()) {
            return;
          }
          auto& indexes = entry.Cast<duckdb::DuckTableEntry>()
                            .GetStorage()
                            .GetDataTableInfo()
                            ->GetIndexes();
          for (auto index : indexes.IndexEntries()) {
            if (index->GetBindState() == duckdb::IndexBindState::BOUND &&
                index->GetIndexType() == InvertedStoreIndex::kTypeName) {
              const auto handle = index->GetReadHandle<InvertedStoreIndex>();
              storages.push_back(handle->Storage());
            }
          }
        });
    });
  return storages;
}

class SereneDBStorageExtension final : public duckdb::StorageExtension {
 public:
  void OnCheckpointBeforeHeader(duckdb::AttachedDatabase& db,
                                duckdb::CheckpointOptions) final {
    const auto iteration =
      db.GetStorageManager().GetBlockManager().GetCheckpointIteration() + 1;
    std::vector<std::weak_ptr<search::InvertedIndexStorage>> saves;
    for (auto& storage : BoundStorages(db)) {
      storage->PrepareCheckpoint(iteration);
      saves.push_back(storage);
    }
    duckdb::lock_guard<duckdb::mutex> lock{_saves_mutex};
    if (saves.empty()) {
      _saves.erase(db.oid);
    } else {
      _saves[db.oid] = std::move(saves);
    }
  }

  void OnCheckpointEnd(duckdb::AttachedDatabase& db,
                       duckdb::CheckpointOptions) final {
    std::vector<std::weak_ptr<search::InvertedIndexStorage>> saves;
    {
      duckdb::lock_guard<duckdb::mutex> lock{_saves_mutex};
      const auto it = _saves.find(db.oid);
      if (it == _saves.end()) {
        return;
      }
      saves = std::move(it->second);
      _saves.erase(it);
    }
    for (const auto& save : saves) {
      if (const auto storage = save.lock()) {
        storage->FinishCheckpoint();
      }
    }
  }

 private:
  duckdb::mutex _saves_mutex;
  absl::flat_hash_map<duckdb::idx_t,
                      std::vector<std::weak_ptr<search::InvertedIndexStorage>>>
    _saves;
};

}  // namespace

void RegisterSereneDBStorage(
  duckdb::DBConfig& config, duckdb::shared_ptr<catalog::DataDirectory> layout) {
  auto extension = duckdb::make_shared_ptr<SereneDBStorageExtension>();
  extension->attach = AttachSereneDB;
  extension->create_transaction_manager = CreateTransactionManager;
  extension->storage_info = std::move(layout);
  duckdb::StorageExtension::Register(config, "serenedb", std::move(extension));
}

void RegisterSereneDBOptimizers(duckdb::DatabaseInstance& db) {
  optimizer::RegisterWrapUnsupportedTypesExtension(db);
  optimizer::RegisterIResearchPlanOptimizer(db);
}

}  // namespace sdb::connector
