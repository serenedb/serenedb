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

#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <duckdb/storage/storage_extension.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/system_compiler.hpp>

#include "catalog/boot.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/foreign_server.h"
#include "connector/duckdb_client_state.h"
#include "connector/optimizer/iresearch_plan.h"
#include "connector/optimizer/wrap_unsupported_types.h"
#include "pg/connection_context.h"
#include "pg/pg_types.h"
#include "pg/sql_utils.h"
#include "server/utils/app_server.h"

namespace sdb::connector {
namespace {

duckdb::unique_ptr<duckdb::Catalog> AttachSereneDB(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::ClientContext& context, duckdb::AttachedDatabase& db,
  const duckdb::string& name, duckdb::AttachInfo& info,
  duckdb::AttachOptions& options) {
  if (info.path.empty()) {
    auto& cluster = catalog::ClusterOf(context);
    const auto transaction = cluster.GetCatalogTransaction(context);
    auto entry = cluster.GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
                   .GetEntry(transaction, info.name);
    if (!entry) {
      duckdb::CreateDatabaseInfo database;
      database.SetName(info.name);
      auto* connection = GetSereneDBContextPtr(context);
      database.permissions.owner =
        connection ? connection->GetRoleId() : pg::kRootUser;
      entry = cluster.CreateDatabase(transaction, database);
    }
    db.oid = entry->oid;
    info.path = static_cast<const catalog::DataDirectory&>(*storage_info)
                  .DatabaseFile(entry->oid);
  }
  // Every serenedb on-disk format sits behind our storage version, so a
  // duckdb-version database is unaffected by anything we change.
  options.options.emplace("storage_version", duckdb::Value{"serenedb_v1"});
  return duckdb::make_uniq<catalog::SereneDBCatalog>(db);
}

duckdb::unique_ptr<duckdb::TransactionManager> CreateTransactionManager(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::AttachedDatabase& db, duckdb::Catalog& catalog) {
  return duckdb::make_uniq<duckdb::DuckTransactionManager>(db);
}

}  // namespace

void RegisterSereneDBStorage(
  duckdb::DBConfig& config, duckdb::shared_ptr<catalog::DataDirectory> layout) {
  auto extension = duckdb::make_shared_ptr<duckdb::StorageExtension>();
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
