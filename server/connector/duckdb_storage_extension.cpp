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

#include <duckdb/main/config.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>

#include "app/app_server.h"
#include "catalog/catalog.h"
#include "catalog/databases.h"
#include "connector/duckdb_catalog.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_transaction.h"
#include "pg/connection_context.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "rest_server/serened.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::connector {
namespace {

duckdb::unique_ptr<duckdb::Catalog> AttachSereneDB(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::ClientContext& context, duckdb::AttachedDatabase& db,
  const duckdb::string& name, duckdb::AttachInfo& info,
  duckdb::AttachOptions& options) {
  auto& catalog_feature =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();

  if (info.path.empty()) {
    // CREATE DATABASE: create new database in SereneDB catalog
    auto state = context.registered_state->Get<SereneDBClientState>(
      kSereneDBClientStateKey);
    const auto& exec_ctx =
      state ? state->GetConnectionContext() : ExecContext::superuser();
    auto r =
      catalog::CreateDatabase(exec_ctx, catalog::DatabaseOptions{.name = name});
    if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
      if (info.on_conflict == duckdb::OnCreateConflict::ERROR_ON_CONFLICT) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_DATABASE),
                        ERR_MSG("database \"", name, "\" already exists"));
      }
    } else if (!r.ok()) {
      SDB_THROW(std::move(r));
    }
    // Re-fetch snapshot to see the new database
    auto snapshot = catalog_feature.Global().GetCatalogSnapshot();
    auto database = snapshot->GetDatabase(name);
    if (!database) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INTERNAL_ERROR),
        ERR_MSG("database \"", name, "\" not found after creation"));
    }
    return duckdb::make_uniq<SereneDBCatalog>(db, std::move(database));
  }

  // ATTACH with path = open existing database by ObjectId
  uint64_t id = 0;
  if (!absl::SimpleAtoi(info.path, &id)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("database \"", name, "\" not found"));
  }
  auto snapshot = catalog_feature.Global().GetCatalogSnapshot();
  auto database = snapshot->GetDatabase(ObjectId{id});
  if (!database) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("database \"", name, "\" not found"));
  }
  return duckdb::make_uniq<SereneDBCatalog>(db, std::move(database));
}

duckdb::unique_ptr<duckdb::TransactionManager> CreateTransactionManager(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::AttachedDatabase& db, duckdb::Catalog& catalog) {
  return duckdb::make_uniq<SereneDBTransactionManager>(db);
}

void DropSereneDB(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::ClientContext& context, const duckdb::string& name,
  duckdb::OnEntryNotFound if_not_found) {
  auto state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey);
  if (state && state->GetConnectionContext().GetDatabase() == name) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_IN_USE),
      ERR_MSG("cannot drop the currently open database"));
  }
  const auto& exec_ctx =
    state ? state->GetConnectionContext() : ExecContext::superuser();
  auto r = catalog::DropDatabase(exec_ctx, name);
  if (r.is(ERROR_SERVER_DATABASE_NOT_FOUND)) {
    if (if_not_found == duckdb::OnEntryNotFound::RETURN_NULL) {
      return;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                    ERR_MSG("database \"", name, "\" does not exist"));
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
}

}  // namespace

SereneDBStorageExtension::SereneDBStorageExtension() {
  attach = AttachSereneDB;
  create_transaction_manager = CreateTransactionManager;
  drop_database = DropSereneDB;
}

void RegisterSereneDBStorage(duckdb::DBConfig& config) {
  auto ext = duckdb::make_shared_ptr<SereneDBStorageExtension>();
  duckdb::StorageExtension::Register(config, "serenedb", std::move(ext));
}

}  // namespace sdb::connector
