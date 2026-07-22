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

#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>

#include "app/app_server.h"
#include "basics/debugging.h"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "catalog/databases.h"
#include "connector/duckdb_catalog.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_transaction.h"
#include "connector/optimizer/iresearch_plan.h"
#include "connector/optimizer/rbac.h"
#include "connector/optimizer/rls.h"
#include "connector/optimizer/wrap_unsupported_types.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"

namespace sdb::connector {
namespace {

duckdb::unique_ptr<duckdb::Catalog> AttachSereneDB(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::ClientContext& context, duckdb::AttachedDatabase& db,
  const duckdb::string& name, duckdb::AttachInfo& info,
  duckdb::AttachOptions& options) {
  if (info.path.empty()) {
    // CREATE DATABASE: create new database in SereneDB catalog
    auto state = context.registered_state->Get<SereneDBClientState>(
      kSereneDBClientStateKey);
    const auto ax =
      state ? catalog::ActingAs(state->GetConnectionContext().GetRoleId())
            : catalog::NoAccessCheck();
    const bool if_not_exists =
      info.on_conflict != duckdb::OnCreateConflict::ERROR_ON_CONFLICT;
    catalog::CreateDatabase(ax, name, if_not_exists);
    // Re-fetch snapshot to see the new database
    auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
    auto database = snapshot->GetDatabase(name);
    if (!database) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INTERNAL_ERROR),
        ERR_MSG("database \"", name, "\" not found after creation"));
    }
    return duckdb::make_uniq<SereneDBCatalog>(db, database->GetId());
  }

  // ATTACH with path = open existing database by ObjectId
  uint64_t id = 0;
  if (!absl::SimpleAtoi(info.path, &id)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("database \"", name, "\" not found"));
  }
  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  auto database = snapshot->GetDatabase(ObjectId{id});
  if (!database) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("database \"", name, "\" not found"));
  }
  return duckdb::make_uniq<SereneDBCatalog>(db, database->GetId());
}

duckdb::unique_ptr<duckdb::TransactionManager> CreateTransactionManager(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::AttachedDatabase& db, duckdb::Catalog& catalog) {
  return duckdb::make_uniq<SereneDBTransactionManager>(db);
}

}  // namespace

void SereneDBCatalog::OnDetach(duckdb::ClientContext& context) {
  auto state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey);

  auto ax = catalog::NoAccessCheck();
  if (state) {
    auto& conn_ctx = state->GetConnectionContext();
    ax = catalog::ActingAs(conn_ctx.GetRoleId());
    conn_ctx.DropCatalogSnapshot();
  }

  duckdb::shared_ptr<void> keep_alive = GetAttached().shared_from_this();
  catalog::DropDatabase(ax, GetName().GetIdentifierName(),
                        std::move(keep_alive));
  SDB_IF_FAILURE("crash_on_drop") { SDB_IMMEDIATE_ABORT(); }
}

SereneDBStorageExtension::SereneDBStorageExtension() {
  attach = AttachSereneDB;
  create_transaction_manager = CreateTransactionManager;
}

void RegisterSereneDBStorage(duckdb::DBConfig& config) {
  auto ext = duckdb::make_shared_ptr<SereneDBStorageExtension>();
  duckdb::StorageExtension::Register(config, "serenedb", std::move(ext));
}

void RegisterSereneDBOptimizers(duckdb::DatabaseInstance& db) {
  optimizer::RegisterWrapUnsupportedTypesExtension(db);
  optimizer::RegisterIResearchPlanOptimizer(db);
  optimizer::RegisterRbacAccessCheck(db);
  RegisterRlsEnforcement(db);
}

}  // namespace sdb::connector
