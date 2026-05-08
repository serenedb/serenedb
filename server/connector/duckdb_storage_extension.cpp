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
#include "basics/debugging.h"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "catalog/databases.h"
#include "connector/duckdb_catalog.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_transaction.h"
#include "connector/optimizer/iresearch_plan.h"
#include "connector/optimizer/rocksdb_plan.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "rest_server/serened.h"

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
    auto r = catalog::CreateDatabase(exec_ctx, name);
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
    return duckdb::make_uniq<SereneDBCatalog>(db, database->GetId());
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
  return duckdb::make_uniq<SereneDBCatalog>(db, database->GetId());
}

duckdb::unique_ptr<duckdb::TransactionManager> CreateTransactionManager(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::AttachedDatabase& db, duckdb::Catalog& catalog) {
  return duckdb::make_uniq<SereneDBTransactionManager>(db);
}

void DropSereneDB(duckdb::ClientContext& context, const duckdb::string& name,
                  duckdb::OnEntryNotFound if_not_found) {
  auto state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey);
  if (state && state->GetConnectionContext().GetDatabase() == name) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                    ERR_MSG("cannot drop the currently open database"));
  }
  const auto& exec_ctx =
    state ? state->GetConnectionContext() : ExecContext::superuser();
  // Release current connection's catalog snapshot so the Database object
  // can become unreferenced, allowing synchronous detach by DuckDB.
  if (state) {
    state->GetConnectionContext().DropCatalogSnapshot();
  }
  auto r = catalog::DropDatabase(exec_ctx, name);
  if (r.is(ERROR_SERVER_DATABASE_NOT_FOUND)) {
    if (if_not_found == duckdb::OnEntryNotFound::RETURN_NULL) {
      return;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                    ERR_MSG("database \"", name, "\" does not exist"));
  }
  SDB_IF_FAILURE("crash_on_drop") { SDB_IMMEDIATE_ABORT(); }
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

void RegisterSereneDBOptimizers(duckdb::DatabaseInstance& db) {
  // iresearch_plan runs first: iresearch-only predicates (sdb_phrase,
  // distance(), ...) cannot be evaluated by the rocksdb layer, so any
  // such predicate must claim the scan as iresearch_search / ann_topk /
  // ann_range. Mutation subtrees are skipped (iresearch is eventually
  // consistent with the rocksdb-backed table).
  optimizer::RegisterIresearchPlanOptimizer(db);
  // rocksdb_plan runs after iresearch: PK / SK predicates become
  // specialised pk_*/sk_* scans only when no iresearch claim took the
  // scan first.
  optimizer::RegisterRocksDBPlanOptimizer(db);
}

}  // namespace sdb::connector
