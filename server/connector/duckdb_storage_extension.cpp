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
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>

#include "app/app_server.h"
#include "basics/debugging.h"
#include "basics/duckdb_engine.h"
#include "basics/system-compiler.h"
#include "catalog1/boot.h"
#include "catalog1/catalog.h"
#include "catalog1/entry/foreign_server.h"
#include "connector/duckdb_client_state.h"
#include "connector/optimizer/iresearch_plan.h"
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
  // Resolving the name to the database's own duckdb file is what gives the
  // attachment a real SingleFileStorageManager: its own storage, its own data
  // WAL, its own checkpoint. AttachedDatabase reads info.path after this
  // returns.
  if (info.path.empty()) {
    // CREATE DATABASE rather than a boot-time re-attach: the cluster record is
    // what makes the name resolvable, and it rides this statement's
    // transaction so a rollback takes it too.
    catalog::RegisterDatabaseIn(context, name);
    info.path = catalog::DatabaseFilePath(name);
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
}

}  // namespace sdb::connector
