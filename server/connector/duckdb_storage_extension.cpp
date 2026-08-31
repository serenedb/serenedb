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

#include "app/app_server.h"
#include "basics/debugging.h"
#include "basics/duckdb_engine.h"
#include "basics/system-compiler.h"
#include "catalog1/catalog.h"
#include "catalog1/entry/foreign_server.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_transaction.h"
#include "connector/optimizer/iresearch_plan.h"
#include "connector/optimizer/rbac.h"
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
  // The attach carries the database's duckdb::idx_t, not a file. Resolving it
  // to the database's own duckdb file is what gives the attachment a real
  // SingleFileStorageManager: its own storage, its own data WAL, its own
  // checkpoint. AttachedDatabase reads info.path after this returns.
  const auto open = [&db, &info, &options](duckdb::idx_t database_id,
                                           duckdb::idx_t public_schema_id,
                                           catalog::Permissions owner) {
    info.path = catalog::CatalogStore::DatabaseFilePath(database_id);
    // Every serenedb on-disk format sits behind our storage version, so a
    // duckdb-version database is unaffected by anything we change.
    options.options.emplace("storage_version", duckdb::Value{"serenedb_v1"});
    return duckdb::make_uniq<catalog::SereneDBCatalog>(
      db, database_id, public_schema_id, std::move(owner));
  };

  if (info.path.empty()) {
    // CREATE DATABASE: create new database in SereneDB catalog
    auto state = context.registered_state->Get<SereneDBClientState>(
      kSereneDBClientStateKey);
    const duckdb::idx_t owner =
      state ? state->GetConnectionContext().GetRoleId() : 0;
    const bool if_not_exists =
      info.on_conflict != duckdb::OnCreateConflict::ERROR_ON_CONFLICT;
    // The public schema's id and owner come back rather than the schema: the
    // catalog this call is about to build is what makes it.
    auto [public_schema_id, public_schema_owner] =
      catalog::GetCatalog().CreateDatabase(
        duckdb::make_uniq<catalog::CreateDatabaseInfo>(0, name, 0), owner,
        if_not_exists);
    const auto database_id = catalog::FindDatabaseId(&context, name);
    if (!database_id.isSet()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INTERNAL_ERROR),
        ERR_MSG("database \"", name, "\" not found after creation"));
    }
    return open(database_id, public_schema_id, std::move(public_schema_owner));
  }

  // ATTACH with path = open existing database by duckdb::idx_t
  uint64_t id = 0;
  if (!absl::SimpleAtoi(info.path, &id)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("database \"", name, "\" not found"));
  }
  auto database = catalog::FindDatabase(nullptr, id);
  if (!database) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("database \"", name, "\" not found"));
  }
  return open((*database).oid, database->PublicSchemaId(),
              database->permissions);
}

duckdb::unique_ptr<duckdb::TransactionManager> CreateTransactionManager(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::AttachedDatabase& db, duckdb::Catalog& catalog) {
  return duckdb::make_uniq<SereneDBTransactionManager>(db);
}

}  // namespace

void AttachDatabaseCatalog(duckdb::idx_t id, std::string_view name) {
  auto& manager =
    duckdb::DatabaseManager::Get(DuckDBEngine::Instance().instance());
  if (manager.GetDatabase(duckdb::Identifier{name})) {
    // A later version of the same database record -- an owner or ACL change.
    return;
  }
  auto conn = DuckDBEngine::Instance().CreateConnection();
  auto& context = *conn->context;
  duckdb::AttachInfo info;
  info.name = duckdb::Identifier{name};
  info.path = std::to_string(id.id());
  info.options.emplace(
    "type", duckdb::Value{std::string{catalog::kSereneDBCatalogType}});
  duckdb::AttachOptions options{info.options, duckdb::AccessMode::READ_WRITE};
  options.defer_storage_load = true;
  conn->BeginTransaction();
  try {
    duckdb::DatabaseManager::Get(context).AttachDatabase(context, info,
                                                         options);
  } catch (...) {
    conn->Rollback();
    throw;
  }
  conn->Commit();
}

void DiscardDatabaseAttachment(std::string_view name) {
  auto& manager =
    duckdb::DatabaseManager::Get(DuckDBEngine::Instance().instance());
  auto attached = manager.DetachInternal(duckdb::Identifier{name});
  if (!attached) {
    return;
  }
  catalog::DataStore::ForgetDatabase(
    attached->GetCatalog().Cast<catalog::SereneDBCatalog>().GetDatabaseId());
  // The detach takes the name out of the database manager, so nothing new
  // reaches this database. What is already in flight holds it, and closing it
  // under a statement takes the storage out from under an entry that statement
  // is reading -- so the close waits for the last reference, exactly as
  // duckdb's own DETACH does.
  duckdb::AttachedDatabase::InvokeCloseIfLastReference(
    attached, duckdb::DatabaseCloseAction::SKIP_CHECKPOINT);
}

void LoadDatabaseStorage(std::string_view name) {
  auto conn = DuckDBEngine::Instance().CreateConnection();
  auto& context = *conn->context;
  // Inside a transaction, exactly as the ATTACH statement that would otherwise
  // have run this: the load rebuilds the storage of every table it reads back,
  // and reaching an attachment at all goes through the meta transaction.
  bool repaired = false;
  duckdb::optional_ptr<duckdb::AttachedDatabase> database;
  conn->BeginTransaction();
  try {
    if (auto attached = duckdb::DatabaseManager::Get(context).GetDatabase(
          context, duckdb::Identifier{name})) {
      attached->InitializeStorage(context);
      attached->FinalizeLoad(context);
      database = attached.get();
      repaired = attached->GetCatalog()
                   .Cast<catalog::SereneDBCatalog>()
                   .FinishStorageReplay(context);
    }
  } catch (...) {
    conn->Rollback();
    throw;
  }
  conn->Commit();
  if (repaired) {
    // The rows the boot moved live nowhere but memory until the file is told:
    // the next one would read the shape this one repaired and replay the WAL of
    // a database that has since moved on from it.
    duckdb::CheckpointOptions options;
    options.action = duckdb::CheckpointAction::ALWAYS_CHECKPOINT;
    database->GetStorageManager().CreateCheckpoint(duckdb::QueryContext{},
                                                   options);
  }
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
}

}  // namespace sdb::connector
