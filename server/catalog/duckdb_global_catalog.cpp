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

#include "catalog/duckdb_global_catalog.h"

#include <absl/strings/str_cat.h>

#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <duckdb/storage/storage_extension.hpp>
#include <duckdb/transaction/meta_transaction.hpp>

#include "basics/duckdb_engine.h"
#include "basics/log.h"
#include "catalog/duckdb_dependency.h"
#include "catalog/duckdb_schema_entry.h"

namespace sdb::catalog {
namespace {

duckdb::unique_ptr<duckdb::Catalog> AttachGlobal(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::ClientContext& context, duckdb::AttachedDatabase& db,
  const duckdb::string& name, duckdb::AttachInfo& info,
  duckdb::AttachOptions& options) {
  // AttachedDatabase reads info.path after this returns and builds a
  // SingleFileStorageManager from it. Empty means IN_MEMORY_PATH, which is what
  // makes this attachment storage-less: no data file, no data WAL, and
  // storage_options.Initialize is skipped so no storage version applies.
  info.path.clear();
  return duckdb::make_uniq<SereneDBGlobalCatalog>(db);
}

duckdb::unique_ptr<duckdb::TransactionManager> CreateGlobalTransactionManager(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::AttachedDatabase& db, duckdb::Catalog& catalog) {
  return duckdb::make_uniq<SereneDBGlobalTransactionManager>(db);
}

class SereneDBGlobalStorageExtension final : public duckdb::StorageExtension {
 public:
  SereneDBGlobalStorageExtension() {
    attach = AttachGlobal;
    create_transaction_manager = CreateGlobalTransactionManager;
  }
};

}  // namespace

SereneDBGlobalCatalog::SereneDBGlobalCatalog(duckdb::AttachedDatabase& db)
  : duckdb::DuckCatalog{db},
    // Case-sensitive for the same reason the schema sets are: serenedb folds an
    // unquoted identifier at parse time and then matches exactly.
    _roles{*this, nullptr, /*case_sensitive=*/true},
    _databases{*this, nullptr, /*case_sensitive=*/true} {}

duckdb::optional_ptr<duckdb::CatalogSet>
SereneDBGlobalCatalog::TryGetCatalogSet(duckdb::CatalogType type) {
  switch (type) {
    case duckdb::CatalogType::ROLE_ENTRY:
      return &_roles;
    case duckdb::CatalogType::DATABASE_ENTRY:
      return &_databases;
    default:
      return nullptr;
  }
}

namespace {

duckdb::optional_ptr<SereneDBGlobalCatalog> AsGlobalCatalog(
  duckdb::optional_ptr<duckdb::AttachedDatabase> db) {
  if (!db) {
    return nullptr;
  }
  auto& catalog = db->GetCatalog();
  if (catalog.GetCatalogType() != kGlobalStorageType) {
    return nullptr;
  }
  return &catalog.Cast<SereneDBGlobalCatalog>();
}

}  // namespace

duckdb::CatalogEntryInfo SereneDBGlobalCatalog::GetDependencyInfo(
  const duckdb::CatalogEntry& entry) const {
  // A role and a database are the only kinds this catalog holds, and both
  // are addressed by their stable id. Anything else -- duckdb's own
  // dependency entries -- keeps duckdb's name-keyed address.
  return IsHostedEntry(entry) ? DependencyInfo(catalog::IdOf(entry))
                              : duckdb::Catalog::GetDependencyInfo(entry);
}

duckdb::optional_ptr<duckdb::CatalogEntry>
SereneDBGlobalCatalog::GetDependencyEntry(
  duckdb::CatalogTransaction transaction,
  const duckdb::CatalogEntryInfo& info) {
  const auto id = DependencyInfoId(info);
  if (!id.isSet()) {
    return duckdb::Catalog::GetDependencyEntry(transaction, info);
  }
  // Roles and databases are few and hang off the catalog rather than a schema,
  // so identity is matched on the scan rather than through an id index.
  duckdb::optional_ptr<duckdb::CatalogEntry> found;
  const auto search = [&](duckdb::CatalogSet& set) {
    set.Scan(transaction, [&](duckdb::CatalogEntry& entry) {
      if (!found && entry.oid == id.id()) {
        found = &entry;
      }
    });
  };
  search(_roles);
  if (!found) {
    search(_databases);
  }
  return found;
}

duckdb::optional_ptr<SereneDBGlobalCatalog> TryGlobalCatalog(
  duckdb::ClientContext& context) {
  return AsGlobalCatalog(duckdb::DatabaseManager::Get(context).GetDatabase(
    context, duckdb::Identifier{kGlobalDatabaseName}));
}

duckdb::optional_ptr<SereneDBGlobalCatalog> TryGlobalCatalog() {
  auto db = duckdb::DatabaseManager::Get(DuckDBEngine::Instance().instance())
              .GetDatabase(duckdb::Identifier{kGlobalDatabaseName});
  return AsGlobalCatalog(db.get());
}

void RegisterSereneDBGlobalStorage(duckdb::DBConfig& config) {
  auto ext = duckdb::make_shared_ptr<SereneDBGlobalStorageExtension>();
  duckdb::StorageExtension::Register(config, std::string{kGlobalStorageType},
                                     std::move(ext));
}

void AttachGlobalDatabase() {
  auto conn = DuckDBEngine::Instance().CreateConnection();
  // HIDDEN keeps it out of duckdb_databases() and out of the unqualified
  // lookup fallback, the same way the data store is hidden: no user statement
  // names it.
  auto result = conn->Query(absl::StrCat("ATTACH '' AS \"", kGlobalDatabaseName,
                                         "\" (TYPE ", kGlobalStorageType,
                                         ", HIDDEN true)"));
  if (result->HasError()) {
    SDB_FATAL(STARTUP, "failed to attach the cluster-global database: ",
              result->GetError());
  }
}

void ModifyGlobalDatabase(duckdb::ClientContext& context,
                          duckdb::DatabaseModificationType modification) {
  if (!context.transaction.HasActiveTransaction()) {
    return;
  }
  auto db = duckdb::DatabaseManager::Get(context).GetDatabase(
    context, duckdb::Identifier{kGlobalDatabaseName});
  if (!db) {
    return;
  }
  duckdb::MetaTransaction::Get(context).ModifyDatabase(*db, modification);
}

}  // namespace sdb::catalog
