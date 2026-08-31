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

#include "catalog1/cluster.h"

#include <duckdb/common/exception.hpp>
#include <duckdb/main/attached_database.hpp>
#include <utility>

#include "basics/duckdb_engine.h"
#include "catalog1/entry/database.h"
#include "catalog1/entry/role.h"

namespace sdb::catalog {
namespace {

template<typename Entry, typename Info>
duckdb::optional_ptr<duckdb::CatalogEntry> CreateClusterEntry(
  duckdb::Catalog& catalog, duckdb::CatalogSet& set,
  duckdb::CatalogTransaction transaction, Info& info) {
  const auto& entry_name = info.GetQualifiedName().Name();
  if (info.on_conflict != duckdb::OnCreateConflict::ERROR_ON_CONFLICT) {
    const auto existing = set.GetEntry(transaction, entry_name);
    if (existing) {
      if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
        return nullptr;
      }
      set.DropEntry(transaction, entry_name, false);
    }
  }
  auto entry = duckdb::make_uniq<Entry>(catalog, info);
  auto result = entry.get();
  if (!set.CreateEntry(transaction, entry_name, std::move(entry),
                       info.dependencies)) {
    throw duckdb::CatalogException::EntryAlreadyExists(Entry::Type, entry_name);
  }
  return result;
}

}  // namespace

ClusterCatalog::ClusterCatalog(duckdb::AttachedDatabase& db)
  : duckdb::DuckCatalog{db}, _roles{*this}, _databases{*this} {}

void ClusterCatalog::Initialize(bool load_builtin) {
  duckdb::DuckCatalog::Initialize(load_builtin);
}

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterCatalog::CreateRole(
  duckdb::CatalogTransaction transaction, CreateRoleInfo& info) {
  return CreateClusterEntry<RoleCatalogEntry>(*this, _roles, transaction, info);
}

bool ClusterCatalog::DropRole(duckdb::CatalogTransaction transaction,
                              const duckdb::Identifier& name, bool cascade) {
  return _roles.DropEntry(transaction, name, cascade);
}

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterCatalog::LookupRole(
  duckdb::CatalogTransaction transaction, const duckdb::Identifier& name) {
  return _roles.GetEntry(transaction, name);
}

void ClusterCatalog::ScanRoles(
  duckdb::CatalogTransaction transaction,
  const std::function<void(duckdb::CatalogEntry&)>& callback) {
  _roles.Scan(transaction, callback);
}

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterCatalog::CreateDatabase(
  duckdb::CatalogTransaction transaction, CreateDatabaseInfo& info) {
  return CreateClusterEntry<DatabaseCatalogEntry>(*this, _databases,
                                                  transaction, info);
}

bool ClusterCatalog::DropDatabase(duckdb::CatalogTransaction transaction,
                                  const duckdb::Identifier& name,
                                  bool cascade) {
  return _databases.DropEntry(transaction, name, cascade);
}

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterCatalog::LookupDatabase(
  duckdb::CatalogTransaction transaction, const duckdb::Identifier& name) {
  return _databases.GetEntry(transaction, name);
}

void ClusterCatalog::ScanDatabases(
  duckdb::CatalogTransaction transaction,
  const std::function<void(duckdb::CatalogEntry&)>& callback) {
  _databases.Scan(transaction, callback);
}

ClusterCatalog& ClusterOf(duckdb::ClientContext& context) {
  const duckdb::Identifier name{ClusterCatalog::kDatabaseName};
  return duckdb::Catalog::GetCatalog(context, name).Cast<ClusterCatalog>();
}

ClusterCatalog& ClusterOf(duckdb::DatabaseInstance& db) {
  const duckdb::Identifier name{ClusterCatalog::kDatabaseName};
  return duckdb::Catalog::GetCatalog(db, name).Cast<ClusterCatalog>();
}

ClusterCatalog& ClusterOf() {
  return ClusterOf(DuckDBEngine::Instance().instance());
}

}  // namespace sdb::catalog
