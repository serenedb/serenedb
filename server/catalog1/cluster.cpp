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

#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <string_view>

#include "basics/duckdb_engine.h"
#include "catalog1/entry/database.h"
#include "catalog1/entry/role.h"
#include "pg/pg_types.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kRootRole = "postgres";

void DeclareModified(duckdb::CatalogTransaction transaction,
                     duckdb::Catalog& catalog) {
  if (!transaction.context) {
    return;
  }
  duckdb::MetaTransaction::Get(transaction.GetContext())
    .ModifyDatabase(catalog.GetAttached(),
                    duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY);
}

}  // namespace

ClusterCatalog::ClusterCatalog(duckdb::AttachedDatabase& db)
  : duckdb::DuckCatalog{db} {}

duckdb::unique_ptr<duckdb::InCatalogEntry> ClusterCatalog::MakeRoleEntry(
  duckdb::CreateRoleInfo& info) {
  return duckdb::make_uniq<RoleCatalogEntry>(*this, info);
}

duckdb::unique_ptr<duckdb::InCatalogEntry> ClusterCatalog::MakeDatabaseEntry(
  duckdb::CreateDatabaseInfo& info) {
  return duckdb::make_uniq<DatabaseCatalogEntry>(*this, info);
}

void ClusterCatalog::FinalizeLoad(
  duckdb::optional_ptr<duckdb::ClientContext> context) {
  duckdb::DuckCatalog::FinalizeLoad(context);
  if (!context) {
    return;
  }
  const auto transaction = GetCatalogTransaction(*context);
  const duckdb::Identifier root{kRootRole};
  if (GetCatalogSet(duckdb::CatalogType::ROLE_ENTRY)
        .GetEntry(transaction, root)) {
    return;
  }
  duckdb::CreateRoleInfo info;
  info.SetName(root);
  info.oid = pg::kRootUser;
  info.options = RoleOption::Superuser | RoleOption::Inherit |
                 RoleOption::CreateRole | RoleOption::CreateDb |
                 RoleOption::Login | RoleOption::Replication |
                 RoleOption::BypassRls;
  CreateRole(transaction, info);
}

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterCatalog::CreateRole(
  duckdb::CatalogTransaction transaction, duckdb::CreateRoleInfo& info) {
  DeclareModified(transaction, *this);
  return duckdb::DuckCatalog::CreateRole(transaction, info);
}

void ClusterCatalog::DropRole(duckdb::CatalogTransaction transaction,
                              duckdb::DropInfo& info) {
  DeclareModified(transaction, *this);
  duckdb::DuckCatalog::DropRole(transaction, info);
}

void ClusterCatalog::Alter(duckdb::CatalogTransaction transaction,
                           duckdb::AlterInfo& info) {
  DeclareModified(transaction, *this);
  const auto type = info.GetCatalogType();
  const auto& name = info.GetQualifiedName().Name();
  if (!GetCatalogSet(type).AlterEntry(transaction, name, info)) {
    throw duckdb::CatalogException::MissingEntry(type, name, std::string{});
  }
}

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterCatalog::CreateDatabase(
  duckdb::CatalogTransaction transaction, duckdb::CreateDatabaseInfo& info) {
  DeclareModified(transaction, *this);
  return duckdb::DuckCatalog::CreateDatabase(transaction, info);
}

void ClusterCatalog::DropDatabase(duckdb::CatalogTransaction transaction,
                                  duckdb::DropInfo& info) {
  DeclareModified(transaction, *this);
  duckdb::DuckCatalog::DropDatabase(transaction, info);
}

ClusterCatalog& ClusterOf(duckdb::ClientContext& context) {
  const duckdb::Identifier name{ClusterCatalog::kDatabaseName};
  return duckdb::Catalog::GetCatalog(context, name).Cast<ClusterCatalog>();
}

// Not Catalog::GetCatalog(DatabaseInstance&, name): the pin declares that
// overload and never defines it. This is what it would have done.
ClusterCatalog& ClusterOf(duckdb::DatabaseInstance& db) {
  const duckdb::Identifier name{ClusterCatalog::kDatabaseName};
  auto attached = duckdb::DatabaseManager::Get(db).GetDatabase(name);
  if (!attached) {
    throw duckdb::InternalException("the cluster catalog is not attached");
  }
  return attached->GetCatalog().Cast<ClusterCatalog>();
}

ClusterCatalog& ClusterOf() {
  return ClusterOf(DuckDBEngine::Instance().instance());
}

}  // namespace sdb::catalog
