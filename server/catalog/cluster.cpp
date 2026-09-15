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

#include "catalog/cluster.h"

#include <cstdlib>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <string_view>

#include "catalog/entry/database.h"
#include "catalog/entry/role.h"
#include "network/credentials.h"
#include "pg/pg_types.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kRootRole = "postgres";

void DeclareModified(duckdb::CatalogTransaction transaction,
                     duckdb::Catalog& catalog,
                     duckdb::DatabaseModificationType type =
                       duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY) {
  if (!transaction.context) {
    return;
  }
  duckdb::MetaTransaction::Get(transaction.GetContext())
    .ModifyDatabase(catalog.GetAttached(), type);
}

}  // namespace

ClusterCatalog::ClusterCatalog(duckdb::AttachedDatabase& db)
  : duckdb::DuckCatalog{db} {}

void ClusterCatalog::FinalizeLoad(
  duckdb::optional_ptr<duckdb::ClientContext> context) {
  duckdb::DuckCatalog::FinalizeLoad(context);
  if (!context) {
    return;
  }
  const auto transaction = GetCatalogTransaction(*context);
  const duckdb::Identifier root{kRootRole};
  if (!GetCatalogSet(duckdb::CatalogType::ROLE_ENTRY)
         .GetEntry(transaction, root)) {
    duckdb::CreateRoleInfo info;
    info.SetName(root);
    info.oid = pg::kRootUser;
    info.options = RoleOption::Superuser | RoleOption::Inherit |
                   RoleOption::CreateRole | RoleOption::CreateDb |
                   RoleOption::Login | RoleOption::Replication |
                   RoleOption::BypassRls;
    if (const char* password = std::getenv("POSTGRES_PASSWORD");
        password && *password) {
      auto verifier = network::BuildScramVerifierString(password);
      if (!verifier) {
        SDB_FATAL(GENERAL,
                  "could not derive a password verifier from "
                  "POSTGRES_PASSWORD");
      }
      info.password = std::move(*verifier);
      SDB_INFO(GENERAL, "bootstrap: initial password set for role '", kRootRole,
               "' from POSTGRES_PASSWORD");
    }
    CreateRole(transaction, info);
  }
  const duckdb::Identifier postgres{irs::StaticStrings::kDefaultDatabase};
  if (!GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
         .GetEntry(transaction, postgres)) {
    duckdb::CreateDatabaseInfo info;
    info.SetName(postgres);
    info.oid = pg::kPgPostgresDatabase;
    info.permissions.owner = pg::kRootUser;
    CreateDatabase(transaction, info);
  }
}

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterCatalog::CreateRole(
  duckdb::CatalogTransaction transaction, duckdb::CreateRoleInfo& info) {
  DeclareModified(transaction, *this);
  return duckdb::DuckCatalog::CreateRole(transaction, info);
}

void ClusterCatalog::DropRole(duckdb::CatalogTransaction transaction,
                              duckdb::DropInfo& info) {
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  duckdb::DuckCatalog::DropRole(transaction, info);
}

void ClusterCatalog::Alter(duckdb::CatalogTransaction transaction,
                           duckdb::AlterInfo& info) {
  DeclareModified(transaction, *this);
  const auto type = info.GetCatalogType();
  const auto& name = info.GetQualifiedName().Name();
  if (!GetCatalogSet(type).AlterEntry(transaction, name, info)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG(duckdb::CatalogTypeToString(type), " with name ",
                            name.GetIdentifierName(), " does not exist!"));
  }
}

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterCatalog::CreateDatabase(
  duckdb::CatalogTransaction transaction, duckdb::CreateDatabaseInfo& info) {
  DeclareModified(transaction, *this);
  return duckdb::DuckCatalog::CreateDatabase(transaction, info);
}

void ClusterCatalog::DropDatabase(duckdb::CatalogTransaction transaction,
                                  duckdb::DropInfo& info) {
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
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
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("the cluster catalog is not attached"));
  }
  return attached->GetCatalog().Cast<ClusterCatalog>();
}

ClusterCatalog& ClusterOf() {
  return ClusterOf(irs::DuckDBEngine::Instance().instance());
}

}  // namespace sdb::catalog
