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

#include "auth/visibility.h"

#include <absl/algorithm/container.h>

#include <array>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/function_entry.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <span>
#include <string_view>
#include <vector>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"

namespace sdb::auth {
namespace {

using duckdb::AclMode;
using duckdb::CatalogType;

std::vector<std::span<const duckdb::AclItem>> AllColumnAcls(
  const duckdb::TableCatalogEntry& table) {
  std::vector<std::span<const duckdb::AclItem>> acls;
  for (const auto& column : table.GetColumns().Logical()) {
    acls.emplace_back(column.Acl());
  }
  return acls;
}

duckdb::optional_ptr<duckdb::CatalogEntry> FindServer(
  duckdb::ClientContext& context, const duckdb::Identifier& name) {
  for (const auto& db :
       duckdb::DatabaseManager::Get(*context.db).GetDatabases()) {
    auto& catalog = db->GetCatalog();
    if (catalog.GetCatalogType() != catalog::SereneDBCatalog::kStorageType) {
      continue;
    }
    auto& duck_catalog = catalog.Cast<duckdb::DuckCatalog>();
    auto server =
      duck_catalog.GetCatalogSet(CatalogType::FOREIGN_SERVER_ENTRY)
        .GetEntry(duck_catalog.GetCatalogTransaction(context), name);
    if (server) {
      return server;
    }
  }
  return nullptr;
}

bool CanUseDatabase(duckdb::ClientContext& context,
                    const ConnectionContext& connection,
                    const duckdb::RoleAccess& closure, std::string_view name) {
  if (name == connection.GetDatabase()) {
    return true;
  }
  auto& cluster = catalog::ClusterOf(context);
  auto database = cluster.GetCatalogSet(CatalogType::DATABASE_ENTRY)
                    .GetEntry(cluster.GetCatalogTransaction(context),
                              duckdb::Identifier{name});
  return !database || closure.Can(CatalogType::DATABASE_ENTRY,
                                  database->permissions, AclMode::Connect);
}

bool DatabaseVisible(duckdb::ClientContext& context,
                     const ConnectionContext& connection,
                     const duckdb::RoleAccess& closure,
                     duckdb::AttachedDatabase& database) {
  const auto type = database.GetCatalog().GetCatalogType();
  if (type == catalog::SereneDBCatalog::kStorageType) {
    return CanUseDatabase(context, connection, closure,
                          database.GetName().GetIdentifierName());
  }
  if (type == catalog::ClusterCatalog::kStorageType || database.IsSystem() ||
      database.IsTemporary()) {
    return true;
  }
  auto server = FindServer(context, database.GetName());
  return server && closure.Can(CatalogType::FOREIGN_SERVER_ENTRY,
                               server->permissions, AclMode::Usage);
}

bool EntryVisible(duckdb::ClientContext& context,
                  const ConnectionContext& connection,
                  const duckdb::RoleAccess& closure,
                  duckdb::CatalogEntry& entry) {
  if (entry.type == CatalogType::DATABASE_ENTRY) {
    return CanUseDatabase(context, connection, closure,
                          entry.name.GetIdentifierName());
  }
  auto& parent = entry.ParentCatalog();
  if (parent.GetCatalogType() != catalog::ClusterCatalog::kStorageType &&
      !DatabaseVisible(context, connection, closure, parent.GetAttached())) {
    return false;
  }
  if ((entry.type == CatalogType::TABLE_FUNCTION_ENTRY ||
       entry.type == CatalogType::SCALAR_FUNCTION_ENTRY) &&
      entry.Cast<duckdb::FunctionEntry>().superuser_only) {
    return false;
  }
  if (entry.internal || duckdb::IsUnowned(entry)) {
    return true;
  }
  const auto& perm = entry.permissions;
  switch (entry.type) {
    case CatalogType::SCHEMA_ENTRY:
      return closure.CanAny(entry.type, perm, AclMode::Usage | AclMode::Create);
    case CatalogType::TABLE_ENTRY:
    case CatalogType::VIEW_ENTRY: {
      constexpr auto kAny = AclMode::Select | AclMode::Insert |
                            AclMode::Update | AclMode::Delete |
                            AclMode::Truncate | AclMode::References |
                            AclMode::Trigger | AclMode::Maintain;
      if (closure.CanAny(CatalogType::TABLE_ENTRY, perm, kAny)) {
        return true;
      }
      if (entry.type != CatalogType::TABLE_ENTRY) {
        return false;
      }
      const auto acls = AllColumnAcls(entry.Cast<duckdb::TableCatalogEntry>());
      return absl::c_any_of(
        std::array{AclMode::Select, AclMode::Insert, AclMode::Update,
                   AclMode::References},
        [&](AclMode mode) { return closure.CanAnyColumn(perm, mode, acls); });
    }
    case CatalogType::SEQUENCE_ENTRY:
      return closure.CanAny(entry.type, perm,
                            AclMode::Usage | AclMode::Select | AclMode::Update);
    case CatalogType::MACRO_ENTRY:
    case CatalogType::TABLE_MACRO_ENTRY:
      return closure.Can(entry.type, perm, AclMode::Execute);
    case CatalogType::TYPE_ENTRY:
      return closure.Can(entry.type, perm, AclMode::Usage);
    case CatalogType::JOB_ENTRY:
      return closure.Owns(perm.owner);
    case CatalogType::INDEX_ENTRY: {
      auto& index = entry.Cast<duckdb::IndexCatalogEntry>();
      auto relation =
        index.GetRelation(index.catalog.GetCatalogTransaction(context));
      return !relation || EntryVisible(context, connection, closure, *relation);
    }
    default:
      return true;
  }
}

void CheckReference(duckdb::ClientContext& context,
                    const ConnectionContext& connection,
                    const duckdb::RoleAccess& caller,
                    duckdb::CatalogEntry& entry) {
  if (caller.is_superuser) {
    return;
  }
  auto& database = entry.type == CatalogType::DATABASE_ENTRY
                     ? entry.Cast<duckdb::AttachedDatabase>()
                     : entry.ParentCatalog().GetAttached();
  if (!DatabaseVisible(context, connection, caller, database)) {
    if (database.GetCatalog().GetCatalogType() ==
        catalog::SereneDBCatalog::kStorageType) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                      ERR_MSG("permission denied for database \"",
                              database.GetName().GetIdentifierName(), "\""),
                      ERR_DETAIL("User does not have CONNECT privilege."));
    }
    if (const auto server = FindServer(context, database.GetName())) {
      duckdb::ThrowPermissionDenied(*server);
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied for database \"",
              database.GetName().GetIdentifierName(), "\""),
      ERR_DETAIL("Only roles with the SUPERUSER attribute may use "
                 "a database attached without a foreign server."));
  }
  if (entry.type != CatalogType::DATABASE_ENTRY &&
      !EntryVisible(context, connection, caller, entry)) {
    duckdb::ThrowPermissionDenied(entry);
  }
}

template<typename Fn>
auto WithCaller(duckdb::ClientContext& context,
                const ConnectionContext& connection, Fn&& fn) {
  if (const auto principal = context.principal) {
    return fn(*principal);
  }
  const auto closure = ClosureFor(&context, connection.GetRoleId());
  return fn(*closure);
}

}  // namespace

bool CanSee(duckdb::ClientContext& context, duckdb::CatalogEntry& entry) {
  const auto* connection = connector::GetSereneDBContextPtr(context);
  if (!connection) {
    return true;
  }
  return WithCaller(context, *connection,
                    [&](const duckdb::RoleAccess& caller) {
                      return caller.is_superuser ||
                             EntryVisible(context, *connection, caller, entry);
                    });
}

bool CanSeeDatabase(duckdb::ClientContext& context,
                    duckdb::AttachedDatabase& database) {
  const auto* connection = connector::GetSereneDBContextPtr(context);
  if (!connection) {
    return true;
  }
  return WithCaller(
    context, *connection, [&](const duckdb::RoleAccess& caller) {
      return caller.is_superuser ||
             DatabaseVisible(context, *connection, caller, database);
    });
}

void CheckEntryReference(duckdb::ClientContext& context,
                         duckdb::CatalogEntry& entry) {
  const auto* connection = connector::GetSereneDBContextPtr(context);
  if (!connection) {
    return;
  }
  WithCaller(context, *connection, [&](const duckdb::RoleAccess& caller) {
    CheckReference(context, *connection, caller, entry);
  });
}

}  // namespace sdb::auth
