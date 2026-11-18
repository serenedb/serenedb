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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <cstdlib>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/default/default_coordinate_systems.hpp>
#include <duckdb/catalog/default/default_functions.hpp>
#include <duckdb/catalog/default/default_table_functions.hpp>
#include <duckdb/catalog/default/default_types.hpp>
#include <duckdb/catalog/default/default_views.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/common/exception/catalog_exception.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/function/built_in_functions.hpp>
#include <duckdb/function/function_list.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/alter_info.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <duckdb/parser/parsed_data/create_database_info.hpp>
#include <duckdb/parser/parsed_data/create_role_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <iterator>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "auth/role_closure.h"
#include "catalog/boot.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/entry/replication_lsn.h"
#include "catalog/schema.h"
#include "pg/catalog/functions/pg_catalog/objectaddress.h"
#include "pg/catalog/oids.h"
#include "pg/catalog/utils/acl.h"
#include "pg/connection_context.h"
#include "query/config.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kRootRole = "postgres";

enum class RoleUse : uint8_t {
  None,
  Owner,
  Privileges,
};

bool Names(std::span<const duckdb::AclItem> acl, duckdb::idx_t role) {
  return absl::c_any_of(acl, [&](const duckdb::AclItem& item) {
    return item.grantee == role || item.grantor == role;
  });
}

RoleUse UseOf(const duckdb::CatalogEntry& entry, duckdb::idx_t role) {
  if (entry.permissions.owner == role) {
    return RoleUse::Owner;
  }
  if (Names(entry.permissions.acl, role)) {
    return RoleUse::Privileges;
  }
  if (entry.type == duckdb::CatalogType::TABLE_ENTRY &&
      absl::c_any_of(
        entry.Cast<duckdb::TableCatalogEntry>().GetColumns().Logical(),
        [&](const duckdb::ColumnDefinition& column) {
          return Names(column.Acl(), role);
        })) {
    return RoleUse::Privileges;
  }
  return RoleUse::None;
}

RoleUse UseOf(const duckdb::DefaultAcl& row, duckdb::idx_t role) {
  if (row.role == role) {
    return RoleUse::Owner;
  }
  return Names(row.acl, role) ? RoleUse::Privileges : RoleUse::None;
}

std::string Describe(RoleUse use, std::string_view object) {
  return absl::StrCat(use == RoleUse::Owner ? "owner of " : "privileges for ",
                      object);
}

std::string_view DefaultAclObjects(duckdb::CatalogType type) {
  switch (type) {
    case duckdb::CatalogType::SEQUENCE_ENTRY:
      return "sequences";
    case duckdb::CatalogType::MACRO_ENTRY:
      return "functions";
    case duckdb::CatalogType::TYPE_ENTRY:
      return "types";
    case duckdb::CatalogType::SCHEMA_ENTRY:
      return "schemas";
    default:
      return "relations";
  }
}

std::string DescribeDefaultAcl(SereneDBCatalog& database,
                               duckdb::CatalogTransaction transaction,
                               const auth::RoleGraph& roles,
                               const duckdb::DefaultAcl& row) {
  auto object =
    absl::StrCat("default privileges on new ", DefaultAclObjects(row.objtype),
                 " belonging to role ", roles.NameOf(row.role));
  if (row.scope != pg::kInvalidOid) {
    absl::StrAppend(
      &object, " in schema ",
      database.LookupOid(transaction, row.scope)->name.GetIdentifierName());
  }
  return object;
}

void RefuseDependents(duckdb::ClientContext& context, ClusterCatalog& cluster,
                      const duckdb::CatalogEntry& role) {
  std::vector<std::string> lines;
  std::vector<std::string> remote;
  const auto current = DefaultSereneDBCatalog(context);
  const auto roles = auth::RolesOf(&context);
  const auto cluster_transaction = cluster.GetCatalogTransaction(context);
  for (const auto& db :
       SereneDBDatabases(duckdb::DatabaseManager::Get(context))) {
    auto& database = db->GetCatalog().Cast<SereneDBCatalog>();
    const bool local = current.get() == &database;
    size_t count = 0;
    const auto visit = [&](duckdb::CatalogEntry& entry) {
      if (entry.type == duckdb::CatalogType::INDEX_ENTRY) {
        return;
      }
      const auto use = UseOf(entry, role.oid);
      if (use == RoleUse::None) {
        return;
      }
      ++count;
      if (local) {
        lines.push_back(Describe(use, pg::DescribeObject(context, entry)));
      }
    };
    const auto transaction = database.GetCatalogTransaction(context);
    database.Set(duckdb::CatalogType::SCHEMA_ENTRY)
      .Scan(transaction, [&](duckdb::CatalogEntry& schema) {
        visit(schema);
        schema.Cast<Schema>().ScanTree(transaction, visit);
      });
    database.Servers().Scan(transaction, visit);
    database.Subscriptions().Scan(transaction, visit);
    if (const auto entry = cluster.Set(duckdb::CatalogType::DATABASE_ENTRY)
                             .Get(cluster_transaction, db->GetName())) {
      pg::VisitDefaultAcls(
        context, *entry, [&](duckdb::idx_t, const duckdb::DefaultAcl& row) {
          const auto use = UseOf(row, role.oid);
          if (use == RoleUse::None) {
            return;
          }
          ++count;
          if (local) {
            lines.push_back(Describe(
              use, DescribeDefaultAcl(database, transaction, *roles, row)));
          }
        });
    }
    if (!local && count != 0) {
      remote.push_back(absl::StrCat(count, count == 1 ? " object" : " objects",
                                    " in database ",
                                    db->GetName().GetIdentifierName()));
    }
  }
  cluster.Set(duckdb::CatalogType::DATABASE_ENTRY)
    .Scan(cluster_transaction, [&](duckdb::CatalogEntry& entry) {
      if (const auto use = UseOf(entry, role.oid); use != RoleUse::None) {
        lines.push_back(Describe(
          use, absl::StrCat("database ", entry.name.GetIdentifierName())));
      }
    });
  absl::c_move(remote, std::back_inserter(lines));
  if (lines.empty()) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                  ERR_MSG("role \"", role.name.GetIdentifierName(),
                          "\" cannot be dropped because some objects depend "
                          "on it"),
                  ERR_DETAIL(absl::StrJoin(lines, "\n")));
}

void AssignObjectOid(duckdb::Catalog& catalog, duckdb::CreateInfo& info) {
  if (!info.oid) {
    info.oid = duckdb::DatabaseManager::Get(catalog.GetDatabase()).NextOid();
  }
}

void AttachDatabaseCatalog(duckdb::ClientContext& context,
                           const DatabaseCatalogEntry& database, bool durable) {
  auto& manager = duckdb::DatabaseManager::Get(context);
  if (auto attached = manager.GetDatabase(database.name)) {
    if (attached->oid == database.oid) {
      return;
    }
    manager.DetachDatabase(context, database.name,
                           duckdb::OnEntryNotFound::RETURN_NULL);
  }
  duckdb::AttachInfo info;
  info.name = database.name;
  duckdb::AttachOptions options{info.options, duckdb::AccessMode::READ_WRITE};
  options.create_catalog = [&](duckdb::AttachedDatabase& db) {
    db.oid = database.oid;
    return duckdb::InternalCatalog{
      .catalog =
        duckdb::make_uniq<SereneDBCatalog>(db, database.Directory(), durable),
      .transaction_manager = duckdb::make_uniq<TransactionManager>(db),
    };
  };
  manager.AttachDatabase(context, info, options);
}

}  // namespace

void ClusterCatalog::Initialize(bool load_builtin) {
  auto data = duckdb::CatalogTransaction::GetSystemTransaction(GetDatabase());
  for (const auto* name : {"information_schema", "pg_catalog"}) {
    duckdb::CreateSchemaInfo info;
    info.SetQualifiedName(
      duckdb::QualifiedName({duckdb::Identifier{name}}, duckdb::Identifier()));
    info.internal = true;
    info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
    CreateSchema(data, info);
  }
  ScanSchemas([&](Schema& schema) {
    schema.Set(duckdb::CatalogType::TABLE_ENTRY)
      .SetDefaults(
        duckdb::make_uniq<duckdb::DefaultViewGenerator>(*this, schema));
    schema.Set(duckdb::CatalogType::TABLE_FUNCTION_ENTRY)
      .SetDefaults(duckdb::make_uniq<duckdb::DefaultTableFunctionGenerator>(
        *this, schema));
    schema.Set(duckdb::CatalogType::SCALAR_FUNCTION_ENTRY)
      .SetDefaults(
        duckdb::make_uniq<duckdb::DefaultFunctionGenerator>(*this, schema));
    schema.Set(duckdb::CatalogType::COORDINATE_SYSTEM_ENTRY)
      .SetDefaults(duckdb::make_uniq<duckdb::DefaultCoordinateSystemGenerator>(
        *this, schema));
    schema.Set(duckdb::CatalogType::TYPE_ENTRY)
      .SetDefaults(
        duckdb::make_uniq<duckdb::DefaultTypeGenerator>(*this, schema));
  });
  if (!load_builtin) {
    return;
  }
  duckdb::BuiltinFunctions builtin{data, *this};
  builtin.Initialize();
  duckdb::FunctionList::RegisterFunctions(*this, data);
}

EntrySet& ClusterCatalog::Set(duckdb::CatalogType type) {
  switch (type) {
    case duckdb::CatalogType::ROLE_ENTRY:
      return _roles;
    case duckdb::CatalogType::DATABASE_ENTRY:
      return _databases;
    case duckdb::CatalogType::REPLICATION_ORIGIN_ENTRY:
      return _origins;
    default:
      return BaseCatalog::Set(type);
  }
}

void ClusterCatalog::OnCommit() { auth::PublishRoles(*this); }

void ClusterCatalog::Bootstrap(duckdb::ClientContext& context,
                               bool default_database) {
  const auto transaction = GetCatalogTransaction(context);
  const duckdb::Identifier root{kRootRole};
  if (!_roles.Get(transaction, root)) {
    duckdb::CreateRoleInfo info;
    info.SetName(root);
    info.oid = pg::kRootUser;
    info.options = RoleOption::Superuser | RoleOption::Inherit |
                   RoleOption::CreateRole | RoleOption::CreateDb |
                   RoleOption::Login | RoleOption::Replication |
                   RoleOption::BypassRls;
    if (const char* password = std::getenv("POSTGRES_PASSWORD");
        password && *password) {
      info.password = StoredPassword(password);
      SDB_INFO(STARTUP, "bootstrap: initial password set for role '", kRootRole,
               "' from POSTGRES_PASSWORD");
    }
    DeclareModified(transaction, *this);
    _roles.Create(transaction,
                  duckdb::make_uniq<RoleCatalogEntry>(*this, info));
  }
  const duckdb::Identifier postgres{irs::StaticStrings::kDefaultDatabase};
  if (default_database && !_databases.Get(transaction, postgres)) {
    duckdb::CreateDatabaseInfo info;
    info.SetName(postgres);
    info.oid = pg::kPgPostgresDatabase;
    info.permissions.owner = pg::kRootUser;
    CreateDatabase(transaction, info);
  }
}

void ClusterCatalog::Alter(duckdb::CatalogTransaction transaction,
                           duckdb::AlterInfo& info) {
  const auto type = info.GetCatalogType();
  if (type != duckdb::CatalogType::ROLE_ENTRY &&
      type != duckdb::CatalogType::DATABASE_ENTRY) {
    BaseCatalog::Alter(transaction, info);
    return;
  }
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::ALTER_TABLE);
  const auto& name = info.GetQualifiedName().Name();
  auto& set = Set(type);
  auto entry = set.Get(transaction, name);
  if (!entry) {
    const bool database = type == duckdb::CatalogType::DATABASE_ENTRY;
    THROW_SQL_ERROR(ERR_CODE(database ? ERRCODE_UNDEFINED_DATABASE
                                      : ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG(database ? "database \"" : "role \"",
                            name.GetIdentifierName(), "\" does not exist"));
  }
  catalog::AlterEntry(transaction, set, *entry, info);
}

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterCatalog::CreateRole(
  duckdb::CatalogTransaction transaction, duckdb::CreateRoleInfo& info) {
  DeclareModified(transaction, *this);
  RequireUnreservedRoleName(info.GetQualifiedName().Name());
  AssignObjectOid(*this, info);
  if (!info.password.empty()) {
    info.password = StoredPassword(info.password);
  }
  auto entry = duckdb::make_uniq<RoleCatalogEntry>(*this, info);
  duckdb::optional_ptr<duckdb::CatalogEntry> role = entry.get();
  if (!_roles.Create(transaction, std::move(entry))) {
    if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
      return nullptr;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
      ERR_MSG("role \"", info.GetQualifiedName().Name().GetIdentifierName(),
              "\" already exists"));
  }
  const auto grant = [&](const duckdb::Identifier& member,
                         const duckdb::Identifier& granted, bool admin) {
    duckdb::AlterRoleInfo alter{member};
    alter.grant_role = granted.GetIdentifierName();
    alter.admin_option = admin;
    Alter(transaction, alter);
  };
  for (const auto& name : info.in_roles) {
    grant(role->name, name, false);
  }
  for (const auto& name : info.role_members) {
    grant(name, role->name, false);
  }
  for (const auto& name : info.admin_members) {
    grant(name, role->name, true);
  }
  auto& context = transaction.GetContext();
  if (const auto creator = GrantorOfMembership(context);
      creator != pg::kRootUser) {
    duckdb::AlterRoleInfo alter{
      duckdb::Identifier{auth::RolesOf(&context)->NameOf(creator)}};
    alter.grant_role = role->name.GetIdentifierName();
    alter.grantor_id = pg::kRootUser;
    alter.admin_option = true;
    alter.inherit_option = false;
    alter.set_option = false;
    Alter(transaction, alter);
  }
  return role;
}

void ClusterCatalog::DropRole(duckdb::CatalogTransaction transaction,
                              duckdb::DropInfo& info) {
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  const auto& name = info.GetQualifiedName().Name();
  auto role = _roles.Get(transaction, name);
  if (!role) {
    if (info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL) {
      return;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("role \"", name.GetIdentifierName(), "\" does not exist"));
  }
  const auto& session = ConnectionContext::Get(transaction.GetContext());
  if (role->oid == session.GetRoleId() ||
      role->oid == session.GetSessionRoleId() ||
      role->oid == session.GetLoginRoleId()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                    ERR_MSG("current user cannot be dropped"));
  }
  if (role->oid == pg::kRootUser) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_IN_USE),
      ERR_MSG("cannot drop role \"", role->name.GetIdentifierName(),
              "\" because it is required by the database system"));
  }
  RefuseDependents(transaction.GetContext(), *this, *role);
  std::vector<duckdb::Identifier> members;
  _roles.Scan(transaction, [&](duckdb::CatalogEntry& other) {
    if (absl::c_any_of(other.Cast<RoleCatalogEntry>().MemberOf(),
                       [&](const duckdb::Membership& membership) {
                         return membership.role == role->oid;
                       })) {
      members.emplace_back(other.name);
    }
  });
  for (const auto& member : members) {
    duckdb::AlterRoleInfo alter{member};
    alter.grant_role_id = role->oid;
    alter.revoke = true;
    Alter(transaction, alter);
  }
  _roles.Drop(transaction, name);
}

duckdb::optional_ptr<duckdb::CatalogEntry> ClusterCatalog::CreateDatabase(
  duckdb::CatalogTransaction transaction, duckdb::CreateDatabaseInfo& info) {
  for (auto& [name, value] : info.options) {
    value = connector::ValidateSetting(transaction.GetContext(), name, value);
  }
  DeclareModified(transaction, *this);
  AssignObjectOid(*this, info);
  auto entry = duckdb::make_uniq<DatabaseCatalogEntry>(*this, info);
  auto& database = *entry;
  const auto attached = duckdb::DatabaseManager::Get(transaction.GetContext())
                          .GetDatabase(info.GetQualifiedName().Name());
  if ((attached && !IsSereneDB(attached->GetCatalog())) ||
      !_databases.Create(transaction, std::move(entry))) {
    if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
      return nullptr;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DUPLICATE_DATABASE),
      ERR_MSG("database \"", info.GetQualifiedName().Name().GetIdentifierName(),
              "\" already exists"));
  }
  SDB_IF_FAILURE("unable_to_create") {
    THROW_SQL_ERROR(ERR_MSG("internal error"));
  }
  AttachDatabaseCatalog(transaction.GetContext(), database,
                        !_layout.InMemory());
  return &database;
}

void ClusterCatalog::DropDatabase(duckdb::CatalogTransaction transaction,
                                  duckdb::DropInfo& info) {
  if (auto entry =
        _databases.Get(transaction, info.GetQualifiedName().Name())) {
    DropDependents(transaction, *entry, info.cascade);
  }
  if (!DropFrom(_databases, transaction, duckdb::CatalogType::DATABASE_ENTRY,
                info)) {
    return;
  }
  auto& context = transaction.GetContext();
  duckdb::DatabaseManager::Get(context).DetachDatabase(
    context, info.GetQualifiedName().Name(),
    duckdb::OnEntryNotFound::RETURN_NULL);
}

duckdb::optional_ptr<duckdb::CatalogEntry>
ClusterCatalog::CreateReplicationOrigin(
  duckdb::CatalogTransaction transaction,
  duckdb::CreateReplicationOriginInfo& info) {
  AssignObjectOid(*this, info);
  return AddTo(_origins, transaction,
               duckdb::make_uniq<ReplicationOriginEntry>(*this, info),
               info.on_conflict);
}

void ClusterCatalog::DropReplicationOrigin(
  duckdb::CatalogTransaction transaction, duckdb::DropInfo& info) {
  DropFrom(_origins, transaction, duckdb::CatalogType::REPLICATION_ORIGIN_ENTRY,
           info);
}

void ClusterCatalog::ReplayCreate(duckdb::ClientContext& context,
                                  duckdb::unique_ptr<duckdb::CreateInfo> info) {
  const auto transaction = GetCatalogTransaction(context);
  DeclareModified(transaction, *this);
  switch (info->type) {
    case duckdb::CatalogType::ROLE_ENTRY:
      _roles.Create(transaction,
                    duckdb::make_uniq<RoleCatalogEntry>(
                      *this, info->Cast<duckdb::CreateRoleInfo>()));
      return;
    case duckdb::CatalogType::DATABASE_ENTRY: {
      auto entry = duckdb::make_uniq<DatabaseCatalogEntry>(
        *this, info->Cast<duckdb::CreateDatabaseInfo>());
      auto& database = *entry;
      _databases.Create(transaction, std::move(entry));
      AttachDatabaseCatalog(context, database, !_layout.InMemory());
      return;
    }
    case duckdb::CatalogType::REPLICATION_ORIGIN_ENTRY:
      CreateReplicationOrigin(
        transaction, info->Cast<duckdb::CreateReplicationOriginInfo>());
      return;
    default:
      BaseCatalog::ReplayCreate(context, std::move(info));
  }
}

void ClusterCatalog::ReplayDrop(duckdb::ClientContext& context,
                                duckdb::CatalogEntry& entry) {
  const auto type = entry.type;
  if (type != duckdb::CatalogType::ROLE_ENTRY &&
      type != duckdb::CatalogType::DATABASE_ENTRY &&
      type != duckdb::CatalogType::REPLICATION_ORIGIN_ENTRY) {
    BaseCatalog::ReplayDrop(context, entry);
    return;
  }
  const auto transaction = GetCatalogTransaction(context);
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  const auto name = entry.name;
  Set(type).Drop(transaction, name);
  if (type == duckdb::CatalogType::DATABASE_ENTRY) {
    duckdb::DatabaseManager::Get(context).DetachDatabase(
      context, name, duckdb::OnEntryNotFound::RETURN_NULL);
  }
}

ClusterCatalog& ClusterOf(duckdb::ClientContext& context) {
  return duckdb::Catalog::GetSystemCatalog(context).Cast<ClusterCatalog>();
}

ClusterCatalog& ClusterOf(duckdb::DatabaseInstance& db) {
  return duckdb::Catalog::GetSystemCatalog(db).Cast<ClusterCatalog>();
}

ClusterCatalog& ClusterOf() {
  return ClusterOf(irs::DuckDBEngine::Instance().instance());
}

}  // namespace sdb::catalog
