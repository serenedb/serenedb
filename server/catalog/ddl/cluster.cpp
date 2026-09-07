////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <absl/functional/function_ref.h>
#include <absl/strings/str_cat.h>
#include <absl/synchronization/mutex.h>

#include <algorithm>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/exception.hpp>
#include <filesystem>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "auth/acl.h"
#include "auth/role_closure.h"
#include "basics/debugging.h"
#include "basics/log.h"
#include "basics/static_strings.h"
#include "catalog/database.h"
#include "catalog/ddl/catalog.h"
#include "catalog/entry.h"
#include "catalog/entry/duckdb_index_entry.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/log/data_store.h"
#include "catalog/log/duckdb_global_catalog.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/role.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::catalog {
namespace {

// Roles and databases hang off the instance: their writes are attributed to
// the storage-less cluster-global attachment, not the statement's database.
// Called ahead of the mutation's own work, like JoinStoreTransaction: it opens
// the transaction. Context-less callers (boot, replay, background drops) have
// none to join.
void JoinClusterGlobal(duckdb::ClientContext* context,
                       duckdb::DatabaseModificationType modification) {
  if (context != nullptr) {
    catalog::ModifyGlobalDatabase(*context, modification);
  }
}

void RequireDatabaseOwner(duckdb::ClientContext* context, ObjectId role,
                          const catalog::SereneDBDatabaseEntry* database) {
  if (database == nullptr || auth::ClosureFor(context, role)
                               ->Owns(ObjectId{database->permissions.owner})) {
    return;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ERR_MSG("must be owner of database ", database->name.GetIdentifierName()));
}

void RequireRoleMembership(duckdb::ClientContext* context, ObjectId actor_id,
                           const SereneDBRoleEntry& target) {
  if (auth::ClosureFor(context, actor_id)->MemberOf(target.GetId())) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied"),
                  ERR_DETAIL("Must be a member of role \"", target.GetName(),
                             "\" to alter its default privileges."));
}

void RequireRoleAdmin(duckdb::ClientContext* context, ObjectId actor_id,
                      const SereneDBRoleEntry& target, std::string_view verb) {
  auto actor = catalog::FindRole(context, actor_id);
  if (actor && actor->IsSuperuser()) {
    return;
  }
  if (target.IsSuperuser()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied to ", verb, " role"),
                    ERR_DETAIL("Only roles with the SUPERUSER attribute may ",
                               verb, " roles with the SUPERUSER attribute."));
  }
  if (!actor || !actor->Has(RoleOption::CreateRole) ||
      !auth::HasAdminOption(*auth::RolesOf(context), actor_id,
                            target.GetId())) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to ", verb, " role"),
      ERR_DETAIL("Only roles with the CREATEROLE attribute and the ADMIN "
                 "option on role \"",
                 target.GetName(), "\" may ", verb, " this role."));
  }
}

void RequireRoleAttribute(duckdb::ClientContext* context, ObjectId actor_id,
                          RoleOption attribute, std::string_view denied_action,
                          std::string_view detail) {
  if (actor_id == id::kRootUser) {
    return;
  }
  auto actor = catalog::FindRole(context, actor_id);
  if (!actor || actor->IsSuperuser() || actor->Has(attribute)) {
    return;
  }
  if (detail.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied to ", denied_action));
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied to ", denied_action),
                  ERR_DETAIL(detail));
}

// A role may only confer a privileged attribute it holds itself (superuser for
// SUPERUSER; the matching bit for CREATEDB/REPLICATION/BYPASSRLS). `granting`
// is the set of attributes being conferred; CREATEROLE/LOGIN/INHERIT are not
// gated (a CREATEROLE actor may set them, matching PostgreSQL).
void RequireAttributesGrantable(duckdb::ClientContext* context,
                                ObjectId actor_id, RoleOption granting,
                                bool creating) {
  if (actor_id == id::kRootUser) {
    return;
  }
  auto actor = catalog::FindRole(context, actor_id);
  const bool actor_super = actor && actor->IsSuperuser();

  const auto require = [&](RoleOption attr, bool actor_has,
                           std::string_view attr_name) {
    if ((granting & attr) == RoleOption::None || actor_has) {
      return;
    }
    const auto detail =
      creating
        ? absl::StrCat("Only roles with the ", attr_name,
                       " attribute may create roles with the ", attr_name,
                       " attribute.")
        : absl::StrCat("Only roles with the ", attr_name,
                       " attribute may change the ", attr_name, " attribute.");
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to ", creating ? "create" : "alter", " role"),
      ERR_DETAIL(detail));
  };

  require(RoleOption::Superuser, actor_super, "SUPERUSER");
  require(RoleOption::CreateDb,
          actor_super || (actor && actor->Has(RoleOption::CreateDb)),
          "CREATEDB");
  require(RoleOption::Replication,
          actor_super || (actor && actor->Has(RoleOption::Replication)),
          "REPLICATION");
  require(RoleOption::BypassRls,
          actor_super || (actor && actor->Has(RoleOption::BypassRls)),
          "BYPASSRLS");
}

}  // namespace

std::pair<ObjectId, Permissions> Catalog::CreateDatabase(
  const AccessContext& ax, duckdb::unique_ptr<CreateDatabaseInfo> database,
  ObjectId owner, bool if_not_exists) {
  JoinClusterGlobal(ax.context,
                    duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY);
  RequireRoleAttribute(ax.context, ax.role, RoleOption::CreateDb,
                       "create database", {});
  if (catalog::FindDatabase(ax.context, database->GetName())) {
    if (if_not_exists) {
      return {};
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DUPLICATE_DATABASE),
      ERR_MSG("database \"", database->GetName(), "\" already exists"));
  }
  if (!database->GetId().isSet()) {
    database->SetId(NextId());
  }
  SDB_IF_FAILURE("unable_to_create") {
    THROW_SQL_ERROR(ERR_MSG("internal error"));
  }
  Permissions perm{owner};
  // The public schema is made when the catalog is opened, the way duckdb makes
  // its own default schema -- so what has to be durable is its id, and the
  // database states it. That keeps CREATE DATABASE a write to one database.
  const auto public_schema_id = NextId();
  database->SetPublicSchemaId(public_schema_id);
  catalog::PutDatabase(ax.context, {}, std::move(database), perm);
  return {public_schema_id, perm};
}

void Catalog::CreateRole(const AccessContext& ax,
                         duckdb::unique_ptr<CreateRoleInfo> role) {
  SDB_DEBUG(GENERAL, "Creating role: ", role->GetName());
  JoinClusterGlobal(ax.context,
                    duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY);
  RequireRoleAttribute(
    ax.context, ax.role, RoleOption::CreateRole, "create role",
    "Only roles with the CREATEROLE attribute may create roles.");
  RequireAttributesGrantable(ax.context, ax.role, role->Options(),
                             /*creating=*/true);
  if (catalog::FindRole(ax.context, role->GetName())) {
    ThrowDuplicateName(NameKind::Role, role->GetName());
  }
  if (!role->GetId().isSet()) {
    role->SetId(NextId());
  }
  duckdb::unique_ptr<CreateRoleInfo> updated;
  if (auto creator = catalog::FindRole(ax.context, ax.role);
      creator && !creator->IsSuperuser()) {
    updated = creator->Record();
    updated->AddMembership(Membership{
      .role = role->GetId(),
      .admin_option = true,
      .inherit_option = false,
      .set_option = false,
    });
  }
  catalog::PutRole(ax.context, {}, std::move(role));
  if (updated) {
    const auto name = std::string{updated->GetName()};
    catalog::PutRole(ax.context, name, std::move(updated));
  }
}

void Catalog::ChangeRoleImpl(
  duckdb::ClientContext* context, ObjectId actor_id, std::string_view name,
  absl::FunctionRef<void(duckdb::ClientContext*, const SereneDBRoleEntry&)>
    check,
  ChangeCallback<SereneDBRoleEntry, CreateRoleInfo> callback) {
  JoinClusterGlobal(context,
                    duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY);
  auto current = catalog::FindRole(context, name);
  if (!current) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", name, "\" does not exist"));
  }
  catalog::RequireRoleNotVanished(context, name);
  check(context, *current);  // caller's access check, on the live entry

  duckdb::unique_ptr<CreateRoleInfo> updated;
  callback(*current, updated);
  if (!updated) {
    return;
  }
  // A change may only add privileged attributes the actor holds itself. A
  // rename onto a taken name is refused here rather than by the set, so the
  // error is PG's "role already exists" rather than a serialization failure.
  RequireAttributesGrantable(context, actor_id,
                             updated->Options() & ~current->Options(),
                             /*creating=*/false);
  const auto old_name = std::string{current->GetName()};
  if (updated->GetName() != old_name &&
      catalog::FindRole(context, updated->GetName())) {
    ThrowDuplicateName(NameKind::Role, updated->GetName());
  }
  catalog::PutRole(context, old_name, std::move(updated));
}

void Catalog::ChangeRole(
  const AccessContext& ax, std::string_view name, std::string_view verb,
  bool allow_self, ChangeCallback<SereneDBRoleEntry, CreateRoleInfo> callback) {
  ChangeRoleImpl(
    ax.context, ax.role, name,
    [&](duckdb::ClientContext* context, const SereneDBRoleEntry& role) {
      if (allow_self && ax.role == role.GetId()) {
        return;  // a role may change its own entry (e.g. SET config)
      }
      RequireRoleAdmin(context, ax.role, role, verb);
    },
    std::move(callback));
}

void Catalog::ChangeDefaultAcl(const AccessContext& ax,
                               std::string_view role_name, ObjectId schema,
                               char objtype, duckdb::CatalogType type,
                               absl::AnyInvocable<void(Acl&)> mutate) {
  ChangeRoleImpl(
    ax.context, ax.role, role_name,
    [&](duckdb::ClientContext* context, const SereneDBRoleEntry& role) {
      RequireRoleMembership(context, ax.role, role);
    },
    [schema, objtype, type, mutate = std::move(mutate)](
      const SereneDBRoleEntry& old_role,
      duckdb::unique_ptr<CreateRoleInfo>& new_role) mutable {
      new_role = old_role.Record();
      new_role->ChangeDefaultAcl(schema, objtype, type, mutate);
    });
}

void Catalog::ChangeMembership(const AccessContext& ax, ObjectId role,
                               std::string_view role_name, ObjectId member,
                               std::string_view member_name,
                               const Membership& edge, bool revoke,
                               bool admin_option_only) {
  JoinClusterGlobal(ax.context,
                    duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY);
  auto roles = auth::RolesOf(ax.context);
  auto actor = catalog::FindRole(ax.context, ax.role);
  if (!(actor && actor->IsSuperuser()) &&
      !auth::HasAdminOption(*roles, ax.role, role)) {
    const auto verb = revoke ? "revoke" : "grant";
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to ", verb, " role \"", role_name, "\""),
      ERR_DETAIL("Only roles with the ADMIN option on role \"", role_name,
                 "\" may ", verb, " this role."));
  }
  if (!revoke) {
    if (!catalog::FindRole(ax.context, role)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("role \"", role_name, "\" does not exist"));
    }
    catalog::RequireRoleNotVanished(ax.context, role_name);
    if (auth::ComputeMembershipClosure(*roles, role).contains(member)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_GRANT_OPERATION),
                      ERR_MSG("role \"", role_name, "\" is a member of role \"",
                              member_name, "\""));
    }
  }

  const auto member_role = catalog::FindRole(ax.context, member);
  if (!member_role) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", member_name, "\" does not exist"));
  }
  catalog::RequireRoleNotVanished(ax.context, member_name);

  auto new_role = member_role->Record();
  if (revoke && admin_option_only) {
    auto edges = new_role->MemberOf();
    auto it = std::ranges::find(edges, role, &Membership::role);
    if (it != edges.end()) {
      Membership kept = *it;
      kept.admin_option = false;
      new_role->AddMembership(kept);
    }
  } else if (revoke) {
    new_role->RemoveMembership(role);
  } else {
    new_role->AddMembership(edge);
  }
  const auto name = std::string{new_role->GetName()};
  catalog::PutRole(ax.context, name, std::move(new_role));
}

void Catalog::ChangeDatabaseAcl(const AccessContext& ax, ObjectId database_id,
                                AclMutator mutate) {
  JoinClusterGlobal(ax.context,
                    duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY);
  auto database = catalog::FindDatabase(ax.context, database_id);
  if (!database) [[unlikely]] {
    ThrowConcurrentlyDropped(database_id);
  }
  auto perm = auth::MutatedAcl(database->permissions,
                               duckdb::CatalogType::DATABASE_ENTRY, mutate);
  auto updated = database->Definition();
  const auto name = std::string{updated->GetName()};
  catalog::PutDatabase(ax.context, name, std::move(updated), std::move(perm));
}

bool Catalog::DropRole(const AccessContext& ax, std::string_view role,
                       bool missing_ok) {
  JoinClusterGlobal(ax.context,
                    duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  RequireRoleAttribute(ax.context, ax.role, RoleOption::CreateRole, "drop role",
                       "Only roles with the CREATEROLE attribute and the ADMIN "
                       "option on the target roles may drop roles.");
  auto role_ptr = catalog::FindRole(ax.context, role);
  if (!role_ptr) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", role, "\" does not exist"));
  }
  if (role_ptr->GetId() == ax.role) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                    ERR_MSG("current user cannot be dropped"));
  }
  if (role == StaticStrings::kDefaultUser) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    ERR_MSG("cannot drop role ", role,
                            " because it is required by the database system"));
  }
  RequireRoleAdmin(ax.context, ax.role, *role_ptr, "drop");
  try {
    catalog::DropRoleEntry(ax.context, role);
  } catch (const duckdb::DependencyException& blocked) {
    // The dependency walk found the blockers; postgres words the refusal by
    // how many there are rather than by naming them.
    const duckdb::ErrorData error{blocked};
    const auto it = error.ExtraInfo().find("blocking_dependents");
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("role \"", role,
              "\" cannot be dropped because some objects depend on it"),
      ERR_DETAIL(it == error.ExtraInfo().end() ? std::string{} : it->second,
                 " object(s) in database depend on role \"", role, "\""));
  }
  return true;
}

void Catalog::DropDatabase(const AccessContext& ax, std::string_view name,
                           duckdb::shared_ptr<void> keep_alive) {
  JoinStoreTransaction(ax.context);
  JoinClusterGlobal(ax.context,
                    duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  auto database = catalog::FindDatabase(ax.context, name);
  if (!database) {
    THROW_SQL_ERROR(ERR_MSG("database \"", name, "\" does not exist"));
  }
  const auto database_id = std::optional{catalog::IdOf(*database)};
  RequireDatabaseOwner(ax.context, ax.role, database);

  // The database's containment is not walked (PG has no cross-db refs), so
  // everything here is this road's own: the store half, the counter rows, the
  // artifacts and the file.
  std::vector<ObjectId> owned_sequences;
  catalog::Visit<catalog::SereneDBSequenceEntry>(
    ax.context, *database_id,
    [&](const catalog::SereneDBSequenceEntry& sequence) {
      owned_sequences.push_back(ObjectId{sequence.oid});
    });
  std::vector<const catalog::SereneDBIndexEntry*> owned_indexes;
  catalog::Visit<catalog::SereneDBIndexEntry>(
    ax.context, *database_id, [&](const catalog::SereneDBIndexEntry& index) {
      owned_indexes.push_back(&index);
    });
  std::vector<const catalog::SereneDBTableEntry*> tables;
  catalog::Visit<catalog::SereneDBTableEntry>(
    ax.context, *database_id, [&](const catalog::SereneDBTableEntry& table) {
      tables.push_back(&table);
    });
  for (const auto seq_id : owned_sequences) {
    catalog::DeferDropAction(
      ax.context, [seq_id] { GetCatalogStore().DropSequence(seq_id); });
  }
  // Check that SereneDB won't open this database after reboot
  bool crash_on_drop = false;
  SDB_IF_FAILURE("crash_on_drop") { crash_on_drop = true; }
  if (!crash_on_drop) {
    for (const auto* table : tables) {
      catalog::DropSearchTableArtifacts(ax.context, *table);
    }
    for (const auto* index : owned_indexes) {
      const auto record = index->GetInfo();
      catalog::DropIndexArtifacts(ax.context, *database_id,
                                  record->Cast<catalog::CreateIndexInfo>(),
                                  index->GetInvertedData());
    }
    // No committed record names the file, so it is garbage whatever happens
    // next: a crash before the unlink leaves it for boot reclamation.
    catalog::DeferDropAction(
      ax.context, [db_id = *database_id, keep_alive = std::move(keep_alive)] {
        const auto path = CatalogStore::DatabaseFilePath(db_id);
        for (const auto& file : {path, path + ".wal"}) {
          std::error_code ec;
          std::filesystem::remove(file, ec);
          if (ec) {
            SDB_WARN(GENERAL, "could not remove '", file, "': ", ec.message());
          }
        }
      });
  }
  catalog::DropDatabaseEntry(ax.context, name);
}

}  // namespace sdb::catalog
