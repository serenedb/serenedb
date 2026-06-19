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

#include "pg/commands/rbac.h"

#include <absl/algorithm/container.h>
#include <absl/functional/function_ref.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include "app/app_server.h"
#include "auth/acl.h"
#include "auth/privilege.h"
#include "catalog/catalog.h"
#include "catalog/table.h"
#include "pg/errcodes.h"
#include "pg/pg_types.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"

namespace sdb::pg {
namespace {

catalog::Catalog& GlobalCatalog() { return catalog::GetCatalog(); }

std::shared_ptr<const catalog::Snapshot> FreshSnapshot() {
  return GlobalCatalog().GetCatalogSnapshot();
}

std::shared_ptr<catalog::Role> CurrentRole(ConnectionContext& ctx) {
  auto role = FreshSnapshot()->GetRole(ctx.user());
  if (!role) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("current role \"", ctx.user(), "\" does not exist"));
  }
  return role;
}

// PG: creating/dropping/altering roles requires the CREATEROLE attribute (or
// superuser); the error carries a DETAIL naming the missing attribute.
void RequireCreateRole(ConnectionContext& ctx, std::string_view action) {
  auto role = CurrentRole(ctx);
  if (!role->IsSuperuser() && !role->Has(catalog::RoleOption::CreateRole)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to ", action),
      ERR_DETAIL("Only roles with the CREATEROLE attribute may create roles."));
  }
}

void RequireCreateRolePrivilege(ConnectionContext& ctx, std::string_view verb) {
  auto role = CurrentRole(ctx);
  if (role->IsSuperuser() || role->Has(catalog::RoleOption::CreateRole)) {
    return;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ERR_MSG("permission denied to ", verb, " role"),
    ERR_DETAIL("Only roles with the CREATEROLE attribute and the ADMIN option "
               "on the target roles may ",
               verb, " roles."));
}

void RequireRoleAdmin(ConnectionContext& ctx, const catalog::Snapshot& snapshot,
                      const catalog::Role& target, std::string_view verb) {
  auto current = CurrentRole(ctx);
  if (current->IsSuperuser()) {
    return;
  }
  if (target.IsSuperuser()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied to ", verb, " role"),
                    ERR_DETAIL("Only roles with the SUPERUSER attribute may ",
                               verb, " roles with the SUPERUSER attribute."));
  }
  if (!current->Has(catalog::RoleOption::CreateRole) ||
      !auth::HasAdminOption(snapshot, current->GetId(), target.GetId())) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to ", verb, " role"),
      ERR_DETAIL("Only roles with the CREATEROLE attribute and the ADMIN "
                 "option on role \"",
                 target.GetName(), "\" may ", verb, " this role."));
  }
}

}  // namespace

void RequirePrivilege(ConnectionContext& ctx, const catalog::Object& object,
                      catalog::AclMode need) {
  auto snapshot = ctx.EnsureCatalogSnapshot();
  auto current = snapshot->GetObject(object.GetId());
  snapshot->RequireAccess(ctx.GetRoleId(), current ? *current : object, need);
}

void RequireColumnPrivilege(ConnectionContext& ctx, const catalog::Table& table,
                            catalog::AclMode need,
                            std::span<const catalog::Column* const> columns) {
  auto snapshot = ctx.EnsureCatalogSnapshot();
  snapshot->RequireColumnAccess(ctx.GetRoleId(), table, need, columns);
}

namespace {

ObjectId OwnerOrRoot(const catalog::Object& obj) {
  return obj.GetOwner().isSet() ? obj.GetOwner() : id::kRootUser;
}

void EnforceOwnership(
  ConnectionContext& ctx, std::string_view obj_type, std::string_view name,
  absl::FunctionRef<ObjectId(const catalog::Snapshot&)> lookup) {
  auto snapshot = FreshSnapshot();
  snapshot->RequireOwnership(ctx.GetRoleId(), lookup(*snapshot), obj_type,
                             name);
}

}  // namespace

void EnforceRelationOwnership(ConnectionContext& ctx, std::string_view schema,
                              std::string_view name,
                              std::string_view obj_type) {
  EnforceOwnership(ctx, obj_type, name, [&](const catalog::Snapshot& s) {
    auto rel = s.GetRelation(catalog::NoAccessCheck(), ctx.GetDatabaseId(),
                             schema, name);
    return rel ? OwnerOrRoot(*rel) : id::kInvalid;
  });
}

void EnforceDatabaseOwnership(ConnectionContext& ctx, std::string_view name) {
  EnforceOwnership(ctx, "database", name, [&](const catalog::Snapshot& s) {
    auto db = s.GetDatabase(name);
    return db ? OwnerOrRoot(*db) : id::kInvalid;
  });
}

void CreateRole(ConnectionContext& ctx, std::string_view name,
                const CreateRoleOptions& options) {
  RequireCreateRole(ctx, "create role");

  auto& catalog = GlobalCatalog();
  auto role = catalog::Role::NewUser(name, options.password);
  catalog::RoleOption opts = catalog::RoleOption::None;
  if (options.login) {
    opts |= catalog::RoleOption::Login;
  }
  if (options.superuser) {
    opts |= catalog::RoleOption::Superuser;
  }
  if (options.inherit) {
    opts |= catalog::RoleOption::Inherit;
  }
  role->SetOptions(opts);
  if (!options.has_password) {
    role->ClearPassword();
  }
  role->SetConnLimit(options.conn_limit);
  role->SetValidUntil(options.valid_until);

  auto r = catalog.CreateRole(std::move(role));
  if (r.is(ERROR_USER_DUPLICATE)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("role \"", name, "\" already exists"));
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  auto creator = CurrentRole(ctx);
  if (!creator->IsSuperuser()) {
    auto new_role = FreshSnapshot()->GetRole(name);
    const catalog::Membership edge{
      .role = new_role->GetId(),
      .admin_option = true,
      .inherit_option = false,
      .set_option = false,
    };
    auto gr = catalog.ChangeRole(
      creator->GetName(),
      [&](const catalog::Role& old_role,
          std::shared_ptr<catalog::Role>& updated) -> Result {
        updated = std::static_pointer_cast<catalog::Role>(old_role.Clone());
        updated->AddMembership(edge);
        return {};
      });
    if (!gr.ok()) {
      SDB_THROW(std::move(gr));
    }
  }
}

std::size_t CountRoleDependencies(const catalog::Snapshot& snapshot,
                                  ObjectId role_id) {
  std::size_t deps = 0;
  for (const auto& db : snapshot.GetDatabases()) {
    for (const auto& schema : snapshot.GetSchemas(db->GetId())) {
      for (const auto& table :
           snapshot.GetTables(db->GetId(), schema->GetName())) {
        const bool referenced =
          table->GetOwner() == role_id ||
          absl::c_any_of(table->GetAcl(), [&](const catalog::AclItem& item) {
            return item.grantee == role_id || item.grantor == role_id;
          });
        if (referenced) {
          ++deps;
        }
      }
    }
  }
  return deps;
}

void DropRole(ConnectionContext& ctx, std::string_view name, bool missing_ok) {
  RequireCreateRolePrivilege(ctx, "drop");

  auto& catalog = GlobalCatalog();
  auto snapshot = catalog.GetCatalogSnapshot();
  auto role = snapshot->GetRole(name);
  if (!role) {
    if (missing_ok) {
      ctx.AddNotice(SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
        ERR_MSG("role \"", name, "\" does not exist, skipping")));
      return;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", name, "\" does not exist"));
  }
  RequireRoleAdmin(ctx, *snapshot, *role, "drop");
  if (name == ctx.user()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                    ERR_MSG("current user cannot be dropped"));
  }
  if (name == StaticStrings::kDefaultUser) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    ERR_MSG("cannot drop role ", name,
                            " because it is required by the database system"));
  }

  if (auto deps = CountRoleDependencies(*snapshot, role->GetId()); deps > 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("role \"", name,
              "\" cannot be dropped because some objects "
              "depend on it"),
      ERR_DETAIL(deps, " object(s) in database depend on role \"", name, "\""));
  }

  auto r = catalog.DropRole(name);
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
}

namespace {

catalog::RoleOption SetBit(catalog::RoleOption options, catalog::RoleOption bit,
                           int tri) {
  if (tri == 1) {
    return options | bit;
  }
  if (tri == 0) {
    return options & ~bit;
  }
  return options;
}

}  // namespace
namespace {

bool IsSelfPasswordOnly(const AlterRoleOptions& opts) {
  return opts.set_password && opts.login == -1 && opts.superuser == -1 &&
         opts.createdb == -1 && opts.createrole == -1 && opts.inherit == -1 &&
         opts.conn_limit == -2 && !opts.set_valid_until;
}

}  // namespace

void AlterRole(ConnectionContext& ctx, std::string_view name,
               const AlterRoleOptions& opts) {
  if (!(name == ctx.user() && IsSelfPasswordOnly(opts))) {
    auto snapshot = FreshSnapshot();
    if (auto target = snapshot->GetRole(name)) {
      RequireRoleAdmin(ctx, *snapshot, *target, "alter");
    }
  }

  auto& catalog = GlobalCatalog();
  auto r = catalog.ChangeRole(
    name,
    [&](const catalog::Role& old_role,
        std::shared_ptr<catalog::Role>& new_role) -> Result {
      new_role = std::static_pointer_cast<catalog::Role>(old_role.Clone());
      catalog::RoleOption o = new_role->Options();
      o = SetBit(o, catalog::RoleOption::Login, opts.login);
      o = SetBit(o, catalog::RoleOption::Superuser, opts.superuser);
      o = SetBit(o, catalog::RoleOption::CreateDb, opts.createdb);
      o = SetBit(o, catalog::RoleOption::CreateRole, opts.createrole);
      o = SetBit(o, catalog::RoleOption::Inherit, opts.inherit);
      new_role->SetOptions(o);
      if (opts.set_password) {
        if (opts.password_null) {
          new_role->ClearPassword();
        } else {
          new_role->UpdatePassword(opts.password);
        }
      }
      if (opts.conn_limit != -2) {
        new_role->SetConnLimit(opts.conn_limit);
      }
      if (opts.set_valid_until) {
        new_role->SetValidUntil(opts.valid_until);
      }
      return {};
    });
  if (r.is(ERROR_SERVER_ILLEGAL_NAME)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", name, "\" does not exist"));
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
}

void RenameRole(ConnectionContext& ctx, std::string_view name,
                std::string_view new_name) {
  {
    auto snapshot = FreshSnapshot();
    if (auto target = snapshot->GetRole(name)) {
      RequireRoleAdmin(ctx, *snapshot, *target, "rename");
    }
  }

  auto& catalog = GlobalCatalog();
  auto r = catalog.ChangeRole(
    name,
    [&](const catalog::Role& old_role,
        std::shared_ptr<catalog::Role>& new_role) -> Result {
      new_role = std::static_pointer_cast<catalog::Role>(old_role.Clone());
      new_role->UpdateName(new_name);
      return {};
    });
  if (r.is(ERROR_SERVER_ILLEGAL_NAME)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", name, "\" does not exist"));
  }
  if (r.is(ERROR_USER_DUPLICATE)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("role \"", new_name, "\" already exists"));
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
}

void AlterRoleConfig(ConnectionContext& ctx, std::string_view name,
                     std::string_view op, std::string_view setting,
                     std::string_view value) {
  if (name != ctx.user()) {
    auto snapshot = FreshSnapshot();
    if (auto target = snapshot->GetRole(name)) {
      RequireRoleAdmin(ctx, *snapshot, *target, "alter");
    }
  }

  auto& catalog = GlobalCatalog();
  auto r = catalog.ChangeRole(
    name,
    [&](const catalog::Role& old_role,
        std::shared_ptr<catalog::Role>& new_role) -> Result {
      new_role = std::static_pointer_cast<catalog::Role>(old_role.Clone());
      if (op == "RESET_ALL") {
        new_role->ResetAllConfig();
      } else if (op == "RESET") {
        new_role->ResetConfig(setting);
      } else {
        new_role->SetConfig(setting, value);
      }
      return {};
    });
  if (r.is(ERROR_SERVER_ILLEGAL_NAME)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", name, "\" does not exist"));
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
}

namespace {

bool AclItemEq(const catalog::AclItem& a, const catalog::AclItem& b) {
  return a.grantee == b.grantee && a.grantor == b.grantor &&
         a.privs == b.privs && a.grant_option == b.grant_option;
}

// True when `acl` carries nothing beyond the implicit owner default (PG removes
// the pg_default_acl row in that case).
bool AclMatchesDefault(const catalog::Acl& acl, const catalog::Acl& def) {
  return std::ranges::equal(acl, def, AclItemEq);
}

// pg_default_acl objtype char -> catalog::ObjectType (for acldefault seeding /
// privilege validation).
catalog::ObjectType DefaultAclObjType(std::string_view objtype_char) {
  if (objtype_char == "S") {
    return catalog::ObjectType::Sequence;
  }
  if (objtype_char == "f") {
    return catalog::ObjectType::PgSqlFunction;
  }
  if (objtype_char == "T") {
    return catalog::ObjectType::PgSqlType;
  }
  if (objtype_char == "n") {
    return catalog::ObjectType::Schema;
  }
  return catalog::ObjectType::Table;  // 'r'
}

catalog::AclMode ParseAclModeOrThrow(std::string_view privileges,
                                     catalog::ObjectType type) {
  try {
    return auth::ParseAclMode(privileges, type);
  } catch (const basics::Exception& e) {
    // PG's GRANT statement rejects an unknown privilege keyword with
    // 42601 / unrecognized privilege type "<lower>" (the keyword is downcased,
    // no colon -- distinct from the has_*_privilege function path which keeps
    // the colon and SQLSTATE 22023). A keyword valid elsewhere but not for this
    // object's class is the "invalid privilege type ..." form (0LP01).
    const auto msg = e.message();
    if (absl::StartsWith(msg, "unrecognized privilege type:")) {
      auto open = msg.find('"');
      auto close = msg.rfind('"');
      std::string tok = (open != std::string::npos && close > open)
                          ? msg.substr(open + 1, close - open - 1)
                          : std::string{};
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      ERR_MSG("unrecognized privilege type \"",
                              absl::AsciiStrToLower(tok), "\""));
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_GRANT_OPERATION), ERR_MSG(msg));
  }
}

// One parsed privilege from the GRANT pragma string: a keyword and an optional
// column list. The transformer encodes columns inline as "SELECT(a:b)".
struct ParsedPriv {
  std::string keyword;               // e.g. "SELECT" / "ALL" / a bad name
  std::vector<std::string> columns;  // empty => table-level (no '(...)')
};

// Parse the encoded privilege string ("SELECT(a:b),UPDATE(c)" or
// "SELECT,INSERT" or "ALL") into its privileges + per-privilege column lists.
std::vector<ParsedPriv> ParsePrivList(std::string_view privileges) {
  std::vector<ParsedPriv> out;
  for (std::string_view tok :
       absl::StrSplit(privileges, ',', absl::SkipEmpty())) {
    ParsedPriv p;
    auto open = tok.find('(');
    if (open == std::string_view::npos) {
      p.keyword = std::string{tok};
    } else {
      p.keyword = std::string{tok.substr(0, open)};
      auto close = tok.rfind(')');
      auto inner = tok.substr(open + 1, close - open - 1);
      for (std::string_view c : absl::StrSplit(inner, ':', absl::SkipEmpty())) {
        p.columns.emplace_back(c);
      }
    }
    out.push_back(std::move(p));
  }
  return out;
}

bool AnyColumnPrivs(const std::vector<ParsedPriv>& parsed) {
  return std::ranges::any_of(
    parsed, [](const ParsedPriv& p) { return !p.columns.empty(); });
}

// Resolve a grantee name to a role id, mapping PUBLIC to the public pseudo-id
// and throwing on an unknown role.
ObjectId ResolveGranteeId(const catalog::Snapshot& snap,
                          std::string_view grantee) {
  if (grantee == "PUBLIC" || grantee == "public") {
    return catalog::kPublicGrantee;
  }
  auto grantee_role = snap.GetRole(grantee);
  if (!grantee_role) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", grantee, "\" does not exist"));
  }
  return grantee_role->GetId();
}

// Apply a GRANT/REVOKE to `acl` for the full requested `privs` (no grant-option
// narrowing). Used by the default-privileges and built-in-type paths; the
// object GRANT path in GrantObject does its own narrowing and dependent-priv
// handling and does not share this.
void ApplyAclChange(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
                    catalog::AclMode privs, bool revoke, bool with_grant_option,
                    bool grant_option_only, bool cascade) {
  const auto grant_option =
    with_grant_option ? privs : catalog::AclMode::NoRights;
  if (!revoke) {
    auth::AclGrant(acl, grantee, grantor, privs, grant_option);
  } else if (grant_option_only) {
    auth::AclRemoveGrantOption(acl, grantee, grantor, privs);
  } else if (cascade) {
    auth::AclRevokeCascade(acl, grantee, grantor, privs);
  } else {
    auth::AclRevoke(acl, grantee, grantor, privs);
  }
}

}  // namespace

void AlterDefaultPrivileges(ConnectionContext& ctx, std::string_view privileges,
                            std::string_view objtype_char,
                            std::string_view grantee, bool revoke,
                            const DefaultPrivilegesOptions& opts) {
  auto& catalog = GlobalCatalog();
  auto snapshot = FreshSnapshot();

  // FOR ROLE defaults to the current user (PG: the role running the command).
  const std::string_view defacl_role_name =
    opts.for_role.empty() ? ctx.user() : opts.for_role;
  auto defacl_role = snapshot->GetRole(defacl_role_name);
  if (!defacl_role) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", defacl_role_name, "\" does not exist"));
  }
  const ObjectId defacl_role_id = defacl_role->GetId();

  const ObjectId grantee_id = ResolveGranteeId(*snapshot, grantee);

  // IN SCHEMA: id::kInvalid means "all schemas" (PG defaclnamespace = 0).
  ObjectId schema_id = id::kInvalid;
  if (!opts.in_schema.empty()) {
    auto schema = snapshot->GetSchema(ctx.GetDatabaseId(), opts.in_schema);
    if (!schema) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
        ERR_MSG("schema \"", opts.in_schema, "\" does not exist"));
    }
    schema_id = schema->GetId();
  }

  const auto type = DefaultAclObjType(objtype_char);
  const char objtype_c = objtype_char.empty() ? 'r' : objtype_char.front();

  const catalog::AclMode privs = ParseAclModeOrThrow(privileges, type);

  auto r = catalog.ChangeRole(
    defacl_role_name,
    [&](const catalog::Role& old_role,
        std::shared_ptr<catalog::Role>& new_role) -> Result {
      new_role = std::static_pointer_cast<catalog::Role>(old_role.Clone());
      auto& entry = new_role->MutableDefaultAcl(schema_id, objtype_c);
      // Seed an empty entry from the implicit owner default (PG's acldefault),
      // so the rendered defaclacl carries the owner self-grant just like PG.
      if (entry.acl.empty()) {
        entry.acl = auth::AclDefault(type, defacl_role_id);
      }
      ApplyAclChange(entry.acl, grantee_id, defacl_role_id, privs, revoke,
                     opts.with_grant_option, opts.grant_option_only,
                     opts.cascade);
      // PG drops the pg_default_acl row when its ACL collapses back to the bare
      // owner default (nothing extra granted).
      if (AclMatchesDefault(entry.acl,
                            auth::AclDefault(type, defacl_role_id))) {
        new_role->RemoveDefaultAcl(schema_id, objtype_c);
      }
      return {};
    });
  if (r.is(ERROR_SERVER_ILLEGAL_NAME)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", defacl_role_name, "\" does not exist"));
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
}

namespace {

// Resolves the GRANT target and yields its schema/name plus the owning role
// (relowner). Returns nullptr if the object does not exist. The owner comes
// from the object itself, never from a self-grant ACL row -- ALTER OWNER and
// owner-self-REVOKE leave that row stale or absent while GetOwner() stays
// authoritative.
std::shared_ptr<catalog::Object> ResolveGrantTarget(
  ConnectionContext& ctx, const catalog::Snapshot& snap,
  catalog::ObjectType type, std::string_view raw_name, std::string& out_schema,
  std::string& out_name) {
  if (type == catalog::ObjectType::Database) {
    out_name = std::string{raw_name};
    return snap.GetDatabase(raw_name);
  }
  if (type == catalog::ObjectType::Schema) {
    out_name = std::string{raw_name};
    return snap.GetSchema(ctx.GetDatabaseId(), raw_name);
  }
  const auto parsed = ParseObjectName(raw_name, ctx.GetCurrentSchema());
  out_schema = parsed.schema;
  out_name = parsed.relation;
  if (type == catalog::ObjectType::PgSqlFunction) {
    return snap.GetFunction(catalog::NoAccessCheck(), ctx.GetDatabaseId(),
                            parsed.schema, parsed.relation);
  }
  if (type == catalog::ObjectType::PgSqlType) {
    return snap.GetType(catalog::NoAccessCheck(), ctx.GetDatabaseId(),
                        parsed.schema, parsed.relation);
  }
  return snap.GetRelation(catalog::NoAccessCheck(), ctx.GetDatabaseId(),
                          parsed.schema, parsed.relation);
}

// GRANT/REVOKE ... ON TYPE <builtin> -> store the grant on the root role's
// built-in-type ACL map (built-in types own no catalog object). The stored Acl
// is the non-default grant set; pg_type seeds the owner default when rendering.
void GrantBuiltinType(ConnectionContext& ctx, uint64_t type_oid,
                      std::string_view privileges, std::string_view grantee,
                      bool revoke, const GrantObjectOptions& opts) {
  auto& catalog = GlobalCatalog();
  auto snapshot = FreshSnapshot();

  const ObjectId grantee_id = ResolveGranteeId(*snapshot, grantee);

  const catalog::AclMode privs =
    ParseAclModeOrThrow(privileges, catalog::ObjectType::PgSqlType);

  // Built-in types are implicitly owned by the root role; the grantor is root.
  const ObjectId owner = id::kRootUser;
  auto root = snapshot->GetObject<catalog::Role>(owner);
  const std::string root_name{root ? root->GetName() : std::string{}};

  auto r = catalog.ChangeRole(
    root_name,
    [&](const catalog::Role& old_role,
        std::shared_ptr<catalog::Role>& new_role) -> Result {
      new_role = std::static_pointer_cast<catalog::Role>(old_role.Clone());
      auto& acl = new_role->MutableBuiltinTypeAcl(type_oid);
      ApplyAclChange(acl, grantee_id, owner, privs, revoke,
                     opts.with_grant_option, opts.grant_option_only,
                     opts.cascade);
      if (acl.empty()) {
        new_role->RemoveBuiltinTypeAcl(type_oid);
      }
      return {};
    });
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
}

}  // namespace

void ThrowNotADomain(std::string_view name) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                  ERR_MSG("\"", name, "\" is not a domain"));
}

namespace {

// Column-level GRANT/REVOKE (PG column privileges). Only tables carry
// per-column ACLs; `privileges` is the parsed list, each entry naming its own
// columns.
void GrantObjectColumns(ConnectionContext& ctx, catalog::ObjectType type,
                        const std::vector<ParsedPriv>& parsed,
                        std::string_view obj_name, std::string_view grantee,
                        bool revoke, const GrantObjectOptions& opts) {
  auto& catalog = GlobalCatalog();
  auto snapshot = FreshSnapshot();

  std::string schema_name;
  std::string rel_name;
  auto target =
    ResolveGrantTarget(ctx, *snapshot, type, obj_name, schema_name, rel_name);
  if (!target || target->GetType() != catalog::ObjectType::Table) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", rel_name, "\" does not exist"));
  }
  const ObjectId owner = target->GetOwner();
  auto current = CurrentRole(ctx);
  const bool is_superuser = current->IsSuperuser();
  const ObjectId current_id = current->GetId();
  const ObjectId grantee_id = ResolveGranteeId(*snapshot, grantee);

  ObjectId granted_by_id = id::kInvalid;
  if (!opts.granted_by.empty()) {
    auto gb = snapshot->GetRole(opts.granted_by);
    if (!gb) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("role \"", opts.granted_by, "\" does not exist"));
    }
    granted_by_id = gb->GetId();
  }

  const auto roles = auth::ComputeEffectiveRoles(*snapshot, current_id);
  const bool is_owner = is_superuser || roles.contains(owner);

  bool no_authority = false;
  bool nothing_applied = false;
  bool dependents_block = false;
  // PG column-applicable privileges: GRANT ALL (col) confers only these (a
  // column has no DELETE/TRUNCATE/TRIGGER/MAINTAIN of its own).
  constexpr catalog::AclMode kColumnPrivs =
    catalog::AclMode::Select | catalog::AclMode::Insert |
    catalog::AclMode::Update | catalog::AclMode::References;
  for (const auto& p : parsed) {
    catalog::AclMode privs =
      ParseAclModeOrThrow(p.keyword, catalog::ObjectType::Table);
    // PG: an explicit non-column-applicable privilege named with a column list
    // (e.g. GRANT DELETE (a)) is a hard error; ALL silently narrows.
    const bool is_all = absl::EqualsIgnoreCase(p.keyword, "ALL");
    if (!is_all && (privs & ~kColumnPrivs) != catalog::AclMode::NoRights) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_GRANT_OPERATION),
                      ERR_MSG("invalid privilege type ",
                              absl::AsciiStrToUpper(p.keyword), " for column"));
    }
    privs &= kColumnPrivs;
    for (const auto& column : p.columns) {
      auto r = catalog.ChangeColumnAcl(
        ctx.GetDatabaseId(), schema_name, rel_name, column,
        [&](catalog::Acl& acl) {
          // PG: the owner/superuser may grant anything; a non-owner may confer
          // only the column privileges it holds WITH GRANT OPTION on this
          // column. The grant is recorded under that grantor.
          const ObjectId grantor = granted_by_id.isSet()
                                     ? granted_by_id
                                     : (is_owner ? owner : current_id);
          catalog::AclMode allowed = privs;
          if (!is_owner) {
            allowed &= auth::AclGrantOptionHeld(acl, roles);
          }
          if (allowed == catalog::AclMode::NoRights) {
            if (!is_owner &&
                auth::AclPrivsHeld(acl, roles) == catalog::AclMode::NoRights) {
              no_authority = true;
            } else {
              nothing_applied = true;
            }
            return;
          }
          if (!revoke) {
            const auto grant_option =
              opts.with_grant_option ? allowed : catalog::AclMode::NoRights;
            auth::AclGrant(acl, grantee_id, grantor, allowed, grant_option);
          } else if (opts.grant_option_only) {
            auth::AclRemoveGrantOption(acl, grantee_id, grantor, allowed);
          } else if (opts.cascade) {
            auth::AclRevokeCascade(acl, grantee_id, grantor, allowed);
          } else if (auth::AclDependentPrivs(acl, grantee_id, allowed) !=
                     catalog::AclMode::NoRights) {
            dependents_block = true;
          } else {
            auth::AclRevoke(acl, grantee_id, grantor, allowed);
          }
        });
      if (!r.ok()) {
        SDB_THROW(std::move(r));
      }
    }
  }
  if (dependents_block) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    ERR_MSG("dependent privileges exist"),
                    ERR_HINT("Use CASCADE to revoke them too."));
  }
  if (no_authority) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied for table ", rel_name));
  }
  if (nothing_applied) {
    ctx.AddNotice(SQL_ERROR_DATA(
      ERR_CODE(revoke ? ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED
                      : ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
      ERR_MSG("no privileges were ", revoke ? "revoked" : "granted", " for \"",
              rel_name, "\"")));
  }
}

}  // namespace

void GrantObject(ConnectionContext& ctx, catalog::ObjectType type,
                 std::string_view privileges, std::string_view obj_name,
                 std::string_view grantee, bool revoke,
                 const GrantObjectOptions& opts) {
  // Column-level GRANT (SELECT (a,b) ON t) takes the per-column path.
  if (auto parsed = ParsePrivList(privileges); AnyColumnPrivs(parsed)) {
    GrantObjectColumns(ctx, type, parsed, obj_name, grantee, revoke, opts);
    return;
  }

  auto& catalog = GlobalCatalog();
  auto snapshot = FreshSnapshot();

  std::string schema_name;
  std::string rel_name;
  auto target =
    ResolveGrantTarget(ctx, *snapshot, type, obj_name, schema_name, rel_name);
  if (!target) {
    // A GRANT ON TYPE that names a built-in/system type (no catalog object, but
    // a fixed pg_type OID) is valid in PG and sets pg_type.typacl.
    if (type == catalog::ObjectType::PgSqlType) {
      if (auto oid = RegtypeIn(rel_name); oid != kInvalidOid) {
        GrantBuiltinType(ctx, oid, privileges, grantee, revoke, opts);
        return;
      }
    }
    const bool is_relation = type == catalog::ObjectType::Table ||
                             type == catalog::ObjectType::PgSqlView ||
                             type == catalog::ObjectType::Sequence;
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG(is_relation ? "relation" : ToPgObjectTypeName(type),
                            " \"", rel_name, "\" does not exist"));
  }
  // A GRANT target (relation / schema / database / function / type) is always
  // created with a real owner, so GetOwner() is set.
  const ObjectId owner = target->GetOwner();

  auto current = CurrentRole(ctx);
  const bool is_superuser = current->IsSuperuser();
  const ObjectId current_id = current->GetId();

  const ObjectId grantee_id = ResolveGranteeId(*snapshot, grantee);

  ObjectId granted_by_id = id::kInvalid;
  if (!opts.granted_by.empty()) {
    auto gb = snapshot->GetRole(opts.granted_by);
    if (!gb) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("role \"", opts.granted_by, "\" does not exist"));
    }
    granted_by_id = gb->GetId();
    if (!is_superuser && !auth::ComputeMembershipClosure(*snapshot, current_id)
                            .contains(granted_by_id)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("must be member of role \"", opts.granted_by, "\""));
    }
  }

  const catalog::AclMode privs = ParseAclModeOrThrow(privileges, type);

  bool no_authority = false;
  bool nothing_applied = false;
  bool dependents_block = false;
  // For a Database target ChangeAcl looks up the object by `database_id`
  // directly, so it must be the *target* database -- not the connection's
  // current database, which may differ (GRANT ON DATABASE postgres while
  // connected to another db). Every other target resolves its schema/relation
  // within the connection's database.
  const ObjectId acl_database_id = type == catalog::ObjectType::Database
                                     ? target->GetId()
                                     : ctx.GetDatabaseId();
  auto r = catalog.ChangeAcl(
    acl_database_id, schema_name, rel_name, type, [&](catalog::Acl& acl) {
      auto live = catalog.GetCatalogSnapshot();
      const auto roles = auth::ComputeEffectiveRoles(*live, current_id);
      const bool is_owner = is_superuser || roles.contains(owner);
      const ObjectId grantor =
        granted_by_id.isSet() ? granted_by_id : (is_owner ? owner : current_id);
      catalog::AclMode allowed = privs;
      if (!is_owner) {
        allowed &= auth::AclGrantOptionHeld(acl, roles);
      }
      if (allowed == catalog::AclMode::NoRights) {
        // PG: a grantor with NO privilege at all on the object is refused
        // outright; one that holds the privilege but not its grant option (so
        // it cannot confer it) gets a warning + no-op.
        if (!is_owner &&
            auth::AclPrivsHeld(acl, roles) == catalog::AclMode::NoRights) {
          no_authority = true;
        } else {
          nothing_applied = true;
        }
        return;
      }
      if (!revoke) {
        const auto grant_option =
          opts.with_grant_option ? allowed : catalog::AclMode::NoRights;
        auth::AclGrant(acl, grantee_id, grantor, allowed, grant_option);
        return;
      }
      if (opts.grant_option_only) {
        auth::AclRemoveGrantOption(acl, grantee_id, grantor, allowed);
        return;
      }
      if (opts.cascade) {
        auth::AclRevokeCascade(acl, grantee_id, grantor, allowed);
        return;
      }
      if (auth::AclDependentPrivs(acl, grantee_id, allowed) !=
          catalog::AclMode::NoRights) {
        dependents_block = true;
        return;
      }
      auth::AclRevoke(acl, grantee_id, grantor, allowed);
    });
  if (dependents_block) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    ERR_MSG("dependent privileges exist"),
                    ERR_HINT("Use CASCADE to revoke them too."));
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
  if (no_authority) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied for ", ToPgObjectTypeName(type),
                            " ", rel_name));
  }
  if (nothing_applied) {
    ctx.AddNotice(SQL_ERROR_DATA(
      ERR_CODE(revoke ? ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED
                      : ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
      ERR_MSG("no privileges were ", revoke ? "revoked" : "granted", " for \"",
              rel_name, "\"")));
  }

  // PG: a table-level REVOKE of a privilege also strips that privilege from
  // every column-level grant of the same grantee (the column entry cannot
  // outlive its table-level counterpart). Cascade the revoke to each column.
  if (revoke && type == catalog::ObjectType::Table) {
    auto fresh = FreshSnapshot();
    if (auto tbl = fresh->GetObject<catalog::Table>(target->GetId())) {
      for (const auto& col : tbl->Columns()) {
        if (col.GetId() == catalog::Column::kGeneratedPKId ||
            col.GetAcl().empty()) {
          continue;
        }
        auto cr = catalog.ChangeColumnAcl(
          ctx.GetDatabaseId(), schema_name, rel_name, col.GetName(),
          [&](catalog::Acl& acl) {
            auth::AclRevoke(acl, grantee_id, owner, privs);
          });
        if (!cr.ok()) {
          SDB_THROW(std::move(cr));
        }
      }
    }
  }
}

void GrantObjectAllInSchema(ConnectionContext& ctx, catalog::ObjectType type,
                            std::string_view privileges,
                            std::string_view schema_name,
                            std::string_view grantee, bool revoke,
                            const GrantObjectOptions& opts) {
  auto snapshot = FreshSnapshot();
  if (!snapshot->GetSchema(ctx.GetDatabaseId(), schema_name)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                    ERR_MSG("schema \"", schema_name, "\" does not exist"));
  }

  // Collect the object names first; each GrantObject call takes a fresh
  // snapshot, so resolve names up front. PG applies to every existing object
  // (an empty schema is a no-op, not an error).
  std::vector<std::string> names;
  const ObjectId db = ctx.GetDatabaseId();
  if (type == catalog::ObjectType::PgSqlFunction) {
    for (const auto& fn : snapshot->GetFunctions(db, schema_name)) {
      names.push_back(std::string{fn->GetName()});
    }
  } else if (type == catalog::ObjectType::Sequence) {
    for (const auto& seq : snapshot->GetSequences(db, schema_name)) {
      names.push_back(std::string{seq->GetName()});
    }
  } else {
    for (const auto& tbl : snapshot->GetTables(db, schema_name)) {
      names.push_back(std::string{tbl->GetName()});
    }
  }

  for (const auto& name : names) {
    GrantObject(ctx, type, privileges, absl::StrCat(schema_name, ".", name),
                grantee, revoke, opts);
  }
}

void GrantRole(ConnectionContext& ctx, std::string_view role,
               std::string_view member, bool revoke,
               const MemberOptions& opts) {
  auto& catalog = GlobalCatalog();
  auto snapshot = FreshSnapshot();
  auto role_obj = snapshot->GetRole(role);
  auto member_obj = snapshot->GetRole(member);
  if (!role_obj) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", role, "\" does not exist"));
  }
  if (!member_obj) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", member, "\" does not exist"));
  }

  const ObjectId role_id = role_obj->GetId();
  const ObjectId member_id = member_obj->GetId();

  // PG: the grantor must be a superuser or hold ADMIN OPTION on `role`.
  if (!CurrentRole(ctx)->IsSuperuser() &&
      !auth::HasAdminOption(*snapshot, ctx.GetRoleId(), role_id)) {
    const auto verb = revoke ? "revoke" : "grant";
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to ", verb, " role \"", role, "\""),
      ERR_DETAIL("Only roles with the ADMIN option on role \"", role, "\" may ",
                 verb, " this role."));
  }

  const catalog::Membership edge{
    .role = role_id,
    .admin_option = opts.admin == 1,
    .inherit_option = opts.inherit == -1
                        ? member_obj->Has(catalog::RoleOption::Inherit)
                        : opts.inherit == 1,
    .set_option = opts.set != 0,
  };

  if (!revoke && role_id == member_id) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_GRANT_OPERATION),
      ERR_MSG("role \"", role, "\" is a member of role \"", member, "\""));
  }

  enum class Reject { None, RoleGone, Cycle };
  Reject reject = Reject::None;
  auto r = catalog.ChangeRole(
    member,
    [&](const catalog::Role& old_role,
        std::shared_ptr<catalog::Role>& new_role) -> Result {
      if (!revoke) {
        auto live = catalog.GetCatalogSnapshot();
        if (!live->GetObject<catalog::Role>(role_id)) {
          reject = Reject::RoleGone;
          return Result{ERROR_USER_NOT_FOUND};
        }
        if (auth::ComputeMembershipClosure(*live, role_id)
              .contains(member_id)) {
          reject = Reject::Cycle;
          return Result{ERROR_BAD_PARAMETER, "membership cycle"};
        }
      }
      new_role = std::static_pointer_cast<catalog::Role>(old_role.Clone());
      if (revoke && opts.admin_option_only) {
        auto edges = new_role->MemberOf();
        auto it = std::ranges::find(edges, role_id, &catalog::Membership::role);
        if (it != edges.end()) {
          catalog::Membership kept = *it;
          kept.admin_option = false;
          new_role->AddMembership(kept);
        }
      } else if (revoke) {
        new_role->RemoveMembership(role_id);
      } else {
        new_role->AddMembership(edge);
      }
      return {};
    });
  if (reject == Reject::RoleGone) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", role, "\" does not exist"));
  }
  if (reject == Reject::Cycle) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_GRANT_OPERATION),
      ERR_MSG("role \"", role, "\" is a member of role \"", member, "\""));
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
}

namespace {

catalog::ObjectType OwnerObjType(std::string_view word) {
  if (word == "TABLE") {
    return catalog::ObjectType::Table;
  }
  if (word == "VIEW") {
    return catalog::ObjectType::PgSqlView;
  }
  if (word == "SEQUENCE") {
    return catalog::ObjectType::Sequence;
  }
  if (word == "SCHEMA") {
    return catalog::ObjectType::Schema;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG("ALTER ", word, " ... OWNER TO is not supported"));
}

}  // namespace

void AlterOwner(ConnectionContext& ctx, std::string_view obj_type,
                std::string_view name, std::string_view new_owner) {
  const auto type = OwnerObjType(obj_type);
  auto& catalog = GlobalCatalog();
  auto snapshot = FreshSnapshot();

  auto current = CurrentRole(ctx);
  const bool is_superuser = current->IsSuperuser();
  const ObjectId current_id = current->GetId();

  std::string_view new_owner_name = new_owner;
  if (new_owner == "CURRENT_USER" || new_owner == "SESSION_USER" ||
      new_owner == "CURRENT_ROLE") {
    new_owner_name = ctx.user();
  }
  auto new_owner_role = snapshot->GetRole(new_owner_name);
  if (!new_owner_role) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", new_owner_name, "\" does not exist"));
  }
  const ObjectId new_owner_id = new_owner_role->GetId();

  std::string schema_name;
  std::string rel_name;
  ObjectId cur_owner;
  if (type == catalog::ObjectType::Schema) {
    auto schema = snapshot->GetSchema(ctx.GetDatabaseId(), name);
    if (!schema) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                      ERR_MSG("schema \"", name, "\" does not exist"));
    }
    rel_name = std::string{name};
    cur_owner = schema->GetOwner();
  } else {
    const auto parsed = ParseObjectName(name, ctx.GetCurrentSchema());
    auto rel =
      snapshot->GetRelation(catalog::NoAccessCheck(), ctx.GetDatabaseId(),
                            parsed.schema, parsed.relation);
    if (!rel) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                      ERR_MSG(ToPgObjectTypeName(type), " \"", parsed.relation,
                              "\" does not exist"));
    }
    schema_name = parsed.schema;
    rel_name = parsed.relation;
    cur_owner = rel->GetOwner();
  }
  SDB_ASSERT(cur_owner.isSet());
  const ObjectId real_owner = cur_owner;

  if (!is_superuser) {
    if (!auth::ComputeEffectiveRoles(*snapshot, current_id)
           .contains(real_owner)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("must be owner of ", ToPgObjectTypeName(type), " ", rel_name));
    }
    if (!auth::ComputeSetRoleClosure(*snapshot, current_id)
           .contains(new_owner_id)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("must be able to SET ROLE \"", new_owner_name, "\""));
    }
    // PG: the new owner must hold CREATE on the relation's containing schema
    // (a schema itself has no containing schema, so this applies only to
    // relations). A no-op transfer to the current owner skips the check.
    if (type != catalog::ObjectType::Schema && new_owner_id != real_owner) {
      auto schema = snapshot->GetSchema(ctx.GetDatabaseId(), schema_name);
      if (schema && !auth::HasPrivilege(*snapshot, new_owner_id, *schema,
                                        catalog::AclMode::Create)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        ERR_MSG("permission denied for schema ", schema_name));
      }
    }
  }

  auto r = catalog.ChangeOwner(ctx.GetDatabaseId(), schema_name, rel_name, type,
                               new_owner_id);
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
}

}  // namespace sdb::pg
