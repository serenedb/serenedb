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
#include <limits>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include "app/app_server.h"
#include "auth/acl.h"
#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/persistence/role.h"
#include "catalog/table.h"
#include "network/credentials.h"
#include "pg/errcodes.h"
#include "pg/pg_types.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"

namespace sdb::pg {
namespace {

// The access context every mutating catalog call runs under (the caller's role).
catalog::AccessContext Acting(ConnectionContext& ctx) {
  const ObjectId role = ctx.GetRoleId();
  return catalog::ActingAs(role);
}

auto FindAclItem(catalog::Acl& acl, ObjectId grantee, ObjectId grantor) {
  return std::ranges::find_if(acl, [&](const catalog::AclItem& item) {
    return item.grantee == grantee && item.grantor == grantor;
  });
}

catalog::AclMode AclDependentPrivs(catalog::AclView acl, ObjectId grantee,
                                   catalog::AclMode privs) {
  catalog::AclMode dependent = catalog::AclMode::NoRights;
  for (const auto& item : acl) {
    if (item.grantor == grantee) {
      dependent |= item.privs & privs;
    }
  }
  return dependent;
}

void AclRevokeCascade(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
                      catalog::AclMode privs) {
  std::vector<std::pair<ObjectId, catalog::AclMode>> work{{grantee, privs}};
  while (!work.empty()) {
    const auto [who, bits] = work.back();
    work.pop_back();
    for (const auto& item : acl) {
      if (item.grantor != who) {
        continue;
      }
      const catalog::AclMode dependent = item.privs & bits;
      if (dependent != catalog::AclMode::NoRights) {
        work.emplace_back(item.grantee, dependent);
      }
    }
    for (auto it = acl.begin(); it != acl.end();) {
      const bool top =
        it->grantee == grantee && it->grantor == grantor && who == grantee;
      if (it->grantor == who || top) {
        it->privs &= ~bits;
        it->grant_option &= ~bits;
        if (it->privs == catalog::AclMode::NoRights) {
          it = acl.erase(it);
          continue;
        }
      }
      ++it;
    }
  }
}

void AclGrant(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
              catalog::AclMode privs,
              catalog::AclMode grant_option = catalog::AclMode::NoRights) {
  if (auto it = FindAclItem(acl, grantee, grantor); it != acl.end()) {
    it->privs |= privs;
    it->grant_option |= (grant_option & privs);
    return;
  }
  acl.push_back(catalog::AclItem{
    .grantee = grantee,
    .grantor = grantor,
    .privs = privs,
    .grant_option = grant_option & privs,
  });
}

void AclRevoke(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
               catalog::AclMode privs) {
  auto it = FindAclItem(acl, grantee, grantor);
  if (it == acl.end()) {
    return;
  }
  it->privs &= ~privs;
  it->grant_option &= ~privs;
  if (it->privs == catalog::AclMode::NoRights) {
    acl.erase(it);
  }
}

void AclRemoveGrantOption(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
                          catalog::AclMode privs) {
  if (auto it = FindAclItem(acl, grantee, grantor); it != acl.end()) {
    it->grant_option &= ~privs;
  }
}

// REVOKE GRANT OPTION FOR ... CASCADE: drop the grantee's grant option (keeping
// the privilege itself) and cascade-revoke every grant that depended on it (the
// grants the grantee made, and their subtrees).
void AclRemoveGrantOptionCascade(catalog::Acl& acl, ObjectId grantee,
                                 ObjectId grantor, catalog::AclMode privs) {
  std::vector<std::pair<ObjectId, catalog::AclMode>> dependents;
  for (const auto& item : acl) {
    if (item.grantor == grantee) {
      const auto bits = item.privs & privs;
      if (bits != catalog::AclMode::NoRights) {
        dependents.emplace_back(item.grantee, bits);
      }
    }
  }
  for (const auto& [dep_grantee, dep_bits] : dependents) {
    AclRevokeCascade(acl, dep_grantee, grantee, dep_bits);
  }
  AclRemoveGrantOption(acl, grantee, grantor, privs);
}

catalog::Catalog& GlobalCatalog() { return catalog::GetCatalog(); }

std::shared_ptr<const catalog::Snapshot> FreshSnapshot() {
  return GlobalCatalog().GetCatalogSnapshot();
}

int32_t ParseConnLimit(bool has_conn_limit, int64_t value) {
  if (!has_conn_limit) {
    return catalog::Role::kNoConnLimit;
  }
  if (value < catalog::Role::kNoConnLimit ||
      value > std::numeric_limits<int32_t>::max()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("invalid connection limit: ", value));
  }
  return static_cast<int32_t>(value);
}

int64_t ValidUntilOrUnset(bool has_valid_until, int64_t micros) {
  return has_valid_until ? micros : catalog::Role::kNoValidUntil;
}

std::string MakePasswordVerifier(ConnectionContext& ctx, bool has_password,
                                 std::string_view password, bool is_null) {
  if (!has_password || is_null) {
    return {};
  }
  // PostgreSQL: `PASSWORD ''` is not a valid password -- it warns and clears
  // the password rather than storing a verifier for the empty string.
  if (password.empty()) {
    ctx.AddNotice(SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_WARNING),
      ERR_MSG("empty string is not a valid password, clearing password")));
    return {};
  }
  // A pre-hashed verifier (SCRAM or md5, from pg_dumpall / psql \password /
  // migrations) is stored verbatim -- re-hashing it would make the literal
  // string the password. Cleartext is hashed to a SCRAM verifier (the default).
  if (network::IsScramVerifier(password) || network::IsMd5Verifier(password)) {
    return std::string{password};
  }
  auto verifier = network::BuildScramVerifierString(password);
  if (!verifier) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("could not derive password verifier"));
  }
  return *verifier;
}

}  // namespace

void CreateRole(ConnectionContext& ctx, std::string_view name,
                const CreateRoleOptions& options) {
  // Reserved role names (PostgreSQL rejects these as reserved_name): the `pg_`
  // prefix is reserved for predefined roles, and `public`/`none` are reserved
  // pseudo-roles (PUBLIC = every role; NONE used by SET ROLE).
  if (absl::StartsWithIgnoreCase(name, "pg_")) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_RESERVED_NAME),
      ERR_MSG("role name \"", name, "\" is reserved"),
      ERR_DETAIL("Role names starting with \"pg_\" are reserved."));
  }
  if (absl::EqualsIgnoreCase(name, "public") ||
      absl::EqualsIgnoreCase(name, "none")) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_RESERVED_NAME),
                    ERR_MSG("role name \"", name, "\" is reserved"));
  }
  const int32_t conn_limit =
    ParseConnLimit(options.has_conn_limit, options.conn_limit);
  const int64_t valid_until =
    ValidUntilOrUnset(options.has_valid_until, options.valid_until);

  auto& catalog = GlobalCatalog();
  catalog::RoleOption opts = catalog::RoleOption::None;
  if (options.login) {
    opts |= catalog::RoleOption::Login;
  }
  if (options.superuser) {
    opts |= catalog::RoleOption::Superuser;
  }
  if (options.createdb) {
    opts |= catalog::RoleOption::CreateDb;
  }
  if (options.createrole) {
    opts |= catalog::RoleOption::CreateRole;
  }
  if (options.replication) {
    opts |= catalog::RoleOption::Replication;
  }
  if (options.bypassrls) {
    opts |= catalog::RoleOption::BypassRls;
  }
  if (options.inherit) {
    opts |= catalog::RoleOption::Inherit;
  }
  auto role = std::make_shared<catalog::Role>(catalog::persistence::RoleData{
    .name = std::string{name},
    .options = static_cast<uint32_t>(opts),
    .conn_limit = conn_limit,
    .valid_until = valid_until,
    .password_verifier = MakePasswordVerifier(
      ctx, options.has_password, options.password, options.password_is_null),
  });

  catalog.CreateRole(Acting(ctx), std::move(role));

  for (const auto& g : options.in_roles) {
    GrantRole(ctx, g, name, /*revoke=*/false, MemberOptions{});
  }
  for (const auto& m : options.role_members) {
    GrantRole(ctx, name, m, /*revoke=*/false, MemberOptions{});
  }
  for (const auto& a : options.admin_members) {
    GrantRole(ctx, name, a, /*revoke=*/false, MemberOptions{.admin = 1});
  }
}

void DropRole(ConnectionContext& ctx, std::string_view name, bool missing_ok) {
  auto& catalog = GlobalCatalog();
  if (!catalog.DropRole(Acting(ctx), name, missing_ok)) {
    ctx.AddNotice(
      SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                     ERR_MSG("role \"", name, "\" does not exist, skipping")));
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

void SyncIsSuperuser(ConnectionContext& conn) {
  const bool super =
    conn.CatalogSnapshot()->ClosureFor(conn.GetRoleId()).is_superuser;
  conn.SetSetting("is_superuser", super ? "on" : "off", /*is_local=*/false);
}

}  // namespace

void AlterRole(ConnectionContext& ctx, std::string_view name,
               const AlterRoleOptions& opts) {
  const int32_t conn_limit =
    ParseConnLimit(opts.has_conn_limit, opts.conn_limit);
  const int64_t valid_until =
    ValidUntilOrUnset(opts.has_valid_until, opts.valid_until);

  // Hash outside the mutate lambda (which runs under the catalog lock).
  const std::string verifier = MakePasswordVerifier(
    ctx, opts.has_password, opts.password, opts.password_is_null);

  auto& catalog = GlobalCatalog();
  catalog.ChangeRole(
    Acting(ctx), name, "alter",
    /*allow_self=*/false,
    [&](const catalog::Role& old_role,
        std::shared_ptr<catalog::Role>& new_role) {
      new_role = std::static_pointer_cast<catalog::Role>(old_role.Clone());
      catalog::RoleOption o = new_role->Options();
      o = SetBit(o, catalog::RoleOption::Login, opts.login);
      o = SetBit(o, catalog::RoleOption::Superuser, opts.superuser);
      o = SetBit(o, catalog::RoleOption::CreateDb, opts.createdb);
      o = SetBit(o, catalog::RoleOption::CreateRole, opts.createrole);
      o = SetBit(o, catalog::RoleOption::Replication, opts.replication);
      o = SetBit(o, catalog::RoleOption::BypassRls, opts.bypassrls);
      o = SetBit(o, catalog::RoleOption::Inherit, opts.inherit);
      new_role->SetOptions(o);
      if (opts.has_password) {
        new_role->SetPasswordVerifier(verifier);
      }
      if (opts.has_valid_until) {
        new_role->SetValidUntil(valid_until);
      }
      if (opts.has_conn_limit) {
        new_role->SetConnLimit(conn_limit);
      }
    });
}

void RenameRole(ConnectionContext& ctx, std::string_view name,
                std::string_view new_name) {
  auto& catalog = GlobalCatalog();
  // PostgreSQL refuses to rename the session or current user.
  if (auto target = FreshSnapshot()->GetRole(name)) {
    const auto target_id = target->GetId();
    if (target_id == ctx.GetSessionRoleId()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("session user cannot be renamed"));
    }
    if (target_id == ctx.GetRoleId()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("current user cannot be renamed"));
    }
  }
  catalog.ChangeRole(
    Acting(ctx), name, "rename",
    /*allow_self=*/false,
    [&](const catalog::Role& old_role,
        std::shared_ptr<catalog::Role>& new_role) {
      new_role = std::static_pointer_cast<catalog::Role>(old_role.Clone());
      new_role->SetName(new_name);
    });
}

std::string SetRole(ConnectionContext& conn, std::string_view name) {
  if (absl::EqualsIgnoreCase(name, "none")) {
    conn.SetEffectiveRole(conn.GetSessionRoleId());
    SyncIsSuperuser(conn);
    return "none";
  }
  auto snapshot = conn.CatalogSnapshot();
  auto target = snapshot->GetRole(name);
  if (!target) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("role \"", name, "\" does not exist"));
  }
  // SET ROLE is relative to the session role, not the (possibly already
  // switched) effective role: members-of via set_option edges, or superuser.
  const ObjectId session = conn.GetSessionRoleId();
  if (!snapshot->ClosureFor(session).is_superuser &&
      !auth::ComputeSetRoleClosure(*snapshot, session)
         .contains(target->GetId())) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied to set role \"", name, "\""));
  }
  conn.SetEffectiveRole(target->GetId());
  SyncIsSuperuser(conn);
  return std::string{target->GetName()};
}

void ResetRole(ConnectionContext& conn) {
  conn.SetEffectiveRole(conn.GetSessionRoleId());
  SyncIsSuperuser(conn);
}

std::string SetSessionAuthorization(ConnectionContext& conn,
                                    std::string_view name) {
  auto snapshot = conn.CatalogSnapshot();
  auto target = snapshot->GetRole(name);
  if (!target) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("role \"", name, "\" does not exist"));
  }
  const bool login_super =
    snapshot->ClosureFor(conn.GetLoginRoleId()).is_superuser;
  if (!login_super && target->GetId() != conn.GetLoginRoleId()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to set session authorization \"", name, "\""));
  }
  conn.SetSessionRole(target->GetId());
  conn.SetSetting("role", "none", /*is_local=*/false);
  SyncIsSuperuser(conn);
  return std::string{target->GetName()};
}

void ResetSessionAuthorization(ConnectionContext& conn) {
  conn.ResetIdentity();
  conn.SetSetting("role", "none", /*is_local=*/false);
  SyncIsSuperuser(conn);
}

void AlterRoleConfig(ConnectionContext& ctx, std::string_view name,
                     std::string_view op, std::string_view setting,
                     std::string_view value) {
  const bool is_self = name == ctx.user();

  auto& catalog = GlobalCatalog();
  catalog.ChangeRole(
    Acting(ctx), name, "alter",
    /*allow_self=*/is_self,
    [&](const catalog::Role& old_role,
        std::shared_ptr<catalog::Role>& new_role) {
      new_role = std::static_pointer_cast<catalog::Role>(old_role.Clone());
      if (op == "RESET_ALL") {
        new_role->ResetAllConfig();
      } else if (op == "RESET") {
        new_role->ResetConfig(setting);
      } else {
        new_role->SetConfig(setting, value);
      }
    });
}

namespace {

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
  return catalog::ObjectType::Table;
}

catalog::AclMode ParseAclModeOrThrow(std::span<const ParsedPriv> privileges,
                                     catalog::ObjectType type) {
  const std::string_view object_word =
    type == catalog::ObjectType::Table ? "relation" : ToPgObjectTypeName(type);
  catalog::AclMode out = catalog::AclMode::NoRights;
  for (const auto& p : privileges) {
    auto parsed = auth::TryParseAclKeyword(p.keyword, type);
    if (!parsed) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_GRANT_OPERATION),
        ERR_MSG("invalid privilege type ", absl::AsciiStrToUpper(p.keyword),
                " for ", object_word));
    }
    out |= *parsed;
  }
  return out;
}

bool AnyColumnPrivs(std::span<const ParsedPriv> parsed) {
  return std::ranges::any_of(
    parsed, [](const ParsedPriv& p) { return !p.columns.empty(); });
}

// OWNER TO / REASSIGN OWNED TO role spec: the parser uppercases the keyword
// alternatives, identifiers arrive lower-folded. CURRENT_USER / CURRENT_ROLE
// resolve to the effective role, SESSION_USER to the session role.
std::string ResolveRoleSpecName(ConnectionContext& ctx,
                                const catalog::Snapshot& snap,
                                std::string_view spec) {
  ObjectId id;
  if (spec == "CURRENT_USER" || spec == "CURRENT_ROLE") {
    id = ctx.GetRoleId();
  } else if (spec == "SESSION_USER") {
    id = ctx.GetSessionRoleId();
  } else {
    return std::string{spec};
  }
  auto role = snap.GetObject<catalog::Role>(id);
  SDB_ASSERT(role);
  return std::string{role->GetName()};
}

ObjectId ResolveGranteeId(ConnectionContext& ctx, const catalog::Snapshot& snap,
                          std::string_view grantee) {
  if (grantee == "PUBLIC" || grantee == "public") {
    return catalog::kPublicGrantee;
  }
  // Exact match only: the parser lower-folds the unquoted keywords, and a
  // quoted mixed-case role name (e.g. "Current_User") must stay a role name.
  if (grantee == "CURRENT_USER" || grantee == "current_user" ||
      grantee == "CURRENT_ROLE" || grantee == "current_role") {
    return ctx.GetRoleId();
  }
  if (grantee == "SESSION_USER" || grantee == "session_user") {
    return ctx.GetSessionRoleId();
  }
  auto grantee_role = snap.GetRole(grantee);
  if (!grantee_role) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", grantee, "\" does not exist"));
  }
  return grantee_role->GetId();
}

ObjectId ResolveGrantedBy(const catalog::Snapshot& snap,
                          std::string_view granted_by) {
  if (granted_by.empty()) {
    return id::kInvalid;
  }
  auto gb = snap.GetRole(granted_by);
  if (!gb) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", granted_by, "\" does not exist"));
  }
  return gb->GetId();
}

void ApplyAclChange(catalog::Acl& acl, ObjectId grantee, ObjectId grantor,
                    catalog::AclMode privs, bool revoke, bool with_grant_option,
                    bool grant_option_only, bool cascade) {
  const auto grant_option =
    with_grant_option ? privs : catalog::AclMode::NoRights;
  if (!revoke) {
    AclGrant(acl, grantee, grantor, privs, grant_option);
  } else if (grant_option_only) {
    AclRemoveGrantOption(acl, grantee, grantor, privs);
  } else if (cascade) {
    AclRevokeCascade(acl, grantee, grantor, privs);
  } else {
    AclRevoke(acl, grantee, grantor, privs);
  }
}

}  // namespace

void AlterDefaultPrivileges(ConnectionContext& ctx,
                            std::span<const ParsedPriv> privileges,
                            std::string_view objtype_char,
                            std::string_view grantee, bool revoke,
                            const DefaultPrivilegesOptions& opts) {
  auto& catalog = GlobalCatalog();
  auto snapshot = FreshSnapshot();

  const std::string_view defacl_role_name =
    opts.for_role.empty() ? ctx.user() : opts.for_role;
  auto defacl_role = snapshot->GetRole(defacl_role_name);
  if (!defacl_role) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", defacl_role_name, "\" does not exist"));
  }
  const ObjectId defacl_role_id = defacl_role->GetId();

  const ObjectId grantee_id = ResolveGranteeId(ctx, *snapshot, grantee);

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

  catalog.ChangeDefaultAcl(Acting(ctx), defacl_role_name,
                           schema_id, objtype_c, type, [&](catalog::Acl& acl) {
                             ApplyAclChange(
                               acl, grantee_id, defacl_role_id, privs, revoke,
                               opts.with_grant_option, opts.grant_option_only,
                               opts.cascade);
                           });
}

namespace {

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
  const std::string current_schema = ctx.GetCurrentSchema();
  const auto parsed = ParseObjectName(raw_name, current_schema);
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

}  // namespace
namespace {

struct AclGrantContext {
  catalog::AclMode privs;
  ObjectId grantee_id;
  ObjectId current_id;
  ObjectId granted_by_id;
  bool revoke;
  const GrantObjectOptions& opts;
  bool* no_authority;
  bool* nothing_applied;
  bool* dependents_block;
  bool* not_member;
};

void ApplyAclGrant(const catalog::Snapshot& live, ObjectId owner,
                   catalog::Acl& acl, const AclGrantContext& gc) {
  const auto& rc = live.ClosureFor(gc.current_id);
  const bool is_superuser = rc.is_superuser;
  if (gc.granted_by_id.isSet() && !is_superuser &&
      !auth::ComputeMembershipClosure(live, gc.current_id)
         .contains(gc.granted_by_id)) {
    *gc.not_member = true;
    return;
  }
  const bool is_owner = rc.Owns(owner);
  const ObjectId grantor = gc.granted_by_id.isSet()
                             ? gc.granted_by_id
                             : (is_owner ? owner : gc.current_id);
  catalog::AclMode allowed = gc.privs;
  if (!is_owner) {
    allowed &= rc.GrantableModes(acl);
  }
  if (allowed == catalog::AclMode::NoRights) {
    if (!is_owner && rc.HeldModes(acl) == catalog::AclMode::NoRights) {
      *gc.no_authority = true;
    } else {
      *gc.nothing_applied = true;
    }
    return;
  }
  if (!gc.revoke) {
    // PUBLIC cannot act as a grantor, so it cannot hold a grant option
    // (PostgreSQL rejects `GRANT ... TO PUBLIC WITH GRANT OPTION`).
    if (gc.opts.with_grant_option && gc.grantee_id == catalog::kPublicGrantee) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_GRANT_OPERATION),
                      ERR_MSG("grant options can only be granted to roles"));
    }
    const auto grant_option =
      gc.opts.with_grant_option ? allowed : catalog::AclMode::NoRights;
    AclGrant(acl, gc.grantee_id, grantor, allowed, grant_option);
  } else if (!gc.opts.cascade &&
             AclDependentPrivs(acl, gc.grantee_id, allowed) !=
               catalog::AclMode::NoRights) {
    // Dependent grants (ones the grantee made) block RESTRICT (the default);
    // CASCADE removes them.
    *gc.dependents_block = true;
  } else if (gc.opts.grant_option_only) {
    if (gc.opts.cascade) {
      AclRemoveGrantOptionCascade(acl, gc.grantee_id, grantor, allowed);
    } else {
      AclRemoveGrantOption(acl, gc.grantee_id, grantor, allowed);
    }
  } else if (gc.opts.cascade) {
    AclRevokeCascade(acl, gc.grantee_id, grantor, allowed);
  } else {
    AclRevoke(acl, gc.grantee_id, grantor, allowed);
  }
}

void GrantObjectColumns(ConnectionContext& ctx, catalog::ObjectType type,
                        std::span<const ParsedPriv> parsed,
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
  const ObjectId current_id = ctx.GetRoleId();
  const ObjectId grantee_id = ResolveGranteeId(ctx, *snapshot, grantee);

  const ObjectId granted_by_id = ResolveGrantedBy(*snapshot, opts.granted_by);

  bool no_authority = false;
  bool nothing_applied = false;
  bool dependents_block = false;
  bool not_member = false;
  constexpr catalog::AclMode kColumnPrivs =
    catalog::AclMode::Select | catalog::AclMode::Insert |
    catalog::AclMode::Update | catalog::AclMode::References;
  for (const auto& p : parsed) {
    catalog::AclMode privs =
      auth::TryParseAclKeyword(p.keyword, catalog::ObjectType::Table)
        .value_or(catalog::AclMode::NoRights);
    const bool is_all = absl::EqualsIgnoreCase(p.keyword, "ALL");
    if (!is_all && (privs & ~kColumnPrivs) != catalog::AclMode::NoRights) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_GRANT_OPERATION),
                      ERR_MSG("invalid privilege type ",
                              absl::AsciiStrToUpper(p.keyword), " for column"));
    }
    privs &= kColumnPrivs;
    for (const auto& column : p.columns) {
      catalog.ChangeColumnAcl(
        ctx.GetDatabaseId(), schema_name, rel_name, column,
        [&](const catalog::Snapshot& live, ObjectId owner, catalog::Acl& acl) {
          ApplyAclGrant(
            live, owner, acl,
            {privs, grantee_id, current_id, granted_by_id, revoke, opts,
             &no_authority, &nothing_applied, &dependents_block, &not_member});
        });
      if (not_member) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
          ERR_MSG("must be member of role \"", opts.granted_by, "\""));
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
                 std::span<const ParsedPriv> privileges,
                 std::string_view obj_name, std::string_view grantee,
                 bool revoke, const GrantObjectOptions& opts) {
  if (AnyColumnPrivs(privileges)) {
    GrantObjectColumns(ctx, type, privileges, obj_name, grantee, revoke, opts);
    return;
  }

  auto& catalog = GlobalCatalog();
  auto snapshot = FreshSnapshot();

  std::string schema_name;
  std::string rel_name;
  auto target =
    ResolveGrantTarget(ctx, *snapshot, type, obj_name, schema_name, rel_name);
  if (!target) {
    if (type == catalog::ObjectType::PgSqlType &&
        RegtypeIn(rel_name) != kInvalidOid) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("cannot change privileges of built-in type ", rel_name));
    }
    const bool is_relation = type == catalog::ObjectType::Table ||
                             type == catalog::ObjectType::PgSqlView ||
                             type == catalog::ObjectType::Sequence;
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG(is_relation ? "relation" : ToPgObjectTypeName(type),
                            " \"", rel_name, "\" does not exist"));
  }

  const ObjectId current_id = ctx.GetRoleId();

  const ObjectId grantee_id = ResolveGranteeId(ctx, *snapshot, grantee);

  const ObjectId granted_by_id = ResolveGrantedBy(*snapshot, opts.granted_by);

  const catalog::AclMode privs = ParseAclModeOrThrow(privileges, type);

  bool no_authority = false;
  bool nothing_applied = false;
  bool dependents_block = false;
  bool not_member = false;
  const ObjectId acl_database_id = type == catalog::ObjectType::Database
                                     ? target->GetId()
                                     : ctx.GetDatabaseId();
  catalog.ChangeAcl(
    acl_database_id, schema_name, rel_name, type,
    [&](const catalog::Snapshot& live, ObjectId owner, catalog::Acl& acl) {
      ApplyAclGrant(
        live, owner, acl,
        {privs, grantee_id, current_id, granted_by_id, revoke, opts,
         &no_authority, &nothing_applied, &dependents_block, &not_member});
    });
  if (not_member) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("must be member of role \"", opts.granted_by, "\""));
  }
  if (dependents_block) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    ERR_MSG("dependent privileges exist"),
                    ERR_HINT("Use CASCADE to revoke them too."));
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

  if (revoke && type == catalog::ObjectType::Table) {
    auto fresh = FreshSnapshot();
    if (auto tbl = fresh->GetObject<catalog::Table>(target->GetId())) {
      for (const auto& col : tbl->Columns()) {
        if (col.GetId() == catalog::Column::kGeneratedPKId ||
            col.GetAcl().empty()) {
          continue;
        }
        catalog.ChangeColumnAcl(
          ctx.GetDatabaseId(), schema_name, rel_name, col.GetName(),
          [&](const catalog::Snapshot&, ObjectId owner, catalog::Acl& acl) {
            AclRevoke(acl, grantee_id, owner, privs);
          });
      }
    }
  }
}

void GrantObjectAllInSchema(ConnectionContext& ctx, catalog::ObjectType type,
                            std::span<const ParsedPriv> privileges,
                            std::string_view schema_name,
                            std::string_view grantee, bool revoke,
                            const GrantObjectOptions& opts) {
  auto snapshot = FreshSnapshot();
  if (!snapshot->GetSchema(ctx.GetDatabaseId(), schema_name)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                    ERR_MSG("schema \"", schema_name, "\" does not exist"));
  }

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

  const catalog::Membership edge{
    .role = role_id,
    .admin_option = opts.admin == 1,
    .inherit_option = opts.inherit == -1
                        ? member_obj->Has(catalog::RoleOption::Inherit)
                        : opts.inherit == 1,
    .set_option = opts.set != 0,
  };

  const ObjectId granted_by_id = ResolveGrantedBy(*snapshot, opts.granted_by);

  catalog.ChangeMembership(Acting(ctx), role_id, role,
                           member_id, member, edge, revoke,
                           opts.admin_option_only, granted_by_id);
}

void AlterOwner(ConnectionContext& ctx, std::string_view obj_type,
                std::string_view name, std::string_view new_owner) {
  const auto type = FromPgObjectTypeName(obj_type);
  SDB_ASSERT(type != catalog::ObjectType::Invalid);
  auto& catalog = GlobalCatalog();
  auto snapshot = FreshSnapshot();

  const ObjectId current_id = ctx.GetRoleId();

  const std::string new_owner_name =
    ResolveRoleSpecName(ctx, *snapshot, new_owner);
  auto new_owner_role = snapshot->GetRole(new_owner_name);
  if (!new_owner_role) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", new_owner_name, "\" does not exist"));
  }
  const ObjectId new_owner_id = new_owner_role->GetId();

  std::string schema_name;
  std::string rel_name;
  ObjectId target_database_id = ctx.GetDatabaseId();
  if (type == catalog::ObjectType::Schema) {
    rel_name = std::string{name};
  } else if (type == catalog::ObjectType::Database) {
    // The target is the named database, possibly not the connected one.
    rel_name = std::string{name};
    auto target_db = snapshot->GetDatabase(name);
    if (!target_db) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                      ERR_MSG("database \"", name, "\" does not exist"));
    }
    target_database_id = target_db->GetId();
  } else {
    const std::string current_schema = ctx.GetCurrentSchema();
    const auto parsed = ParseObjectName(name, current_schema);
    schema_name = parsed.schema;
    rel_name = parsed.relation;
  }
  catalog.ChangeOwner(catalog::ActingAs(current_id), target_database_id,
                      schema_name, rel_name, type, new_owner_id,
                      new_owner_name);
}

void ReassignOwned(ConnectionContext& ctx,
                   std::span<const std::string> from_roles,
                   std::string_view to_role) {
  GlobalCatalog().ReassignOwned(
    Acting(ctx), from_roles,
    ResolveRoleSpecName(ctx, *FreshSnapshot(), to_role));
}

void DropOwned(ConnectionContext& ctx, std::span<const std::string> from_roles,
               bool cascade) {
  GlobalCatalog().DropOwned(Acting(ctx), from_roles,
                            cascade);
}

// LOCK [TABLE] name,... IN <mode> MODE. serened's MVCC snapshots make the lock
// advisory, so this validates like PG (each relation must exist and the caller
// must hold the mode-appropriate privilege) and otherwise no-ops. ACCESS SHARE
// needs any of SELECT/INSERT/UPDATE/DELETE/TRUNCATE/MAINTAIN; every stronger
// mode needs one of UPDATE/DELETE/TRUNCATE/MAINTAIN. Superuser and owner
// bypass.
void LockTable(ConnectionContext& ctx, std::span<const std::string> names,
               std::string_view mode) {
  using catalog::AclMode;
  const AclMode required =
    mode == "ACCESS SHARE"
      ? (AclMode::Select | AclMode::Insert | AclMode::Update | AclMode::Delete |
         AclMode::Truncate | AclMode::Maintain)
      : (AclMode::Update | AclMode::Delete | AclMode::Truncate |
         AclMode::Maintain);

  auto snapshot = FreshSnapshot();
  const auto& rc = snapshot->ClosureFor(ctx.GetRoleId());
  const std::string current_schema = ctx.GetCurrentSchema();
  for (const auto& raw : names) {
    const auto parsed = ParseObjectName(raw, current_schema);
    auto table =
      snapshot->GetRelation(catalog::NoAccessCheck(), ctx.GetDatabaseId(),
                            parsed.schema, parsed.relation);
    if (!table || table->GetType() != catalog::ObjectType::Table) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_TABLE),
        ERR_MSG("relation \"", parsed.relation, "\" does not exist"));
    }
    if (rc.is_superuser || rc.Owns(*table) ||
        (rc.HeldModes(table->GetAcl()) & required) != AclMode::NoRights) {
      continue;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied for table ", parsed.relation));
  }
}

namespace {

catalog::persistence::PolicyCommand ParsePolicyCommand(std::string_view cmd) {
  using PC = catalog::persistence::PolicyCommand;
  if (cmd == "SELECT") {
    return PC::Select;
  }
  if (cmd == "INSERT") {
    return PC::Insert;
  }
  if (cmd == "UPDATE") {
    return PC::Update;
  }
  if (cmd == "DELETE") {
    return PC::Delete;
  }
  return PC::All;
}

// Resolve policy role names to ids. PUBLIC (or an empty list) -> empty vector,
// which the enforcement path treats as "applies to every role".
std::vector<ObjectId> ResolvePolicyRoles(
  ConnectionContext& ctx, const catalog::Snapshot& snap,
  std::span<const std::string> roles) {
  std::vector<ObjectId> out;
  for (const auto& role : roles) {
    auto id = ResolveGranteeId(ctx, snap, role);
    if (id == catalog::kPublicGrantee) {
      return {};
    }
    out.push_back(id);
  }
  return out;
}

}  // namespace

void CreatePolicy(ConnectionContext& ctx, std::string_view name,
                  std::string_view table, const CreatePolicyOptions& opts) {
  auto snapshot = FreshSnapshot();
  const std::string current_schema = ctx.GetCurrentSchema();
  const auto parsed = ParseObjectName(table, current_schema);

  catalog::persistence::PolicyData data;
  data.name = std::string{name};
  data.command = ParsePolicyCommand(opts.cmd);
  data.permissive = opts.permissive;
  data.roles = ResolvePolicyRoles(ctx, *snapshot, opts.roles);
  data.has_using = opts.has_using;
  data.using_text = opts.using_text;
  data.has_check = opts.has_check;
  data.check_text = opts.check_text;

  GlobalCatalog().CreatePolicy(Acting(ctx), ctx.GetDatabaseId(), parsed.schema,
                               parsed.relation, std::move(data));
}

void AlterPolicy(ConnectionContext& ctx, std::string_view name,
                 std::string_view table, const AlterPolicyOptions& opts) {
  const std::string current_schema = ctx.GetCurrentSchema();
  const auto parsed = ParseObjectName(table, current_schema);

  std::vector<ObjectId> roles;
  if (opts.has_roles) {
    roles = ResolvePolicyRoles(ctx, *FreshSnapshot(), opts.roles);
  }
  GlobalCatalog().AlterPolicy(
    Acting(ctx), ctx.GetDatabaseId(), parsed.schema, parsed.relation, name,
    opts.is_rename ? opts.new_name : std::string_view{}, opts.has_roles,
    std::move(roles), opts.has_using, opts.using_text, opts.has_check,
    opts.check_text);
}

void DropPolicy(ConnectionContext& ctx, std::string_view name,
                std::string_view table, bool if_exists) {
  const std::string current_schema = ctx.GetCurrentSchema();
  const auto parsed = ParseObjectName(table, current_schema);
  GlobalCatalog().DropPolicy(Acting(ctx), ctx.GetDatabaseId(), parsed.schema,
                             parsed.relation, name, if_exists);
}

void SetTableRowSecurity(ConnectionContext& ctx, std::string_view table,
                         std::string_view action) {
  const std::string current_schema = ctx.GetCurrentSchema();
  const auto parsed = ParseObjectName(table, current_schema);
  std::optional<bool> enabled;
  std::optional<bool> forced;
  if (action == "ENABLE") {
    enabled = true;
  } else if (action == "DISABLE") {
    enabled = false;
  } else if (action == "FORCE") {
    forced = true;
  } else if (action == "NOFORCE") {
    forced = false;
  }
  GlobalCatalog().SetRowSecurity(Acting(ctx), ctx.GetDatabaseId(),
                                 parsed.schema, parsed.relation, enabled,
                                 forced);
}

}  // namespace sdb::pg
