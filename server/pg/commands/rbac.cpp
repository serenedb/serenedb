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
#include "catalog/duckdb_catalog.h"
#include "catalog/duckdb_catalog_sets.h"
#include "catalog/duckdb_object_entry.h"
#include "catalog/duckdb_table_entry.h"
#include "catalog/duckdb_view_entry.h"
#include "catalog/persistence/role.h"
#include "catalog/table.h"
#include "network/credentials.h"
#include "pg/errcodes.h"
#include "pg/pg_types.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"

namespace sdb::pg {
namespace {

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

catalog::Catalog& GlobalCatalog() { return catalog::GetCatalog(); }

// The version of `schema_id` this transaction sees, refused as concurrently
// dropped when another one has taken it.
catalog::SchemaRef RequireSchema(duckdb::ClientContext* context,
                                 ObjectId schema_id,
                                 catalog::Permissions* perm) {
  auto schema = catalog::FindSchema(context, schema_id, perm);
  if (!schema) [[unlikely]] {
    catalog::ThrowConcurrentlyDropped(schema_id);
  }
  return schema;
}

// GRANT / REVOKE ON SCHEMA and ALTER SCHEMA ... OWNER TO. A schema is one of
// the hand-written puts -- the entry owns its contents' sets -- so the rewrite
// goes through PutSchema rather than through the entry-kind machinery.
void ChangeSchemaAcl(const catalog::AccessContext& ax, ObjectId schema_id,
                     auth::AclMutator mutate) {
  catalog::Permissions schema_perm;
  auto schema = RequireSchema(ax.context, schema_id, &schema_perm);
  auto perm =
    auth::MutatedAcl(schema_perm, duckdb::CatalogType::SCHEMA_ENTRY, mutate);
  const auto name = std::string{catalog::SchemaNameOf(*schema)};
  catalog::PutSchema(ax.context, name, std::move(schema), std::move(perm));
}

void ChangeSchemaOwner(const catalog::AccessContext& ax, ObjectId schema_id,
                       ObjectId new_owner, std::string_view new_owner_name) {
  catalog::Permissions schema_perm;
  auto schema = RequireSchema(ax.context, schema_id, &schema_perm);
  // A schema has no schema above it, so the parent check has nothing to
  // resolve.
  catalog::RequireOwnerTransfer(ax, ObjectId{}, schema_perm, new_owner,
                                new_owner_name, "schema",
                                catalog::SchemaNameOf(*schema));
  auto perm = auth::TransferredOwner(schema_perm, new_owner);
  const auto name = std::string{catalog::SchemaNameOf(*schema)};
  catalog::PutSchema(ax.context, name, std::move(schema), std::move(perm));
}

// GRANT / REVOKE on one table. A table's entry is the object, so this is a
// rewrite of the version the statement resolved -- re-read here because the
// mutation scope was taken after that resolution.
void ChangeTableAcl(const catalog::AccessContext& ax,
                    const duckdb::CreateTableInfo& table,
                    duckdb::CatalogType type, auth::AclMutator mutate) {
  const auto* current = catalog::Find<catalog::SereneDBTableEntry>(
    ax.context, catalog::ParentIdOf(table), catalog::IdOf(table));
  if (current == nullptr) {
    catalog::ThrowConcurrentlyDropped(duckdb::CatalogType::TABLE_ENTRY,
                                      catalog::TableNameOf(table));
  }
  catalog::PutEntry(ax.context, current->name.GetIdentifierName(),
                    current->Definition(),
                    auth::MutatedAcl(current->permissions, type, mutate));
}

// The same for one column's grants, which ride the table's definition.
// Returns the new version, so a caller changing several columns feeds each
// result into the next call instead of re-resolving the name.
catalog::TableInfoRef ChangeColumnAcl(const catalog::AccessContext& ax,
                                      const duckdb::CreateTableInfo& table,
                                      std::string_view column,
                                      auth::AclMutator mutate) {
  const auto schema_id = catalog::ParentIdOf(table);
  const auto table_id = catalog::IdOf(table);
  const auto* entry =
    catalog::Find<catalog::SereneDBTableEntry>(ax.context, schema_id, table_id);
  if (entry == nullptr) {
    catalog::ThrowConcurrentlyDropped(duckdb::CatalogType::TABLE_ENTRY,
                                      catalog::TableNameOf(table));
  }
  const auto current = entry->Definition();
  const auto* definition = catalog::ColumnByName(*current, column);
  if (definition == nullptr) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
      ERR_MSG("column \"", column, "\" of relation \"",
              catalog::TableNameOf(*current), "\" does not exist"));
  }
  const ObjectId column_id{definition->CatalogOid()};
  // A grant is not a change to what the table is, so the definition is
  // republished unchanged and only the permissions beside it move.
  auto perm = entry->permissions;
  const ObjectId owner = catalog::OwnerOf(perm);
  catalog::Acl acl{catalog::ColumnAclOf(perm.column_acl, column_id).begin(),
                   catalog::ColumnAclOf(perm.column_acl, column_id).end()};
  mutate(owner, acl);
  catalog::SetColumnAcl(perm.column_acl, column_id, std::move(acl));
  catalog::PutEntry(ax.context, catalog::TableNameOf(*current), current,
                    std::move(perm));
  return current;
}

int32_t ParseConnLimit(bool has_conn_limit, int64_t value) {
  if (!has_conn_limit) {
    return catalog::CreateRoleInfo::kNoConnLimit;
  }
  if (value < catalog::CreateRoleInfo::kNoConnLimit ||
      value > std::numeric_limits<int32_t>::max()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("invalid connection limit: ", value));
  }
  return static_cast<int32_t>(value);
}

int64_t ValidUntilOrUnset(bool has_valid_until, int64_t micros) {
  return has_valid_until ? micros : catalog::CreateRoleInfo::kNoValidUntil;
}

std::string MakePasswordVerifier(bool has_password, std::string_view password,
                                 bool is_null) {
  if (!has_password || is_null) {
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
  auto role = std::make_shared<catalog::CreateRoleInfo>(
    ObjectId{},
    catalog::persistence::RoleData{
      .name = std::string{name},
      .options = static_cast<uint32_t>(opts),
      .conn_limit = conn_limit,
      .valid_until = valid_until,
      .password_verifier = {MakePasswordVerifier(
        options.has_password, options.password, options.password_is_null)},
    });

  catalog.CreateRole(catalog::ActingAs(ctx.GetRoleId(), ctx.GetClientContext()),
                     std::move(role));

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
  if (!catalog.DropRole(
        catalog::ActingAs(ctx.GetRoleId(), ctx.GetClientContext()), name,
        missing_ok)) {
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
    auth::ClosureFor(&conn.GetClientContext(), conn.GetRoleId())->is_superuser;
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
    opts.has_password, opts.password, opts.password_is_null);

  auto& catalog = GlobalCatalog();
  catalog.ChangeRole(
    catalog::ActingAs(ctx.GetRoleId(), ctx.GetClientContext()), name, "alter",
    /*allow_self=*/false,
    [opts, verifier, conn_limit, valid_until](
      const catalog::CreateRoleInfo& old_role,
      std::shared_ptr<catalog::CreateRoleInfo>& new_role) {
      new_role = old_role.CloneRole();
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
  catalog.ChangeRole(catalog::ActingAs(ctx.GetRoleId(), ctx.GetClientContext()),
                     name, "rename",
                     /*allow_self=*/false,
                     [new_name = std::string{new_name}](
                       const catalog::CreateRoleInfo& old_role,
                       std::shared_ptr<catalog::CreateRoleInfo>& new_role) {
                       new_role = old_role.CloneRole();
                       new_role->SetRoleName(new_name);
                     });
}

std::string SetRole(ConnectionContext& conn, std::string_view name) {
  if (absl::EqualsIgnoreCase(name, "none")) {
    conn.SetEffectiveRole(conn.GetSessionRoleId());
    SyncIsSuperuser(conn);
    return "none";
  }
  auto* client = &conn.GetClientContext();
  auto target = catalog::FindRole(client, name);
  if (!target) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("role \"", name, "\" does not exist"));
  }
  // SET ROLE is relative to the session role, not the (possibly already
  // switched) effective role: members-of via set_option edges, or superuser.
  const ObjectId session = conn.GetSessionRoleId();
  if (!auth::ClosureFor(client, session)->is_superuser &&
      !auth::ComputeSetRoleClosure(*auth::RolesOf(client), session)
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
  auto* client = &conn.GetClientContext();
  auto target = catalog::FindRole(client, name);
  if (!target) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("role \"", name, "\" does not exist"));
  }
  const bool login_super =
    auth::ClosureFor(client, conn.GetLoginRoleId())->is_superuser;
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
  catalog.ChangeRole(catalog::ActingAs(ctx.GetRoleId(), ctx.GetClientContext()),
                     name, "alter",
                     /*allow_self=*/is_self,
                     [op = std::string{op}, setting = std::string{setting},
                      value = std::string{value}](
                       const catalog::CreateRoleInfo& old_role,
                       std::shared_ptr<catalog::CreateRoleInfo>& new_role) {
                       new_role = old_role.CloneRole();
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

duckdb::CatalogType DefaultAclObjType(std::string_view objtype_char) {
  if (objtype_char == "S") {
    return duckdb::CatalogType::SEQUENCE_ENTRY;
  }
  if (objtype_char == "f") {
    return duckdb::CatalogType::MACRO_ENTRY;
  }
  if (objtype_char == "T") {
    return duckdb::CatalogType::TYPE_ENTRY;
  }
  if (objtype_char == "n") {
    return duckdb::CatalogType::SCHEMA_ENTRY;
  }
  return duckdb::CatalogType::TABLE_ENTRY;
}

catalog::AclMode ParseAclModeOrThrow(std::span<const ParsedPriv> privileges,
                                     duckdb::CatalogType type) {
  const std::string_view object_word = type == duckdb::CatalogType::TABLE_ENTRY
                                         ? "relation"
                                         : ToPgObjectTypeName(type);
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

ObjectId ResolveGranteeId(duckdb::ClientContext& context,
                          std::string_view grantee) {
  if (grantee == "PUBLIC" || grantee == "public") {
    return catalog::kPublicGrantee;
  }
  auto grantee_role = catalog::FindRole(&context, grantee);
  if (!grantee_role) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", grantee, "\" does not exist"));
  }
  return grantee_role->GetId();
}

ObjectId ResolveGrantedBy(duckdb::ClientContext& context,
                          std::string_view granted_by) {
  if (granted_by.empty()) {
    return id::kInvalid;
  }
  auto gb = catalog::FindRole(&context, granted_by);
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

  // Without FOR ROLE postgres sets the default privileges of GetUserId() -- the
  // current role, which SET ROLE moves -- and not of the session's login user.
  const std::string current_role = ctx.EffectiveUserName();
  const std::string_view defacl_role_name =
    opts.for_role.empty() ? current_role : opts.for_role;
  auto defacl_role =
    catalog::FindRole(&ctx.GetClientContext(), defacl_role_name);
  if (!defacl_role) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", defacl_role_name, "\" does not exist"));
  }
  const ObjectId defacl_role_id = defacl_role->GetId();

  const ObjectId grantee_id = ResolveGranteeId(ctx.GetClientContext(), grantee);

  ObjectId schema_id = id::kInvalid;
  if (!opts.in_schema.empty()) {
    auto schema = catalog::FindSchema(&ctx.GetClientContext(),
                                      ctx.GetDatabaseId(), opts.in_schema);
    if (!schema) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
        ERR_MSG("schema \"", opts.in_schema, "\" does not exist"));
    }
    schema_id = catalog::IdOf(*schema);
  }

  const auto type = DefaultAclObjType(objtype_char);
  const char objtype_c = objtype_char.empty() ? 'r' : objtype_char.front();

  const catalog::AclMode privs = ParseAclModeOrThrow(privileges, type);

  catalog.ChangeDefaultAcl(
    catalog::ActingAs(ctx.GetRoleId(), ctx.GetClientContext()),
    defacl_role_name, schema_id, objtype_c, type,
    [grantee_id, defacl_role_id, privs, revoke,
     with_grant_option = opts.with_grant_option,
     grant_option_only = opts.grant_option_only,
     cascade = opts.cascade](catalog::Acl& acl) {
      ApplyAclChange(acl, grantee_id, defacl_role_id, privs, revoke,
                     with_grant_option, grant_option_only, cascade);
    });
}

namespace {

// Whether one of the kinds whose entry is the object holds `name` in `schema`.
bool EntryExists(ConnectionContext& ctx, duckdb::CatalogType type,
                 ObjectId schema_id, std::string_view name) {
  auto* context = &ctx.GetClientContext();
  switch (type) {
    case duckdb::CatalogType::TYPE_ENTRY:
      return catalog::Find<catalog::SereneDBTypeEntry>(context, schema_id,
                                                       name) != nullptr;
    case duckdb::CatalogType::MACRO_ENTRY:
      return catalog::FindFunction(context, schema_id, name) != nullptr;
    case duckdb::CatalogType::VIEW_ENTRY:
      return catalog::Find<catalog::SereneDBViewEntry>(context, schema_id,
                                                       name) != nullptr;
    case duckdb::CatalogType::SEQUENCE_ENTRY:
      return catalog::Find<catalog::SereneDBSequenceEntry>(context, schema_id,
                                                           name) != nullptr;
    default:
      return false;
  }
}

// The transaction's own view: a GRANT may name a table an earlier statement of
// the same transaction created, and one issued after a mutation inside this
// statement has to see that mutation too.
catalog::TableInfoRef ResolveGrantTarget(ConnectionContext& ctx,
                                         std::string_view raw_name,
                                         std::string& out_schema,
                                         std::string& out_name,
                                         catalog::Permissions* perm = nullptr) {
  const std::string current_schema = ctx.GetCurrentSchema();
  const auto parsed = ParseObjectName(raw_name, current_schema);
  out_schema = parsed.schema;
  out_name = parsed.relation;
  const auto schema_id = catalog::FindSchemaId(
    &ctx.GetClientContext(), ctx.GetDatabaseId(), parsed.schema);
  const auto* entry = schema_id.isSet()
                        ? catalog::Find<catalog::SereneDBTableEntry>(
                            &ctx.GetClientContext(), schema_id, parsed.relation)
                        : nullptr;
  if (entry == nullptr) {
    return nullptr;
  }
  if (perm != nullptr) {
    *perm = entry->permissions;
  }
  return entry->Definition();
}

}  // namespace
namespace {

// What a grant reported back to the statement. Heap-allocated so the op that
// fills it can hold it by value: the op outlives the frame that staged it.
struct AclGrantOutcome {
  bool no_authority = false;
  bool nothing_applied = false;
  bool dependents_block = false;
  bool not_member = false;
};

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

void ApplyAclGrant(duckdb::ClientContext& context, ObjectId owner,
                   catalog::Acl& acl, const AclGrantContext& gc) {
  const auto rc_ptr = auth::ClosureFor(&context, gc.current_id);
  const auto& rc = *rc_ptr;
  const bool is_superuser = rc.is_superuser;
  if (gc.granted_by_id.isSet() && !is_superuser &&
      !auth::ComputeMembershipClosure(*auth::RolesOf(&context), gc.current_id)
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
    const auto grant_option =
      gc.opts.with_grant_option ? allowed : catalog::AclMode::NoRights;
    AclGrant(acl, gc.grantee_id, grantor, allowed, grant_option);
  } else if (gc.opts.grant_option_only) {
    AclRemoveGrantOption(acl, gc.grantee_id, grantor, allowed);
  } else if (gc.opts.cascade) {
    AclRevokeCascade(acl, gc.grantee_id, grantor, allowed);
  } else if (AclDependentPrivs(acl, gc.grantee_id, allowed) !=
             catalog::AclMode::NoRights) {
    *gc.dependents_block = true;
  } else {
    AclRevoke(acl, gc.grantee_id, grantor, allowed);
  }
}

void GrantObjectColumns(ConnectionContext& ctx, duckdb::CatalogType type,
                        std::span<const ParsedPriv> parsed,
                        std::string_view obj_name, std::string_view grantee,
                        bool revoke, const GrantObjectOptions& opts) {
  std::string schema_name;
  std::string rel_name;
  auto table = ResolveGrantTarget(ctx, obj_name, schema_name, rel_name);
  if (!table) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", rel_name, "\" does not exist"));
  }
  const ObjectId current_id = ctx.GetRoleId();
  const ObjectId grantee_id = ResolveGranteeId(ctx.GetClientContext(), grantee);

  const ObjectId granted_by_id =
    ResolveGrantedBy(ctx.GetClientContext(), opts.granted_by);

  auto outcome = std::make_shared<AclGrantOutcome>();
  constexpr catalog::AclMode kColumnPrivs =
    catalog::AclMode::Select | catalog::AclMode::Insert |
    catalog::AclMode::Update | catalog::AclMode::References;
  for (const auto& p : parsed) {
    catalog::AclMode privs =
      auth::TryParseAclKeyword(p.keyword, duckdb::CatalogType::TABLE_ENTRY)
        .value_or(catalog::AclMode::NoRights);
    const bool is_all = absl::EqualsIgnoreCase(p.keyword, "ALL");
    if (!is_all && (privs & ~kColumnPrivs) != catalog::AclMode::NoRights) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_GRANT_OPERATION),
                      ERR_MSG("invalid privilege type ",
                              absl::AsciiStrToUpper(p.keyword), " for column"));
    }
    privs &= kColumnPrivs;
    for (const auto& column : p.columns) {
      catalog::Catalog::MutationScope mutation{GlobalCatalog()};
      table = ChangeColumnAcl(
        catalog::ActingAs(current_id, ctx.GetClientContext()), *table, column,
        [outcome, privs, grantee_id, current_id, granted_by_id, revoke, opts,
         client = &ctx.GetClientContext()](ObjectId owner, catalog::Acl& acl) {
          ApplyAclGrant(
            *client, owner, acl,
            {privs, grantee_id, current_id, granted_by_id, revoke, opts,
             &outcome->no_authority, &outcome->nothing_applied,
             &outcome->dependents_block, &outcome->not_member});
        });
      if (outcome->not_member) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
          ERR_MSG("must be member of role \"", opts.granted_by, "\""));
      }
    }
  }
  if (outcome->dependents_block) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    ERR_MSG("dependent privileges exist"),
                    ERR_HINT("Use CASCADE to revoke them too."));
  }
  if (outcome->no_authority) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied for table ", rel_name));
  }
  if (outcome->nothing_applied) {
    ctx.AddNotice(SQL_ERROR_DATA(
      ERR_CODE(revoke ? ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED
                      : ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
      ERR_MSG("no privileges were ", revoke ? "revoked" : "granted", " for \"",
              rel_name, "\"")));
  }
}

// GRANT / REVOKE ON DATABASE. A database is not in the snapshot -- its entry
// is the object -- so it takes the catalog's database-specific mutator rather
// than the generic one below.
void GrantDatabase(ConnectionContext& ctx,
                   std::span<const ParsedPriv> privileges,
                   std::string_view db_name, std::string_view grantee,
                   bool revoke, const GrantObjectOptions& opts) {
  auto& catalog = GlobalCatalog();
  auto database = catalog::FindDatabase(&ctx.GetClientContext(), db_name);
  if (!database) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("database \"", db_name, "\" does not exist"));
  }
  const ObjectId current_id = ctx.GetRoleId();
  const ObjectId grantee_id = ResolveGranteeId(ctx.GetClientContext(), grantee);
  const ObjectId granted_by_id =
    ResolveGrantedBy(ctx.GetClientContext(), opts.granted_by);
  const catalog::AclMode privs =
    ParseAclModeOrThrow(privileges, duckdb::CatalogType::DATABASE_ENTRY);

  auto outcome = std::make_shared<AclGrantOutcome>();
  catalog.ChangeDatabaseAcl(
    catalog::ActingAs(current_id, ctx.GetClientContext()), database.Id(),
    [outcome, privs, grantee_id, current_id, granted_by_id, revoke, opts,
     client = &ctx.GetClientContext()](ObjectId owner, catalog::Acl& acl) {
      ApplyAclGrant(*client, owner, acl,
                    {privs, grantee_id, current_id, granted_by_id, revoke, opts,
                     &outcome->no_authority, &outcome->nothing_applied,
                     &outcome->dependents_block, &outcome->not_member});
    });
  if (outcome->not_member) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("must be member of role \"", opts.granted_by, "\""));
  }
  if (outcome->dependents_block) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    ERR_MSG("dependent privileges exist"),
                    ERR_HINT("Use CASCADE to revoke them too."));
  }
  if (outcome->no_authority) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied for database ", db_name));
  }
  if (outcome->nothing_applied) {
    ctx.AddNotice(SQL_ERROR_DATA(
      ERR_CODE(revoke ? ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED
                      : ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
      ERR_MSG("no privileges were ", revoke ? "revoked" : "granted", " for \"",
              db_name, "\"")));
  }
}

}  // namespace

void GrantObject(ConnectionContext& ctx, duckdb::CatalogType type,
                 std::span<const ParsedPriv> privileges,
                 std::string_view obj_name, std::string_view grantee,
                 bool revoke, const GrantObjectOptions& opts) {
  if (type == duckdb::CatalogType::DATABASE_ENTRY) {
    GrantDatabase(ctx, privileges, obj_name, grantee, revoke, opts);
    return;
  }
  if (AnyColumnPrivs(privileges)) {
    GrantObjectColumns(ctx, type, privileges, obj_name, grantee, revoke, opts);
    return;
  }

  auto& catalog = GlobalCatalog();
  std::string schema_name;
  std::string rel_name;
  ObjectId schema_target;
  // Every kind but a table is named by the schema it lives in and its own name;
  // a table is the one the mutator takes by definition.
  ObjectId entry_schema;
  // And a foreign server's entry is the object too, named by its database: it
  // is a database child with no schema, as it is in postgres.
  ObjectId server_database;
  if (type == duckdb::CatalogType::FOREIGN_SERVER_ENTRY) {
    rel_name = obj_name;
    if (catalog::FindForeignServer(&ctx.GetClientContext(), ctx.GetDatabaseId(),
                                   obj_name)) {
      server_database = ctx.GetDatabaseId();
    }
  } else if (type == duckdb::CatalogType::SCHEMA_ENTRY) {
    rel_name = obj_name;
    auto schema = catalog::FindSchema(&ctx.GetClientContext(),
                                      ctx.GetDatabaseId(), obj_name);
    if (!schema) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                      ERR_MSG("schema \"", obj_name, "\" does not exist"));
    }
    schema_target = catalog::IdOf(*schema);
  } else if (type == duckdb::CatalogType::TYPE_ENTRY ||
             type == duckdb::CatalogType::MACRO_ENTRY ||
             type == duckdb::CatalogType::VIEW_ENTRY ||
             type == duckdb::CatalogType::SEQUENCE_ENTRY) {
    // A function name carries its argument list; the entry is keyed on the bare
    // name, as duckdb's macro sets are.
    const auto bare = type == duckdb::CatalogType::MACRO_ENTRY
                        ? obj_name.substr(0, obj_name.find('('))
                        : obj_name;
    const std::string current_schema = ctx.GetCurrentSchema();
    const auto parsed =
      ParseObjectName(absl::StripAsciiWhitespace(bare), current_schema);
    schema_name = parsed.schema;
    rel_name = parsed.relation;
    const auto schema_id = catalog::FindSchemaId(
      &ctx.GetClientContext(), ctx.GetDatabaseId(), parsed.schema);
    if (schema_id.isSet() &&
        EntryExists(ctx, type, schema_id, parsed.relation)) {
      entry_schema = schema_id;
    }
  }
  // A GRANT on a relation name that turns out to be a view lands on the view's
  // own entry. The privilege keywords stay the relation set's -- PG spells a
  // view's grants that way.
  bool entry_is_view = false;
  if (type == duckdb::CatalogType::TABLE_ENTRY && !entry_schema.isSet()) {
    const std::string current_schema = ctx.GetCurrentSchema();
    const auto parsed = ParseObjectName(obj_name, current_schema);
    const auto schema_id = catalog::FindSchemaId(
      &ctx.GetClientContext(), ctx.GetDatabaseId(), parsed.schema);
    if (schema_id.isSet() &&
        catalog::Find<catalog::SereneDBViewEntry>(&ctx.GetClientContext(),
                                                  schema_id, parsed.relation)) {
      schema_name = parsed.schema;
      rel_name = parsed.relation;
      entry_schema = schema_id;
      entry_is_view = true;
    }
  }
  // A sequence is never the table half: a name the schema's sequence set does
  // not hold is not a sequence, whatever else the relation namespace has under
  // it, and resolving one would grant sequence privileges on a table.
  auto target =
    (schema_target.isSet() || entry_schema.isSet() || server_database.isSet() ||
     type == duckdb::CatalogType::FOREIGN_SERVER_ENTRY ||
     type == duckdb::CatalogType::SEQUENCE_ENTRY)
      ? nullptr
      : ResolveGrantTarget(ctx, obj_name, schema_name, rel_name);
  if (!target && !schema_target.isSet() && !entry_schema.isSet() &&
      !server_database.isSet()) {
    if (type == duckdb::CatalogType::TYPE_ENTRY &&
        RegtypeIn(rel_name) != kInvalidOid) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("cannot change privileges of built-in type ", rel_name));
    }
    // The other half of the relation namespace still holds the name, and PG
    // reports the kind mismatch rather than a missing relation.
    if (type == duckdb::CatalogType::SEQUENCE_ENTRY) {
      const auto schema_id = catalog::FindSchemaId(
        &ctx.GetClientContext(), ctx.GetDatabaseId(), schema_name);
      if (schema_id.isSet() &&
          catalog::Find<catalog::SereneDBTableEntry>(&ctx.GetClientContext(),
                                                     schema_id, rel_name)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                        ERR_MSG("\"", rel_name, "\" is not a sequence"));
      }
    }
    const bool is_relation = type == duckdb::CatalogType::TABLE_ENTRY ||
                             type == duckdb::CatalogType::VIEW_ENTRY ||
                             type == duckdb::CatalogType::SEQUENCE_ENTRY;
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG(is_relation ? "relation" : ToPgObjectTypeName(type),
                            " \"", rel_name, "\" does not exist"));
  }

  const ObjectId current_id = ctx.GetRoleId();

  const ObjectId grantee_id = ResolveGranteeId(ctx.GetClientContext(), grantee);

  const ObjectId granted_by_id =
    ResolveGrantedBy(ctx.GetClientContext(), opts.granted_by);

  const catalog::AclMode privs = ParseAclModeOrThrow(privileges, type);

  auto outcome = std::make_shared<AclGrantOutcome>();
  auto mutate = [outcome, privs, grantee_id, current_id, granted_by_id, revoke,
                 opts, client = &ctx.GetClientContext()](ObjectId owner,
                                                         catalog::Acl& acl) {
    ApplyAclGrant(*client, owner, acl,
                  {privs, grantee_id, current_id, granted_by_id, revoke, opts,
                   &outcome->no_authority, &outcome->nothing_applied,
                   &outcome->dependents_block, &outcome->not_member});
  };
  {
    const auto ax = catalog::ActingAs(current_id, ctx.GetClientContext());
    catalog::Catalog::MutationScope mutation{catalog};
    if (server_database.isSet()) {
      catalog::ChangeEntryAcl(ax, duckdb::CatalogType::FOREIGN_SERVER_ENTRY,
                              server_database, rel_name, std::move(mutate));
    } else if (schema_target.isSet()) {
      ChangeSchemaAcl(ax, schema_target, std::move(mutate));
    } else if (entry_is_view || type == duckdb::CatalogType::VIEW_ENTRY) {
      catalog::ChangeEntryAcl(ax, duckdb::CatalogType::VIEW_ENTRY, entry_schema,
                              rel_name, std::move(mutate));
    } else if (!entry_schema.isSet()) {
      ChangeTableAcl(ax, *target, type, std::move(mutate));
    } else if (type == duckdb::CatalogType::TYPE_ENTRY) {
      catalog::ChangeEntryAcl(ax, duckdb::CatalogType::TYPE_ENTRY, entry_schema,
                              rel_name, std::move(mutate));
    } else if (type == duckdb::CatalogType::SEQUENCE_ENTRY) {
      catalog::ChangeEntryAcl(ax, duckdb::CatalogType::SEQUENCE_ENTRY,
                              entry_schema, rel_name, std::move(mutate));
    } else {
      catalog::ChangeEntryAcl(ax, duckdb::CatalogType::MACRO_ENTRY,
                              entry_schema, rel_name, std::move(mutate));
    }
  }
  if (outcome->not_member) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("must be member of role \"", opts.granted_by, "\""));
  }
  if (outcome->dependents_block) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    ERR_MSG("dependent privileges exist"),
                    ERR_HINT("Use CASCADE to revoke them too."));
  }
  if (outcome->no_authority) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied for ", ToPgObjectTypeName(type),
                            " ", rel_name));
  }
  if (outcome->nothing_applied) {
    ctx.AddNotice(SQL_ERROR_DATA(
      ERR_CODE(revoke ? ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED
                      : ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
      ERR_MSG("no privileges were ", revoke ? "revoked" : "granted", " for \"",
              rel_name, "\"")));
  }

  // Only a table has column grants to follow the relation's -- a view under the
  // same relation namespace landed on its own entry instead.
  if (revoke && target && type == duckdb::CatalogType::TABLE_ENTRY) {
    const auto* tbl_entry = catalog::Find<catalog::SereneDBTableEntry>(
      &ctx.GetClientContext(), catalog::ParentIdOf(*target),
      catalog::IdOf(*target));
    if (tbl_entry != nullptr) {
      auto tbl = tbl_entry->Definition();
      // The column list is read off one version, but each revoke has to build
      // on the previous one's result, so the returned versions chain.
      std::vector<std::string> granted;
      for (const auto& entry : tbl_entry->permissions.column_acl) {
        if (const auto* column =
              catalog::ColumnById(*tbl, ObjectId{entry.catalog_oid})) {
          granted.emplace_back(column->Name().GetIdentifierName());
        }
      }
      for (const auto& column : granted) {
        catalog::Catalog::MutationScope mutation{catalog};
        tbl = ChangeColumnAcl(
          catalog::ActingAs(current_id, ctx.GetClientContext()), *tbl, column,
          [grantee_id, privs](ObjectId owner, catalog::Acl& acl) {
            AclRevoke(acl, grantee_id, owner, privs);
          });
      }
    }
  }
}

void GrantObjectAllInSchema(ConnectionContext& ctx, duckdb::CatalogType type,
                            std::span<const ParsedPriv> privileges,
                            std::string_view schema_name,
                            std::string_view grantee, bool revoke,
                            const GrantObjectOptions& opts) {
  const ObjectId db = ctx.GetDatabaseId();
  auto schema = catalog::FindSchema(&ctx.GetClientContext(), db, schema_name);
  if (!schema) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                    ERR_MSG("schema \"", schema_name, "\" does not exist"));
  }
  const ObjectId schema_id = catalog::IdOf(*schema);

  std::vector<std::string> names;
  if (type == duckdb::CatalogType::MACRO_ENTRY) {
    catalog::VisitFunctions(
      &ctx.GetClientContext(), db,
      [&](const duckdb::MacroCatalogEntry& function) {
        if (ObjectId{function.ParentSchema().oid} == schema_id) {
          names.emplace_back(function.name.GetIdentifierName());
        }
      });
  } else if (type == duckdb::CatalogType::SEQUENCE_ENTRY) {
    // Only the free-standing ones, as PG's GRANT ON ALL SEQUENCES is: a
    // SERIAL's sequence is granted through the table that owns it.
    catalog::Visit<catalog::SereneDBSequenceEntry>(
      &ctx.GetClientContext(), db,
      [&](const catalog::SereneDBSequenceEntry& seq) {
        if (ObjectId{seq.ParentSchema().oid} == schema_id &&
            !seq.GetOwnerTableId().isSet()) {
          names.emplace_back(seq.name.GetIdentifierName());
        }
      });
  } else {
    catalog::VisitDefinitions<catalog::SereneDBTableEntry>(
      &ctx.GetClientContext(), db,
      [&](const catalog::TableInfoRef& table, const catalog::Permissions&) {
        if (catalog::ParentIdOf(*table) == schema_id) {
          names.emplace_back(catalog::TableNameOf(*table));
        }
      });
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
  auto role_obj = catalog::FindRole(&ctx.GetClientContext(), role);
  auto member_obj = catalog::FindRole(&ctx.GetClientContext(), member);
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

  catalog.ChangeMembership(
    catalog::ActingAs(ctx.GetRoleId(), ctx.GetClientContext()), role_id, role,
    member_id, member, edge, revoke, opts.admin_option_only);
}

void AlterOwner(ConnectionContext& ctx, std::string_view obj_type,
                std::string_view name, std::string_view new_owner) {
  const auto type = FromPgObjectTypeName(obj_type);
  SDB_ASSERT(type != duckdb::CatalogType::INVALID);
  auto& catalog =
    catalog::DatabaseCatalog(&ctx.GetClientContext(), ctx.GetDatabaseId());
  const ObjectId current_id = ctx.GetRoleId();

  std::string_view new_owner_name = new_owner;
  if (new_owner == "CURRENT_USER" || new_owner == "SESSION_USER" ||
      new_owner == "CURRENT_ROLE") {
    new_owner_name = ctx.user();
  }
  auto new_owner_role =
    catalog::FindRole(&ctx.GetClientContext(), new_owner_name);
  if (!new_owner_role) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", new_owner_name, "\" does not exist"));
  }
  const ObjectId new_owner_id = new_owner_role->GetId();

  const ObjectId database_id = ctx.GetDatabaseId();
  catalog::TableInfoRef target;
  if (type == duckdb::CatalogType::SCHEMA_ENTRY) {
    auto schema =
      catalog::FindSchema(&ctx.GetClientContext(), database_id, name);
    if (!schema) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                      ERR_MSG("schema \"", name, "\" does not exist"));
    }
    catalog::Catalog::MutationScope mutation{catalog::GetCatalog()};
    ChangeSchemaOwner(catalog::ActingAs(current_id, ctx.GetClientContext()),
                      catalog::IdOf(*schema), new_owner_id, new_owner_name);
    return;
  }
  {
    const std::string current_schema = ctx.GetCurrentSchema();
    const auto parsed = ParseObjectName(name, current_schema);
    if (!catalog::FindSchema(&ctx.GetClientContext(), database_id,
                             parsed.schema)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                      ERR_MSG("schema \"", parsed.schema, "\" does not exist"));
    }
    // Types and functions live in their own per-schema namespaces, separate
    // from relations -- and their entry is the object, so the rewrite is
    // theirs.
    const auto schema_id = catalog::FindSchemaId(&ctx.GetClientContext(),
                                                 database_id, parsed.schema);
    const auto ax = catalog::ActingAs(current_id, ctx.GetClientContext());
    // A relation name that turns out to be a view is the view's own rewrite,
    // as a type's and a function's are.
    auto kind = type;
    if (kind == duckdb::CatalogType::TABLE_ENTRY && schema_id.isSet() &&
        catalog::Find<catalog::SereneDBViewEntry>(&ctx.GetClientContext(),
                                                  schema_id, parsed.relation)) {
      kind = duckdb::CatalogType::VIEW_ENTRY;
    }
    if (kind == duckdb::CatalogType::TYPE_ENTRY ||
        kind == duckdb::CatalogType::MACRO_ENTRY ||
        kind == duckdb::CatalogType::VIEW_ENTRY ||
        kind == duckdb::CatalogType::SEQUENCE_ENTRY) {
      if (!schema_id.isSet() ||
          !EntryExists(ctx, kind, schema_id, parsed.relation)) {
        // A sequence shares the relation namespace, so the other half of it
        // still answers for the name and PG reports the kind mismatch.
        if (kind == duckdb::CatalogType::SEQUENCE_ENTRY && schema_id.isSet() &&
            catalog::Find<catalog::SereneDBTableEntry>(
              &ctx.GetClientContext(), schema_id, parsed.relation)) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
            ERR_MSG("\"", parsed.relation, "\" is not a sequence"));
        }
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                        ERR_MSG(ToPgObjectTypeName(kind), " \"",
                                parsed.relation, "\" does not exist"));
      }
      catalog::Catalog::MutationScope mutation{catalog::GetCatalog()};
      if (kind == duckdb::CatalogType::TYPE_ENTRY) {
        catalog::ChangeEntryOwner(ax, duckdb::CatalogType::TYPE_ENTRY,
                                  schema_id, parsed.relation, new_owner_id,
                                  new_owner_name);
      } else if (kind == duckdb::CatalogType::VIEW_ENTRY) {
        catalog::ChangeEntryOwner(ax, duckdb::CatalogType::VIEW_ENTRY,
                                  schema_id, parsed.relation, new_owner_id,
                                  new_owner_name);
      } else if (kind == duckdb::CatalogType::SEQUENCE_ENTRY) {
        catalog::ChangeEntryOwner(ax, duckdb::CatalogType::SEQUENCE_ENTRY,
                                  schema_id, parsed.relation, new_owner_id,
                                  new_owner_name);
      } else {
        catalog::ChangeEntryOwner(ax, duckdb::CatalogType::MACRO_ENTRY,
                                  schema_id, parsed.relation, new_owner_id,
                                  new_owner_name);
      }
      return;
    }
    const auto* target_entry =
      schema_id.isSet() ? catalog::Find<catalog::SereneDBTableEntry>(
                            &ctx.GetClientContext(), schema_id, parsed.relation)
                        : nullptr;
    target = target_entry != nullptr ? target_entry->Definition() : nullptr;
    if (!target) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                      ERR_MSG(ToPgObjectTypeName(type), " \"", parsed.relation,
                              "\" does not exist"));
    }
  }
  catalog.ChangeTableOwner(
    catalog::ActingAs(current_id, ctx.GetClientContext()), *target, type,
    new_owner_id, new_owner_name);
}

}  // namespace sdb::pg
