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

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/function/pragma_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "auth/role_closure.h"
#include "catalog/cluster.h"
#include "catalog/entry/role.h"
#include "connector/duckdb_client_state.h"
#include "network/credentials.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/pg_types.h"
#include "pg/role_dependencies.h"
#include "pg/sql_exception_macro.h"

namespace sdb::pg {
namespace {

using catalog::RoleOption;
using duckdb::CatalogType;

struct Session {
  ConnectionContext& conn;
  duckdb::ClientContext& client;
  std::shared_ptr<const auth::RoleGraph> roles;
  std::shared_ptr<const auth::RoleClosure> closure;

  duckdb::idx_t Role() const { return conn.GetRoleId(); }
  bool Superuser() const { return closure->is_superuser; }
  catalog::ClusterCatalog& Cluster() const {
    return catalog::ClusterOf(client);
  }
  duckdb::CatalogTransaction ClusterTransaction() const {
    return Cluster().GetCatalogTransaction(client);
  }
};

Session SessionOf(duckdb::ClientContext& client) {
  auto& conn = connector::GetSereneDBContext(client);
  return Session{conn, client, auth::RolesOf(&client),
                 auth::ClosureFor(&client, conn.GetRoleId())};
}

const duckdb::Value& Arg(const duckdb::FunctionParameters& params, size_t i) {
  return params.values[i];
}

std::string Str(const duckdb::FunctionParameters& params, size_t i) {
  return Arg(params, i).GetValue<std::string>();
}

bool Flag(const duckdb::FunctionParameters& params, size_t i) {
  return Arg(params, i).GetValue<bool>();
}

int64_t Big(const duckdb::FunctionParameters& params, size_t i) {
  return Arg(params, i).GetValue<int64_t>();
}

std::vector<std::string> Names(const duckdb::FunctionParameters& params,
                               size_t i) {
  std::vector<std::string> out;
  for (const auto& value : duckdb::ListValue::GetChildren(Arg(params, i))) {
    out.push_back(value.GetValue<std::string>());
  }
  return out;
}

duckdb::optional_ptr<duckdb::CatalogEntry> FindRole(const Session& s,
                                                    std::string_view name) {
  return s.Cluster()
    .GetCatalogSet(CatalogType::ROLE_ENTRY)
    .GetEntry(s.ClusterTransaction(), duckdb::Identifier{std::string{name}});
}

catalog::RoleCatalogEntry& RoleByName(const Session& s, std::string_view name) {
  auto entry = FindRole(s, name);
  if (!entry) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", name, "\" does not exist"));
  }
  return entry->Cast<catalog::RoleCatalogEntry>();
}

std::string RoleName(const Session& s, duckdb::idx_t role) {
  return std::string{s.roles->NameOf(role)};
}

[[noreturn]] void DenyRoleAction(std::string_view verb, std::string detail) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied to ", verb, " role"),
                  ERR_DETAIL(detail));
}

void RequireAttributeGrant(const Session& s, std::string_view verb,
                           RoleOption options) {
  if (s.Superuser()) {
    return;
  }
  const auto deny = [&](std::string_view attribute) {
    DenyRoleAction(
      verb, absl::StrCat("Only roles with the ", attribute, " attribute may ",
                         verb, " roles with the ", attribute, " attribute."));
  };
  if (HasOption(options, RoleOption::Superuser)) {
    deny("SUPERUSER");
  }
  if (HasOption(options, RoleOption::CreateDb) &&
      !s.closure->Has(RoleOption::CreateDb)) {
    deny("CREATEDB");
  }
  if (HasOption(options, RoleOption::Replication) &&
      !s.closure->Has(RoleOption::Replication)) {
    deny("REPLICATION");
  }
  if (HasOption(options, RoleOption::BypassRls) &&
      !s.closure->Has(RoleOption::BypassRls)) {
    deny("BYPASSRLS");
  }
}

void RequireRoleAdmin(const Session& s, const catalog::RoleCatalogEntry& role,
                      std::string_view verb) {
  if (s.Superuser()) {
    return;
  }
  if (role.IsSuperuser()) {
    DenyRoleAction(
      verb, absl::StrCat("Only roles with the SUPERUSER attribute may ", verb,
                         " roles with the SUPERUSER attribute."));
  }
  if (!s.closure->Has(RoleOption::CreateRole) ||
      !s.closure->IsAdminOf(role.oid)) {
    DenyRoleAction(verb,
                   absl::StrCat("Only roles with the CREATEROLE attribute and "
                                "the ADMIN option on role \"",
                                role.name.GetIdentifierName(), "\" may ", verb,
                                " this role."));
  }
}

void RequireConnectionLimit(int64_t limit) {
  if (limit < -1) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("invalid connection limit: ", limit));
  }
}

std::string StoredPassword(std::string_view password) {
  if (network::IsScramVerifier(password) || network::IsMd5Verifier(password)) {
    return std::string{password};
  }
  auto verifier = network::BuildScramVerifierString(password);
  if (!verifier) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("could not hash the password"));
  }
  return *verifier;
}

void RequireCreateRoleForDrop(const Session& s) {
  if (!s.Superuser() && !s.closure->Has(RoleOption::CreateRole)) {
    DenyRoleAction("drop",
                   "Only roles with the CREATEROLE attribute and the ADMIN "
                   "option on the target roles may drop roles.");
  }
}

void RefreshSuperuser(ConnectionContext& ctx) {
  const bool superuser =
    auth::ClosureFor(&ctx.GetClientContext(), ctx.GetRoleId())->is_superuser;
  ctx.SetSetting("is_superuser", superuser ? "on" : "off", false);
}

RoleOption OptionIf(bool enabled, RoleOption option) {
  return enabled ? option : RoleOption::None;
}

duckdb::idx_t GrantorOfMembership(const Session& s) {
  return s.Superuser() ? kRootUser : s.Role();
}

void GrantMembership(const Session& s, catalog::RoleCatalogEntry& member,
                     duckdb::idx_t role, duckdb::idx_t grantor, bool admin,
                     bool inherit, bool set) {
  duckdb::AlterRoleInfo alter{member.name};
  alter.grant_role_id = role;
  alter.grantor_id = grantor;
  alter.admin_option = admin;
  alter.inherit_option = inherit;
  alter.set_option = set;
  s.Cluster().Alter(s.ClusterTransaction(), alter);
}

void CreateRolePragma(duckdb::ClientContext& client,
                      const duckdb::FunctionParameters& params) {
  auto s = SessionOf(client);
  const auto name = Str(params, 0);
  const bool login = Flag(params, 1);
  const bool superuser = Flag(params, 2);
  const bool inherit = Flag(params, 3);
  const bool has_password = Flag(params, 4);
  const auto password = Str(params, 5);
  const bool password_is_null = Flag(params, 6);
  const bool has_conn_limit = Flag(params, 7);
  const bool has_valid_until = Flag(params, 8);
  const auto valid_until = Big(params, 9);
  const bool createdb = Flag(params, 10);
  const bool createrole = Flag(params, 11);
  const auto conn_limit = Big(params, 12);
  const bool replication = Flag(params, 13);
  const bool bypassrls = Flag(params, 14);
  const auto in_roles = Names(params, 15);
  const auto role_members = Names(params, 16);
  const auto admin_members = Names(params, 17);

  if (!s.Superuser() && !s.closure->Has(RoleOption::CreateRole)) {
    DenyRoleAction(
      "create", "Only roles with the CREATEROLE attribute may create roles.");
  }
  RequireAttributeGrant(s, "create",
                        OptionIf(superuser, RoleOption::Superuser) |
                          OptionIf(createdb, RoleOption::CreateDb) |
                          OptionIf(replication, RoleOption::Replication) |
                          OptionIf(bypassrls, RoleOption::BypassRls));
  if (has_conn_limit) {
    RequireConnectionLimit(conn_limit);
  }
  if (FindRole(s, name)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("role \"", name, "\" already exists"));
  }

  duckdb::CreateRoleInfo info;
  info.SetName(duckdb::Identifier{name});
  info.options = OptionIf(login, RoleOption::Login) |
                 OptionIf(superuser, RoleOption::Superuser) |
                 OptionIf(inherit, RoleOption::Inherit) |
                 OptionIf(createdb, RoleOption::CreateDb) |
                 OptionIf(createrole, RoleOption::CreateRole) |
                 OptionIf(replication, RoleOption::Replication) |
                 OptionIf(bypassrls, RoleOption::BypassRls);
  if (has_password && !password_is_null) {
    info.password = StoredPassword(password);
  }
  if (has_conn_limit) {
    info.conn_limit = static_cast<int32_t>(conn_limit);
  }
  if (has_valid_until) {
    info.valid_until = valid_until;
  }
  const auto grantor = GrantorOfMembership(s);
  for (const auto& role_name : in_roles) {
    info.member_of.push_back(duckdb::Membership{
      .role = RoleByName(s, role_name).oid,
      .grantor = grantor,
      .admin_option = false,
      .inherit_option = inherit,
      .set_option = true,
    });
  }
  auto created = s.Cluster().CreateRole(s.ClusterTransaction(), info);
  if (!created) {
    return;
  }
  const auto new_role = created->oid;
  for (const auto& member_name : role_members) {
    auto& member = RoleByName(s, member_name);
    GrantMembership(s, member, new_role, grantor, false,
                    HasOption(member.Options(), RoleOption::Inherit), true);
  }
  for (const auto& member_name : admin_members) {
    auto& member = RoleByName(s, member_name);
    GrantMembership(s, member, new_role, grantor, true,
                    HasOption(member.Options(), RoleOption::Inherit), true);
  }
  if (!s.Superuser()) {
    GrantMembership(s, RoleByName(s, RoleName(s, s.Role())), new_role,
                    kRootUser, true, false, false);
  }
}

void DropRolePragma(duckdb::ClientContext& client,
                    const duckdb::FunctionParameters& params) {
  auto s = SessionOf(client);
  const bool if_exists = Flag(params, 1);
  RequireCreateRoleForDrop(s);
  for (const auto& name : Names(params, 0)) {
    auto entry = FindRole(s, name);
    if (!entry) {
      if (if_exists) {
        continue;
      }
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("role \"", name, "\" does not exist"));
    }
    auto& role = entry->Cast<catalog::RoleCatalogEntry>();
    RequireRoleAdmin(s, role, "drop");
    if (role.oid == s.Role() || role.oid == s.conn.GetSessionRoleId() ||
        role.oid == s.conn.GetLoginRoleId()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                      ERR_MSG("current user cannot be dropped"));
    }

    size_t dependencies = 0;
    VisitRoleDependencies(client, [&](const RoleDependency& dependency) {
      if (dependency.role == role.oid) {
        ++dependencies;
      }
    });
    if (dependencies != 0) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
        ERR_MSG("role \"", name,
                "\" cannot be dropped because some objects depend on it"),
        ERR_DETAIL(dependencies, " object(s) in database depend on role \"",
                   name, "\""));
    }

    std::vector<duckdb::Identifier> members;
    s.Cluster()
      .GetCatalogSet(CatalogType::ROLE_ENTRY)
      .Scan(s.ClusterTransaction(), [&](duckdb::CatalogEntry& other) {
        const auto& candidate = other.Cast<catalog::RoleCatalogEntry>();
        if (std::ranges::contains(candidate.MemberOf(), role.oid,
                                  &duckdb::Membership::role)) {
          members.push_back(candidate.name);
        }
      });
    for (const auto& member : members) {
      duckdb::AlterRoleInfo alter{member};
      alter.grant_role_id = role.oid;
      alter.revoke = true;
      s.Cluster().Alter(s.ClusterTransaction(), alter);
    }
    duckdb::DropInfo drop;
    drop.type = CatalogType::ROLE_ENTRY;
    drop.SetName(role.name);
    s.Cluster().DropRole(s.ClusterTransaction(), drop);
  }
}

void ResolveMembership(const Session& s, duckdb::AlterRoleInfo& info,
                       const catalog::RoleCatalogEntry& member) {
  const auto& target = RoleByName(s, info.grant_role);
  if (!s.Superuser() && !s.closure->IsAdminOf(target.oid)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to grant role \"",
              target.name.GetIdentifierName(), "\""),
      ERR_DETAIL("Only roles with the ADMIN option on role \"",
                 target.name.GetIdentifierName(), "\" may grant this role."));
  }
  if (!info.revoke &&
      (member.oid == target.oid ||
       auth::ComputeRoleClosure(*s.roles, target.oid).IsMember(member.oid))) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_GRANT_OPERATION),
                    ERR_MSG("role \"", target.name.GetIdentifierName(),
                            "\" is a member of role \"",
                            member.name.GetIdentifierName(), "\""));
  }
  info.grant_role_id = target.oid;
  info.grantor_id = GrantorOfMembership(s);
}

}  // namespace

void ResolveAlterRole(duckdb::ClientContext& client,
                      duckdb::AlterRoleInfo& info) {
  auto s = SessionOf(client);
  auto& role =
    RoleByName(s, info.GetQualifiedName().Name().GetIdentifierName());
  if (!info.grant_role.empty()) {
    ResolveMembership(s, info, role);
    return;
  }
  if (!info.new_name.empty()) {
    if (role.oid == s.Role() || role.oid == s.conn.GetSessionRoleId()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("session user cannot be renamed"));
    }
    RequireRoleAdmin(s, role, "rename");
    if (FindRole(s, info.new_name.GetIdentifierName())) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                      ERR_MSG("role \"", info.new_name.GetIdentifierName(),
                              "\" already exists"));
    }
    return;
  }
  const bool attributes = info.set_options != RoleOption::None ||
                          info.clear_options != RoleOption::None ||
                          info.set_conn_limit || info.set_valid_until;
  if (!s.Superuser() && (role.oid != s.Role() || attributes)) {
    RequireRoleAdmin(s, role, "alter");
    RequireAttributeGrant(s, "alter", info.set_options);
  }
  if (info.set_conn_limit) {
    RequireConnectionLimit(info.conn_limit);
  }
  if (info.set_password) {
    info.password =
      info.null_password ? std::string{} : StoredPassword(info.password);
  }
}

void RegisterRbacFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");
  using duckdb::LogicalType;
  const auto names = LogicalType::LIST(LogicalType::VARCHAR);
  const auto add = [&](const char* name, duckdb::pragma_function_t function,
                       duckdb::vector<LogicalType> arguments) {
    loader.RegisterFunction(duckdb::PragmaFunction::PragmaCall(
      duckdb::Identifier{name}, function, std::move(arguments)));
  };
  add("serenedb_create_role", CreateRolePragma,
      {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN,
       LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::VARCHAR,
       LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN,
       LogicalType::BIGINT, LogicalType::BOOLEAN, LogicalType::BOOLEAN,
       LogicalType::BIGINT, LogicalType::BOOLEAN, LogicalType::BOOLEAN, names,
       names, names});
  add("serenedb_drop_role", DropRolePragma, {names, LogicalType::BOOLEAN});
}

std::string SetRole(ConnectionContext& ctx, std::string_view name) {
  if (name.empty() || absl::EqualsIgnoreCase(name, "none")) {
    ctx.SetEffectiveRole(ctx.GetSessionRoleId());
    RefreshSuperuser(ctx);
    return "none";
  }
  auto s = SessionOf(ctx.GetClientContext());
  auto& role = RoleByName(s, name);
  const auto session_role = ctx.GetSessionRoleId();
  const auto session = auth::ComputeRoleClosure(*s.roles, session_role);
  if (role.oid != session_role && !session.is_superuser &&
      !session.CanSet(role.oid)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied to set role \"", name, "\""));
  }
  ctx.SetEffectiveRole(role.oid);
  RefreshSuperuser(ctx);
  return std::string{name};
}

void ResetRole(ConnectionContext& ctx) {
  ctx.SetEffectiveRole(ctx.GetSessionRoleId());
  RefreshSuperuser(ctx);
}

std::string SetSessionAuthorization(ConnectionContext& ctx,
                                    std::string_view name) {
  auto s = SessionOf(ctx.GetClientContext());
  auto& role = RoleByName(s, name);
  const auto login = ctx.GetLoginRoleId();
  if (role.oid != login && !auth::ClosureFor(&s.client, login)->is_superuser) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to set session authorization \"", name, "\""));
  }
  ctx.SetSessionRole(role.oid);
  RefreshSuperuser(ctx);
  return std::string{name};
}

void ResetSessionAuthorization(ConnectionContext& ctx) {
  ctx.ResetIdentity();
  RefreshSuperuser(ctx);
}

}  // namespace sdb::pg
