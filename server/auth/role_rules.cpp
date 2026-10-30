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

#include "auth/role_rules.h"

#include <absl/strings/str_cat.h>

#include <duckdb/catalog/permissions.hpp>
#include <duckdb/main/client_context.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string_view>

#include "auth/role_closure.h"
#include "catalog/cluster.h"
#include "catalog/entry/role.h"

namespace sdb::auth {
namespace {

using catalog::RoleOption;

[[noreturn]] void DenyRoleAction(std::string_view verb,
                                 std::string_view detail) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied to ", verb, " role"),
                  ERR_DETAIL(detail));
}

const duckdb::RoleAccess* Enforced(duckdb::CatalogTransaction transaction) {
  if (!transaction.HasContext()) {
    return nullptr;
  }
  const auto principal = transaction.GetContext().principal;
  return principal && !principal->is_superuser ? principal.get() : nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> FindRole(
  duckdb::CatalogTransaction transaction, std::string_view name) {
  return catalog::ClusterOf(transaction.GetContext())
    .GetCatalogSet(duckdb::CatalogType::ROLE_ENTRY)
    .GetEntry(transaction, duckdb::Identifier{name});
}

bool IsAdminOf(duckdb::ClientContext& context, duckdb::idx_t role) {
  return MembershipsOf(*RolesOf(&context), context.principal_role)
    .IsAdminOf(role);
}

void RequireAttributeGrant(const duckdb::RoleAccess& caller,
                           std::string_view verb, RoleOption options) {
  const auto deny = [&](std::string_view attribute) {
    DenyRoleAction(
      verb, absl::StrCat("Only roles with the ", attribute, " attribute may ",
                         verb, " roles with the ", attribute, " attribute."));
  };
  if (HasOption(options, RoleOption::Superuser)) {
    deny("SUPERUSER");
  }
  if (HasOption(options, RoleOption::CreateDb) &&
      !caller.Has(RoleOption::CreateDb)) {
    deny("CREATEDB");
  }
  if (HasOption(options, RoleOption::Replication) &&
      !caller.Has(RoleOption::Replication)) {
    deny("REPLICATION");
  }
  if (HasOption(options, RoleOption::BypassRls) &&
      !caller.Has(RoleOption::BypassRls)) {
    deny("BYPASSRLS");
  }
}

void RequireRoleAdmin(duckdb::ClientContext& context,
                      const duckdb::RoleAccess& caller,
                      const duckdb::CatalogEntry& role, std::string_view verb) {
  if (role.Cast<catalog::RoleCatalogEntry>().IsSuperuser()) {
    DenyRoleAction(
      verb, absl::StrCat("Only roles with the SUPERUSER attribute may ", verb,
                         " roles with the SUPERUSER attribute."));
  }
  if (!caller.Has(RoleOption::CreateRole) || !IsAdminOf(context, role.oid)) {
    DenyRoleAction(verb,
                   absl::StrCat("Only roles with the CREATEROLE attribute and "
                                "the ADMIN option on role \"",
                                role.name.GetIdentifierName(), "\" may ", verb,
                                " this role."));
  }
}

void RequireGrantableRole(duckdb::CatalogTransaction transaction,
                          std::string_view name) {
  auto role = FindRole(transaction, name);
  if (role && !IsAdminOf(transaction.GetContext(), role->oid)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to grant role \"",
              role->name.GetIdentifierName(), "\""),
      ERR_DETAIL("Only roles with the ADMIN option on role \"",
                 role->name.GetIdentifierName(), "\" may grant this role."));
  }
}

}  // namespace

void CheckCreateRole(duckdb::CatalogTransaction transaction,
                     const duckdb::CreateRoleInfo& info) {
  const auto* caller = Enforced(transaction);
  if (!caller) {
    return;
  }
  if (!caller->Has(RoleOption::CreateRole)) {
    DenyRoleAction(
      "create", "Only roles with the CREATEROLE attribute may create roles.");
  }
  RequireAttributeGrant(*caller, "create", info.options);
  for (const auto& name : info.in_roles) {
    RequireGrantableRole(transaction, name.GetIdentifierName());
  }
}

void CheckAlterRole(duckdb::CatalogTransaction transaction,
                    const duckdb::AlterRoleInfo& info) {
  const auto* caller = Enforced(transaction);
  if (!caller) {
    return;
  }
  auto role =
    FindRole(transaction, info.GetQualifiedName().Name().GetIdentifierName());
  if (!role) {
    return;
  }
  if (!info.grant_role.empty()) {
    RequireGrantableRole(transaction, info.grant_role);
    return;
  }
  auto& context = transaction.GetContext();
  if (!info.new_name.empty()) {
    RequireRoleAdmin(context, *caller, *role, "rename");
    return;
  }
  const bool attributes = info.set_options != RoleOption::None ||
                          info.clear_options != RoleOption::None ||
                          info.set_conn_limit || info.set_valid_until;
  if (role->oid != caller->role || attributes) {
    RequireRoleAdmin(context, *caller, *role, "alter");
    RequireAttributeGrant(*caller, "alter", info.set_options);
  }
}

void CheckDropRole(duckdb::CatalogTransaction transaction,
                   const duckdb::DropInfo& info) {
  const auto* caller = Enforced(transaction);
  if (!caller) {
    return;
  }
  if (!caller->Has(RoleOption::CreateRole)) {
    DenyRoleAction("drop",
                   "Only roles with the CREATEROLE attribute and the ADMIN "
                   "option on the target roles may drop roles.");
  }
  if (auto role = FindRole(
        transaction, info.GetQualifiedName().Name().GetIdentifierName())) {
    RequireRoleAdmin(transaction.GetContext(), *caller, *role, "drop");
  }
}

}  // namespace sdb::auth
