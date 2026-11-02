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
#include <duckdb/main/config.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <span>
#include <string_view>

#include "auth/role_closure.h"
#include "catalog/cluster.h"
#include "catalog/entry/database.h"
#include "catalog/entry/role.h"
#include "query/config.h"

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

void RequireAttributes(const duckdb::RoleAccess& caller, std::string_view verb,
                       std::string_view action, RoleOption options) {
  for (const auto& [option, attribute] :
       {std::pair{RoleOption::CreateDb, "CREATEDB"},
        std::pair{RoleOption::Replication, "REPLICATION"},
        std::pair{RoleOption::BypassRls, "BYPASSRLS"}}) {
    if (HasOption(options, option) && !caller.Has(option)) {
      DenyRoleAction(
        verb, absl::StrCat("Only roles with the ", attribute, " attribute may ",
                           action, attribute, " attribute."));
    }
  }
}

void RequireNotSuperuser(const duckdb::CatalogEntry& role,
                         std::string_view verb) {
  if (role.Cast<catalog::RoleCatalogEntry>().IsSuperuser()) {
    DenyRoleAction(
      verb, absl::StrCat("Only roles with the SUPERUSER attribute may ", verb,
                         " roles with the SUPERUSER attribute."));
  }
}

bool AdministersRole(duckdb::ClientContext& context,
                     const duckdb::RoleAccess& caller,
                     const duckdb::CatalogEntry& role) {
  return caller.Has(RoleOption::CreateRole) && IsAdminOf(context, role.oid);
}

[[noreturn]] void DenyRoleAdmin(const duckdb::CatalogEntry& role,
                                std::string_view verb) {
  DenyRoleAction(verb,
                 absl::StrCat("Only roles with the CREATEROLE attribute and "
                              "the ADMIN option on role \"",
                              role.name.GetIdentifierName(), "\" may ", verb,
                              " this role."));
}

void RequireRoleAdmin(duckdb::ClientContext& context,
                      const duckdb::RoleAccess& caller,
                      const duckdb::CatalogEntry& role, std::string_view verb) {
  RequireNotSuperuser(role, verb);
  if (!AdministersRole(context, caller, role)) {
    DenyRoleAdmin(role, verb);
  }
}

std::string_view CanonicalSetting(std::string_view name) {
  const auto option =
    duckdb::DBConfig::GetOptionByName(duckdb::Identifier{name});
  return option ? std::string_view{option->name} : name;
}

void CheckConfigChange(duckdb::ClientContext& context,
                       std::span<const std::string> set_config,
                       duckdb::vector<std::string>& reset_config,
                       bool& reset_all_config,
                       std::span<const std::string> current) {
  for (const std::string_view entry : set_config) {
    const auto key = catalog::ConfigKey(entry);
    RequireSuperuserToSet(context, key, CanonicalSetting(key),
                          duckdb::SetScope::SESSION, false);
  }
  for (const std::string_view key : reset_config) {
    RequireSuperuserToSet(context, key, CanonicalSetting(key),
                          duckdb::SetScope::SESSION, false);
  }
  if (reset_all_config) {
    reset_all_config = false;
    for (const std::string_view entry : current) {
      const auto key = catalog::ConfigKey(entry);
      if (SettingContext(CanonicalSetting(key)) == "user") {
        reset_config.emplace_back(key);
      }
    }
  }
}

void RequireGrantableRole(duckdb::CatalogTransaction transaction,
                          std::string_view name, bool revoke) {
  auto role = FindRole(transaction, name);
  if (!role) {
    return;
  }
  const std::string_view verb = revoke ? "revoke" : "grant";
  const auto& role_name = role->name.GetIdentifierName();
  if (role->Cast<catalog::RoleCatalogEntry>().IsSuperuser()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to ", verb, " role \"", role_name, "\""),
      ERR_DETAIL("Only roles with the SUPERUSER attribute may ", verb,
                 " roles with the SUPERUSER attribute."));
  }
  if (!IsAdminOf(transaction.GetContext(), role->oid)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to ", verb, " role \"", role_name, "\""),
      ERR_DETAIL("Only roles with the ADMIN option on role \"", role_name,
                 "\" may ", verb, " this role."));
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
  if (HasOption(info.options, RoleOption::Superuser)) {
    DenyRoleAction("create",
                   "Only roles with the SUPERUSER attribute may create roles "
                   "with the SUPERUSER attribute.");
  }
  RequireAttributes(*caller, "create", "create roles with the ", info.options);
  for (const auto& name : info.in_roles) {
    RequireGrantableRole(transaction, name.GetIdentifierName(), false);
  }
}

void CheckAlterRole(duckdb::CatalogTransaction transaction,
                    duckdb::AlterRoleInfo& info) {
  if (!transaction.HasContext() || !transaction.GetContext().principal) {
    return;
  }
  auto role =
    FindRole(transaction, info.GetQualifiedName().Name().GetIdentifierName());
  if (!role) {
    return;
  }
  if (info.grant_role.empty() && IsPredefinedRole(role->oid)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_RESERVED_NAME),
      ERR_MSG("role name \"", role->name.GetIdentifierName(), "\" is reserved"),
      ERR_DETAIL("Cannot alter reserved roles."));
  }
  const auto* caller = Enforced(transaction);
  if (!caller) {
    return;
  }
  if (!info.grant_role.empty()) {
    RequireGrantableRole(transaction, info.grant_role, info.revoke);
    return;
  }
  auto& context = transaction.GetContext();
  if (!info.new_name.empty()) {
    RequireRoleAdmin(context, *caller, *role, "rename");
    return;
  }
  const auto changed = info.set_options | info.clear_options;
  const bool attributes =
    changed != RoleOption::None || info.set_conn_limit || info.set_valid_until;
  const bool other = role->oid != caller->role;
  RequireNotSuperuser(*role, "alter");
  if (HasOption(changed, RoleOption::Superuser)) {
    DenyRoleAction("alter",
                   "Only roles with the SUPERUSER attribute may change the "
                   "SUPERUSER attribute.");
  }
  if (AdministersRole(context, *caller, *role)) {
    RequireAttributes(*caller, "alter", "change the ", changed);
  } else if (attributes) {
    DenyRoleAdmin(*role, "alter");
  } else if (other && (info.set_password || info.null_password)) {
    DenyRoleAction("alter",
                   "To change another role's password, the current user must "
                   "have the CREATEROLE attribute and the ADMIN option on the "
                   "role.");
  } else if (other) {
    DenyRoleAdmin(*role, "alter");
  }
  CheckConfigChange(context, info.set_config, info.reset_config,
                    info.reset_all_config,
                    role->Cast<catalog::RoleCatalogEntry>().Config());
}

void CheckAlterDatabaseSettings(duckdb::CatalogTransaction transaction,
                                duckdb::AlterDatabaseSettingsInfo& info) {
  if (!transaction.HasContext() || !transaction.GetContext().principal) {
    return;
  }
  auto& context = transaction.GetContext();
  const auto& name = info.GetQualifiedName().Name();
  const auto database = catalog::ClusterOf(context)
                          .GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
                          .GetEntry(transaction, name);
  if (!database) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
      ERR_MSG("database \"", name.GetIdentifierName(), "\" does not exist"));
  }
  const auto* caller = Enforced(transaction);
  if (!caller) {
    return;
  }
  if (info.role_id == 0) {
    if (!caller->MemberOf(database->permissions.owner)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("must be owner of database ", name.GetIdentifierName()));
    }
  } else if (info.role_id != caller->role) {
    if (const auto role =
          FindRole(transaction, RolesOf(&context)->NameOf(info.role_id))) {
      RequireRoleAdmin(context, *caller, *role, "alter");
    }
  }
  const auto* current =
    database->Cast<catalog::DatabaseCatalogEntry>().SettingsOf(info.role_id);
  CheckConfigChange(context, info.set_config, info.reset_config,
                    info.reset_all_config,
                    current ? std::span<const std::string>{*current}
                            : std::span<const std::string>{});
}

void CheckDropRole(duckdb::CatalogTransaction transaction,
                   const duckdb::DropInfo& info) {
  if (!transaction.HasContext() || !transaction.GetContext().principal) {
    return;
  }
  const auto* caller = Enforced(transaction);
  if (caller && !caller->Has(RoleOption::CreateRole)) {
    DenyRoleAction("drop",
                   "Only roles with the CREATEROLE attribute and the ADMIN "
                   "option on the target roles may drop roles.");
  }
  auto role =
    FindRole(transaction, info.GetQualifiedName().Name().GetIdentifierName());
  if (!role) {
    return;
  }
  if (IsPredefinedRole(role->oid)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    ERR_MSG("cannot drop role ", role->name.GetIdentifierName(),
                            " because it is required by the database system"));
  }
  if (caller) {
    RequireRoleAdmin(transaction.GetContext(), *caller, *role, "drop");
  }
}

}  // namespace sdb::auth
