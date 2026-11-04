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

#include "pg/connection_context.h"

#include <absl/strings/str_split.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/main/client_data.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string>
#include <string_view>
#include <utility>

#include "catalog/cluster.h"
#include "catalog/entry/database.h"
#include "catalog/entry/role.h"
#include "network/pg/wire_frames.h"
#include "pg/progress_registry.h"
#include "query/transaction.h"
#include "server/utils/app_server.h"

namespace sdb::pg {

LoginCheck RequireLoginRole(std::string_view user, std::string_view dbname) {
  // No ClientContext yet -- the connection is still being established -- so
  // this reads the committed cluster state.
  struct {
    duckdb::idx_t oid = 0;
    bool can_login = false;
    bool superuser = false;
    int32_t conn_limit = -1;
    std::string name;
  } role;
  if (!catalog::ReadRole(user, [&](const catalog::RoleCatalogEntry& entry) {
        role.oid = entry.oid;
        role.can_login = entry.CanLogin();
        role.superuser = entry.IsSuperuser();
        role.conn_limit = entry.ConnLimit();
        role.name = entry.name.GetIdentifierName();
      })) {
    return {.error = SQL_ERROR_DATA(
              ERR_CODE(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
              ERR_MSG("role \"", user, "\" does not exist"))};
  }
  if (!role.can_login) {
    return {.error = SQL_ERROR_DATA(
              ERR_CODE(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
              ERR_MSG("role \"", user, "\" is not permitted to log in"))};
  }
  if (!role.superuser && role.conn_limit >= 0 &&
      ProgressRegistry::Instance().SessionsOf(role.oid) >=
        static_cast<size_t>(role.conn_limit)) {
    return {.error = SQL_ERROR_DATA(
              ERR_CODE(ERRCODE_TOO_MANY_CONNECTIONS),
              ERR_MSG("too many connections for role \"", role.name, "\""))};
  }
  struct {
    duckdb::idx_t oid = 0;
    duckdb::Permissions permissions;
    bool allow_connections = true;
    int32_t conn_limit = -1;
  } database;
  if (!catalog::ReadDatabase(
        dbname, [&](const catalog::DatabaseCatalogEntry& entry) {
          database.oid = entry.oid;
          database.permissions = entry.permissions;
          database.allow_connections = entry.AllowConnections();
          database.conn_limit = entry.ConnLimit();
        })) {
    return {.error = SQL_ERROR_DATA(
              ERR_CODE(ERRCODE_INVALID_CATALOG_NAME),
              ERR_MSG("database \"", dbname, "\" does not exist"))};
  }
  if (!database.allow_connections) {
    return {.error = SQL_ERROR_DATA(
              ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
              ERR_MSG("database \"", dbname,
                      "\" is not currently accepting connections"))};
  }
  const auto roles = catalog::RolesOf(nullptr);
  duckdb::RoleAccess missing;
  missing.role = role.oid;
  const auto access = roles->Access(role.oid);
  if (!(access ? *access : missing)
         .Can(duckdb::CatalogType::DATABASE_ENTRY, database.permissions,
              duckdb::AclMode::Connect)) {
    return {.error = SQL_ERROR_DATA(
              ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
              ERR_MSG("permission denied for database \"", dbname, "\""),
              ERR_DETAIL("User does not have CONNECT privilege."))};
  }
  if (!role.superuser && database.conn_limit >= 0 &&
      ProgressRegistry::Instance().SessionsIn(database.oid) >=
        static_cast<size_t>(database.conn_limit)) {
    return {.error = SQL_ERROR_DATA(
              ERR_CODE(ERRCODE_TOO_MANY_CONNECTIONS),
              ERR_MSG("too many connections for database \"", dbname, "\""))};
  }
  return {
    .role = role.oid, .database = database.oid, .superuser = role.superuser};
}

}  // namespace sdb::pg
namespace sdb {

ConnectionContext::ConnectionContext(
  duckdb::ClientContext& duckdb_ctx, std::string_view user,
  std::string_view system_user, duckdb::idx_t role_id, std::string_view dbname,
  duckdb::idx_t database_id, message::Buffer* send_buffer, int32_t backend_pid,
  network::CancelRegistry* cancel_registry)
  : Transaction{duckdb_ctx},
    _user{user},
    _system_user{system_user},
    _database_id{database_id},
    _backend_pid{backend_pid},
    _cancel_registry{cancel_registry},
    _send_buffer{send_buffer},
    _login_role_id{role_id} {
  duckdb_ctx.session_user = user;
  duckdb_ctx.session_role = role_id;
  duckdb_ctx.client_data->default_database = duckdb::Identifier{dbname};
  duckdb_ctx.SetCurrentRole(role_id);
}

namespace {

std::string RoleName(const duckdb::SecuritySnapshot& roles, duckdb::idx_t role,
                     std::string_view fallback) {
  auto name = roles.NameOf(role);
  return std::string{name.empty() ? fallback : name};
}

}  // namespace

std::string ConnectionContext::EffectiveUserName() const {
  return RoleName(*catalog::RolesOf(&GetClientContext()), GetRoleId(), _user);
}

std::string ConnectionContext::SessionUserName() const {
  return RoleName(*catalog::RolesOf(&GetClientContext()), GetSessionRoleId(),
                  _user);
}

void ConnectionContext::ApplyRoleSettings() {
  duckdb::vector<std::string> global_config;
  catalog::ReadRole(catalog::kRootRole,
                    [&](const catalog::RoleCatalogEntry& root) {
                      global_config = root.AllRolesConfig();
                    });
  duckdb::vector<std::string> database_config;
  duckdb::vector<std::string> database_role_config;
  catalog::ReadDatabase(
    GetDatabase(), [&](const catalog::DatabaseCatalogEntry& database) {
      if (const auto* config = database.SettingsOf(0)) {
        database_config = *config;
      }
      if (const auto* config = database.SettingsOf(_login_role_id)) {
        database_role_config = *config;
      }
    });
  duckdb::vector<std::string> role_config;
  catalog::ReadRole(_user, [&](const catalog::RoleCatalogEntry& role) {
    role_config = role.Config();
  });
  for (const auto* config : {&global_config, &database_config, &role_config,
                             &database_role_config}) {
    for (const std::string_view entry : *config) {
      const std::pair<std::string_view, std::string_view> setting =
        absl::StrSplit(entry, absl::MaxSplits('=', 1));
      if (setting.second.empty()) {
        continue;
      }
      try {
        SetSettingChecked(setting.first, std::string{setting.second});
      } catch (const std::exception& exception) {
        AddNotice(network::pg::ToSqlError(exception));
      }
    }
  }
}

void ConnectionContext::PreventInTransactionBlock(
  std::string_view statement) const {
  if (_in_transaction_block) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_ACTIVE_SQL_TRANSACTION),
      ERR_MSG(statement, " cannot run inside a transaction block"));
  }
}

}  // namespace sdb
