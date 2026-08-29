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

#include "app/app_server.h"
#include "auth/role_closure.h"
#include "catalog/database.h"
#include "catalog/ddl/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/role.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "query/transaction.h"

namespace sdb::pg {

LoginCheck RequireLoginRole(std::string_view user, std::string_view dbname,
                            const catalog::Permissions& perm) {
  // No ClientContext yet -- the connection is still being established -- so
  // this reads the committed cluster state.
  auto role = catalog::FindRole(nullptr, user);
  if (!role) {
    return {.error = SQL_ERROR_DATA(
              ERR_CODE(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
              ERR_MSG("role \"", user, "\" does not exist"))};
  }
  if (!role->CanLogin()) {
    return {.error = SQL_ERROR_DATA(
              ERR_CODE(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
              ERR_MSG("role \"", user, "\" is not permitted to log in"))};
  }
  if (!auth::ClosureFor(nullptr, role->GetId())
         ->Can(duckdb::CatalogType::DATABASE_ENTRY, perm,
               catalog::AclMode::Connect)) {
    return {.error = SQL_ERROR_DATA(
              ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
              ERR_MSG("permission denied for database \"", dbname, "\""),
              ERR_DETAIL("User does not have CONNECT privilege."))};
  }
  return {.role = role->GetId(), .superuser = role->IsSuperuser()};
}

}  // namespace sdb::pg
namespace sdb {

ConnectionContext::ConnectionContext(
  duckdb::ClientContext& duckdb_ctx, std::string_view user, ObjectId role_id,
  std::string_view dbname, ObjectId database_id, message::Buffer* send_buffer,
  int32_t backend_pid, network::CancelRegistry* cancel_registry)
  : Transaction{duckdb_ctx},
    _user{user},
    _database_name{dbname},
    _database_id{database_id},
    _backend_pid{backend_pid},
    _cancel_registry{cancel_registry},
    _send_buffer{send_buffer},
    _login_role_id{role_id},
    _session_role_id{role_id},
    _effective_role_id{role_id} {}

namespace {

std::string RoleName(const auth::RoleGraph& roles, ObjectId role,
                     const std::string& fallback) {
  auto name = roles.NameOf(role);
  return name.empty() ? fallback : std::string{name};
}

}  // namespace

std::string ConnectionContext::EffectiveUserName() const {
  return RoleName(*auth::RolesOf(&GetClientContext()), _effective_role_id,
                  _user);
}

std::string ConnectionContext::SessionUserName() const {
  return RoleName(*auth::RolesOf(&GetClientContext()), _session_role_id, _user);
}

std::string ConnectionContext::GetCurrentSchema() const {
  auto database_id = GetDatabaseId();
  auto search_path = GetSearchPath();
  auto it = absl::c_find_if(search_path, [&](const std::string& schema_name) {
    return catalog::FindSchema(nullptr, database_id, schema_name) != nullptr;
  });

  return it != search_path.end() ? *it : "";
}

}  // namespace sdb
