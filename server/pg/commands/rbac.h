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

#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/object.h"
#include "pg/connection_context.h"
namespace sdb::pg {

struct ParsedPriv {
  std::string keyword;
  std::vector<std::string> columns;
};

struct CreateRoleOptions {
  bool login = false;
  bool superuser = false;
  bool createdb = false;
  bool createrole = false;
  bool replication = false;
  bool bypassrls = false;
  bool inherit = true;
  bool has_password = false;
  std::string password;
  bool password_is_null = false;
  bool has_conn_limit = false;
  int64_t conn_limit = -1;
  bool has_valid_until = false;
  int64_t valid_until = 0;
  std::vector<std::string> in_roles;
  std::vector<std::string> role_members;
  std::vector<std::string> admin_members;
};

struct MemberOptions {
  int admin = -1;
  int inherit = -1;
  int set = -1;
  bool admin_option_only = false;
};

void CreateRole(ConnectionContext& ctx, std::string_view name,
                const CreateRoleOptions& options);

void DropRole(ConnectionContext& ctx, std::string_view name, bool missing_ok);

struct AlterRoleOptions {
  int login = -1;
  int superuser = -1;
  int createdb = -1;
  int createrole = -1;
  int replication = -1;
  int bypassrls = -1;
  int inherit = -1;
  bool has_password = false;
  std::string password;
  bool password_is_null = false;
  bool has_conn_limit = false;
  int64_t conn_limit = -1;
  bool has_valid_until = false;
  int64_t valid_until = 0;
};

void AlterRole(ConnectionContext& ctx, std::string_view name,
               const AlterRoleOptions& opts);

void AlterRoleConfig(ConnectionContext& ctx, std::string_view name,
                     std::string_view op, std::string_view setting,
                     std::string_view value);

void RenameRole(ConnectionContext& ctx, std::string_view name,
                std::string_view new_name);

std::string SetRole(ConnectionContext& ctx, std::string_view name);
void ResetRole(ConnectionContext& ctx);
std::string SetSessionAuthorization(ConnectionContext& ctx,
                                    std::string_view name);
void ResetSessionAuthorization(ConnectionContext& ctx);

struct GrantObjectOptions {
  bool with_grant_option = false;
  bool grant_option_only = false;
  bool cascade = false;
  std::string granted_by;
};

void GrantObject(ConnectionContext& ctx, catalog::ObjectType type,
                 std::span<const ParsedPriv> privileges,
                 std::string_view obj_name, std::string_view grantee,
                 bool revoke, const GrantObjectOptions& opts = {});

void GrantObjectAllInSchema(ConnectionContext& ctx, catalog::ObjectType type,
                            std::span<const ParsedPriv> privileges,
                            std::string_view schema_name,
                            std::string_view grantee, bool revoke,
                            const GrantObjectOptions& opts = {});

void GrantRole(ConnectionContext& ctx, std::string_view role,
               std::string_view member, bool revoke,
               const MemberOptions& opts = {});

void AlterOwner(ConnectionContext& ctx, std::string_view obj_type,
                std::string_view name, std::string_view new_owner);

struct DefaultPrivilegesOptions {
  bool with_grant_option = false;
  bool grant_option_only = false;
  bool cascade = false;
  std::string for_role;   // empty -> current user
  std::string in_schema;  // empty -> all schemas (defaclnamespace = 0)
};

void AlterDefaultPrivileges(ConnectionContext& ctx,
                            std::span<const ParsedPriv> privileges,
                            std::string_view objtype_char,
                            std::string_view grantee, bool revoke,
                            const DefaultPrivilegesOptions& opts);


// Row-Level Security DDL. `cmd` is one of ALL/SELECT/INSERT/UPDATE/DELETE;
// `roles` may include CURRENT_USER/SESSION_USER/PUBLIC. Empty roles -> PUBLIC.
struct CreatePolicyOptions {
  bool permissive = true;
  std::string cmd = "ALL";
  std::vector<std::string> roles;
  bool has_using = false;
  std::string using_text;
  bool has_check = false;
  std::string check_text;
};
void CreatePolicy(ConnectionContext& ctx, std::string_view name,
                  std::string_view table, const CreatePolicyOptions& opts);

struct AlterPolicyOptions {
  bool is_rename = false;
  std::string new_name;
  bool has_roles = false;
  std::vector<std::string> roles;
  bool has_using = false;
  std::string using_text;
  bool has_check = false;
  std::string check_text;
};
void AlterPolicy(ConnectionContext& ctx, std::string_view name,
                 std::string_view table, const AlterPolicyOptions& opts);

void DropPolicy(ConnectionContext& ctx, std::string_view name,
                std::string_view table, bool if_exists);

// action is ENABLE / DISABLE / FORCE / NOFORCE.
void SetTableRowSecurity(ConnectionContext& ctx, std::string_view table,
                         std::string_view action);
}  // namespace sdb::pg
