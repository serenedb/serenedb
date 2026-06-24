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
  bool inherit = true;
  // PASSWORD / CONNECTION LIMIT / VALID UNTIL are parsed but unsupported: the
  // command rejects them when given (auth + connection limits are not
  // modelled).
  bool has_password = false;
  bool has_conn_limit = false;
  bool has_valid_until = false;
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
  int inherit = -1;
  // PASSWORD / CONNECTION LIMIT / VALID UNTIL are parsed but unsupported: the
  // command rejects them when given (auth + connection limits are not
  // modelled).
  bool has_password = false;
  bool has_conn_limit = false;
  bool has_valid_until = false;
};

void AlterRole(ConnectionContext& ctx, std::string_view name,
               const AlterRoleOptions& opts);

void AlterRoleConfig(ConnectionContext& ctx, std::string_view name,
                     std::string_view op, std::string_view setting,
                     std::string_view value);

void RenameRole(ConnectionContext& ctx, std::string_view name,
                std::string_view new_name);

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

}  // namespace sdb::pg
