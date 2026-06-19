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
#include <string_view>

#include "catalog/object.h"
#include "pg/connection_context.h"

namespace sdb::catalog {

class Table;
class Column;

}  // namespace sdb::catalog
namespace sdb::pg {

void RequirePrivilege(ConnectionContext& ctx, const catalog::Object& object,
                      catalog::AclMode need);

// Column-level privilege check (PG ExecCheckOneRelPerms): `need` on `table`,
// satisfied table-wide or per-column on each of `columns`. An empty `columns`
// means "no specific column" -> requires `need` on any one column (SELECT
// count(*)). Throws 42501 "permission denied for table X" on failure.
void RequireColumnPrivilege(ConnectionContext& ctx, const catalog::Table& table,
                            catalog::AclMode need,
                            std::span<const catalog::Column* const> columns);

void EnforceRelationOwnership(ConnectionContext& ctx, std::string_view schema,
                              std::string_view name, std::string_view obj_type);

void EnforceDatabaseOwnership(ConnectionContext& ctx, std::string_view name);

struct CreateRoleOptions {
  bool login = false;
  bool superuser = false;
  bool inherit = true;
  bool has_password = false;
  std::string password;
  int conn_limit = -1;      // pg_authid.rolconnlimit (-1 = unlimited)
  std::string valid_until;  // empty -> NULL (no expiry)
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
  bool set_password = false;
  bool password_null = false;
  std::string password;
  int conn_limit = -2;  // -2 unspecified (-1 = unlimited)
  bool set_valid_until = false;
  std::string valid_until;  // empty + set_valid_until -> NULL
};

void AlterRole(ConnectionContext& ctx, std::string_view name,
               const AlterRoleOptions& opts);

// ALTER ROLE name SET/RESET <guc> -> per-role GUC settings (rolconfig). `op` is
// "SET", "RESET", or "RESET_ALL". `value` is the rendered config value (SET).
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

// GRANT/REVOKE ... ON DOMAIN: SereneDB has no domain types, so the named object
// is never a domain -- reject with PostgreSQL's semantic error (PG accepts the
// syntax then rejects the same way for a non-domain).
[[noreturn]] void ThrowNotADomain(std::string_view name);

void GrantObject(ConnectionContext& ctx, catalog::ObjectType type,
                 std::string_view privileges, std::string_view obj_name,
                 std::string_view grantee, bool revoke,
                 const GrantObjectOptions& opts = {});

// GRANT/REVOKE ... ON ALL <TABLES|SEQUENCES|FUNCTIONS> IN SCHEMA s: apply the
// grant to every existing object of `type` in schema `schema_name`.
void GrantObjectAllInSchema(ConnectionContext& ctx, catalog::ObjectType type,
                            std::string_view privileges,
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

// ALTER DEFAULT PRIVILEGES ... -> a pg_default_acl row. `objtype_char` is the
// PG catalog objtype ('r'/'S'/'f'/'T'/'n').
void AlterDefaultPrivileges(ConnectionContext& ctx, std::string_view privileges,
                            std::string_view objtype_char,
                            std::string_view grantee, bool revoke,
                            const DefaultPrivilegesOptions& opts);

}  // namespace sdb::pg
