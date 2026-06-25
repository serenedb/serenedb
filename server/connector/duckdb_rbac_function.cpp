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

#include "connector/duckdb_rbac_function.h"

#include <duckdb/common/types/value.hpp>
#include <duckdb/function/pragma_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <optional>

#include "connector/duckdb_client_state.h"
#include "pg/commands/rbac.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"

namespace sdb::connector {
namespace {

using duckdb::LogicalType;

// Read a client-supplied pragma arg, rejecting NULL before GetValue<T>() (which
// would throw or read uninitialized data).
template<typename T>
T Arg(const duckdb::FunctionParameters& params, size_t i, const char* name) {
  if (params.values[i].IsNull()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_NULL_VALUE_NOT_ALLOWED),
      ERR_MSG("serenedb RBAC pragma: argument '", name, "' must not be NULL"));
  }
  return params.values[i].GetValue<T>();
}

std::string ArgStr(const duckdb::FunctionParameters& params, size_t i,
                   const char* name) {
  return Arg<std::string>(params, i, name);
}

bool ArgBool(const duckdb::FunctionParameters& params, size_t i,
             const char* name) {
  return Arg<bool>(params, i, name);
}

int32_t ArgInt(const duckdb::FunctionParameters& params, size_t i,
               const char* name) {
  return Arg<int32_t>(params, i, name);
}

LogicalType PrivListType() {
  return LogicalType::LIST(LogicalType::STRUCT(
    {{"keyword", LogicalType::VARCHAR},
     {"columns", LogicalType::LIST(LogicalType::VARCHAR)}}));
}

std::vector<pg::ParsedPriv> ArgPrivList(
  const duckdb::FunctionParameters& params, size_t i, const char* name) {
  if (params.values[i].IsNull()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_NULL_VALUE_NOT_ALLOWED),
      ERR_MSG("serenedb RBAC pragma: argument '", name, "' must not be NULL"));
  }
  std::vector<pg::ParsedPriv> out;
  for (const auto& entry : duckdb::ListValue::GetChildren(params.values[i])) {
    const auto& fields = duckdb::StructValue::GetChildren(entry);
    pg::ParsedPriv p;
    p.keyword = fields[0].GetValue<std::string>();
    for (const auto& col : duckdb::ListValue::GetChildren(fields[1])) {
      p.columns.push_back(col.GetValue<std::string>());
    }
    out.push_back(std::move(p));
  }
  return out;
}

// PRAGMA serenedb_create_role('name', login, superuser, inherit, has_password,
//   has_conn_limit, has_valid_until)
//   [1] login BOOLEAN, [2] superuser BOOLEAN, [3] inherit BOOLEAN (INHERIT vs
//   NOINHERIT). PASSWORD / CONNECTION LIMIT / VALID UNTIL are parsed by the
//   grammar but unsupported; the trailing flags only signal that one was given
//   so the command can reject it.
void CreateRolePragma(duckdb::ClientContext& context,
                      const duckdb::FunctionParameters& params) {
  auto& conn_ctx = GetSereneDBContext(context);

  pg::CreateRoleOptions options;
  options.login = ArgBool(params, 1, "login");
  options.superuser = ArgBool(params, 2, "superuser");
  options.inherit = ArgBool(params, 3, "inherit");
  options.has_password = ArgBool(params, 4, "has_password");
  options.password = ArgStr(params, 5, "password");
  options.has_conn_limit = ArgBool(params, 6, "has_conn_limit");
  options.has_valid_until = ArgBool(params, 7, "has_valid_until");

  pg::CreateRole(conn_ctx, ArgStr(params, 0, "name"), options);
}

// PRAGMA serenedb_drop_role('name', missing_ok)
void DropRolePragma(duckdb::ClientContext& context,
                    const duckdb::FunctionParameters& params) {
  auto& conn_ctx = GetSereneDBContext(context);
  pg::DropRole(conn_ctx, ArgStr(params, 0, "name"),
               ArgBool(params, 1, "missing_ok"));
}

// PRAGMA serenedb_alter_role('name', login, super, createdb, createrole,
//   inherit, has_password, has_conn_limit, has_valid_until). Attribute flags
//   are tri-state ints (-1 unspecified / 0 false / 1 true). PASSWORD /
//   CONNECTION LIMIT / VALID UNTIL are parsed but unsupported; the trailing
//   flags only signal that one was given so the command can reject it.
void AlterRolePragma(duckdb::ClientContext& context,
                     const duckdb::FunctionParameters& params) {
  auto& conn_ctx = GetSereneDBContext(context);
  pg::AlterRoleOptions opts;
  opts.login = ArgInt(params, 1, "login");
  opts.superuser = ArgInt(params, 2, "superuser");
  opts.createdb = ArgInt(params, 3, "createdb");
  opts.createrole = ArgInt(params, 4, "createrole");
  opts.inherit = ArgInt(params, 5, "inherit");
  opts.has_password = ArgBool(params, 6, "has_password");
  opts.password = ArgStr(params, 7, "password");
  opts.has_conn_limit = ArgBool(params, 8, "has_conn_limit");
  opts.has_valid_until = ArgBool(params, 9, "has_valid_until");
  pg::AlterRole(conn_ctx, ArgStr(params, 0, "name"), opts);
}

// PRAGMA serenedb_alter_role_config('name', op, setting, value). op is
//   "SET" / "RESET" / "RESET_ALL".
void AlterRoleConfigPragma(duckdb::ClientContext& context,
                           const duckdb::FunctionParameters& params) {
  auto& conn_ctx = GetSereneDBContext(context);
  pg::AlterRoleConfig(conn_ctx, ArgStr(params, 0, "name"),
                      ArgStr(params, 1, "op"), ArgStr(params, 2, "setting"),
                      ArgStr(params, 3, "value"));
}

// PRAGMA serenedb_alter_default_privileges('privileges', 'objtype_char',
//   'grantee', revoke, with_grant_option, 'for_role', 'in_schema',
//   grant_option_only, cascade)
void AlterDefaultPrivilegesPragma(duckdb::ClientContext& context,
                                  const duckdb::FunctionParameters& params) {
  auto& conn_ctx = GetSereneDBContext(context);
  pg::DefaultPrivilegesOptions opts;
  opts.with_grant_option = ArgBool(params, 4, "with_grant_option");
  opts.for_role = ArgStr(params, 5, "for_role");
  opts.in_schema = ArgStr(params, 6, "in_schema");
  opts.grant_option_only = ArgBool(params, 7, "grant_option_only");
  opts.cascade = ArgBool(params, 8, "cascade");
  pg::AlterDefaultPrivileges(conn_ctx, ArgPrivList(params, 0, "privileges"),
                             ArgStr(params, 1, "objtype_char"),
                             ArgStr(params, 2, "grantee"),
                             ArgBool(params, 3, "revoke"), opts);
}

// PRAGMA serenedb_rename_role('name', 'new_name')
void RenameRolePragma(duckdb::ClientContext& context,
                      const duckdb::FunctionParameters& params) {
  auto& conn_ctx = GetSereneDBContext(context);
  pg::RenameRole(conn_ctx, ArgStr(params, 0, "name"),
                 ArgStr(params, 1, "new_name"));
}

// The bulk GRANT ... ON ALL <kind> IN SCHEMA forms carry a marker objtype
// string. Returns the per-object catalog type, or nullopt for a single object.
std::optional<catalog::ObjectType> BulkObjTypeOf(std::string_view word) {
  if (word == "ALL_TABLES_IN_SCHEMA") {
    return catalog::ObjectType::Table;
  }
  if (word == "ALL_SEQUENCES_IN_SCHEMA") {
    return catalog::ObjectType::Sequence;
  }
  if (word == "ALL_FUNCTIONS_IN_SCHEMA") {
    return catalog::ObjectType::PgSqlFunction;
  }
  return std::nullopt;
}

// PRAGMA serenedb_grant_table('privileges', 'name', 'grantee', revoke,
//   with_grant_option, objtype, grant_option_only, cascade, 'granted_by')
void GrantTablePragma(duckdb::ClientContext& context,
                      const duckdb::FunctionParameters& params) {
  auto& conn_ctx = GetSereneDBContext(context);
  pg::GrantObjectOptions opts;
  opts.with_grant_option = ArgBool(params, 4, "with_grant_option");
  opts.grant_option_only = ArgBool(params, 6, "grant_option_only");
  opts.cascade = ArgBool(params, 7, "cascade");
  opts.granted_by = ArgStr(params, 8, "granted_by");
  const auto objtype = ArgStr(params, 5, "objtype");
  const auto privileges = ArgPrivList(params, 0, "privileges");
  if (auto bulk = BulkObjTypeOf(objtype)) {
    pg::GrantObjectAllInSchema(
      conn_ctx, *bulk, privileges, ArgStr(params, 1, "name"),
      ArgStr(params, 2, "grantee"), ArgBool(params, 3, "revoke"), opts);
    return;
  }
  auto type = pg::FromPgObjectTypeName(objtype);
  if (type == catalog::ObjectType::Invalid) {
    type = catalog::ObjectType::Table;
  }
  pg::GrantObject(conn_ctx, type, privileges, ArgStr(params, 1, "name"),
                  ArgStr(params, 2, "grantee"), ArgBool(params, 3, "revoke"),
                  opts);
}

// PRAGMA serenedb_alter_owner('objtype', 'name', 'new_owner')
void AlterOwnerPragma(duckdb::ClientContext& context,
                      const duckdb::FunctionParameters& params) {
  auto& conn_ctx = GetSereneDBContext(context);
  pg::AlterOwner(conn_ctx, ArgStr(params, 0, "objtype"),
                 ArgStr(params, 1, "name"), ArgStr(params, 2, "new_owner"));
}

// PRAGMA serenedb_grant_role('role', 'member', revoke, admin, inherit, set,
//   option_only). admin/inherit/set are tri-state ints (-1 unspecified / 0
//   false / 1 true). option_only marks REVOKE ADMIN OPTION FOR (keep the edge,
//   drop only its admin option).
void GrantRolePragma(duckdb::ClientContext& context,
                     const duckdb::FunctionParameters& params) {
  auto& conn_ctx = GetSereneDBContext(context);
  pg::MemberOptions opts;
  opts.admin = ArgInt(params, 3, "admin");
  opts.inherit = ArgInt(params, 4, "inherit");
  opts.set = ArgInt(params, 5, "set");
  opts.admin_option_only = ArgBool(params, 6, "option_only");
  pg::GrantRole(conn_ctx, ArgStr(params, 0, "role"),
                ArgStr(params, 1, "member"), ArgBool(params, 2, "revoke"),
                opts);
}

}  // namespace

void RegisterRbacPragmas(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");

  loader.RegisterFunction(duckdb::PragmaFunction::PragmaCall(
    "serenedb_create_role", CreateRolePragma,
    {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN,
     LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::VARCHAR,
     LogicalType::BOOLEAN, LogicalType::BOOLEAN}));

  loader.RegisterFunction(duckdb::PragmaFunction::PragmaCall(
    "serenedb_drop_role", DropRolePragma,
    {LogicalType::VARCHAR, LogicalType::BOOLEAN}));

  loader.RegisterFunction(duckdb::PragmaFunction::PragmaCall(
    "serenedb_alter_role", AlterRolePragma,
    {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::INTEGER,
     LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER,
     LogicalType::BOOLEAN, LogicalType::VARCHAR, LogicalType::BOOLEAN,
     LogicalType::BOOLEAN}));

  loader.RegisterFunction(duckdb::PragmaFunction::PragmaCall(
    "serenedb_alter_role_config", AlterRoleConfigPragma,
    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
     LogicalType::VARCHAR}));

  loader.RegisterFunction(duckdb::PragmaFunction::PragmaCall(
    "serenedb_alter_default_privileges", AlterDefaultPrivilegesPragma,
    {PrivListType(), LogicalType::VARCHAR, LogicalType::VARCHAR,
     LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::VARCHAR,
     LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN}));

  loader.RegisterFunction(duckdb::PragmaFunction::PragmaCall(
    "serenedb_rename_role", RenameRolePragma,
    {LogicalType::VARCHAR, LogicalType::VARCHAR}));

  loader.RegisterFunction(duckdb::PragmaFunction::PragmaCall(
    "serenedb_grant_table", GrantTablePragma,
    {PrivListType(), LogicalType::VARCHAR, LogicalType::VARCHAR,
     LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::VARCHAR,
     LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::VARCHAR}));

  loader.RegisterFunction(duckdb::PragmaFunction::PragmaCall(
    "serenedb_grant_role", GrantRolePragma,
    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN,
     LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER,
     LogicalType::BOOLEAN}));

  loader.RegisterFunction(duckdb::PragmaFunction::PragmaCall(
    "serenedb_alter_owner", AlterOwnerPragma,
    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}));
}

}  // namespace sdb::connector
