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

int64_t ArgBigInt(const duckdb::FunctionParameters& params, size_t i,
                  const char* name) {
  return Arg<int64_t>(params, i, name);
}

std::vector<std::string> ArgStrList(const duckdb::FunctionParameters& params,
                                    size_t i) {
  std::vector<std::string> out;
  for (const auto& v : duckdb::ListValue::GetChildren(params.values[i])) {
    out.push_back(v.GetValue<std::string>());
  }
  return out;
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

void CreateRolePragma(duckdb::ClientContext& context,
                      const duckdb::FunctionParameters& params) {
  auto& conn_ctx = GetSereneDBContext(context);

  pg::CreateRoleOptions options;
  options.login = ArgBool(params, 1, "login");
  options.superuser = ArgBool(params, 2, "superuser");
  options.inherit = ArgBool(params, 3, "inherit");
  options.has_password = ArgBool(params, 4, "has_password");
  options.password = ArgStr(params, 5, "password");
  options.password_is_null = ArgBool(params, 6, "password_is_null");
  options.has_conn_limit = ArgBool(params, 7, "has_conn_limit");
  options.has_valid_until = ArgBool(params, 8, "has_valid_until");
  options.valid_until = ArgBigInt(params, 9, "valid_until");
  options.createdb = ArgBool(params, 10, "createdb");
  options.createrole = ArgBool(params, 11, "createrole");
  options.conn_limit = ArgBigInt(params, 12, "conn_limit");
  options.replication = ArgBool(params, 13, "replication");
  options.bypassrls = ArgBool(params, 14, "bypassrls");
  options.in_roles = ArgStrList(params, 15);
  options.role_members = ArgStrList(params, 16);
  options.admin_members = ArgStrList(params, 17);

  pg::CreateRole(conn_ctx, ArgStr(params, 0, "name"), options);
}

void DropRolePragma(duckdb::ClientContext& context,
                    const duckdb::FunctionParameters& params) {
  auto& conn_ctx = GetSereneDBContext(context);
  pg::DropRole(conn_ctx, ArgStr(params, 0, "name"),
               ArgBool(params, 1, "missing_ok"));
}

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
  opts.password_is_null = ArgBool(params, 8, "password_is_null");
  opts.has_conn_limit = ArgBool(params, 9, "has_conn_limit");
  opts.has_valid_until = ArgBool(params, 10, "has_valid_until");
  opts.valid_until = ArgBigInt(params, 11, "valid_until");
  opts.conn_limit = ArgBigInt(params, 12, "conn_limit");
  opts.replication = ArgInt(params, 13, "replication");
  opts.bypassrls = ArgInt(params, 14, "bypassrls");
  pg::AlterRole(conn_ctx, ArgStr(params, 0, "name"), opts);
}

void AlterRoleConfigPragma(duckdb::ClientContext& context,
                           const duckdb::FunctionParameters& params) {
  auto& conn_ctx = GetSereneDBContext(context);
  pg::AlterRoleConfig(conn_ctx, ArgStr(params, 0, "name"),
                      ArgStr(params, 1, "op"), ArgStr(params, 2, "setting"),
                      ArgStr(params, 3, "value"));
}

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

void RenameRolePragma(duckdb::ClientContext& context,
                      const duckdb::FunctionParameters& params) {
  auto& conn_ctx = GetSereneDBContext(context);
  pg::RenameRole(conn_ctx, ArgStr(params, 0, "name"),
                 ArgStr(params, 1, "new_name"));
}

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

void AlterOwnerPragma(duckdb::ClientContext& context,
                      const duckdb::FunctionParameters& params) {
  auto& conn_ctx = GetSereneDBContext(context);
  pg::AlterOwner(conn_ctx, ArgStr(params, 0, "objtype"),
                 ArgStr(params, 1, "name"), ArgStr(params, 2, "new_owner"));
}

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
     LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN,
     LogicalType::BIGINT, LogicalType::BOOLEAN, LogicalType::BOOLEAN,
     LogicalType::BIGINT, LogicalType::BOOLEAN, LogicalType::BOOLEAN,
     LogicalType::LIST(LogicalType::VARCHAR),
     LogicalType::LIST(LogicalType::VARCHAR),
     LogicalType::LIST(LogicalType::VARCHAR)}));

  loader.RegisterFunction(duckdb::PragmaFunction::PragmaCall(
    "serenedb_drop_role", DropRolePragma,
    {LogicalType::VARCHAR, LogicalType::BOOLEAN}));

  loader.RegisterFunction(duckdb::PragmaFunction::PragmaCall(
    "serenedb_alter_role", AlterRolePragma,
    {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::INTEGER,
     LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER,
     LogicalType::BOOLEAN, LogicalType::VARCHAR, LogicalType::BOOLEAN,
     LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BIGINT,
     LogicalType::BIGINT, LogicalType::INTEGER, LogicalType::INTEGER}));

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
