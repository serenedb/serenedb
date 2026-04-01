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

#include <yaclib/async/make.hpp>

#include "app/app_server.h"
#include "basics/debugging.h"
#include "basics/errors.h"
#include "catalog/catalog.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

yaclib::Future<> AlterObjectSchema(ExecContext& context,
                                   const AlterObjectSchemaStmt& stmt) {
  auto& catalogs =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog = catalogs.Global();
  auto& conn_ctx = basics::downCast<ConnectionContext>(context);
  auto current_schema = conn_ctx.GetCurrentSchema();
  const auto db = context.GetDatabaseId();

  SDB_ASSERT(stmt.relation);
  const auto* rel = stmt.relation;
  std::string_view schema =
    rel->schemaname ? std::string_view{rel->schemaname} : current_schema;
  std::string_view name{rel->relname};
  std::string_view new_schema{stmt.newschema};

  Result r = catalog.AlterTableSchema(db, schema, name, new_schema);

  if (r.is(ERROR_SERVER_DATA_SOURCE_NOT_FOUND)) {
    if (!stmt.missing_ok) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                      ERR_MSG("relation \"", name, "\" does not exist"));
    }
    conn_ctx.AddNotice(SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_UNDEFINED_TABLE),
      ERR_MSG("relation \"", name, "\" does not exist, skipping")));
    return {};
  }

  if (r.is(ERROR_SERVER_ILLEGAL_NAME)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_SCHEMA_NAME),
                    ERR_MSG("schema \"", new_schema, "\" does not exist"));
  }

  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DUPLICATE_TABLE),
      ERR_MSG("relation \"", name, "\" already exists in schema \"", new_schema,
              "\""));
  }

  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  return {};
}

}  // namespace sdb::pg
