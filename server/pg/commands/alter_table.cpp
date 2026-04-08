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
#include "basics/string_utils.h"
#include "catalog/catalog.h"
#include "catalog/table.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

yaclib::Future<> AlterTable(ExecContext& context, const AlterTableStmt& stmt) {
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
  std::string_view table_name{rel->relname};

  VisitNodes(stmt.cmds, [&](const AlterTableCmd& cmd) {
    switch (cmd.subtype) {
      case AT_DropConstraint: {
        std::string_view constraint_name{cmd.name};
        Result r = catalog.ChangeTable(
          db, schema, table_name,
          [&](const catalog::Table& table,
              std::shared_ptr<catalog::Table>& updated) {
            return table.DropConstraint(updated, constraint_name);
          });

        if (r.is(ERROR_SERVER_OBJECT_TYPE_MISMATCH)) {
          auto actual_type =
            basics::string_utils::GetPluralFormLowerCase(r.errorMessage());
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
            ERR_MSG("ALTER action DROP CONSTRAINT cannot be performed on "
                    "relation \"",
                    table_name, "\""),
            ERR_DETAIL("This operation is not supported for ", actual_type,
                       "."));
        }

        if (r.is(ERROR_SERVER_DATA_SOURCE_NOT_FOUND)) {
          if (!stmt.missing_ok) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_UNDEFINED_TABLE),
              ERR_MSG("relation \"", table_name, "\" does not exist"));
          }
          conn_ctx.AddNotice(SQL_ERROR_DATA(
            ERR_CODE(ERRCODE_UNDEFINED_TABLE),
            ERR_MSG("relation \"", table_name, "\" does not exist, skipping")));
          return;
        }

        if (r.is(ERROR_SERVER_ILLEGAL_NAME)) {
          if (!cmd.missing_ok) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
              ERR_MSG("constraint \"", constraint_name, "\" of relation \"",
                      table_name, "\" does not exist"));
          }
          conn_ctx.AddNotice(SQL_ERROR_DATA(
            ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
            ERR_MSG("constraint \"", constraint_name, "\" of relation \"",
                    table_name, "\" does not exist, skipping")));
          return;
        }

        if (!r.ok()) {
          SDB_THROW(std::move(r));
        }
        return;
      }
      default:
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ERR_MSG("ALTER TABLE subcommand is not yet supported"));
    }
  });

  return {};
}

}  // namespace sdb::pg
