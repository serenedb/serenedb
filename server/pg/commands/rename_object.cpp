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
#include "pg/sql_collector.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {
namespace {

yaclib::Future<> RenameColumnOrConstraint(
  catalog::LogicalCatalog& catalog, ExecContext& context,
  const RenameStmt& stmt, std::string_view schema, std::string_view table_name,
  std::string_view old_name, std::string_view new_name) {
  const auto db = context.GetDatabaseId();
  const bool is_column = stmt.renameType == OBJECT_COLUMN;

  Result r = catalog.ChangeTable(
    db, schema, table_name,
    [&](const catalog::Table& table, std::shared_ptr<catalog::Table>& updated) {
      return is_column ? table.RenameColumn(updated, old_name, new_name)
                       : table.RenameConstraint(updated, old_name, new_name);
    });

  if (r.is(ERROR_SERVER_OBJECT_TYPE_MISMATCH)) {
    if (is_column) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("cannot rename columns of a non-table relation"));
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("constraint \"", old_name, "\" for table \"",
                            table_name, "\" does not exist"));
  }

  if (r.is(ERROR_SERVER_DATA_SOURCE_NOT_FOUND)) {
    if (!stmt.missing_ok) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                      ERR_MSG("relation \"", table_name, "\" does not exist"));
    }
    auto& conn_ctx = basics::downCast<ConnectionContext>(context);
    conn_ctx.AddNotice(SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_UNDEFINED_TABLE),
      ERR_MSG("relation \"", table_name, "\" does not exist, skipping")));
    return {};
  }

  if (r.is(ERROR_SERVER_ILLEGAL_NAME)) {
    if (is_column) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                      ERR_MSG("column \"", old_name, "\" does not exist"));
    } else {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("constraint \"", old_name, "\" for table \"",
                              table_name, "\" does not exist"));
    }
  }

  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    if (is_column) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_COLUMN),
                      ERR_MSG("column \"", new_name, "\" of relation \"",
                              table_name, "\" already exists"));
    } else {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                      ERR_MSG("constraint \"", new_name, "\" for relation \"",
                              table_name, "\" already exists"));
    }
  }

  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  return {};
}

}  // namespace

yaclib::Future<> RenameObject(ExecContext& context, const RenameStmt& stmt) {
  auto& catalogs =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog = catalogs.Global();
  auto& conn_ctx = basics::downCast<ConnectionContext>(context);
  auto current_schema = conn_ctx.GetCurrentSchema();
  const auto db = context.GetDatabaseId();

  std::string_view new_name{stmt.newname};
  std::string_view schema;
  std::string_view name;

  if (stmt.renameType == OBJECT_FUNCTION) {
    SDB_ASSERT(stmt.object);
    auto* func_with_args = castNode(ObjectWithArgs, stmt.object);
    auto [s, n] = ParseObjectName(func_with_args->objname,
                                  context.GetDatabase(), current_schema);
    schema = s;
    name = n;
  } else {
    SDB_ASSERT(stmt.relation);
    const auto* rel = stmt.relation;
    schema =
      rel->schemaname ? std::string_view{rel->schemaname} : current_schema;
    name = rel->relname;
  }

  if (stmt.renameType == OBJECT_COLUMN ||
      stmt.renameType == OBJECT_TABCONSTRAINT) {
    std::string_view subname{stmt.subname};
    return RenameColumnOrConstraint(catalog, context, stmt, schema, name,
                                    subname, new_name);
  }

  Result r;

  switch (stmt.renameType) {
    case OBJECT_TABLE:
    case OBJECT_INDEX:
      r = catalog.RenameRelation(db, schema, name, new_name);
      break;
    case OBJECT_VIEW:
      r = catalog.RenameView(db, schema, name, new_name);
      break;
    case OBJECT_FUNCTION:
      r = catalog.RenameFunction(db, schema, name, new_name);
      break;
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("ALTER ", magic_enum::enum_name(stmt.renameType),
                              " RENAME is not yet supported"));
  }

  if (r.is(ERROR_SERVER_OBJECT_TYPE_MISMATCH)) {
    auto object_name = ToPgObjectTypeName(stmt.renameType);
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
      ERR_MSG("\"", name, "\" is not ",
              basics::string_utils::GetArticle(object_name), " ", object_name));
  }

  if (r.is(ERROR_SERVER_DATA_SOURCE_NOT_FOUND) ||
      r.is(ERROR_SERVER_ILLEGAL_NAME)) {
    if (stmt.renameType == OBJECT_FUNCTION) {
      if (!stmt.missing_ok) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
          ERR_MSG("could not find a function named \"", name, "\""));
      }
      conn_ctx.AddNotice(SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
        ERR_MSG("function ", name, "() does not exist, skipping")));
    } else {
      if (!stmt.missing_ok) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                        ERR_MSG("relation \"", name, "\" does not exist"));
      }
      conn_ctx.AddNotice(SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_UNDEFINED_TABLE),
        ERR_MSG("relation \"", name, "\" does not exist, skipping")));
    }
    r = {};
  }

  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_TABLE),
                    ERR_MSG("relation \"", new_name, "\" already exists"));
  }

  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  return {};
}

}  // namespace sdb::pg
