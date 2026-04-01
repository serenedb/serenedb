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
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

namespace {

std::string_view ObjectTypeName(ObjectType rename_type) {
  switch (rename_type) {
    case OBJECT_TABLE:
      return "table";
    case OBJECT_VIEW:
      return "view";
    case OBJECT_INDEX:
      return "index";
    case OBJECT_FUNCTION:
      return "function";
    default:
      return "object";
  }
}

}  // namespace

yaclib::Future<> RenameObject(ExecContext& context, const RenameStmt& stmt) {
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
  std::string_view new_name{stmt.newname};

  Result r;

  switch (stmt.renameType) {
    case OBJECT_TABLE:
      r = catalog.RenameTable(db, schema, name, new_name);
      break;
    case OBJECT_VIEW:
      r = catalog.RenameView(db, schema, name, new_name);
      break;
    default:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("ALTER ", magic_enum::enum_name(stmt.renameType),
                " RENAME is not yet supported"));
  }

  auto object_name = ObjectTypeName(stmt.renameType);

  if (r.is(ERROR_SERVER_OBJECT_TYPE_MISMATCH)) {
    auto actual_type = r.errorMessage();
    auto actual_name = absl::AsciiStrToLower(actual_type);
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
      ERR_MSG("\"", name, "\" is not ",
              basics::string_utils::GetArticle(object_name), " ", object_name),
      ERR_HINT("Use ALTER ", absl::AsciiStrToUpper(actual_type),
               " ... RENAME to rename ",
               basics::string_utils::GetArticle(actual_name), " ",
               actual_name, "."));
  }

  if (r.is(ERROR_SERVER_DATA_SOURCE_NOT_FOUND) ||
      r.is(ERROR_SERVER_ILLEGAL_NAME)) {
    if (!stmt.missing_ok) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                      ERR_MSG(object_name, " \"", name, "\" does not exist"));
    }
    conn_ctx.AddNotice(
      SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                     ERR_MSG(object_name, " \"", name,
                             "\" does not exist, skipping")));
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
