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

#include <absl/functional/overload.h>

#include <yaclib/async/make.hpp>

#include "app/app_server.h"
#include "basics/debugging.h"
#include "basics/errors.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
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

yaclib::Future<> DropObject(ExecContext& context, const DropStmt& stmt) {
  using CatalogObjectType = catalog::ObjectType;
  auto& catalogs =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog = catalogs.Global();
  auto* names = [&]() -> List* {
    if (stmt.removeType == OBJECT_SCHEMA) {
      return stmt.objects;
    } else if (stmt.removeType == OBJECT_FUNCTION) {
      auto* object_with_args =
        castNode(ObjectWithArgs, list_nth(stmt.objects, 0));
      return object_with_args->objname;
    } else {
      return list_nth_node(List, stmt.objects, 0);
    }
  }();
  auto current_schema =
    basics::downCast<const ConnectionContext>(context).GetCurrentSchema();
  auto [schema, name] =
    ParseObjectName(names, context.GetDatabase(), current_schema);
  Result r;
  const auto db = context.GetDatabaseId();

  auto object_type_name = [&] -> std::string_view {
    switch (stmt.removeType) {
      case OBJECT_TABLE:
        return "table";
      case OBJECT_INDEX:
        return "index";
      case OBJECT_VIEW:
        return "view";
      case OBJECT_FUNCTION:
        return "function";
      case OBJECT_SCHEMA:
        return "schema";
      case OBJECT_TSDICTIONARY:
        return "text search dictionary";
      default:
        SDB_ASSERT(false);  // it's better to specify the name
        return "object";
    }
  }();

  auto expected_relation = [&] {
    switch (stmt.removeType) {
      using enum CatalogObjectType;
      case OBJECT_TABLE:
        return Table;
      case OBJECT_INDEX:
        return Index;
      case OBJECT_VIEW:
        return View;
      default:
        return Invalid;
    }
  }();

  // For relation types check that we delete the same type (as PG does)
  if (expected_relation != CatalogObjectType::Invalid) {
    auto snapshot = catalog.GetSnapshot();
    if (auto relation = snapshot->GetRelation(db, schema, name)) {
      auto actual_type = relation->GetType();
      if (actual_type != expected_relation) {
        auto actual_name =
          absl::AsciiStrToLower(magic_enum::enum_name(actual_type));
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
          ERR_MSG("\"", name, "\" is not ",
                  basics::string_utils::GetArticle(object_type_name), " ",
                  object_type_name),
          ERR_HINT("Use DROP ",
                   absl::AsciiStrToUpper(magic_enum::enum_name(actual_type)),
                   " to remove ", basics::string_utils::GetArticle(actual_name),
                   " ", actual_name, "."));
      }
    }
  }

  switch (stmt.removeType) {
    case OBJECT_TABLE:
      r = catalog.DropTable(db, schema, name);
      break;
    case OBJECT_INDEX:
      r = catalog.DropIndex(db, schema, name);
      break;
    case OBJECT_VIEW: {
      r = catalog.DropView(db, schema, name);
    } break;
    case OBJECT_FUNCTION: {
      r = catalog.DropFunction(db, schema, name);
    } break;
    case OBJECT_SCHEMA: {
      // TODO: ensure that schema is empty
      if (name == StaticStrings::kPgCatalogSchema ||
          name == StaticStrings::kInformationSchema) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_SCHEMA_NAME),
          ERR_MSG("cannot drop schema ", name,
                  " because it is required by the database system"));
      } else {
        const bool cascade = stmt.behavior == DROP_CASCADE;
        r = catalog.DropSchema(db, name, cascade);
      }
    } break;
    case OBJECT_TSDICTIONARY: {
      r = catalog.DropTokenizer(db, schema, name);
    } break;
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("DROP for this object type is not implemented: ",
                              magic_enum::enum_name(stmt.removeType)));
  }
  if (r.is(ERROR_SERVER_ILLEGAL_NAME)) {
    if (stmt.removeType == OBJECT_FUNCTION) {
      if (!stmt.missing_ok) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
          ERR_MSG("could not find a function named \"", name, "\""));
      }
      basics::downCast<ConnectionContext>(context).AddNotice(SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
        ERR_MSG("function ", name, "() does not exist, skipping")));
    } else {
      if (!stmt.missing_ok) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
          ERR_MSG(object_type_name, " \"", name, "\" does not exist"));
      }
      basics::downCast<ConnectionContext>(context).AddNotice(SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
        ERR_MSG(object_type_name, " \"", name, "\" does not exist, skipping")));
    }
    r = {};
  }
  SDB_IF_FAILURE("crash_on_drop") { SDB_IMMEDIATE_ABORT(); }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
  return {};
}

}  // namespace sdb::pg
