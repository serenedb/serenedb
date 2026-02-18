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
#include "basics/errors.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_collector.h"

namespace sdb::pg {

yaclib::Future<Result> DropObject(ExecContext& context, const DropStmt& stmt) {
  auto& catalogs =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog = catalogs.Global();
  auto* names = stmt.removeType == OBJECT_SCHEMA
                  ? stmt.objects
                  : list_nth_node(List, stmt.objects, 0);
  auto current_schema =
    basics::downCast<const ConnectionContext>(context).GetCurrentSchema();
  auto [schema, name] =
    ParseObjectName(names, context.GetDatabase(), current_schema);

  Result r;
  const auto db = context.GetDatabaseId();
  switch (stmt.removeType) {
    case OBJECT_TABLE:
      r = catalog.DropTable(db, schema, name, nullptr);
      break;
    case OBJECT_INDEX:
      r = catalog.DropIndex(db, schema, name, nullptr);
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
        r = {ERROR_BAD_PARAMETER, "cannot drop schema ", name,
             " because it is required by the database system"};
      } else {
        const bool cascade = stmt.behavior == DROP_CASCADE;
        r = catalog.DropSchema(db, name, cascade, nullptr);
      }
    } break;
    default:
      r = {ERROR_NOT_IMPLEMENTED,
           "DROP for this object type is not implemented: ",
           magic_enum::enum_name(stmt.removeType)};
  }
  if (r.is(ERROR_SERVER_ILLEGAL_NAME) && !stmt.missing_ok) {
    std::string_view object_type;
    switch (stmt.removeType) {
      case OBJECT_TABLE:
        object_type = "table";
        break;
      case OBJECT_INDEX:
        object_type = "index";
        break;
      case OBJECT_VIEW:
        object_type = "view";
        break;
      case OBJECT_FUNCTION:
        object_type = "function";
        break;
      case OBJECT_SCHEMA:
        object_type = "schema";
        break;
      default:
        object_type = "object";
        break;
    }
    r = {ERROR_SERVER_ILLEGAL_NAME, object_type, " \"", name,
         "\" does not exist"};
  } else if (r.is(ERROR_SERVER_ILLEGAL_NAME)) {
    r = {};
  }
  return yaclib::MakeFuture(std::move(r));
}

}  // namespace sdb::pg
