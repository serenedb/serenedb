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
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "pg/commands.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_collector.h"

namespace sdb::pg {

yaclib::Future<Result> DropObject(ExecContext& context, const DropStmt& stmt) {
  auto& catalogs =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog = catalogs.Global();
  Result r;
  if (stmt.behavior == DROP_RESTRICT) {
    return yaclib::MakeFuture<Result>(ERROR_NOT_IMPLEMENTED,
                                      "DROP ... RESTRICT is not implemented");
  }

  List* names = list_nth_node(List, stmt.objects, 0);

  auto [schema, name] = VisitName(
    names,
    absl::Overload{
      [](auto name) { return pg::Objects::ObjectName{{}, name}; },
      [](std::string_view schema, std::string_view name) {
        return pg::Objects::ObjectName{schema, name};
      },
      [&](std::string_view db, std::string_view schema, std::string_view name) {
        if (context.GetDatabase() != db) {
          SDB_THROW(ERROR_BAD_PARAMETER,
                    "Cross database queries are not allowed: ", db,
                    " accessed instead of ", context.GetDatabase());
        }
        return pg::Objects::ObjectName{schema, name};
      },
      [&](...) -> pg::Objects::ObjectName {
        SDB_THROW(ERROR_NOT_IMPLEMENTED,
                  "unsupported function call with too many dotted names");
      }});

  if (stmt.removeType != OBJECT_SCHEMA && schema.empty()) {
    // TODO: fix schema resolution
    schema = StaticStrings::kPublic;
  }

  const bool cascade = stmt.behavior == DROP_CASCADE;
  const auto db = context.GetDatabaseId();
  switch (stmt.removeType) {
    case OBJECT_TABLE:
      r = catalog.DropTable(db, schema, name, nullptr);
      break;
    case OBJECT_VIEW: {
      r = catalog.DropView(db, schema, name);
    } break;
    case OBJECT_FUNCTION: {
      r = catalog.DropFunction(db, schema, name);
    } break;
    case OBJECT_SCHEMA: {
      // TODO: ensure that schema is empty
      r = catalog.DropSchema(db, name, cascade, nullptr);
    } break;
    default:
      r = {ERROR_NOT_IMPLEMENTED,
           "DROP for this object type is not implemented: ",
           magic_enum::enum_name(stmt.removeType)};
  }
  if (r.is(ERROR_SERVER_DATA_SOURCE_NOT_FOUND) && stmt.missing_ok) {
    r = {};
  }
  return yaclib::MakeFuture(std::move(r));
}

}  // namespace sdb::pg
