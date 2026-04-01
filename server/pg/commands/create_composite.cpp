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
#include "catalog/catalog.h"
#include "catalog/composite_type.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_analyzer_velox.h"
#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

yaclib::Future<> CreateComposite(ExecContext& ctx,
                                 const CompositeTypeStmt& stmt) {
  const auto& conn_ctx = basics::downCast<const ConnectionContext>(ctx);
  const auto db = ctx.GetDatabaseId();
  auto current_schema = conn_ctx.GetCurrentSchema();

  std::string_view type_schema = stmt.typevar->schemaname
                                   ? std::string_view{stmt.typevar->schemaname}
                                   : std::string_view{current_schema};
  std::string_view type_name = stmt.typevar->relname;

  std::vector<std::string> field_names;
  std::vector<velox::TypePtr> field_types;
  VisitNodes(stmt.coldeflist, [&](const ColumnDef& col) {
    for (const auto& existing : field_names) {
      if (existing == col.colname) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DUPLICATE_COLUMN),
          ERR_MSG("column \"", col.colname, "\" specified more than once"));
      }
    }
    field_names.emplace_back(col.colname);
    field_types.emplace_back(NameToType(*col.typeName, &ctx));
  });

  auto row_type = velox::ROW(std::move(field_names), std::move(field_types));
  auto composite = std::make_shared<catalog::CompositeType>(
    ObjectId{0}, type_name, std::move(row_type));

  auto& catalogs =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog = catalogs.Global();
  auto r = catalog.CreateCompositeType(db, type_schema, std::move(composite));

  if (!r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("type \"", type_name, "\" already exists"));
  }
  return {};
}

}  // namespace sdb::pg
