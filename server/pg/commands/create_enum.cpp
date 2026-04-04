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
#include "catalog/enum_type.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_collector.h"
#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

yaclib::Future<> CreateEnum(ExecContext& ctx, const CreateEnumStmt& stmt) {
  const auto& conn_ctx = basics::downCast<const ConnectionContext>(ctx);
  const auto db = ctx.GetDatabaseId();
  auto current_schema = conn_ctx.GetCurrentSchema();
  const auto type_name =
    ParseObjectName(stmt.typeName, ctx.GetDatabase(), current_schema);

  std::vector<catalog::EnumLabel> entries;
  uint64_t sortorder = 1;
  VisitNodes(stmt.vals, [&](const String& val) {
    std::string_view label = strVal(&val);
    for (const auto& existing : entries) {
      if (existing.label == label) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_OBJECT_DEFINITION),
                        ERR_MSG("invalid enum label \"", label, "\""),
                        ERR_DETAIL("Labels must be unique for the enum type."));
      }
    }
    entries.push_back(catalog::EnumLabel{
      .sortorder = sortorder,
      .label = std::string{label},
    });
    sortorder += 1;
  });

  auto enum_type = std::make_shared<catalog::EnumType>(
    ObjectId{0}, type_name.relation, std::move(entries));

  auto& catalogs =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog = catalogs.Global();
  auto r = catalog.CreateEnumType(db, type_name.schema, std::move(enum_type));

  if (!r.ok()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
      ERR_MSG("type \"", type_name.relation, "\" already exists"));
  }
  return {};
}

}  // namespace sdb::pg
