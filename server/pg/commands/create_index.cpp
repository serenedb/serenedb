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
#include "basics/errors.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "magic_enum/magic_enum.hpp"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/sql_utils.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "parser/parse_node.h"
#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

template<typename T>
inline int ExprLocation(const T* node) noexcept {
  return exprLocation(reinterpret_cast<const Node*>(node));
}

std::optional<catalog::IndexType> GetIndexType(char* method) {
  return method ? magic_enum::enum_cast<catalog::IndexType>(method)
                : catalog::IndexType::Secondary;
}

// TODO: use ErrorPosition in ThrowSqlError
yaclib::Future<Result> CreateIndex(ExecContext& context,
                                   const IndexStmt& stmt) {
  const auto db = context.GetDatabaseId();
  const auto& conn_ctx = basics::downCast<const ConnectionContext>(context);

  const auto index_type = GetIndexType(stmt.accessMethod);
  if (!index_type) {
    return yaclib::MakeFuture<Result>(ERROR_BAD_PARAMETER, "access method \"",
                                      stmt.accessMethod, "\" does not exist");
  }

  const std::string_view relation_name = stmt.relation->relname;
  const std::string current_schema = conn_ctx.GetCurrentSchema();
  const std::string_view schema =
    stmt.relation->schemaname ? std::string_view{stmt.relation->schemaname}
                              : current_schema;
  if (schema.empty()) {
    return yaclib::MakeFuture<Result>(
      ERROR_BAD_PARAMETER, "no schema has been selected to create in");
  }

  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  auto snapshot = catalog.GetSnapshot();

  auto relation = snapshot->GetRelation(db, schema, relation_name);

  if (!relation) {
    return yaclib::MakeFuture<Result>(ERROR_BAD_PARAMETER, "Relation \'",
                                      relation_name, "\' does not exist");
  }

  catalog::IndexOptions options;
  options.name = stmt.idxname;
  options.relation_id = relation->GetId();
  options.type = *index_type;

  auto r = catalog.CreateIndex(db, schema, std::move(options));
  if (r.is(ERROR_SERVER_DUPLICATE_NAME) && stmt.if_not_exists) {
    r = {};
  }
  return yaclib::MakeFuture(std::move(r));
}

}  // namespace sdb::pg
