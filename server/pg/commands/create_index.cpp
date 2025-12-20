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

#include <memory>
#include <string_view>
#include <yaclib/async/make.hpp>

#include "app/app_server.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/object.h"
#include "catalog/secondary_index.h"
#include "catalog/table.h"
#include "magic_enum/magic_enum.hpp"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_utils.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "parser/parse_node.h"
#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {
namespace {

std::optional<catalog::IndexType> GetIndexType(char* method) {
  return method ? magic_enum::enum_cast<catalog::IndexType>(method)
                : catalog::IndexType::Secondary;
}

ResultOr<std::shared_ptr<catalog::Index>> MakeSecondaryIndex(
  catalog::IndexBaseOptions options, const catalog::SchemaObject& relation,
  PgListWrapper<IndexElem> index_columns) {
  if (relation.GetType() != catalog::ObjectType::Table) {
    return std::unexpected<Result>{
      std::in_place, ERROR_NOT_IMPLEMENTED,
      "Indexes over views are not implemented yet."};
  }
  auto& table = basics::downCast<catalog::Table>(relation);
  auto& columns = table.Columns();

  auto find_column = [&](std::string_view name) {
    auto it = absl::c_find_if(
      columns, [&](const catalog::Column& c) { return c.name == name; });
    return it != columns.end() ? &*it : nullptr;
  };

  catalog::SecondaryIndexOptions impl_options;
  impl_options.columns.reserve(index_columns.size());

  for (auto* index_elem : index_columns) {
    auto* column = find_column(index_elem->name);
    if (!column) {
      return std::unexpected<Result>{
        std::in_place, ERROR_SERVER_DATA_SOURCE_NOT_FOUND, "column \"",
        index_elem->name, "\" does not exist"};
    }
    impl_options.columns.push_back(column->id);
  }

  return std::make_shared<catalog::SecondaryIndex>(
    catalog::IndexOptions<catalog::SecondaryIndexOptions>{
      .base = std::move(options),
      .impl = std::move(impl_options),
    },
    relation.GetDatabaseId());
}

ResultOr<std::shared_ptr<catalog::Index>> MakeIndex(
  catalog::IndexBaseOptions options, const catalog::SchemaObject& relation,
  PgListWrapper<IndexElem> index_columns) {
  switch (options.type) {
    case catalog::IndexType::Secondary:
      return MakeSecondaryIndex(std::move(options), relation, index_columns);
    case catalog::IndexType::Inverted:
      return std::unexpected<Result>{std::in_place, ERROR_NOT_IMPLEMENTED};
  }
}

}  // namespace

template<typename T>
inline int ExprLocation(const T* node) noexcept {
  return exprLocation(reinterpret_cast<const Node*>(node));
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

  catalog::IndexBaseOptions options;
  options.name = stmt.idxname;
  options.type = *index_type;

  auto r = catalog.CreateIndex(
    db, schema, relation_name, [&](const catalog::SchemaObject* relation) {
      SDB_ASSERT(relation);
      options.relation_id = relation->GetId();

      return MakeIndex(std::move(options), *relation,
                       PgListWrapper<IndexElem>{stmt.indexParams});
    });

  if (r.is(ERROR_SERVER_DUPLICATE_NAME) && stmt.if_not_exists) {
    r = {};
  }
  return yaclib::MakeFuture(std::move(r));
}

}  // namespace sdb::pg
