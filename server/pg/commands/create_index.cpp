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
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "rest_server/serened_single.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "parser/parse_node.h"
#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {
namespace {

IndexType GetIndexType(char* method) {
  SDB_ASSERT(method);
  return magic_enum::enum_cast<IndexType>(method, magic_enum::case_insensitive)
    .value_or(IndexType::Unknown);
}

Result ParseIndexOptions(const IndexStmt& index,
                         std::vector<std::string>& column_names,
                         catalog::IndexBaseOptions& options) {
  if (!index.accessMethod) {
    return Result{ERROR_BAD_PARAMETER, "access method is not provided"};
  }
  auto index_type = GetIndexType(index.accessMethod);
  if (index_type == IndexType::Unknown) {
    return Result{ERROR_BAD_PARAMETER, "access method \"", index.accessMethod,
                  "\" does not exist"};
  }

  pg::PgListWrapper<IndexElem> index_columns{index.indexParams};
  options.column_ids.reserve(index_columns.size());

  for (auto* index_elem : index_columns) {
    column_names.push_back(index_elem->name);
  }

  options.name = index.idxname;
  options.type = index_type;
  return {};
}

vpack::Slice ParseIndexArgs(pg::PgListWrapper<DefElem> args) {
  vpack::Builder builder;
  builder.openObject();
  for (DefElem* elem : args) {
    char* val = castNode(BitString, elem->arg)->bsval;
    builder.add(elem->defname, val);
  }
  builder.close();
  return builder.slice();
}

}  // namespace

// TODO: use ErrorPosition in ThrowSqlError
yaclib::Future<Result> CreateIndex(ExecContext& context,
                                   const IndexStmt& stmt) {
  const auto db = context.GetDatabaseId();
  const auto& conn_ctx = basics::downCast<const ConnectionContext>(context);

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

  if (stmt.concurrent) {
    return yaclib::MakeFuture(Result{ERROR_NOT_IMPLEMENTED});
  }
  std::vector<std::string> column_names;
  catalog::IndexBaseOptions options;

  if (auto r = ParseIndexOptions(stmt, column_names, options); !r.ok()) {
    return yaclib::MakeFuture(std::move(r));
  }

  pg::PgListWrapper<DefElem> pg_args{stmt.options};
  auto args = ParseIndexArgs(pg_args);

  Result r =
    catalog.CreateIndex(db, schema, relation_name, std::move(column_names),
                        std::move(options), std::move(args));

  if (r.is(ERROR_SERVER_DUPLICATE_NAME) && stmt.if_not_exists) {
    r = {};
  } else if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("relation \"", stmt.idxname, "\" already exists"));
  }
  return yaclib::MakeFuture(std::move(r));
}

}  // namespace sdb::pg
