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
#include "catalog/database.h"
#include "catalog/sharding_strategy.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_analyzer_velox.h"

namespace sdb::pg {

yaclib::Future<Result> CreateTable(ExecContext& context,
                                   const CreateStmt& stmt) {
  const auto db = context.GetDatabaseId();
  const auto& conn_ctx = basics::downCast<const ConnectionContext>(context);
  std::string current_schema = conn_ctx.GetCurrentSchema();
  const std::string_view schema =
    stmt.relation->schemaname ? std::string_view{stmt.relation->schemaname}
                              : current_schema;
  if (schema.empty()) {
    return yaclib::MakeFuture<Result>(
      ERROR_BAD_PARAMETER, "no schema has been selected to create in");
  }
  const std::string_view table = stmt.relation->relname;

  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  auto database = catalog.GetObject<catalog::Database>(db);
  SDB_ENSURE(database, ERROR_SERVER_DATABASE_NOT_FOUND);

  catalog::CreateTableRequest request;
  request.name = table;

  std::vector<std::string> pk_column_names;
  std::vector<velox::TypePtr> pk_column_types;
  std::vector<std::string> column_names;
  std::vector<velox::TypePtr> column_types;
  VisitNodes(stmt.tableElts, [&](const Node& node) {
    if (IsA(&node, ColumnDef)) {
      const auto& col_def = *castNode(ColumnDef, &node);
      column_names.push_back(col_def.colname);
      column_types.push_back(pg::NameToType(*col_def.typeName));
      VisitNodes(col_def.constraints, [&](const Constraint& constraint) {
        if (constraint.contype == CONSTR_PRIMARY) {
          pk_column_names.push_back(column_names.back());
          pk_column_types.push_back(column_types.back());
        }
      });
    } else if (IsA(&node, Constraint)) {
      const auto& constraint = *castNode(Constraint, &node);
      if (constraint.contype == CONSTR_PRIMARY) {
        VisitNodes(constraint.keys, [&](const String& key) {
          std::string_view name = pk_column_names.emplace_back(key.sval);
          auto it = std::find(column_names.begin(), column_names.end(), name);
          if (it == column_names.end()) {
            VELOX_USER_FAIL("Primary key column '{}' not found", name);
          }
          const auto index = std::distance(column_names.begin(), it);
          pk_column_types.push_back(column_types[index]);
        });
      }
    } else {
      SDB_ENSURE(false, ERROR_NOT_IMPLEMENTED,
                 "Unsupported table element type: ", node.type);
    }
  });
  SDB_ASSERT(!stmt.constraints);

  request.pkType =
    velox::ROW(std::move(pk_column_names), std::move(pk_column_types));
  if (request.pkType->size() != request.pkType->nameToIndex().size()) {
    return yaclib::MakeFuture<Result>(ERROR_BAD_PARAMETER,
                                      "Duplicate column names in primary key");
  }
  request.rowType =
    velox::ROW(std::move(column_names), std::move(column_types));
  if (request.rowType->size() != request.rowType->nameToIndex().size()) {
    return yaclib::MakeFuture<Result>(ERROR_BAD_PARAMETER,
                                      "Duplicate column names in row type");
  }
  catalog::CreateTableOptions options;
  auto r = MakeTableOptions(std::move(request), database->GetId(), options,
                            database->GetReplicationFactor(),
                            database->GetWriteConcern(), {});
  if (!r.ok()) {
    return yaclib::MakeFuture(std::move(r));
  }
  r = catalog.CreateTable(db, schema, std::move(options), {});
  if (r.is(ERROR_SERVER_DUPLICATE_NAME) && stmt.if_not_exists) {
    r = {};
  }
  return yaclib::MakeFuture(std::move(r));
}

}  // namespace sdb::pg
