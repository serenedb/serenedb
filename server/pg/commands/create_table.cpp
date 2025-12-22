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

#include <unordered_set>
#include <yaclib/async/make.hpp>

#include "app/app_server.h"
#include "basics/errors.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/sharding_strategy.h"
#include "catalog/table_options.h"
#include "pg/commands.h"
#include "pg/connection_context.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_analyzer_velox.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
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

// TODO: use ErrorPosition in ThrowSqlError
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
  auto database = catalog.GetSnapshot()->GetDatabase(db);
  SDB_ENSURE(database, ERROR_SERVER_DATABASE_NOT_FOUND);

  catalog::CreateTableRequest request;
  request.name = table;
  request.columns.reserve(list_length(stmt.tableElts));

  auto append_column = [&](catalog::Column column, int location) {
    if (absl::c_any_of(request.columns,
                       [&](const catalog::Column& existing_column) {
                         return existing_column.name == column.name;
                       })) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DUPLICATE_COLUMN), CURSOR_POS(location),
        ERR_MSG("column \"", column.name, "\" specified more than once"));
    }
    request.columns.push_back(std::move(column));
  };
  auto append_pk = [&](const catalog::Column::Id column_id, int location) {
    if (absl::c_linear_search(request.pkColumns, column_id)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_COLUMN), CURSOR_POS(location),
                      ERR_MSG("column \"", request.columns[column_id].name,
                              "\" appears twice in primary key constraint"));
    }
    SDB_ASSERT(column_id < request.columns.size());
    if (request.columns[column_id].generated_type ==
        catalog::Column::GeneratedType::kVirtual) {
      // pg 18 doesn't support either
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED), CURSOR_POS(location),
        ERR_MSG("primary keys on virtual generated columns are not supported"));
    }
    request.pkColumns.push_back(column_id);
  };

  auto error_constraint_not_supported = [&](const Constraint& constraint) {
    auto constraint_name = absl::NullSafeStringView(constraint.conname);
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      CURSOR_POS(ExprLocation(&constraint)),
      ERR_MSG("constraint is not supported yet: ", constraint_name));
  };

  catalog::Column::Id next_column_id = 0;
  VisitNodes(stmt.tableElts, [&](const Node& node) {
    if (IsA(&node, ColumnDef)) {
      const auto& col_def = *castNode(ColumnDef, &node);
      append_column(catalog::Column{.id = next_column_id++,
                                    .type = pg::NameToType(*col_def.typeName),
                                    .name = col_def.colname},
                    ExprLocation(&col_def));
      auto& col = request.columns.back();
      VisitNodes(col_def.constraints, [&](const Constraint& constraint) {
        switch (constraint.contype) {
          case CONSTR_DEFAULT: {
            switch (col.generated_type) {
              using enum catalog::Column::GeneratedType;
              case kVirtual:
              case kStored:
                THROW_SQL_ERROR(
                  ERR_CODE(ERRCODE_INVALID_COLUMN_DEFINITION),
                  CURSOR_POS(ExprLocation(&constraint)),
                  ERR_MSG("both default and generation expression specified "
                          "for column \"",
                          col.name, "\" of table \"", table, "\""));
              case kNone:
                if (col.expr) {
                  THROW_SQL_ERROR(
                    ERR_CODE(ERRCODE_INVALID_COLUMN_DEFINITION),
                    CURSOR_POS(ExprLocation(&constraint)),
                    ERR_MSG("multiple default values specified for column \"",
                            col.name, "\" of table \"", table, "\""));
                }
            }

            auto default_value = std::make_shared<ColumnExpr>();
            auto r = default_value->Init(db, constraint.raw_expr);
            SDB_ENSURE(r.ok(), r.errorNumber(), std::move(r).errorMessage());
            col.expr = std::move(default_value);
          } break;
          case CONSTR_PRIMARY:  // create table (field integer primary key)
            append_pk(col.id, ExprLocation(&constraint));
            break;
          case CONSTR_GENERATED: {
            switch (col.generated_type) {
              using enum catalog::Column::GeneratedType;
              case kVirtual:
              case kStored:
                THROW_SQL_ERROR(
                  ERR_CODE(ERRCODE_INVALID_COLUMN_DEFINITION),
                  CURSOR_POS(ExprLocation(&constraint)),
                  ERR_MSG("multiple generation clauses specified for column \"",
                          col.name, "\" of table \"", table, "\""));
              case kNone:
                if (col.expr) {
                  THROW_SQL_ERROR(
                    ERR_CODE(ERRCODE_INVALID_COLUMN_DEFINITION),
                    CURSOR_POS(ExprLocation(&constraint)),
                    ERR_MSG("both default and generation expression specified "
                            "for column \"",
                            col.name, "\" of table \"", table, "\""));
                }
            }

            // guaranteed by parser
            SDB_ASSERT(constraint.generated_when == ATTRIBUTE_IDENTITY_ALWAYS);

            auto generated_value = std::make_shared<ColumnExpr>();
            auto r = generated_value->Init(db, constraint.raw_expr);
            SDB_ENSURE(r.ok(), r.errorNumber(), std::move(r).errorMessage());
            col.expr = std::move(generated_value);
            col.generated_type = catalog::Column::GeneratedType::kStored;
          } break;
          default:
            error_constraint_not_supported(constraint);
        }
      });

      return;
    }

    SDB_ASSERT(IsA(&node, Constraint));
    const auto& constraint = *castNode(Constraint, &node);
    switch (constraint.contype) {
      case CONSTR_PRIMARY: {
        // create table (field integer, primary key(field))
        VisitNodes(constraint.keys, [&](const String& key) {
          std::string_view name = key.sval;

          auto it = std::ranges::find_if(
            request.columns,
            [name](const catalog::Column& col) { return col.name == name; });
          if (absl::c_none_of(request.columns, [&](const catalog::Column& col) {
                return col.name == name;
              })) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
              CURSOR_POS(ExprLocation(&key)),
              ERR_MSG("column \"", name, "\" named in key does not exist"));
          }
          append_pk(it->id, ExprLocation(&key));
        });
      } break;
      case CONSTR_DEFAULT:
      case CONSTR_GENERATED:
        SDB_UNREACHABLE();
      default:
        error_constraint_not_supported(constraint);
    }
  });
  SDB_ASSERT(!stmt.constraints);

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
