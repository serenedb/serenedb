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

std::shared_ptr<ColumnExpr> MakeColumnExpr(ObjectId database_id, Node* expr) {
  auto column_expr = std::make_shared<ColumnExpr>();
  auto r = column_expr->Init(database_id, expr);
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
  return column_expr;
}

std::shared_ptr<ColumnExpr> MakeColumnExpr(ObjectId database_id,
                                           std::string deparsed) {
  auto column_expr = std::make_shared<ColumnExpr>();
  auto r = column_expr->Init(database_id, std::move(deparsed));
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
  return column_expr;
}

enum class NullInfo : uint8_t { NotStated = 0, Null = 1, NotNull = 2 };

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
    request.columns.emplace_back(std::move(column));
  };

  // tries to behave like ChooseConstraintName pg source code function
  auto choose_constraint_name = [&](std::string_view table,
                                    std::string_view column,
                                    std::string_view label) -> std::string {
    std::string base_name;
    if (column.empty()) {
      base_name = absl::StrCat(table, "_", label);
    } else {
      base_name = absl::StrCat(table, "_", column, "_", label);
    }

    auto name_exists = [&](std::string_view candidate) {
      return absl::c_any_of(
        request.checkConstraints,
        [&](const catalog::CheckConstraint& c) { return c.name == candidate; });
    };

    if (!name_exists(base_name)) {
      return base_name;
    }

    for (size_t counter = 1;; ++counter) {
      std::string candidate = absl::StrCat(base_name, counter);
      if (!name_exists(candidate)) {
        return candidate;
      }
    }
  };

  auto append_check_constraint = [&](const Constraint& constraint,
                                     std::string_view column_name = {}) {
    SDB_ASSERT(constraint.contype == CONSTR_CHECK);
    std::string name;
    if (constraint.conname) {
      name = constraint.conname;
    } else {
      name = choose_constraint_name(table, column_name, "check");
    }
    request.checkConstraints.emplace_back(catalog::CheckConstraint{
      .name = std::move(name),
      .expr = MakeColumnExpr(db, constraint.raw_expr),
    });
  };

  auto append_not_null_constraint = [&](std::string_view column_name) {
    request.checkConstraints.emplace_back(catalog::CheckConstraint{
      .name = choose_constraint_name(table, column_name, "not_null"),
      .expr = MakeColumnExpr(db, absl::StrCat(column_name, " IS NOT NULL")),
    });
  };

  auto append_pk = [&](const catalog::Column::Id column_id, int location) {
    if (absl::c_linear_search(request.pkColumns, column_id)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_COLUMN), CURSOR_POS(location),
                      ERR_MSG("column \"", request.columns[column_id].name,
                              "\" appears twice in primary key constraint"));
    }
    SDB_ASSERT(column_id < request.columns.size());
    const auto& column = request.columns[column_id];
    if (column.generated_type == catalog::Column::GeneratedType::kVirtual) {
      // pg 18 doesn't support either
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED), CURSOR_POS(location),
        ERR_MSG("primary keys on virtual generated columns are not supported"));
    }

    append_not_null_constraint(column.name);

    request.pkColumns.emplace_back(column_id);
  };

  auto error_constraint_not_supported = [&](const Constraint& constraint) {
    auto constraint_name = absl::NullSafeStringView(constraint.conname);
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      CURSOR_POS(ExprLocation(&constraint)),
      ERR_MSG("constraint is not supported yet: ", constraint_name));
  };

  auto error_no_inherit_not_supported = [&](const Constraint& constraint) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    CURSOR_POS(ExprLocation(&constraint)),
                    ERR_MSG("NO INHERIT is not supported yet for constraints"));
  };

  auto error_null_conflict = [&](const Constraint& constraint,
                                 std::string_view col_name) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR), CURSOR_POS(ExprLocation(&constraint)),
      ERR_MSG("conflicting NULL/NOT NULL declarations for column \"", col_name,
              "\" of table \"", table, "\""));
  };

  catalog::Column::Id next_column_id = 0;
  VisitNodes(stmt.tableElts, [&](const Node& node) {
    if (IsA(&node, TableLikeClause)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        CURSOR_POS(ExprLocation(&node)),
        ERR_MSG("CREATE TABLE ... (LIKE ...) is not supported yet"));
    }

    if (IsA(&node, ColumnDef)) {
      const auto& col_def = *castNode(ColumnDef, &node);
      append_column(catalog::Column{.id = next_column_id++,
                                    .type = pg::NameToType(*col_def.typeName),
                                    .name = col_def.colname},
                    ExprLocation(&col_def));
      auto& col = request.columns.back();
      NullInfo null_info = NullInfo::NotStated;
      VisitNodes(col_def.constraints, [&](const Constraint& constraint) {
        if (constraint.is_no_inherit) {
          error_no_inherit_not_supported(constraint);
        }

        switch (constraint.contype) {
          case CONSTR_NULL:
            if (null_info == NullInfo::NotNull) {
              error_null_conflict(constraint, col.name);
            }
            null_info = NullInfo::Null;
            break;
          case CONSTR_NOTNULL:
            if (null_info == NullInfo::Null) {
              error_null_conflict(constraint, col.name);
            }
            if (null_info == NullInfo::NotNull) {
              break;
            }
            append_not_null_constraint(col.name);
            null_info = NullInfo::NotNull;
            break;
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

            col.expr = MakeColumnExpr(db, constraint.raw_expr);
          } break;
          case CONSTR_PRIMARY:  // create table (field integer primary key)
            append_pk(col.id, ExprLocation(&constraint));
            null_info = NullInfo::NotNull;
            break;
          case CONSTR_CHECK:
            append_check_constraint(constraint, col.name);
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

            col.expr = MakeColumnExpr(db, constraint.raw_expr);
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
    if (constraint.is_no_inherit) {
      error_no_inherit_not_supported(constraint);
    }
    switch (constraint.contype) {
      case CONSTR_PRIMARY: {
        // create table (field integer, primary key(field))

        // fun fact: it's supposed to be checked wheter NULL constraint were
        // set for the column, but postgres doesn't do it and neither do we
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
      case CONSTR_CHECK:
        append_check_constraint(constraint);
        break;
      case CONSTR_NULL:
      case CONSTR_NOTNULL:
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
