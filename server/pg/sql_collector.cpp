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

#include "sql_collector.h"

#include <absl/functional/overload.h>

#include <string_view>

#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "basics/exceptions.h"
#include "connector/serenedb_connector.hpp"
#include "pg/pg_list_utils.h"
#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {
namespace {

using pg::ParamIndex;

struct State {
  const State* parent = nullptr;
  containers::FlatHashSet<std::string_view> ctes;
};

// TODO(mbkkt) handle access to data in expressions
class ObjectCollector {
 public:
  explicit ObjectCollector(std::string_view database, Objects& objects,
                           ParamIndex& max_bind_param_idx) noexcept
    : _database{database},
      _objects{objects},
      _max_bind_param_idx{max_bind_param_idx} {}

  void CollectStmt(const State* parent, const Node* node);

  void CollectExprNode(const State& state, const Node* expr);

 private:
  void CollectFromClause(const State& state, const List* from_clause);
  void CollectWithClause(State& state, const WithClause* with_clause);
  void CollectFromNode(const State& state, const Node* node);

  void CollectInsertStmt(State& state, const InsertStmt& stmt);
  void CollectDeleteStmt(State& state, const DeleteStmt& stmt);
  void CollectUpdateStmt(State& state, const UpdateStmt& stmt);
  void CollectMergeStmt(State& state, const MergeStmt& stmt);
  void CollectSelectStmt(State& state, const SelectStmt* stmt);
  void CollectExplainStmt(const State* parent, const ExplainStmt& stmt);
  void CollectCallStmt(State& state, const CallStmt& stmt);
  void CollectViewStmt(State& state, const ViewStmt& stmt);
  void CollectCreateFunctionStmt(State& state, const CreateFunctionStmt& stmt);
  void CollectCreateStmt(State& state, const CreateStmt& stmt);

  void CollectRangeVar(const State& state, const RangeVar* var,
                       Objects::AccessType type);
  void CollectRangeSubSelect(const State& state,
                             const RangeSubselect& subselect);
  void CollectJoinExpr(const State& state, const JoinExpr& expr);
  void CollectRangeFunction(const State& state, const RangeFunction& function);

  void CollectSortClause(const State& state, const List* sort_clause);
  void CollectExprList(const State& state, const List* expr_list);
  void CollectValuesLists(const State& state, const List* values_lists);

  void CollectSubLink(const State& state, const SubLink& sublink);
  void CollectCaseExpr(const State& state, const CaseExpr& expr);
  void CollectAExpr(const State& state, const A_Expr& expr);
  void CollectFuncCall(const State& state, const FuncCall& expr);
  void CollectJsonObjectConstructor(const State& state,
                                    const JsonObjectConstructor& expr);
  void CollectJsonArrayConstructor(const State& state,
                                   const JsonArrayConstructor& expr);
  void CollectAIndirection(const State& state, const A_Indirection& expr);

  // database null when parsing from definitions
  std::string_view _database;
  Objects& _objects;
  ParamIndex& _max_bind_param_idx;
};

void ObjectCollector::CollectSubLink(const State& state,
                                     const SubLink& sublink) {
  SDB_ASSERT(sublink.subselect);
  CollectStmt(&state, sublink.subselect);
  CollectExprNode(state, sublink.testexpr);
}

void ObjectCollector::CollectCaseExpr(const State& state,
                                      const CaseExpr& expr) {
  CollectExprNode(state, castNode(Node, expr.arg));
  VisitNodes(expr.args, [&](const CaseWhen& n) {
    CollectExprNode(state, castNode(Node, n.expr));
    CollectExprNode(state, castNode(Node, n.result));
  });
  CollectExprNode(state, castNode(Node, expr.defresult));
}

void ObjectCollector::CollectAExpr(const State& state, const A_Expr& expr) {
  switch (expr.kind) {
    case AEXPR_OP:
    case AEXPR_OP_ANY:
    case AEXPR_OP_ALL:
    case AEXPR_DISTINCT:
    case AEXPR_NOT_DISTINCT:
    case AEXPR_NULLIF:
    case AEXPR_LIKE:
    case AEXPR_ILIKE:
    case AEXPR_SIMILAR: {
      CollectExprNode(state, expr.lexpr);
      CollectExprNode(state, expr.rexpr);
    } break;
    case AEXPR_IN: {
      CollectExprNode(state, expr.lexpr);
      // TODO(gnusi): handle case when lexpr and elements in rexpr are RowExpr
      CollectExprList(state, castNode(List, expr.rexpr));
    } break;
    case AEXPR_BETWEEN:
    case AEXPR_NOT_BETWEEN:
    case AEXPR_BETWEEN_SYM:
    case AEXPR_NOT_BETWEEN_SYM: {
      CollectExprNode(state, expr.lexpr);
      const auto* right = castNode(List, expr.rexpr);
      SDB_ASSERT(list_length(right) == 2);
      CollectExprNode(state, linitial_node(Node, right));
      CollectExprNode(state, lsecond_node(Node, right));
    } break;
  }
}

void ObjectCollector::CollectFuncCall(const State& state,
                                      const FuncCall& expr) {
  auto name = ParseObjectName(expr.funcname, _database);
  if (expr.agg_within_group || expr.func_variadic) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "unsupported function call with aggregate options");
  }
  CollectExprList(state, expr.args);
  CollectExprNode(state, expr.agg_filter);
  CollectSortClause(state, expr.agg_order);

  if (const auto* over = expr.over) {
    CollectExprList(state, over->partitionClause);
    CollectExprNode(state, over->endOffset);
    CollectExprNode(state, over->startOffset);
    CollectSortClause(state, over->orderClause);
  }
  _objects.ensureData(name.schema, name.relation);
}

void ObjectCollector::CollectJsonObjectConstructor(
  const State& state, const JsonObjectConstructor& expr) {
  VisitNodes(expr.exprs, [&](const JsonKeyValue& n) {
    CollectExprNode(state, castNode(Node, n.key));
    SDB_ASSERT(n.value);
    CollectExprNode(state, castNode(Node, n.value->raw_expr));
  });
}

void ObjectCollector::CollectJsonArrayConstructor(
  const State& state, const JsonArrayConstructor& expr) {
  VisitNodes(expr.exprs, [&](const JsonValueExpr& n) {
    CollectExprNode(state, castNode(Node, n.raw_expr));
  });
}

void ObjectCollector::CollectAIndirection(const State& state,
                                          const A_Indirection& expr) {
  CollectExprNode(state, expr.arg);
  VisitNodes(expr.indirection, [&](const Node& n) {
    if (IsA(&n, A_Indices)) {
      const auto& indices = *castNode(A_Indices, &n);
      CollectExprNode(state, indices.lidx);
      CollectExprNode(state, indices.uidx);
    } else if (IsA(&n, String)) {
      // nothing to collect
    } else if (IsA(&n, A_Star)) {
      // nothing to collect
    } else {
      SDB_THROW(ERROR_NOT_IMPLEMENTED,
                "only array subscripts are supported in indirection");
    }
  });
}

void ObjectCollector::CollectRangeVar(const State& state, const RangeVar* var,
                                      Objects::AccessType type) {
  if (!var) {
    return;
  }
  const std::string_view relation = var->relname;

  if (!var->catalogname && !var->schemaname) {
    for (const auto* current = &state; current; current = current->parent) {
      if (current->ctes.contains(relation)) {
        return;
      }
    }
  }

  if (_database.data() && var->catalogname && var->catalogname != _database) {
    SDB_THROW(ERROR_BAD_PARAMETER,
              "Cross database queries are not allowed: ", var->catalogname,
              " accessed instead of ", _database);
  }

  auto& object = _objects.ensureData(var->schemaname, relation);
  object.type = static_cast<Objects::AccessType>(
    std::to_underlying(object.type) | std::to_underlying(type));
}

void ObjectCollector::CollectRangeSubSelect(const State& state,
                                            const RangeSubselect& subselect) {
  CollectStmt(&state, subselect.subquery);
}

void ObjectCollector::CollectJoinExpr(const State& state,
                                      const JoinExpr& expr) {
  CollectFromNode(state, expr.larg);
  CollectFromNode(state, expr.rarg);
  CollectExprNode(state, expr.quals);
}

void ObjectCollector::CollectRangeFunction(const State& state,
                                           const RangeFunction& function) {
  SDB_ASSERT(function.functions);
  VisitNodes(function.functions, [&](const List& function_columns) {
    SDB_ASSERT(list_length(&function_columns) == 2);
    auto* function = linitial(&function_columns);
    const auto tag = nodeTag(function);
    switch (tag) {
      case T_FuncCall: {
        auto* n = castNode(FuncCall, function);
        auto name = ParseObjectName(n->funcname, _database);
        if (n->agg_within_group || n->func_variadic) {
          SDB_THROW(ERROR_NOT_IMPLEMENTED,
                    "unsupported function call with aggregate options");
        }
        CollectExprList(state, n->args);
        _objects.ensureData(name.schema, name.relation);
        return;
      } break;
      // case T_SQLValueFunction: {
      //   auto* n = castNode(SQLValueFunction, function);
      //   std::cerr << magic_enum::enum_name(n->op) << std::endl;
      // } break;
      // case T_TypeCast: {
      //   auto* n = castNode(TypeCast, function);
      //   std::cerr << n->typeName << std::endl;
      // } break;
      // case T_CoalesceExpr: {
      //   auto* n = castNode(CoalesceExpr, function);
      //   std::cerr << "coalesce: " << n->location << std::endl;
      // } break;
      // case T_MinMaxExpr: {
      //   auto* n = castNode(MinMaxExpr, function);
      //   std::cerr << magic_enum::enum_name(n->op) << std::endl;
      // } break;
      default:
        break;
    }
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "unsupported function type: ", magic_enum::enum_name(tag));
  });
}

void ObjectCollector::CollectFromNode(const State& state, const Node* node) {
  if (!node) {
    return;
  }
  switch (node->type) {
    case T_RangeVar:
      CollectRangeVar(state, castNode(RangeVar, node),
                      Objects::AccessType::Read);
      break;
    case T_RangeSubselect:
      CollectRangeSubSelect(state, *castNode(RangeSubselect, node));
      break;
    case T_JoinExpr:
      CollectJoinExpr(state, *castNode(JoinExpr, node));
      break;
    case T_RangeFunction:
      CollectRangeFunction(state, *castNode(RangeFunction, node));
      break;
    default:
      SDB_ASSERT(false);
  }
}

void ObjectCollector::CollectFromClause(const State& state,
                                        const List* from_clause) {
  VisitNodes(from_clause,
             [&](const Node& node) { CollectFromNode(state, &node); });
}

void ObjectCollector::CollectExprNode(const State& state, const Node* expr) {
  if (!expr) {
    return;
  }
  switch (expr->type) {
    case T_BoolExpr:
      return CollectExprList(state, castNode(BoolExpr, expr)->args);
    case T_SubLink:
      return CollectSubLink(state, *castNode(SubLink, expr));
    case T_CaseExpr:
      return CollectCaseExpr(state, *castNode(CaseExpr, expr));
    case T_RowExpr:
      return CollectExprList(state, castNode(RowExpr, expr)->args);
    case T_CoalesceExpr:
      return CollectExprList(state, castNode(CoalesceExpr, expr)->args);
    case T_MinMaxExpr:
      return CollectExprList(state, castNode(MinMaxExpr, expr)->args);
    case T_NullTest:
      return CollectExprNode(state,
                             castNode(Node, castNode(NullTest, expr)->arg));
    case T_BooleanTest:
      return CollectExprNode(state,
                             castNode(Node, castNode(BooleanTest, expr)->arg));
    case T_A_Expr:
      return CollectAExpr(state, *castNode(A_Expr, expr));
    case T_FuncCall:
      return CollectFuncCall(state, *castNode(FuncCall, expr));
    case T_A_ArrayExpr:
      return CollectExprList(state, castNode(A_ArrayExpr, expr)->elements);
    case T_ResTarget:
      return CollectExprNode(state, castNode(ResTarget, expr)->val);
    case T_JsonObjectConstructor:
      return CollectJsonObjectConstructor(
        state, *castNode(JsonObjectConstructor, expr));
    case T_JsonArrayConstructor:
      return CollectJsonArrayConstructor(state,
                                         *castNode(JsonArrayConstructor, expr));
    case T_JsonArrayQueryConstructor:
      return CollectStmt(&state,
                         castNode(JsonArrayQueryConstructor, expr)->query);
    case T_TypeCast:
      return CollectExprNode(state, castNode(TypeCast, expr)->arg);
    case T_ParamRef: {
      const auto& param_ref = castNode(ParamRef, expr);
      SDB_ASSERT(param_ref->number > 0);
      if (param_ref->number >= std::numeric_limits<ParamIndex>::max()) {
        SDB_THROW(ERROR_BAD_PARAMETER,
                  "number of parameters must be between 0 and ",
                  std::numeric_limits<ParamIndex>::max());
      }
      _max_bind_param_idx =
        std::max<ParamIndex>(_max_bind_param_idx, param_ref->number);
      return;
    }
    case T_A_Indirection:
      return CollectAIndirection(state, *castNode(A_Indirection, expr));
    case T_SQLValueFunction:
    case T_ColumnRef:
    case T_A_Const:
    case T_CollateClause:
    case T_SetToDefault:
      // nothing to collect
      // TODO(mbkkt) but validate names, etc should be here
      return;
    default:
      SDB_THROW(ERROR_NOT_IMPLEMENTED, "unsupported node type: ", expr->type);
  }
}

void ObjectCollector::CollectExprList(const State& state,
                                      const List* expr_list) {
  VisitNodes(expr_list,
             [&](const Node& expr) { CollectExprNode(state, &expr); });
}

void ObjectCollector::CollectSortClause(const State& state,
                                        const List* sort_clause) {
  VisitNodes(sort_clause, [&](const SortBy& sort_by) {
    CollectExprNode(state, sort_by.node);
  });
}

void ObjectCollector::CollectValuesLists(const State& state,
                                         const List* values_list) {
  int tuple_length = -1;
  VisitNodes(values_list, [&](const List& tuple) {
    if (tuple_length < 0) {
      tuple_length = list_length(&tuple);
    } else if (tuple_length != list_length(&tuple)) {
      SDB_THROW(ERROR_BAD_PARAMETER,
                "VALUES lists must have the same number of columns");
    }
    VisitNodes(&tuple,
               [&](const Node& expr) { CollectExprNode(state, &expr); });
  });
}

void ObjectCollector::CollectWithClause(State& state,
                                        const WithClause* with_clause) {
  if (!with_clause) {
    return;
  }
  VisitNodes(with_clause->ctes, [&](const CommonTableExpr& cte) {
    const auto* ctequery = cte.ctequery;
    SDB_ASSERT(ctequery);
    CollectStmt(&state, ctequery);

    SDB_ASSERT(cte.ctename);
    state.ctes.emplace(cte.ctename);
  });
}

void ObjectCollector::CollectInsertStmt(State& state, const InsertStmt& stmt) {
  CollectWithClause(state, stmt.withClause);
  CollectStmt(&state, stmt.selectStmt);
  CollectRangeVar(state, stmt.relation, Objects::AccessType::Insert);
}

void ObjectCollector::CollectDeleteStmt(State& state, const DeleteStmt& stmt) {
  CollectWithClause(state, stmt.withClause);
  // postgres for DeleteStmt named fromClause as usingClause
  CollectFromClause(state, stmt.usingClause);
  CollectRangeVar(state, stmt.relation, Objects::AccessType::Delete);
}

void ObjectCollector::CollectUpdateStmt(State& state, const UpdateStmt& stmt) {
  CollectWithClause(state, stmt.withClause);
  CollectFromClause(state, stmt.fromClause);
  CollectRangeVar(state, stmt.relation, Objects::AccessType::Update);
}

void ObjectCollector::CollectMergeStmt(State& state, const MergeStmt& stmt) {
  CollectWithClause(state, stmt.withClause);
  CollectFromNode(state, stmt.sourceRelation);
  CollectRangeVar(state, stmt.relation, Objects::AccessType::Merge);
}

void ObjectCollector::CollectSelectStmt(State& state, const SelectStmt* stmt) {
  if (!stmt) {
    return;
  }
  if (stmt->intoClause) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED, "SELECT INTO is not supported yet");
  }
  CollectWithClause(state, stmt->withClause);

  CollectValuesLists(state, stmt->valuesLists);
  CollectFromClause(state, stmt->fromClause);
  CollectSelectStmt(state, stmt->larg);
  CollectSelectStmt(state, stmt->rarg);

  CollectExprList(state, stmt->distinctClause);
  CollectExprList(state, stmt->targetList);

  CollectExprNode(state, stmt->whereClause);
  CollectSortClause(state, stmt->sortClause);
  CollectExprNode(state, stmt->havingClause);
  CollectExprList(state, stmt->groupClause);
  CollectExprNode(state, stmt->limitOffset);
  CollectExprNode(state, stmt->limitCount);
}

void ObjectCollector::CollectExplainStmt(const State* parent,
                                         const ExplainStmt& stmt) {
  CollectStmt(parent, stmt.query);
}

void ObjectCollector::CollectCallStmt(State& state, const CallStmt& stmt) {
  CollectFuncCall(state, *stmt.funccall);
}

void ObjectCollector::CollectViewStmt(State& state, const ViewStmt& stmt) {
  SDB_ASSERT(stmt.query->type == T_SelectStmt);
  CollectSelectStmt(state, castNode(SelectStmt, stmt.query));

  if (_max_bind_param_idx > 0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_PARAMETER),
                    ERR_MSG("there is no parameter $1"));
  }
}

void ObjectCollector::CollectCreateFunctionStmt(
  State& state, const CreateFunctionStmt& stmt) {
  if (!stmt.sql_body) {
    return;
  }

  if (IsA(stmt.sql_body, List)) {
    const auto* outer_list = castNode(List, stmt.sql_body);
    VisitNodes(outer_list, [&](const List& inner_list) {
      VisitNodes(&inner_list,
                 [&](const Node& node) { CollectStmt(&state, &node); });
    });
  } else {
    CollectStmt(&state, stmt.sql_body);
  }

  // function body unnamed parameters are param refs
  // but not counted as bind parameters
  _max_bind_param_idx = 0;
}

void ObjectCollector::CollectCreateStmt(State& state, const CreateStmt& stmt) {
  VisitNodes(stmt.tableElts, [&](const Node& node) {
    if (IsA(&node, ColumnDef)) {
      const auto& col_def = *castNode(ColumnDef, &node);
      VisitNodes(col_def.constraints, [&](const Constraint& constraint) {
        switch (constraint.contype) {
          case CONSTR_DEFAULT:
            CollectExprNode(state, constraint.raw_expr);
            break;
          default:
            break;
        }
      });
    }
  });
}

void ObjectCollector::CollectStmt(const State* parent, const Node* node) {
  if (!node) {
    return;
  }
  State state{.parent = parent};
  switch (node->type) {
    case T_InsertStmt:
      return CollectInsertStmt(state, *castNode(InsertStmt, node));
    case T_DeleteStmt:
      return CollectDeleteStmt(state, *castNode(DeleteStmt, node));
    case T_UpdateStmt:
      return CollectUpdateStmt(state, *castNode(UpdateStmt, node));
    case T_MergeStmt:
      return CollectMergeStmt(state, *castNode(MergeStmt, node));
    case T_SelectStmt:
      return CollectSelectStmt(state, castNode(SelectStmt, node));
    case T_CallStmt:
      return CollectCallStmt(state, *castNode(CallStmt, node));
    case T_ExplainStmt:
      return CollectExplainStmt(parent, *castNode(ExplainStmt, node));
    case T_ViewStmt:
      return CollectViewStmt(state, *castNode(ViewStmt, node));
    case T_CreateFunctionStmt:
      return CollectCreateFunctionStmt(state,
                                       *castNode(CreateFunctionStmt, node));
    case T_CreateStmt:
      return CollectCreateStmt(state, *castNode(CreateStmt, node));
    default:
      break;
  }
}

}  // namespace

void Objects::ObjectData::EnsureTable() const {
  if (!table) {
    SDB_ASSERT(object);
    table = std::make_shared<connector::RocksDBTable>(
      basics::downCast<catalog::Table>(*object));
  }
}

void Collect(std::string_view database, const RawStmt& node, Objects& objects,
             ParamIndex& max_bind_param_idx) {
  ObjectCollector collector{database, objects, max_bind_param_idx};
  SDB_ASSERT(node.stmt);
  collector.CollectStmt(nullptr, node.stmt);
}

void CollectExpr(std::string_view database, const Node& expr,
                 Objects& objects) {
  ParamIndex dummy_idx = 0;
  ObjectCollector collector{database, objects, dummy_idx};
  State dummy_state;
  collector.CollectExprNode(dummy_state, &expr);
}

void Collect(std::string_view database, const RawStmt& node, Objects& objects) {
  ParamIndex dummy = 0;
  Collect(database, node, objects, dummy);
}

Objects::ObjectName ParseObjectName(const List* names,
                                    std::string_view database,
                                    std::string_view default_schema) {
  return VisitName(
    names, absl::Overload{
             [&](std::string_view relation) {
               return Objects::ObjectName{default_schema, relation};
             },
             [](std::string_view schema, std::string_view relation) {
               return Objects::ObjectName{schema, relation};
             },
             [&](std::string_view db, std::string_view schema,
                 std::string_view relation) {
               if (database.data() && db != database) {
                 SDB_THROW(ERROR_BAD_PARAMETER,
                           "Cross database queries are not allowed: ", db,
                           " accessed instead of ", database);
               }
               return Objects::ObjectName{schema, relation};
             },
             [&](...) -> Objects::ObjectName {
               SDB_THROW(
                 ERROR_NOT_IMPLEMENTED,
                 "unsupported function call with too many dotted names");
             },
           });
}

}  // namespace sdb::pg
