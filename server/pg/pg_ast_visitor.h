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

#pragma once

#include <functional>
#include <type_traits>

#include "pg/pg_list_utils.h"
#include "pg/sql_utils.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

template<typename TargetType>
class AstVisitor {
 public:
  template<typename VisitorFunc>
  void Visit(const Node* root_node, VisitorFunc&& visitor_func) {
    _visitor_func = std::forward<VisitorFunc>(visitor_func);
    TraverseNode(root_node);
  }

  template<typename VisitorFunc>
  void VisitList(const List* nodes_list, VisitorFunc&& visitor_func) {
    _visitor_func = std::forward<VisitorFunc>(visitor_func);
    TraverseList(nodes_list);
  }

 private:
  std::function<void(const TargetType&)> _visitor_func;

  bool IsTargetType(const Node* node) const {
    if (!node)
      return false;

    NodeTag tag = nodeTag(node);
    return GetNodeTag<TargetType>() == tag;
  }

  void VisitIfTarget(const Node* node) {
    if (!IsTargetType(node)) {
      return;
    }

    NodeTag tag = nodeTag(node);

    if (GetNodeTag<TargetType>() == tag) {
      _visitor_func(*castNode(TargetType, node));
    }
  }

  void TraverseNode(const Node* node) {
    if (!node) {
      return;
    }

    VisitIfTarget(node);

    switch (nodeTag(node)) {
      case T_SelectStmt:
      case T_InsertStmt:
      case T_UpdateStmt:
      case T_DeleteStmt:
      case T_RangeSubselect:
      case T_CommonTableExpr:
        // We do not traverse the statements here
        // because subqueries processed differently
        break;
      case T_FuncCall:
        TraverseFuncCall(castNode(FuncCall, node));
        break;

      case T_A_Expr:
        TraverseAExpr(castNode(A_Expr, node));
        break;

      case T_BoolExpr:
        TraverseBoolExpr(castNode(BoolExpr, node));
        break;

      case T_CaseExpr:
        TraverseCaseExpr(castNode(CaseExpr, node));
        break;

      case T_CoalesceExpr:
        TraverseCoalesceExpr(castNode(CoalesceExpr, node));
        break;

      case T_MinMaxExpr:
        TraverseMinMaxExpr(castNode(MinMaxExpr, node));
        break;

      case T_SubLink:
        TraverseSubLink(castNode(SubLink, node));
        break;

      case T_TypeCast:
        TraverseTypeCast(castNode(TypeCast, node));
        break;

      case T_A_ArrayExpr:
        TraverseArrayExpr(castNode(A_ArrayExpr, node));
        break;

      case T_NullTest:
        TraverseNullTest(castNode(NullTest, node));
        break;

      case T_BooleanTest:
        TraverseBooleanTest(castNode(BooleanTest, node));
        break;

      case T_ResTarget:
        TraverseResTarget(castNode(ResTarget, node));
        break;

      case T_JoinExpr:
        TraverseJoinExpr(castNode(JoinExpr, node));
        break;

      case T_RangeFunction:
        TraverseRangeFunction(castNode(RangeFunction, node));
        break;

      case T_WithClause:
        TraverseWithClause(castNode(WithClause, node));
        break;

      case T_WindowFunc:
        TraverseWindowFunc(castNode(WindowFunc, node));
        break;

      case T_Aggref:
        TraverseAggref(castNode(Aggref, node));
        break;

      case T_GroupingFunc:
        TraverseGroupingFunc(castNode(GroupingFunc, node));
        break;

      case T_RowExpr:
        TraverseRowExpr(castNode(RowExpr, node));
        break;

      case T_A_Indirection:
        TraverseIndirection(castNode(A_Indirection, node));
        break;

      case T_CollateClause:
        TraverseCollateClause(castNode(CollateClause, node));
        break;

      case T_SortBy:
        TraverseSortBy(castNode(SortBy, node));
        break;

      case T_List:
        TraverseList(castNode(List, node));
        break;
      // doesn't contain child nodes
      case T_ColumnRef:
      case T_A_Const:
      case T_ParamRef:
      case T_A_Star:
      case T_RangeVar:
      case T_SetToDefault:
      case T_CurrentOfExpr:
        break;

      default:
        break;
    }
  }

  void TraverseList(const List* list) {
    if (!list) {
      return;
    }

    VisitNodes(list, [this](const Node& node) { TraverseNode(&node); });
  }

  void TraverseSelectStmt(const SelectStmt* stmt) {
    if (!stmt) {
      return;
    }

    TraverseWithClause(stmt->withClause);
    TraverseList(stmt->targetList);
    TraverseList(stmt->fromClause);
    TraverseNode(stmt->whereClause);
    TraverseList(stmt->groupClause);
    TraverseNode(stmt->havingClause);
    TraverseList(stmt->windowClause);
    TraverseList(stmt->sortClause);
    TraverseNode(stmt->limitOffset);
    TraverseNode(stmt->limitCount);
    TraverseList(stmt->lockingClause);
    TraverseNode(castNode(Node, stmt->larg));
    TraverseNode(castNode(Node, stmt->rarg));
    TraverseList(stmt->valuesLists);
  }

  void TraverseInsertStmt(const InsertStmt* stmt) {
    if (!stmt) {
      return;
    }

    TraverseWithClause(stmt->withClause);
    TraverseNode(castNode(Node, stmt->relation));
    TraverseList(stmt->cols);
    TraverseNode(stmt->selectStmt);
    TraverseNode(castNode(Node, stmt->onConflictClause));
    TraverseList(stmt->returningList);
  }

  void TraverseUpdateStmt(const UpdateStmt* stmt) {
    if (!stmt) {
      return;
    }

    TraverseWithClause(stmt->withClause);
    TraverseNode(castNode(Node, stmt->relation));
    TraverseList(stmt->targetList);
    TraverseNode(stmt->whereClause);
    TraverseList(stmt->fromClause);
    TraverseList(stmt->returningList);
  }

  void TraverseDeleteStmt(const DeleteStmt* stmt) {
    if (!stmt) {
      return;
    }

    TraverseWithClause(stmt->withClause);
    TraverseNode(castNode(Node, stmt->relation));
    TraverseList(stmt->usingClause);
    TraverseNode(stmt->whereClause);
    TraverseList(stmt->returningList);
  }

  void TraverseFuncCall(const FuncCall* func_call) {
    if (!func_call) {
      return;
    }

    TraverseList(func_call->args);
    TraverseNode(func_call->agg_filter);
    TraverseList(func_call->agg_order);
    TraverseNode(castNode(Node, func_call->over));
  }

  void TraverseAExpr(const A_Expr* a_expr) {
    if (!a_expr) {
      return;
    }

    TraverseNode(a_expr->lexpr);
    TraverseNode(a_expr->rexpr);
  }

  void TraverseBoolExpr(const BoolExpr* bool_expr) {
    if (!bool_expr) {
      return;
    }

    TraverseList(bool_expr->args);
  }

  void TraverseCaseExpr(const CaseExpr* case_expr) {
    if (!case_expr) {
      return;
    }

    TraverseNode(castNode(Node, case_expr->arg));
    TraverseNode(castNode(Node, case_expr->defresult));

    VisitNodes(case_expr->args, [this](const CaseWhen& case_when) {
      TraverseNode(castNode(Node, case_when.expr));
      TraverseNode(castNode(Node, case_when.result));
    });
  }

  void TraverseCoalesceExpr(const CoalesceExpr* coalesce_expr) {
    if (!coalesce_expr) {
      return;
    }

    TraverseList(coalesce_expr->args);
  }

  void TraverseMinMaxExpr(const MinMaxExpr* minmax_expr) {
    if (!minmax_expr) {
      return;
    }

    TraverseList(minmax_expr->args);
  }

  void TraverseSubLink(const SubLink* sublink) {
    if (!sublink) {
      return;
    }

    TraverseNode(sublink->testexpr);
    TraverseList(sublink->operName);
    // We do not traverse the subselect here
    // because subqueries processed differently
  }

  void TraverseTypeCast(const TypeCast* type_cast) {
    if (!type_cast) {
      return;
    }

    TraverseNode(type_cast->arg);
  }

  void TraverseArrayExpr(const A_ArrayExpr* array_expr) {
    if (!array_expr) {
      return;
    }

    TraverseList(array_expr->elements);
  }

  void TraverseNullTest(const NullTest* null_test) {
    if (!null_test) {
      return;
    }

    TraverseNode(castNode(Node, null_test->arg));
  }

  void TraverseBooleanTest(const BooleanTest* boolean_test) {
    if (!boolean_test) {
      return;
    }

    TraverseNode(castNode(Node, boolean_test->arg));
  }

  void TraverseResTarget(const ResTarget* res_target) {
    if (!res_target) {
      return;
    }

    TraverseNode(res_target->val);
  }

  void TraverseJoinExpr(const JoinExpr* join_expr) {
    if (!join_expr) {
      return;
    }

    TraverseNode(join_expr->larg);
    TraverseNode(join_expr->rarg);
    TraverseNode(join_expr->quals);
    TraverseList(join_expr->usingClause);
  }

  void TraverseRangeFunction(const RangeFunction* range_function) {
    if (!range_function) {
      return;
    }

    TraverseList(range_function->functions);
  }

  void TraverseWithClause(const WithClause* with_clause) {
    if (!with_clause) {
      return;
    }

    TraverseList(with_clause->ctes);
  }

  void TraverseWindowFunc(const WindowFunc* window_func) {
    if (!window_func) {
      return;
    }

    TraverseList(window_func->args);
  }

  void TraverseAggref(const Aggref* aggref) {
    if (!aggref) {
      return;
    }

    TraverseList(aggref->args);
    TraverseList(aggref->aggorder);
    TraverseList(aggref->aggdistinct);
    TraverseNode(castNode(Node, aggref->aggfilter));
  }

  void TraverseGroupingFunc(const GroupingFunc* grouping_func) {
    if (!grouping_func) {
      return;
    }

    TraverseList(grouping_func->args);
  }

  void TraverseRowExpr(const RowExpr* row_expr) {
    if (!row_expr) {
      return;
    }

    TraverseList(row_expr->args);
  }

  void TraverseIndirection(const A_Indirection* indirection) {
    if (!indirection) {
      return;
    }

    TraverseNode(indirection->arg);
    TraverseList(indirection->indirection);
  }

  void TraverseCollateClause(const CollateClause* collate_clause) {
    if (!collate_clause) {
      return;
    }

    TraverseNode(collate_clause->arg);
  }

  void TraverseSortBy(const SortBy* sort_by) {
    if (!sort_by) {
      return;
    }

    TraverseNode(sort_by->node);
  }
};

}  // namespace sdb::pg
