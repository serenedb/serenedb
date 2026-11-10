////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "connector/optimizer/ann_join_plan.h"

#include <absl/algorithm/container.h>

#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_limit.hpp>
#include <duckdb/planner/operator/logical_order.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>

#include "connector/column_id.h"
#include "connector/optimizer/iresearch_plan.h"
#include "connector/optimizer/iresearch_plan_common.hpp"
#include "connector/scan/scan_ann_join.h"
#include "connector/scan/scan_function.h"

namespace sdb::optimizer {
namespace {

bool AlwaysTrue(const duckdb::Expression* condition) {
  if (!condition) {
    return true;
  }
  if (condition->GetExpressionClass() !=
      duckdb::ExpressionClass::BOUND_CONSTANT) {
    return false;
  }
  const auto& value =
    condition->Cast<duckdb::BoundConstantExpression>().GetValue();
  return value.type() == duckdb::LogicalType::BOOLEAN && !value.IsNull() &&
         value.GetValue<bool>();
}

bool IsCorrelated(const duckdb::Expression& expr) {
  bool correlated = false;
  duckdb::ExpressionIterator::VisitExpression<duckdb::BoundColumnRefExpression>(
    expr, [&](const duckdb::BoundColumnRefExpression& ref) {
      correlated = correlated || ref.Depth() != 0;
    });
  return correlated;
}

bool OnlyCorrelated(const duckdb::Expression& expr) {
  bool any = false;
  bool only = true;
  duckdb::ExpressionIterator::VisitExpression<duckdb::BoundColumnRefExpression>(
    expr, [&](const duckdb::BoundColumnRefExpression& ref) {
      any = true;
      only = only && ref.Depth() != 0;
    });
  return any && only && !expr.IsVolatile() && !expr.HasSubquery();
}

struct DistanceSides {
  const duckdb::Expression* column = nullptr;
  const duckdb::Expression* value = nullptr;
};

DistanceSides SplitDistance(const duckdb::BoundFunctionExpression& func,
                            duckdb::TableIndex get_index) {
  const auto& children = func.GetChildren();
  if (children.size() != 2) {
    return {};
  }
  for (size_t side = 0; side != 2; ++side) {
    const auto& column = PeelArrayToListCast(*children[side]);
    const auto& value = *children[1 - side];
    if (!IsCorrelated(column) &&
        SingleReferencedTableIndex(column) == get_index &&
        OnlyCorrelated(value)) {
      return {.column = &column, .value = &value};
    }
  }
  return {};
}

bool IsNotNullOf(const duckdb::Expression& conjunct,
                 const duckdb::Expression& distance) {
  if (conjunct.GetExpressionType() !=
      duckdb::ExpressionType::OPERATOR_IS_NOT_NULL) {
    return false;
  }
  const auto& op = conjunct.Cast<duckdb::BoundOperatorExpression>();
  return op.GetChildren().size() == 1 && op.GetChildren()[0]->Equals(distance);
}

bool ClaimedWhole(const connector::ScanBindData& bind) {
  return bind.search.row_filters.empty() && bind.search.duals.empty() &&
         !bind.search.index_only && bind.search.deferred.empty() &&
         !bind.plan_cache.deferred && !bind.plan_cache.declined_parameter;
}

void ReplaceDistance(duckdb::unique_ptr<duckdb::Expression>& expr,
                     const duckdb::Expression& distance,
                     const duckdb::BoundColumnRefExpression& score,
                     duckdb::ClientContext& context) {
  if (expr->Equals(distance)) {
    duckdb::unique_ptr<duckdb::Expression> ref = score.Copy();
    if (expr->GetReturnType() != ref->GetReturnType()) {
      ref = duckdb::BoundCastExpression::AddCastToType(context, std::move(ref),
                                                       expr->GetReturnType());
    }
    ref->SetAlias(expr->GetAlias());
    expr = std::move(ref);
    return;
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      ReplaceDistance(child, distance, score, context);
    });
}

bool ToOuter(duckdb::unique_ptr<duckdb::Expression>& expr,
             const duckdb::vector<duckdb::ColumnBinding>& outer) {
  if (expr->GetExpressionType() == duckdb::ExpressionType::BOUND_COLUMN_REF) {
    auto& ref = expr->Cast<duckdb::BoundColumnRefExpression>();
    if (ref.Depth() != 1 || !absl::c_linear_search(outer, ref.Binding())) {
      return false;
    }
    ref.DepthMutable() = 0;
    return true;
  }
  bool ok = true;
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      ok = ok && ToOuter(child, outer);
    });
  return ok;
}

void Inline(duckdb::unique_ptr<duckdb::Expression>& expr,
            const duckdb::LogicalProjection& select) {
  if (expr->GetExpressionType() == duckdb::ExpressionType::BOUND_COLUMN_REF) {
    const auto& ref = expr->Cast<duckdb::BoundColumnRefExpression>();
    if (ref.Depth() == 0 && ref.Binding().table_index == select.table_index) {
      auto alias = expr->GetAlias();
      expr = select.expressions[ref.Binding().column_index.GetIndex()]->Copy();
      expr->SetAlias(std::move(alias));
    }
    return;
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      Inline(child, select);
    });
}

bool ToRows(duckdb::unique_ptr<duckdb::Expression>& expr,
            duckdb::TableIndex rows) {
  if (expr->GetExpressionType() == duckdb::ExpressionType::BOUND_COLUMN_REF) {
    const auto& ref = expr->Cast<duckdb::BoundColumnRefExpression>();
    if (ref.Depth() != 0 || ref.Binding().table_index != rows) {
      return false;
    }
    expr = duckdb::make_uniq<duckdb::BoundReferenceExpression>(
      expr->GetAlias(), expr->GetReturnType(),
      ref.Binding().column_index.GetIndex());
    return true;
  }
  bool ok = true;
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      ok = ok && ToRows(child, rows);
    });
  return ok;
}

}  // namespace

duckdb::unique_ptr<duckdb::LogicalOperator> PlanAnnJoin(
  duckdb::PlannerExtensionInput& input, duckdb::LateralJoinInput& join) {
  auto& context = input.context;
  if ((join.join_type != duckdb::JoinType::INNER &&
       join.join_type != duckdb::JoinType::LEFT) ||
      !AlwaysTrue(join.condition.get())) {
    return nullptr;
  }

  auto* limit_op = join.right.get();
  const duckdb::LogicalProjection* prune = nullptr;
  if (limit_op->type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION &&
      limit_op->children.size() == 1 &&
      limit_op->children[0]->type ==
        duckdb::LogicalOperatorType::LOGICAL_LIMIT) {
    prune = &limit_op->Cast<duckdb::LogicalProjection>();
    limit_op = limit_op->children[0].get();
  }
  if (limit_op->type != duckdb::LogicalOperatorType::LOGICAL_LIMIT) {
    return nullptr;
  }
  auto& limit = limit_op->Cast<duckdb::LogicalLimit>();
  if (limit.limit_val.Type() != duckdb::LimitNodeType::CONSTANT_VALUE ||
      (limit.offset_val.Type() != duckdb::LimitNodeType::UNSET &&
       !(limit.offset_val.Type() == duckdb::LimitNodeType::CONSTANT_VALUE &&
         limit.offset_val.GetConstantValue() == 0))) {
    return nullptr;
  }
  const auto k = limit.limit_val.GetConstantValue();
  if (k == 0 || k > std::numeric_limits<uint32_t>::max() ||
      limit.children.size() != 1 ||
      limit.children[0]->type !=
        duckdb::LogicalOperatorType::LOGICAL_ORDER_BY) {
    return nullptr;
  }
  auto& order = limit.children[0]->Cast<duckdb::LogicalOrder>();
  if (order.orders.size() != 1 || order.children.size() != 1 ||
      order.children[0]->type !=
        duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    return nullptr;
  }
  auto& select = order.children[0]->Cast<duckdb::LogicalProjection>();
  const duckdb::Expression* distance = order.orders[0].expression.get();
  if (distance->GetExpressionType() ==
      duckdb::ExpressionType::BOUND_COLUMN_REF) {
    const auto& ref = distance->Cast<duckdb::BoundColumnRefExpression>();
    if (ref.Depth() != 0 || ref.Binding().table_index != select.table_index ||
        ref.Binding().column_index.GetIndex() >= select.expressions.size()) {
      return nullptr;
    }
    distance = select.expressions[ref.Binding().column_index.GetIndex()].get();
  }
  if (distance->GetExpressionClass() !=
      duckdb::ExpressionClass::BOUND_FUNCTION) {
    return nullptr;
  }
  const auto& func = distance->Cast<duckdb::BoundFunctionExpression>();
  const auto info = GetAnnFunctionInfo(func);
  if (!info || info->is_norm || order.orders[0].type != info->order) {
    return nullptr;
  }

  std::vector<duckdb::unique_ptr<duckdb::Expression>*> conjuncts;
  auto* below = &select.children[0];
  while ((*below)->type == duckdb::LogicalOperatorType::LOGICAL_FILTER &&
         (*below)->children.size() == 1) {
    for (auto& conjunct : (*below)->expressions) {
      conjuncts.push_back(&conjunct);
    }
    below = &(*below)->children[0];
  }
  if ((*below)->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
    return nullptr;
  }
  auto& get = (*below)->Cast<duckdb::LogicalGet>();
  if (!connector::IsSereneDBScan(get) || !get.children.empty()) {
    return nullptr;
  }
  const auto& scan = get.bind_data->Cast<connector::ScanBindData>();
  if (scan.score.text || scan.score.vector || scan.ts_dict.Active() ||
      scan.offsets.Active() || scan.search.filter) {
    return nullptr;
  }
  const auto sides = SplitDistance(func, get.table_index);
  if (!sides.column) {
    return nullptr;
  }
  const auto field_id =
    ResolveAnnTargetFieldId(*sides.column, get, scan, context);
  if (!irs::field_limits::valid(field_id)) {
    return nullptr;
  }
  const auto* ann =
    scan.relation.inverted_config->FindAnn(field_id, info->metric);
  if (!ann) {
    return nullptr;
  }

  const auto dims = static_cast<duckdb::idx_t>(ann->d);
  const auto query_type =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, dims);
  auto query = sides.value->Copy();
  if (!ToOuter(query, join.left->GetColumnBindings())) {
    return nullptr;
  }
  if (query->GetReturnType() != query_type) {
    query = duckdb::BoundCastExpression::AddCastToType(
      context, std::move(query), query_type);
  }

  const auto score_idx = get.GetColumnIds().size();
  const duckdb::BoundColumnRefExpression score{
    duckdb::Identifier{connector::kScoreName}, duckdb::LogicalType::FLOAT,
    duckdb::ColumnBinding{get.table_index, duckdb::ProjectionIndex{score_idx}}};
  join.right->ResolveOperatorTypes();
  const auto bindings = join.right->GetColumnBindings();
  const auto output_index = bindings.front().table_index;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> outputs;
  for (size_t i = 0; i < bindings.size(); ++i) {
    if (bindings[i].table_index != output_index ||
        bindings[i].column_index.GetIndex() != i) {
      return nullptr;
    }
    auto output =
      prune ? prune->expressions[i]->Copy() : select.expressions[i]->Copy();
    if (prune) {
      Inline(output, select);
    }
    ReplaceDistance(output, *distance, score, context);
    if (!ToRows(output, get.table_index)) {
      return nullptr;
    }
    outputs.push_back(std::move(output));
  }

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> filters;
  for (auto* conjunct : conjuncts) {
    if (IsNotNullOf(**conjunct, *distance)) {
      continue;
    }
    if (IsCorrelated(**conjunct)) {
      return nullptr;
    }
    filters.push_back((*conjunct)->Copy());
  }

  auto bind =
    duckdb::unique_ptr_cast<duckdb::FunctionData, connector::ScanBindData>(
      get.bind_data->Copy());
  bind->score.vector = MakeVectorScorer(field_id, *info, *ann, context);
  bind->score.vector->exact = bind->score.vector->exact || join.exact_nearest;
  bind->score.order = info->order;
  bind->score.top_k = k;
  if (!filters.empty()) {
    IResearchPushdownComplexFilter(context, get, bind.get(), filters);
    if (!filters.empty() || !ClaimedWhole(*bind)) {
      return nullptr;
    }
  }

  [[maybe_unused]] const auto appended =
    AppendVirtualGetColumn(*bind, get, connector::kInvertedIndexScoreId,
                           duckdb::LogicalType::FLOAT, connector::kScoreName);
  SDB_ASSERT(appended == score_idx);

  auto ann_bind = duckdb::make_uniq<connector::AnnJoinBindData>();
  ann_bind->scan = std::move(bind);
  ann_bind->columns = get.GetColumnIds();
  ann_bind->left_join = join.join_type == duckdb::JoinType::LEFT;
  duckdb::vector<duckdb::Identifier> names;
  for (const auto& output : outputs) {
    names.push_back(output->GetAlias());
  }
  ann_bind->outputs = std::move(outputs);
  auto result = duckdb::make_uniq<duckdb::LogicalGet>(
    output_index,
    duckdb::BoundTableFunction{connector::CreateAnnJoinFunction()},
    std::move(ann_bind), join.right->types, std::move(names),
    duckdb::virtual_column_map_t{});
  for (duckdb::idx_t i = 0; i < bindings.size(); ++i) {
    result->AddColumnId(i);
  }
  const auto outer_columns = join.left->GetColumnBindings().size();
  for (duckdb::idx_t i = 0; i < outer_columns; ++i) {
    result->projected_input.push_back(i);
  }
  result->expressions.push_back(std::move(query));
  result->AddChild(std::move(join.left));
  result->ResolveOperatorTypes();
  return result;
}

}  // namespace sdb::optimizer
