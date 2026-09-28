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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/optimizer/optimizer.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/column_binding_map.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_top_n.hpp>
#include <utility>
#include <vector>

#include "connector/functions/ai/common.h"

namespace sdb::connector::ai {
namespace {

bool IsAICall(const duckdb::Expression& expr) {
  return expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION &&
         dynamic_cast<const AIFunctionData*>(
           expr.Cast<duckdb::BoundFunctionExpression>().BindInfo().get()) !=
           nullptr;
}

bool HasAICall(const duckdb::Expression& expr) {
  bool found = IsAICall(expr);
  duckdb::ExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::Expression& child) {
      found = found || HasAICall(child);
    });
  return found;
}

bool Deferrable(const duckdb::Expression& expr, bool& has_ai) {
  if (IsAICall(expr)) {
    has_ai = true;
  } else if (expr.GetExpressionClass() ==
               duckdb::ExpressionClass::BOUND_FUNCTION &&
             expr.Cast<duckdb::BoundFunctionExpression>()
                 .Function()
                 .GetStability() == duckdb::FunctionStability::VOLATILE) {
    return false;
  } else if (expr.GetExpressionClass() ==
               duckdb::ExpressionClass::BOUND_COLUMN_REF &&
             expr.Cast<duckdb::BoundColumnRefExpression>().Depth() != 0) {
    return false;
  }
  bool ok = true;
  duckdb::ExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::Expression& child) {
      ok = ok && Deferrable(child, has_ai);
    });
  return ok;
}

bool IsRename(const duckdb::LogicalOperator& op) {
  return op.type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION &&
         op.children[0]->type ==
           duckdb::LogicalOperatorType::LOGICAL_PROJECTION &&
         absl::c_all_of(op.expressions, [](const auto& expr) {
           return expr->GetExpressionClass() ==
                  duckdb::ExpressionClass::BOUND_COLUMN_REF;
         });
}

void Rebind(duckdb::unique_ptr<duckdb::Expression>& expr,
            const duckdb::LogicalProjection& rename) {
  duckdb::ExpressionIterator::VisitExpressionMutable<
    duckdb::BoundColumnRefExpression>(
    expr, [&](duckdb::BoundColumnRefExpression& ref, auto&) {
      if (ref.Binding().table_index == rename.table_index) {
        ref.BindingMutable() =
          rename.expressions[ref.Binding().column_index.GetIndex()]
            ->Cast<duckdb::BoundColumnRefExpression>()
            .Binding();
      }
    });
}

void DeferTopN(duckdb::unique_ptr<duckdb::LogicalOperator>& slot,
               duckdb::Binder& binder, const duckdb::LogicalOperator* parent) {
  auto& topn = slot->Cast<duckdb::LogicalTopN>();
  if (!topn.projection_map.empty() &&
      (parent == nullptr ||
       parent->type != duckdb::LogicalOperatorType::LOGICAL_PROJECTION)) {
    return;
  }
  std::vector<duckdb::LogicalProjection*> renames;
  auto* child = topn.children[0].get();
  while (IsRename(*child)) {
    renames.push_back(&child->Cast<duckdb::LogicalProjection>());
    child = child->children[0].get();
  }
  if (child->type != duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    return;
  }
  auto& proj = child->Cast<duckdb::LogicalProjection>();

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> orders;
  for (const auto& order : topn.orders) {
    auto expr = order.expression->Copy();
    for (const auto* rename : renames) {
      Rebind(expr, *rename);
    }
    orders.push_back(std::move(expr));
  }
  std::vector<bool> sorted(proj.expressions.size());
  for (const auto& expr : orders) {
    duckdb::ExpressionIterator::VisitExpression<
      duckdb::BoundColumnRefExpression>(
      *expr, [&](const duckdb::BoundColumnRefExpression& ref) {
        if (ref.Binding().table_index == proj.table_index) {
          sorted[ref.Binding().column_index.GetIndex()] = true;
        }
      });
  }
  std::vector<bool> moved(proj.expressions.size());
  for (size_t i = 0; i != proj.expressions.size(); ++i) {
    bool has_ai = false;
    moved[i] = !sorted[i] && Deferrable(*proj.expressions[i], has_ai) && has_ai;
  }
  if (absl::c_none_of(moved, [](bool m) { return m; })) {
    return;
  }

  const auto lower_index = binder.GenerateTableIndex();
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> lower;
  std::vector<size_t> position(proj.expressions.size());
  for (size_t i = 0; i != proj.expressions.size(); ++i) {
    if (moved[i]) {
      continue;
    }
    auto& expr = proj.expressions[i];
    position[i] = lower.size();
    auto ref = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      expr->GetAlias(), expr->GetReturnType(),
      duckdb::ColumnBinding{lower_index,
                            duckdb::ProjectionIndex{lower.size()}});
    lower.push_back(std::move(expr));
    expr = std::move(ref);
  }
  duckdb::column_binding_map_t<size_t> passthrough;
  for (size_t i = 0; i != proj.expressions.size(); ++i) {
    if (!moved[i]) {
      continue;
    }
    duckdb::ExpressionIterator::VisitExpressionMutable<
      duckdb::BoundColumnRefExpression>(
      proj.expressions[i], [&](duckdb::BoundColumnRefExpression& ref, auto&) {
        const auto [it, inserted] =
          passthrough.try_emplace(ref.Binding(), lower.size());
        if (inserted) {
          lower.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
            ref.GetAlias(), ref.GetReturnType(), ref.Binding()));
        }
        ref.BindingMutable() = duckdb::ColumnBinding{
          lower_index, duckdb::ProjectionIndex{it->second}};
      });
  }
  for (size_t k = 0; k != orders.size(); ++k) {
    duckdb::ExpressionIterator::VisitExpressionMutable<
      duckdb::BoundColumnRefExpression>(
      orders[k], [&](duckdb::BoundColumnRefExpression& ref, auto&) {
        if (ref.Binding().table_index == proj.table_index) {
          ref.BindingMutable() = duckdb::ColumnBinding{
            lower_index, duckdb::ProjectionIndex{
                           position[ref.Binding().column_index.GetIndex()]}};
        }
      });
    topn.orders[k].expression = std::move(orders[k]);
  }

  auto chain = std::move(topn.children[0]);
  auto upper =
    renames.empty() ? std::move(chain) : std::move(renames.back()->children[0]);
  auto low =
    duckdb::make_uniq<duckdb::LogicalProjection>(lower_index, std::move(lower));
  low->children.push_back(std::move(upper->children[0]));
  if (low->children[0]->has_estimated_cardinality) {
    low->SetEstimatedCardinality(low->children[0]->estimated_cardinality);
  }
  topn.children[0] = std::move(low);
  topn.projection_map.clear();
  if (topn.has_estimated_cardinality) {
    upper->SetEstimatedCardinality(topn.estimated_cardinality);
    for (auto* rename : renames) {
      rename->SetEstimatedCardinality(topn.estimated_cardinality);
    }
  }
  upper->children[0] = std::move(slot);
  if (renames.empty()) {
    slot = std::move(upper);
  } else {
    renames.back()->children[0] = std::move(upper);
    slot = std::move(chain);
  }
  slot->ResolveOperatorTypes();
}

void Defer(duckdb::unique_ptr<duckdb::LogicalOperator>& op,
           duckdb::Binder& binder, const duckdb::LogicalOperator* parent) {
  for (auto& child : op->children) {
    Defer(child, binder, op.get());
  }
  if (op->type == duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
    DeferTopN(op, binder, parent);
  }
}

void DeferAICalls(duckdb::OptimizerExtensionInput& input,
                  duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  Defer(plan, input.optimizer.binder, nullptr);
}

void FilterBeforeAICalls(duckdb::LogicalOperator& op) {
  for (auto& child : op.children) {
    FilterBeforeAICalls(*child);
  }
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_FILTER &&
      absl::c_any_of(op.expressions,
                     [](const auto& expr) { return HasAICall(*expr); })) {
    std::ranges::stable_partition(
      op.expressions, [](const auto& expr) { return !expr->CanThrow(); });
  }
}

void FilterBeforeAICalls(duckdb::OptimizerExtensionInput&,
                         duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  FilterBeforeAICalls(*plan);
}

}  // namespace

void RegisterAIOptimizer(duckdb::DatabaseInstance& db) {
  duckdb::OptimizerExtension::Register(
    db.config, duckdb::OptimizerExtension{
                 .rule = &DeferAICalls,
                 .anchor = duckdb::OptimizerType::TOP_N,
                 .where = duckdb::OptimizerHookPosition::After,
               });
  duckdb::OptimizerExtension::Register(
    db.config, duckdb::OptimizerExtension{
                 .rule = &FilterBeforeAICalls,
                 .anchor = duckdb::OptimizerType::REORDER_FILTER,
                 .where = duckdb::OptimizerHookPosition::After,
               });
}

}  // namespace sdb::connector::ai
