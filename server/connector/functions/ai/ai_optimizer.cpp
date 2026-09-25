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

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp>
#include <duckdb/function/function_binder.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/optimizer/column_binding_replacer.hpp>
#include <duckdb/optimizer/optimizer.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_aggregate_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <iresearch/utils/assert.hpp>
#include <utility>

#include "connector/functions/ai/ai_operator.h"
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

bool IsConditional(const duckdb::Expression& expr) {
  switch (expr.GetExpressionType()) {
    case duckdb::ExpressionType::CASE_EXPR:
    case duckdb::ExpressionType::OPERATOR_COALESCE:
    case duckdb::ExpressionType::OPERATOR_TRY:
    case duckdb::ExpressionType::CONJUNCTION_AND:
    case duckdb::ExpressionType::CONJUNCTION_OR:
      return true;
    default:
      return false;
  }
}

class Extractor {
 public:
  explicit Extractor(duckdb::Binder& binder) : _binder{binder} {}

  void Collect(duckdb::unique_ptr<duckdb::Expression>& expr) {
    if (IsAICall(*expr)) {
      const auto& children =
        expr->Cast<duckdb::BoundFunctionExpression>().GetChildren();
      if (absl::c_none_of(
            children, [](const auto& child) { return HasAICall(*child); })) {
        Replace(expr);
        return;
      }
    } else if (IsConditional(*expr)) {
      return;
    }
    duckdb::ExpressionIterator::EnumerateChildren(
      *expr,
      [&](duckdb::unique_ptr<duckdb::Expression>& child) { Collect(child); });
  }

  bool Wrap(duckdb::unique_ptr<duckdb::LogicalOperator>& child) {
    if (_calls.empty()) {
      return false;
    }
    auto evaluate =
      duckdb::make_uniq<LogicalAIEvaluate>(_table_index, std::move(_calls));
    evaluate->children.push_back(std::move(child));
    child = std::move(evaluate);
    return true;
  }

 private:
  void Replace(duckdb::unique_ptr<duckdb::Expression>& expr) {
    if (_calls.empty()) {
      _table_index = _binder.GenerateTableIndex();
    }
    auto ref = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      expr->GetAlias(), expr->GetReturnType(),
      duckdb::ColumnBinding{_table_index,
                            duckdb::ProjectionIndex{_calls.size()}});
    _calls.push_back(std::move(expr));
    expr = std::move(ref);
  }

  duckdb::Binder& _binder;
  duckdb::TableIndex _table_index;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> _calls;
};

bool ExtractCalls(duckdb::LogicalOperator& op, duckdb::Binder& binder) {
  Extractor extractor{binder};
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    for (auto& group : op.Cast<duckdb::LogicalAggregate>().groups) {
      extractor.Collect(group);
    }
  }
  for (auto& expr : op.expressions) {
    extractor.Collect(expr);
  }
  return extractor.Wrap(op.children[0]);
}

void EvaluateFilter(duckdb::LogicalFilter& filter, duckdb::Binder& binder) {
  duckdb::LogicalFilter::SplitPredicates(filter.expressions);
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> kept;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> below;
  for (auto& expr : filter.expressions) {
    (HasAICall(*expr) ? kept : below).push_back(std::move(expr));
  }
  filter.expressions = std::move(kept);
  if (filter.expressions.empty()) {
    filter.expressions = std::move(below);
    return;
  }
  auto& child = filter.children[0];
  const auto columns = child->GetColumnBindings().size();
  if (!below.empty()) {
    auto lower = duckdb::make_uniq<duckdb::LogicalFilter>();
    lower->expressions = std::move(below);
    lower->SetEstimatedCardinality(child->estimated_cardinality);
    lower->children.push_back(std::move(child));
    child = std::move(lower);
  }
  bool extracted = false;
  while (ExtractCalls(filter, binder)) {
    extracted = true;
  }
  if (extracted && filter.projection_map.empty()) {
    for (auto index : duckdb::ProjectionIndex::GetIndexes(columns)) {
      filter.projection_map.push_back(index);
    }
  }
}

duckdb::unique_ptr<duckdb::BoundAggregateExpression> BindList(
  duckdb::ClientContext& context, duckdb::BoundAggregateExpression& aggregate) {
  auto& catalog = duckdb::Catalog::GetSystemCatalog(context);
  auto& entry = catalog.GetEntry<duckdb::AggregateFunctionCatalogEntry>(
    context, duckdb::QualifiedName(catalog.GetName(), DEFAULT_SCHEMA,
                                   duckdb::Identifier{"list"}));
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
  children.push_back(std::move(aggregate.GetChildrenMutable()[0]));
  duckdb::FunctionBinder binder{context};
  duckdb::ErrorData error;
  const auto best = binder.BindFunction(duckdb::Identifier{"list"},
                                        entry.functions, children, {}, error);
  SDB_ASSERT(best.IsValid());
  auto list = binder.BindAggregateFunction(
    entry.functions.GetFunctionByOffset(best.GetIndex()), std::move(children),
    std::move(aggregate.GetFilterMutable()), aggregate.GetAggregateType());
  list->GetOrderBysMutable() = std::move(aggregate.GetOrderBysMutable());
  return list;
}

void RewriteAggregates(duckdb::unique_ptr<duckdb::LogicalOperator>& op,
                       duckdb::LogicalOperator& root,
                       duckdb::ClientContext& context, duckdb::Binder& binder) {
  auto& aggregate = op->Cast<duckdb::LogicalAggregate>();
  duckdb::TableIndex table_index;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> reducers;
  duckdb::ColumnBindingReplacer replacer;
  for (size_t i = 0; i != aggregate.expressions.size(); ++i) {
    auto& expr = aggregate.expressions[i];
    if (expr->GetExpressionClass() !=
          duckdb::ExpressionClass::BOUND_AGGREGATE ||
        !IsAIAggregate(expr->Cast<duckdb::BoundAggregateExpression>())) {
      continue;
    }
    if (reducers.empty()) {
      table_index = binder.GenerateTableIndex();
    }
    auto& call = expr->Cast<duckdb::BoundAggregateExpression>();
    auto list = BindList(context, call);
    const duckdb::ColumnBinding binding{aggregate.aggregate_index,
                                        duckdb::ProjectionIndex{i}};
    replacer.replacement_bindings.emplace_back(
      binding, duckdb::ColumnBinding{table_index,
                                     duckdb::ProjectionIndex{reducers.size()}});
    reducers.push_back(MakeAggregateReducer(
      call, duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
              list->GetReturnType(), binding)));
    expr = std::move(list);
  }
  if (reducers.empty()) {
    return;
  }
  replacer.stop_operator = op.get();
  replacer.VisitOperator(root);
  auto evaluate =
    duckdb::make_uniq<LogicalAIEvaluate>(table_index, std::move(reducers));
  evaluate->children.push_back(std::move(op));
  op = std::move(evaluate);
}

void Rewrite(duckdb::unique_ptr<duckdb::LogicalOperator>& op,
             duckdb::LogicalOperator& root, duckdb::ClientContext& context,
             duckdb::Binder& binder, bool under_limit) {
  const auto type = op->type;
  const bool streaming =
    type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION ||
    type == duckdb::LogicalOperatorType::LOGICAL_FILTER;
  for (auto& child : op->children) {
    Rewrite(child, root, context, binder,
            type == duckdb::LogicalOperatorType::LOGICAL_LIMIT ||
              (under_limit && streaming));
  }
  switch (type) {
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
      if (!under_limit) {
        while (ExtractCalls(*op, binder)) {
        }
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_FILTER:
      if (!under_limit) {
        EvaluateFilter(op->Cast<duckdb::LogicalFilter>(), binder);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
      while (ExtractCalls(*op, binder)) {
      }
      RewriteAggregates(op, root, context, binder);
      break;
    default:
      break;
  }
}

void OptimizeAICalls(duckdb::OptimizerExtensionInput& input,
                     duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  Rewrite(plan, *plan, input.context, input.optimizer.binder, false);
}

}  // namespace

void RegisterAIOptimizer(duckdb::DatabaseInstance& db) {
  duckdb::OptimizerExtension::Register(
    db.config,
    duckdb::OptimizerExtension{.optimize_function = &OptimizeAICalls});
}

}  // namespace sdb::connector::ai
