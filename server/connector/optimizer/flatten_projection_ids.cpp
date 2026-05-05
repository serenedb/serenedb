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

#include "connector/optimizer/flatten_projection_ids.h"

#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/logical_operator_visitor.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_join.hpp>
#include <duckdb/planner/operator/logical_order.hpp>

#include "basics/containers/flat_hash_map.h"

namespace sdb::optimizer {
namespace {

// Rewrites BoundColumnRefExpression bindings that reference a specific
// LogicalGet's table_index using the old->new column_index map.
class BindingRewriter : public duckdb::LogicalOperatorVisitor {
 public:
  BindingRewriter(
    duckdb::TableIndex table_index,
    const containers::FlatHashMap<duckdb::idx_t, duckdb::idx_t>& old_to_new)
    : _table_index(table_index), _old_to_new(old_to_new) {}

 protected:
  duckdb::unique_ptr<duckdb::Expression> VisitReplace(
    duckdb::BoundColumnRefExpression& expr,
    duckdb::unique_ptr<duckdb::Expression>* /*expr_ptr*/) final {
    if (expr.binding.table_index == _table_index) {
      auto it = _old_to_new.find(expr.binding.column_index);
      if (it != _old_to_new.end()) {
        expr.binding.column_index = duckdb::ProjectionIndex(it->second);
      }
    }
    return nullptr;
  }

 private:
  duckdb::TableIndex _table_index;
  const containers::FlatHashMap<duckdb::idx_t, duckdb::idx_t>& _old_to_new;
};

}  // namespace

void FlattenProjectionIds(duckdb::LogicalOperator& root,
                          duckdb::LogicalGet& get) {
  if (get.projection_ids.empty()) {
    return;
  }
  const auto& column_ids = get.GetColumnIds();
  containers::FlatHashMap<duckdb::idx_t, duckdb::idx_t> old_to_new;
  old_to_new.reserve(get.projection_ids.size());
  duckdb::vector<duckdb::ColumnIndex> new_column_ids;
  new_column_ids.reserve(get.projection_ids.size());
  for (duckdb::idx_t i = 0; i < get.projection_ids.size(); ++i) {
    const auto old = get.projection_ids[i].GetIndex();
    old_to_new.emplace(old, i);
    new_column_ids.push_back(column_ids[old]);
  }
  get.SetColumnIds(std::move(new_column_ids));
  get.projection_ids.clear();
  // types is a cache rebuilt by ResolveTypes; clear so it matches the new
  // column_ids size if anything reads it before ResolveTypes fires again.
  get.types.clear();

  BindingRewriter rewriter(get.table_index, old_to_new);
  rewriter.VisitOperator(root);
}

void ClearProjectionMaps(duckdb::LogicalOperator& plan) {
  for (auto& child : plan.children) {
    ClearProjectionMaps(*child);
  }
  switch (plan.type) {
    case duckdb::LogicalOperatorType::LOGICAL_FILTER:
      plan.Cast<duckdb::LogicalFilter>().projection_map.clear();
      break;
    case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY:
      plan.Cast<duckdb::LogicalOrder>().projection_map.clear();
      break;
    case duckdb::LogicalOperatorType::LOGICAL_ASOF_JOIN:
    case duckdb::LogicalOperatorType::LOGICAL_DELIM_JOIN:
    case duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
    case duckdb::LogicalOperatorType::LOGICAL_ANY_JOIN: {
      auto& join = plan.Cast<duckdb::LogicalJoin>();
      join.left_projection_map.clear();
      join.right_projection_map.clear();
    } break;
    default:
      break;
  }
}

}  // namespace sdb::optimizer
