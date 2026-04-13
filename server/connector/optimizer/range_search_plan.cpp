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

// Optimizer rule: range search plan rewriter
//
// Detected pattern (logical plan):
//   LogicalFilter  (distance_func(col, const_vector) < radius_const)
//    - LogicalGet  (function = "serenedb_scan")
//
// Transformation (if an InvertedIndex exists on the distance column):
//   1. Mark the LogicalGet bind data with RangeSearchScan{shard, field, query,
//      radius}.
//   2. Remove the matched filter expression. If no other filter predicates
//      remain the LogicalFilter node is dropped entirely; otherwise it is kept
//      so that DuckDB applies the remaining predicates on top.
//
// Recognised comparison forms:
//   distance_func(col, vec) <  radius   (COMPARE_LESSTHAN)
//   distance_func(col, vec) <= radius   (COMPARE_LESSTHANOREQUALTO)
//   radius >  distance_func(col, vec)   (COMPARE_GREATERTHAN, normalised)
//   radius >= distance_func(col, vec)   (COMPARE_GREATERTHANOREQUALTO,
//   normalised)
//
// Skipped when:
//   - The scan source is already set (e.g., a SearchScan from pushdown).
//   - No suitable InvertedIndex is found for the distance column.
//   - The HNSW shard has no live snapshot.

#include "connector/optimizer/range_search_plan.h"

#include <absl/base/internal/endian.h>

#include <duckdb/main/config.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>

#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_table_function.h"
#include "functions/vector.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/index_shard.h"

namespace sdb::optimizer {
namespace {

static bool IsDistanceFunction(std::string_view name) {
  return name == functions::kL2Distance;
}

// Extract a float query vector from a constant ARRAY Value.
// Returns false if the value is not a flat float/double array.
static bool TryExtractQueryVector(const duckdb::Value& val,
                                  std::vector<float>& out) {
  using namespace duckdb;
  const auto type_id = val.type().id();

  const std::vector<Value>* children = nullptr;
  if (type_id == LogicalTypeId::ARRAY) {
    children = &ArrayValue::GetChildren(val);
  } else {
    return false;
  }

  if (!children || children->empty()) {
    return false;
  }

  out.reserve(children->size());
  for (const auto& child : *children) {
    if (child.IsNull()) {
      return false;
    }
    switch (child.type().id()) {
      case LogicalTypeId::FLOAT:
        out.push_back(child.GetValue<float>());
        break;
      case LogicalTypeId::DOUBLE:
        out.push_back(static_cast<float>(child.GetValue<double>()));
        break;
      default:
        return false;
    }
  }
  return !out.empty();
}

class RangeSearchPlanOptimizer : public duckdb::OptimizerExtension {
 public:
  RangeSearchPlanOptimizer() { optimize_function = Optimize; }

  // Attempts to rewrite a LogicalFilter subtree into a RangeSearchScan.
  // Returns true if the rewrite was applied.
  static bool TryOptimize(duckdb::ClientContext& /*context*/,
                          duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
    if (plan->type != duckdb::LogicalOperatorType::LOGICAL_FILTER) {
      return false;
    }
    auto& filter = plan->Cast<duckdb::LogicalFilter>();

    // The direct child must be a serenedb_scan LogicalGet.
    if (filter.children.size() != 1 ||
        filter.children[0]->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
      return false;
    }
    auto& get = filter.children[0]->Cast<duckdb::LogicalGet>();
    if (get.function.name != "serenedb_scan" || !get.bind_data) {
      return false;
    }
    auto& bind_data = get.bind_data->Cast<connector::SereneDBScanBindData>();
    if (!bind_data.table) {
      return false;
    }
    // Only rewrite when the scan source has not already been claimed
    // (e.g., by the text-search pushdown filter or ANNScan optimizer).
    if (!std::holds_alternative<connector::FullTableScan>(
          bind_data.scan_source)) {
      return false;
    }

    // Scan filter expressions for a recognised distance comparison.
    duckdb::idx_t match_idx = duckdb::DConstants::INVALID_INDEX;
    duckdb::BoundFunctionExpression* func_expr_ptr = nullptr;
    float radius = 0.0f;

    for (duckdb::idx_t i = 0; i < filter.expressions.size(); ++i) {
      auto& expr = *filter.expressions[i];
      if (expr.expression_class != duckdb::ExpressionClass::BOUND_COMPARISON) {
        continue;
      }
      auto& cmp = expr.Cast<duckdb::BoundComparisonExpression>();

      // Normalise so that the distance function is always on func_side and the
      // radius constant is always on const_side.
      duckdb::Expression* func_side = nullptr;
      duckdb::Expression* const_side = nullptr;

      switch (cmp.type) {
        case duckdb::ExpressionType::COMPARE_LESSTHAN:
        case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
          // distance_func(...) < radius
          func_side = cmp.left.get();
          const_side = cmp.right.get();
          break;
        case duckdb::ExpressionType::COMPARE_GREATERTHAN:
        case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
          // radius > distance_func(...)
          func_side = cmp.right.get();
          const_side = cmp.left.get();
          break;
        default:
          continue;
      }

      if (func_side->expression_class !=
          duckdb::ExpressionClass::BOUND_FUNCTION) {
        continue;
      }
      auto& func = func_side->Cast<duckdb::BoundFunctionExpression>();
      if (!IsDistanceFunction(func.function.name) || func.children.size() < 2) {
        continue;
      }

      if (const_side->expression_class !=
          duckdb::ExpressionClass::BOUND_CONSTANT) {
        continue;
      }
      auto& const_expr = const_side->Cast<duckdb::BoundConstantExpression>();
      if (const_expr.value.IsNull()) {
        continue;
      }

      switch (const_expr.value.type().id()) {
        case duckdb::LogicalTypeId::FLOAT:
          radius = const_expr.value.GetValue<float>();
          break;
        case duckdb::LogicalTypeId::DOUBLE:
          radius = static_cast<float>(const_expr.value.GetValue<double>());
          break;
        default:
          continue;
      }

      func_expr_ptr = &func;
      match_idx = i;
      break;
    }

    if (match_idx == duckdb::DConstants::INVALID_INDEX) {
      return false;
    }

    // Identify the column argument and the constant vector argument inside the
    // distance function.
    duckdb::Expression* col_arg = nullptr;
    duckdb::BoundConstantExpression* const_arg = nullptr;
    for (auto& child : func_expr_ptr->children) {
      if (child->expression_class == duckdb::ExpressionClass::BOUND_CONSTANT) {
        const_arg = &child->Cast<duckdb::BoundConstantExpression>();
      } else if (child->expression_class ==
                   duckdb::ExpressionClass::BOUND_COLUMN_REF ||
                 child->expression_class ==
                   duckdb::ExpressionClass::BOUND_REF) {
        col_arg = child.get();
      }
    }
    if (!col_arg || !const_arg) {
      return false;
    }

    // Extract float query vector from the constant.
    std::vector<float> query_vector;
    if (!TryExtractQueryVector(const_arg->value, query_vector)) {
      return false;
    }

    // Map the column argument's name to the table's catalog Column::Id.
    const std::string col_name = col_arg->GetName();
    catalog::Column::Id col_id = 0;
    bool col_found = false;
    for (const auto& col : bind_data.table->Columns()) {
      if (col.name == col_name) {
        col_id = col.id;
        col_found = true;
        break;
      }
    }
    if (!col_found) {
      return false;
    }

    // TODO(codeworse): add auto-detect indexes
    const auto table_id = bind_data.table->GetId();
    auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();

    std::shared_ptr<const catalog::InvertedIndex> inverted_index;

    if (bind_data.table_entry) {
      if (const auto* sdb_entry =
            dynamic_cast<const connector::SereneDBTableEntry*>(
              &*bind_data.table_entry)) {
        inverted_index = sdb_entry->GetInvertedIndex();
      }
    }
    if (!inverted_index) {
      return false;
    }

    // Resolve the live shard for this index.
    std::shared_ptr<search::InvertedIndexShard> inverted_index_shard;
    for (auto& shard : snapshot->GetIndexShardsByTable(table_id)) {
      if (shard->GetIndexId() == inverted_index->GetId() &&
          shard->GetType() == catalog::ObjectType::InvertedIndexShard) {
        inverted_index_shard =
          std::dynamic_pointer_cast<search::InvertedIndexShard>(shard);
        break;
      }
    }
    if (!inverted_index_shard) {
      return false;
    }

    // Verify the shard has a live snapshot (index is readable).
    if (!inverted_index_shard->GetInvertedIndexSnapshot()) {
      return false;
    }

    // Build the HNSW field name: big-endian catalog::Column::Id, no mangle.
    std::string field_name(sizeof(col_id), '\0');
    absl::big_endian::Store(field_name.data(), col_id);

    // Rewrite the bind data to RangeSearchScan mode.
    connector::RangeSearchScan rss;
    rss.shard = std::move(inverted_index_shard);
    rss.field_name = std::move(field_name);
    rss.query_vector = std::move(query_vector);
    rss.radius = radius;
    bind_data.scan_source = std::move(rss);

    // Remove the matched distance comparison from the filter. If it was the
    // only predicate, drop the LogicalFilter node entirely (the range scan
    // already returns pre-filtered rows); otherwise keep the filter for the
    // remaining predicates.
    filter.expressions.erase(filter.expressions.begin() + match_idx);
    if (filter.expressions.empty()) {
      plan = std::move(filter.children[0]);
    }
    return true;
  }

  // Recursively walks the plan tree bottom-up, applying TryOptimize.
  static bool OptimizeChildren(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
    bool changed = false;
    for (auto& child : plan->children) {
      changed |= OptimizeChildren(context, child);
    }
    changed |= TryOptimize(context, plan);
    return changed;
  }

  static void Optimize(duckdb::OptimizerExtensionInput& input,
                       duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
    OptimizeChildren(input.context, plan);
  }
};

}  // namespace

void RegisterRangeSearchOptimizer(duckdb::DatabaseInstance& db) {
  duckdb::OptimizerExtension::Register(db.config, RangeSearchPlanOptimizer());
}

}  // namespace sdb::optimizer
