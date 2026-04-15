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

// Optimizer rule: ANN search plan rewriter
//
// Detected pattern (logical plan):
//   LogicalTopN  (ORDER BY <bound_col_ref> ASC, LIMIT k)
//   - LogicalProjection  (expr[i] = distance_func(col, const_vector), ...)
//      - LogicalGet  (function = "serenedb_scan")
//
// Transformation (if an InvertedIndex exists on the distance column):
//   1. Mark the LogicalGet bind data with ANNScan{shard, field, query, k}.
//   2. Remove the LogicalTopN -- the HNSW scan returns rows pre-sorted by
//      distance and already limited to top_k entries.
//
// Requirement: LIMIT (top_k > 0) is mandatory; without it the pattern is
// skipped because HNSW needs a finite k to scan.

#include "connector/optimizer/ann_search_plan.h"

#include <absl/base/internal/endian.h>

#include <duckdb/main/config.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_top_n.hpp>

#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/functions/vector.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/index_shard.h"

namespace sdb::optimizer {
namespace {

// DuckDB function names that compute L2 (Euclidean) distance between two
// array/list arguments.  Both built-ins are matched so the optimizer works
// regardless of whether the column type is LIST(FLOAT) or FLOAT[N].
static bool IsDistanceFunction(std::string_view name) {
  return name == connector::kL2Distance;
}

// Extract a float query vector from a constant LIST or ARRAY Value.
// Returns false if the value is not a flat float/double collection.
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

class ANNSearchPlanOptimizer : public duckdb::OptimizerExtension {
 public:
  ANNSearchPlanOptimizer() { optimize_function = Optimize; }

  // Attempts to rewrite a single LogicalTopN subtree into an ANNScan.
  // Returns true if the rewrite was applied.
  static bool TryOptimize(duckdb::ClientContext& /*context*/,
                          duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
    if (plan->type != duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
      return false;
    }
    auto& top_n = plan->Cast<duckdb::LogicalTopN>();

    // LIMIT is mandatory: HNSW needs a finite k.
    if (top_n.limit == 0) {
      return false;
    }

    // Single ORDER BY expression, ascending.
    if (top_n.orders.size() != 1 ||
        top_n.orders[0].type != duckdb::OrderType::ASCENDING) {
      return false;
    }

    // The ORDER BY expr must reference a column in the child projection.
    if (top_n.orders[0].expression->type !=
        duckdb::ExpressionType::BOUND_COLUMN_REF) {
      return false;
    }
    auto& order_col_ref =
      top_n.orders[0].expression->Cast<duckdb::BoundColumnRefExpression>();

    // Child must be a single projection.
    if (top_n.children.size() != 1 ||
        top_n.children[0]->type !=
          duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
      return false;
    }
    auto& projection = top_n.children[0]->Cast<duckdb::LogicalProjection>();

    // Find the expression the ORDER BY references in the projection.
    const auto proj_col_idx = order_col_ref.binding.column_index;
    if (proj_col_idx >= projection.expressions.size()) {
      return false;
    }
    auto& dist_expr = *projection.expressions[proj_col_idx];

    // That expression must be a distance function call.
    if (dist_expr.expression_class != duckdb::ExpressionClass::BOUND_FUNCTION) {
      return false;
    }
    auto& func_expr = dist_expr.Cast<duckdb::BoundFunctionExpression>();
    if (!IsDistanceFunction(func_expr.function.name)) {
      return false;
    }
    if (func_expr.children.size() < 2) {
      return false;
    }

    // The projection must sit directly above a serenedb_scan LogicalGet.
    if (projection.children.size() != 1 ||
        projection.children[0]->type !=
          duckdb::LogicalOperatorType::LOGICAL_GET) {
      return false;
    }
    auto& get = projection.children[0]->Cast<duckdb::LogicalGet>();
    if (get.function.name != "serenedb_scan" || !get.bind_data) {
      return false;
    }
    auto& bind_data = get.bind_data->Cast<connector::SereneDBScanBindData>();
    if (!bind_data.table) {
      return false;
    }

    // Identify the column argument and the constant vector argument.
    duckdb::Expression* col_arg = nullptr;
    duckdb::BoundConstantExpression* const_arg = nullptr;
    for (auto& child : func_expr.children) {
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

    // Extract the float query vector from the constant.
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

    // Resolve the InvertedIndex to use.
    //
    // Priority 1: explicit index from "FROM i ON t" -- the table entry was
    // created from an index name and already has the right index attached.
    // Priority 2(TODO): auto-detect -- scan the catalog for any InvertedIndex
    // on the table that covers the distance column (implicit index selection).
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
    // TODO(codeworse): auto-select the index

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

    // Rewrite the bind data to ANNScan mode.
    connector::ANNScan ann;
    ann.shard = std::move(inverted_index_shard);
    ann.field_name = std::move(field_name);
    ann.query_vector = std::move(query_vector);
    ann.top_k = static_cast<size_t>(top_n.limit);
    bind_data.scan_source = std::move(ann);

    // Remove the TopN: the HNSW scan returns rows pre-sorted and bounded.
    plan = std::move(top_n.children[0]);
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

void RegisterANNSearchOptimizer(duckdb::DatabaseInstance& db) {
  duckdb::OptimizerExtension::Register(db.config, ANNSearchPlanOptimizer());
}

}  // namespace sdb::optimizer
