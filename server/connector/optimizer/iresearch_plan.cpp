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

#include "connector/optimizer/iresearch_plan.h"

#include <absl/base/internal/endian.h>

#include <duckdb/function/aggregate/distributive_functions.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/optimizer/remove_unused_columns.hpp>
#include <duckdb/planner/expression/bound_aggregate_expression.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression/bound_window_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_join.hpp>
#include <duckdb/planner/operator/logical_limit.hpp>
#include <duckdb/planner/operator/logical_order.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_top_n.hpp>

#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "connector/duckdb_index_scan_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/functions/search.h"
#include "connector/functions/vector.h"
#include "connector/optimizer/flatten_projection_ids.h"
#include "connector/search_filter_builder.hpp"
#include "connector/search_filter_printer.hpp"
#include "search/inverted_index_shard.h"
#include "storage_engine/index_shard.h"

namespace sdb::optimizer {
namespace {

duckdb::unique_ptr<duckdb::Expression> CombineFilterExpressions(
  std::vector<duckdb::unique_ptr<duckdb::Expression>> exprs) {
  if (exprs.empty()) {
    return nullptr;
  }
  if (exprs.size() == 1) {
    return std::move(exprs.front());
  }
  auto conj = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
    duckdb::ExpressionType::CONJUNCTION_AND);
  conj->children.reserve(exprs.size());
  for (auto& e : exprs) {
    conj->children.push_back(std::move(e));
  }
  return conj;
}

bool IsMutationOp(duckdb::LogicalOperatorType type) {
  switch (type) {
    case duckdb::LogicalOperatorType::LOGICAL_DELETE:
    case duckdb::LogicalOperatorType::LOGICAL_UPDATE:
    case duckdb::LogicalOperatorType::LOGICAL_MERGE_INTO:
      return true;
    default:
      return false;
  }
}

struct ResolvedIresearch {
  std::shared_ptr<const catalog::InvertedIndex> index;
  std::shared_ptr<search::InvertedIndexShard> shard;
};

// Index source: explicit (FROM idx_name) or auto-detect first InvertedIndex
// on a table-backed scan. Returns nullopt if none or shard not yet readable.
std::optional<ResolvedIresearch> ResolveIresearch(
  const connector::SereneDBScanBindData& bind_data,
  const catalog::Snapshot& snapshot) {
  ResolvedIresearch out;
  out.index = bind_data.inverted_index;

  if (!out.index && !bind_data.IsViewBacked()) {
    const auto table_id = bind_data.RelationId();
    for (auto& obj : snapshot.GetIndexesByRelation(table_id)) {
      if (obj->GetType() == catalog::ObjectType::InvertedIndex) {
        out.index = basics::downCast<const catalog::InvertedIndex>(obj);
        break;
      }
    }
  }
  if (!out.index) {
    return std::nullopt;
  }

  for (auto& shard :
       snapshot.GetIndexShardsByRelation(out.index->GetRelationId())) {
    if (shard->GetIndexId() == out.index->GetId() &&
        shard->GetType() == catalog::ObjectType::InvertedIndexShard) {
      out.shard =
        basics::downCast<search::InvertedIndexShard>(std::move(shard));
      break;
    }
  }
  if (!out.shard || !out.shard->GetInvertedIndexSnapshot()) {
    return std::nullopt;
  }
  return out;
}

catalog::Column::Id ColumnIdByName(
  const connector::SereneDBScanBindData& bind_data, std::string_view name) {
  return bind_data.ColumnIdByName(name);
}

// 8 big-endian bytes, no Mangle suffix.
std::string MakeHnswFieldName(catalog::Column::Id col_id) {
  std::string name(sizeof(col_id), '\0');
  absl::big_endian::Store(name.data(), col_id);
  return name;
}

struct ExpectedHNSW {
  irs::HNSWMetric metric;
  duckdb::OrderType order;
  bool is_norm;
};

// Returns the expected HNSW metric / sort order / norm-arity for `func`
// when it's a vector-ANN-shaped call we can rewrite into an HNSW scan,
// or nullopt otherwise.
//
// Beyond the name->metric mapping this also enforces:
//   - arity (norms take 1 child, distances take 2);
//   - the geo guard: `<->` / `<+>` and friends are also registered with
//     geo overloads (JSON / GEOMETRY) in search.cpp, so an ARRAY column
//     on children[0] is what proves we're on the vector overload here.
// Keeping these inside the lookup means the call sites (TryAnnTopk /
// TryAnnRange) can't drift apart on what makes a call "vector ANN".
std::optional<ExpectedHNSW> ExpectedHNSWForFunction(
  const duckdb::BoundFunctionExpression& func) {
  const auto& name = func.function.name;
  ExpectedHNSW result;
  if (name == connector::kL2Distance || name == connector::kL2DistanceOp ||
      name == connector::kL2SqrDistance) {
    result = {irs::HNSWMetric::L2Sqr, duckdb::OrderType::ASCENDING, false};
  } else if (name == connector::kL1Distance ||
             name == connector::kL1DistanceOp) {
    result = {irs::HNSWMetric::L1, duckdb::OrderType::ASCENDING, false};
  } else if (name == connector::kCosineDistance ||
             name == connector::kCosineDistanceOp) {
    result = {irs::HNSWMetric::Cosine, duckdb::OrderType::ASCENDING, false};
  } else if (name == connector::kCosineSimilarity) {
    result = {irs::HNSWMetric::Cosine, duckdb::OrderType::DESCENDING, false};
  } else if (name == connector::kIP) {
    result = {irs::HNSWMetric::NegativeIP, duckdb::OrderType::DESCENDING,
              false};
  } else if (name == connector::kNegativeIP ||
             name == connector::kNegativeIPDistanceOp) {
    result = {irs::HNSWMetric::NegativeIP, duckdb::OrderType::ASCENDING, false};
  } else if (name == connector::kL2Norm) {
    result = {irs::HNSWMetric::L2Sqr, duckdb::OrderType::ASCENDING, true};
  } else if (name == connector::kL1Norm) {
    result = {irs::HNSWMetric::L1, duckdb::OrderType::ASCENDING, true};
  } else {
    return std::nullopt;
  }
  if (result.is_norm ? func.children.size() != 1 : func.children.size() < 2) {
    return std::nullopt;
  }
  const auto& arg_type = func.children[0]->return_type;
  duckdb::LogicalTypeId element_id;
  if (arg_type.id() == duckdb::LogicalTypeId::ARRAY) {
    element_id = duckdb::ArrayType::GetChildType(arg_type).id();
  } else if (arg_type.id() == duckdb::LogicalTypeId::LIST) {
    element_id = duckdb::ListType::GetChildType(arg_type).id();
  } else {
    return std::nullopt;
  }
  if (element_id != duckdb::LogicalTypeId::FLOAT &&
      element_id != duckdb::LogicalTypeId::DOUBLE) {
    return std::nullopt;
  }

  return result;
}

// Pull a flat float vector from a constant ARRAY Value. Rejects mixed /
// non-float / null elements.
bool TryExtractQueryVector(const duckdb::Value& val, std::vector<float>& out) {
  using duckdb::LogicalTypeId;
  if (val.type().id() != LogicalTypeId::ARRAY) {
    return false;
  }
  const auto& children = duckdb::ArrayValue::GetChildren(val);
  if (children.empty()) {
    return false;
  }
  out.reserve(children.size());
  for (const auto& child : children) {
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
  return true;
}

// Identify column + constant inside a distance call: distance(col, vec)
// or distance(vec, col). Returns nullptr on shape mismatch.
struct DistanceArgs {
  duckdb::Expression* col_arg = nullptr;
  duckdb::BoundConstantExpression* const_arg = nullptr;
};
DistanceArgs ExtractDistanceArgs(duckdb::BoundFunctionExpression& func_expr) {
  DistanceArgs out;
  for (auto& child : func_expr.children) {
    if (child->expression_class == duckdb::ExpressionClass::BOUND_CONSTANT) {
      out.const_arg = &child->Cast<duckdb::BoundConstantExpression>();
    } else if (child->expression_class ==
                 duckdb::ExpressionClass::BOUND_COLUMN_REF ||
               child->expression_class == duckdb::ExpressionClass::BOUND_REF) {
      out.col_arg = child.get();
    }
  }
  return out;
}

bool RewriteFilterColumnRefs(
  duckdb::Expression& expr, const duckdb::LogicalGet& get,
  const connector::SereneDBScanBindData& bind_data,
  std::vector<catalog::Column::Id>& referenced_col_ids) {
  bool ok = true;
  duckdb::ExpressionIterator::EnumerateChildren(
    expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      if (!ok) {
        return;
      }
      if (child->expression_class ==
          duckdb::ExpressionClass::BOUND_COLUMN_REF) {
        auto& ref = child->Cast<duckdb::BoundColumnRefExpression>();
        if (ref.binding.table_index != get.table_index) {
          ok = false;
          return;
        }
        const auto col_idx = ref.binding.column_index;
        if (col_idx >= get.GetColumnIds().size()) {
          ok = false;
          return;
        }
        const auto& ci = get.GetColumnIds()[col_idx];
        if (!ci.HasPrimaryIndex()) {
          ok = false;
          return;
        }
        const auto phys = ci.GetPrimaryIndex();
        if (phys >= bind_data.column_ids.size()) {
          ok = false;
          return;
        }
        const auto cat_id = bind_data.column_ids[phys];
        auto it = absl::c_find(referenced_col_ids, cat_id);
        size_t slot = static_cast<size_t>(it - referenced_col_ids.begin());
        if (it == referenced_col_ids.end()) {
          referenced_col_ids.push_back(cat_id);
        }
        child = duckdb::make_uniq<duckdb::BoundReferenceExpression>(
          ref.return_type, slot);
        return;
      }
      if (!RewriteFilterColumnRefs(*child, get, bind_data,
                                   referenced_col_ids)) {
        ok = false;
      }
    });
  return ok;
}

bool TryAnnTopk(duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  if (plan->type != duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
    return false;
  }
  auto& top_n = plan->Cast<duckdb::LogicalTopN>();
  if (top_n.limit == 0) {
    return false;
  }
  if (top_n.orders.size() != 1 ||
      top_n.orders[0].type == duckdb::OrderType::INVALID) {
    return false;
  }
  if (top_n.orders[0].expression->type !=
      duckdb::ExpressionType::BOUND_COLUMN_REF) {
    return false;
  }
  const auto order_type =
    top_n.orders[0].type == duckdb::OrderType::ORDER_DEFAULT
      ? duckdb::OrderType::ASCENDING
      : top_n.orders[0].type;
  auto& order_col_ref =
    top_n.orders[0].expression->Cast<duckdb::BoundColumnRefExpression>();

  if (top_n.children.size() != 1 ||
      top_n.children[0]->type !=
        duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    return false;
  }
  auto& projection = top_n.children[0]->Cast<duckdb::LogicalProjection>();

  const auto proj_col_idx = order_col_ref.binding.column_index;
  if (proj_col_idx >= projection.expressions.size()) {
    return false;
  }
  auto& dist_expr = *projection.expressions[proj_col_idx];
  if (dist_expr.expression_class != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return false;
  }
  auto& func_expr = dist_expr.Cast<duckdb::BoundFunctionExpression>();
  auto expected_func = ExpectedHNSWForFunction(func_expr);
  if (!expected_func) {
    return false;
  }
  const bool is_norm = expected_func->is_norm;

  if (projection.children.size() != 1) {
    return false;
  }
  std::vector<duckdb::LogicalFilter*> residual_filters;
  duckdb::LogicalOperator* child = projection.children[0].get();
  while (child->type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
    auto& f = child->Cast<duckdb::LogicalFilter>();
    if (f.children.size() != 1) {
      return false;
    }
    residual_filters.push_back(&f);
    child = f.children[0].get();
  }
  if (child->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
    return false;
  }
  auto& get = child->Cast<duckdb::LogicalGet>();
  if (!connector::IsSereneDBScan(get)) {
    return false;
  }
  auto& bind_data = get.bind_data->Cast<connector::SereneDBScanBindData>();
  if (bind_data.scan_source->Kind() != connector::ScanSourceKind::FullTable) {
    return false;
  }

  duckdb::Expression* col_arg = nullptr;
  std::vector<float> query_vector;
  if (is_norm) {
    auto* child = func_expr.children[0].get();
    if (child->expression_class != duckdb::ExpressionClass::BOUND_COLUMN_REF &&
        child->expression_class != duckdb::ExpressionClass::BOUND_REF) {
      return false;
    }
    col_arg = child;
  } else {
    auto args = ExtractDistanceArgs(func_expr);
    if (!args.col_arg || !args.const_arg) {
      return false;
    }
    if (!TryExtractQueryVector(args.const_arg->value, query_vector)) {
      return false;
    }
    col_arg = args.col_arg;
  }
  auto col_id = ColumnIdByName(bind_data, col_arg->GetName());
  if (col_id == std::numeric_limits<catalog::Column::Id>::max()) {
    return false;
  }

  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  auto resolved = ResolveIresearch(bind_data, *snapshot);
  if (!resolved) {
    return false;
  }

  auto hnsw_info = resolved->index->GetColumnHNSWInfo(col_id);
  if (!hnsw_info || hnsw_info->metric != expected_func->metric) {
    return false;
  }
  if (is_norm) {
    query_vector.assign(hnsw_info->d, 0.0f);
  } else if (static_cast<size_t>(hnsw_info->d) != query_vector.size()) {
    return false;
  }
  if (order_type != expected_func->order) {
    return false;
  }

  auto ann = std::make_unique<connector::ANNScan>();
  ann->index_id = resolved->index->GetId();
  ann->field_name = MakeHnswFieldName(col_id);
  ann->query_vector = std::move(query_vector);
  ann->top_k = static_cast<size_t>(top_n.limit);

  bool pushdown_filter = true;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> rewritten_exprs;
  std::vector<catalog::Column::Id> filter_col_ids;
  for (auto* f : residual_filters) {
    for (auto& e : f->expressions) {
      auto copy = e->Copy();
      if (!RewriteFilterColumnRefs(*copy, get, bind_data, filter_col_ids)) {
        pushdown_filter = false;
        break;
      }
      rewritten_exprs.push_back(std::move(copy));
    }
    if (!pushdown_filter) {
      break;
    }
  }
  if (pushdown_filter) {
    ann->filter_expression =
      CombineFilterExpressions(std::move(rewritten_exprs));
    ann->filter_column_ids = std::move(filter_col_ids);
  }

  bind_data.scan_source = std::move(ann);
  get.function = connector::CreateIResearchANNScanFunction();

  if (pushdown_filter && !residual_filters.empty()) {
    projection.children[0] = std::move(residual_filters.back()->children[0]);
  }

  // HNSW returns rows pre-sorted and bounded; drop the TopN.
  plan = std::move(top_n.children[0]);
  return true;
}

bool TryAnnRange(duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  if (plan->type != duckdb::LogicalOperatorType::LOGICAL_FILTER) {
    return false;
  }
  auto& filter = plan->Cast<duckdb::LogicalFilter>();
  if (filter.children.size() != 1) {
    return false;
  }
  duckdb::LogicalOperator* get_op = filter.children[0].get();
  while (get_op->type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
    if (get_op->children.size() != 1) {
      return false;
    }
    get_op = get_op->children[0].get();
  }
  if (get_op->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
    return false;
  }
  auto& get = get_op->Cast<duckdb::LogicalGet>();
  if (!connector::IsSereneDBScan(get)) {
    return false;
  }
  auto& bind_data = get.bind_data->Cast<connector::SereneDBScanBindData>();
  if (bind_data.scan_source->Kind() != connector::ScanSourceKind::FullTable) {
    return false;
  }

  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  auto resolved = ResolveIresearch(bind_data, *snapshot);
  if (!resolved) {
    return false;
  }

  duckdb::idx_t match_idx = duckdb::DConstants::INVALID_INDEX;
  float radius = 0.0f;
  bool radius_needs_square = false;
  std::vector<float> query_vector;
  catalog::Column::Id col_id = std::numeric_limits<catalog::Column::Id>::max();

  for (duckdb::idx_t i = 0; i < filter.expressions.size(); ++i) {
    auto& expr = *filter.expressions[i];
    if (expr.expression_class != duckdb::ExpressionClass::BOUND_COMPARISON) {
      continue;
    }
    auto& cmp = expr.Cast<duckdb::BoundComparisonExpression>();
    duckdb::Expression* func_side = nullptr;
    duckdb::Expression* const_side = nullptr;
    switch (cmp.type) {
      case duckdb::ExpressionType::COMPARE_LESSTHAN:
      case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
        func_side = cmp.left.get();
        const_side = cmp.right.get();
        break;
      case duckdb::ExpressionType::COMPARE_GREATERTHAN:
      case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
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
    auto expected_current = ExpectedHNSWForFunction(func);
    if (!expected_current) {
      continue;
    }
    const bool is_norm = expected_current->is_norm;
    if (expected_current->order != duckdb::OrderType::ASCENDING) {
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
    float candidate_radius = 0.0f;
    switch (const_expr.value.type().id()) {
      case duckdb::LogicalTypeId::FLOAT:
        candidate_radius = const_expr.value.GetValue<float>();
        break;
      case duckdb::LogicalTypeId::DOUBLE:
        candidate_radius =
          static_cast<float>(const_expr.value.GetValue<double>());
        break;
      default:
        continue;
    }
    duckdb::Expression* col_expr = nullptr;
    std::vector<float> candidate_vector;
    if (is_norm) {
      auto* child = func.children[0].get();
      if (child->expression_class !=
            duckdb::ExpressionClass::BOUND_COLUMN_REF &&
          child->expression_class != duckdb::ExpressionClass::BOUND_REF) {
        continue;
      }
      col_expr = child;
    } else {
      auto args = ExtractDistanceArgs(func);
      if (!args.col_arg || !args.const_arg) {
        continue;
      }
      if (!TryExtractQueryVector(args.const_arg->value, candidate_vector)) {
        continue;
      }
      col_expr = args.col_arg;
    }
    auto candidate_col_id = ColumnIdByName(bind_data, col_expr->GetName());
    if (candidate_col_id == std::numeric_limits<catalog::Column::Id>::max()) {
      continue;
    }
    auto hnsw_info = resolved->index->GetColumnHNSWInfo(candidate_col_id);
    if (!hnsw_info || hnsw_info->metric != expected_current->metric) {
      continue;
    }
    if (is_norm) {
      candidate_vector.assign(hnsw_info->d, 0.0f);
    } else if (static_cast<size_t>(hnsw_info->d) != candidate_vector.size()) {
      continue;
    }

    radius = candidate_radius;
    // The iresearch index stores L2-squared distances. When the user wrote
    // l2_distance / `<->` / l2_norm (un-squared L2), the radius must be
    // squared before being compared against stored values. l2_sqr_distance
    // already speaks in squared units. Other metrics (L1, cosine, IP) are
    // not squared.
    radius_needs_square = func.function.name == connector::kL2Distance ||
                          func.function.name == connector::kL2DistanceOp ||
                          func.function.name == connector::kL2Norm;
    query_vector = std::move(candidate_vector);
    col_id = candidate_col_id;
    match_idx = i;
    break;
  }

  if (match_idx == duckdb::DConstants::INVALID_INDEX) {
    return false;
  }

  auto rss = std::make_unique<connector::RangeSearchScan>();
  rss->index_id = resolved->index->GetId();
  rss->field_name = MakeHnswFieldName(col_id);
  rss->query_vector = std::move(query_vector);
  rss->radius = radius;
  rss->effective_radius = radius_needs_square ? radius * radius : radius;

  filter.expressions.erase(filter.expressions.begin() + match_idx);

  bool pushdown_filter = true;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> rewritten_exprs;
  std::vector<catalog::Column::Id> filter_col_ids;
  for (auto& e : filter.expressions) {
    auto copy = e->Copy();
    if (!RewriteFilterColumnRefs(*copy, get, bind_data, filter_col_ids)) {
      pushdown_filter = false;
      break;
    }
    rewritten_exprs.push_back(std::move(copy));
  }
  if (pushdown_filter) {
    rss->filter_expression =
      CombineFilterExpressions(std::move(rewritten_exprs));
    rss->filter_column_ids = std::move(filter_col_ids);
    filter.expressions.clear();
  }

  bind_data.scan_source = std::move(rss);
  get.function = connector::CreateIResearchANNRangeScanFunction();

  if (filter.expressions.empty()) {
    plan = std::move(filter.children[0]);
  }
  return true;
}

struct SearchColumnContext {
  duckdb::TableIndex table_index;
  std::span<const catalog::Column::Id> projected_column_ids;
  containers::FlatHashMap<catalog::Column::Id, duckdb::LogicalType>
    column_type_by_id;
  containers::FlatHashSet<catalog::Column::Id> indexed_column_ids;
  std::function<catalog::ColumnTokenizer(catalog::Column::Id)>
    tokenizer_provider;
  // Optional JSON-path tokenizer lookup: returns the per-path tokenizer
  // resolved against the catalog, or nullopt if the column is not
  // indexed at `path`.
  std::function<std::optional<catalog::ColumnTokenizer>(
    catalog::Column::Id, std::span<const std::string>)>
    json_path_tokenizer_provider;
};

connector::ColumnGetter MakeColumnGetter(SearchColumnContext& ctx) {
  return [&ctx](const duckdb::BoundColumnRefExpression& ref)
           -> std::optional<connector::SearchColumnInfo> {
    if (ref.binding.table_index != ctx.table_index) {
      return std::nullopt;
    }
    if (ref.binding.column_index >= ctx.projected_column_ids.size()) {
      return std::nullopt;
    }
    const auto col_id = ctx.projected_column_ids[ref.binding.column_index];
    if (col_id == std::numeric_limits<catalog::Column::Id>::max()) {
      return std::nullopt;
    }
    if (!ctx.indexed_column_ids.contains(col_id)) {
      return std::nullopt;
    }
    auto type_it = ctx.column_type_by_id.find(col_id);
    if (type_it == ctx.column_type_by_id.end()) {
      return std::nullopt;
    }
    connector::SearchColumnInfo info;
    info.column_id = col_id;
    info.logical_type = type_it->second;
    info.tokenizer = ctx.tokenizer_provider(col_id);
    return info;
  };
}

connector::JsonPathGetter MakeJsonPathGetter(SearchColumnContext& ctx) {
  return [&ctx](const duckdb::BoundColumnRefExpression& ref,
                std::span<const std::string> path)
           -> std::optional<connector::SearchColumnInfo> {
    if (ref.binding.table_index != ctx.table_index) {
      return std::nullopt;
    }
    if (ref.binding.column_index >= ctx.projected_column_ids.size()) {
      return std::nullopt;
    }
    const auto col_id = ctx.projected_column_ids[ref.binding.column_index];
    if (col_id == std::numeric_limits<catalog::Column::Id>::max()) {
      return std::nullopt;
    }
    if (!ctx.json_path_tokenizer_provider) {
      return std::nullopt;
    }
    auto tokenizer = ctx.json_path_tokenizer_provider(col_id, path);
    if (!tokenizer) {
      return std::nullopt;
    }
    connector::SearchColumnInfo info;
    info.column_id = col_id;
    // The default leaf type is VARCHAR (the string-leaf path); the filter
    // builder may override this when the user wraps the path in an
    // explicit cast like `(content->>'val')::int`, so numeric/bool/null
    // queries hit the right pre-emitted per-type field.
    info.logical_type = duckdb::LogicalType::VARCHAR;
    info.tokenizer = *std::move(tokenizer);
    info.json_path.assign(path.begin(), path.end());
    return info;
  };
}

bool TrySearchFilter(duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
                     const connector::SearchFilterOptions& options) {
  if (plan->type != duckdb::LogicalOperatorType::LOGICAL_FILTER) {
    return false;
  }
  auto& filter = plan->Cast<duckdb::LogicalFilter>();
  if (filter.children.size() != 1) {
    return false;
  }
  // Descend through any LogicalFilter/LogicalProjection inserted by
  // DuckDB's other optimisers.
  duckdb::LogicalOperator* get_op = filter.children[0].get();
  while (get_op->type == duckdb::LogicalOperatorType::LOGICAL_FILTER ||
         get_op->type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    if (get_op->children.size() != 1) {
      return false;
    }
    get_op = get_op->children[0].get();
  }
  if (get_op->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
    return false;
  }
  auto& get = get_op->Cast<duckdb::LogicalGet>();
  if (!connector::IsSereneDBScan(get)) {
    return false;
  }
  auto& bind_data = get.bind_data->Cast<connector::SereneDBScanBindData>();
  if (bind_data.scan_source->Kind() != connector::ScanSourceKind::FullTable) {
    return false;
  }
  if (filter.expressions.empty()) {
    return false;
  }

  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  auto resolved = ResolveIresearch(bind_data, *snapshot);
  if (!resolved) {
    return false;
  }

  // ctx.projected_column_ids spans projected_ids -- it must outlive ctx.
  constexpr auto kInvalidId = std::numeric_limits<catalog::Column::Id>::max();
  std::vector<catalog::Column::Id> projected_ids;
  projected_ids.reserve(get.GetColumnIds().size());
  for (const auto& ci : get.GetColumnIds()) {
    if (!ci.HasPrimaryIndex()) {
      projected_ids.push_back(kInvalidId);
      continue;
    }
    const auto phys = ci.GetPrimaryIndex();
    projected_ids.push_back(phys < bind_data.column_ids.size()
                              ? bind_data.column_ids[phys]
                              : kInvalidId);
  }
  SearchColumnContext ctx;
  ctx.table_index = get.table_index;
  ctx.projected_column_ids = projected_ids;

  bind_data.IterateColumns(
    [&](catalog::Column::Id id, const duckdb::LogicalType& type) {
      ctx.column_type_by_id.emplace(id, type);
    });
  for (auto col_id : resolved->index->GetColumnIds()) {
    ctx.indexed_column_ids.insert(col_id);
  }
  auto index_ptr = resolved->index;
  auto snapshot_for_analyzer = snapshot;
  ctx.tokenizer_provider = [index_ptr,
                            snapshot_for_analyzer](catalog::Column::Id col_id) {
    return index_ptr->GetColumnTokenizer(snapshot_for_analyzer, col_id);
  };
  ctx.json_path_tokenizer_provider = [index_ptr, snapshot_for_analyzer](
                                       catalog::Column::Id col_id,
                                       std::span<const std::string> path) {
    return index_ptr->GetJsonPathTokenizer(snapshot_for_analyzer, col_id, path);
  };
  auto getter = MakeColumnGetter(ctx);
  auto json_getter = MakeJsonPathGetter(ctx);

  // Try each expression individually -- non-iresearch predicates stay on the
  // LogicalFilter.
  auto root = std::make_shared<irs::And>();
  std::vector<size_t> claimed_indices;
  for (size_t i = 0; i < filter.expressions.size(); ++i) {
    const auto before = root->size();
    std::span<const duckdb::unique_ptr<duckdb::Expression>> single{
      &filter.expressions[i], 1};
    auto result =
      connector::MakeSearchFilter(*root, single, getter, options, json_getter);
    if (result.ok() && root->size() > before) {
      claimed_indices.push_back(i);
    } else {
      while (root->size() > before) {
        root->PopBack();
      }
    }
  }
  if (claimed_indices.empty()) {
    return false;
  }
  // DuckDB pushed simple comparisons into get.table_filters before we ran.
  // SearchScan materialises pre-filter rows, so we MUST re-claim them
  // here -- otherwise the executor has nothing left to filter and queries
  // like `WHERE PHRASE(...) AND id <= 3` silently return unfiltered hits.
  std::vector<duckdb::ProjectionIndex> pushed_to_remove;
  for (auto entry : get.table_filters) {
    auto proj_idx = entry.GetIndex();
    auto idx = proj_idx.GetIndex();
    if (idx >= get.GetColumnIds().size()) {
      continue;
    }
    auto& col_id_ref = get.GetColumnIds()[idx];
    if (!col_id_ref.HasPrimaryIndex()) {
      continue;
    }
    auto phys = col_id_ref.GetPrimaryIndex();
    if (phys >= bind_data.column_ids.size() ||
        phys >= bind_data.column_types.size()) {
      continue;
    }
    auto col_ref = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      "", bind_data.column_types[phys],
      duckdb::ColumnBinding(get.table_index, proj_idx));
    auto pushed_expr = entry.Filter().ToExpression(*col_ref);
    if (!pushed_expr) {
      continue;
    }
    const auto before = root->size();
    duckdb::unique_ptr<duckdb::Expression> wrapper{std::move(pushed_expr)};
    std::span<const duckdb::unique_ptr<duckdb::Expression>> single{&wrapper, 1};
    auto result = connector::MakeSearchFilter(*root, single, getter, options);
    if (result.ok() && root->size() > before) {
      pushed_to_remove.push_back(proj_idx);
    } else {
      while (root->size() > before) {
        root->PopBack();
      }
    }
  }
  for (auto proj_idx : pushed_to_remove) {
    get.table_filters.RemoveFilterByColumnIndex(proj_idx);
  }

  // Capture the demangled filter summary BEFORE preparing (prepare
  // consumes the tree into an opaque Query).
  auto col_name_lookup =
    [&bind_data](catalog::Column::Id col_id) -> std::string_view {
    static thread_local std::string fallback;
    auto name = bind_data.ColumnNameById(col_id);
    if (!name.empty()) {
      return name;
    }
    fallback = absl::StrCat("col", col_id);
    return fallback;
  };
  std::string filter_summary = irs::ToStringDemangled(*root, col_name_lookup);

  auto search = std::make_unique<connector::SearchScan>();
  search->snapshot = resolved->shard->GetInvertedIndexSnapshot();
  search->stored_filter = root;
  // `Query` is built lazily in SearchFullScanInitGlobal so prepare runs
  // exactly once per execution, with the scorer if one ends up attached.
  search->filter_summary = std::move(filter_summary);
  bind_data.scan_source = std::move(search);
  get.function = connector::CreateIResearchScanFunction();

  // Drop claimed expressions from the filter. If everything was claimed,
  // lift the LogicalGet up to replace the LogicalFilter entirely.
  for (auto it = claimed_indices.rbegin(); it != claimed_indices.rend(); ++it) {
    filter.expressions.erase(filter.expressions.begin() +
                             static_cast<std::ptrdiff_t>(*it));
  }
  if (filter.expressions.empty()) {
    plan = std::move(filter.children[0]);
  }
  return true;
}

struct FoundScan {
  duckdb::LogicalGet* get;
  connector::SereneDBScanBindData* bind_data;
  connector::SearchScan* search_scan;
};
std::optional<FoundScan> FindSearchScanChild(duckdb::LogicalOperator& op) {
  if (op.children.size() != 1) {
    return std::nullopt;
  }
  auto& child = *op.children[0];
  if (child.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    auto& get = child.Cast<duckdb::LogicalGet>();
    if (!connector::IsSereneDBScan(get)) {
      return std::nullopt;
    }
    auto& bind_data = get.bind_data->Cast<connector::SereneDBScanBindData>();
    if (bind_data.scan_source->Kind() != connector::ScanSourceKind::Search) {
      return std::nullopt;
    }
    auto* ss = &bind_data.scan_source->Cast<connector::SearchScan>();
    return FoundScan{&get, &bind_data, ss};
  }
  if (child.type == duckdb::LogicalOperatorType::LOGICAL_FILTER ||
      child.type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION ||
      child.type == duckdb::LogicalOperatorType::LOGICAL_LIMIT ||
      child.type == duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
    // Allow the search-scan chain to span any composition of
    // projection / filter / limit / topn nodes between the rule's
    // current node and the LogicalGet at the bottom. The score /
    // offsets / score_top_k attachments below all rely on this lookup
    // to find the right scan to mutate.
    return FindSearchScanChild(child);
  }
  return std::nullopt;
}

// Walk ALL children recursively to find a SearchScan LogicalGet whose
// table_index matches `target`. Unlike FindSearchScanChild, this traverses
// multi-child nodes (cross joins, etc.) for multi-index query support.
std::optional<FoundScan> FindSearchScanByTableIndex(duckdb::LogicalOperator& op,
                                                    duckdb::TableIndex target) {
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    auto& get = op.Cast<duckdb::LogicalGet>();
    if (get.table_index != target) {
      return std::nullopt;
    }
    if (!connector::IsSereneDBScan(get)) {
      return std::nullopt;
    }
    auto& bind_data = get.bind_data->Cast<connector::SereneDBScanBindData>();
    if (bind_data.scan_source->Kind() != connector::ScanSourceKind::Search) {
      return std::nullopt;
    }
    auto* ss = &bind_data.scan_source->Cast<connector::SearchScan>();
    return FoundScan{&get, &bind_data, ss};
  }
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    auto& proj = op.Cast<duckdb::LogicalProjection>();
    if (proj.table_index == target) {
      return FindSearchScanChild(op);
    }
  }
  for (auto& child : op.children) {
    auto result = FindSearchScanByTableIndex(*child, target);
    if (result) {
      return result;
    }
  }
  return std::nullopt;
}

duckdb::LogicalProjection* FindProjectionByTableIndex(
  duckdb::LogicalOperator& op, duckdb::TableIndex target) {
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    auto& proj = op.Cast<duckdb::LogicalProjection>();
    if (proj.table_index == target) {
      return &proj;
    }
  }
  for (auto& child : op.children) {
    if (auto* result = FindProjectionByTableIndex(*child, target)) {
      return result;
    }
  }
  return nullptr;
}

bool BindingIsScoreColumn(duckdb::LogicalOperator& op,
                          duckdb::ColumnBinding binding) {
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    auto& get = op.Cast<duckdb::LogicalGet>();
    if (get.table_index != binding.table_index) {
      return false;
    }
    if (!connector::IsSereneDBScan(get)) {
      return false;
    }
    auto& bd = get.bind_data->Cast<connector::SereneDBScanBindData>();
    const auto col_idx = binding.column_index.GetIndex();
    const auto& col_ids = get.GetColumnIds();
    if (col_idx >= col_ids.size() || !col_ids[col_idx].HasPrimaryIndex()) {
      return false;
    }
    const auto phys = col_ids[col_idx].GetPrimaryIndex();
    if (phys >= bd.column_ids.size()) {
      return false;
    }
    return bd.column_ids[phys] == catalog::Column::kInvertedIndexScoreId;
  }
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    auto& proj = op.Cast<duckdb::LogicalProjection>();
    if (proj.table_index == binding.table_index) {
      const auto col_idx = binding.column_index.GetIndex();
      if (col_idx >= proj.expressions.size()) {
        return false;
      }
      auto& e = *proj.expressions[col_idx];
      if (e.type != duckdb::ExpressionType::BOUND_COLUMN_REF) {
        return false;
      }
      auto inner = e.Cast<duckdb::BoundColumnRefExpression>().binding;
      for (auto& c : proj.children) {
        if (BindingIsScoreColumn(*c, inner)) {
          return true;
        }
      }
      return false;
    }
  }
  for (auto& c : op.children) {
    if (BindingIsScoreColumn(*c, binding)) {
      return true;
    }
  }
  return false;
}

// Add kInvertedIndexScoreId as a new column to the scan's bind_data and
// LogicalGet. Returns the position in get.column_ids (used for
// BoundColumnRefExpression). Idempotent: if score column already present,
// returns its existing position.
duckdb::idx_t AddScoreColumn(connector::SereneDBScanBindData& bind_data,
                             duckdb::LogicalGet& get) {
  // Check if already added to bind_data.
  auto score_bind_idx = duckdb::DConstants::INVALID_INDEX;
  for (duckdb::idx_t i = 0; i < bind_data.column_ids.size(); ++i) {
    if (bind_data.column_ids[i] == catalog::Column::kInvertedIndexScoreId) {
      score_bind_idx = i;
      break;
    }
  }
  if (score_bind_idx == duckdb::DConstants::INVALID_INDEX) {
    // Not yet added -- append.
    score_bind_idx = bind_data.column_ids.size();
    bind_data.column_ids.push_back(catalog::Column::kInvertedIndexScoreId);
    bind_data.column_types.push_back(duckdb::LogicalType::FLOAT);
  }
  // Check if the LogicalGet already references this bind_data slot.
  auto& col_ids = get.GetColumnIds();
  for (duckdb::idx_t i = 0; i < col_ids.size(); ++i) {
    if (col_ids[i].HasPrimaryIndex() &&
        col_ids[i].GetPrimaryIndex() == score_bind_idx) {
      return i;
    }
  }
  // Add the reference in LogicalGet.
  // returned_types/names are the full schema; score_bind_idx must be a valid
  // index into returned_types, so extend the schema first.
  get.returned_types.push_back(duckdb::LogicalType::FLOAT);
  get.names.emplace_back(catalog::Column::kScoreName);
  const auto get_col_idx = col_ids.size();
  const auto proj_idx =
    get.AddColumnId(static_cast<duckdb::column_t>(score_bind_idx));
  // If RemoveUnusedColumns has already populated projection_ids, the new
  // column must be added there too, otherwise GetColumnBindings() won't
  // include it and filter/expression resolution will crash.
  if (!get.projection_ids.empty()) {
    get.projection_ids.push_back(proj_idx);
  }
  // Cached operator types must match bindings count -- the column binding
  // resolver checks both. Follow the RowNumberRewriter pattern and push
  // unconditionally; ResolveTypes would add this entry whether or not
  // projection_ids was used.
  get.types.push_back(duckdb::LogicalType::FLOAT);
  return get_col_idx;
}

// When a score column is added to a LogicalGet, intermediate LogicalFilter
// operators above it may have a non-empty `projection_map` that drops the
// new column from their upward-exposed bindings. Walk the subtree rooted at
// `op` and extend each such projection_map so the score column remains
// visible to operators above (TopN ORDER BY, outer Projections, etc.).
//
// Returns the position of the score column in `op`'s output bindings, or
// INVALID_INDEX if the target Get isn't below `op`.
duckdb::idx_t ExposeScoreThroughSubtree(duckdb::LogicalOperator& op,
                                        const duckdb::LogicalGet* target_get,
                                        duckdb::idx_t score_pos_in_child) {
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    auto* get = &op.Cast<duckdb::LogicalGet>();
    return (get == target_get) ? score_pos_in_child
                               : duckdb::DConstants::INVALID_INDEX;
  }
  // Extend a projection_map-style vector so `pos` survives upward.
  // Returns the new column's position in the operator's output bindings,
  // or INVALID_INDEX if the map already includes it.
  auto extend_map = [](std::vector<duckdb::ProjectionIndex>& map,
                       duckdb::idx_t pos) -> duckdb::idx_t {
    if (map.empty()) {
      return pos;  // map is inactive -> all child bindings pass through
    }
    for (const auto& p : map) {
      if (p.GetIndex() == pos) {
        return duckdb::DConstants::INVALID_INDEX;
      }
    }
    map.emplace_back(duckdb::ProjectionIndex(pos));
    return map.size() - 1;
  };
  for (duckdb::idx_t child_idx = 0; child_idx < op.children.size();
       ++child_idx) {
    auto& child = op.children[child_idx];
    auto pos =
      ExposeScoreThroughSubtree(*child, target_get, score_pos_in_child);
    if (pos == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    if (op.type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
      return extend_map(op.Cast<duckdb::LogicalFilter>().projection_map, pos);
    }
    if (op.type == duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
        op.type == duckdb::LogicalOperatorType::LOGICAL_ANY_JOIN ||
        op.type == duckdb::LogicalOperatorType::LOGICAL_DELIM_JOIN ||
        op.type == duckdb::LogicalOperatorType::LOGICAL_ASOF_JOIN ||
        op.type == duckdb::LogicalOperatorType::LOGICAL_CROSS_PRODUCT ||
        op.type == duckdb::LogicalOperatorType::LOGICAL_POSITIONAL_JOIN) {
      auto& j = op.Cast<duckdb::LogicalJoin>();
      auto& map =
        child_idx == 0 ? j.left_projection_map : j.right_projection_map;
      const auto new_pos = extend_map(map, pos);
      if (new_pos == duckdb::DConstants::INVALID_INDEX) {
        return duckdb::DConstants::INVALID_INDEX;
      }
      // Join's output concatenates left bindings then right bindings, so
      // a right-side column sits after all of the left's exposed columns.
      if (child_idx == 1) {
        const auto left_count = j.left_projection_map.empty()
                                  ? j.children[0]->GetColumnBindings().size()
                                  : j.left_projection_map.size();
        return left_count + new_pos;
      }
      return new_pos;
    }
    // Pass-through operators (Filter without projection_map, Limit, Order,
    // etc.): column position is preserved in child-to-parent mapping.
    return pos;
  }
  return duckdb::DConstants::INVALID_INDEX;
}

// Extract a constant Value pointer from `expr`, or null if not a
// BoundConstantExpression. Used by the scorer-param parser to read
// compile-time k1 / b / with_norms arguments.
const duckdb::Value* TryGetConstantValue(const duckdb::Expression& expr) {
  if (expr.expression_class != duckdb::ExpressionClass::BOUND_CONSTANT) {
    return nullptr;
  }
  return &expr.Cast<duckdb::BoundConstantExpression>().value;
}

bool TrySetScorer(connector::SearchScan::ScorerParams& scorer,
                  const duckdb::BoundFunctionExpression& func,
                  std::string_view name);

// Recursively rewrites bm25/tfidf calls anywhere in an expression tree.
// Handles expressions wrapped in comparisons, casts, operators, and functions.
// When `set_scorer` is true, sets the scorer on the scan (pass 1).
// When false, only rewrites if scorer was already set (pass 2).
// Returns a replacement expression if THIS node is a scorer call; otherwise
// mutates children in-place and returns nullptr.
duckdb::unique_ptr<duckdb::Expression> RewriteScoreCallInExpr(
  duckdb::unique_ptr<duckdb::Expression>& expr, duckdb::LogicalOperator& root,
  bool& changed, bool set_scorer);

void RewriteScoreCallInChildren(duckdb::unique_ptr<duckdb::Expression>& expr,
                                duckdb::LogicalOperator& root, bool& changed,
                                bool set_scorer) {
  using EC = duckdb::ExpressionClass;
  if (!expr) {
    return;
  }
  switch (expr->expression_class) {
    case EC::BOUND_FUNCTION: {
      auto& f = expr->Cast<duckdb::BoundFunctionExpression>();
      for (auto& c : f.children) {
        auto r = RewriteScoreCallInExpr(c, root, changed, set_scorer);
        if (r) {
          c = std::move(r);
        }
      }
      break;
    }
    case EC::BOUND_COMPARISON: {
      auto& cmp = expr->Cast<duckdb::BoundComparisonExpression>();
      auto l = RewriteScoreCallInExpr(cmp.left, root, changed, set_scorer);
      if (l) {
        cmp.left = std::move(l);
      }
      auto r = RewriteScoreCallInExpr(cmp.right, root, changed, set_scorer);
      if (r) {
        cmp.right = std::move(r);
      }
      break;
    }
    case EC::BOUND_CAST: {
      auto& c = expr->Cast<duckdb::BoundCastExpression>();
      auto r = RewriteScoreCallInExpr(c.child, root, changed, set_scorer);
      if (r) {
        c.child = std::move(r);
      }
      break;
    }
    case EC::BOUND_OPERATOR: {
      auto& op = expr->Cast<duckdb::BoundOperatorExpression>();
      for (auto& c : op.children) {
        auto r = RewriteScoreCallInExpr(c, root, changed, set_scorer);
        if (r) {
          c = std::move(r);
        }
      }
      break;
    }
    case EC::BOUND_WINDOW: {
      auto& w = expr->Cast<duckdb::BoundWindowExpression>();
      auto rewrite_one = [&](duckdb::unique_ptr<duckdb::Expression>& c) {
        if (!c) {
          return;
        }
        auto r = RewriteScoreCallInExpr(c, root, changed, set_scorer);
        if (r) {
          c = std::move(r);
        }
      };
      for (auto& c : w.children) {
        rewrite_one(c);
      }
      for (auto& c : w.partitions) {
        rewrite_one(c);
      }
      for (auto& o : w.orders) {
        rewrite_one(o.expression);
      }
      for (auto& o : w.arg_orders) {
        rewrite_one(o.expression);
      }
      rewrite_one(w.filter_expr);
      rewrite_one(w.start_expr);
      rewrite_one(w.end_expr);
      break;
    }
    default:
      break;
  }
}

bool IsScorerFunctionName(std::string_view name);

duckdb::unique_ptr<duckdb::Expression> RewriteScoreCallInExpr(
  duckdb::unique_ptr<duckdb::Expression>& expr, duckdb::LogicalOperator& root,
  bool& changed, bool set_scorer) {
  if (!expr) {
    return nullptr;
  }
  if (expr->expression_class != duckdb::ExpressionClass::BOUND_FUNCTION) {
    if (expr->expression_class == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
      auto& ref = expr->Cast<duckdb::BoundColumnRefExpression>();
      if (!ref.alias.empty() && ref.alias != catalog::Column::kScoreName &&
          BindingIsScoreColumn(root, ref.binding)) {
        ref.alias = catalog::Column::kScoreName;
        changed = true;
      }
    }
    RewriteScoreCallInChildren(expr, root, changed, set_scorer);
    return nullptr;
  }
  auto& func = expr->Cast<duckdb::BoundFunctionExpression>();
  const auto& name = func.function.name;
  if (!IsScorerFunctionName(name)) {
    RewriteScoreCallInChildren(expr, root, changed, set_scorer);
    return nullptr;
  }
  // Scorer call -- check tableoid argument.
  if (func.children.empty() || func.children[0]->expression_class !=
                                 duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return nullptr;
  }
  auto& anchor = func.children[0]->Cast<duckdb::BoundColumnRefExpression>();
  auto found = FindSearchScanByTableIndex(root, anchor.binding.table_index);
  if (!found) {
    return nullptr;  // No SearchScan -- stub will raise.
  }
  // Restrict scorer functions to explicit inverted-index queries
  // (FROM <idx_name>). When the query is FROM <table> and the search-filter
  // builder opportunistically promotes a base-table scan into a
  // SearchScan (e.g. because a range predicate on an indexed column is
  // pushable), the user is not asking to score the text index and
  // bm25()/tfidf() should fall through to the runtime stub error.
  if (found->bind_data->entry_kind == connector::ScanEntryKind::BaseTable) {
    return nullptr;
  }
  if (set_scorer) {
    if (!TrySetScorer(found->search_scan->scorer, func, name)) {
      return nullptr;  // Non-constant params.
    }
  } else {
    if (found->search_scan->scorer.kind ==
        connector::SearchScan::ScorerKind::None) {
      return nullptr;  // Not claimed in pass 1 -- leave for stub.
    }
  }
  auto idx = AddScoreColumn(*found->bind_data, *found->get);
  // Ensure any LogicalFilter between the Get and the rewrite site exposes
  // the new score column through its projection_map. Without this, upstream
  // operators (e.g. TopN ORDER BY) can't resolve the column reference.
  ExposeScoreThroughSubtree(root, found->get, idx);

  duckdb::ColumnBinding result_binding;
  if (anchor.binding.table_index == found->get->table_index) {
    // Anchor is directly on the Get -- the rewrite site sees Get columns,
    // so a Get binding resolves.
    result_binding = {found->get->table_index, duckdb::ProjectionIndex{idx}};
  } else {
    // Anchor is on an intermediate LogicalProjection (DuckDB rebound
    // the ref during column-lifetime analysis). A Get binding won't
    // resolve past the projection boundary; find an already-materialised
    // score ref in that projection and reference its output column.
    auto* proj = FindProjectionByTableIndex(root, anchor.binding.table_index);
    if (!proj) {
      return nullptr;
    }
    auto score_col_in_proj = duckdb::DConstants::INVALID_INDEX;
    for (duckdb::idx_t i = 0; i < proj->expressions.size(); ++i) {
      auto& e = *proj->expressions[i];
      if (e.type != duckdb::ExpressionType::BOUND_COLUMN_REF) {
        continue;
      }
      auto& ref = e.Cast<duckdb::BoundColumnRefExpression>();
      if (ref.binding.table_index == found->get->table_index &&
          ref.binding.column_index.GetIndex() == idx) {
        score_col_in_proj = i;
        break;
      }
    }
    if (score_col_in_proj == duckdb::DConstants::INVALID_INDEX) {
      return nullptr;
    }
    result_binding = {proj->table_index,
                      duckdb::ProjectionIndex{score_col_in_proj}};
  }
  changed = true;
  return duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    std::string{catalog::Column::kScoreName}, duckdb::LogicalType::FLOAT,
    result_binding);
}

// True iff `expr` is a BoundColumnRefExpression bound to the given
// scan's table_index. We treat the actual column position as opaque --
// Resolve a column reference (post projection-pushdown) to its catalog
// Column::Id via `bind_data.column_ids`. Returns kInvalidId if the ref
// doesn't belong to the scan.
catalog::Column::Id ResolveColumnId(
  const duckdb::BoundColumnRefExpression& ref,
  const connector::SereneDBScanBindData& bind_data,
  const duckdb::LogicalGet& get) {
  if (ref.binding.table_index != get.table_index) {
    return std::numeric_limits<catalog::Column::Id>::max();
  }
  const auto col_idx = ref.binding.column_index;
  const auto& column_ids = get.GetColumnIds();
  if (col_idx >= column_ids.size()) {
    return std::numeric_limits<catalog::Column::Id>::max();
  }
  if (!column_ids[col_idx].HasPrimaryIndex()) {
    return std::numeric_limits<catalog::Column::Id>::max();
  }
  const auto phys = column_ids[col_idx].GetPrimaryIndex();
  if (phys >= bind_data.column_ids.size()) {
    return std::numeric_limits<catalog::Column::Id>::max();
  }
  return bind_data.column_ids[phys];
}

// True iff `name` is one of the supported scorer function names.
bool IsScorerFunctionName(std::string_view name) {
  return name == connector::kBm25 || name == connector::kTfidf ||
         name == connector::kRawTf || name == connector::kLmJm ||
         name == connector::kLmDirichlet ||
         name == connector::kIndriDirichlet || name == connector::kDfi;
}

// Parse scorer parameters from `func` into `scorer`. Returns false if the
// parameters are non-constant (rule refuses to claim; stub raises).
// Also validates scorer kind conflicts: same kind+params is idempotent,
// different kind/params throws.
bool TrySetScorer(connector::SearchScan::ScorerParams& scorer,
                  const duckdb::BoundFunctionExpression& func,
                  std::string_view name) {
  using ScorerParams = connector::SearchScan::ScorerParams;
  using ScorerKind = connector::SearchScan::ScorerKind;
  ScorerParams candidate;

  if (name == connector::kBm25) {
    candidate.kind = ScorerKind::Bm25;
    candidate.bm25 = ScorerParams::Bm25{};
    if (func.children.size() == 3) {
      auto* k1v = TryGetConstantValue(*func.children[1]);
      auto* bv = TryGetConstantValue(*func.children[2]);
      if (!k1v || !bv) {
        return false;
      }
      candidate.bm25.k1 = k1v->GetValue<double>();
      candidate.bm25.b = bv->GetValue<double>();
    }
  } else if (name == connector::kTfidf) {
    candidate.kind = ScorerKind::Tfidf;
    candidate.tfidf = ScorerParams::Tfidf{};
    if (func.children.size() == 2) {
      auto* cv = TryGetConstantValue(*func.children[1]);
      if (!cv) {
        return false;
      }
      candidate.tfidf.with_norms = cv->GetValue<bool>();
    }
  } else if (name == connector::kRawTf) {
    candidate.kind = ScorerKind::RawTf;
    // raw_tf has no parameters; `raw_tf` arm already default-constructed.
  } else if (name == connector::kLmJm) {
    candidate.kind = ScorerKind::LmJm;
    candidate.lm_jm = ScorerParams::LmJm{};
    if (func.children.size() == 2) {
      auto* lv = TryGetConstantValue(*func.children[1]);
      if (!lv) {
        return false;
      }
      candidate.lm_jm.lambda = lv->GetValue<double>();
      if (!(candidate.lm_jm.lambda > 0.0 && candidate.lm_jm.lambda <= 1.0)) {
        throw duckdb::InvalidInputException(
          "lm_jm lambda must be in (0, 1], got " +
          std::to_string(candidate.lm_jm.lambda));
      }
    }
  } else if (name == connector::kLmDirichlet) {
    candidate.kind = ScorerKind::LmDirichlet;
    candidate.lm_dirichlet = ScorerParams::LmDirichlet{};
    if (func.children.size() == 2) {
      auto* mv = TryGetConstantValue(*func.children[1]);
      if (!mv) {
        return false;
      }
      candidate.lm_dirichlet.mu = mv->GetValue<double>();
      if (candidate.lm_dirichlet.mu < 0.0 ||
          !std::isfinite(candidate.lm_dirichlet.mu)) {
        throw duckdb::InvalidInputException(
          "lm_dirichlet mu must be a non-negative finite value, got " +
          std::to_string(candidate.lm_dirichlet.mu));
      }
    }
  } else if (name == connector::kIndriDirichlet) {
    candidate.kind = ScorerKind::IndriDirichlet;
    candidate.indri_dirichlet = ScorerParams::IndriDirichlet{};
    if (func.children.size() == 2) {
      auto* mv = TryGetConstantValue(*func.children[1]);
      if (!mv) {
        return false;
      }
      candidate.indri_dirichlet.mu = mv->GetValue<double>();
      if (candidate.indri_dirichlet.mu < 0.0 ||
          !std::isfinite(candidate.indri_dirichlet.mu)) {
        throw duckdb::InvalidInputException(
          "indri_dirichlet mu must be a non-negative finite value, got " +
          std::to_string(candidate.indri_dirichlet.mu));
      }
    }
  } else if (name == connector::kDfi) {
    candidate.kind = ScorerKind::Dfi;
    candidate.dfi = ScorerParams::Dfi{};
    if (func.children.size() == 2) {
      auto* mv = TryGetConstantValue(*func.children[1]);
      if (!mv) {
        return false;
      }
      auto s = mv->GetValue<std::string>();
      if (s == "standardized") {
        candidate.dfi.measure = connector::SearchScan::DfiMeasure::Standardized;
      } else if (s == "saturated") {
        candidate.dfi.measure = connector::SearchScan::DfiMeasure::Saturated;
      } else if (s == "chi_squared" || s == "chisquared") {
        candidate.dfi.measure = connector::SearchScan::DfiMeasure::ChiSquared;
      } else {
        throw duckdb::InvalidInputException(
          "dfi measure must be one of: standardized, saturated, chi_squared; "
          "got '" +
          s + "'");
      }
    }
  } else {
    return false;  // Unreachable -- caller filters on IsScorerFunctionName.
  }

  if (scorer.kind == ScorerKind::None) {
    scorer = candidate;
    return true;
  }
  // Already set -- check for conflict. Compare only the live arm; other
  // arms of the union may be uninitialized.
  if (scorer.kind == candidate.kind) {
    bool same = false;
    switch (scorer.kind) {
      case ScorerKind::Bm25:
        same = scorer.bm25.k1 == candidate.bm25.k1 &&
               scorer.bm25.b == candidate.bm25.b;
        break;
      case ScorerKind::Tfidf:
        same = scorer.tfidf.with_norms == candidate.tfidf.with_norms;
        break;
      case ScorerKind::RawTf:
        same = true;
        break;
      case ScorerKind::LmJm:
        same = scorer.lm_jm.lambda == candidate.lm_jm.lambda;
        break;
      case ScorerKind::LmDirichlet:
        same = scorer.lm_dirichlet.mu == candidate.lm_dirichlet.mu;
        break;
      case ScorerKind::IndriDirichlet:
        same = scorer.indri_dirichlet.mu == candidate.indri_dirichlet.mu;
        break;
      case ScorerKind::Dfi:
        same = scorer.dfi.measure == candidate.dfi.measure;
        break;
      case ScorerKind::None:
        break;  // unreachable -- covered by outer if above
    }
    if (same) {
      return true;  // Idempotent.
    }
  }
  throw duckdb::InvalidInputException(
    "Only one scorer function is allowed per inverted index\n"
    "HINT: Use UNION to combine different score functions for same "
    "inverted index");
}

// True iff `expr` is a BoundFunctionExpression named bm25/tfidf whose first
// argument is a BoundColumnRef anchored on a SearchScan reachable from `root`.
bool IsScorerCallAnchoredOnSearchScan(duckdb::LogicalOperator& root,
                                      const duckdb::Expression& expr) {
  if (expr.expression_class != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return false;
  }
  const auto& func = expr.Cast<duckdb::BoundFunctionExpression>();
  const auto& name = func.function.name;
  if (!IsScorerFunctionName(name)) {
    return false;
  }
  if (func.children.empty() || func.children[0]->expression_class !=
                                 duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return false;
  }
  const auto& anchor =
    func.children[0]->Cast<duckdb::BoundColumnRefExpression>();
  return FindSearchScanByTableIndex(root, anchor.binding.table_index)
    .has_value();
}

// Recognize `score_fn(tableoid) > 0` (or `> 0.0`) and replace it in-place
// with a BoundConstantExpression TRUE. Returns true if any rewrite happened.
// Since scorer outputs are positive for matching docs, the comparison is
// redundant and safely drops. Recurses into children of Cast / Operator /
// Comparison nodes so nested occurrences are caught.
bool SimplifyScoreGtZero(duckdb::LogicalOperator& root,
                         duckdb::unique_ptr<duckdb::Expression>& expr) {
  if (!expr) {
    return false;
  }
  using EC = duckdb::ExpressionClass;
  bool changed = false;
  if (expr->expression_class == EC::BOUND_COMPARISON) {
    auto& cmp = expr->Cast<duckdb::BoundComparisonExpression>();
    auto is_zero = [](const duckdb::Expression& e) {
      if (e.expression_class != EC::BOUND_CONSTANT) {
        return false;
      }
      const auto& v = e.Cast<duckdb::BoundConstantExpression>().value;
      if (v.IsNull()) {
        return false;
      }
      try {
        return v.GetValue<double>() == 0.0;
      } catch (...) {
        return false;
      }
    };
    // Matches `score > 0`, `score >= 0` (non-negative comparisons).
    const bool is_gt_zero_shape =
      (cmp.type == duckdb::ExpressionType::COMPARE_GREATERTHAN ||
       cmp.type == duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO) &&
      cmp.left && cmp.right && is_zero(*cmp.right) &&
      IsScorerCallAnchoredOnSearchScan(root, *cmp.left);
    // Matches `0 < score`.
    const bool is_zero_lt_shape =
      (cmp.type == duckdb::ExpressionType::COMPARE_LESSTHAN ||
       cmp.type == duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO) &&
      cmp.left && cmp.right && is_zero(*cmp.left) &&
      IsScorerCallAnchoredOnSearchScan(root, *cmp.right);
    if (is_gt_zero_shape || is_zero_lt_shape) {
      expr = duckdb::make_uniq<duckdb::BoundConstantExpression>(
        duckdb::Value::BOOLEAN(true));
      return true;
    }
  }
  // Recurse into structural children so nested filter predicates are caught.
  switch (expr->expression_class) {
    case EC::BOUND_OPERATOR: {
      auto& op = expr->Cast<duckdb::BoundOperatorExpression>();
      for (auto& c : op.children) {
        changed |= SimplifyScoreGtZero(root, c);
      }
      break;
    }
    case EC::BOUND_CAST: {
      auto& c = expr->Cast<duckdb::BoundCastExpression>();
      changed |= SimplifyScoreGtZero(root, c.child);
      break;
    }
    case EC::BOUND_COMPARISON: {
      auto& cmp = expr->Cast<duckdb::BoundComparisonExpression>();
      changed |= SimplifyScoreGtZero(root, cmp.left);
      changed |= SimplifyScoreGtZero(root, cmp.right);
      break;
    }
    case EC::BOUND_FUNCTION: {
      auto& f = expr->Cast<duckdb::BoundFunctionExpression>();
      for (auto& c : f.children) {
        changed |= SimplifyScoreGtZero(root, c);
      }
      break;
    }
    default:
      break;
  }
  return changed;
}

// Append a virtual LIST(BIGINT) offsets column to the scan's bind_data
// and LogicalGet. Returns the column's slot in get.column_ids -- the
// value a BoundColumnRefExpression should carry in ColumnBinding.
duckdb::idx_t AddOffsetsColumn(connector::SereneDBScanBindData& bind_data,
                               duckdb::LogicalGet& get,
                               catalog::Column::Id target_col_id) {
  const auto offsets_type = catalog::Column::MakeOffsetsType();
  const auto bind_idx = bind_data.column_ids.size();
  bind_data.column_ids.push_back(catalog::Column::kInvertedIndexOffsetsId);
  bind_data.column_types.push_back(offsets_type);

  get.returned_types.push_back(offsets_type);
  get.names.push_back(catalog::Column::MakeOffsetsName(target_col_id));

  const auto get_col_idx = get.GetColumnIds().size();
  const auto proj_idx =
    get.AddColumnId(static_cast<duckdb::column_t>(bind_idx));
  if (!get.projection_ids.empty()) {
    get.projection_ids.push_back(proj_idx);
  }
  get.types.push_back(offsets_type);
  return get_col_idx;
}

// Result of parsing and validating an OFFSETS(col [, limit]) projection
// expression. The call has already been resolved against the plan --
// scan + catalog column id are filled in, limits are validated.
struct ParsedOffsetsCall {
  FoundScan scan;
  catalog::Column::Id target_col_id;
  size_t limit;  // 0 == unlimited
  std::string col_name;
};

// Catalog column name for `ref`. Falls back to the ref's alias for
// error messages when the scan hasn't been resolved yet.
std::string OffsetsColumnName(const duckdb::BoundColumnRefExpression& ref,
                              const duckdb::LogicalGet* get) {
  if (get) {
    const auto& col_ids = get->GetColumnIds();
    const auto col_idx = ref.binding.column_index;
    if (col_idx < col_ids.size()) {
      return get->GetColumnName(col_ids[col_idx]);
    }
  }
  return ref.alias;
}

// Validate an OFFSETS() projection call and resolve it against the
// surrounding plan. Throws `duckdb::InvalidInputException` with a
// specific message when the call is malformed, anchored off an
// inverted-index scan, or references a column that isn't in the index.
// Arity and static arg types are enforced by the function registration
// (see `search.cpp`), so this only does the remaining semantic checks.
ParsedOffsetsCall ParseOffsetsCall(duckdb::BoundFunctionExpression& func,
                                   duckdb::LogicalOperator& root) {
  if (func.children[0]->expression_class !=
      duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    throw duckdb::InvalidInputException(
      "OFFSETS() first argument must be a column reference");
  }
  auto& col_ref = func.children[0]->Cast<duckdb::BoundColumnRefExpression>();

  // Default cap applied when no explicit limit is given. Matches the
  // Velox-era default in server/pg/sql_collector.h:kDefaultOffsetsLimit.
  // `OFFSETS(col, 0)` means "no limit"; `OFFSETS(col)` means "cap at the
  // default".
  constexpr size_t kDefaultOffsetsLimit = 10;
  size_t limit = kDefaultOffsetsLimit;
  if (func.children.size() == 2) {
    auto& arg1 = *func.children[1];
    if (arg1.expression_class != duckdb::ExpressionClass::BOUND_CONSTANT) {
      throw duckdb::InvalidInputException(
        "OFFSETS() second argument must be an integer literal");
    }
    const auto raw =
      arg1.Cast<duckdb::BoundConstantExpression>().value.GetValue<int32_t>();
    if (raw < 0) {
      throw duckdb::InvalidInputException(
        "OFFSETS() limit must be greater than zero or 0 for no limit");
    }
    limit = static_cast<size_t>(raw);
  }

  auto found = FindSearchScanByTableIndex(root, col_ref.binding.table_index);
  if (!found) {
    throw duckdb::InvalidInputException(
      "OFFSETS(%s) requires an inverted index scan in the same sub-query",
      OffsetsColumnName(col_ref, nullptr));
  }

  // Require FROM <idx_name>. OFFSETS() on a base-table scan that was
  // opportunistically promoted to a SearchScan is not supported.
  const auto col_name = OffsetsColumnName(col_ref, found->get);
  if (!found->bind_data->IsInvertedIndexEntry()) {
    throw duckdb::InvalidInputException(
      "OFFSETS(%s) requires an inverted index scan in the same sub-query",
      col_name);
  }

  const auto target_col_id =
    ResolveColumnId(col_ref, *found->bind_data, *found->get);
  if (target_col_id == std::numeric_limits<catalog::Column::Id>::max()) {
    throw duckdb::InvalidInputException(
      "OFFSETS(): column '%s' not found in table", col_name);
  }
  const auto& idx_col_ids = found->bind_data->inverted_index->GetColumnIds();
  const bool in_index =
    absl::c_find(idx_col_ids, target_col_id) != idx_col_ids.end();
  if (!in_index) {
    throw duckdb::InvalidInputException(
      "OFFSETS(): column '%s' not found in index", col_name);
  }

  return ParsedOffsetsCall{.scan = *found,
                           .target_col_id = target_col_id,
                           .limit = limit,
                           .col_name = col_name};
}

// If `expr` is an OFFSETS() call anchored on a SearchScan reachable
// from `root`, validate it and return a BoundColumnRefExpression
// pointing at a freshly-added virtual LIST(BIGINT) column on that scan.
// Otherwise returns nullptr (the caller keeps the original expression).
// Throws `InvalidInputException` for malformed calls or when offsets
// semantics cannot be satisfied (duplicate field with conflicting
// limit, column not in index, etc.).
duckdb::unique_ptr<duckdb::Expression> RewriteOffsetsCall(
  duckdb::Expression& expr, duckdb::LogicalOperator& root) {
  if (expr.expression_class != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return nullptr;
  }
  auto& func = expr.Cast<duckdb::BoundFunctionExpression>();
  if (func.function.name != connector::kOffsets) {
    return nullptr;
  }

  auto parsed = ParseOffsetsCall(func, root);

  // Same field requested twice with DIFFERENT limits is unsupported
  // (the runtime collects one offset vector per request; differing
  // limits would need two independent collection passes for one field).
  // Same-field same-limit duplicates are legal; we just allocate another
  // virtual column and the runtime emits the data twice.
  for (const auto& req : parsed.scan.search_scan->offsets) {
    if (req.column_id == parsed.target_col_id && req.limit != parsed.limit) {
      throw duckdb::InvalidInputException(
        "OFFSETS() called multiple times for field '%s' with different limits",
        parsed.col_name);
    }
  }

  parsed.scan.search_scan->offsets.push_back(
    {.column_id = parsed.target_col_id, .limit = parsed.limit});
  const auto get_col_idx = AddOffsetsColumn(
    *parsed.scan.bind_data, *parsed.scan.get, parsed.target_col_id);
  // Expose the new column through any Filter projection_map between the
  // Get and this projection. projection_map indexes into
  // children[0]->GetColumnBindings(), which has size column_ids.size()
  // when get.projection_ids is empty, else projection_ids.size(). We
  // just appended to whichever applies, so the new position is size-1.
  const auto binding_position = parsed.scan.get->projection_ids.empty()
                                  ? get_col_idx
                                  : parsed.scan.get->projection_ids.size() - 1;
  ExposeScoreThroughSubtree(root, parsed.scan.get, binding_position);

  duckdb::ColumnBinding binding{parsed.scan.get->table_index,
                                duckdb::ProjectionIndex{get_col_idx}};
  auto col = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    catalog::Column::MakeOffsetsName(parsed.target_col_id),
    catalog::Column::MakeOffsetsType(), binding);
  col->alias = expr.alias;
  return col;
}

// Walk `expr` recursively: rewrite this node if it's an OFFSETS call,
// otherwise recurse into children. Mutates in-place. Returns a
// replacement for THIS node (caller substitutes it into its parent)
// when the top-level expression is itself an OFFSETS call.
duckdb::unique_ptr<duckdb::Expression> RewriteOffsetsInExpr(
  duckdb::unique_ptr<duckdb::Expression>& expr, duckdb::LogicalOperator& root,
  bool& changed) {
  if (!expr) {
    return nullptr;
  }
  if (auto rep = RewriteOffsetsCall(*expr, root)) {
    changed = true;
    return rep;
  }
  // Use duckdb's ExpressionIterator so we cover every wrapper class
  // (BOUND_CAST, BOUND_OPERATOR, aggregate filters, CASE, etc.) without
  // having to enumerate them here.
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      auto r = RewriteOffsetsInExpr(child, root, changed);
      if (r) {
        child = std::move(r);
      }
    });
  return nullptr;
}

// Walk a LogicalProjection's expressions for bm25/tfidf/offsets calls
// anchored on a SearchScan. Sets scorer params and rewrites offsets
// calls (at any nesting depth) into BoundColumnRefExpression pointing
// at a freshly-added virtual LIST(BIGINT) column on the scan.
bool TryAttachScoreOffsets(duckdb::LogicalOperator& root,
                           duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  if (plan->type != duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    return false;
  }
  auto& projection = plan->Cast<duckdb::LogicalProjection>();
  bool changed = false;
  for (duckdb::idx_t i = 0; i < projection.expressions.size(); ++i) {
    auto scorer_rep = RewriteScoreCallInExpr(projection.expressions[i], root,
                                             changed, /*set_scorer=*/true);
    if (scorer_rep) {
      projection.expressions[i] = std::move(scorer_rep);
    }
    auto offsets_rep =
      RewriteOffsetsInExpr(projection.expressions[i], root, changed);
    if (offsets_rep) {
      projection.expressions[i] = std::move(offsets_rep);
    }
  }
  return changed;
}

// Rewrite bm25/tfidf calls in non-projection contexts (Filter, TopN ORDER BY)
// by replacing them with BoundColumnRefExpression pointing at the score
// column that pass 1 already attached. Called in pass 2.
bool TryRewriteScorerExpressions(
  duckdb::LogicalOperator& root,
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  bool changed = false;

  auto rewrite_expr = [&](duckdb::unique_ptr<duckdb::Expression>& expr) {
    auto r = RewriteScoreCallInExpr(expr, root, changed, /*set_scorer=*/false);
    if (r) {
      expr = std::move(r);
    }
  };

  if (plan->type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
    auto& filter = plan->Cast<duckdb::LogicalFilter>();
    for (auto& e : filter.expressions) {
      rewrite_expr(e);
    }
  } else if (plan->type == duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
    auto& top_n = plan->Cast<duckdb::LogicalTopN>();
    for (auto& order : top_n.orders) {
      rewrite_expr(order.expression);
    }
  } else if (plan->type == duckdb::LogicalOperatorType::LOGICAL_ORDER_BY) {
    auto& order_op = plan->Cast<duckdb::LogicalOrder>();
    for (auto& order : order_op.orders) {
      rewrite_expr(order.expression);
    }
  } else if (plan->type == duckdb::LogicalOperatorType::LOGICAL_WINDOW) {
    // BoundWindowExpression instances live directly in plan->expressions;
    // the recursive rewriter descends into partitions/orders/bounds.
    for (auto& e : plan->expressions) {
      rewrite_expr(e);
    }
  }

  return changed;
}

// Pull an ORDER BY <score> DESC LIMIT k upstream into SearchScan.score_top_k.
// Only LogicalTopN is recognised here: a bare LogicalLimit (no ORDER BY)
// does NOT request top-K by score, so pulling it would silently change
// "any k matching rows" into "the k highest-scoring rows" -- different
// result sets.
bool TryAttachScoreTopK(duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  if (plan->type != duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
    return false;
  }
  auto found = FindSearchScanChild(*plan);
  if (!found) {
    return false;
  }
  if (found->search_scan->scorer.kind ==
      connector::SearchScan::ScorerKind::None) {
    return false;  // No scoring requested -- nothing to prune.
  }
  if (found->search_scan->score_top_k) {
    return false;  // Already pulled.
  }
  auto& top_n = plan->Cast<duckdb::LogicalTopN>();
  if (top_n.limit == 0 || top_n.offset != 0 || top_n.orders.size() != 1 ||
      top_n.orders[0].type != duckdb::OrderType::DESCENDING) {
    return false;
  }
  if (top_n.orders[0].expression->type !=
      duckdb::ExpressionType::BOUND_COLUMN_REF) {
    return false;
  }
  auto binding = top_n.orders[0]
                   .expression->Cast<duckdb::BoundColumnRefExpression>()
                   .binding;
  if (!BindingIsScoreColumn(*plan->children[0], binding)) {
    return false;
  }
  found->search_scan->score_top_k = static_cast<size_t>(top_n.limit);
  // All preconditions for drop are now guaranteed by the checks above:
  // the scan emits exactly `pulled` rows sorted DESC by score from a
  // single global heap (NthPartitionScoreCollector::Finalize + streaming
  // path in duckdb_search_full_scan.cpp), and either there's no ordering
  // to preserve (LIMIT) or the ordering is provably on that same score
  // column (TopN). Drop the redundant node unconditionally.
  plan = std::move(plan->children[0]);
  return true;
}

// Match at AGGREGATE level so we can verify output shape and strip
// column_ids ourselves; projection pushdown can leave filter-only columns
// in get.column_ids that don't reflect the real output need.
bool IsCountStarLikeAggregate(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_AGGREGATE) {
    return false;
  }
  auto& agg = expr.Cast<duckdb::BoundAggregateExpression>();
  if (agg.IsDistinct() || agg.filter || agg.order_bys) {
    return false;
  }
  return agg.function.name == duckdb::CountFun::Name ||
         agg.function.name == duckdb::CountStarFun::Name;
}

bool TryConvertAggregateToCount(
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  if (plan->type !=
      duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    return false;
  }
  auto& agg = plan->Cast<duckdb::LogicalAggregate>();
  if (!agg.groups.empty() || !agg.grouping_sets.empty() ||
      !agg.grouping_functions.empty()) {
    return false;
  }
  if (agg.expressions.empty()) {
    return false;
  }
  for (auto& e : agg.expressions) {
    if (!IsCountStarLikeAggregate(*e)) {
      return false;
    }
  }
  if (agg.children.size() != 1 ||
      agg.children[0]->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
    return false;
  }
  auto& get = agg.children[0]->Cast<duckdb::LogicalGet>();
  if (!connector::IsSereneDBScan(get)) {
    return false;
  }
  auto& bind_data = get.bind_data->Cast<connector::SereneDBScanBindData>();
  // Bail on non-PHRASE pushed filters: clearing column_ids below would
  // orphan their ProjectionIndex keys and the executor would crash.
  // (PHRASE filters live in scan_source, not table_filters, so this only
  // catches `id <= N`-style pushed filters.)
  if (get.table_filters.HasFilters()) {
    return false;
  }

  auto count_scan = std::make_unique<connector::CountScan>();

  switch (bind_data.scan_source->Kind()) {
    case connector::ScanSourceKind::Search: {
      auto& search = bind_data.scan_source->Cast<connector::SearchScan>();
      if (search.scorer.kind != connector::SearchScan::ScorerKind::None) {
        return false;
      }
      if (search.EmitOffsets()) {
        return false;
      }
      count_scan->stored_filter = std::move(search.stored_filter);
      count_scan->snapshot = std::move(search.snapshot);
      count_scan->filter_summary = std::move(search.filter_summary);
      break;
    }
    case connector::ScanSourceKind::FullTable: {
      // FROM <inverted_idx> only -- table count(*) should not silently
      // expose the index lag.
      if (!bind_data.IsInvertedIndexEntry()) {
        return false;
      }
      auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
      auto resolved = ResolveIresearch(bind_data, *snapshot);
      if (!resolved) {
        return false;
      }
      count_scan->snapshot = resolved->shard->GetInvertedIndexSnapshot();
      break;
    }
    default:
      return false;
  }

  bind_data.scan_source = std::move(count_scan);
  get.function = connector::CreateIResearchCountFunction();
  get.ClearColumnIds();
  get.projection_ids.clear();
  get.types.clear();
  return true;
}

class IresearchPlanOptimizer : public duckdb::OptimizerExtension {
 public:
  IresearchPlanOptimizer() { optimize_function = Optimize; }

 private:
  // Pass 1: attach search filters + scorer detection + expression rewriting
  // for BM25/TFIDF in projections.
  static bool TryOptimizePass1(
    duckdb::unique_ptr<duckdb::LogicalOperator>& root,
    duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
    const connector::SearchFilterOptions& options) {
    if (plan->type == duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
      if (TryAnnTopk(plan)) {
        return true;
      }
      bool changed = TryAttachScoreTopK(plan);
      // Attach may have dropped the TopN (replacing `plan` with its child)
      // when the ORDER BY was provably on the scan's score column.
      if (plan->type != duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
        return changed;
      }
      // Also claim BM25/TFIDF in TOP_N ORDER BY as scorer (set_scorer=true).
      auto& top_n = plan->Cast<duckdb::LogicalTopN>();
      for (auto& order : top_n.orders) {
        auto r = RewriteScoreCallInExpr(order.expression, *root, changed,
                                        /*set_scorer=*/true);
        if (r) {
          order.expression = std::move(r);
        }
      }
      return changed;
    }
    if (plan->type == duckdb::LogicalOperatorType::LOGICAL_ORDER_BY) {
      // Claim BM25/TFIDF in ORDER BY as scorer (set_scorer=true).
      auto& order_op = plan->Cast<duckdb::LogicalOrder>();
      bool changed = false;
      for (auto& order : order_op.orders) {
        auto r = RewriteScoreCallInExpr(order.expression, *root, changed,
                                        /*set_scorer=*/true);
        if (r) {
          order.expression = std::move(r);
        }
      }
      return changed;
    }
    if (plan->type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
      if (TryAnnRange(plan)) {
        return true;
      }
      bool changed = TrySearchFilter(plan, options);
      // Simplify `BM25/TFIDF(tableoid) > 0` predicates to TRUE: a scorer's
      // value is always positive for matching docs, so the comparison adds
      // no filtering. This lets scorer selection stay controlled by the
      // output-producing sites (SELECT, ORDER BY) without being hijacked
      // when a subquery that only references the score in `>0` gets
      // flattened into an outer filter.
      if (plan->type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
        auto& filter = plan->Cast<duckdb::LogicalFilter>();
        for (auto& e : filter.expressions) {
          if (SimplifyScoreGtZero(*root, e)) {
            changed = true;
          }
        }
      }
      return changed;
    }
    if (plan->type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
      return TryAttachScoreOffsets(*root, plan);
    }
    if (plan->type == duckdb::LogicalOperatorType::LOGICAL_WINDOW) {
      // Claim BM25/TFIDF inside the window's partitions / ORDER BY as scorer.
      bool changed = false;
      for (auto& e : plan->expressions) {
        auto r = RewriteScoreCallInExpr(e, *root, changed,
                                        /*set_scorer=*/true);
        if (r) {
          e = std::move(r);
        }
      }
      return changed;
    }
    return false;
  }

  // Pass 2: top-K pullup + rewrite BM25/TFIDF in Filter/TopN order-by.
  static bool TryOptimizePass2(
    duckdb::unique_ptr<duckdb::LogicalOperator>& root,
    duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
    if (plan->type == duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
      bool changed = TryAttachScoreTopK(plan);
      changed |= TryRewriteScorerExpressions(*root, plan);
      return changed;
    }
    if (plan->type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
      return TryRewriteScorerExpressions(*root, plan);
    }
    if (plan->type == duckdb::LogicalOperatorType::LOGICAL_ORDER_BY) {
      return TryRewriteScorerExpressions(*root, plan);
    }
    if (plan->type == duckdb::LogicalOperatorType::LOGICAL_WINDOW) {
      return TryRewriteScorerExpressions(*root, plan);
    }
    if (plan->type ==
        duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
      return TryConvertAggregateToCount(plan);
    }
    return false;
  }

  static bool TopDownAnnPass(duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
                             bool in_mutation) {
    const bool subtree_in_mutation = in_mutation || IsMutationOp(plan->type);
    bool changed = false;
    if (!subtree_in_mutation) {
      if (plan->type == duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
        if (TryAnnTopk(plan)) {
          changed = true;
        }
      } else if (plan->type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
        if (TryAnnRange(plan)) {
          changed = true;
        }
      }
    }
    for (auto& child : plan->children) {
      changed |= TopDownAnnPass(child, subtree_in_mutation);
    }
    return changed;
  }

  // Bottom-up walk. `root` is the plan root (for FindSearchScanByTableIndex),
  // `plan` is the current node being visited.
  static bool Walk(duckdb::unique_ptr<duckdb::LogicalOperator>& root,
                   duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
                   bool in_mutation, int pass,
                   const connector::SearchFilterOptions& options) {
    const bool subtree_in_mutation = in_mutation || IsMutationOp(plan->type);
    bool changed = false;
    for (auto& child : plan->children) {
      changed |= Walk(root, child, subtree_in_mutation, pass, options);
    }
    if (!subtree_in_mutation) {
      if (pass == 1) {
        changed |= TryOptimizePass1(root, plan, options);
      } else {
        changed |= TryOptimizePass2(root, plan);
      }
    }
    return changed;
  }

  // Post-walk cleanup: wherever we swapped a LogicalGet.function to one
  // of our iresearch scan variants (all filter_prune=false), collapse any
  // `projection_ids` that an earlier pass (e.g. parquet's filter_prune=true
  // RemoveUnusedColumns) left behind. Required before re-running
  // RemoveUnusedColumns -- see flatten_projection_ids.h for the rationale.
  static void FlattenSwappedGets(
    duckdb::LogicalOperator& root,
    duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
    for (auto& child : plan->children) {
      FlattenSwappedGets(root, child);
    }
    if (plan->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
      return;
    }
    auto& get = plan->Cast<duckdb::LogicalGet>();
    if (get.projection_ids.empty() || get.function.filter_prune) {
      return;
    }
    FlattenProjectionIds(root, get);
  }

  static void Optimize(duckdb::OptimizerExtensionInput& input,
                       duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
    // Pass 1: attach search filters, detect BM25/TFIDF in projections,
    // rewrite their expressions, and attach scorer to bind_data.
    // Pass 2: pull top-K limits (now scorer is set) and rewrite
    // BM25/TFIDF in Filter/TopN ORDER BY contexts (bottom-up, so by
    // the time we visit Projection the scorer may already be set).
    connector::SearchFilterOptions options{.client_context = input.context};
    {
      duckdb::Value v;
      if (input.context.TryGetCurrentSetting("sdb_scored_terms_limit", v) &&
          !v.IsNull()) {
        options.scored_terms_limit = static_cast<size_t>(v.GetValue<int32_t>());
      }
    }
    bool changed = TopDownAnnPass(plan, /*in_mutation=*/false);
    changed |= Walk(plan, plan, /*in_mutation=*/false, /*pass=*/1, options);
    changed |= Walk(plan, plan, /*in_mutation=*/false, /*pass=*/2, options);

    if (changed) {
      FlattenSwappedGets(*plan, plan);
      // ColumnLifetimeAnalyzer ran before us and populated projection_maps
      // on LogicalFilter/LogicalOrder/LogicalJoin based on the pre-swap
      // key positions. Our filter mutations + the follow-up
      // RemoveUnusedColumns will shift column_ids, leaving those maps
      // pointing at positions that no longer exist. Wipe them so
      // RemoveUnusedColumns operates on a clean slate.
      ClearProjectionMaps(*plan);
      duckdb::RemoveUnusedColumns unused{input.optimizer};
      unused.VisitOperator(plan);
    }
  }
};

}  // namespace

void RegisterIresearchPlanOptimizer(duckdb::DatabaseInstance& db) {
  duckdb::OptimizerExtension::Register(db.config, IresearchPlanOptimizer());
}

}  // namespace sdb::optimizer
