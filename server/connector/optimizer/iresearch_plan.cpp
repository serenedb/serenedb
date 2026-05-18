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
#include <duckdb/function/function_binder.hpp>
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
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/proxy_filter.hpp>

#include "basics/down_cast.h"
#include "basics/resource_manager.hpp"
#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "catalog/scorer_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_scan_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/functions/search.h"
#include "connector/functions/ts_offsets.h"
#include "connector/functions/vector.h"
#include "connector/optimizer/flatten_projection_ids.h"
#include "connector/search_filter_builder.hpp"
#include "connector/search_filter_printer.hpp"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
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

// Builds a SearchColumnContext for a LogicalGet that produces the same
// column-id space as the bind data; reused by TryAnnTopk / TryAnnRange to
// drive iresearch text-filter pushdown.
void InitSearchColumnContextForGet(
  SearchColumnContext& ctx,
  std::vector<catalog::Column::Id>& projected_ids_storage,
  const duckdb::LogicalGet& get,
  const connector::SereneDBScanBindData& bind_data,
  const ResolvedIresearch& resolved,
  std::shared_ptr<const catalog::Snapshot> snapshot) {
  constexpr auto kInvalidId = std::numeric_limits<catalog::Column::Id>::max();
  projected_ids_storage.clear();
  projected_ids_storage.reserve(get.GetColumnIds().size());
  for (const auto& ci : get.GetColumnIds()) {
    if (!ci.HasPrimaryIndex()) {
      projected_ids_storage.push_back(kInvalidId);
      continue;
    }
    const auto phys = ci.GetPrimaryIndex();
    projected_ids_storage.push_back(phys < bind_data.column_ids.size()
                                      ? bind_data.column_ids[phys]
                                      : kInvalidId);
  }
  ctx.table_index = get.table_index;
  ctx.projected_column_ids = projected_ids_storage;
  bind_data.IterateColumns(
    [&](catalog::Column::Id id, const duckdb::LogicalType& type) {
      ctx.column_type_by_id.emplace(id, type);
    });
  auto columns = resolved.index->GetColumnIds();
  ctx.indexed_column_ids.insert(columns.begin(), columns.end());
  auto index_ptr = resolved.index;
  ctx.tokenizer_provider = [index_ptr, snapshot](catalog::Column::Id col_id) {
    return index_ptr->GetColumnTokenizer(snapshot, col_id);
  };
  ctx.json_path_tokenizer_provider = [index_ptr, snapshot](
                                       catalog::Column::Id col_id,
                                       std::span<const std::string> path) {
    return index_ptr->GetJsonPathTokenizer(snapshot, col_id, path);
  };
}

// Returns a column-name lookup suitable for `irs::ToStringDemangled`.
// Falls back to "col<id>" for ids not known to the bind data; the
// fallback string lives in a thread-local so the returned string_view
// stays valid for the duration of one printer invocation.
auto MakeColumnNameLookup(const connector::SereneDBScanBindData& bind_data) {
  return [&bind_data](catalog::Column::Id col_id) -> std::string_view {
    static thread_local std::string fallback;
    auto name = bind_data.ColumnNameById(col_id);
    if (!name.empty()) {
      return name;
    }
    fallback = absl::StrCat("col", col_id);
    return fallback;
  };
}

// Try to push a single residual conjunct into `and_root` as an iresearch
// filter. Snapshots `and_root.size()` so we can roll back on failure.
bool TryClaimIresearchConjunct(
  irs::And& and_root, const duckdb::unique_ptr<duckdb::Expression>& conjunct,
  const connector::ColumnGetter& getter,
  const connector::JsonPathGetter& json_getter,
  const connector::SearchFilterOptions& options) {
  const auto before = and_root.size();
  std::span<const duckdb::unique_ptr<duckdb::Expression>> single{&conjunct, 1};
  auto r =
    connector::MakeSearchFilter(and_root, single, getter, options, json_getter);
  if (r.ok() && and_root.size() > before) {
    return true;
  }
  while (and_root.size() > before) {
    and_root.PopBack();
  }
  return false;
}

bool TryAnnTopk(duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
                const connector::SearchFilterOptions& options) {
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

  // Claim iresearch-indexable conjuncts as a cheap text filter; the rest
  // fall through to the existing row-by-row ANNFilter.
  SearchColumnContext sctx;
  std::vector<catalog::Column::Id> proj_ids_storage;
  InitSearchColumnContextForGet(sctx, proj_ids_storage, get, bind_data,
                                *resolved, snapshot);
  auto getter = MakeColumnGetter(sctx);
  auto json_getter = MakeJsonPathGetter(sctx);

  auto proxy = std::make_unique<irs::ProxyFilter>();
  auto [and_root, cache] =
    proxy->set_filter<irs::And>(irs::IResourceManager::gNoop);

  std::vector<std::vector<bool>> claimed_per_filter;
  bool any_claimed = false;
  claimed_per_filter.reserve(residual_filters.size());
  for (auto* f : residual_filters) {
    std::vector<bool> claimed(f->expressions.size(), false);
    for (size_t i = 0; i < f->expressions.size(); ++i) {
      if (TryClaimIresearchConjunct(and_root, f->expressions[i], getter,
                                    json_getter, options)) {
        claimed[i] = true;
        any_claimed = true;
      }
    }
    claimed_per_filter.emplace_back(std::move(claimed));
  }

  bool pushdown_filter = true;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> rewritten_exprs;
  std::vector<catalog::Column::Id> filter_col_ids;
  for (size_t fi = 0; fi < residual_filters.size(); ++fi) {
    auto* f = residual_filters[fi];
    for (size_t i = 0; i < f->expressions.size(); ++i) {
      if (claimed_per_filter[fi][i]) {
        continue;
      }
      auto copy = f->expressions[i]->Copy();
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
    if (any_claimed) {
      ann->text_filter_root = &and_root;
      ann->stored_text_filter = std::move(proxy);
    }
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

bool TryAnnRange(duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
                 const connector::SearchFilterOptions& options) {
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

  // Claim iresearch-indexable conjuncts as a cheap text filter; the rest
  // fall through to the existing row-by-row ANNFilter.
  SearchColumnContext sctx;
  std::vector<catalog::Column::Id> proj_ids_storage;
  InitSearchColumnContextForGet(sctx, proj_ids_storage, get, bind_data,
                                *resolved, snapshot);
  auto getter = MakeColumnGetter(sctx);
  auto json_getter = MakeJsonPathGetter(sctx);

  auto proxy = std::make_unique<irs::ProxyFilter>();
  auto [and_root, cache] =
    proxy->set_filter<irs::And>(irs::IResourceManager::gNoop);

  std::vector<bool> claimed(filter.expressions.size(), false);
  bool any_claimed = false;
  for (size_t i = 0; i < filter.expressions.size(); ++i) {
    if (TryClaimIresearchConjunct(and_root, filter.expressions[i], getter,
                                  json_getter, options)) {
      claimed[i] = true;
      any_claimed = true;
    }
  }

  bool pushdown_filter = true;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> rewritten_exprs;
  std::vector<catalog::Column::Id> filter_col_ids;
  for (size_t i = 0; i < filter.expressions.size(); ++i) {
    if (claimed[i]) {
      continue;
    }
    auto copy = filter.expressions[i]->Copy();
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
    if (any_claimed) {
      rss->text_filter_root = &and_root;
      rss->stored_text_filter = std::move(proxy);
    }
    filter.expressions.clear();
  }

  bind_data.scan_source = std::move(rss);
  get.function = connector::CreateIResearchANNRangeScanFunction();

  if (filter.expressions.empty()) {
    plan = std::move(filter.children[0]);
  }
  return true;
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

  SearchColumnContext ctx;
  std::vector<catalog::Column::Id> projected_ids;
  InitSearchColumnContextForGet(ctx, projected_ids, get, bind_data, *resolved,
                                snapshot);
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
  std::string filter_summary =
    irs::ToStringDemangled(*root, MakeColumnNameLookup(bind_data));

  auto search = std::make_unique<connector::SearchScan>();
  search->snapshot = resolved->shard->GetInvertedIndexSnapshot();
  search->stored_filter = root;
  // `Query` is built lazily in SearchFullScanInitGlobal so prepare runs
  // exactly once per execution, with the scorer if one ends up attached.
  search->filter_summary = std::move(filter_summary);
  if (resolved->index) {
    search->wand_scorer = resolved->index->GetWandScorer();
  }
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

// Build the binding an expression at `anchor_ti` should use to reference
// `target_get->column_ids[get_col_idx]`. When `anchor_ti` is the GET's
// table_index, the binding is direct. Otherwise the anchor sits on an
// intermediate LogicalProjection (DuckDB rebinds refs through one when
// ORDER BY references a value not in SELECT, or after column-lifetime
// analysis): reuse a column ref in that projection that already
// forwards the target, or inject one. Used by both the BM25/TFIDF score
// rewrite and the ts_offsets rewrite.
duckdb::ColumnBinding ExposeGetColumnAt(duckdb::LogicalOperator& root,
                                        duckdb::TableIndex anchor_ti,
                                        const duckdb::LogicalGet& target_get,
                                        duckdb::idx_t get_col_idx,
                                        std::string_view col_name,
                                        const duckdb::LogicalType& col_type) {
  if (anchor_ti == target_get.table_index) {
    return {target_get.table_index, duckdb::ProjectionIndex{get_col_idx}};
  }
  auto* proj = FindProjectionByTableIndex(root, anchor_ti);
  if (!proj) [[unlikely]] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("scan rewrite: anchor binds to table_index ",
                            static_cast<unsigned long long>(anchor_ti.index),
                            " with no matching LogicalProjection in the plan"));
  }
  for (duckdb::idx_t i = 0; i < proj->expressions.size(); ++i) {
    auto& e = *proj->expressions[i];
    if (e.type != duckdb::ExpressionType::BOUND_COLUMN_REF) {
      continue;
    }
    auto& ref = e.Cast<duckdb::BoundColumnRefExpression>();
    if (ref.binding.table_index == target_get.table_index &&
        ref.binding.column_index.GetIndex() == get_col_idx) {
      return {proj->table_index, duckdb::ProjectionIndex{i}};
    }
  }
  proj->expressions.push_back(
    duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      std::string{col_name}, col_type,
      duckdb::ColumnBinding{target_get.table_index,
                            duckdb::ProjectionIndex{get_col_idx}}));
  if (!proj->types.empty()) {
    proj->types.push_back(col_type);
  }
  return {proj->table_index,
          duckdb::ProjectionIndex{proj->expressions.size() - 1}};
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

bool TrySetScorer(std::optional<catalog::ScorerOptions>& scorer,
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
  } else if (!found->search_scan->scorer) {
    return nullptr;  // Not claimed in pass 1 -- leave for stub.
  }
  auto idx = AddScoreColumn(*found->bind_data, *found->get);
  const auto result_binding =
    ExposeGetColumnAt(root, anchor.binding.table_index, *found->get, idx,
                      catalog::Column::kScoreName, duckdb::LogicalType::FLOAT);
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
  duckdb::ColumnBinding binding,
  const connector::SereneDBScanBindData& bind_data,
  const duckdb::LogicalGet& get) {
  if (binding.table_index != get.table_index) {
    return std::numeric_limits<catalog::Column::Id>::max();
  }
  const auto col_idx = binding.column_index.GetIndex();
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

bool IsScorerFunctionName(std::string_view name) {
  using S = catalog::ScorerOptions;
  return name == S::Bm25::kName || name == S::Tfidf::kName ||
         name == S::RawTf::kName || name == S::LmJm::kName ||
         name == S::LmDirichlet::kName || name == S::IndriDirichlet::kName ||
         name == S::Dfi::kName;
}

// Returns false if args are non-constant (caller refuses to claim). Throws
// on conflicting scorer kinds; identical scorers are idempotent.
bool TrySetScorer(std::optional<catalog::ScorerOptions>& scorer,
                  const duckdb::BoundFunctionExpression& func,
                  std::string_view name) {
  auto extracted = catalog::ExtractScorerFromBound(func, name);
  if (!extracted) {
    return false;  // non-constant arg
  }
  if (!scorer) {
    scorer = std::move(*extracted);
    return true;
  }
  if (*scorer == *extracted) {
    return true;  // Idempotent.
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
    ERR_MSG("Only one scorer function is allowed per inverted index"),
    ERR_HINT("Use UNION to combine different score functions for the same "
             "inverted index"));
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

// Result of parsing and validating an ts_offsets(col [, limit]) projection
// expression. The call has already been resolved against the plan --
// scan + catalog column id are filled in, limits are validated.
struct ParsedOffsetsCall {
  FoundScan scan;
  catalog::Column::Id target_col_id;
  size_t limit;  // SIZE_MAX == unlimited (0 is translated at parse time)
  std::string col_name;
};

// Catalog column name of the column at `binding` on `get`. Falls back to
// `alias_fallback` for error messages when the scan hasn't been resolved
// or the binding is out of range.
std::string OffsetsColumnName(duckdb::ColumnBinding binding,
                              std::string_view alias_fallback,
                              const duckdb::LogicalGet* get) {
  if (get) {
    const auto& col_ids = get->GetColumnIds();
    const auto col_idx = binding.column_index.GetIndex();
    if (col_idx < col_ids.size()) {
      return get->GetColumnName(col_ids[col_idx]);
    }
  }
  return std::string{alias_fallback};
}

// Walk a column binding through LogicalProjection forwarders until it
// lands on a GET. Needed because DuckDB inserts an intermediate
// projection above TopN when ORDER BY references a value not in SELECT.
duckdb::ColumnBinding ResolveBindingToGet(duckdb::LogicalOperator& root,
                                          duckdb::ColumnBinding binding) {
  while (auto* proj = FindProjectionByTableIndex(root, binding.table_index)) {
    const auto idx = binding.column_index.GetIndex();
    if (idx >= proj->expressions.size()) {
      break;
    }
    auto& forwarded = *proj->expressions[idx];
    if (forwarded.type != duckdb::ExpressionType::BOUND_COLUMN_REF) {
      break;
    }
    binding = forwarded.Cast<duckdb::BoundColumnRefExpression>().binding;
  }
  return binding;
}

ParsedOffsetsCall ParseOffsetsCall(duckdb::BoundFunctionExpression& func,
                                   duckdb::LogicalOperator& root) {
  if (func.children[0]->expression_class !=
      duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets() first argument must be a column reference"));
  }
  const auto& col_ref =
    func.children[0]->Cast<duckdb::BoundColumnRefExpression>();
  const auto resolved = ResolveBindingToGet(root, col_ref.binding);

  // Explicit 0 means unlimited.
  constexpr size_t kDefaultOffsetsLimit = 1 << 12;
  size_t limit = kDefaultOffsetsLimit;
  if (func.children.size() == 2) {
    auto& arg1 = *func.children[1];
    if (arg1.expression_class != duckdb::ExpressionClass::BOUND_CONSTANT) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("ts_offsets() second argument must be an integer literal"));
    }
    const auto raw =
      arg1.Cast<duckdb::BoundConstantExpression>().value.GetValue<int32_t>();
    if (raw < 0) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("ts_offsets() limit must be greater than zero or 0 for no "
                "limit"));
    }
    limit =
      raw == 0 ? std::numeric_limits<size_t>::max() : static_cast<size_t>(raw);
  }

  auto found = FindSearchScanByTableIndex(root, resolved.table_index);
  if (!found) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(",
              OffsetsColumnName(resolved, col_ref.alias, nullptr),
              ") requires an inverted index scan in the same sub-query"));
  }

  const auto col_name = OffsetsColumnName(resolved, col_ref.alias, found->get);
  if (!found->bind_data->IsInvertedIndexEntry()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(", col_name,
              ") requires an inverted index scan in the same sub-query"));
  }

  const auto target_col_id =
    ResolveColumnId(resolved, *found->bind_data, *found->get);
  if (target_col_id == std::numeric_limits<catalog::Column::Id>::max()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(): column '", col_name, "' not found in table"));
  }
  const auto& idx_col_ids = found->bind_data->inverted_index->GetColumnIds();
  const bool in_index =
    absl::c_find(idx_col_ids, target_col_id) != idx_col_ids.end();
  if (!in_index) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(): column '", col_name, "' not found in index"));
  }

  return ParsedOffsetsCall{.scan = *found,
                           .target_col_id = target_col_id,
                           .limit = limit,
                           .col_name = col_name};
}

duckdb::unique_ptr<duckdb::Expression> RewriteOffsetsCall(
  duckdb::Expression& expr, duckdb::LogicalOperator& root) {
  if (expr.expression_class != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return nullptr;
  }
  auto& func = expr.Cast<duckdb::BoundFunctionExpression>();
  if (func.function.name != connector::kOffsets) {
    return nullptr;
  }
  if (func.children.size() != 1 && func.children.size() != 2) {
    return nullptr;
  }

  auto parsed = ParseOffsetsCall(func, root);

  const auto* col_info =
    parsed.scan.bind_data->inverted_index->FindColumnInfo(parsed.target_col_id);
  const bool is_text = col_info && col_info->text_dictionary.isSet();
  const bool offs_stored =
    col_info && col_info->features.HasFeatures(irs::IndexFeatures::Offs);

  if (is_text && !offs_stored) {
    auto bind = duckdb::make_uniq<connector::OffsetsBindData>();
    bind->inverted_index = parsed.scan.bind_data->inverted_index;
    bind->column_id = parsed.target_col_id;
    bind->limit = parsed.limit;
    bind->stored_filter = parsed.scan.search_scan->stored_filter;
    func.bind_info = std::move(bind);
    func.function.function = connector::OffsetsScalarFn;
    auto body_expr = std::move(func.children[0]);
    func.children.clear();
    func.children.emplace_back(
      duckdb::make_uniq<duckdb::BoundConstantExpression>(
        duckdb::Value{std::string{}}));
    func.children.emplace_back(std::move(body_expr));
    return nullptr;
  }

  duckdb::idx_t get_col_idx;
  bool reused = false;
  for (const auto& req : parsed.scan.search_scan->offsets) {
    if (req.column_id != parsed.target_col_id) {
      continue;
    }
    if (req.limit != parsed.limit) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("ts_offsets() called multiple times for field '",
                              parsed.col_name, "' with different limits"));
    }
    get_col_idx = req.get_col_idx;
    reused = true;
    break;
  }

  if (!reused) {
    get_col_idx = AddOffsetsColumn(*parsed.scan.bind_data, *parsed.scan.get,
                                   parsed.target_col_id);
    parsed.scan.search_scan->offsets.push_back(
      {.column_id = parsed.target_col_id,
       .limit = parsed.limit,
       .get_col_idx = get_col_idx});
  }
  const auto col_name = catalog::Column::MakeOffsetsName(parsed.target_col_id);
  const auto col_type = catalog::Column::MakeOffsetsType();
  auto& col_ref = func.children[0]->Cast<duckdb::BoundColumnRefExpression>();
  const auto binding =
    ExposeGetColumnAt(root, col_ref.binding.table_index, *parsed.scan.get,
                      get_col_idx, col_name, col_type);
  auto col = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    col_name, col_type, binding);
  col->alias = expr.alias;
  return col;
}

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

bool TryAttachScoreOffsets(duckdb::LogicalOperator& root,
                           duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
                           duckdb::ClientContext& /*context*/) {
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
bool TryAttachScoreTopK(duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
                        const connector::SearchFilterOptions& options) {
  if (options.disable_top_k_optimization) {
    return false;
  }
  if (plan->type != duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
    return false;
  }
  auto found = FindSearchScanChild(*plan);
  if (!found) {
    return false;
  }
  if (!found->search_scan->scorer) {
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
      if (search.scorer.has_value()) {
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
      if (TryAnnTopk(plan, options)) {
        return true;
      }
      bool changed = TryAttachScoreTopK(plan, options);
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
      if (TryAnnRange(plan, options)) {
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
      return TryAttachScoreOffsets(*root, plan, options.client_context);
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
    duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
    const connector::SearchFilterOptions& options) {
    if (plan->type == duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
      bool changed = TryAttachScoreTopK(plan, options);
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
                             bool in_mutation,
                             const connector::SearchFilterOptions& options) {
    const bool subtree_in_mutation = in_mutation || IsMutationOp(plan->type);
    bool changed = false;
    if (!subtree_in_mutation) {
      if (plan->type == duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
        if (TryAnnTopk(plan, options)) {
          changed = true;
        }
      } else if (plan->type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
        if (TryAnnRange(plan, options)) {
          changed = true;
        }
      }
    }
    for (auto& child : plan->children) {
      changed |= TopDownAnnPass(child, subtree_in_mutation, options);
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
        changed |= TryOptimizePass2(root, plan, options);
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
      if (input.context.TryGetCurrentSetting("sdb_disable_top_k_optimization",
                                             v) &&
          !v.IsNull()) {
        options.disable_top_k_optimization = v.GetValue<bool>();
      }
    }
    bool changed = TopDownAnnPass(plan, /*in_mutation=*/false, options);
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
