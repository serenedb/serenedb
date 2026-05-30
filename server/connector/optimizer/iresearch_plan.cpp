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

#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/function/aggregate/distributive_functions.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
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
#include "connector/index_expression.hpp"
#include "connector/search_field_name.hpp"
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

std::shared_ptr<const catalog::Snapshot> PinnedSnapshot(
  const connector::SearchFilterOptions& options) {
  return connector::GetSereneDBContext(options.client_context)
    .EnsureCatalogSnapshot();
}

search::InvertedIndexSnapshotPtr PinnedSearchSnapshot(
  const connector::SearchFilterOptions& options, ObjectId index_id) {
  return connector::GetSereneDBContext(options.client_context)
    .EnsureSearchSnapshot(index_id);
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

  if (auto shard = snapshot.GetIndexShard(out.index->GetId());
      shard && shard->GetType() == catalog::ObjectType::InvertedIndexShard) {
    out.shard = basics::downCast<search::InvertedIndexShard>(std::move(shard));
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

struct ExpectedHNSW {
  irs::HNSWMetric metric;
  duckdb::OrderType order;
  bool is_norm;
};

// Geo guard: `<->` / `<+>` are also registered with JSON/GEOMETRY overloads
// in search.cpp; an ARRAY child[0] proves the vector overload.
std::optional<ExpectedHNSW> ExpectedHNSWForFunction(
  const duckdb::BoundFunctionExpression& func) {
  const auto& name = func.function.GetName();
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
  const auto& arg_type = func.children[0]->GetReturnType();
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

bool TryExtractQueryVector(const duckdb::Value& val, std::vector<float>& out) {
  using duckdb::LogicalTypeId;
  const std::vector<duckdb::Value>* children_ptr = nullptr;
  if (val.type().id() == LogicalTypeId::ARRAY) {
    children_ptr = &duckdb::ArrayValue::GetChildren(val);
  } else if (val.type().id() == LogicalTypeId::LIST) {
    children_ptr = &duckdb::ListValue::GetChildren(val);
  } else {
    return false;
  }
  const auto& children = *children_ptr;
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

struct DistanceArgs {
  duckdb::Expression* col_arg = nullptr;
  duckdb::Expression* value_arg = nullptr;
};
DistanceArgs ExtractDistanceArgs(duckdb::BoundFunctionExpression& func_expr) {
  DistanceArgs out;
  if (func_expr.children.size() != 2) {
    return out;
  }
  auto& lhs = func_expr.children[0];
  auto& rhs = func_expr.children[1];
  // Foldable means can be evaluated at plan time, for example a constant
  if (!lhs->IsFoldable() && rhs->IsFoldable()) {
    out.col_arg = lhs.get();
    out.value_arg = rhs.get();
  } else if (lhs->IsFoldable() && !rhs->IsFoldable()) {
    out.col_arg = rhs.get();
    out.value_arg = lhs.get();
  }
  return out;
}

bool TryFoldExpression(duckdb::ClientContext& context, duckdb::Expression& expr,
                       duckdb::Value& out) {
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONSTANT) {
    out = expr.Cast<duckdb::BoundConstantExpression>().value;
    return !out.IsNull();
  }
  if (!expr.IsFoldable()) {
    return false;
  }
  if (!duckdb::ExpressionExecutor::TryEvaluateScalar(context, expr, out)) {
    return false;
  }
  return !out.IsNull();
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
      if (child->GetExpressionClass() ==
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
          ref.GetReturnType(), slot);
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
  duckdb::ClientContext* client_context = nullptr;
  std::span<const catalog::Column::Id> projected_column_ids;
  containers::FlatHashMap<catalog::Column::Id, duckdb::LogicalType>
    column_type_by_id;
  containers::FlatHashSet<catalog::Column::Id> indexed_column_ids;
  struct IndexedExpressionMeta {
    duckdb::LogicalType return_type;
    irs::field_id field_id = 0;
  };
  containers::FlatHashMap<std::string_view, IndexedExpressionMeta>
    indexed_expressions;
  std::function<catalog::ColumnTokenizer(catalog::Column::Id)>
    tokenizer_provider;
  std::function<catalog::ColumnTokenizer(std::string_view)>
    expr_tokenizer_provider;
  std::function<bool(catalog::Column::Id)> has_postings_provider;
  ObjectId relation_id;
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
    if (col_id == catalog::Column::kInvalidId) {
      return std::nullopt;
    }
    if (!ctx.indexed_column_ids.contains(col_id)) {
      return std::nullopt;
    }
    if (ctx.has_postings_provider && !ctx.has_postings_provider(col_id)) {
      return std::nullopt;
    }
    auto type_it = ctx.column_type_by_id.find(col_id);
    if (type_it == ctx.column_type_by_id.end()) {
      return std::nullopt;
    }
    connector::SearchColumnInfo info;
    info.field_id = static_cast<irs::field_id>(col_id);
    info.logical_type = type_it->second;
    info.tokenizer = ctx.tokenizer_provider(col_id);
    return info;
  };
}

// True iff at least one column ref was found and all bind to `table_index`.
bool AllColumnRefsBindTo(const duckdb::Expression& expr,
                         duckdb::TableIndex table_index, bool& any_seen) {
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    any_seen = true;
    return expr.Cast<duckdb::BoundColumnRefExpression>().binding.table_index ==
           table_index;
  }
  bool ok = true;
  duckdb::ExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::Expression& child) {
      if (ok && !AllColumnRefsBindTo(child, table_index, any_seen)) {
        ok = false;
      }
    });
  return ok;
}

irs::field_id ResolveAnnTargetFieldId(
  const duckdb::Expression& col_arg, const duckdb::LogicalGet& get,
  const connector::SereneDBScanBindData& bind_data,
  const catalog::InvertedIndex& index, duckdb::ClientContext& client_context) {
  if (col_arg.GetExpressionClass() ==
        duckdb::ExpressionClass::BOUND_COLUMN_REF ||
      col_arg.GetExpressionClass() == duckdb::ExpressionClass::BOUND_REF) {
    if (const auto id = ColumnIdByName(bind_data, col_arg.GetName());
        id != catalog::Column::kInvalidId) {
      return static_cast<irs::field_id>(id);
    }
  }
  bool any_ref_seen = false;
  if (!AllColumnRefsBindTo(col_arg, get.table_index, any_ref_seen) ||
      !any_ref_seen) {
    return 0;
  }
  constexpr auto kInvalidId = catalog::Column::kInvalidId;
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
  auto normalized = connector::NormalizeBoundExpression(
    col_arg, index.GetRelationId(), projected_ids, client_context);
  auto serialized = connector::SerializeBoundExpression(*normalized);
  return index.FindFieldIdBySerialized(serialized).value_or(0);
}

connector::ExpressionGetter MakeExpressionGetter(SearchColumnContext& ctx) {
  return [&ctx](const duckdb::Expression& expr)
           -> std::optional<connector::SearchColumnInfo> {
    if (!ctx.expr_tokenizer_provider || ctx.client_context == nullptr) {
      return std::nullopt;
    }
    bool any_ref_seen = false;
    if (!AllColumnRefsBindTo(expr, ctx.table_index, any_ref_seen) ||
        !any_ref_seen) {
      return std::nullopt;
    }
    auto normalized = connector::NormalizeBoundExpression(
      expr, ctx.relation_id, ctx.projected_column_ids, *ctx.client_context);
    auto serialized = connector::SerializeBoundExpression(*normalized);
    auto entry = ctx.indexed_expressions.find(serialized);
    if (entry == ctx.indexed_expressions.end()) {
      return std::nullopt;
    }
    connector::SearchColumnInfo info;
    info.field_id = entry->second.field_id;
    info.logical_type = entry->second.return_type;
    info.tokenizer = ctx.expr_tokenizer_provider(serialized);
    return info;
  };
}

void InitSearchColumnContextForGet(
  SearchColumnContext& ctx,
  std::vector<catalog::Column::Id>& projected_ids_storage,
  const duckdb::LogicalGet& get,
  const connector::SereneDBScanBindData& bind_data,
  const ResolvedIresearch& resolved,
  std::shared_ptr<const catalog::Snapshot> snapshot) {
  constexpr auto kInvalidId = catalog::Column::kInvalidId;
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
  ctx.relation_id = resolved.index->GetRelationId();
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
  ctx.has_postings_provider = [index_ptr](catalog::Column::Id col_id) {
    const auto* info = index_ptr->FindColumnInfo(col_id);
    if (info == nullptr) {
      return false;
    }
    return !info->store_values || info->text_dictionary.isSet();
  };
  ctx.expr_tokenizer_provider = [index_ptr,
                                 snapshot](std::string_view serialized_expr) {
    return index_ptr->GetExprTokenizer(snapshot, serialized_expr);
  };
  for (const auto& [field_id, entry] : resolved.index->GetEntries()) {
    const auto* expr = entry.GetExpressionData();
    if (!expr) {
      continue;
    }
    ctx.indexed_expressions.emplace(expr->serialized_expr,
                                    SearchColumnContext::IndexedExpressionMeta{
                                      .return_type = expr->return_type,
                                      .field_id = field_id,
                                    });
  }
}

auto MakeColumnNameLookup(const connector::SereneDBScanBindData& bind_data,
                          const catalog::InvertedIndex& index) {
  return [&](catalog::Column::Id col_id) {
    auto name = bind_data.ColumnNameById(col_id);
    if (!name.empty()) {
      return std::string{name};
    }
    if (const auto* entry =
          index.FindEntry(static_cast<irs::field_id>(col_id))) {
      if (const auto* expr = entry->GetExpressionData()) {
        if (!expr->pretty_printed.empty()) {
          return expr->pretty_printed;
        }
      }
    }
    return absl::StrCat("col", col_id);
  };
}

// Try to push a single residual conjunct into `and_root` as an iresearch
// filter. Snapshots `and_root.size()` so we can roll back on failure.
bool TryClaimIresearchConjunct(
  irs::And& and_root, const duckdb::unique_ptr<duckdb::Expression>& conjunct,
  const connector::ColumnGetter& getter,
  const connector::ExpressionGetter& expr_getter,
  const connector::SearchFilterOptions& options) {
  const auto before = and_root.size();
  std::span<const duckdb::unique_ptr<duckdb::Expression>> single{&conjunct, 1};
  auto r =
    connector::MakeSearchFilter(and_root, single, getter, options, expr_getter);
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
  // Matches both LogicalTopN (post-#27 fusion) and pre-fusion
  // LogicalLimit > LogicalOrder. We run before #18 so UNUSED_COLUMNS prunes
  // the stranded distance() expression from the projection below.
  duckdb::idx_t limit = 0;
  duckdb::OrderType order_type = duckdb::OrderType::ASCENDING;
  duckdb::Expression* order_expr = nullptr;
  duckdb::unique_ptr<duckdb::LogicalOperator>* child_slot = nullptr;

  if (plan->type == duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
    auto& top_n = plan->Cast<duckdb::LogicalTopN>();
    if (top_n.limit == 0 || top_n.offset != 0 || top_n.orders.size() != 1 ||
        top_n.orders[0].type == duckdb::OrderType::INVALID) {
      return false;
    }
    limit = static_cast<duckdb::idx_t>(top_n.limit);
    order_type = top_n.orders[0].type == duckdb::OrderType::ORDER_DEFAULT
                   ? duckdb::OrderType::ASCENDING
                   : top_n.orders[0].type;
    order_expr = top_n.orders[0].expression.get();
    child_slot = &top_n.children[0];
  } else if (plan->type == duckdb::LogicalOperatorType::LOGICAL_LIMIT) {
    auto& limit_op = plan->Cast<duckdb::LogicalLimit>();
    if (limit_op.limit_val.Type() != duckdb::LimitNodeType::CONSTANT_VALUE) {
      return false;
    }
    if (limit_op.offset_val.Type() != duckdb::LimitNodeType::UNSET &&
        !(limit_op.offset_val.Type() == duckdb::LimitNodeType::CONSTANT_VALUE &&
          limit_op.offset_val.GetConstantValue() == 0)) {
      return false;
    }
    if (limit_op.children.size() != 1 ||
        limit_op.children[0]->type !=
          duckdb::LogicalOperatorType::LOGICAL_ORDER_BY) {
      return false;
    }
    auto& order_op = limit_op.children[0]->Cast<duckdb::LogicalOrder>();
    if (order_op.orders.size() != 1 ||
        order_op.orders[0].type == duckdb::OrderType::INVALID) {
      return false;
    }
    limit = limit_op.limit_val.GetConstantValue();
    if (limit == 0) {
      return false;
    }
    order_type = order_op.orders[0].type == duckdb::OrderType::ORDER_DEFAULT
                   ? duckdb::OrderType::ASCENDING
                   : order_op.orders[0].type;
    order_expr = order_op.orders[0].expression.get();
    child_slot = &order_op.children[0];
  } else {
    return false;
  }

  if (order_expr->GetExpressionType() !=
      duckdb::ExpressionType::BOUND_COLUMN_REF) {
    return false;
  }
  auto& order_col_ref = order_expr->Cast<duckdb::BoundColumnRefExpression>();

  if ((*child_slot)->type != duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    return false;
  }
  auto& projection = (*child_slot)->Cast<duckdb::LogicalProjection>();

  const auto proj_col_idx = order_col_ref.binding.column_index;
  if (proj_col_idx >= projection.expressions.size()) {
    return false;
  }
  auto& dist_expr = *projection.expressions[proj_col_idx];
  if (dist_expr.GetExpressionClass() !=
      duckdb::ExpressionClass::BOUND_FUNCTION) {
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
    if (child->IsFoldable()) {
      return false;
    }
    col_arg = child;
  } else {
    auto args = ExtractDistanceArgs(func_expr);
    if (!args.col_arg || !args.value_arg) {
      return false;
    }
    duckdb::Value folded;
    if (!TryFoldExpression(options.client_context, *args.value_arg, folded)) {
      return false;
    }
    if (!TryExtractQueryVector(folded, query_vector)) {
      return false;
    }
    col_arg = args.col_arg;
  }

  auto snapshot = PinnedSnapshot(options);
  auto resolved = ResolveIresearch(bind_data, *snapshot);
  if (!resolved) {
    return false;
  }

  const auto field_id = ResolveAnnTargetFieldId(
    *col_arg, get, bind_data, *resolved->index, options.client_context);
  if (field_id == 0) {
    return false;
  }

  auto hnsw_info = resolved->index->GetHNSWInfo(field_id);
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
  ann->snapshot = PinnedSearchSnapshot(options, resolved->index->GetId());
  ann->field_id = field_id;
  ann->query_vector = std::move(query_vector);
  ann->top_k = limit;

  // Indexable conjuncts -> cheap text filter; rest -> row-by-row ANNFilter.
  SearchColumnContext sctx;
  std::vector<catalog::Column::Id> proj_ids_storage;
  InitSearchColumnContextForGet(sctx, proj_ids_storage, get, bind_data,
                                *resolved, snapshot);
  sctx.client_context = &options.client_context;
  auto getter = MakeColumnGetter(sctx);

  auto expr_getter = MakeExpressionGetter(sctx);

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
                                    expr_getter, options)) {
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
  plan = std::move(*child_slot);
  return true;
}

bool TryClaimAnnRange(
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  duckdb::LogicalGet& get, connector::SereneDBScanBindData& bind_data,
  const ResolvedIresearch& resolved,
  std::shared_ptr<const catalog::Snapshot> snapshot,
  const connector::SearchFilterOptions& options) {
  duckdb::idx_t match_idx = duckdb::DConstants::INVALID_INDEX;
  float radius = 0.0f;
  bool radius_needs_square = false;
  std::vector<float> query_vector;
  irs::field_id field_id = 0;

  for (duckdb::idx_t i = 0; i < filters.size(); ++i) {
    auto& expr = *filters[i];
    if (!duckdb::BoundComparisonExpression::IsComparison(expr)) {
      continue;
    }
    auto& cmp = expr.Cast<duckdb::BoundFunctionExpression>();
    auto& cmp_left = duckdb::BoundComparisonExpression::LeftMutable(cmp);
    auto& cmp_right = duckdb::BoundComparisonExpression::RightMutable(cmp);
    duckdb::Expression* func_side = nullptr;
    duckdb::Expression* const_side = nullptr;
    switch (cmp.GetExpressionType()) {
      case duckdb::ExpressionType::COMPARE_LESSTHAN:
      case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
        func_side = cmp_left.get();
        const_side = cmp_right.get();
        break;
      case duckdb::ExpressionType::COMPARE_GREATERTHAN:
      case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
        func_side = cmp_right.get();
        const_side = cmp_left.get();
        break;
      default:
        continue;
    }
    if (func_side->GetExpressionClass() !=
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
    duckdb::Value radius_value;
    if (!TryFoldExpression(options.client_context, *const_side, radius_value)) {
      continue;
    }
    float candidate_radius = 0.0f;
    switch (radius_value.type().id()) {
      case duckdb::LogicalTypeId::FLOAT:
        candidate_radius = radius_value.GetValue<float>();
        break;
      case duckdb::LogicalTypeId::DOUBLE:
        candidate_radius = static_cast<float>(radius_value.GetValue<double>());
        break;
      default:
        continue;
    }
    duckdb::Expression* col_expr = nullptr;
    std::vector<float> candidate_vector;
    if (is_norm) {
      auto* child = func.children[0].get();
      if (child->IsFoldable()) {
        continue;
      }
      col_expr = child;
    } else {
      auto args = ExtractDistanceArgs(func);
      if (!args.col_arg || !args.value_arg) {
        continue;
      }
      duckdb::Value folded;
      if (!TryFoldExpression(options.client_context, *args.value_arg, folded)) {
        continue;
      }
      if (!TryExtractQueryVector(folded, candidate_vector)) {
        continue;
      }
      col_expr = args.col_arg;
    }
    const auto candidate_field_id = ResolveAnnTargetFieldId(
      *col_expr, get, bind_data, *resolved.index, options.client_context);
    if (candidate_field_id == 0) {
      continue;
    }
    auto hnsw_info = resolved.index->GetHNSWInfo(candidate_field_id);
    if (!hnsw_info || hnsw_info->metric != expected_current->metric) {
      continue;
    }
    if (is_norm) {
      candidate_vector.assign(hnsw_info->d, 0.0f);
    } else if (static_cast<size_t>(hnsw_info->d) != candidate_vector.size()) {
      continue;
    }

    radius = candidate_radius;
    // Index stores L2-squared. l2_distance / `<->` / l2_norm callers need
    // the radius pre-squared; l2_sqr_distance and other metrics don't.
    radius_needs_square = func.function.GetName() == connector::kL2Distance ||
                          func.function.GetName() == connector::kL2DistanceOp ||
                          func.function.GetName() == connector::kL2Norm;
    query_vector = std::move(candidate_vector);
    field_id = candidate_field_id;
    match_idx = i;
    break;
  }

  if (match_idx == duckdb::DConstants::INVALID_INDEX) {
    return false;
  }

  auto rss = std::make_unique<connector::RangeSearchScan>();
  rss->snapshot = PinnedSearchSnapshot(options, resolved.index->GetId());
  rss->field_id = field_id;
  rss->query_vector = std::move(query_vector);
  rss->radius = radius;
  rss->effective_radius = radius_needs_square ? radius * radius : radius;

  filters.erase(filters.begin() + match_idx);

  // Indexable conjuncts -> cheap text filter; rest -> row-by-row ANNFilter.
  SearchColumnContext sctx;
  std::vector<catalog::Column::Id> proj_ids_storage;
  InitSearchColumnContextForGet(sctx, proj_ids_storage, get, bind_data,
                                resolved, snapshot);
  sctx.client_context = &options.client_context;
  auto getter = MakeColumnGetter(sctx);

  auto expr_getter = MakeExpressionGetter(sctx);

  auto proxy = std::make_unique<irs::ProxyFilter>();
  auto [and_root, cache] =
    proxy->set_filter<irs::And>(irs::IResourceManager::gNoop);

  std::vector<bool> claimed(filters.size(), false);
  bool any_claimed = false;
  for (size_t i = 0; i < filters.size(); ++i) {
    if (TryClaimIresearchConjunct(and_root, filters[i], getter, expr_getter,
                                  options)) {
      claimed[i] = true;
      any_claimed = true;
    }
  }

  bool pushdown_filter = true;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> rewritten_exprs;
  std::vector<catalog::Column::Id> filter_col_ids;
  for (size_t i = 0; i < filters.size(); ++i) {
    if (claimed[i]) {
      continue;
    }
    auto copy = filters[i]->Copy();
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
    filters.clear();
  }

  bind_data.scan_source = std::move(rss);
  get.function = connector::CreateIResearchANNRangeScanFunction();
  return true;
}

bool TryClaimSearchFilter(
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  duckdb::LogicalGet& get, connector::SereneDBScanBindData& bind_data,
  const ResolvedIresearch& resolved,
  std::shared_ptr<const catalog::Snapshot> snapshot,
  const connector::SearchFilterOptions& options) {
  SearchColumnContext ctx;
  std::vector<catalog::Column::Id> projected_ids;
  InitSearchColumnContextForGet(ctx, projected_ids, get, bind_data, resolved,
                                snapshot);
  ctx.client_context = &options.client_context;
  auto getter = MakeColumnGetter(ctx);

  auto expr_getter = MakeExpressionGetter(ctx);

  auto root = std::make_shared<irs::And>();
  std::vector<size_t> claimed_indices;
  for (size_t i = 0; i < filters.size(); ++i) {
    if (TryClaimIresearchConjunct(*root, filters[i], getter, expr_getter,
                                  options)) {
      claimed_indices.push_back(i);
    }
  }
  if (claimed_indices.empty()) {
    return false;
  }

  // Capture summary BEFORE preparing -- prepare consumes the tree.
  std::string filter_summary = irs::ToStringDemangled(
    *root, MakeColumnNameLookup(bind_data, *resolved.index));

  auto search = std::make_unique<connector::SearchScan>();
  search->snapshot = PinnedSearchSnapshot(options, resolved.index->GetId());
  search->stored_filter = root;
  search->filter_summary = std::move(filter_summary);
  if (resolved.index) {
    search->topk_scorer = resolved.index->GetTopKScorer();
  }
  bind_data.scan_source = std::move(search);
  get.function = connector::CreateIResearchScanFunction();

  // Erase highest-index first so earlier indices stay valid.
  for (auto it = claimed_indices.rbegin(); it != claimed_indices.rend(); ++it) {
    filters.erase(filters.begin() + static_cast<std::ptrdiff_t>(*it));
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
    return FindSearchScanChild(child);
  }
  return std::nullopt;
}

// Unlike FindSearchScanChild, traverses multi-child nodes (cross joins, etc.)
// for multi-index query support.
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

// When `anchor_ti` != get.table_index, the anchor sits on an intermediate
// projection (DuckDB rebinds refs through one for out-of-SELECT ORDER BY).
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
    if (e.GetExpressionType() != duckdb::ExpressionType::BOUND_COLUMN_REF) {
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
      if (e.GetExpressionType() != duckdb::ExpressionType::BOUND_COLUMN_REF) {
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

// Idempotent. Returns the position in `get.column_ids`.
duckdb::idx_t AddScoreColumn(connector::SereneDBScanBindData& bind_data,
                             duckdb::LogicalGet& get) {
  auto score_bind_idx = duckdb::DConstants::INVALID_INDEX;
  for (duckdb::idx_t i = 0; i < bind_data.column_ids.size(); ++i) {
    if (bind_data.column_ids[i] == catalog::Column::kInvertedIndexScoreId) {
      score_bind_idx = i;
      break;
    }
  }
  if (score_bind_idx == duckdb::DConstants::INVALID_INDEX) {
    score_bind_idx = bind_data.column_ids.size();
    bind_data.column_ids.push_back(catalog::Column::kInvertedIndexScoreId);
    bind_data.column_types.push_back(duckdb::LogicalType::FLOAT);
  }
  auto& col_ids = get.GetColumnIds();
  for (duckdb::idx_t i = 0; i < col_ids.size(); ++i) {
    if (col_ids[i].HasPrimaryIndex() &&
        col_ids[i].GetPrimaryIndex() == score_bind_idx) {
      return i;
    }
  }
  // returned_types must be a valid index space for score_bind_idx; extend
  // it first so the AddColumnId below has somewhere to point.
  get.returned_types.push_back(duckdb::LogicalType::FLOAT);
  get.names.emplace_back(catalog::Column::kScoreName);
  const auto get_col_idx = col_ids.size();
  const auto proj_idx =
    get.AddColumnId(static_cast<duckdb::column_t>(score_bind_idx));
  if (!get.projection_ids.empty()) {
    get.projection_ids.push_back(proj_idx);
  }
  get.types.push_back(duckdb::LogicalType::FLOAT);
  return get_col_idx;
}

bool TrySetScorer(std::optional<catalog::ScorerOptions>& scorer,
                  const duckdb::BoundFunctionExpression& func,
                  std::string_view name);

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
  switch (expr->GetExpressionClass()) {
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
  if (expr->GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
    if (expr->GetExpressionClass() ==
        duckdb::ExpressionClass::BOUND_COLUMN_REF) {
      auto& ref = expr->Cast<duckdb::BoundColumnRefExpression>();
      if (!ref.GetAlias().empty() &&
          ref.GetAlias() != catalog::Column::kScoreName &&
          BindingIsScoreColumn(root, ref.binding)) {
        ref.SetAlias(std::string{catalog::Column::kScoreName});
        changed = true;
      }
    }
    RewriteScoreCallInChildren(expr, root, changed, set_scorer);
    return nullptr;
  }
  auto& func = expr->Cast<duckdb::BoundFunctionExpression>();
  const auto& name = func.function.GetName();
  if (!IsScorerFunctionName(name)) {
    RewriteScoreCallInChildren(expr, root, changed, set_scorer);
    return nullptr;
  }
  // Scorer call -- check tableoid argument.
  if (func.children.empty() || func.children[0]->GetExpressionClass() !=
                                 duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return nullptr;
  }
  auto& anchor = func.children[0]->Cast<duckdb::BoundColumnRefExpression>();
  auto found = FindSearchScanByTableIndex(root, anchor.binding.table_index);
  if (!found) {
    return nullptr;
  }
  // Score only when user wrote `FROM <idx_name>`. `FROM <table>` queries
  // get a SearchScan opportunistically but never requested scoring.
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

// Returns kInvalidId if the binding doesn't belong to `get`.
catalog::Column::Id ResolveColumnId(
  duckdb::ColumnBinding binding,
  const connector::SereneDBScanBindData& bind_data,
  const duckdb::LogicalGet& get) {
  if (binding.table_index != get.table_index) {
    return catalog::Column::kInvalidId;
  }
  const auto col_idx = binding.column_index.GetIndex();
  const auto& column_ids = get.GetColumnIds();
  if (col_idx >= column_ids.size()) {
    return catalog::Column::kInvalidId;
  }
  if (!column_ids[col_idx].HasPrimaryIndex()) {
    return catalog::Column::kInvalidId;
  }
  const auto phys = column_ids[col_idx].GetPrimaryIndex();
  if (phys >= bind_data.column_ids.size()) {
    return catalog::Column::kInvalidId;
  }
  return bind_data.column_ids[phys];
}

bool IsScorerFunctionName(std::string_view name) {
  using S = catalog::ScorerOptions;
  // TODO(mbkkt) make TrivialBiSet?
  return name == S::Bm25::kName || name == S::Tfidf::kName ||
         name == S::LmJm::kName || name == S::LmDirichlet::kName ||
         name == S::IndriDirichlet::kName || name == S::Dfi::kName ||
         name == S::RawBoost::kName || name == S::RawTf::kName ||
         name == S::RawDL::kName;
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
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return false;
  }
  const auto& func = expr.Cast<duckdb::BoundFunctionExpression>();
  const auto& name = func.function.GetName();
  if (!IsScorerFunctionName(name)) {
    return false;
  }
  if (func.children.empty() || func.children[0]->GetExpressionClass() !=
                                 duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return false;
  }
  const auto& anchor =
    func.children[0]->Cast<duckdb::BoundColumnRefExpression>();
  return FindSearchScanByTableIndex(root, anchor.binding.table_index)
    .has_value();
}

// `score_fn(tableoid) > 0` -> constant TRUE: scorer outputs are positive
// for matching docs, comparison drops. Recurses through Cast/Operator/
// Comparison wrappers.
bool SimplifyScoreGtZero(duckdb::LogicalOperator& root,
                         duckdb::unique_ptr<duckdb::Expression>& expr) {
  if (!expr) {
    return false;
  }
  using EC = duckdb::ExpressionClass;
  bool changed = false;
  if (duckdb::BoundComparisonExpression::IsComparison(*expr)) {
    auto& cmp = expr->Cast<duckdb::BoundFunctionExpression>();
    auto& cmp_left = duckdb::BoundComparisonExpression::LeftMutable(cmp);
    auto& cmp_right = duckdb::BoundComparisonExpression::RightMutable(cmp);
    auto is_zero = [](const duckdb::Expression& e) {
      if (e.GetExpressionClass() != EC::BOUND_CONSTANT) {
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
    const auto cmp_type = cmp.GetExpressionType();
    // Matches `score > 0`, `score >= 0` (non-negative comparisons).
    const bool is_gt_zero_shape =
      (cmp_type == duckdb::ExpressionType::COMPARE_GREATERTHAN ||
       cmp_type == duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO) &&
      cmp_left && cmp_right && is_zero(*cmp_right) &&
      IsScorerCallAnchoredOnSearchScan(root, *cmp_left);
    // Matches `0 < score`.
    const bool is_zero_lt_shape =
      (cmp_type == duckdb::ExpressionType::COMPARE_LESSTHAN ||
       cmp_type == duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO) &&
      cmp_left && cmp_right && is_zero(*cmp_left) &&
      IsScorerCallAnchoredOnSearchScan(root, *cmp_right);
    if (is_gt_zero_shape || is_zero_lt_shape) {
      expr = duckdb::make_uniq<duckdb::BoundConstantExpression>(
        duckdb::Value::BOOLEAN(true));
      return true;
    }
  }
  // Recurse into structural children so nested filter predicates are caught.
  switch (expr->GetExpressionClass()) {
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

// Falls back to `alias_fallback` for error messages when the scan hasn't
// been resolved or the binding is out of range.
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

// Needed because DuckDB inserts an intermediate projection above TopN
// when ORDER BY references a value not in SELECT.
duckdb::ColumnBinding ResolveBindingToGet(duckdb::LogicalOperator& root,
                                          duckdb::ColumnBinding binding) {
  while (auto* proj = FindProjectionByTableIndex(root, binding.table_index)) {
    const auto idx = binding.column_index.GetIndex();
    if (idx >= proj->expressions.size()) {
      break;
    }
    auto& forwarded = *proj->expressions[idx];
    if (forwarded.GetExpressionType() !=
        duckdb::ExpressionType::BOUND_COLUMN_REF) {
      break;
    }
    binding = forwarded.Cast<duckdb::BoundColumnRefExpression>().binding;
  }
  return binding;
}

ParsedOffsetsCall ParseOffsetsCall(duckdb::BoundFunctionExpression& func,
                                   duckdb::LogicalOperator& root) {
  if (func.children[0]->GetExpressionClass() !=
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
    if (arg1.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
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
              OffsetsColumnName(resolved, col_ref.GetAlias(), nullptr),
              ") requires an inverted index scan in the same sub-query"));
  }

  // Require FROM <idx_name>. ts_offsets() on a base-table scan that was
  // opportunistically promoted to a SearchScan is not supported.
  const auto col_name =
    OffsetsColumnName(resolved, col_ref.GetAlias(), found->get);
  if (!found->bind_data->IsInvertedIndexEntry()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(", col_name,
              ") requires an inverted index scan in the same sub-query"));
  }

  const auto target_col_id =
    ResolveColumnId(resolved, *found->bind_data, *found->get);
  if (target_col_id == catalog::Column::kInvalidId) {
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
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return nullptr;
  }
  auto& func = expr.Cast<duckdb::BoundFunctionExpression>();
  if (func.function.GetName() != connector::kOffsets) {
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
    func.function.SetFunctionCallback(connector::OffsetsScalarFn);
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
  col->SetAlias(expr.GetAlias());
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
  // ExpressionIterator covers every wrapper class (Cast/Operator/CASE/etc.).
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      auto r = RewriteOffsetsInExpr(child, root, changed);
      if (r) {
        child = std::move(r);
      }
    });
  return nullptr;
}

bool TryAttachScoreTopK(duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
                        const connector::SearchFilterOptions& options) {
  if (options.disable_top_k_optimization) {
    return false;
  }

  // Extract limit, order direction, order expression from either shape.
  duckdb::idx_t limit = 0;
  duckdb::BoundOrderByNode* order_node = nullptr;
  duckdb::unique_ptr<duckdb::LogicalOperator>* child_slot = nullptr;
  duckdb::LogicalOperator* below_orderlike = nullptr;

  if (plan->type == duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
    auto& top_n = plan->Cast<duckdb::LogicalTopN>();
    if (top_n.limit == 0 || top_n.offset != 0 || top_n.orders.size() != 1 ||
        top_n.orders[0].type != duckdb::OrderType::DESCENDING) {
      return false;
    }
    limit = static_cast<duckdb::idx_t>(top_n.limit);
    order_node = &top_n.orders[0];
    child_slot = &top_n.children[0];
    below_orderlike = top_n.children[0].get();
  } else if (plan->type == duckdb::LogicalOperatorType::LOGICAL_LIMIT) {
    auto& limit_op = plan->Cast<duckdb::LogicalLimit>();
    if (limit_op.limit_val.Type() != duckdb::LimitNodeType::CONSTANT_VALUE) {
      return false;
    }
    if (limit_op.offset_val.Type() != duckdb::LimitNodeType::UNSET &&
        !(limit_op.offset_val.Type() == duckdb::LimitNodeType::CONSTANT_VALUE &&
          limit_op.offset_val.GetConstantValue() == 0)) {
      return false;
    }
    if (limit_op.children.size() != 1 ||
        limit_op.children[0]->type !=
          duckdb::LogicalOperatorType::LOGICAL_ORDER_BY) {
      return false;
    }
    auto& order_op = limit_op.children[0]->Cast<duckdb::LogicalOrder>();
    if (order_op.orders.size() != 1 ||
        order_op.orders[0].type != duckdb::OrderType::DESCENDING) {
      return false;
    }
    limit = limit_op.limit_val.GetConstantValue();
    if (limit == 0) {
      return false;
    }
    order_node = &order_op.orders[0];
    child_slot = &order_op.children[0];
    below_orderlike = order_op.children[0].get();
  } else {
    return false;
  }

  // Walk the single-child chain below the Order/TopN to find the
  // SearchScan-backed LogicalGet.
  duckdb::LogicalOperator* cur = below_orderlike;
  while (cur && cur->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
    if (cur->children.size() != 1) {
      return false;
    }
    cur = cur->children[0].get();
  }
  if (!cur || !connector::IsSereneDBScan(cur->Cast<duckdb::LogicalGet>())) {
    return false;
  }
  auto& get = cur->Cast<duckdb::LogicalGet>();
  auto& bind_data = get.bind_data->Cast<connector::SereneDBScanBindData>();
  if (bind_data.scan_source->Kind() != connector::ScanSourceKind::Search) {
    return false;
  }
  auto* search_scan = &bind_data.scan_source->Cast<connector::SearchScan>();
  if (!search_scan->scorer) {
    return false;  // No scoring requested -- nothing to prune.
  }
  if (search_scan->score_top_k) {
    return false;  // Already pulled.
  }
  if (order_node->expression->GetExpressionType() !=
      duckdb::ExpressionType::BOUND_COLUMN_REF) {
    return false;
  }
  auto binding =
    order_node->expression->Cast<duckdb::BoundColumnRefExpression>().binding;
  if (!BindingIsScoreColumn(*below_orderlike, binding)) {
    return false;
  }
  search_scan->score_top_k = limit;
  // Scan emits exactly `limit` rows DESC by score; drop the Limit+Order/TopN.
  plan = std::move(*child_slot);
  return true;
}

bool IsCountStarLikeAggregate(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_AGGREGATE) {
    return false;
  }
  auto& agg = expr.Cast<duckdb::BoundAggregateExpression>();
  if (agg.IsDistinct() || agg.filter || agg.order_bys) {
    return false;
  }
  return agg.function.GetName() == duckdb::CountFun::Name ||
         agg.function.GetName() == duckdb::CountStarFun::Name;
}

bool TryConvertAggregateToCount(
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
  const connector::SearchFilterOptions& options) {
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
  // Pushed filters (e.g. `id <= N`) live in table_filters; clearing
  // column_ids would orphan their ProjectionIndex keys.
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
      auto snapshot = PinnedSnapshot(options);
      auto resolved = ResolveIresearch(bind_data, *snapshot);
      if (!resolved) {
        return false;
      }
      count_scan->snapshot =
        PinnedSearchSnapshot(options, resolved->index->GetId());
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

bool RewriteIresearchExpressions(
  duckdb::unique_ptr<duckdb::LogicalOperator>& root,
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan, bool in_mutation) {
  const bool subtree_in_mutation = in_mutation || IsMutationOp(plan->type);
  bool changed = false;
  for (auto& child : plan->children) {
    changed |= RewriteIresearchExpressions(root, child, subtree_in_mutation);
  }
  if (subtree_in_mutation) {
    return changed;
  }

  auto rewrite_claim = [&](duckdb::unique_ptr<duckdb::Expression>& expr) {
    auto r = RewriteScoreCallInExpr(expr, *root, changed, /*set_scorer=*/true);
    if (r) {
      expr = std::move(r);
    }
  };
  auto rewrite_recapture = [&](duckdb::unique_ptr<duckdb::Expression>& expr) {
    auto r = RewriteScoreCallInExpr(expr, *root, changed, /*set_scorer=*/false);
    if (r) {
      expr = std::move(r);
    }
  };

  switch (plan->type) {
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
      for (auto& e : plan->Cast<duckdb::LogicalProjection>().expressions) {
        rewrite_claim(e);
        auto offsets_rep = RewriteOffsetsInExpr(e, *root, changed);
        if (offsets_rep) {
          e = std::move(offsets_rep);
        }
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_FILTER:
      for (auto& e : plan->Cast<duckdb::LogicalFilter>().expressions) {
        if (SimplifyScoreGtZero(*root, e)) {
          changed = true;
        }
        rewrite_recapture(e);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY:
      for (auto& order : plan->Cast<duckdb::LogicalOrder>().orders) {
        rewrite_claim(order.expression);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_TOP_N:
      for (auto& order : plan->Cast<duckdb::LogicalTopN>().orders) {
        rewrite_claim(order.expression);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_WINDOW:
      for (auto& e : plan->expressions) {
        rewrite_claim(e);
      }
      break;
    default:
      break;
  }
  return changed;
}

connector::SearchFilterOptions BuildOptions(duckdb::ClientContext& context) {
  connector::SearchFilterOptions options{.client_context = context};
  duckdb::Value v;
  if (context.TryGetCurrentSetting("sdb_scored_terms_limit", v) &&
      !v.IsNull()) {
    options.scored_terms_limit = static_cast<size_t>(v.GetValue<int32_t>());
  }
  if (context.TryGetCurrentSetting("sdb_disable_top_k_optimization", v) &&
      !v.IsNull()) {
    options.disable_top_k_optimization = v.GetValue<bool>();
  }
  return options;
}

enum class WalkOrder { TopDown, BottomUp };

template<WalkOrder Order, typename Visit>
void Walk(duckdb::unique_ptr<duckdb::LogicalOperator>& plan, bool in_mutation,
          Visit&& visit) {
  const bool subtree_in_mutation = in_mutation || IsMutationOp(plan->type);
  if (!subtree_in_mutation && Order == WalkOrder::TopDown) {
    visit(plan);
  }
  for (auto& child : plan->children) {
    Walk<Order>(child, subtree_in_mutation, visit);
  }
  if (!subtree_in_mutation && Order == WalkOrder::BottomUp) {
    visit(plan);
  }
}

}  // namespace

void PushdownAnnTopK(duckdb::OptimizerExtensionInput& input,
                     duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  auto options = BuildOptions(input.context);
  Walk<WalkOrder::TopDown>(plan, /*in_mutation=*/false, [&](auto& node) {
    if (node->type == duckdb::LogicalOperatorType::LOGICAL_TOP_N ||
        node->type == duckdb::LogicalOperatorType::LOGICAL_LIMIT) {
      TryAnnTopk(node, options);
    }
  });
}

// Push three classes of compute into the SearchScan so the rest of the plan
// can rely on the scan emitting whatever the surface query asks for:
//   1. bm25() / ts_offsets() expressions -> synthetic _score / offsets columns
//      produced by the scan, with the call sites rewritten to read them.
//   2. LogicalTopN / LogicalLimit over a scored SearchScan -> score_top_k
//      pushed onto the scan (engine streams only the top K).
//   3. count_star() aggregate over a bare SearchScan -> scan source swapped
//      to CountScan (no PKs / column values materialised).
// All three need SearchScan in place (so they fire post
// pushdown_complex_filter) and rely on the following UNUSED_COLUMNS pass to
// prune any synthetic columns the surface plan never reads.
void LowerToSearchScan(duckdb::OptimizerExtensionInput& input,
                       duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  RewriteIresearchExpressions(plan, plan, /*in_mutation=*/false);
  auto options = BuildOptions(input.context);
  Walk<WalkOrder::BottomUp>(plan, /*in_mutation=*/false, [&](auto& node) {
    if (node->type == duckdb::LogicalOperatorType::LOGICAL_TOP_N ||
        node->type == duckdb::LogicalOperatorType::LOGICAL_LIMIT) {
      TryAttachScoreTopK(node, options);
    } else if (node->type ==
               duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
      TryConvertAggregateToCount(node, options);
    }
  });
}

void ReconcileSearchOffsets(duckdb::OptimizerExtensionInput& /*input*/,
                            duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  Walk<WalkOrder::TopDown>(plan, /*in_mutation=*/false, [](auto& node) {
    if (node->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
      return;
    }
    auto& get = node->template Cast<duckdb::LogicalGet>();
    if (!connector::IsSereneDBScan(get)) {
      return;
    }
    auto& bind_data =
      get.bind_data->template Cast<connector::SereneDBScanBindData>();
    if (!bind_data.scan_source ||
        bind_data.scan_source->Kind() != connector::ScanSourceKind::Search) {
      return;
    }
    auto& ss = bind_data.scan_source->template Cast<connector::SearchScan>();
    if (ss.offsets.empty()) {
      return;
    }
    containers::FlatHashSet<duckdb::idx_t> surviving;
    for (const auto& cidx : get.GetColumnIds()) {
      if (!cidx.HasPrimaryIndex()) {
        continue;
      }
      const auto p = cidx.GetPrimaryIndex();
      if (p < bind_data.column_ids.size() &&
          bind_data.column_ids[p] == catalog::Column::kInvertedIndexOffsetsId) {
        surviving.insert(p);
      }
    }
    size_t fi = 0;
    size_t write = 0;
    for (size_t bi = 0; bi < bind_data.column_ids.size(); ++bi) {
      if (bind_data.column_ids[bi] !=
          catalog::Column::kInvertedIndexOffsetsId) {
        continue;
      }
      if (surviving.contains(bi)) {
        if (write != fi) {
          ss.offsets[write] = std::move(ss.offsets[fi]);
        }
        ++write;
      }
      ++fi;
    }
    ss.offsets.resize(write);
  });
}

void IresearchPushdownComplexFilter(
  duckdb::ClientContext& context, duckdb::LogicalGet& get,
  duckdb::FunctionData* bind_data_ptr,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters) {
  if (filters.empty() || bind_data_ptr == nullptr) {
    return;
  }
  auto& conn_ctx = connector::GetSereneDBContext(context);
  if (conn_ctx.GetReadYourOwnWrites() && conn_ctx.HasRocksDBTransaction()) {
    return;
  }
  auto& bind_data = bind_data_ptr->Cast<connector::SereneDBScanBindData>();
  if (bind_data.scan_source->Kind() != connector::ScanSourceKind::FullTable) {
    return;
  }
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  auto resolved = ResolveIresearch(bind_data, *snapshot);
  if (!resolved) {
    return;
  }
  auto options = BuildOptions(context);
  // Try ANN range first -- it's more specific and absorbs co-occurring
  // iresearch-claimable text conjuncts itself.
  if (TryClaimAnnRange(filters, get, bind_data, *resolved, snapshot, options)) {
    return;
  }
  TryClaimSearchFilter(filters, get, bind_data, *resolved, snapshot, options);
}

void RegisterIresearchPlanOptimizer(duckdb::DatabaseInstance& db) {
  using duckdb::OptimizerExtension;
  using duckdb::OptimizerHookPosition;
  using OT = duckdb::OptimizerType;

  // ANN topk absorption needs the raw `LogicalFilter > LogicalGet(FullTable)`
  // shape -- pushdown_complex_filter hasn't fired yet, so filters are still
  // visible.
  OptimizerExtension::Register(db.config,
                               OptimizerExtension{
                                 .rule = &PushdownAnnTopK,
                                 .anchor = OT::FILTER_PUSHDOWN,
                                 .where = OptimizerHookPosition::Before,
                               });
  // push bm25/ts_offsets projections, score-TopK, and count_star() into the
  // SearchScan -- see LowerToSearchScan above for the three sub-rewrites.
  OptimizerExtension::Register(db.config,
                               OptimizerExtension{
                                 .rule = &LowerToSearchScan,
                                 .anchor = OT::UNUSED_COLUMNS,
                                 .where = OptimizerHookPosition::Before,
                               });
  // compact ss.offsets entries so surviving column_ids
  // align with output_idx. Without this, patterns like
  // SELECT count(*) FROM (SELECT ts_offsets(...) ...) prune the offsets
  // column entirely.
  OptimizerExtension::Register(db.config,
                               OptimizerExtension{
                                 .rule = &ReconcileSearchOffsets,
                                 .anchor = OT::UNUSED_COLUMNS,
                                 .where = OptimizerHookPosition::After,
                               });
}

}  // namespace sdb::optimizer
