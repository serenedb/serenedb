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

#include <absl/container/flat_hash_set.h>

#include <optional>
#include <span>
#include <vector>

#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/optimizer/optimizer.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp>
#include <duckdb/function/function_binder.hpp>
#include <duckdb/planner/expression/bound_aggregate_expression.hpp>
#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_join.hpp>
#include <duckdb/planner/operator/logical_order.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/expression/bound_unnest_expression.hpp>
#include <duckdb/planner/operator/logical_top_n.hpp>
#include <duckdb/planner/operator/logical_unnest.hpp>
#include <iresearch/search/automaton_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/filter_optimizer.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/proxy_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>

#include "basics/containers/flat_hash_map.h"
#include "basics/down_cast.h"
#include "catalog/inverted_index.h"
#include "catalog/scorer_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_function.h"
#include "connector/functions/search.h"
#include "connector/functions/ts_offsets.h"
#include "connector/functions/vector.h"
#include "connector/index_expression.hpp"
#include "connector/search_filter_builder.hpp"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::optimizer {
namespace {

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

bool TryFoldQueryVector(duckdb::ClientContext& context,
                        duckdb::Expression& expr, size_t dim,
                        std::vector<float>& out) {
  duckdb::Value folded;
  if (!TryFoldExpression(context, expr, folded)) {
    return false;
  }
  duckdb::Value casted;
  const auto target =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, dim);
  if (!folded.DefaultTryCastAs(target, casted, nullptr) || casted.IsNull()) {
    return false;
  }
  out.reserve(dim);
  for (const auto& child : duckdb::ArrayValue::GetChildren(casted)) {
    if (child.IsNull()) {
      return false;
    }
    out.push_back(child.GetValue<float>());
  }
  return true;
}

std::optional<duckdb::TableIndex> SingleReferencedTableIndex(
  const duckdb::Expression& expr) {
  duckdb::unordered_set<duckdb::TableIndex> bindings;
  duckdb::LogicalJoin::GetExpressionBindings(expr, bindings);
  if (bindings.size() != 1) {
    return std::nullopt;
  }
  return *bindings.begin();
}

catalog::Column::Id ResolveColumnId(
  duckdb::ColumnBinding binding,
  const connector::SereneDBScanBindData& bind_data,
  const duckdb::LogicalGet& get) {
  if (binding.table_index != get.table_index) {
    return catalog::Column::kInvalidId;
  }
  const auto col_idx = binding.column_index.GetIndex();
  const auto& column_ids = get.GetColumnIds();
  if (col_idx >= column_ids.size() || !column_ids[col_idx].HasPrimaryIndex()) {
    return catalog::Column::kInvalidId;
  }
  const auto phys = column_ids[col_idx].GetPrimaryIndex();
  if (phys >= bind_data.column_ids.size()) {
    return catalog::Column::kInvalidId;
  }
  return bind_data.column_ids[phys];
}

std::vector<catalog::Column::Id> BuildProjectedColumnIds(
  const duckdb::LogicalGet& get,
  const connector::SereneDBScanBindData& bind_data) {
  std::vector<catalog::Column::Id> projected_ids(get.GetColumnIds().size());
  for (duckdb::idx_t i = 0; i < projected_ids.size(); ++i) {
    projected_ids[i] = ResolveColumnId(
      {get.table_index, duckdb::ProjectionIndex{i}}, bind_data, get);
  }
  return projected_ids;
}

irs::field_id ResolveAnnTargetFieldId(
  const duckdb::Expression& col_arg, const duckdb::LogicalGet& get,
  const connector::SereneDBScanBindData& bind_data,
  const catalog::InvertedIndex& index, duckdb::ClientContext& client_context) {
  if (col_arg.GetExpressionClass() ==
        duckdb::ExpressionClass::BOUND_COLUMN_REF ||
      col_arg.GetExpressionClass() == duckdb::ExpressionClass::BOUND_REF) {
    if (const auto id = bind_data.ColumnIdByName(col_arg.GetName());
        id != catalog::Column::kInvalidId) {
      return id;
    }
  }
  if (SingleReferencedTableIndex(col_arg) != get.table_index) {
    return 0;
  }
  auto normalized = connector::NormalizeBoundExpression(
    col_arg, index.GetRelationId(), BuildProjectedColumnIds(get, bind_data),
    client_context);
  auto serialized = connector::SerializeBoundExpression(*normalized);
  return index.FindFieldIdBySerialized(serialized);
}

struct FoundScan {
  duckdb::LogicalGet* get;
  connector::SereneDBScanBindData* bind_data;
  connector::SearchScan* scan;
};

std::optional<FoundScan> AsSearchScan(duckdb::LogicalOperator& op) {
  if (op.type != duckdb::LogicalOperatorType::LOGICAL_GET) {
    return std::nullopt;
  }
  auto& get = op.Cast<duckdb::LogicalGet>();
  if (!connector::IsSereneDBScan(get)) {
    return std::nullopt;
  }
  auto& bd = get.bind_data->Cast<connector::SereneDBScanBindData>();
  if (bd.scan_source->Kind() != connector::ScanSourceKind::Search) {
    return std::nullopt;
  }
  return FoundScan{&get, &bd, &bd.scan_source->Cast<connector::SearchScan>()};
}

std::optional<FoundScan> FindIResearchScan(duckdb::LogicalOperator& op,
                                           duckdb::TableIndex target) {
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    if (op.Cast<duckdb::LogicalGet>().table_index != target) {
      return std::nullopt;
    }
    return AsSearchScan(op);
  }
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    auto& proj = op.Cast<duckdb::LogicalProjection>();
    if (proj.table_index == target && proj.children.size() == 1) {
      if (auto result = AsSearchScan(*proj.children[0])) {
        return result;
      }
    }
  }
  for (auto& child : op.children) {
    if (auto result = FindIResearchScan(*child, target)) {
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

duckdb::ColumnBinding ResolveBindingThroughProjections(
  duckdb::LogicalOperator& root, duckdb::ColumnBinding binding) {
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
  SDB_ASSERT(proj);
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

duckdb::idx_t AppendVirtualGetColumn(connector::SereneDBScanBindData& bind_data,
                                     duckdb::LogicalGet& get,
                                     catalog::Column::Id virtual_id,
                                     const duckdb::LogicalType& col_type,
                                     std::string_view col_name) {
  const auto bind_idx = bind_data.column_ids.size();
  bind_data.column_ids.push_back(virtual_id);
  bind_data.column_types.push_back(col_type);
  get.returned_types.push_back(col_type);
  get.names.emplace_back(col_name);
  const auto get_col_idx = get.GetColumnIds().size();
  const auto proj_idx = get.AddColumnId(bind_idx);
  if (!get.projection_ids.empty()) {
    get.projection_ids.push_back(proj_idx);
  }
  get.types.push_back(col_type);
  return get_col_idx;
}

duckdb::idx_t AppendScoreColumn(connector::SereneDBScanBindData& bind_data,
                                duckdb::LogicalGet& get) {
  const auto& col_ids = get.GetColumnIds();
  for (duckdb::idx_t j = 0; j < col_ids.size(); ++j) {
    if (ResolveColumnId({get.table_index, duckdb::ProjectionIndex{j}},
                        bind_data,
                        get) == catalog::Column::kInvertedIndexScoreId) {
      return j;
    }
  }
  return AppendVirtualGetColumn(
    bind_data, get, catalog::Column::kInvertedIndexScoreId,
    duckdb::LogicalType::FLOAT, catalog::Column::kScoreName);
}

duckdb::unique_ptr<duckdb::Expression> MakeScoreRefExpression(
  duckdb::LogicalOperator& root, const FoundScan& found,
  duckdb::TableIndex anchor_ti) {
  const auto idx = AppendScoreColumn(*found.bind_data, *found.get);
  const auto binding =
    ExposeGetColumnAt(root, anchor_ti, *found.get, idx,
                      catalog::Column::kScoreName, duckdb::LogicalType::FLOAT);
  return duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    std::string{catalog::Column::kScoreName}, duckdb::LogicalType::FLOAT,
    binding);
}

bool IsScorerFunctionName(std::string_view name) {
  using S = catalog::ScorerOptions;
  static const absl::flat_hash_set<std::string_view> kScorerNames{
    S::Bm25::Owner::type_name(),           S::Tfidf::Owner::type_name(),
    S::LmJm::Owner::type_name(),           S::LmDirichlet::Owner::type_name(),
    S::IndriDirichlet::Owner::type_name(), S::Dfi::Owner::type_name(),
    S::RawBoost::Owner::type_name(),       S::RawTf::Owner::type_name(),
    S::RawDL::Owner::type_name(),
  };
  return kScorerNames.contains(name);
}

bool BindingResolvesToScoreColumn(const duckdb::BoundColumnRefExpression& ref,
                                  duckdb::LogicalOperator& root) {
  if (ref.GetAlias().empty()) {
    return false;
  }
  const auto binding = ResolveBindingThroughProjections(root, ref.binding);
  auto found = FindIResearchScan(root, binding.table_index);
  if (!found) {
    return false;
  }
  return ResolveColumnId(binding, *found->bind_data, *found->get) ==
         catalog::Column::kInvertedIndexScoreId;
}

bool TrySetScorer(std::optional<catalog::ScorerOptions>& scorer,
                  const duckdb::BoundFunctionExpression& func,
                  std::string_view name) {
  auto extracted = catalog::ExtractScorerFromBound(func, name);
  if (!extracted) {
    return false;
  }
  if (!scorer) {
    scorer = std::move(*extracted);
    return true;
  }
  if (*scorer == *extracted) {
    return true;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
    ERR_MSG("Only one scorer function is allowed per inverted index"),
    ERR_HINT("Use UNION to combine different score functions for the same "
             "inverted index"));
}

duckdb::unique_ptr<duckdb::Expression> PushdownScorerCall(
  duckdb::BoundFunctionExpression& func, duckdb::LogicalOperator& root) {
  if (func.children.empty() || func.children[0]->GetExpressionClass() !=
                                 duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return nullptr;
  }
  auto& anchor = func.children[0]->Cast<duckdb::BoundColumnRefExpression>();
  auto found = FindIResearchScan(root, anchor.binding.table_index);
  if (!found) {
    return nullptr;
  }
  auto& ss = *found->scan;
  if (ss.vector_scorer) {
    return nullptr;
  }
  if (!TrySetScorer(ss.text_scorer, func, func.function.GetName())) {
    return nullptr;
  }
  auto ref = MakeScoreRefExpression(root, *found, anchor.binding.table_index);
  if (!func.GetAlias().empty()) {
    ref->SetAlias(func.GetAlias());
  }
  return ref;
}

duckdb::unique_ptr<duckdb::Expression> PushdownDistanceCall(
  duckdb::BoundFunctionExpression& func, const connector::AnnFunctionInfo& info,
  duckdb::LogicalOperator& root, duckdb::ClientContext& context) {
  const auto [col_arg, value_arg] =
    [&] -> std::pair<duckdb::Expression*, duckdb::Expression*> {
    if (info.is_norm) {
      if (func.children.empty() || func.children[0]->IsFoldable()) {
        return {nullptr, nullptr};
      }
      return {func.children[0].get(), nullptr};
    }
    if (func.children.size() != 2) {
      return {nullptr, nullptr};
    }
    auto& lhs = func.children[0];
    auto& rhs = func.children[1];
    if (!lhs->IsFoldable() && rhs->IsFoldable()) {
      return {lhs.get(), rhs.get()};
    }
    if (lhs->IsFoldable() && !rhs->IsFoldable()) {
      return {rhs.get(), lhs.get()};
    }
    return {nullptr, nullptr};
  }();
  if (!col_arg) {
    return nullptr;
  }

  const auto anchor_ti = SingleReferencedTableIndex(*col_arg);
  if (!anchor_ti) {
    return nullptr;
  }

  auto found = FindIResearchScan(root, *anchor_ti);
  if (!found) {
    return nullptr;
  }
  auto& ss = *found->scan;
  if (ss.text_scorer || ss.EmitOffsets()) {
    return nullptr;
  }

  auto index = found->bind_data->inverted_index;
  const auto call_field_id = ResolveAnnTargetFieldId(
    *col_arg, *found->get, *found->bind_data, *index, context);
  if (!irs::field_limits::valid(call_field_id)) {
    return nullptr;
  }
  auto ann_info = index->GetHNSWInfo(call_field_id);
  if (!ann_info || ann_info->metric != info.metric) {
    return nullptr;
  }

  std::vector<float> call_qvec;
  if (info.is_norm) {
    call_qvec.assign(ann_info->d, 0.0f);
  } else if (!TryFoldQueryVector(context, *value_arg, ann_info->d, call_qvec)) {
    return nullptr;
  }

  if (!ss.vector_scorer) {
    ss.vector_scorer = connector::VectorScorerOptions{
      .field_id = call_field_id,
      .query_vector = std::move(call_qvec),
      .metric = info.metric,
      .score_emit = info.score_emit,
      .natural_order = info.order,
    };
  } else {
    const auto& vs = *ss.vector_scorer;
    if (vs.field_id != call_field_id || vs.metric != info.metric ||
        vs.score_emit != info.score_emit || vs.query_vector != call_qvec) {
      return nullptr;
    }
  }

  auto ref = MakeScoreRefExpression(root, *found, *anchor_ti);
  const auto& want_type = func.GetReturnType();
  if (want_type.id() != duckdb::LogicalTypeId::FLOAT) {
    ref = duckdb::BoundCastExpression::AddCastToType(context, std::move(ref),
                                                     want_type);
  }
  if (!func.GetAlias().empty()) {
    ref->SetAlias(func.GetAlias());
  }
  return ref;
}

duckdb::unique_ptr<duckdb::Expression> PushdownOffsetsCall(
  duckdb::BoundFunctionExpression& func, duckdb::LogicalOperator& root) {
  if (func.children.size() != 1 && func.children.size() != 2) {
    return nullptr;
  }
  if (func.children[0]->GetExpressionClass() !=
      duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets() first argument must be a column reference"));
  }
  const auto& col_ref =
    func.children[0]->Cast<duckdb::BoundColumnRefExpression>();
  const auto resolved = ResolveBindingThroughProjections(root, col_ref.binding);

  const auto limit = [&] -> size_t {
    constexpr size_t kDefaultOffsetsLimit = 1 << 12;
    if (func.children.size() != 2) {
      return kDefaultOffsetsLimit;
    }
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
    return raw == 0 ? std::numeric_limits<size_t>::max() : raw;
  }();

  auto found = FindIResearchScan(root, resolved.table_index);
  if (!found) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(", col_ref.GetAlias(),
              ") requires an inverted index scan in the same sub-query"));
  }
  auto& search_scan = *found->scan;

  const auto col_name = [&] -> std::string_view {
    if (const auto& cids = found->get->GetColumnIds();
        resolved.column_index.GetIndex() < cids.size()) {
      return found->get->GetColumnName(cids[resolved.column_index.GetIndex()]);
    }
    return col_ref.GetAlias();
  };

  const auto target_col_id =
    ResolveColumnId(resolved, *found->bind_data, *found->get);
  if (target_col_id == catalog::Column::kInvalidId) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(): column '", col_name(), "' not found in table"));
  }

  const auto* col_info =
    found->bind_data->inverted_index->FindColumnInfo(target_col_id);
  if (!col_info) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(): column '", col_name(), "' not found in index"));
  }
  const bool is_text = col_info->text_dictionary.isSet();
  const bool offs_stored =
    col_info->features.HasFeatures(irs::IndexFeatures::Offs);

  if (is_text && !offs_stored) {
    auto bind = duckdb::make_uniq<connector::OffsetsBindData>();
    bind->inverted_index = found->bind_data->inverted_index;
    bind->column_id = target_col_id;
    bind->limit = limit;
    search_scan.offsets.push_back(
      {.column_id = target_col_id, .limit = limit, .bind = bind.get()});
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

  duckdb::idx_t get_col_idx = duckdb::DConstants::INVALID_INDEX;
  const auto existing = absl::c_find_if(
    search_scan.offsets,
    [&](const auto& req) { return req.column_id == target_col_id; });
  if (existing != search_scan.offsets.end()) {
    if (existing->limit != limit) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("ts_offsets() called multiple times for field '",
                              col_name(), "' with different limits"));
    }
    get_col_idx = existing->get_col_idx;
  }

  const auto col_type = catalog::Column::MakeOffsetsType();
  const auto offsets_col_name = catalog::Column::MakeOffsetsName(target_col_id);
  if (get_col_idx == duckdb::DConstants::INVALID_INDEX) {
    get_col_idx = AppendVirtualGetColumn(
      *found->bind_data, *found->get, catalog::Column::kInvertedIndexOffsetsId,
      col_type, offsets_col_name);
    search_scan.offsets.push_back(
      {.column_id = target_col_id, .limit = limit, .get_col_idx = get_col_idx});
  }
  const auto binding =
    ExposeGetColumnAt(root, col_ref.binding.table_index, *found->get,
                      get_col_idx, offsets_col_name, col_type);
  auto out = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    offsets_col_name, col_type, binding);
  out->SetAlias(func.GetAlias());
  return out;
}

struct TsDictOutput {
  catalog::Column::Id virtual_id;
  duckdb::LogicalType type;
  std::string_view name;
  std::string_view agg;
};

std::optional<TsDictOutput> TsDictOutputFor(std::string_view fn) {
  using C = catalog::Column;
  if (fn == connector::kTsDictAgg) {
    return TsDictOutput{C::kInvertedIndexTermId, duckdb::LogicalType::VARCHAR,
                          C::kTermName, "list"};
  }
  if (fn == connector::kTsDictRawAgg) {
    return TsDictOutput{C::kInvertedIndexTermRawId, duckdb::LogicalType::BLOB,
                          C::kTermRawName, "list"};
  }
  if (fn == connector::kTsDictCount) {
    return TsDictOutput{C::kInvertedIndexTermCountId,
                          duckdb::LogicalType::INTEGER, C::kTermCountName,
                          "list"};
  }
  if (fn == connector::kTsDictFreq) {
    return TsDictOutput{C::kInvertedIndexTermFreqId,
                          duckdb::LogicalType::BIGINT, C::kTermFreqName, "list"};
  }
  if (fn == connector::kTsDictScore) {
    return TsDictOutput{C::kInvertedIndexTermScoreId,
                          duckdb::LogicalType::FLOAT, C::kTermScoreName, "list"};
  }
  if (fn == connector::kTsDictMin) {
    return TsDictOutput{C::kInvertedIndexTermId, duckdb::LogicalType::VARCHAR,
                          C::kTermName, "min"};
  }
  if (fn == connector::kTsDictMax) {
    return TsDictOutput{C::kInvertedIndexTermId, duckdb::LogicalType::VARCHAR,
                          C::kTermName, "max"};
  }
  return std::nullopt;
}

duckdb::idx_t& TsDictColIdxFor(connector::SearchScan::TsDictRequest& req,
                                 std::string_view fn) {
  if (fn == connector::kTsDictRawAgg) {
    return req.term_raw_col_idx;
  }
  if (fn == connector::kTsDictCount) {
    return req.count_col_idx;
  }
  if (fn == connector::kTsDictFreq) {
    return req.freq_col_idx;
  }
  if (fn == connector::kTsDictScore) {
    return req.score_col_idx;
  }
  return req.term_col_idx;
}

duckdb::unique_ptr<duckdb::Expression> BuildTsDictAggregate(
  duckdb::ClientContext& context, std::string_view agg_name,
  duckdb::unique_ptr<duckdb::Expression> child) {
  auto& sys = duckdb::Catalog::GetSystemCatalog(context);
  auto& entry = sys.GetEntry<duckdb::AggregateFunctionCatalogEntry>(
    context, std::string{DEFAULT_SCHEMA}, std::string{agg_name});
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
  children.push_back(std::move(child));
  duckdb::FunctionBinder binder{context};
  duckdb::ErrorData error;
  const auto best =
    binder.BindFunction(std::string{agg_name}, entry.functions, children,
                        error);
  if (!best.IsValid()) {
    error.Throw();
  }
  auto fn = entry.functions.GetFunctionByOffset(best.GetIndex());
  return binder.BindAggregateFunction(fn, std::move(children));
}

duckdb::unique_ptr<duckdb::Expression> PushdownTsDictCall(
  duckdb::BoundAggregateExpression& agg, duckdb::LogicalOperator& root,
  duckdb::ClientContext& context) {
  const auto fn = agg.function.GetName();
  auto output = TsDictOutputFor(fn);
  SDB_ASSERT(output.has_value());
  if (agg.children.size() != 1 ||
      agg.children[0]->GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(fn, "() argument must be an indexed column"));
  }
  const auto& col_ref =
    agg.children[0]->Cast<duckdb::BoundColumnRefExpression>();
  const auto resolved = ResolveBindingThroughProjections(root, col_ref.binding);
  auto found = FindIResearchScan(root, resolved.table_index);
  if (!found) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(fn, "() requires an inverted index scan in the same sub-query"));
  }
  const auto col_id = ResolveColumnId(resolved, *found->bind_data, *found->get);
  if (col_id == catalog::Column::kInvalidId) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(fn, "(): column not found in index"));
  }
  const auto* info = found->bind_data->inverted_index->FindColumnInfo(col_id);
  if (!info || !info->IsTermDict()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG(fn, "(): column has no term dictionary"),
      ERR_HINT(
        "ts_dict_agg requires a text-tokenized or term-dictionary column"));
  }

  const auto field_id = static_cast<irs::field_id>(col_id);
  auto& ss = *found->scan;
  auto& req = ss.TsDictFor(field_id);

  auto& col_idx = TsDictColIdxFor(req, fn);
  if (col_idx == duckdb::DConstants::INVALID_INDEX) {
    col_idx = AppendVirtualGetColumn(*found->bind_data, *found->get,
                                     output->virtual_id, output->type,
                                     output->name);
  }
  const auto binding =
    ExposeGetColumnAt(root, col_ref.binding.table_index, *found->get, col_idx,
                      output->name, output->type);
  auto ref = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    std::string{output->name}, output->type, binding);
  auto result = BuildTsDictAggregate(context, output->agg, std::move(ref));
  // With several fields the scan emits each field's terms in its own rows and
  // leaves other fields' columns NULL; the list aggregate must skip those.
  // count/min/max already ignore NULLs.
  if (output->agg == "list") {
    auto& bound = result->Cast<duckdb::BoundAggregateExpression>();
    auto filter_ref = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      std::string{output->name}, output->type, binding);
    auto is_not_null = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
      duckdb::ExpressionType::OPERATOR_IS_NOT_NULL,
      duckdb::LogicalType::BOOLEAN);
    is_not_null->children.push_back(std::move(filter_ref));
    bound.filter = std::move(is_not_null);
  }
  result->SetAlias(agg.GetAlias());
  return result;
}

connector::SearchScan* FindTsDictScan(duckdb::LogicalOperator& op) {
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    auto found = AsSearchScan(op);
    return found && found->scan->TsDictMode() ? found->scan : nullptr;
  }
  for (auto& child : op.children) {
    if (auto* scan = FindTsDictScan(*child)) {
      return scan;
    }
  }
  return nullptr;
}

enum class TsDictColKind { kTerm, kTermRaw, kCount, kFreq, kScore };

struct TsDictColRef {
  size_t req_index;
  TsDictColKind kind;
};

std::optional<TsDictColRef> ClassifyTsDictGetCol(const connector::SearchScan& ss,
                                                 duckdb::idx_t get_col_idx) {
  for (size_t r = 0; r < ss.ts_dicts.size(); ++r) {
    const auto& req = ss.ts_dicts[r];
    if (get_col_idx == req.term_col_idx) {
      return TsDictColRef{r, TsDictColKind::kTerm};
    }
    if (get_col_idx == req.term_raw_col_idx) {
      return TsDictColRef{r, TsDictColKind::kTermRaw};
    }
    if (get_col_idx == req.count_col_idx) {
      return TsDictColRef{r, TsDictColKind::kCount};
    }
    if (get_col_idx == req.freq_col_idx) {
      return TsDictColRef{r, TsDictColKind::kFreq};
    }
    if (get_col_idx == req.score_col_idx) {
      return TsDictColRef{r, TsDictColKind::kScore};
    }
  }
  return std::nullopt;
}

std::optional<FoundScan> FindTsDictFoundScan(duckdb::LogicalOperator& op) {
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    if (auto f = AsSearchScan(op); f && f->scan->TsDictMode()) {
      return f;
    }
    return std::nullopt;
  }
  for (auto& child : op.children) {
    if (auto f = FindTsDictFoundScan(*child)) {
      return f;
    }
  }
  return std::nullopt;
}

template<typename F>
void WalkColumnRefs(duckdb::unique_ptr<duckdb::Expression>& expr, F&& fn) {
  if (expr->GetExpressionClass() ==
      duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    fn(expr);
    return;
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      WalkColumnRefs(child, fn);
    });
}

struct TsDictGroupEntry {
  duckdb::ColumnBinding source;
  duckdb::LogicalType source_type;
  TsDictColKind kind;
  size_t req_index;
  duckdb::ColumnBinding output;
  duckdb::LogicalType output_type;
};

duckdb::unique_ptr<duckdb::LogicalOperator> InjectTsDictGroupBy(
  duckdb::unique_ptr<duckdb::LogicalOperator> child,
  std::span<duckdb::unique_ptr<duckdb::Expression>> consumer_roots,
  duckdb::Binder& binder, duckdb::ClientContext& context) {
  auto found = FindTsDictFoundScan(*child);
  SDB_ASSERT(found.has_value());
  const auto get_ti = found->get->table_index;
  auto& ss = *found->scan;

  std::vector<TsDictGroupEntry> entries;
  const auto find_entry =
    [&](const duckdb::ColumnBinding& b) -> TsDictGroupEntry* {
    for (auto& e : entries) {
      if (e.source == b) {
        return &e;
      }
    }
    return nullptr;
  };

  for (auto& root : consumer_roots) {
    WalkColumnRefs(root, [&](duckdb::unique_ptr<duckdb::Expression>& ref_expr) {
      auto& ref = ref_expr->Cast<duckdb::BoundColumnRefExpression>();
      if (find_entry(ref.binding) != nullptr) {
        return;
      }
      const auto resolved =
        ResolveBindingThroughProjections(*child, ref.binding);
      if (resolved.table_index != get_ti) {
        return;
      }
      const auto col = ClassifyTsDictGetCol(ss, resolved.column_index.GetIndex());
      if (!col) {
        return;
      }
      entries.push_back({.source = ref.binding,
                         .source_type = ref.GetReturnType(),
                         .kind = col->kind,
                         .req_index = col->req_index});
    });
  }
  SDB_ASSERT(!entries.empty());

  for (size_t r = 0; r < ss.ts_dicts.size(); ++r) {
    bool referenced = false;
    bool has_group = false;
    for (const auto& e : entries) {
      if (e.req_index != r) {
        continue;
      }
      referenced = true;
      if (e.kind == TsDictColKind::kTerm || e.kind == TsDictColKind::kTermRaw) {
        has_group = true;
      }
    }
    if (!referenced || has_group) {
      continue;
    }
    auto& req = ss.ts_dicts[r];
    if (req.term_col_idx == duckdb::DConstants::INVALID_INDEX) {
      req.term_col_idx = AppendVirtualGetColumn(
        *found->bind_data, *found->get, catalog::Column::kInvertedIndexTermId,
        duckdb::LogicalType::VARCHAR, catalog::Column::kTermName);
    }
    entries.push_back(
      {.source = duckdb::ColumnBinding{get_ti,
                                       duckdb::ProjectionIndex{req.term_col_idx}},
       .source_type = duckdb::LogicalType::VARCHAR,
       .kind = TsDictColKind::kTerm,
       .req_index = r});
  }

  const auto group_index = binder.GenerateTableIndex();
  const auto agg_index = binder.GenerateTableIndex();
  const auto groupings_index = binder.GenerateTableIndex();

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> groups;
  for (auto& e : entries) {
    if (e.kind != TsDictColKind::kTerm && e.kind != TsDictColKind::kTermRaw) {
      continue;
    }
    e.output = {group_index, duckdb::ProjectionIndex{groups.size()}};
    e.output_type = e.source_type;
    groups.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      e.source_type, e.source));
  }
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> select_list;
  for (auto& e : entries) {
    if (e.kind == TsDictColKind::kTerm || e.kind == TsDictColKind::kTermRaw) {
      continue;
    }
    auto agg = BuildTsDictAggregate(
      context, e.kind == TsDictColKind::kScore ? "max" : "sum",
      duckdb::make_uniq<duckdb::BoundColumnRefExpression>(e.source_type,
                                                          e.source));
    e.output = {agg_index, duckdb::ProjectionIndex{select_list.size()}};
    e.output_type = agg->GetReturnType();
    select_list.push_back(std::move(agg));
  }

  auto aggregate = duckdb::make_uniq<duckdb::LogicalAggregate>(
    group_index, agg_index, std::move(select_list));
  aggregate->groupings_index = groupings_index;
  aggregate->groups = std::move(groups);
  aggregate->children.push_back(std::move(child));
  aggregate->ResolveOperatorTypes();
  aggregate->group_stats.resize(aggregate->groups.size());

  for (auto& root : consumer_roots) {
    WalkColumnRefs(root, [&](duckdb::unique_ptr<duckdb::Expression>& ref_expr) {
      auto& ref = ref_expr->Cast<duckdb::BoundColumnRefExpression>();
      auto* e = find_entry(ref.binding);
      if (e == nullptr) {
        return;
      }
      duckdb::unique_ptr<duckdb::Expression> repl =
        duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
          ref.GetAlias(), e->output_type, e->output);
      if (e->output_type != e->source_type) {
        repl = duckdb::BoundCastExpression::AddCastToType(
          context, std::move(repl), e->source_type);
      }
      ref_expr = std::move(repl);
    });
  }

  return aggregate;
}

// `unnest(ts_dict_agg(f))` builds a per-group LIST then explodes it back to one
// row per term -- an identity round-trip over the rows the term-dict scan
// already emits. Collapse `Unnest( Aggregate[list(col)...] )` into a plain
// projection of those columns when the subtree is a term-dict scan.
void CollapseTsDictUnnest(duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  auto& unnest = plan->Cast<duckdb::LogicalUnnest>();
  if (plan->children.size() != 1 ||
      plan->children[0]->type !=
        duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    return;
  }
  auto& agg = plan->children[0]->Cast<duckdb::LogicalAggregate>();
  if (!agg.groups.empty() || agg.children.size() != 1) {
    return;
  }
  for (auto& a : agg.expressions) {
    if (a->GetExpressionClass() != duckdb::ExpressionClass::BOUND_AGGREGATE) {
      return;
    }
    auto& ba = a->Cast<duckdb::BoundAggregateExpression>();
    if (ba.function.GetName() != "list" || ba.children.size() != 1) {
      return;
    }
    const auto* inner = ba.children[0].get();
    if (inner->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CAST) {
      inner = inner->Cast<duckdb::BoundCastExpression>().child.get();
    }
    if (inner->GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_COLUMN_REF) {
      return;
    }
  }
  // Only safe for a single enumerated field: with several fields the terms
  // live in disjoint scan rows, so unnest must positionally zip the lists
  // (DuckDB's normal Unnest) rather than read the scan rows directly.
  auto* scan = FindTsDictScan(*agg.children[0]);
  if (scan == nullptr || scan->ts_dicts.size() != 1) {
    return;
  }

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> proj_exprs;
  proj_exprs.reserve(unnest.expressions.size());
  for (auto& u : unnest.expressions) {
    if (u->GetExpressionClass() != duckdb::ExpressionClass::BOUND_UNNEST) {
      return;
    }
    auto& bu = u->Cast<duckdb::BoundUnnestExpression>();
    if (bu.child->GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_COLUMN_REF) {
      return;
    }
    const auto& ref = bu.child->Cast<duckdb::BoundColumnRefExpression>();
    if (ref.binding.table_index != agg.aggregate_index) {
      return;
    }
    const auto k = ref.binding.column_index.GetIndex();
    if (k >= agg.expressions.size()) {
      return;
    }
    auto& ba = agg.expressions[k]->Cast<duckdb::BoundAggregateExpression>();
    proj_exprs.push_back(std::move(ba.children[0]));
  }

  auto child = std::move(agg.children[0]);
  auto proj = duckdb::make_uniq<duckdb::LogicalProjection>(
    unnest.unnest_index, std::move(proj_exprs));
  for (auto& e : proj->expressions) {
    proj->types.push_back(e->GetReturnType());
  }
  proj->children.push_back(std::move(child));
  plan = std::move(proj);
}

bool ColumnIsNotNull(const catalog::Table& table,
                     catalog::Column::Id col_id) {
  for (const auto pk : table.PKColumns()) {
    if (pk == col_id) {
      return true;
    }
  }
  const auto& cols = table.Columns();
  std::optional<size_t> idx;
  for (size_t i = 0; i < cols.size(); ++i) {
    if (cols[i].GetId() == col_id) {
      idx = i;
      break;
    }
  }
  if (!idx.has_value()) {
    return false;
  }
  for (const auto& cc : table.CheckConstraints()) {
    if (cc.IsNotNull(cols) == idx) {
      return true;
    }
  }
  return false;
}

struct ArrayAggMatch {
  duckdb::ColumnBinding binding;
  irs::field_id field_id;
};

// `array_agg(DISTINCT col)` over a keyword-analyzed inverted-index column
// enumerates exactly the field's distinct terms, so it can be served from the
// term dictionary. Requires a NOT NULL column: `list()` keeps a NULL element
// for missing values while the term dictionary has none.
std::optional<ArrayAggMatch> ClassifyArrayAggDistinct(
  duckdb::BoundAggregateExpression& agg, duckdb::LogicalOperator& root,
  const catalog::Snapshot& snapshot) {
  const auto& name = agg.function.GetName();
  if (name != "array_agg" && name != "list") {
    return std::nullopt;
  }
  if (!agg.IsDistinct() || agg.filter != nullptr || agg.order_bys != nullptr) {
    return std::nullopt;
  }
  if (agg.children.size() != 1 ||
      agg.children[0]->GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return std::nullopt;
  }
  const auto& col_ref =
    agg.children[0]->Cast<duckdb::BoundColumnRefExpression>();
  if (col_ref.GetReturnType().id() != duckdb::LogicalTypeId::VARCHAR) {
    return std::nullopt;
  }
  const auto resolved = ResolveBindingThroughProjections(root, col_ref.binding);
  auto found = FindIResearchScan(root, resolved.table_index);
  if (!found ||
      found->bind_data->GetKind() !=
        connector::SereneDBScanBindData::Kind::Table) {
    return std::nullopt;
  }
  const auto col_id =
    ResolveColumnId(resolved, *found->bind_data, *found->get);
  if (col_id == catalog::Column::kInvalidId) {
    return std::nullopt;
  }
  const auto* index = found->bind_data->inverted_index.get();
  if (index == nullptr) {
    return std::nullopt;
  }
  const auto field_id = static_cast<irs::field_id>(col_id);
  if (!index->IsKeywordField(snapshot, field_id)) {
    return std::nullopt;
  }
  const auto& tbd = found->bind_data->As<connector::TableScanBindData>();
  if (!tbd.table || !ColumnIsNotNull(*tbd.table, col_id)) {
    return std::nullopt;
  }
  return ArrayAggMatch{col_ref.binding, field_id};
}

duckdb::unique_ptr<duckdb::Expression> BuildKeywordTsDictList(
  const duckdb::ColumnBinding& orig_binding, irs::field_id field_id,
  duckdb::LogicalOperator& root, duckdb::ClientContext& context,
  const std::string& alias) {
  using C = catalog::Column;
  const auto resolved = ResolveBindingThroughProjections(root, orig_binding);
  auto found = FindIResearchScan(root, resolved.table_index);
  SDB_ASSERT(found.has_value());
  auto& req = found->scan->TsDictFor(field_id);
  if (req.term_col_idx == duckdb::DConstants::INVALID_INDEX) {
    req.term_col_idx =
      AppendVirtualGetColumn(*found->bind_data, *found->get,
                             C::kInvertedIndexTermId,
                             duckdb::LogicalType::VARCHAR, C::kTermName);
  }
  const auto binding =
    ExposeGetColumnAt(root, orig_binding.table_index, *found->get,
                      req.term_col_idx, C::kTermName,
                      duckdb::LogicalType::VARCHAR);
  auto ref = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    std::string{C::kTermName}, duckdb::LogicalType::VARCHAR, binding);
  auto result = BuildTsDictAggregate(context, "list", std::move(ref));
  auto& bound = result->Cast<duckdb::BoundAggregateExpression>();
  auto filter_ref = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    std::string{C::kTermName}, duckdb::LogicalType::VARCHAR, binding);
  auto is_not_null = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
    duckdb::ExpressionType::OPERATOR_IS_NOT_NULL,
    duckdb::LogicalType::BOOLEAN);
  is_not_null->children.push_back(std::move(filter_ref));
  bound.filter = std::move(is_not_null);
  result->SetAlias(alias);
  return result;
}

void PushdownTsDictAggregates(duckdb::LogicalAggregate& aggr,
                                duckdb::LogicalOperator& root,
                                duckdb::Binder& binder,
                                duckdb::ClientContext& context) {
  std::vector<size_t> ts_dict_calls;
  std::vector<std::pair<size_t, ArrayAggMatch>> array_aggs;
  bool other = false;
  std::shared_ptr<const catalog::Snapshot> snapshot;
  for (size_t i = 0; i < aggr.expressions.size(); ++i) {
    auto& expr = aggr.expressions[i];
    if (expr->GetExpressionClass() != duckdb::ExpressionClass::BOUND_AGGREGATE) {
      other = true;
      continue;
    }
    auto& agg = expr->Cast<duckdb::BoundAggregateExpression>();
    if (connector::IsTsDictFunctionName(agg.function.GetName())) {
      ts_dict_calls.push_back(i);
      continue;
    }
    if (aggr.groups.empty()) {
      if (!snapshot) {
        snapshot = connector::GetSereneDBContext(context).EnsureCatalogSnapshot();
      }
      if (auto match = ClassifyArrayAggDistinct(agg, root, *snapshot)) {
        array_aggs.push_back({i, *match});
        continue;
      }
    }
    other = true;
  }

  bool converted = false;
  for (const auto i : ts_dict_calls) {
    auto& agg = aggr.expressions[i]->Cast<duckdb::BoundAggregateExpression>();
    aggr.expressions[i] = PushdownTsDictCall(agg, root, context);
    converted = true;
  }

  // Converting array_agg forces the scan into term-dict enumeration, so it is
  // only safe when every aggregate in the node can be served that way.
  if (!other && !array_aggs.empty()) {
    for (const auto& [i, match] : array_aggs) {
      const auto alias = aggr.expressions[i]->GetAlias();
      aggr.expressions[i] = BuildKeywordTsDictList(
        match.binding, match.field_id, root, context, alias);
    }
    converted = true;
  }

  if (converted && !aggr.children.empty()) {
    auto injected = InjectTsDictGroupBy(
      std::move(aggr.children[0]),
      {aggr.expressions.data(), aggr.expressions.size()}, binder, context);
    aggr.children[0] = std::move(injected);
  }
}

duckdb::unique_ptr<duckdb::Expression> RewriteCallInExpr(
  duckdb::unique_ptr<duckdb::Expression>& expr, duckdb::LogicalOperator& root,
  duckdb::ClientContext& context) {
  if (!expr) {
    return nullptr;
  }
  if (expr->GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
    auto& func = expr->Cast<duckdb::BoundFunctionExpression>();
    const auto& name = func.function.GetName();
    if (IsScorerFunctionName(name)) {
      if (auto repl = PushdownScorerCall(func, root)) {
        return repl;
      }
      // Native scans cannot materialize the tableoid argument, so the
      // runtime stub would never be reached; raise its error here.
      throw duckdb::InvalidInputException(
        "%s() requires an inverted index scan in the same sub-query", name);
    } else if (name == connector::kOffsets) {
      if (auto repl = PushdownOffsetsCall(func, root)) {
        return repl;
      }
    } else if (auto info = connector::GetAnnFunctionInfo(func)) {
      if (auto repl = PushdownDistanceCall(func, *info, root, context)) {
        return repl;
      }
    }
  } else if (expr->GetExpressionClass() ==
             duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    auto& ref = expr->Cast<duckdb::BoundColumnRefExpression>();
    if (BindingResolvesToScoreColumn(ref, root)) {
      ref.SetAlias({});
    }
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      if (auto r = RewriteCallInExpr(child, root, context)) {
        child = std::move(r);
      }
    });
  return nullptr;
}

void RewriteIResearchExpressions(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::LogicalOperator>& root,
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan, bool in_mutation,
  duckdb::Binder& binder) {
  const bool subtree_in_mutation =
    in_mutation || plan->type == duckdb::LogicalOperatorType::LOGICAL_DELETE ||
    plan->type == duckdb::LogicalOperatorType::LOGICAL_UPDATE ||
    plan->type == duckdb::LogicalOperatorType::LOGICAL_MERGE_INTO;

  for (auto& child : plan->children) {
    RewriteIResearchExpressions(context, root, child, subtree_in_mutation,
                                binder);
  }
  if (subtree_in_mutation) {
    return;
  }

  auto rewrite_call = [&](duckdb::unique_ptr<duckdb::Expression>& expr) {
    if (auto r = RewriteCallInExpr(expr, *root, context)) {
      expr = std::move(r);
    }
  };

  switch (plan->type) {
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
    case duckdb::LogicalOperatorType::LOGICAL_FILTER:
    case duckdb::LogicalOperatorType::LOGICAL_WINDOW:
      for (auto& e : plan->expressions) {
        rewrite_call(e);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY:
      for (auto& o : plan->Cast<duckdb::LogicalOrder>().orders) {
        rewrite_call(o.expression);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_TOP_N:
      for (auto& o : plan->Cast<duckdb::LogicalTopN>().orders) {
        rewrite_call(o.expression);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
      PushdownTsDictAggregates(plan->Cast<duckdb::LogicalAggregate>(), *root,
                                 binder, context);
      break;
    case duckdb::LogicalOperatorType::LOGICAL_UNNEST:
      CollapseTsDictUnnest(plan);
      break;
    default:
      break;
  }
}

bool TryClaimIResearchConjunct(
  irs::And& and_root, const duckdb::unique_ptr<duckdb::Expression>& conjunct,
  const connector::ColumnGetter& getter,
  const connector::ExpressionGetter& expr_getter,
  duckdb::ClientContext& context) {
  const auto before = and_root.size();
  std::span<const duckdb::unique_ptr<duckdb::Expression>> single{&conjunct, 1};
  auto r =
    connector::MakeSearchFilter(and_root, single, getter, context, expr_getter);
  if (r.ok() && and_root.size() > before) {
    return true;
  }
  while (and_root.size() > before) {
    and_root.PopBack();
  }
  return false;
}

std::optional<duckdb::ColumnBinding> CastFreeColumnRefBinding(
  const duckdb::Expression* e) {
  while (e && e->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CAST) {
    e = e->Cast<duckdb::BoundCastExpression>().child.get();
  }
  if (!e ||
      e->GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return std::nullopt;
  }
  return e->Cast<duckdb::BoundColumnRefExpression>().binding;
}

bool TryClaimAnnRange(
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  duckdb::LogicalGet& get, connector::SereneDBScanBindData& bind_data,
  duckdb::ClientContext& context) {
  auto& scan = bind_data.scan_source->Cast<connector::SearchScan>();
  if (!scan.vector_scorer ||
      scan.vector_scorer->natural_order != duckdb::OrderType::ASCENDING) {
    return false;
  }

  for (duckdb::idx_t i = 0; i < filters.size(); ++i) {
    auto& expr = *filters[i];
    if (!duckdb::BoundComparisonExpression::IsComparison(expr)) {
      continue;
    }
    auto& cmp = expr.Cast<duckdb::BoundFunctionExpression>();
    auto& cmp_left = duckdb::BoundComparisonExpression::LeftMutable(cmp);
    auto& cmp_right = duckdb::BoundComparisonExpression::RightMutable(cmp);
    const auto [score_side, const_side] =
      [&] -> std::pair<duckdb::Expression*, duckdb::Expression*> {
      switch (cmp.GetExpressionType()) {
        case duckdb::ExpressionType::COMPARE_LESSTHAN:
        case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
          return {cmp_left.get(), cmp_right.get()};
        case duckdb::ExpressionType::COMPARE_GREATERTHAN:
        case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
          return {cmp_right.get(), cmp_left.get()};
        default:
          return {nullptr, nullptr};
      }
    }();
    const auto score_binding = CastFreeColumnRefBinding(score_side);
    if (!score_binding || ResolveColumnId(*score_binding, bind_data, get) !=
                            catalog::Column::kInvertedIndexScoreId) {
      continue;
    }
    duckdb::Value radius_value;
    if (!TryFoldExpression(context, *const_side, radius_value)) {
      continue;
    }
    scan.vector_scorer->radius = [&] -> float {
      switch (radius_value.type().id()) {
        case duckdb::LogicalTypeId::FLOAT:
          return radius_value.GetValue<float>();
        case duckdb::LogicalTypeId::DOUBLE:
          return static_cast<float>(radius_value.GetValue<double>());
        default:
          return std::numeric_limits<float>::max();
      }
    }();
    if (scan.vector_scorer->radius == std::numeric_limits<float>::max()) {
      continue;
    }
    filters.erase(filters.begin() + i);
    return true;
  }
  return false;
}

bool TryClaimSearchFilter(
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  duckdb::LogicalGet& get, connector::SereneDBScanBindData& bind_data,
  const catalog::InvertedIndex& index,
  std::shared_ptr<const catalog::Snapshot> snapshot,
  duckdb::ClientContext& context) {
  const auto projected_ids = BuildProjectedColumnIds(get, bind_data);
  const auto table_index = get.table_index;
  const auto relation_id = index.GetRelationId();

  containers::FlatHashSet<irs::field_id> analyzed_fields;

  const auto make_info = [&](auto field_id, const auto* info,
                             duckdb::LogicalType type) {
    connector::SearchColumnInfo column_info{
      .field_id = field_id,
      .null_field_id =
        info ? info->null_field_id : irs::field_limits::invalid(),
      .bool_field_id =
        info ? info->bool_field_id : irs::field_limits::invalid(),
      .numeric_field_id =
        info ? info->numeric_field_id : irs::field_limits::invalid(),
      .logical_type = std::move(type),
      .tokenizer = index.GetTokenizer(snapshot, field_id),
    };
    if (column_info.tokenizer.analyzer->type() !=
        irs::Type<irs::StringTokenizer>::id()) {
      analyzed_fields.insert(field_id);
    }
    return column_info;
  };

  connector::ColumnGetter getter =
    [&](const duckdb::BoundColumnRefExpression& ref)
    -> std::optional<connector::SearchColumnInfo> {
    const auto col_id = ResolveColumnId(ref.binding, bind_data, get);
    if (col_id == catalog::Column::kInvalidId) {
      return std::nullopt;
    }
    const auto* info = index.FindColumnInfo(col_id);
    if (!info || !info->IsTermDict()) {
      return std::nullopt;
    }
    auto type = bind_data.ColumnTypeById(col_id);
    if (type.id() == duckdb::LogicalTypeId::INVALID) {
      return std::nullopt;
    }
    return make_info(col_id, info, std::move(type));
  };

  connector::ExpressionGetter expr_getter = [&](const duckdb::Expression& expr)
    -> std::optional<connector::SearchColumnInfo> {
    if (SingleReferencedTableIndex(expr) != table_index) {
      return std::nullopt;
    }
    auto normalized = connector::NormalizeBoundExpression(
      expr, relation_id, projected_ids, context);
    auto serialized = connector::SerializeBoundExpression(*normalized);
    const auto field_id = index.FindFieldIdBySerialized(serialized);
    const auto* expr_data = index.ExpressionByFieldId(field_id);
    if (!expr_data) {
      return std::nullopt;
    }
    const auto* info = index.FindEntry(field_id);
    return make_info(field_id, info, expr_data->return_type);
  };

  auto& scan = bind_data.scan_source->Cast<connector::SearchScan>();

  auto root_and = std::make_unique<irs::And>();
  bool any_claimed = false;
  for (size_t i = 0; i < filters.size();) {
    if (TryClaimIResearchConjunct(*root_and, filters[i], getter, expr_getter,
                                  context)) {
      any_claimed = true;
      std::swap(filters[i], filters.back());
      filters.pop_back();
    } else {
      ++i;
    }
  }
  if (!any_claimed) {
    return false;
  }

  irs::Filter::ptr root = std::move(root_and);
  irs::Optimize(root, {.scored = scan.text_scorer.has_value(),
                       .analyzed_fields = std::move(analyzed_fields)});

  std::shared_ptr<irs::Filter> stored;
  if (scan.vector_scorer) {
    auto proxy = std::make_shared<irs::ProxyFilter>();
    proxy->set_filter(irs::IResourceManager::gNoop, std::move(root));
    stored = std::move(proxy);
  } else {
    stored = std::move(root);
  }

  scan.stored_filter = std::move(stored);
  for (auto& req : scan.offsets) {
    if (req.bind) {
      req.bind->stored_filter = scan.stored_filter;
    }
  }
  return true;
}

// Repoint every column reference that resolves to the enumerated field onto the
// term virtual column, so a single same-field predicate the filter builder
// cannot push as a term acceptor still applies as a post-filter over the emitted
// terms (no postings touched -- it filters the dictionary rows).
void RewriteFieldRefsToTerm(duckdb::unique_ptr<duckdb::Expression>& expr,
                            irs::field_id field_id,
                            connector::SereneDBScanBindData& bind_data,
                            duckdb::LogicalGet& get,
                            duckdb::idx_t term_get_col_idx) {
  if (expr->GetExpressionClass() ==
      duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    auto& ref = expr->Cast<duckdb::BoundColumnRefExpression>();
    const auto col_id = ResolveColumnId(ref.binding, bind_data, get);
    if (col_id != catalog::Column::kInvalidId &&
        static_cast<irs::field_id>(col_id) == field_id) {
      expr = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
        std::string{catalog::Column::kTermName}, duckdb::LogicalType::VARCHAR,
        duckdb::ColumnBinding{get.table_index,
                              duckdb::ProjectionIndex{term_get_col_idx}});
      return;
    }
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      RewriteFieldRefsToTerm(child, field_id, bind_data, get, term_get_col_idx);
    });
}

void CollectPredicateFields(duckdb::Expression& expr, irs::field_id field,
                            connector::SereneDBScanBindData& bind_data,
                            duckdb::LogicalGet& get, bool& refs_field,
                            bool& refs_other) {
  if (expr.GetExpressionClass() ==
      duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    const auto& ref = expr.Cast<duckdb::BoundColumnRefExpression>();
    const auto col_id = ResolveColumnId(ref.binding, bind_data, get);
    if ((col_id != catalog::Column::kInvalidId &&
         static_cast<irs::field_id>(col_id) == field) ||
        col_id == catalog::Column::kInvertedIndexTermId) {
      refs_field = true;
    } else {
      refs_other = true;
    }
    return;
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      CollectPredicateFields(*child, field, bind_data, get, refs_field,
                             refs_other);
    });
}

// True if `filter` is a single term-acceptor leaf (ByTerm / ByTerms / ByPrefix /
// ByRange / Levenshtein / Automaton) constraining `field`. ts_dict_agg() WHERE
// accepts exactly this shape; null, And/Or/Not and other-field leaves return
// false.
bool IsSingleAcceptor(const irs::Filter* filter, irs::field_id field) {
  using namespace irs;
  if (!filter) {
    return false;
  }
  const auto on = [&](const auto& f) { return f.field_id() == field; };
  if (const auto type = filter->type(); type == Type<ByTerm>::id()) {
    return on(basics::downCast<ByTerm>(*filter));
  } else if (type == Type<ByPrefix>::id()) {
    return on(basics::downCast<ByPrefix>(*filter));
  } else if (type == Type<ByRange>::id()) {
    return on(basics::downCast<ByRange>(*filter));
  } else if (type == Type<ByTerms>::id()) {
    return on(basics::downCast<ByTerms>(*filter));
  } else if (type == Type<LevenshteinAutomatonFilter>::id()) {
    return on(basics::downCast<LevenshteinAutomatonFilter>(*filter));
  } else if (type == Type<AutomatonFilter>::id()) {
    return on(basics::downCast<AutomatonFilter>(*filter));
  } else {
    return false;
  }
}

// ts_dict_agg() WHERE accepts ONE predicate on the single enumerated field: a
// claimable term acceptor (enumerated via seek/automaton) or, failing that, a
// single same-field scalar predicate post-filtered on the emitted term column.
// Boolean combinations (multiple conjuncts / OR / NOT) and cross-field
// references are rejected, to be composed via set operations by the caller.
void ClaimTsDictFilter(
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  duckdb::LogicalGet& get, connector::SereneDBScanBindData& bind_data,
  connector::SearchScan& ss, const catalog::InvertedIndex& index,
  std::shared_ptr<const catalog::Snapshot> snapshot,
  duckdb::ClientContext& context) {
  if (ss.ts_dicts.size() != 1) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("ts_dict_agg() over multiple fields does not support a WHERE "
              "clause"),
      ERR_HINT("constrain each field with a single-field ts_dict_agg subquery"));
  }
  const auto field = ss.ts_dicts.front().field_id;
  const size_t conjuncts = filters.size();
  TryClaimSearchFilter(filters, get, bind_data, index, std::move(snapshot),
                       context);

  if (conjuncts == 1 && filters.empty() &&
      IsSingleAcceptor(ss.stored_filter.get(), field)) {
    return;
  }

  if (conjuncts == 1 && filters.size() == 1 && !ss.stored_filter) {
    auto& expr = filters.front();
    const auto cls = expr->GetExpressionClass();
    const bool boolean =
      cls == duckdb::ExpressionClass::BOUND_CONJUNCTION ||
      (cls == duckdb::ExpressionClass::BOUND_OPERATOR &&
       expr->GetExpressionType() == duckdb::ExpressionType::OPERATOR_NOT);
    bool refs_field = false;
    bool refs_other = false;
    CollectPredicateFields(*expr, field, bind_data, get, refs_field, refs_other);
    if (!boolean && refs_field && !refs_other) {
      auto& req = ss.ts_dicts.front();
      if (req.term_col_idx == duckdb::DConstants::INVALID_INDEX) {
        req.term_col_idx = AppendVirtualGetColumn(
          bind_data, get, catalog::Column::kInvertedIndexTermId,
          duckdb::LogicalType::VARCHAR, catalog::Column::kTermName);
      }
      RewriteFieldRefsToTerm(expr, field, bind_data, get, req.term_col_idx);
      return;
    }
  }

  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
    ERR_MSG("ts_dict_agg() WHERE accepts a single predicate on the enumerated "
            "field (=, IN, LIKE, BETWEEN, length, <>, or @@ ts_*)"),
    ERR_HINT("combine conditions with UNION/INTERSECT/EXCEPT over "
             "unnest(ts_dict_agg(...)), or filter terms in an outer query"));
}

}  // namespace

void RewriteSearchCallsToColumnRefs(
  duckdb::OptimizerExtensionInput& input,
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  RewriteIResearchExpressions(input.context, plan, plan, false,
                              input.optimizer.binder);
}

void IResearchPushdownComplexFilter(
  duckdb::ClientContext& context, duckdb::LogicalGet& get,
  duckdb::FunctionData* bind_data_ptr,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters) {
  if (filters.empty() || bind_data_ptr == nullptr) {
    return;
  }
  auto& conn_ctx = connector::GetSereneDBContext(context);
  auto& bind_data = bind_data_ptr->Cast<connector::SereneDBScanBindData>();
  if (bind_data.scan_source->Kind() != connector::ScanSourceKind::Search) {
    return;
  }
  auto& ss = bind_data.scan_source->Cast<connector::SearchScan>();
  if (ss.TsDictMode()) {
    auto index = bind_data.inverted_index;
    auto snapshot = conn_ctx.EnsureCatalogSnapshot();
    ClaimTsDictFilter(filters, get, bind_data, ss, *index, std::move(snapshot),
                        context);
    return;
  }
  if (ss.stored_filter) {
    return;
  }
  TryClaimAnnRange(filters, get, bind_data, context);
  if (filters.empty()) {
    return;
  }
  auto index = bind_data.inverted_index;
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  TryClaimSearchFilter(filters, get, bind_data, *index, snapshot, context);
}

void RegisterIResearchPlanOptimizer(duckdb::DatabaseInstance& db) {
  duckdb::OptimizerExtension::Register(
    db.config, duckdb::OptimizerExtension{
                 .rule = &RewriteSearchCallsToColumnRefs,
                 .anchor = duckdb::OptimizerType::FILTER_PUSHDOWN,
                 .where = duckdb::OptimizerHookPosition::Before,
               });
}

}  // namespace sdb::optimizer
