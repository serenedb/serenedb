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

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/function/function_binder.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/optimizer/column_binding_replacer.hpp>
#include <duckdb/optimizer/optimizer.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_aggregate_expression.hpp>
#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression/bound_unnest_expression.hpp>
#include <duckdb/planner/expression/bound_window_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_cross_product.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_join.hpp>
#include <duckdb/planner/operator/logical_order.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_top_n.hpp>
#include <duckdb/planner/operator/logical_unnest.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/automaton_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/filter_optimizer.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/proxy_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>
#include <numeric>
#include <optional>
#include <span>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
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
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/search/mixed_boolean_filter.hpp"
#include "iresearch/search/optimizer/boolean_rules.hpp"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::optimizer {
namespace {

bool TryFoldExpression(duckdb::ClientContext& context, duckdb::Expression& expr,
                       duckdb::Value& out) {
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONSTANT) {
    out = expr.Cast<duckdb::BoundConstantExpression>().GetValue();
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
    if (const auto id =
          bind_data.ColumnIdByName(col_arg.GetName().GetIdentifierName());
        id != catalog::Column::kInvalidId) {
      return id;
    }
  }
  if (SingleReferencedTableIndex(col_arg) != get.table_index) {
    return irs::field_limits::invalid();
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

struct FoundScanColumn {
  FoundScan found;
  duckdb::ColumnBinding binding;
};

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
    binding = forwarded.Cast<duckdb::BoundColumnRefExpression>().Binding();
  }
  return binding;
}

std::optional<FoundScanColumn> ResolveIResearchScanColumn(
  duckdb::LogicalOperator& root, duckdb::ColumnBinding binding) {
  const auto resolved = ResolveBindingThroughProjections(root, binding);
  auto found = FindIResearchScan(root, resolved.table_index);
  if (!found) {
    return std::nullopt;
  }
  return FoundScanColumn{*found, resolved};
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
    if (ref.Binding().table_index == target_get.table_index &&
        ref.Binding().column_index.GetIndex() == get_col_idx) {
      return {proj->table_index, duckdb::ProjectionIndex{i}};
    }
  }
  proj->expressions.push_back(
    duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      duckdb::Identifier{col_name}, col_type,
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
    duckdb::Identifier{catalog::Column::kScoreName}, duckdb::LogicalType::FLOAT,
    binding);
}

bool IsScorerFunctionName(std::string_view name) {
  using S = catalog::ScorerOptions;
  static const containers::FlatHashSet<std::string_view> kScorerNames{
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
  const auto binding = ResolveBindingThroughProjections(root, ref.Binding());
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
  if (func.GetChildren().empty() ||
      func.GetChildren()[0]->GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return nullptr;
  }
  auto& anchor =
    func.GetChildren()[0]->Cast<duckdb::BoundColumnRefExpression>();
  auto found = FindIResearchScan(root, anchor.Binding().table_index);
  if (!found) {
    return nullptr;
  }
  auto& ss = *found->scan;
  if (ss.vector_scorer) {
    return nullptr;
  }
  if (!TrySetScorer(ss.text_scorer, func,
                    func.Function().GetName().GetIdentifierName())) {
    return nullptr;
  }
  auto ref = MakeScoreRefExpression(root, *found, anchor.Binding().table_index);
  if (!func.GetAlias().empty()) {
    ref->SetAlias(func.GetAlias());
  }
  return ref;
}

uint32_t ReadNprobe(duckdb::ClientContext& context) {
  return connector::ReadBoundedIntSetting(context, "sdb_nprobe", 1, 1);
}

duckdb::unique_ptr<duckdb::Expression> PushdownDistanceCall(
  duckdb::BoundFunctionExpression& func, const connector::AnnFunctionInfo& info,
  duckdb::LogicalOperator& root, duckdb::ClientContext& context) {
  const auto [col_arg, value_arg] =
    [&] -> std::pair<duckdb::Expression*, duckdb::Expression*> {
    if (info.is_norm) {
      if (func.GetChildren().empty() || func.GetChildren()[0]->IsFoldable()) {
        return {nullptr, nullptr};
      }
      return {func.GetChildren()[0].get(), nullptr};
    }
    if (func.GetChildren().size() != 2) {
      return {nullptr, nullptr};
    }
    auto& lhs = func.GetChildren()[0];
    auto& rhs = func.GetChildren()[1];
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
  auto ann_info = index->GetIvfInfo(call_field_id);
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
      .centroids_id = ann_info->centroids_id,
      .postings_id = ann_info->postings_id,
      .quant = ann_info->quant.kind,
      .nprobe = ReadNprobe(context),
    };
    ss.score_order = irs::VectorMetricNearestIsLargest(info.metric)
                       ? duckdb::OrderType::DESCENDING
                       : duckdb::OrderType::ASCENDING;
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
  if (func.GetChildren().size() != 1 && func.GetChildren().size() != 2) {
    return nullptr;
  }
  if (func.GetChildren()[0]->GetExpressionClass() !=
      duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets() first argument must be a column reference"));
  }
  const auto& col_ref =
    func.GetChildren()[0]->Cast<duckdb::BoundColumnRefExpression>();

  const auto limit = [&] -> size_t {
    constexpr size_t kDefaultOffsetsLimit = 1 << 12;
    if (func.GetChildren().size() != 2) {
      return kDefaultOffsetsLimit;
    }
    auto& arg1 = *func.GetChildren()[1];
    if (arg1.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("ts_offsets() second argument must be an integer literal"));
    }
    const auto raw = arg1.Cast<duckdb::BoundConstantExpression>()
                       .GetValue()
                       .GetValue<int32_t>();
    if (raw < 0) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("ts_offsets() limit must be greater than zero or 0 for no "
                "limit"));
    }
    return raw == 0 ? std::numeric_limits<size_t>::max() : raw;
  }();

  auto resolved = ResolveIResearchScanColumn(root, col_ref.Binding());
  if (!resolved) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(", col_ref.GetAlias().GetIdentifierName(),
              ") requires an inverted index scan in the same sub-query"));
  }
  auto& found = resolved->found;
  auto& search_scan = *found.scan;

  const auto col_name = [&] -> std::string_view {
    if (const auto& cids = found.get->GetColumnIds();
        resolved->binding.column_index.GetIndex() < cids.size()) {
      return found.get
        ->GetColumnName(cids[resolved->binding.column_index.GetIndex()])
        .GetIdentifierName();
    }
    return col_ref.GetAlias().GetIdentifierName();
  };

  const auto target_col_id =
    ResolveColumnId(resolved->binding, *found.bind_data, *found.get);
  if (target_col_id == catalog::Column::kInvalidId) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(): column '", col_name(), "' not found in table"));
  }

  const auto* col_info =
    found.bind_data->inverted_index->FindColumnInfo(target_col_id);
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
    bind->inverted_index = found.bind_data->inverted_index;
    bind->column_id = target_col_id;
    bind->limit = limit;
    search_scan.offsets.push_back(
      {.column_id = target_col_id, .limit = limit, .bind = bind.get()});
    func.BindInfoMutable() = std::move(bind);
    func.FunctionMutable().SetFunctionCallback(connector::OffsetsScalarFn);
    auto body_expr = std::move(func.GetChildrenMutable()[0]);
    func.GetChildrenMutable().clear();
    func.GetChildrenMutable().emplace_back(
      duckdb::make_uniq<duckdb::BoundConstantExpression>(
        duckdb::Value{std::string{}}));
    func.GetChildrenMutable().emplace_back(std::move(body_expr));
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
      *found.bind_data, *found.get, catalog::Column::kInvertedIndexOffsetsId,
      col_type, offsets_col_name);
    search_scan.offsets.push_back(
      {.column_id = target_col_id, .limit = limit, .get_col_idx = get_col_idx});
  }
  const auto binding =
    ExposeGetColumnAt(root, col_ref.Binding().table_index, *found.get,
                      get_col_idx, offsets_col_name, col_type);
  auto out = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    duckdb::Identifier{offsets_col_name}, col_type, binding);
  out->SetAlias(func.GetAlias());
  return out;
}

enum class TsDictColKind { Term, TermRaw, Count, Freq, Score };

using TsDictReq = connector::SearchScan::TsDictRequest;

constexpr auto TsDictColFor(TsDictColKind kind)
  -> std::tuple<catalog::Column::Id, duckdb::LogicalTypeId,
                duckdb::idx_t TsDictReq::*> {
  using C = catalog::Column;
  switch (kind) {
    case TsDictColKind::Term:
      return {C::kInvertedIndexTermId, duckdb::LogicalTypeId::VARCHAR,
              &TsDictReq::term_col_idx};
    case TsDictColKind::TermRaw:
      return {C::kInvertedIndexTermRawId, duckdb::LogicalTypeId::BLOB,
              &TsDictReq::term_raw_col_idx};
    case TsDictColKind::Count:
      return {C::kInvertedIndexTermCountId, duckdb::LogicalTypeId::INTEGER,
              &TsDictReq::count_col_idx};
    case TsDictColKind::Freq:
      return {C::kInvertedIndexTermFreqId, duckdb::LogicalTypeId::BIGINT,
              &TsDictReq::freq_col_idx};
    case TsDictColKind::Score:
      return {C::kInvertedIndexTermScoreId, duckdb::LogicalTypeId::FLOAT,
              &TsDictReq::score_col_idx};
  }
  return {};
}

std::string_view TsDictColPrefix(TsDictColKind kind) {
  using C = catalog::Column;
  switch (kind) {
    case TsDictColKind::Term:
      return C::kTermName;
    case TsDictColKind::TermRaw:
      return C::kTermRawName;
    case TsDictColKind::Count:
      return C::kTermCountName;
    case TsDictColKind::Freq:
      return C::kTermFreqName;
    case TsDictColKind::Score:
      return C::kTermScoreName;
  }
  return {};
}

std::string TsDictColName(const connector::SereneDBScanBindData& bind_data,
                          irs::field_id field_id, TsDictColKind kind) {
  const auto field =
    bind_data.ColumnNameById(static_cast<catalog::Column::Id>(field_id));
  if (field.empty()) {
    return absl::StrCat(TsDictColPrefix(kind), "col", field_id);
  }
  return absl::StrCat(TsDictColPrefix(kind), field);
}

constexpr std::optional<std::pair<TsDictColKind, std::string_view>> TsDictFnFor(
  std::string_view fn) {
  using Fn = std::pair<TsDictColKind, std::string_view>;
  if (fn == connector::kTsDictAgg) {
    return Fn{TsDictColKind::Term, "list"};
  }
  if (fn == connector::kTsDictRawAgg) {
    return Fn{TsDictColKind::TermRaw, "list"};
  }
  if (fn == connector::kTsDictCount) {
    return Fn{TsDictColKind::Count, "list"};
  }
  if (fn == connector::kTsDictFreq) {
    return Fn{TsDictColKind::Freq, "list"};
  }
  if (fn == connector::kTsDictScore) {
    return Fn{TsDictColKind::Score, "list"};
  }
  if (fn == connector::kTsDictMin) {
    return Fn{TsDictColKind::Term, "min"};
  }
  if (fn == connector::kTsDictMax) {
    return Fn{TsDictColKind::Term, "max"};
  }
  return std::nullopt;
}

duckdb::idx_t EnsureTsDictCol(connector::SereneDBScanBindData& bind_data,
                              duckdb::LogicalGet& get, TsDictReq& req,
                              TsDictColKind kind) {
  const auto [virtual_id, type, member] = TsDictColFor(kind);
  auto& col_idx = req.*member;
  if (col_idx == duckdb::DConstants::INVALID_INDEX) {
    col_idx = AppendVirtualGetColumn(
      bind_data, get, virtual_id, duckdb::LogicalType{type},
      TsDictColName(bind_data, req.field_id, kind));
  }
  return col_idx;
}

duckdb::unique_ptr<duckdb::Expression> BuildTsDictAggregate(
  duckdb::ClientContext& context, std::string_view agg_name,
  duckdb::unique_ptr<duckdb::Expression> child) {
  auto& sys = duckdb::Catalog::GetSystemCatalog(context);
  auto& entry = sys.GetEntry<duckdb::AggregateFunctionCatalogEntry>(
    context, duckdb::QualifiedName(sys.GetName(), DEFAULT_SCHEMA,
                                   duckdb::Identifier{std::string{agg_name}}));
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
  children.push_back(std::move(child));
  duckdb::FunctionBinder binder{context};
  duckdb::ErrorData error;
  const auto best =
    binder.BindFunction(duckdb::Identifier{std::string{agg_name}},
                        entry.functions, children, {}, error);
  if (!best.IsValid()) {
    error.Throw();
  }
  auto fn = entry.functions.GetFunctionByOffset(best.GetIndex());
  return binder.BindAggregateFunction(fn, std::move(children));
}

// With several fields the scan emits each field's terms in its own rows and
// leaves other fields' columns NULL; the list aggregate must skip those.
// count/min/max already ignore NULLs.
duckdb::unique_ptr<duckdb::Expression> BuildTsDictListAggregate(
  duckdb::ClientContext& context, std::string_view name,
  const duckdb::LogicalType& type, duckdb::ColumnBinding binding) {
  auto ref = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    duckdb::Identifier{std::string{name}}, type, binding);
  auto result = BuildTsDictAggregate(context, "list", std::move(ref));
  auto filter_ref = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    duckdb::Identifier{std::string{name}}, type, binding);
  auto is_not_null = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
    duckdb::ExpressionType::OPERATOR_IS_NOT_NULL, duckdb::LogicalType::BOOLEAN);
  is_not_null->GetChildrenMutable().push_back(std::move(filter_ref));
  result->Cast<duckdb::BoundAggregateExpression>().GetFilterMutable() =
    std::move(is_not_null);
  return result;
}

duckdb::unique_ptr<duckdb::Expression> MakeTsDictAggregate(
  duckdb::LogicalOperator& root, duckdb::ClientContext& context,
  FoundScan& found, irs::field_id field_id, TsDictColKind kind,
  std::string_view agg_name, duckdb::TableIndex table_index,
  const duckdb::Identifier& alias) {
  auto& req = found.scan->TsDictFor(field_id);
  const auto [virtual_id, type, member] = TsDictColFor(kind);
  const duckdb::LogicalType col_type{type};
  const auto col_idx = EnsureTsDictCol(*found.bind_data, *found.get, req, kind);
  using enum connector::TsDictTermUses;
  req.term_uses |= agg_name == "min" ? kMin : agg_name == "max" ? kMax : kFull;
  const auto name = TsDictColName(*found.bind_data, field_id, kind);
  const auto binding =
    ExposeGetColumnAt(root, table_index, *found.get, col_idx, name, col_type);
  auto result = agg_name == "list"
                  ? BuildTsDictListAggregate(context, name, col_type, binding)
                  : BuildTsDictAggregate(
                      context, agg_name,
                      duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
                        duckdb::Identifier{name}, col_type, binding));
  result->SetAlias(alias);
  return result;
}

duckdb::unique_ptr<duckdb::Expression> PushdownTsDictCall(
  duckdb::BoundAggregateExpression& agg, duckdb::LogicalOperator& root,
  duckdb::ClientContext& context) {
  const auto fn = agg.Function().GetName().GetIdentifierName();
  const auto fn_info = TsDictFnFor(fn);
  SDB_ASSERT(fn_info.has_value());
  const auto& children = agg.GetChildren();
  if (children.size() != 1 || children[0]->GetExpressionClass() !=
                                duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(fn, "() argument must be an indexed column"));
  }
  const auto& col_ref = children[0]->Cast<duckdb::BoundColumnRefExpression>();
  auto resolved = ResolveIResearchScanColumn(root, col_ref.Binding());
  if (!resolved) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(fn, "() requires an inverted index scan in the same sub-query"));
  }
  auto& found = resolved->found;
  const auto col_id =
    ResolveColumnId(resolved->binding, *found.bind_data, *found.get);
  if (col_id == catalog::Column::kInvalidId) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(fn, "(): column not found in index"));
  }
  const auto* info = found.bind_data->inverted_index->FindColumnInfo(col_id);
  if (!info || !info->IsTermDict() ||
      col_ref.GetReturnType().id() != duckdb::LogicalTypeId::VARCHAR) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG(fn, "(): column has no text term dictionary"),
      ERR_HINT(
        "ts_dict_agg requires a text-tokenized or keyword VARCHAR column"));
  }

  const auto [kind, agg_name] = *fn_info;
  return MakeTsDictAggregate(root, context, found,
                             static_cast<irs::field_id>(col_id), kind, agg_name,
                             col_ref.Binding().table_index, agg.GetAlias());
}

struct TsDictColRef {
  size_t req_index;
  TsDictColKind kind;
};

std::optional<TsDictColRef> ClassifyTsDictGetCol(
  const connector::SearchScan& ss, duckdb::idx_t get_col_idx) {
  for (size_t r = 0; r < ss.ts_dicts.size(); ++r) {
    for (const auto kind :
         {TsDictColKind::Term, TsDictColKind::TermRaw, TsDictColKind::Count,
          TsDictColKind::Freq, TsDictColKind::Score}) {
      const auto [virtual_id, type, member] = TsDictColFor(kind);
      if (get_col_idx == ss.ts_dicts[r].*member) {
        return TsDictColRef{r, kind};
      }
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

connector::SearchScan* FindTsDictScan(duckdb::LogicalOperator& op) {
  const auto f = FindTsDictFoundScan(op);
  return f ? f->scan : nullptr;
}

template<typename F>
void WalkColumnRefs(duckdb::unique_ptr<duckdb::Expression>& expr, F&& fn) {
  if (expr->GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
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
      if (find_entry(ref.Binding())) {
        return;
      }
      const auto resolved =
        ResolveBindingThroughProjections(*child, ref.Binding());
      if (resolved.table_index != get_ti) {
        return;
      }
      const auto col =
        ClassifyTsDictGetCol(ss, resolved.column_index.GetIndex());
      if (!col) {
        return;
      }
      entries.push_back({.source = ref.Binding(),
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
      if (e.kind == TsDictColKind::Term || e.kind == TsDictColKind::TermRaw) {
        has_group = true;
      }
    }
    if (!referenced || has_group) {
      continue;
    }
    auto& req = ss.ts_dicts[r];
    EnsureTsDictCol(*found->bind_data, *found->get, req, TsDictColKind::Term);
    req.term_uses |= connector::TsDictTermUses::kFull;
    entries.push_back({.source =
                         duckdb::ColumnBinding{
                           get_ti, duckdb::ProjectionIndex{req.term_col_idx}},
                       .source_type = duckdb::LogicalType::VARCHAR,
                       .kind = TsDictColKind::Term,
                       .req_index = r});
  }

  const auto group_index = binder.GenerateTableIndex();
  const auto agg_index = binder.GenerateTableIndex();
  const auto groupings_index = binder.GenerateTableIndex();

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> groups;
  for (auto& e : entries) {
    if (e.kind != TsDictColKind::Term && e.kind != TsDictColKind::TermRaw) {
      continue;
    }
    e.output = {group_index, duckdb::ProjectionIndex{groups.size()}};
    e.output_type = e.source_type;
    groups.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      e.source_type, e.source));
  }
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> select_list;
  for (auto& e : entries) {
    if (e.kind == TsDictColKind::Term || e.kind == TsDictColKind::TermRaw) {
      continue;
    }
    auto agg = BuildTsDictAggregate(
      context, e.kind == TsDictColKind::Score ? "max" : "sum",
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
      auto* e = find_entry(ref.Binding());
      if (!e) {
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
    if (ba.Function().GetName() != "list" || ba.GetChildren().size() != 1) {
      return;
    }
    const auto* inner = ba.GetChildren()[0].get();
    if (inner->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CAST) {
      inner = &inner->Cast<duckdb::BoundCastExpression>().Child();
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
  if (!scan || scan->ts_dicts.size() != 1) {
    return;
  }

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> proj_exprs;
  proj_exprs.reserve(unnest.expressions.size());
  for (auto& u : unnest.expressions) {
    if (u->GetExpressionClass() != duckdb::ExpressionClass::BOUND_UNNEST) {
      return;
    }
    auto& bu = u->Cast<duckdb::BoundUnnestExpression>();
    if (bu.Child()->GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_COLUMN_REF) {
      return;
    }
    const auto& ref = bu.Child()->Cast<duckdb::BoundColumnRefExpression>();
    if (ref.Binding().table_index != agg.aggregate_index) {
      return;
    }
    const auto k = ref.Binding().column_index.GetIndex();
    if (k >= agg.expressions.size()) {
      return;
    }
    auto& ba = agg.expressions[k]->Cast<duckdb::BoundAggregateExpression>();
    proj_exprs.push_back(std::move(ba.GetChildrenMutable()[0]));
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

bool ColumnIsNotNull(const catalog::Table& table, catalog::Column::Id col_id) {
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

struct KeywordDictAgg {
  duckdb::ColumnBinding binding;
  irs::field_id field_id;
  std::string_view agg;
};

void CollectPredicateFields(duckdb::Expression& expr, irs::field_id field,
                            connector::SereneDBScanBindData& bind_data,
                            duckdb::LogicalGet& get, bool& refs_field,
                            bool& refs_other);

// True if the expression tree contains a TSQUERY-typed node, i.e. an
// optimizer-claimed @@ ts_* matcher with no scalar fallback: such a predicate
// can only run as a claimed search filter, never as a post-filter.
bool ContainsTSQueryExpr(const duckdb::Expression& expr) {
  if (const auto& type = expr.GetReturnType();
      type.id() == duckdb::LogicalTypeId::VARCHAR) {
    const auto alias = type.GetAlias();
    if (alias == connector::kTSQueryTypeName ||
        alias == connector::kTokenizedTSQueryTypeName ||
        alias == connector::kBoostedTSQueryTypeName) {
      return true;
    }
  }
  bool found = false;
  duckdb::ExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::Expression& child) {
      found = found || ContainsTSQueryExpr(child);
    });
  return found;
}

// The implicit rewrites below are only sound for a bare `SELECT <aggs> FROM
// idx` shape, optionally with ONE filter whose every conjunct references only
// the enumerated field (the ts_dict claim then turns it into a term acceptor
// or a term post-filter). Anything else between the aggregate and the scan
// consumes document rows, which the term-dict scan no longer produces.
std::pair<duckdb::LogicalGet*, duckdb::LogicalFilter*> ImplicitTsDictTarget(
  duckdb::LogicalOperator& aggr) {
  auto* node = &aggr;
  duckdb::LogicalFilter* filter = nullptr;
  while (node->children.size() == 1) {
    auto* child = node->children[0].get();
    if (child->type == duckdb::LogicalOperatorType::LOGICAL_GET) {
      return {&child->Cast<duckdb::LogicalGet>(), filter};
    }
    if (child->type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
      if (filter) {
        return {nullptr, nullptr};
      }
      filter = &child->Cast<duckdb::LogicalFilter>();
    } else if (child->type != duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
      return {nullptr, nullptr};
    }
    node = child;
  }
  return {nullptr, nullptr};
}

// `array_agg(DISTINCT col)`, `count(DISTINCT col)`, `min(col)` and `max(col)`
// over a keyword-analyzed inverted-index column reduce exactly the field's
// distinct terms, so they can be served from the term dictionary. Only
// list()/array_agg requires a NOT NULL column: it keeps a NULL element for
// missing values while the term dictionary has none; count/min/max skip NULLs.
std::optional<KeywordDictAgg> ClassifyKeywordDictAgg(
  duckdb::BoundAggregateExpression& agg, duckdb::LogicalOperator& root,
  const duckdb::LogicalGet& target, const catalog::Snapshot& snapshot) {
  const auto& name = agg.Function().GetName();
  const bool is_list = name == "array_agg" || name == "list";
  const bool is_count = name == "count";
  const bool is_min = name == "min";
  const bool is_max = name == "max";
  if (!is_list && !is_count && !is_min && !is_max) {
    return std::nullopt;
  }
  if ((is_list || is_count) && !agg.IsDistinct()) {
    return std::nullopt;
  }
  if (agg.GetFilter() || agg.GetOrderBys()) {
    return std::nullopt;
  }
  const auto& children = agg.GetChildren();
  if (children.size() != 1 || children[0]->GetExpressionClass() !=
                                duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return std::nullopt;
  }
  const auto& col_ref = children[0]->Cast<duckdb::BoundColumnRefExpression>();
  if (col_ref.GetReturnType().id() != duckdb::LogicalTypeId::VARCHAR) {
    return std::nullopt;
  }
  const auto resolved =
    ResolveBindingThroughProjections(root, col_ref.Binding());
  auto found = FindIResearchScan(root, resolved.table_index);
  if (!found || found->get != &target ||
      found->bind_data->GetKind() !=
        connector::SereneDBScanBindData::Kind::Table) {
    return std::nullopt;
  }
  const auto col_id = ResolveColumnId(resolved, *found->bind_data, *found->get);
  if (col_id == catalog::Column::kInvalidId) {
    return std::nullopt;
  }
  const auto* index = found->bind_data->inverted_index.get();
  if (!index) {
    return std::nullopt;
  }
  const auto field_id = static_cast<irs::field_id>(col_id);
  if (!index->IsKeywordField(snapshot, field_id)) {
    return std::nullopt;
  }
  if (is_list) {
    const auto& tbd = found->bind_data->As<connector::TableScanBindData>();
    if (!tbd.table || !ColumnIsNotNull(*tbd.table, col_id)) {
      return std::nullopt;
    }
  }
  return KeywordDictAgg{col_ref.Binding(), field_id,
                        is_list    ? std::string_view{"list"}
                        : is_count ? std::string_view{"count"}
                        : is_min   ? std::string_view{"min"}
                                   : std::string_view{"max"}};
}

duckdb::unique_ptr<duckdb::Expression> BuildKeywordTsDictAggregate(
  const KeywordDictAgg& match, duckdb::LogicalOperator& root,
  duckdb::ClientContext& context, const duckdb::Identifier& alias) {
  auto resolved = ResolveIResearchScanColumn(root, match.binding);
  SDB_ASSERT(resolved.has_value());
  return MakeTsDictAggregate(root, context, resolved->found, match.field_id,
                             TsDictColKind::Term, match.agg,
                             match.binding.table_index, alias);
}

// `SELECT col, count(*) FROM idx GROUP BY col` over a keyword NOT NULL column
// is a facet: the groups are exactly the field's live terms and each group's
// size is the term's live doc count. The group key is repointed at the term
// column, every count(*) / count(col) becomes sum(term_count) (the user's own
// GROUP BY merges the per-segment rows), and a projection above casts the sums
// back to count's BIGINT. A bare GROUP BY with no aggregates converts too.
bool TryPushdownTsDictFacet(duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
                            duckdb::LogicalOperator& root,
                            duckdb::LogicalGet& target,
                            duckdb::LogicalFilter* where_filter,
                            duckdb::Binder& binder,
                            duckdb::ClientContext& context) {
  auto& aggr = plan->Cast<duckdb::LogicalAggregate>();
  if (aggr.groups.size() != 1 || aggr.grouping_sets.size() > 1) {
    return false;
  }
  if (aggr.groups[0]->GetExpressionClass() !=
      duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return false;
  }
  auto& group_ref = aggr.groups[0]->Cast<duckdb::BoundColumnRefExpression>();
  if (group_ref.GetReturnType().id() != duckdb::LogicalTypeId::VARCHAR) {
    return false;
  }
  const auto resolved =
    ResolveBindingThroughProjections(root, group_ref.Binding());
  auto found = FindIResearchScan(root, resolved.table_index);
  if (!found || found->get != &target ||
      found->bind_data->GetKind() !=
        connector::SereneDBScanBindData::Kind::Table) {
    return false;
  }
  const auto col_id = ResolveColumnId(resolved, *found->bind_data, *found->get);
  if (col_id == catalog::Column::kInvalidId) {
    return false;
  }
  const auto* index = found->bind_data->inverted_index.get();
  if (!index) {
    return false;
  }
  const auto field_id = static_cast<irs::field_id>(col_id);
  const auto snapshot =
    connector::GetSereneDBContext(context).AcquireCatalogSnapshot();
  if (!index->IsKeywordField(*snapshot, field_id)) {
    return false;
  }
  const auto& tbd = found->bind_data->As<connector::TableScanBindData>();
  if (!tbd.table) {
    return false;
  }
  // Nullable columns form a NULL group the dictionary has no term for; the
  // scan synthesizes it from the column's null-marker field, which is only
  // sound for a bare facet (any same-field WHERE excludes NULL rows in SQL,
  // and count(col) counts them as zero while sum(term_count) would not).
  const bool not_null = ColumnIsNotNull(*tbd.table, col_id);
  auto null_field_id = irs::field_limits::invalid();
  if (!not_null) {
    if (where_filter) {
      return false;
    }
    const auto* col_info = index->FindColumnInfo(col_id);
    if (!col_info || !irs::field_limits::valid(col_info->null_field_id)) {
      return false;
    }
    null_field_id = col_info->null_field_id;
  }

  for (auto& expr : aggr.expressions) {
    if (expr->GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_AGGREGATE) {
      return false;
    }
    auto& agg = expr->Cast<duckdb::BoundAggregateExpression>();
    if (agg.IsDistinct() || agg.GetFilter() || agg.GetOrderBys()) {
      return false;
    }
    const auto& name = agg.Function().GetName();
    if (name == "count_star") {
      continue;
    }
    const auto& children = agg.GetChildren();
    if (name != "count" || children.size() != 1 ||
        children[0]->GetExpressionClass() !=
          duckdb::ExpressionClass::BOUND_COLUMN_REF) {
      return false;
    }
    if (!not_null) {
      return false;
    }
    const auto child = ResolveBindingThroughProjections(
      root, children[0]->Cast<duckdb::BoundColumnRefExpression>().Binding());
    if (ResolveColumnId(child, *found->bind_data, *found->get) != col_id) {
      return false;
    }
  }

  if (where_filter) {
    for (auto& expr : where_filter->expressions) {
      bool refs_field = false;
      bool refs_other = false;
      CollectPredicateFields(*expr, field_id, *found->bind_data, *found->get,
                             refs_field, refs_other);
      if (!refs_field || refs_other || ContainsTSQueryExpr(*expr)) {
        return false;
      }
    }
  }

  auto& req = found->scan->TsDictFor(field_id);
  EnsureTsDictCol(*found->bind_data, *found->get, req, TsDictColKind::Term);
  EnsureTsDictCol(*found->bind_data, *found->get, req, TsDictColKind::Count);
  req.term_uses |= connector::TsDictTermUses::kFull;
  req.null_field_id = null_field_id;

  const auto term_name =
    TsDictColName(*found->bind_data, field_id, TsDictColKind::Term);
  const auto count_name =
    TsDictColName(*found->bind_data, field_id, TsDictColKind::Count);
  const auto anchor = group_ref.Binding().table_index;
  const auto term_binding =
    ExposeGetColumnAt(root, anchor, *found->get, req.term_col_idx, term_name,
                      duckdb::LogicalType::VARCHAR);
  const auto count_binding =
    ExposeGetColumnAt(root, anchor, *found->get, req.count_col_idx, count_name,
                      duckdb::LogicalType::INTEGER);

  aggr.groups[0] = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    duckdb::Identifier{term_name}, duckdb::LogicalType::VARCHAR, term_binding);

  std::vector<duckdb::LogicalType> old_types;
  for (auto& expr : aggr.expressions) {
    old_types.push_back(expr->GetReturnType());
    expr =
      BuildTsDictAggregate(context, "sum",
                           duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
                             duckdb::Identifier{count_name},
                             duckdb::LogicalType::INTEGER, count_binding));
  }
  aggr.group_stats.clear();
  aggr.group_stats.resize(aggr.groups.size());
  aggr.ResolveOperatorTypes();

  if (!old_types.empty()) {
    const auto proj_index = binder.GenerateTableIndex();
    duckdb::ColumnBindingReplacer replacer;
    replacer.replacement_bindings.emplace_back(
      duckdb::ColumnBinding{aggr.group_index, duckdb::ProjectionIndex{0}},
      duckdb::ColumnBinding{proj_index, duckdb::ProjectionIndex{0}});
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> proj_exprs;
    proj_exprs.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      duckdb::Identifier{term_name}, duckdb::LogicalType::VARCHAR,
      duckdb::ColumnBinding{aggr.group_index, duckdb::ProjectionIndex{0}}));
    for (size_t i = 0; i < old_types.size(); ++i) {
      replacer.replacement_bindings.emplace_back(
        duckdb::ColumnBinding{aggr.aggregate_index, duckdb::ProjectionIndex{i}},
        duckdb::ColumnBinding{proj_index, duckdb::ProjectionIndex{i + 1}});
      duckdb::unique_ptr<duckdb::Expression> sum_ref =
        duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
          aggr.expressions[i]->GetReturnType(),
          duckdb::ColumnBinding{aggr.aggregate_index,
                                duckdb::ProjectionIndex{i}});
      proj_exprs.push_back(duckdb::BoundCastExpression::AddCastToType(
        context, std::move(sum_ref), old_types[i]));
    }
    replacer.stop_operator = plan.get();
    replacer.VisitOperator(root);

    auto proj = duckdb::make_uniq<duckdb::LogicalProjection>(
      proj_index, std::move(proj_exprs));
    for (auto& e : proj->expressions) {
      proj->types.push_back(e->GetReturnType());
    }
    proj->children.push_back(std::move(plan));
    plan = std::move(proj);
  }
  return true;
}

void RemapColumnRefs(
  duckdb::Expression& expr,
  const std::vector<std::pair<duckdb::TableIndex, duckdb::TableIndex>>& map) {
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    auto& ref = expr.Cast<duckdb::BoundColumnRefExpression>();
    for (const auto& [from, to] : map) {
      if (ref.Binding().table_index == from) {
        ref.BindingMutable().table_index = to;
        break;
      }
    }
    return;
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    expr, [&](duckdb::Expression& child) { RemapColumnRefs(child, map); });
}

duckdb::unique_ptr<duckdb::LogicalOperator> CopyTsDictSourceChain(
  duckdb::LogicalOperator& top, duckdb::Binder& binder,
  std::vector<std::pair<duckdb::TableIndex, duckdb::TableIndex>>& index_map) {
  if (top.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    auto& get = top.Cast<duckdb::LogicalGet>();
    if (get.table_filters.HasFilters()) {
      return nullptr;
    }
    auto copy = duckdb::make_uniq<duckdb::LogicalGet>(
      binder.GenerateTableIndex(), get.function, get.bind_data->Copy(),
      get.returned_types, get.names, get.virtual_columns);
    copy->GetMutableColumnIds() = get.GetColumnIds();
    index_map.emplace_back(get.table_index, copy->table_index);
    return copy;
  }
  if (top.children.size() != 1) {
    return nullptr;
  }
  auto child = CopyTsDictSourceChain(*top.children[0], binder, index_map);
  if (!child) {
    return nullptr;
  }
  if (top.type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
    auto& filter = top.Cast<duckdb::LogicalFilter>();
    if (!filter.projection_map.empty()) {
      return nullptr;
    }
    auto copy = duckdb::make_uniq<duckdb::LogicalFilter>();
    for (const auto& e : filter.expressions) {
      auto expr = e->Copy();
      RemapColumnRefs(*expr, index_map);
      copy->expressions.push_back(std::move(expr));
    }
    copy->children.push_back(std::move(child));
    return copy;
  }
  if (top.type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    auto& proj = top.Cast<duckdb::LogicalProjection>();
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> exprs;
    for (const auto& e : proj.expressions) {
      auto expr = e->Copy();
      RemapColumnRefs(*expr, index_map);
      exprs.push_back(std::move(expr));
    }
    auto copy = duckdb::make_uniq<duckdb::LogicalProjection>(
      binder.GenerateTableIndex(), std::move(exprs));
    index_map.emplace_back(proj.table_index, copy->table_index);
    copy->children.push_back(std::move(child));
    return copy;
  }
  return nullptr;
}

// Mixed ts_dict_* + ordinary aggregates over one scan: duplicate the source
// chain for the ordinary aggregates, keep the dict aggregates on the original
// (to be converted to term-dict enumeration by the caller), cross-join the
// two single-row results and restore the original output order.
bool TrySplitMixedTsDictAggregates(
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
  duckdb::LogicalOperator& root, duckdb::Binder& binder,
  std::vector<size_t>& ts_dict_calls) {
  auto& aggr = plan->Cast<duckdb::LogicalAggregate>();
  if (!aggr.groups.empty() || aggr.children.size() != 1) {
    return false;
  }
  std::vector<std::pair<duckdb::TableIndex, duckdb::TableIndex>> index_map;
  auto doc_child = CopyTsDictSourceChain(*aggr.children[0], binder, index_map);
  if (!doc_child) {
    return false;
  }

  const size_t n = aggr.expressions.size();
  std::vector<bool> is_dict(n, false);
  for (const auto i : ts_dict_calls) {
    is_dict[i] = true;
  }
  std::vector<duckdb::LogicalType> out_types;
  out_types.reserve(n);
  for (const auto& e : aggr.expressions) {
    out_types.push_back(e->GetReturnType());
  }

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> dict_exprs;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> doc_exprs;
  std::vector<duckdb::idx_t> slot_of(n);
  for (size_t i = 0; i < n; ++i) {
    if (is_dict[i]) {
      slot_of[i] = dict_exprs.size();
      dict_exprs.push_back(std::move(aggr.expressions[i]));
    } else {
      slot_of[i] = doc_exprs.size();
      auto expr = std::move(aggr.expressions[i]);
      RemapColumnRefs(*expr, index_map);
      doc_exprs.push_back(std::move(expr));
    }
  }
  aggr.expressions = std::move(dict_exprs);
  aggr.types.clear();
  for (const auto& e : aggr.expressions) {
    aggr.types.push_back(e->GetReturnType());
  }
  ts_dict_calls.resize(aggr.expressions.size());
  std::iota(ts_dict_calls.begin(), ts_dict_calls.end(), size_t{0});

  auto doc_agg = duckdb::make_uniq<duckdb::LogicalAggregate>(
    binder.GenerateTableIndex(), binder.GenerateTableIndex(),
    std::move(doc_exprs));
  const auto doc_agg_index = doc_agg->aggregate_index;
  doc_agg->children.push_back(std::move(doc_child));
  for (const auto& e : doc_agg->expressions) {
    doc_agg->types.push_back(e->GetReturnType());
  }

  const auto proj_index = binder.GenerateTableIndex();
  duckdb::ColumnBindingReplacer replacer;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> proj_exprs;
  for (size_t i = 0; i < n; ++i) {
    const auto src_index = is_dict[i] ? aggr.aggregate_index : doc_agg_index;
    proj_exprs.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      out_types[i],
      duckdb::ColumnBinding{src_index, duckdb::ProjectionIndex{slot_of[i]}}));
    replacer.replacement_bindings.emplace_back(
      duckdb::ColumnBinding{aggr.aggregate_index, duckdb::ProjectionIndex{i}},
      duckdb::ColumnBinding{proj_index, duckdb::ProjectionIndex{i}});
  }
  replacer.stop_operator = plan.get();
  replacer.VisitOperator(root);

  auto cross =
    duckdb::LogicalCrossProduct::Create(std::move(plan), std::move(doc_agg));
  auto proj = duckdb::make_uniq<duckdb::LogicalProjection>(
    proj_index, std::move(proj_exprs));
  for (auto& e : proj->expressions) {
    proj->types.push_back(e->GetReturnType());
  }
  proj->children.push_back(std::move(cross));
  plan = std::move(proj);
  return true;
}

void PushdownTsDictAggregates(duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
                              duckdb::LogicalOperator& root,
                              duckdb::Binder& binder,
                              duckdb::ClientContext& context) {
  auto& aggr = plan->Cast<duckdb::LogicalAggregate>();
  std::vector<size_t> ts_dict_calls;
  std::vector<std::pair<size_t, KeywordDictAgg>> keyword_aggs;
  bool other = false;
  std::shared_ptr<const catalog::Snapshot> snapshot;
  const auto [implicit_target, implicit_filter] = ImplicitTsDictTarget(aggr);
  if (!aggr.groups.empty() && implicit_target &&
      TryPushdownTsDictFacet(plan, root, *implicit_target, implicit_filter,
                             binder, context)) {
    return;
  }
  for (size_t i = 0; i < aggr.expressions.size(); ++i) {
    auto& expr = aggr.expressions[i];
    if (expr->GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_AGGREGATE) {
      other = true;
      continue;
    }
    auto& agg = expr->Cast<duckdb::BoundAggregateExpression>();
    if (connector::IsTsDictFunctionName(
          agg.Function().GetName().GetIdentifierName())) {
      ts_dict_calls.push_back(i);
      continue;
    }
    if (aggr.groups.empty() && implicit_target) {
      if (!snapshot) {
        snapshot =
          connector::GetSereneDBContext(context).AcquireCatalogSnapshot();
      }
      if (auto match =
            ClassifyKeywordDictAgg(agg, root, *implicit_target, *snapshot)) {
        keyword_aggs.push_back({i, *match});
        continue;
      }
    }
    other = true;
  }

  // A WHERE clause is convertible when every conjunct is claimable by the
  // dict scan: same-field predicates (term level or post-filter) and indexed
  // @@ document predicates (where level). Otherwise the conversion is dropped
  // and the aggregates stay on the document scan.
  if (!keyword_aggs.empty() && implicit_filter) {
    const auto field = keyword_aggs.front().second.field_id;
    auto& bind_data =
      implicit_target->bind_data->Cast<connector::SereneDBScanBindData>();
    bool convertible = true;
    for (const auto& [i, match] : keyword_aggs) {
      convertible &= match.field_id == field;
    }
    for (auto& expr : implicit_filter->expressions) {
      bool refs_field = false;
      bool refs_other = false;
      CollectPredicateFields(*expr, field, bind_data, *implicit_target,
                             refs_field, refs_other);
      convertible &= (refs_field && !refs_other) || ContainsTSQueryExpr(*expr);
    }
    if (!convertible) {
      keyword_aggs.clear();
      other = true;
    }
  }

  // Converting a ts_dict aggregate switches the scan to term-dict rows, which
  // would silently corrupt any sibling aggregate that still expects document
  // rows (count(*) would count terms). Ungrouped mixed aggregates split into
  // two scans instead; grouped ones (or unsupported source shapes) stay
  // rejected.
  if (!ts_dict_calls.empty() && other) {
    if (!TrySplitMixedTsDictAggregates(plan, root, binder, ts_dict_calls)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("ts_dict_* aggregates cannot be combined with other "
                "aggregates over the same scan"),
        ERR_HINT("compute the other aggregates in a separate subquery"));
    }
    keyword_aggs.clear();
    other = false;
  }

  bool converted = false;
  for (const auto i : ts_dict_calls) {
    auto& agg = aggr.expressions[i]->Cast<duckdb::BoundAggregateExpression>();
    aggr.expressions[i] = PushdownTsDictCall(agg, root, context);
    converted = true;
  }

  // Converting an implicit aggregate forces the scan into term-dict
  // enumeration, so it is only safe when every aggregate in the node can be
  // served that way.
  if (!other && !keyword_aggs.empty()) {
    for (const auto& [i, match] : keyword_aggs) {
      const auto alias = aggr.expressions[i]->GetAlias();
      aggr.expressions[i] =
        BuildKeywordTsDictAggregate(match, root, context, alias);
    }
    converted = true;
  }

  // min/max are duplicate-insensitive: the dedup GROUP BY would only bury
  // the scan's two-term min/max seek shortcut under a full hash aggregation.
  const auto only_minmax = absl::c_all_of(
    aggr.expressions, [](const duckdb::unique_ptr<duckdb::Expression>& e) {
      if (e->GetExpressionClass() != duckdb::ExpressionClass::BOUND_AGGREGATE) {
        return false;
      }
      const auto& agg = e->Cast<duckdb::BoundAggregateExpression>();
      const auto& name = agg.Function().GetName();
      return (name == "min" || name == "max") &&
             agg.GetChildren().size() == 1 &&
             agg.GetChildren()[0]->GetExpressionClass() ==
               duckdb::ExpressionClass::BOUND_COLUMN_REF;
    });
  if (converted && !only_minmax && !aggr.children.empty()) {
    auto injected = InjectTsDictGroupBy(
      std::move(aggr.children[0]),
      {aggr.expressions.data(), aggr.expressions.size()}, binder, context);
    aggr.children[0] = std::move(injected);
  }
}

void RewriteCallInExpr(duckdb::unique_ptr<duckdb::Expression>& expr,
                       duckdb::LogicalOperator& root,
                       duckdb::ClientContext& context) {
  if (!expr) {
    return;
  }
  if (expr->GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
    auto& func = expr->Cast<duckdb::BoundFunctionExpression>();
    const auto& name = func.Function().GetName().GetIdentifierName();
    if (IsScorerFunctionName(name)) {
      if (auto repl = PushdownScorerCall(func, root)) {
        expr = std::move(repl);
        return;
      }
      // Native scans cannot materialize the tableoid argument, so the
      // runtime stub would never be reached; raise its error here.
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG(name,
                              "() requires an inverted index scan in the same "
                              "sub-query"));
    } else if (name == connector::kOffsets) {
      if (auto repl = PushdownOffsetsCall(func, root)) {
        expr = std::move(repl);
        return;
      }
    } else if (auto info = connector::GetAnnFunctionInfo(func)) {
      if (auto repl = PushdownDistanceCall(func, *info, root, context)) {
        expr = std::move(repl);
        return;
      }
    }
  } else if (expr->GetExpressionClass() ==
             duckdb::ExpressionClass::BOUND_WINDOW) {
    const auto& window = expr->Cast<duckdb::BoundWindowExpression>();
    const auto& aggregate = window.AggregateFunction();
    if (aggregate && connector::IsTsDictFunctionName(
                       aggregate->GetName().GetIdentifierName())) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG(aggregate->GetName().GetIdentifierName(),
                "() cannot be used as a window function"),
        ERR_HINT("use it as a plain aggregate over an inverted index scan"));
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
      RewriteCallInExpr(child, root, context);
    });
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

  switch (plan->type) {
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
    case duckdb::LogicalOperatorType::LOGICAL_FILTER:
    case duckdb::LogicalOperatorType::LOGICAL_WINDOW:
      for (auto& e : plan->expressions) {
        RewriteCallInExpr(e, *root, context);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY:
      for (auto& o : plan->Cast<duckdb::LogicalOrder>().orders) {
        RewriteCallInExpr(o.expression, *root, context);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_TOP_N:
      for (auto& o : plan->Cast<duckdb::LogicalTopN>().orders) {
        RewriteCallInExpr(o.expression, *root, context);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
      PushdownTsDictAggregates(plan, *root, binder, context);
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
    e = &e->Cast<duckdb::BoundCastExpression>().Child();
  }
  if (!e ||
      e->GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return std::nullopt;
  }
  return e->Cast<duckdb::BoundColumnRefExpression>().Binding();
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
    const auto radius = [&] -> float {
      switch (radius_value.type().id()) {
        case duckdb::LogicalTypeId::FLOAT:
          return radius_value.GetValue<float>();
        case duckdb::LogicalTypeId::DOUBLE:
          return static_cast<float>(radius_value.GetValue<double>());
        default:
          return std::numeric_limits<float>::max();
      }
    }();
    if (radius == std::numeric_limits<float>::max()) {
      continue;
    }
    const auto cmp_type = cmp.GetExpressionType();
    scan.vector_scorer->radius = radius;
    scan.vector_scorer->radius_inclusive =
      cmp_type == duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO ||
      cmp_type == duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO;
    filters.erase(filters.begin() + i);
    return true;
  }
  return false;
}

connector::SearchColumnInfo MakeSearchColumnInfo(
  auto field, const auto* info, duckdb::LogicalType type,
  catalog::ColumnTokenizer tokenizer) {
  return {
    .field_id = field,
    .null_field_id = info ? info->null_field_id : irs::field_limits::invalid(),
    .bool_field_id = info ? info->bool_field_id : irs::field_limits::invalid(),
    .numeric_field_id =
      info ? info->numeric_field_id : irs::field_limits::invalid(),
    .logical_type = std::move(type),
    .tokenizer = std::move(tokenizer),
  };
}

template<typename F>
auto WithSearchGetters(duckdb::LogicalGet& get,
                       connector::SereneDBScanBindData& bind_data,
                       const catalog::InvertedIndex& index,
                       const std::shared_ptr<const catalog::Snapshot>& snapshot,
                       duckdb::ClientContext& context, F&& fn) {
  const auto projected_ids = BuildProjectedColumnIds(get, bind_data);
  const auto table_index = get.table_index;
  const auto relation_id = index.GetRelationId();

  containers::FlatHashSet<irs::field_id> analyzed_fields;

  const auto make_info = [&](auto field_id, const auto* info,
                             duckdb::LogicalType type) {
    auto column_info = MakeSearchColumnInfo(
      field_id, info, std::move(type), index.GetTokenizer(snapshot, field_id));
    if (column_info.tokenizer.analyzer->type() !=
        irs::Type<irs::StringTokenizer>::id()) {
      analyzed_fields.insert(field_id);
    }
    return column_info;
  };

  connector::ColumnGetter getter =
    [&](const duckdb::BoundColumnRefExpression& ref)
    -> std::optional<connector::SearchColumnInfo> {
    const auto col_id = ResolveColumnId(ref.Binding(), bind_data, get);
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

  return fn(getter, expr_getter, analyzed_fields);
}

bool TryClaimSearchFilter(
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  duckdb::LogicalGet& get, connector::SereneDBScanBindData& bind_data,
  const catalog::InvertedIndex& index,
  std::shared_ptr<const catalog::Snapshot> snapshot,
  duckdb::ClientContext& context) {
  return WithSearchGetters(
    get, bind_data, index, snapshot, context,
    [&](const connector::ColumnGetter& getter,
        const connector::ExpressionGetter& expr_getter,
        containers::FlatHashSet<irs::field_id>& analyzed_fields) {
      auto& scan = bind_data.scan_source->Cast<connector::SearchScan>();

      auto root_and = std::make_unique<irs::And>();
      bool any_claimed = false;
      for (size_t i = 0; i < filters.size();) {
        if (TryClaimIResearchConjunct(*root_and, filters[i], getter,
                                      expr_getter, context)) {
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

      scan.stored_filter = std::move(root);
      for (auto& req : scan.offsets) {
        if (req.bind) {
          req.bind->stored_filter = scan.stored_filter;
        }
      }
      return true;
    });
}

// Repoint every column reference that resolves to the enumerated field onto the
// term virtual column, so a single same-field predicate the filter builder
// cannot push as a term acceptor still applies as a post-filter over the
// emitted terms (no postings touched -- it filters the dictionary rows).
void RewriteFieldRefsToTerm(duckdb::unique_ptr<duckdb::Expression>& expr,
                            irs::field_id field_id,
                            connector::SereneDBScanBindData& bind_data,
                            duckdb::LogicalGet& get,
                            duckdb::idx_t term_get_col_idx) {
  if (expr->GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    auto& ref = expr->Cast<duckdb::BoundColumnRefExpression>();
    const auto col_id = ResolveColumnId(ref.Binding(), bind_data, get);
    if (col_id != catalog::Column::kInvalidId &&
        static_cast<irs::field_id>(col_id) == field_id) {
      expr = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
        duckdb::Identifier{
          TsDictColName(bind_data, field_id, TsDictColKind::Term)},
        duckdb::LogicalType::VARCHAR,
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
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    const auto& ref = expr.Cast<duckdb::BoundColumnRefExpression>();
    const auto col_id = ResolveColumnId(ref.Binding(), bind_data, get);
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

// True if `filter` is a single term-acceptor leaf (ByTerm / ByTerms / ByPrefix
// / ByRange / Levenshtein / Automaton) constraining `field`, i.e. a shape the
// term-dict scan can enumerate directly. null, And/Or/Not, other-field leaves
// and ByTerms with min_match > 1 (an intersection the enumeration cannot
// express) return false.
template<typename... Fs>
bool IsAcceptorOn(const irs::Filter& filter, irs::field_id field) {
  const auto type = filter.type();
  return ((type == irs::Type<Fs>::id() &&
           basics::downCast<Fs>(filter).field_id() == field) ||
          ...);
}

bool IsSingleAcceptor(const irs::Filter* filter, irs::field_id field) {
  if (!filter) {
    return false;
  }
  if (filter->type() == irs::Type<irs::ByTerms>::id()) {
    const auto& terms = basics::downCast<irs::ByTerms>(*filter);
    return terms.field_id() == field && terms.options().min_match <= 1;
  }
  return IsAcceptorOn<irs::ByTerm, irs::ByPrefix, irs::ByRange,
                      irs::LevenshteinAutomatonFilter, irs::AutomatonFilter>(
    *filter, field);
}

bool IsAcceptorTreeOn(irs::Filter& filter, irs::field_id field) {
  const auto type = filter.type();
  if (type == irs::Type<irs::All>::id() ||
      type == irs::Type<irs::Empty>::id()) {
    return true;
  }
  if (type == irs::Type<irs::Exclusion>::id()) {
    auto& exclusion = basics::downCast<irs::Exclusion>(filter);
    const auto& include = exclusion.GetInclude();
    if (include && !IsAcceptorTreeOn(*include, field)) {
      return false;
    }
    const auto excludes = exclusion.GetExcludes();
    if (!include && excludes.empty()) {
      return false;
    }
    return absl::c_all_of(excludes, [&](const irs::Filter::ptr& child) {
      return child && IsAcceptorTreeOn(*child, field);
    });
  }
  if (type == irs::Type<irs::And>::id() || type == irs::Type<irs::Or>::id() ||
      type == irs::Type<irs::Not>::id()) {
    const auto children = filter.GetChildren();
    if (children.empty()) {
      return false;
    }
    return absl::c_all_of(children, [&](const irs::Filter::ptr& child) {
      return child && IsAcceptorTreeOn(*child, field);
    });
  }
  return IsSingleAcceptor(&filter, field);
}

[[noreturn]] void ThrowUnclaimableTsDictConjunct(size_t nfields) {
  if (nfields > 1) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("multi-field ts_dict_agg() WHERE accepts indexed "
              "document predicates and keyword term acceptors only"),
      ERR_HINT("scalar term conditions are not supported here; "
               "filter in an outer query over "
               "unnest(ts_dict_agg(...))"));
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
    ERR_MSG("ts_dict_agg() WHERE accepts indexed document predicates "
            "(@@ ts_*, =, IN, LIKE, BETWEEN over indexed columns) "
            "plus scalar post-filters over the enumerated field's "
            "terms"),
    ERR_HINT("this predicate can be neither claimed by the index nor "
             "evaluated over term rows; filter in an outer query "
             "over unnest(ts_dict_agg(...))"));
}

// ts_dict_agg() WHERE accepts predicates on the enumerated fields; each
// claimable conjunct must sit on exactly one of them and is routed to that
// field's request. The
// claimable conjuncts are fused and, when the optimizer folds them into ONE
// term acceptor, that acceptor drives the enumeration. Otherwise the most
// selective individually-claimable acceptor (ByTerm > ByTerms > ByPrefix /
// ByRange > automatons) drives and every other claimable conjunct -- single
// acceptors and boolean trees over them alike -- compiles into a term
// predicate checked per emitted term inside the scan; with no driver at all
// the full dictionary is enumerated under the predicates. Scalar same-field
// conjuncts post-filter the emitted term column (single-field mode only: on
// multi-field rows the other fields' term columns are NULL, so a scalar
// residue would silently drop them). Cross-field references and
// non-compilable @@ shapes are rejected; nothing is committed to the scan
// before all conjuncts validate.
void ClaimTsDictFilter(
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  duckdb::LogicalGet& get, connector::SereneDBScanBindData& bind_data,
  connector::SearchScan& ss, const catalog::InvertedIndex& index,
  std::shared_ptr<const catalog::Snapshot> snapshot,
  duckdb::ClientContext& context) {
  WithSearchGetters(
    get, bind_data, index, snapshot, context,
    [&](const connector::ColumnGetter& getter,
        const connector::ExpressionGetter& expr_getter,
        containers::FlatHashSet<irs::field_id>& analyzed_fields) {
      const auto optimize = [&](irs::Filter::ptr& f,
                                bool fuse_intersections = false) {
        irs::Optimize(f, {.scored = false,
                          .fuse_seekable_acceptors = true,
                          .fuse_acceptor_intersections = fuse_intersections,
                          .analyzed_fields = analyzed_fields});
      };

      const size_t nfields = ss.ts_dicts.size();
      const auto enumerated = [&](irs::field_id field) {
        for (const auto& req : ss.ts_dicts) {
          if (req.field_id == field) {
            return true;
          }
        }
        return false;
      };
      const auto refs_term_level = [&](this auto& self,
                                       const duckdb::Expression& expr,
                                       bool& saw_analyzed) -> bool {
        if (expr.GetExpressionClass() ==
            duckdb::ExpressionClass::BOUND_COLUMN_REF) {
          const auto& ref = expr.Cast<duckdb::BoundColumnRefExpression>();
          const auto col_id = ResolveColumnId(ref.Binding(), bind_data, get);
          if (col_id == catalog::Column::kInvertedIndexTermId) {
            return true;
          }
          const auto field = static_cast<irs::field_id>(col_id);
          if (col_id == catalog::Column::kInvalidId || !enumerated(field)) {
            return false;
          }
          const auto type = bind_data.ColumnTypeById(col_id).id();
          saw_analyzed |= !index.IsKeywordField(*snapshot, field) ||
                          type == duckdb::LogicalTypeId::LIST ||
                          type == duckdb::LogicalTypeId::ARRAY;
          return true;
        }
        bool term_level = true;
        duckdb::ExpressionIterator::EnumerateChildren(
          expr, [&](const duckdb::Expression& child) {
            term_level = term_level && self(child, saw_analyzed);
          });
        return term_level;
      };
      // Comparisons over the enumerated field match TERMS (raw tokens), so
      // term-level claims bypass the doc-level analyzer guards. Everything
      // else on a tokenized field (@@ ts_*, match sugar functions) stays
      // document-level.
      const auto comparison_shape = [](const duckdb::Expression& expr) {
        switch (expr.GetExpressionType()) {
          case duckdb::ExpressionType::COMPARE_EQUAL:
          case duckdb::ExpressionType::COMPARE_NOTEQUAL:
          case duckdb::ExpressionType::COMPARE_LESSTHAN:
          case duckdb::ExpressionType::COMPARE_GREATERTHAN:
          case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
          case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
          case duckdb::ExpressionType::COMPARE_BETWEEN:
          case duckdb::ExpressionType::COMPARE_IN:
          case duckdb::ExpressionType::COMPARE_NOT_IN:
            return true;
          default: {
            if (expr.GetExpressionClass() !=
                duckdb::ExpressionClass::BOUND_FUNCTION) {
              return false;
            }
            // LIKE (`~~`) and the prefix rewrite duckdb lowers `col LIKE 'a%'`
            // to (`prefix`/`starts_with`/`^@`) match terms over the enumerated
            // field, so they claim term-level like the comparisons above --
            // otherwise the lowered sugar shape would be treated as a
            // document-level predicate and never reach the index.
            const auto& name =
              expr.Cast<duckdb::BoundFunctionExpression>().Function().GetName();
            return name == "~~" || name == "prefix" || name == "starts_with" ||
                   name == "^@";
          }
        }
      };
      const auto is_term_level = [&](const duckdb::Expression& expr) {
        bool saw_analyzed = false;
        return refs_term_level(expr, saw_analyzed) &&
               (!saw_analyzed || comparison_shape(expr));
      };
      const auto raw_terms =
        [&](std::optional<connector::SearchColumnInfo> info) {
          if (info && enumerated(info->field_id) && info->tokenizer.analyzer &&
              info->tokenizer.analyzer->type() !=
                irs::Type<irs::StringTokenizer>::id()) {
            info->tokenizer.analyzer = catalog::Tokenizer::TokenizerWrapper{
              new irs::StringTokenizer(), {}};
          }
          return info;
        };
      const auto term_virtual_info =
        [&](const duckdb::BoundColumnRefExpression& ref)
        -> std::optional<connector::SearchColumnInfo> {
        if (ref.Binding().table_index != get.table_index ||
            ResolveColumnId(ref.Binding(), bind_data, get) !=
              catalog::Column::kInvertedIndexTermId) {
          return std::nullopt;
        }
        const auto term_ref =
          ClassifyTsDictGetCol(ss, ref.Binding().column_index.GetIndex());
        if (!term_ref) {
          return std::nullopt;
        }
        const auto field = ss.ts_dicts[term_ref->req_index].field_id;
        return MakeSearchColumnInfo(
          field, index.FindColumnInfo(static_cast<catalog::Column::Id>(field)),
          duckdb::LogicalType::VARCHAR,
          {.analyzer = catalog::Tokenizer::TokenizerWrapper{
             new irs::StringTokenizer(), {}}});
      };
      const connector::ColumnGetter term_getter =
        [&](const duckdb::BoundColumnRefExpression& ref) {
          if (auto info = term_virtual_info(ref)) {
            return std::optional{std::move(*info)};
          }
          return raw_terms(getter(ref));
        };
      const connector::ExpressionGetter term_expr_getter =
        [&](const duckdb::Expression& expr) {
          return raw_terms(expr_getter(expr));
        };

      // The four execution modes of the where/having model: term-level
      // conjuncts drive or predicate the enumeration (having side), the
      // rest restrict documents (where side, executed once per segment) or
      // post-filter term rows.
      enum class Route {
        HavingFast,  // term-level acceptor tree -> having_filter
        Where,       // document filter -> where proxy
        PostFilter,  // term-level scalar over the emitted term rows
        Rejected,
      };
      const auto claim =
        [&](duckdb::unique_ptr<duckdb::Expression>& conjunct,
            const connector::ColumnGetter& col_getter,
            const connector::ExpressionGetter& e_getter) -> irs::Filter::ptr {
        auto one_and = std::make_unique<irs::And>();
        if (!TryClaimIResearchConjunct(*one_and, conjunct, col_getter, e_getter,
                                       context)) {
          return nullptr;
        }
        irs::Filter::ptr one = std::move(one_and);
        optimize(one);
        return one;
      };

      std::vector<Route> routes(filters.size(), Route::Rejected);
      std::vector<std::unique_ptr<irs::And>> having_and(nfields);
      auto where_and = std::make_unique<irs::And>();
      for (size_t i = 0; i < filters.size(); ++i) {
        if (!is_term_level(*filters[i])) {
          if (auto one = claim(filters[i], getter, expr_getter)) {
            where_and->add(std::move(one));
            routes[i] = Route::Where;
          }
          continue;
        }
        auto one = claim(filters[i], term_getter, term_expr_getter);
        if (!one) {
          continue;
        }
        for (size_t f = 0; f < nfields; ++f) {
          if (IsAcceptorTreeOn(*one, ss.ts_dicts[f].field_id) &&
              one->CompileTermPredicate()) {
            if (!having_and[f]) {
              having_and[f] = std::make_unique<irs::And>();
            }
            having_and[f]->add(std::move(one));
            routes[i] = Route::HavingFast;
            break;
          }
        }
      }

      for (size_t i = 0; i < filters.size(); ++i) {
        if (routes[i] != Route::Rejected) {
          continue;
        }
        bool refs_field = false;
        bool refs_other = false;
        if (nfields == 1) {
          CollectPredicateFields(*filters[i], ss.ts_dicts.front().field_id,
                                 bind_data, get, refs_field, refs_other);
        }
        if (!refs_field || refs_other || ContainsTSQueryExpr(*filters[i])) {
          ThrowUnclaimableTsDictConjunct(nfields);
        }
        routes[i] = Route::PostFilter;
      }

      for (size_t f = 0; f < nfields; ++f) {
        if (!having_and[f] || having_and[f]->empty()) {
          continue;
        }
        irs::Filter::ptr fused = std::move(having_and[f]);
        optimize(fused, true);
        if (fused->type() == irs::Type<irs::And>::id()) {
          auto& children = basics::downCast<irs::And>(*fused).mutable_filters();
          std::stable_sort(children.begin(), children.end(),
                           [](const auto& lhs, const auto& rhs) {
                             return irs::optimizer::AcceptorRank(*lhs) <
                                    irs::optimizer::AcceptorRank(*rhs);
                           });
        }
        ss.ts_dicts[f].having_filter = std::move(fused);
      }
      // Term conditions also shrink the document set: a term matching the
      // having filter only occurs in documents that contain such a term,
      // so re-claiming the term-level conjuncts into the where filter
      // leaves every emitted count unchanged while the where set gets
      // smaller.
      if (!where_and->empty()) {
        for (size_t i = 0; i < filters.size(); ++i) {
          if (routes[i] != Route::HavingFast) {
            continue;
          }
          if (auto one = claim(filters[i], term_getter, term_expr_getter)) {
            where_and->add(std::move(one));
          }
        }
      }
      if (!where_and->empty()) {
        irs::Filter::ptr doc = std::move(where_and);
        optimize(doc);
        auto proxy = std::make_shared<irs::ProxyFilter>();
        proxy->set_filter(irs::IResourceManager::gNoop, std::move(doc));
        ss.stored_filter = std::move(proxy);
      }
      if (absl::c_linear_search(routes, Route::PostFilter)) {
        auto& req = ss.ts_dicts.front();
        EnsureTsDictCol(bind_data, get, req, TsDictColKind::Term);
        req.term_uses |= connector::TsDictTermUses::kFull;
        for (size_t i = 0; i < filters.size(); ++i) {
          if (routes[i] == Route::PostFilter) {
            RewriteFieldRefsToTerm(filters[i], req.field_id, bind_data, get,
                                   req.term_col_idx);
          }
        }
      }

      size_t out = 0;
      for (size_t i = 0; i < filters.size(); ++i) {
        const bool drop =
          routes[i] == Route::HavingFast || routes[i] == Route::Where;
        if (!drop) {
          if (out != i) {
            filters[out] = std::move(filters[i]);
          }
          ++out;
        }
      }
      filters.erase(filters.begin() + out, filters.end());
    });
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
  if (filters.empty() || !bind_data_ptr) {
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
    auto snapshot = conn_ctx.AcquireCatalogSnapshot();
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
  auto snapshot = conn_ctx.CatalogSnapshot();
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
