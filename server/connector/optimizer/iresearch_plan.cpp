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

#include <absl/algorithm/container.h>

#include <duckdb/optimizer/optimizer.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_window_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_join.hpp>
#include <duckdb/planner/operator/logical_order.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_top_n.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_set.h"
#include "catalog/inverted_index.h"
#include "catalog/scorer_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_function.h"
#include "connector/functions/search.h"
#include "connector/functions/ts_offsets.h"
#include "connector/functions/vector.h"
#include "connector/index_expression.hpp"
#include "connector/optimizer/iresearch_plan_common.hpp"
#include "connector/optimizer/ts_dict_plan.hpp"
#include "connector/search_filter_builder.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/search/optimizer/boolean_rules.hpp"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::optimizer {

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

std::optional<FoundScan> AsSearchScan(duckdb::LogicalOperator& op) {
  if (op.type != duckdb::LogicalOperatorType::LOGICAL_GET) {
    return std::nullopt;
  }
  auto& get = op.Cast<duckdb::LogicalGet>();
  if (!connector::IsSereneDBScan(get)) {
    return std::nullopt;
  }
  auto& bd = get.bind_data->Cast<connector::SereneDBScanBindData>();
  return FoundScan{&get, &bd};
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

ResolvedProjection WalkProjections(duckdb::LogicalOperator& root,
                                   duckdb::ColumnBinding binding) {
  while (auto* proj = FindProjectionByTableIndex(root, binding.table_index)) {
    const auto idx = binding.column_index.GetIndex();
    if (idx >= proj->expressions.size()) {
      break;
    }
    auto& forwarded = *proj->expressions[idx];
    if (forwarded.GetExpressionType() !=
        duckdb::ExpressionType::BOUND_COLUMN_REF) {
      return {binding, &forwarded};
    }
    binding = forwarded.Cast<duckdb::BoundColumnRefExpression>().Binding();
  }
  return {binding, nullptr};
}

duckdb::ColumnBinding ResolveBindingThroughProjections(
  duckdb::LogicalOperator& root, duckdb::ColumnBinding binding) {
  return WalkProjections(root, binding).binding;
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

bool TryClaimIResearchConjunct(
  irs::And& and_root, const duckdb::unique_ptr<duckdb::Expression>& conjunct,
  const connector::ColumnGetter& getter,
  const connector::ExpressionGetter& expr_getter,
  duckdb::ClientContext& context) {
  // A conjunct with an unbound parameter only appears in a prepared
  // statement's template plan, which duckdb rebinds with the values
  // substituted as constants before every execution. Decline instead of
  // letting the @@ boundary throw; the residual never executes.
  if (conjunct->HasParameter()) {
    return false;
  }
  const auto before = and_root.size();
  std::span<const duckdb::unique_ptr<duckdb::Expression>> single{&conjunct, 1};
  const auto claimed =
    connector::MakeSearchFilter(and_root, single, getter, context, expr_getter);
  if (claimed.ok() && and_root.size() > before) {
    return true;
  }
  while (and_root.size() > before) {
    and_root.PopBack();
  }
  return false;
}

bool WithSearchGetters(duckdb::LogicalGet& get,
                       connector::SereneDBScanBindData& bind_data,
                       const catalog::InvertedIndex& index,
                       const std::shared_ptr<const catalog::Snapshot>& snapshot,
                       duckdb::ClientContext& context,
                       absl::FunctionRef<bool(const SearchGetters&)> fn) {
  const auto projected_ids = BuildProjectedColumnIds(get, bind_data);
  const auto table_index = get.table_index;
  const auto relation_id = index.GetRelationId();
  const auto* table =
    bind_data.GetKind() == connector::SereneDBScanBindData::Kind::Table
      ? bind_data.As<connector::TableScanBindData>().table.get()
      : nullptr;

  containers::FlatHashSet<irs::field_id> analyzed_fields;
  containers::FlatHashMap<irs::field_id, irs::field_id> null_markers;
  containers::FlatHashMap<catalog::Column::Id, bool> not_null_cache;

  const auto column_not_null = [&](catalog::Column::Id col_id) {
    const auto [it, inserted] = not_null_cache.try_emplace(col_id, false);
    if (inserted) {
      it->second = bind_data.IsColumnNotNull(col_id);
    }
    return it->second;
  };

  const auto make_info = [&](irs::field_id field_id,
                             const catalog::InvertedIndexEntryInfo* info,
                             duckdb::LogicalType type, bool column) {
    auto column_info = MakeSearchColumnInfo(
      field_id, info, std::move(type), index.GetTokenizer(snapshot, field_id));
    if (column && table &&
        column_not_null(static_cast<catalog::Column::Id>(field_id))) {
      column_info.null_field_id = irs::field_limits::invalid();
    }
    if (irs::field_limits::valid(column_info.null_field_id)) {
      null_markers[column_info.null_field_id] = column_info.field_id;
    }
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
    return make_info(col_id, info, std::move(type), true);
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
    return make_info(field_id, info, expr_data->return_type, false);
  };

  return fn(SearchGetters{getter, expr_getter, analyzed_fields, null_markers});
}

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

irs::field_id ResolveAnnTargetFieldId(
  const duckdb::Expression& col_arg, const duckdb::LogicalGet& get,
  const connector::SereneDBScanBindData& bind_data,
  const catalog::InvertedIndex& index, duckdb::ClientContext& client_context) {
  if (col_arg.GetExpressionClass() ==
      duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    const auto& ref = col_arg.Cast<duckdb::BoundColumnRefExpression>();
    if (ref.Binding().table_index == get.table_index) {
      if (const auto id = ResolveColumnId(ref.Binding(), bind_data, get);
          id != catalog::Column::kInvalidId) {
        return id;
      }
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

bool ScanColumnIsScore(const FoundScanColumn& sc) {
  return ResolveColumnId(sc.binding, *sc.found.bind_data, *sc.found.get) ==
         catalog::Column::kInvertedIndexScoreId;
}

bool BindingResolvesToScoreColumn(const duckdb::BoundColumnRefExpression& ref,
                                  duckdb::LogicalOperator& root) {
  if (ref.GetAlias().empty()) {
    return false;
  }
  const auto sc = ResolveIResearchScanColumn(root, ref.Binding());
  return sc && ScanColumnIsScore(*sc);
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
  auto& ss = *found->bind_data;
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
  auto& ss = *found->bind_data;
  if (ss.text_scorer || ss.EmitOffsets()) {
    return nullptr;
  }

  const auto& index = found->bind_data->inverted_index;
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
    ss.score_order = info.order;
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
  auto& search_scan = *found.bind_data;

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

void ReuseExistingScoreColumn(duckdb::Expression& order_expr,
                              duckdb::LogicalOperator& root) {
  if (order_expr.GetExpressionType() !=
      duckdb::ExpressionType::BOUND_COLUMN_REF) {
    return;
  }
  auto& ref = order_expr.Cast<duckdb::BoundColumnRefExpression>();
  auto* proj = FindProjectionByTableIndex(root, ref.Binding().table_index);
  if (!proj) {
    return;
  }
  const auto idx = ref.Binding().column_index.GetIndex();
  if (idx >= proj->expressions.size() ||
      proj->expressions[idx]->GetExpressionType() !=
        duckdb::ExpressionType::BOUND_COLUMN_REF) {
    return;
  }
  const auto binding =
    proj->expressions[idx]->Cast<duckdb::BoundColumnRefExpression>().Binding();
  auto sc = ResolveIResearchScanColumn(root, binding);
  if (!sc || !ScanColumnIsScore(*sc)) {
    return;
  }
  for (duckdb::idx_t j = 0; j < idx; ++j) {
    auto& e = *proj->expressions[j];
    if (e.GetExpressionType() == duckdb::ExpressionType::BOUND_COLUMN_REF &&
        e.Cast<duckdb::BoundColumnRefExpression>().Binding() == binding) {
      ref.BindingMutable().column_index = duckdb::ProjectionIndex{j};
      return;
    }
  }
}

void RewriteIResearchExpressions(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::LogicalOperator>& root,
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan, duckdb::Binder& binder) {
  if (plan->type == duckdb::LogicalOperatorType::LOGICAL_DELETE ||
      plan->type == duckdb::LogicalOperatorType::LOGICAL_UPDATE ||
      plan->type == duckdb::LogicalOperatorType::LOGICAL_MERGE_INTO) {
    return;
  }

  for (auto& child : plan->children) {
    RewriteIResearchExpressions(context, root, child, binder);
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
        ReuseExistingScoreColumn(*o.expression, *root);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_TOP_N:
      for (auto& o : plan->Cast<duckdb::LogicalTopN>().orders) {
        RewriteCallInExpr(o.expression, *root, context);
        ReuseExistingScoreColumn(*o.expression, *root);
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

// Resolves the column reference on one side of a comparison, under an optional
// cast and an optional unary negation (`-(col)`). `negated` reports whether the
// negation was present -- the caller folds it into the comparison.
std::optional<duckdb::ColumnBinding> ScoreSideBinding(
  const duckdb::Expression* e, bool& negated) {
  negated = false;
  const auto strip_casts = [](const duckdb::Expression* x) {
    while (x &&
           x->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CAST) {
      x = &x->Cast<duckdb::BoundCastExpression>().Child();
    }
    return x;
  };
  e = strip_casts(e);
  if (e != nullptr &&
      e->GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
    const auto& fn = e->Cast<duckdb::BoundFunctionExpression>();
    if (fn.Function().GetName().GetIdentifierName() == "-" &&
        fn.GetChildren().size() == 1) {
      negated = true;
      e = strip_casts(fn.GetChildren()[0].get());
    }
  }
  if (e == nullptr ||
      e->GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return std::nullopt;
  }
  return e->Cast<duckdb::BoundColumnRefExpression>().Binding();
}

bool TryClaimAnnRange(
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  duckdb::LogicalGet& get, connector::SereneDBScanBindData& bind_data,
  duckdb::ClientContext& context) {
  auto& scan = bind_data;
  if (!scan.vector_scorer ||
      scan.vector_scorer->natural_order != duckdb::OrderType::ASCENDING ||
      scan.vector_scorer->radius != std::numeric_limits<float>::max()) {
    return false;
  }

  for (duckdb::idx_t i = 0; i < filters.size(); ++i) {
    auto& expr = *filters[i];
    if (!duckdb::BoundComparisonExpression::IsComparison(expr)) {
      continue;
    }
    auto& cmp = expr.Cast<duckdb::BoundFunctionExpression>();
    auto op = cmp.GetExpressionType();
    if (op != duckdb::ExpressionType::COMPARE_LESSTHAN &&
        op != duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO &&
        op != duckdb::ExpressionType::COMPARE_GREATERTHAN &&
        op != duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
      continue;
    }
    auto& cmp_left = duckdb::BoundComparisonExpression::LeftMutable(cmp);
    auto& cmp_right = duckdb::BoundComparisonExpression::RightMutable(cmp);

    // Find the side that resolves to the score column (bare or `-(score)`); the
    // other side is the bound.
    const auto is_score = [&](const std::optional<duckdb::ColumnBinding>& b) {
      return b && ResolveColumnId(*b, bind_data, get) ==
                    catalog::Column::kInvertedIndexScoreId;
    };
    bool negated = false;
    bool score_on_left = true;
    duckdb::Expression* const_side = cmp_right.get();
    auto binding = ScoreSideBinding(cmp_left.get(), negated);
    if (!is_score(binding)) {
      score_on_left = false;
      const_side = cmp_left.get();
      binding = ScoreSideBinding(cmp_right.get(), negated);
      if (!is_score(binding)) {
        continue;
      }
    }

    duckdb::Value bound_value;
    if (!TryFoldExpression(context, *const_side, bound_value)) {
      continue;
    }
    const auto bound = [&] -> std::optional<float> {
      switch (bound_value.type().id()) {
        case duckdb::LogicalTypeId::FLOAT:
          return bound_value.GetValue<float>();
        case duckdb::LogicalTypeId::DOUBLE:
          return static_cast<float>(bound_value.GetValue<double>());
        default:
          return std::nullopt;
      }
    }();
    if (!bound) {
      continue;
    }

    // Normalize to `score <op> radius` (score = the raw vector distance): put
    // the score on the left, then fold away a `-(score)` wrapper. A radius is
    // an upper bound on the distance; a lower bound stays a residual filter
    // (the scan evaluates it in user-facing space).
    float radius = *bound;
    if (!score_on_left) {
      op = duckdb::FlipComparisonExpression(op);
    }
    if (negated) {
      op = duckdb::FlipComparisonExpression(op);
      radius = -radius;
    }
    const bool inclusive =
      op == duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO;
    if (op != duckdb::ExpressionType::COMPARE_LESSTHAN && !inclusive) {
      continue;
    }
    scan.vector_scorer->radius = radius;
    scan.vector_scorer->radius_inclusive = inclusive;
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
  return WithSearchGetters(
    get, bind_data, index, snapshot, context,
    [&](const SearchGetters& getters) {
      auto& [getter, expr_getter, analyzed_fields, null_markers] = getters;
      auto& scan = bind_data;

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
                           .analyzed_fields = std::move(analyzed_fields),
                           .null_markers = &null_markers});

      scan.stored_filter = std::move(root);
      for (auto& req : scan.offsets) {
        if (req.bind) {
          req.bind->stored_filter = scan.stored_filter;
        }
      }
      return true;
    });
}

void RewriteSearchCallsToColumnRefs(
  duckdb::OptimizerExtensionInput& input,
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  RewriteIResearchExpressions(input.context, plan, plan,
                              input.optimizer.binder);
}

}  // namespace

void IResearchPushdownComplexFilter(
  duckdb::ClientContext& context, duckdb::LogicalGet& get,
  duckdb::FunctionData* bind_data_ptr,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters) {
  if (filters.empty() || !bind_data_ptr) {
    return;
  }
  auto& conn_ctx = connector::GetSereneDBContext(context);
  auto& bind_data = bind_data_ptr->Cast<connector::SereneDBScanBindData>();
  auto& ss = bind_data;
  // A search table's iresearch store IS the table: there is no index-side
  // predicate, so leave every filter for the standard column-filter pushdown.
  if (!bind_data.inverted_index) {
    return;
  }
  if (ss.TsDictMode()) {
    ClaimTsDictFilter(filters, get, bind_data, ss, *bind_data.inverted_index,
                      conn_ctx.CatalogSnapshot(), context);
    return;
  }
  if (ss.stored_filter) {
    return;
  }
  TryClaimAnnRange(filters, get, bind_data, context);
  if (filters.empty()) {
    return;
  }
  TryClaimSearchFilter(filters, get, bind_data, *bind_data.inverted_index,
                       conn_ctx.CatalogSnapshot(), context);
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
