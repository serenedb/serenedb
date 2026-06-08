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

#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_limit.hpp>
#include <duckdb/planner/operator/logical_order.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_top_n.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>

#include "basics/containers/trivial_map.h"
#include "basics/down_cast.h"
#include "catalog/inverted_index.h"
#include "catalog/scorer_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_scan_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/functions/search.h"
#include "connector/functions/ts_offsets.h"
#include "connector/functions/vector.h"
#include "connector/index_expression.hpp"
#include "connector/search_filter_builder.hpp"
#include "connector/search_filter_printer.hpp"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "search/inverted_index_shard.h"

namespace sdb::optimizer {
namespace {

connector::SearchFilterOptions BuildOptions(duckdb::ClientContext& context) {
  connector::SearchFilterOptions options{.client_context = context};
  duckdb::Value v;
  if (context.TryGetCurrentSetting("sdb_scored_terms_limit", v) &&
      !v.IsNull()) {
    options.scored_terms_limit = v.GetValue<int32_t>();
  }
  if (context.TryGetCurrentSetting("sdb_disable_top_k_optimization", v) &&
      !v.IsNull()) {
    options.disable_top_k_optimization = v.GetValue<bool>();
  }
  return options;
}

std::optional<connector::AnnFunctionInfo> ExpectedAnnForFunction(
  const duckdb::BoundFunctionExpression& func) {
  auto info = connector::GetAnnFunctionInfo(func);
  if (!info) {
    return std::nullopt;
  }
  if (info->is_norm ? func.children.size() != 1 : func.children.size() < 2) {
    return std::nullopt;
  }
  return info;
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
    if (const auto id = bind_data.ColumnIdByName(col_arg.GetName());
        id != catalog::Column::kInvalidId) {
      return id;
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
  return index.FindFieldIdBySerialized(serialized);
}

struct FoundScan {
  duckdb::LogicalGet* get;
  connector::SereneDBScanBindData* bind_data;
};

template<typename AcceptFn>
std::optional<FoundScan> FindIResearchScan(duckdb::LogicalOperator& op,
                                           duckdb::TableIndex target,
                                           AcceptFn&& accept) {
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    auto& get = op.Cast<duckdb::LogicalGet>();
    if (get.table_index != target || !connector::IsSereneDBScan(get)) {
      return std::nullopt;
    }
    auto& bd = get.bind_data->Cast<connector::SereneDBScanBindData>();
    if (!accept(*bd.scan_source)) {
      return std::nullopt;
    }
    return FoundScan{&get, &bd};
  }
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    auto& proj = op.Cast<duckdb::LogicalProjection>();
    if (proj.table_index == target && proj.children.size() == 1) {
      auto& child = *proj.children[0];
      if (child.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
        auto& get = child.Cast<duckdb::LogicalGet>();
        if (connector::IsSereneDBScan(get)) {
          auto& bd = get.bind_data->Cast<connector::SereneDBScanBindData>();
          if (accept(*bd.scan_source)) {
            return FoundScan{&get, &bd};
          }
        }
      }
    }
  }
  for (auto& child : op.children) {
    if (auto result = FindIResearchScan(*child, target, accept)) {
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
  for (duckdb::idx_t i = 0; i < bind_data.column_ids.size(); ++i) {
    if (bind_data.column_ids[i] != catalog::Column::kInvertedIndexScoreId) {
      continue;
    }
    const auto& col_ids = get.GetColumnIds();
    for (duckdb::idx_t j = 0; j < col_ids.size(); ++j) {
      if (col_ids[j].HasPrimaryIndex() && col_ids[j].GetPrimaryIndex() == i) {
        return j;
      }
    }
  }
  return AppendVirtualGetColumn(
    bind_data, get, catalog::Column::kInvertedIndexScoreId,
    duckdb::LogicalType::FLOAT, catalog::Column::kScoreName);
}

bool IsScorerFunctionName(std::string_view name) {
  using S = catalog::ScorerOptions;
  static constexpr containers::TrivialSet kScorerNames = [](auto selector) {
    return selector()
      .Case(S::Bm25::Owner::type_name())
      .Case(S::Tfidf::Owner::type_name())
      .Case(S::LmJm::Owner::type_name())
      .Case(S::LmDirichlet::Owner::type_name())
      .Case(S::IndriDirichlet::Owner::type_name())
      .Case(S::Dfi::Owner::type_name())
      .Case(S::RawBoost::Owner::type_name())
      .Case(S::RawTf::Owner::type_name())
      .Case(S::RawDL::Owner::type_name());
  };
  return kScorerNames.Contains(name);
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
  auto found = FindIResearchScan(
    root, anchor.binding.table_index, [](connector::ScanSource& s) {
      return s.Kind() == connector::ScanSourceKind::Search;
    });
  if (!found) {
    return nullptr;
  }
  auto& ss = found->bind_data->scan_source->Cast<connector::SearchScan>();
  if (ss.vector_scorer) {
    return nullptr;
  }
  if (!TrySetScorer(ss.text_scorer, func, func.function.GetName())) {
    return nullptr;
  }
  const auto idx = AppendScoreColumn(*found->bind_data, *found->get);
  const auto binding =
    ExposeGetColumnAt(root, anchor.binding.table_index, *found->get, idx,
                      catalog::Column::kScoreName, duckdb::LogicalType::FLOAT);
  auto out = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    std::string{catalog::Column::kScoreName}, duckdb::LogicalType::FLOAT,
    binding);
  if (!func.GetAlias().empty()) {
    out->SetAlias(func.GetAlias());
  }
  return out;
}

duckdb::unique_ptr<duckdb::Expression> PushdownDistanceCall(
  duckdb::BoundFunctionExpression& func, const connector::AnnFunctionInfo& info,
  duckdb::LogicalOperator& root, duckdb::ClientContext& context) {
  duckdb::Expression* col_arg = nullptr;
  duckdb::Expression* value_arg = nullptr;
  if (info.is_norm) {
    if (func.children.empty() || func.children[0]->IsFoldable()) {
      return nullptr;
    }
    col_arg = func.children[0].get();
  } else {
    auto args = ExtractDistanceArgs(func);
    if (!args.col_arg) {
      return nullptr;
    }
    col_arg = args.col_arg;
    value_arg = args.value_arg;
  }

  auto anchor_ti = [&]() -> std::optional<duckdb::TableIndex> {
    std::optional<duckdb::TableIndex> ti;
    auto visit = [&](this auto& self, const duckdb::Expression& e) -> void {
      if (ti) {
        return;
      }
      if (e.GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
        ti = e.Cast<duckdb::BoundColumnRefExpression>().binding.table_index;
        return;
      }
      duckdb::ExpressionIterator::EnumerateChildren(
        e, [&](const duckdb::Expression& child) { self(child); });
    };
    visit(*col_arg);
    return ti;
  }();
  if (!anchor_ti) {
    return nullptr;
  }

  auto found =
    FindIResearchScan(root, *anchor_ti, [](connector::ScanSource& s) {
      return s.Kind() == connector::ScanSourceKind::Search;
    });
  if (!found) {
    return nullptr;
  }

  {
    const auto& ss =
      found->bind_data->scan_source->Cast<connector::SearchScan>();
    if (ss.text_scorer || ss.EmitOffsets()) {
      return nullptr;
    }
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
  } else if (!value_arg ||
             !TryFoldQueryVector(context, *value_arg, ann_info->d, call_qvec)) {
    return nullptr;
  }

  auto& ss = found->bind_data->scan_source->Cast<connector::SearchScan>();

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
        vs.score_emit != info.score_emit ||
        vs.query_vector.size() != call_qvec.size() ||
        !std::equal(vs.query_vector.begin(), vs.query_vector.end(),
                    call_qvec.begin())) {
      return nullptr;
    }
  }

  const auto col_idx = AppendScoreColumn(*found->bind_data, *found->get);
  const auto binding =
    ExposeGetColumnAt(root, *anchor_ti, *found->get, col_idx,
                      catalog::Column::kScoreName, duckdb::LogicalType::FLOAT);
  duckdb::unique_ptr<duckdb::Expression> ref =
    duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      std::string{catalog::Column::kScoreName}, duckdb::LogicalType::FLOAT,
      binding);
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
  duckdb::ColumnBinding resolved = col_ref.binding;
  while (auto* proj = FindProjectionByTableIndex(root, resolved.table_index)) {
    const auto idx = resolved.column_index.GetIndex();
    if (idx >= proj->expressions.size()) {
      break;
    }
    auto& forwarded = *proj->expressions[idx];
    if (forwarded.GetExpressionType() !=
        duckdb::ExpressionType::BOUND_COLUMN_REF) {
      break;
    }
    resolved = forwarded.Cast<duckdb::BoundColumnRefExpression>().binding;
  }

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
    limit = raw == 0 ? std::numeric_limits<size_t>::max() : raw;
  }

  auto col_name_from = [&](const duckdb::LogicalGet* get) -> std::string {
    if (get) {
      const auto& cids = get->GetColumnIds();
      const auto col_idx = resolved.column_index.GetIndex();
      if (col_idx < cids.size()) {
        return get->GetColumnName(cids[col_idx]);
      }
    }
    return std::string{col_ref.GetAlias()};
  };

  auto found =
    FindIResearchScan(root, resolved.table_index, [](connector::ScanSource& s) {
      if (s.Kind() != connector::ScanSourceKind::Search) {
        return false;
      }
      const auto& ss = s.Cast<connector::SearchScan>();
      return ss.stored_filter || ss.text_scorer;
    });
  if (!found) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(", col_name_from(nullptr),
              ") requires an inverted index scan in the same sub-query"));
  }
  auto& search_scan =
    found->bind_data->scan_source->Cast<connector::SearchScan>();

  const auto col_name = col_name_from(found->get);

  auto target_col_id = catalog::Column::kInvalidId;
  if (resolved.table_index == found->get->table_index) {
    const auto col_idx = resolved.column_index.GetIndex();
    const auto& col_ids = found->get->GetColumnIds();
    if (col_idx < col_ids.size() && col_ids[col_idx].HasPrimaryIndex()) {
      const auto phys = col_ids[col_idx].GetPrimaryIndex();
      if (phys < found->bind_data->column_ids.size()) {
        target_col_id = found->bind_data->column_ids[phys];
      }
    }
  }
  if (target_col_id == catalog::Column::kInvalidId) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(): column '", col_name, "' not found in table"));
  }
  const auto& idx_col_ids = found->bind_data->inverted_index->GetColumnIds();
  if (absl::c_find(idx_col_ids, target_col_id) == idx_col_ids.end()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(): column '", col_name, "' not found in index"));
  }

  const auto* col_info =
    found->bind_data->inverted_index->FindColumnInfo(target_col_id);
  const bool is_text = col_info && col_info->text_dictionary.isSet();
  const bool offs_stored =
    col_info && col_info->features.HasFeatures(irs::IndexFeatures::Offs);

  if (is_text && !offs_stored) {
    auto bind = duckdb::make_uniq<connector::OffsetsBindData>();
    bind->inverted_index = found->bind_data->inverted_index;
    bind->column_id = target_col_id;
    bind->limit = limit;
    bind->stored_filter = search_scan.stored_filter;
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
  for (const auto& req : search_scan.offsets) {
    if (req.column_id != target_col_id) {
      continue;
    }
    if (req.limit != limit) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("ts_offsets() called multiple times for field '",
                              col_name, "' with different limits"));
    }
    get_col_idx = req.get_col_idx;
    break;
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

duckdb::unique_ptr<duckdb::Expression> RewriteCallInExpr(
  duckdb::unique_ptr<duckdb::Expression>& expr, duckdb::LogicalOperator& root,
  bool& changed, duckdb::ClientContext& context) {
  if (!expr) {
    return nullptr;
  }
  if (expr->GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
    auto& func = expr->Cast<duckdb::BoundFunctionExpression>();
    const auto& name = func.function.GetName();
    if (IsScorerFunctionName(name)) {
      if (auto repl = PushdownScorerCall(func, root)) {
        changed = true;
        return repl;
      }
    } else if (name == connector::kOffsets) {
      if (auto repl = PushdownOffsetsCall(func, root)) {
        changed = true;
        return repl;
      }
    } else if (auto info = connector::GetAnnFunctionInfo(func)) {
      if (auto repl = PushdownDistanceCall(func, *info, root, context)) {
        changed = true;
        return repl;
      }
    }
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      if (auto r = RewriteCallInExpr(child, root, changed, context)) {
        child = std::move(r);
      }
    });
  return nullptr;
}

bool IsMutationOp(duckdb::LogicalOperatorType t) {
  return t == duckdb::LogicalOperatorType::LOGICAL_DELETE ||
         t == duckdb::LogicalOperatorType::LOGICAL_UPDATE ||
         t == duckdb::LogicalOperatorType::LOGICAL_MERGE_INTO;
}

bool RewriteIResearchExpressions(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::LogicalOperator>& root,
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan, bool in_mutation) {
  const bool subtree_in_mutation = in_mutation || IsMutationOp(plan->type);
  bool changed = false;

  for (auto& child : plan->children) {
    changed |=
      RewriteIResearchExpressions(context, root, child, subtree_in_mutation);
  }
  if (subtree_in_mutation) {
    return changed;
  }

  auto rewrite_call = [&](duckdb::unique_ptr<duckdb::Expression>& expr) {
    if (auto r = RewriteCallInExpr(expr, *root, changed, context)) {
      expr = std::move(r);
    }
  };

  switch (plan->type) {
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
      for (auto& e : plan->Cast<duckdb::LogicalProjection>().expressions) {
        rewrite_call(e);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_FILTER:
      for (auto& e : plan->Cast<duckdb::LogicalFilter>().expressions) {
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
    case duckdb::LogicalOperatorType::LOGICAL_WINDOW:
      for (auto& e : plan->expressions) {
        rewrite_call(e);
      }
      break;
    default:
      break;
  }
  return changed;
}

bool TryClaimIResearchConjunct(
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

bool TryClaimAnnRange(
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  duckdb::LogicalGet& get, connector::SereneDBScanBindData& bind_data,
  const catalog::InvertedIndex& index,
  const connector::SearchFilterOptions& options) {
  duckdb::idx_t match_idx = duckdb::DConstants::INVALID_INDEX;
  float radius;
  std::vector<float> query_vector;
  irs::field_id field_id;
  irs::HNSWMetric metric;
  connector::ScoreEmit score_emit;
  duckdb::OrderType natural_order;

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
    auto expected_current = ExpectedAnnForFunction(func);
    if (!expected_current ||
        expected_current->order != duckdb::OrderType::ASCENDING) {
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
        candidate_radius = radius_value.GetValue<double>();
        break;
      default:
        continue;
    }
    duckdb::Expression* col_expr = nullptr;
    duckdb::Expression* value_expr = nullptr;
    if (expected_current->is_norm) {
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
      col_expr = args.col_arg;
      value_expr = args.value_arg;
    }
    const auto candidate_field_id = ResolveAnnTargetFieldId(
      *col_expr, get, bind_data, index, options.client_context);
    if (!irs::field_limits::valid(candidate_field_id)) {
      continue;
    }
    auto ann_info = index.GetHNSWInfo(candidate_field_id);
    if (!ann_info || ann_info->metric != expected_current->metric) {
      continue;
    }
    std::vector<float> candidate_vector;
    if (expected_current->is_norm) {
      candidate_vector.assign(ann_info->d, 0.0f);
    } else if (!TryFoldQueryVector(options.client_context, *value_expr,
                                   ann_info->d, candidate_vector)) {
      continue;
    }

    radius = candidate_radius;
    query_vector = std::move(candidate_vector);
    field_id = candidate_field_id;
    metric = expected_current->metric;
    score_emit = expected_current->score_emit;
    natural_order = expected_current->order;
    match_idx = i;
    break;
  }

  if (match_idx == duckdb::DConstants::INVALID_INDEX) {
    return false;
  }

  auto& search = bind_data.scan_source->Cast<connector::SearchScan>();
  search.vector_scorer = connector::VectorScorerOptions{
    .field_id = field_id,
    .query_vector = std::move(query_vector),
    .metric = metric,
    .score_emit = score_emit,
    .natural_order = natural_order,
    .radius = radius,
  };
  filters.erase(filters.begin() + match_idx);
  return true;
}

bool TryClaimSearchFilter(
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  duckdb::LogicalGet& get, connector::SereneDBScanBindData& bind_data,
  const catalog::InvertedIndex& index,
  std::shared_ptr<const catalog::Snapshot> snapshot,
  const connector::SearchFilterOptions& options) {
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
  const auto table_index = get.table_index;
  const auto relation_id = index.GetRelationId();
  auto& client_context = options.client_context;

  containers::FlatHashMap<catalog::Column::Id, duckdb::LogicalType>
    column_type_by_id;
  bind_data.IterateColumns(
    [&](catalog::Column::Id id, const duckdb::LogicalType& type) {
      column_type_by_id.emplace(id, type);
    });

  containers::FlatHashSet<catalog::Column::Id> indexed_column_ids;
  {
    auto columns = index.GetColumnIds();
    indexed_column_ids.insert(columns.begin(), columns.end());
  }

  struct IndexedExprMeta {
    duckdb::LogicalType return_type;
    irs::field_id field_id = 0;
  };
  containers::FlatHashMap<std::string_view, IndexedExprMeta>
    indexed_expressions;
  for (const auto& [field_id, entry] : index.GetEntries()) {
    const auto* expr = entry.GetExpressionData();
    if (!expr) {
      continue;
    }
    indexed_expressions.emplace(
      expr->serialized_expr,
      IndexedExprMeta{.return_type = expr->return_type, .field_id = field_id});
  }

  connector::ColumnGetter getter =
    [&](const duckdb::BoundColumnRefExpression& ref)
    -> std::optional<connector::SearchColumnInfo> {
    if (ref.binding.table_index != table_index) {
      return std::nullopt;
    }
    if (ref.binding.column_index >= projected_ids.size()) {
      return std::nullopt;
    }
    const auto col_id = projected_ids[ref.binding.column_index];
    if (col_id == kInvalidId || !indexed_column_ids.contains(col_id)) {
      return std::nullopt;
    }
    const auto* info = index.FindColumnInfo(col_id);
    if (info && !info->IsTermDict()) {
      return std::nullopt;
    }
    auto type_it = column_type_by_id.find(col_id);
    if (type_it == column_type_by_id.end()) {
      return std::nullopt;
    }
    return connector::SearchColumnInfo{
      .field_id = col_id,
      .null_field_id =
        info ? info->null_field_id : irs::field_limits::invalid(),
      .bool_field_id =
        info ? info->bool_field_id : irs::field_limits::invalid(),
      .numeric_field_id =
        info ? info->numeric_field_id : irs::field_limits::invalid(),
      .logical_type = type_it->second,
      .tokenizer = index.GetTokenizer(snapshot, col_id),
    };
  };

  connector::ExpressionGetter expr_getter = [&](const duckdb::Expression& expr)
    -> std::optional<connector::SearchColumnInfo> {
    bool any_ref_seen = false;
    if (!AllColumnRefsBindTo(expr, table_index, any_ref_seen) ||
        !any_ref_seen) {
      return std::nullopt;
    }
    auto normalized = connector::NormalizeBoundExpression(
      expr, relation_id, projected_ids, client_context);
    auto serialized = connector::SerializeBoundExpression(*normalized);
    auto entry = indexed_expressions.find(serialized);
    if (entry == indexed_expressions.end()) {
      return std::nullopt;
    }
    const auto* info = index.FindEntry(entry->second.field_id);
    return connector::SearchColumnInfo{
      .field_id = entry->second.field_id,
      .null_field_id =
        info ? info->null_field_id : irs::field_limits::invalid(),
      .bool_field_id =
        info ? info->bool_field_id : irs::field_limits::invalid(),
      .numeric_field_id =
        info ? info->numeric_field_id : irs::field_limits::invalid(),
      .logical_type = entry->second.return_type,
      .tokenizer = index.GetTokenizer(snapshot, entry->second.field_id),
    };
  };

  auto root = std::make_shared<irs::And>();
  bool any_claimed = false;
  for (size_t i = 0; i < filters.size();) {
    if (TryClaimIResearchConjunct(*root, filters[i], getter, expr_getter,
                                  options)) {
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

  bind_data.scan_source->Cast<connector::SearchScan>().stored_filter = root;
  return true;
}

}  // namespace

void RewriteSearchCallsToColumnRefs(
  duckdb::OptimizerExtensionInput& input,
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  RewriteIResearchExpressions(input.context, plan, plan, false);
}

void IResearchPushdownComplexFilter(
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
  if (bind_data.scan_source->Kind() != connector::ScanSourceKind::Search) {
    return;
  }
  const auto& ss = bind_data.scan_source->Cast<connector::SearchScan>();
  if (ss.stored_filter || ss.text_scorer || ss.vector_scorer) {
    return;
  }
  auto index = bind_data.inverted_index;
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  auto options = BuildOptions(context);
  if (TryClaimAnnRange(filters, get, bind_data, *index, options)) {
    return;
  }
  TryClaimSearchFilter(filters, get, bind_data, *index, snapshot, options);
}

void RegisterIResearchPlanOptimizer(duckdb::DatabaseInstance& db) {
  duckdb::OptimizerExtension::Register(
    db.config, duckdb::OptimizerExtension{
                 .rule = &RewriteSearchCallsToColumnRefs,
                 .anchor = duckdb::OptimizerType::UNUSED_COLUMNS,
                 .where = duckdb::OptimizerHookPosition::Before,
               });
}

}  // namespace sdb::optimizer
