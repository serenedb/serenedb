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
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_join.hpp>
#include <duckdb/planner/operator/logical_order.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_top_n.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/proxy_filter.hpp>

#include "basics/containers/trivial_map.h"
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
  const auto& idx_col_ids = found->bind_data->inverted_index->GetColumnIds();
  if (absl::c_find(idx_col_ids, target_col_id) == idx_col_ids.end()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(): column '", col_name(), "' not found in index"));
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
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan, bool in_mutation) {
  const bool subtree_in_mutation =
    in_mutation || plan->type == duckdb::LogicalOperatorType::LOGICAL_DELETE ||
    plan->type == duckdb::LogicalOperatorType::LOGICAL_UPDATE ||
    plan->type == duckdb::LogicalOperatorType::LOGICAL_MERGE_INTO;

  for (auto& child : plan->children) {
    RewriteIResearchExpressions(context, root, child, subtree_in_mutation);
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

  const auto make_info = [&](auto field_id, const auto* info,
                             duckdb::LogicalType type) {
    return connector::SearchColumnInfo{
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
  };

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
    const auto col_id = ResolveColumnId(ref.binding, bind_data, get);
    if (col_id == catalog::Column::kInvalidId ||
        !absl::c_linear_search(index.GetColumnIds(), col_id)) {
      return std::nullopt;
    }
    const auto* info = index.FindColumnInfo(col_id);
    if (info && !info->IsTermDict()) {
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
    auto entry = indexed_expressions.find(serialized);
    if (entry == indexed_expressions.end()) {
      return std::nullopt;
    }
    const auto* info = index.FindEntry(entry->second.field_id);
    return make_info(entry->second.field_id, info, entry->second.return_type);
  };

  auto& scan = bind_data.scan_source->Cast<connector::SearchScan>();
  auto [stored,
        root_ptr] = [&] -> std::pair<std::shared_ptr<irs::Filter>, irs::And*> {
    if (scan.vector_scorer) {
      auto proxy = std::make_shared<irs::ProxyFilter>();
      auto* root =
        &proxy->set_filter<irs::And>(irs::IResourceManager::gNoop).first;
      return {std::move(proxy), root};
    }
    auto and_filter = std::make_shared<irs::And>();
    auto* root = and_filter.get();
    return {std::move(and_filter), root};
  }();

  bool any_claimed = false;
  for (size_t i = 0; i < filters.size();) {
    if (TryClaimIResearchConjunct(*root_ptr, filters[i], getter, expr_getter,
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

  scan.stored_filter = std::move(stored);
  for (auto& req : scan.offsets) {
    if (req.bind) {
      req.bind->stored_filter = scan.stored_filter;
    }
  }
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
