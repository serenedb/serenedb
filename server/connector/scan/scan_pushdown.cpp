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

#include <duckdb/optimizer/column_lifetime_analyzer.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>

#include "connector/scan/scan_bind.h"
#include "connector/scan/scan_function.h"

namespace sdb::connector {
namespace {

bool ScoreFloorApplies(const ScanBindData& bind) {
  if (bind.score.text) {
    return true;
  }
  return bind.score.vector &&
         bind.score.vector->score_emit == ScoreEmit::Identity &&
         bind.score.vector->quant == irs::VectorQuantization::None;
}

duckdb::TableFilterPushdown HandleScoreFilter(ScanBindData& bind,
                                              duckdb::TableFilter& filter) {
  auto& expr_filter =
    duckdb::ExpressionFilter::GetExpressionFilter(filter, "HandleScoreFilter");
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> conjuncts;
  if (expr_filter.expr->GetExpressionClass() ==
        duckdb::ExpressionClass::BOUND_CONJUNCTION &&
      expr_filter.expr->GetExpressionType() ==
        duckdb::ExpressionType::CONJUNCTION_AND) {
    conjuncts =
      std::move(expr_filter.expr->Cast<duckdb::BoundConjunctionExpression>()
                  .GetChildrenMutable());
  } else {
    conjuncts.push_back(std::move(expr_filter.expr));
  }
  const bool collector_enforces = bind.score.top_k.has_value();
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> residue;
  for (auto& conjunct : conjuncts) {
    if (duckdb::ExpressionFilter::GetOptionalDynamicFilterData(*conjunct)) {
      if (!collector_enforces) {
        residue.push_back(std::move(conjunct));
      }
      continue;
    }
    if (ScoreFloorApplies(bind)) {
      bool exact = false;
      const auto floor = StaticScoreFloor(*conjunct, exact);
      bind.score.static_floor = std::max(bind.score.static_floor, floor);
      if (exact && collector_enforces) {
        continue;
      }
    }
    residue.push_back(std::move(conjunct));
  }
  if (residue.empty()) {
    return duckdb::TableFilterPushdown::Drop;
  }
  if (residue.size() == 1) {
    expr_filter.expr = std::move(residue.front());
  } else {
    auto conj = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
      duckdb::ExpressionType::CONJUNCTION_AND);
    conj->GetChildrenMutable() = std::move(residue);
    expr_filter.expr = std::move(conj);
  }
  return duckdb::TableFilterPushdown::BeforeLimit;
}

}  // namespace

bool IResearchSupportsPushdownExtract(const duckdb::FunctionData& bind_data_p,
                                      const duckdb::LogicalIndex& col_idx) {
  const auto& bind = bind_data_p.Cast<ScanBindData>();
  if (!bind.relation.IsInvertedIndex()) {
    return false;
  }
  const auto bind_col = col_idx.index;
  if (bind_col >= bind.columns.ids.size()) {
    return false;
  }
  const auto type_id = bind.columns.types[bind_col].id();
  if (type_id != duckdb::LogicalTypeId::VARIANT &&
      type_id != duckdb::LogicalTypeId::STRUCT) {
    return false;
  }
  const auto* info =
    bind.relation.ScannedIndex().FindColumnInfo(bind.columns.ids[bind_col]);
  return info != nullptr && info->store_values;
}

duckdb::TableFilterPushdown IResearchSupportsPushdownFilter(
  duckdb::FunctionData& bind_data_p, duckdb::idx_t col_idx,
  duckdb::TableFilter& filter) {
  auto& bind = bind_data_p.Cast<ScanBindData>();
  if (col_idx >= bind.columns.ids.size()) {
    return duckdb::TableFilterPushdown::Reject;
  }
  const auto col_id = bind.columns.ids[col_idx];
  if (col_id == catalog::kInvertedIndexScoreId) {
    return HandleScoreFilter(bind, filter);
  }
  if (col_id.id() > catalog::kMaxRealColumnIdValue) {
    return duckdb::TableFilterPushdown::Reject;
  }
  if (bind.relation.IsSearchTable()) {
    return duckdb::TableFilterPushdown::BeforeLimit;
  }
  if (bind.relation.IsInvertedIndex()) {
    const auto* info = bind.relation.ScannedIndex().FindColumnInfo(col_id);
    if (info && info->IsStored()) {
      return duckdb::TableFilterPushdown::BeforeLimit;
    }
    if (bind.lookup.supports_filters) {
      return bind.score.vector ? duckdb::TableFilterPushdown::BeforeLimit
                               : duckdb::TableFilterPushdown::AfterLimit;
    }
  }
  return duckdb::TableFilterPushdown::Reject;
}

bool IResearchPushdownExpression(duckdb::ClientContext&,
                                 const duckdb::LogicalGet& get,
                                 duckdb::Expression& expr) {
  const auto& bind = get.bind_data->Cast<ScanBindData>();
  duckdb::vector<duckdb::ColumnBinding> bindings;
  duckdb::ColumnLifetimeAnalyzer::ExtractColumnBindings(expr, bindings);
  if (bindings.empty()) {
    return false;
  }
  const auto& column_ids = get.GetColumnIds();
  if (bindings[0].column_index >= column_ids.size()) {
    return false;
  }
  const auto col_idx = column_ids[bindings[0].column_index].GetPrimaryIndex();
  if (col_idx >= bind.columns.ids.size()) {
    return false;
  }
  const auto col_id = bind.columns.ids[col_idx];
  if (col_id == catalog::kInvertedIndexScoreId) {
    return true;
  }
  if (col_id.id() > catalog::kMaxRealColumnIdValue) {
    return false;
  }
  if (bind.relation.IsSearchTable()) {
    return true;
  }
  if (bind.relation.IsInvertedIndex()) {
    const auto* info = bind.relation.ScannedIndex().FindColumnInfo(col_id);
    return info != nullptr && info->IsStored();
  }
  return false;
}

}  // namespace sdb::connector
