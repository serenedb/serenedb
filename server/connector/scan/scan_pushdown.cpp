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

// A user-space score lower bound is a raw-space collector floor only where
// the user-facing score IS the raw score: text scorers, and Identity-emit
// vector scorers whose collector holds exact (unquantized) scores. The
// decreasing emits (l2/cosine distance, ...) invert the bound -- their
// near-keeping bounds are consumed as the vector query radius instead -- and
// a quantized collector's approximate scores must not be cut by an exact
// floor.
bool ScoreFloorApplies(const ScanBindData& bind) {
  if (bind.score.text) {
    return true;
  }
  return bind.score.vector &&
         bind.score.vector->score_emit == ScoreEmit::Identity &&
         bind.score.vector->quant == irs::VectorQuantization::None;
}

// Score-column filter policy. The filter may be one predicate or an AND
// combination (a TableFilterSet holds one filter per column). Static lower
// bounds (`score > c` / `>= c`) are recorded as `score.static_floor`: it
// seeds the streaming prune threshold, the top-k collectors and the Min Score
// display. On the top-k collector path the scan enforces score bounds itself,
// so those conjuncts are stripped from the filter: the dynamic TOP_N boundary
// (the collector maintains its own) and static lower bounds where the floor
// is the collector's raw space (ScoreFloorApplies) -- the collector starts at
// the floor. An empty residue drops the filter from the plan entirely.
// Everything else stays pushed and evaluated per row: on the streaming path
// the prune threshold only skips blocks, it enforces nothing.
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

// Per-filter pushdown decision (see TableFilterPushdown), finer than the
// per-column supports_pushdown_type. BeforeLimit: covered `.col` INCLUDE
// filters, and score-column filters (or what remains of them after
// HandleScoreFilter strips the collector-enforced conjuncts) -- applied
// in-scan before any limit/lookup, so a pushed top-k stays valid and the
// lookup only fetches survivors. Drop: a score filter the scan enforces
// entirely by itself (HandleScoreFilter), removed from the plan. AfterLimit:
// lookup-source filters (parquet/duckdb apply them during the source scan,
// after the doc-id selection) -- forces the scan unlimited. Reject: other
// virtuals, search-table columns, csv/json lookups -- stay a Filter node
// above the scan (which, being a LogicalFilter, also forces streaming, so
// top-k stays correct).
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
      // Vector (ANN) search is approximate: keep the pushed top-k (BeforeLimit)
      // so the collector runs the Top: path -- it over-fetches a pool by score,
      // this lookup filter is applied during materialization, and the TOP_N
      // above trims to the exact k. A text/exact scan keeps AfterLimit: the
      // lookup filter runs after doc selection, forcing the unlimited streaming
      // scan (a pushed top-k there could drop true matches).
      return bind.score.vector ? duckdb::TableFilterPushdown::BeforeLimit
                               : duckdb::TableFilterPushdown::AfterLimit;
    }
  }
  return duckdb::TableFilterPushdown::Reject;
}

// Accept single-column generic expressions (IS NULL, arithmetic, extracts,
// function predicates) as pushed ExpressionFilters, like the native table
// scan: the chain evaluates the whole expression against the decoded column.
// Only for covered `.col` columns (and the computed score) -- expressions on
// merely-indexed text columns, vector columns and lookup columns keep their
// Filter node above the scan, where the dedicated ts_dict / vector-radius /
// lookup handling already deals with them.
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
