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

#include "connector/scan/col_filter_verify.h"

#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <optional>

#include "connector/scan/scan_state.h"

namespace sdb::connector {
namespace {

struct Interval {
  double lo;
  double hi;
};

struct Domain {
  double min;
  double max;
  bool integral;
};

std::optional<Domain> DomainOf(const duckdb::BaseStatistics& stats) {
  const auto& type = stats.GetType();
  if (!type.IsNumeric() || !duckdb::NumericStats::HasMinMax(stats)) {
    return std::nullopt;
  }
  return Domain{.min = duckdb::NumericStats::Min(stats).GetValue<double>(),
                .max = duckdb::NumericStats::Max(stats).GetValue<double>(),
                .integral = type.IsIntegral()};
}

double IntervalShare(const Domain& domain, Interval interval) {
  const auto from = std::max(interval.lo, domain.min);
  const auto to = std::min(interval.hi, domain.max);
  if (to < from) {
    return 0.0;
  }
  if (domain.integral) {
    return (to - from + 1) / (domain.max - domain.min + 1);
  }
  return domain.max > domain.min ? (to - from) / (domain.max - domain.min)
                                 : 1.0;
}

std::optional<double> NumberOf(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
    return std::nullopt;
  }
  const auto& value = expr.Cast<duckdb::BoundConstantExpression>().GetValue();
  if (value.IsNull() || !value.type().IsNumeric()) {
    return std::nullopt;
  }
  return value.GetValue<double>();
}

bool IsColumn(const duckdb::Expression& expr) {
  return expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_REF;
}

std::optional<Interval> ComparisonInterval(duckdb::ExpressionType type,
                                           double value, const Domain& domain) {
  constexpr auto kInf = std::numeric_limits<double>::infinity();
  const double step = domain.integral ? 1.0 : 0.0;
  switch (type) {
    case duckdb::ExpressionType::COMPARE_EQUAL:
      return Interval{.lo = value, .hi = value};
    case duckdb::ExpressionType::COMPARE_LESSTHAN:
      return Interval{.lo = -kInf, .hi = value - step};
    case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
      return Interval{.lo = -kInf, .hi = value};
    case duckdb::ExpressionType::COMPARE_GREATERTHAN:
      return Interval{.lo = value + step, .hi = kInf};
    case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      return Interval{.lo = value, .hi = kInf};
    default:
      return std::nullopt;
  }
}

std::optional<Interval> IntervalOf(const duckdb::Expression& expr,
                                   const Domain& domain) {
  const auto type = expr.GetExpressionType();
  if (type == duckdb::ExpressionType::CONJUNCTION_AND) {
    Interval out{.lo = domain.min, .hi = domain.max};
    for (const auto& child :
         expr.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
      const auto part = IntervalOf(*child, domain);
      if (!part) {
        return std::nullopt;
      }
      out.lo = std::max(out.lo, part->lo);
      out.hi = std::min(out.hi, part->hi);
    }
    return out;
  }
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return std::nullopt;
  }
  const auto& func = expr.Cast<duckdb::BoundFunctionExpression>();
  if (type == duckdb::ExpressionType::COMPARE_BETWEEN) {
    const auto lo = NumberOf(duckdb::BoundBetweenExpression::LowerBound(func));
    const auto hi = NumberOf(duckdb::BoundBetweenExpression::UpperBound(func));
    if (!IsColumn(duckdb::BoundBetweenExpression::Input(func)) || !lo || !hi) {
      return std::nullopt;
    }
    const auto lower = ComparisonInterval(
      duckdb::BoundBetweenExpression::LowerComparisonType(func), *lo, domain);
    const auto upper = ComparisonInterval(
      duckdb::BoundBetweenExpression::UpperComparisonType(func), *hi, domain);
    if (!lower || !upper) {
      return std::nullopt;
    }
    return Interval{.lo = lower->lo, .hi = upper->hi};
  }
  if (!duckdb::BoundComparisonExpression::IsComparison(type)) {
    return std::nullopt;
  }
  const auto& left = duckdb::BoundComparisonExpression::Left(func);
  const auto& right = duckdb::BoundComparisonExpression::Right(func);
  if (IsColumn(left)) {
    if (const auto value = NumberOf(right)) {
      return ComparisonInterval(type, *value, domain);
    }
  } else if (IsColumn(right)) {
    if (const auto value = NumberOf(left)) {
      return ComparisonInterval(duckdb::FlipComparisonExpression(type), *value,
                                domain);
    }
  }
  return std::nullopt;
}

std::optional<double> ShareOf(const duckdb::Expression& expr,
                              const duckdb::BaseStatistics& stats,
                              const std::optional<Domain>& domain) {
  if (domain) {
    if (const auto interval = IntervalOf(expr, *domain)) {
      return IntervalShare(*domain, *interval);
    }
  }
  const auto known = [&](const duckdb::Expression& child) {
    return ShareOf(child, stats, domain).value_or(irs::detail::kUnknownShare);
  };
  switch (expr.GetExpressionType()) {
    case duckdb::ExpressionType::CONJUNCTION_AND: {
      double share = 1.0;
      for (const auto& child :
           expr.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
        share *= known(*child);
      }
      return share;
    }
    case duckdb::ExpressionType::CONJUNCTION_OR: {
      double miss = 1.0;
      for (const auto& child :
           expr.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
        miss *= 1.0 - known(*child);
      }
      return 1.0 - miss;
    }
    case duckdb::ExpressionType::OPERATOR_NOT: {
      const auto& children =
        expr.Cast<duckdb::BoundOperatorExpression>().GetChildren();
      if (const auto inner = ShareOf(*children[0], stats, domain)) {
        return 1.0 - *inner;
      }
      return std::nullopt;
    }
    case duckdb::ExpressionType::COMPARE_NOTEQUAL: {
      if (!domain || expr.GetExpressionClass() !=
                       duckdb::ExpressionClass::BOUND_FUNCTION) {
        return std::nullopt;
      }
      const auto& func = expr.Cast<duckdb::BoundFunctionExpression>();
      const auto& left = duckdb::BoundComparisonExpression::Left(func);
      const auto& right = duckdb::BoundComparisonExpression::Right(func);
      const auto value = IsColumn(left)    ? NumberOf(right)
                         : IsColumn(right) ? NumberOf(left)
                                           : std::nullopt;
      if (!value) {
        return std::nullopt;
      }
      return 1.0 - IntervalShare(*domain, {.lo = *value, .hi = *value});
    }
    case duckdb::ExpressionType::COMPARE_IN: {
      const auto& children =
        expr.Cast<duckdb::BoundOperatorExpression>().GetChildren();
      if (!domain || !domain->integral || !IsColumn(*children[0])) {
        return std::nullopt;
      }
      double share = 0.0;
      for (size_t i = 1; i < children.size(); ++i) {
        const auto value = NumberOf(*children[i]);
        if (!value) {
          return std::nullopt;
        }
        share += IntervalShare(*domain, {.lo = *value, .hi = *value});
      }
      return std::min(1.0, share);
    }
    case duckdb::ExpressionType::OPERATOR_IS_NULL:
      if (!stats.CanHaveNull()) {
        return 0.0;
      }
      return stats.CanHaveNoNull() ? std::nullopt : std::optional{1.0};
    case duckdb::ExpressionType::OPERATOR_IS_NOT_NULL:
      if (!stats.CanHaveNull()) {
        return 1.0;
      }
      return stats.CanHaveNoNull() ? std::nullopt : std::optional{0.0};
    default:
      return std::nullopt;
  }
}

double EstimateShare(const duckdb::TableFilter& filter,
                     const duckdb::BaseStatistics& stats) {
  const auto domain = DomainOf(stats);
  if (filter.filter_type == duckdb::TableFilterType::EXPRESSION_FILTER) {
    return ShareOf(*filter.Cast<duckdb::ExpressionFilter>().expr, stats, domain)
      .value_or(irs::detail::kUnknownShare);
  }
  const auto expr =
    filter.ToExpression(duckdb::BoundReferenceExpression{stats.GetType(), 0});
  return ShareOf(*expr, stats, domain).value_or(irs::detail::kUnknownShare);
}

}  // namespace

void ColFilterVerify::Begin(const irs::SubReader& seg,
                            std::span<const irs::ColFilterSpec> active,
                            duckdb::ClientContext& context,
                            irs::ColFilterStateCache& states) {
  _chain.Clear();
  _score_filter = nullptr;
  _score_state = nullptr;
  _share = 1.0;
  bool any_col = false;
  for (const auto& spec : active) {
    if (spec.is_score) {
      _score_filter = spec.filter;
      _score_state = &states.State(context, *spec.filter);
    } else {
      any_col = true;
      _share *= spec.share;
    }
  }
  if (!any_col) {
    _ctx.reset();
    return;
  }
  const auto* col_reader = seg.GetColReader();
  SDB_ENSURE(col_reader != nullptr,
             "`.col` table filter requires a columnstore segment");
  if (_ctx) {
    _ctx->Reset(*col_reader);
  } else {
    _ctx = std::make_unique<irs::ReadContext>(*col_reader);
  }
  _chain.Bind(*col_reader, *_ctx, active, context, states);
  _chain.FinishBind();
}

uint32_t ColFilterVerify::Narrow(irs::doc_id_t* docs, irs::score_t* scores,
                                 uint32_t n) {
  if (n == 0) {
    return 0;
  }
  SDB_ASSERT(_score_filter == nullptr || scores != nullptr);
  duckdb::idx_t left = n;
  if (_score_filter != nullptr) {
    left = irs::ColFilterChain::FilterDocsScores(*_score_filter, *_score_state,
                                                 docs, scores, left);
  }
  return static_cast<uint32_t>(_chain.FilterDocs(docs, scores, left));
}

uint32_t ColFilterVerify::Narrow(irs::doc_id_t base, const uint64_t* in,
                                 uint64_t* out, irs::score_t* scores,
                                 uint32_t words) {
  SDB_ASSERT(_score_filter == nullptr || scores != nullptr);
  if (_score_filter != nullptr) {
    irs::ColFilterChain::FilterMaskScores(*_score_filter, *_score_state, in,
                                          out, scores, words);
    in = out;
  }
  return static_cast<uint32_t>(_chain.FilterMask(base, in, out, words));
}

uint64_t ColFilterVerify::CountAndClear(irs::doc_id_t base, uint64_t* mask,
                                        uint32_t words) {
  SDB_ASSERT(_score_filter == nullptr);
  return static_cast<uint64_t>(_chain.CountMask(base, mask, words));
}

void ClassifySegmentColFilters(
  const irs::SubReader& seg, ScanGlobalState& g,
  std::span<const ScanGlobalState::ColFilter> filters,
  irs::ColFilterStateCache& states, irs::ColFilterClassification& out,
  bool prepare_leaves) {
  out.segment_dead = false;
  out.active.clear();
  if (filters.empty()) {
    return;
  }
  const auto* col_reader = seg.GetColReader();
  for (const auto& cf : filters) {
    irs::ColFilterSpec spec{
      .field = cf.field,
      .filter = cf.filter,
      .is_score = cf.is_score,
      .is_dynamic = cf.is_dynamic,
      .zonemap_only = cf.zonemap_only,
      .null_check = cf.null_check,
      .row_gather = cf.row_gather,
      .not_null = cf.not_null.get(),
      .extract_path = cf.extract_path,
      .extract_type = &cf.type,
      .row_fields = cf.row_fields,
      .row_types = cf.row_types,
    };
    if (prepare_leaves && !cf.row_leaves.empty()) {
      spec.row_leaves = states.Leaves(*cf.filter, seg, cf.row_leaves);
    }
    if (spec.is_score || !spec.row_fields.empty()) {
      out.active.push_back(spec);
      continue;
    }
    const auto* column =
      col_reader != nullptr ? col_reader->Column(spec.field) : nullptr;
    if (spec.row_gather) {
      if (column == nullptr) {
        out.segment_dead = true;
        out.active.clear();
        return;
      }
      out.active.push_back(spec);
      continue;
    }
    const auto* reader =
      column != nullptr && !cf.extract_path.empty()
        ? irs::DirectExtractLeaf(*column, cf.extract_path, cf.type)
        : column;
    if (column != nullptr && reader == nullptr) {
      out.active.push_back(spec);
      continue;
    }
    if (reader == nullptr) {
      duckdb::Vector null_row{duckdb::Value{cf.type}, duckdb::count_t{1}};
      duckdb::SelectionVector sel;
      duckdb::idx_t approved = 1;
      duckdb::ColumnSegment::FilterSelection(
        sel, null_row, states.State(*g.client_context, *spec.filter), 1,
        approved);
      if (approved == 0) {
        out.segment_dead = true;
        out.active.clear();
        return;
      }
      continue;
    }
    const auto& stats = reader->MergedStatistics();
    const auto verdict =
      spec.filter->Cast<duckdb::ExpressionFilter>().CheckStatistics(
        stats, states.State(*g.client_context, *spec.filter));
    switch (verdict) {
      case duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE:
      case duckdb::FilterPropagateResult::FILTER_FALSE_OR_NULL:
        out.segment_dead = true;
        out.active.clear();
        return;
      case duckdb::FilterPropagateResult::FILTER_ALWAYS_TRUE:
        if (spec.zonemap_only) {
          spec.share = 1.0;
          out.active.push_back(spec);
        }
        break;
      case duckdb::FilterPropagateResult::FILTER_TRUE_OR_NULL:
        if (!spec.zonemap_only && spec.not_null != nullptr) {
          spec.filter = spec.not_null;
          spec.null_check = irs::NullCheckKind::IsNotNull;
        }
        spec.share = 1.0;
        out.active.push_back(spec);
        break;
      case duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE:
        spec.share = EstimateShare(*cf.filter, stats);
        out.active.push_back(spec);
        break;
    }
  }
}

void ScanLocalState::Classify(ScanGlobalState& g, uint32_t seg) {
  if (classified_seg == seg) {
    return;
  }
  ClassifySegmentColFilters((*g.reader)[seg], g, g.col_filters, filter_states,
                            seg_cls, true);
  classified_seg = seg;
}

irs::detail::TableFilter* BeginVerify(ColFilterVerify& verify,
                                      const irs::SubReader& seg,
                                      ScanGlobalState& g, ScanLocalState& l) {
  verify.Begin(seg, l.seg_cls.active, *g.client_context, l.filter_states);
  return verify.Empty() ? nullptr : &verify;
}

irs::detail::TableFilter* BeginColumnVerify(const irs::SubReader& seg,
                                            ScanGlobalState& g,
                                            AnnLocalState& l) {
  if (g.dual_filters.empty()) {
    return nullptr;
  }
  ClassifySegmentColFilters(seg, g, g.dual_filters, l.filter_states, l.dual_cls,
                            true);
  if (l.dual_cls.segment_dead) {
    return nullptr;
  }
  l.column_specs.assign(l.dual_cls.active.begin(), l.dual_cls.active.end());
  l.column_specs.insert(l.column_specs.end(), l.seg_cls.active.begin(),
                        l.seg_cls.active.end());
  l.column_verify.Begin(seg, l.column_specs, *g.client_context,
                        l.filter_states);
  return l.column_verify.Empty() ? nullptr : &l.column_verify;
}

}  // namespace sdb::connector
