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

#include "connector/scan/col_filter_share.h"

#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <iresearch/search/detail/table_filter.hpp>
#include <limits>
#include <optional>

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

}  // namespace

double EstimateShare(const duckdb::Expression& filter,
                     const duckdb::BaseStatistics& stats) {
  return ShareOf(filter, stats, DomainOf(stats))
    .value_or(irs::detail::kUnknownShare);
}

}  // namespace sdb::connector
