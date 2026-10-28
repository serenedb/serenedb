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

#include <absl/algorithm/container.h>

#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <iresearch/utils/assert.hpp>
#include <iterator>
#include <ranges>
#include <utf8proc_wrapper.hpp>

#include "pg/catalog/engine/system_table.h"

namespace sdb::pg {
namespace {

bool IsNumber(const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
      return true;
    default:
      return false;
  }
}

template<typename T>
T Constant(const duckdb::Value& value) {
  if constexpr (std::is_same_v<T, std::string>) {
    SDB_ASSERT(value.type().id() == duckdb::LogicalTypeId::VARCHAR);
    return duckdb::StringValue::Get(value);
  } else {
    SDB_ASSERT(IsNumber(value.type()));
    return value.GetValue<int64_t>();
  }
}

template<typename T>
std::optional<T> Constant(const duckdb::Expression& expr) {
  if (expr.GetExpressionType() != duckdb::ExpressionType::VALUE_CONSTANT) {
    return std::nullopt;
  }
  const auto& value = expr.Cast<duckdb::BoundConstantExpression>().GetValue();
  if (value.IsNull()) {
    return std::nullopt;
  }
  return Constant<T>(value);
}

struct SystemRange {
  std::string lower;
  std::string upper;
};

std::optional<SystemRange> RangeOf(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return std::nullopt;
  }
  const auto& function = expr.Cast<duckdb::BoundFunctionExpression>();
  const auto& name = function.Function().GetName();
  if (name != "prefix" && name != "starts_with" && name != "^@") {
    return std::nullopt;
  }
  const auto& children = function.GetChildren();
  SDB_ASSERT(children.size() == 2);
  const auto* column = children[0].get();
  if (duckdb::BoundCastExpression::IsCast(*column)) {
    const auto& cast = column->Cast<duckdb::BoundFunctionExpression>();
    SDB_ASSERT(cast.GetReturnType().id() == duckdb::LogicalTypeId::VARCHAR);
    if (duckdb::BoundCastExpression::SourceType(cast).id() !=
        duckdb::LogicalTypeId::VARCHAR) {
      return std::nullopt;
    }
    column = &duckdb::BoundCastExpression::Child(cast);
  }
  auto text = Constant<std::string>(*children[1]);
  if (!duckdb::ExpressionFilter::IsSimpleFilterColumnRef(*column) || !text ||
      text->empty()) {
    return std::nullopt;
  }
  SystemRange range{.lower = *text, .upper = std::move(*text)};
  if (!duckdb::Utf8Proc::FindNextLegalUTF8(range.upper)) {
    return std::nullopt;
  }
  return range;
}

std::optional<bool> BooleanOf(const duckdb::Expression& expr) {
  const bool negated =
    expr.GetExpressionType() == duckdb::ExpressionType::OPERATOR_NOT;
  const auto& operand =
    negated ? *expr.Cast<duckdb::BoundOperatorExpression>().GetChildren()[0]
            : expr;
  if (!duckdb::ExpressionFilter::IsSimpleFilterColumnRef(operand)) {
    return std::nullopt;
  }
  SDB_ASSERT(operand.GetReturnType().id() == duckdb::LogicalTypeId::BOOLEAN);
  return !negated;
}

std::optional<bool> NullCheckOf(const duckdb::Expression& expr) {
  const auto type = expr.GetExpressionType();
  if ((type != duckdb::ExpressionType::OPERATOR_IS_NULL &&
       type != duckdb::ExpressionType::OPERATOR_IS_NOT_NULL) ||
      !duckdb::ExpressionFilter::IsSimpleFilterColumnRef(
        *expr.Cast<duckdb::BoundOperatorExpression>().GetChildren()[0])) {
    return std::nullopt;
  }
  return type == duckdb::ExpressionType::OPERATOR_IS_NULL;
}

bool IsVisibility(const duckdb::Expression& expr,
                  const SystemVisibility& visibility) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return false;
  }
  const auto& function = expr.Cast<duckdb::BoundFunctionExpression>();
  if (function.Function().GetName() != visibility.function) {
    return false;
  }
  const auto& children = function.GetChildren();
  SDB_ASSERT(children.size() == 1);
  return duckdb::ExpressionFilter::IsSimpleFilterColumnRef(*children[0]);
}

template<typename T>
void Tighten(std::optional<SystemBound<T>>& bound, SystemBound<T> candidate,
             bool lower) {
  const bool tighter = !bound || (candidate.value == bound->value
                                    ? bound->inclusive && !candidate.inclusive
                                    : (lower ? bound->value < candidate.value
                                             : candidate.value < bound->value));
  if (tighter) {
    bound = std::move(candidate);
  }
}

template<typename T>
void Intersect(std::optional<std::vector<T>>& keys, std::vector<T> set) {
  absl::c_sort(set);
  const auto duplicates = std::ranges::unique(set);
  set.erase(duplicates.begin(), duplicates.end());
  if (keys) {
    std::erase_if(set, [&](const T& value) {
      return !absl::c_binary_search(*keys, value);
    });
  }
  keys = std::move(set);
}

template<typename T>
bool Restrict(SystemCondition<T>& condition, duckdb::ExpressionType type,
              T value) {
  using enum duckdb::ExpressionType;
  if (type == COMPARE_NOT_DISTINCT_FROM) {
    condition.not_null = true;
    Intersect(condition.keys, std::vector<T>{std::move(value)});
  } else if (type == COMPARE_EQUAL) {
    Intersect(condition.keys, std::vector<T>{std::move(value)});
  } else if (type == COMPARE_NOTEQUAL) {
    condition.excluded.emplace_back(std::move(value));
  } else if (type == COMPARE_GREATERTHAN ||
             type == COMPARE_GREATERTHANOREQUALTO) {
    Tighten(
      condition.lower,
      SystemBound<T>{std::move(value), type == COMPARE_GREATERTHANOREQUALTO},
      true);
  } else if (type == COMPARE_LESSTHAN || type == COMPARE_LESSTHANOREQUALTO) {
    Tighten(condition.upper,
            SystemBound<T>{std::move(value), type == COMPARE_LESSTHANOREQUALTO},
            false);
  } else {
    return false;
  }
  return true;
}

template<typename T>
bool Capture(const duckdb::Expression& expr, SystemCondition<T>& condition) {
  const auto type = expr.GetExpressionType();
  if (type == duckdb::ExpressionType::COMPARE_IN ||
      type == duckdb::ExpressionType::COMPARE_NOT_IN) {
    const auto& children =
      expr.Cast<duckdb::BoundOperatorExpression>().GetChildren();
    if (!duckdb::ExpressionFilter::IsSimpleFilterColumnRef(*children[0])) {
      return false;
    }
    std::vector<T> set;
    for (const auto& child : std::span{children}.subspan(1)) {
      if (child->GetExpressionType() !=
          duckdb::ExpressionType::VALUE_CONSTANT) {
        return false;
      }
      const auto& value =
        child->Cast<duckdb::BoundConstantExpression>().GetValue();
      if (!value.IsNull()) {
        set.emplace_back(Constant<T>(value));
      } else if (type == duckdb::ExpressionType::COMPARE_NOT_IN) {
        return false;
      }
    }
    if (type == duckdb::ExpressionType::COMPARE_IN) {
      Intersect(condition.keys, std::move(set));
    } else {
      absl::c_move(set, std::back_inserter(condition.excluded));
    }
    return true;
  }
  if (duckdb::BoundComparisonExpression::IsComparison(expr)) {
    auto comparison = duckdb::ExpressionType::INVALID;
    const auto constant =
      duckdb::ExpressionFilter::TryGetColumnConstantComparison(
        expr.Cast<duckdb::BoundFunctionExpression>(), comparison);
    if (!constant) {
      return false;
    }
    auto value = Constant<T>(*constant);
    return value && Restrict(condition, comparison, std::move(*value));
  }
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONJUNCTION) {
    const auto& children =
      expr.Cast<duckdb::BoundConjunctionExpression>().GetChildren();
    if (type == duckdb::ExpressionType::CONJUNCTION_AND) {
      return absl::c_all_of(
        children, [&](const duckdb::unique_ptr<duckdb::Expression>& child) {
          return Capture(*child, condition);
        });
    }
    std::vector<T> set;
    bool not_null = true;
    for (const auto& child : children) {
      SystemCondition<T> branch;
      if (!Capture(*child, branch) || !branch.KeysOnly()) {
        return false;
      }
      not_null = not_null && branch.not_null;
      absl::c_move(*branch.keys, std::back_inserter(set));
    }
    condition.not_null = condition.not_null || not_null;
    Intersect(condition.keys, std::move(set));
    return true;
  }
  if (duckdb::ExpressionFilter::IsRootOptionalExpression(expr)) {
    if (const auto child =
          duckdb::ExpressionFilter::GetOptionalFilterChild(expr)) {
      Capture(*child, condition);
    }
    return true;
  }
  if constexpr (std::is_same_v<T, int64_t>) {
    if (const auto value = BooleanOf(expr)) {
      Intersect(condition.keys, std::vector<T>{*value ? 1 : 0});
      return true;
    }
  }
  if constexpr (std::is_same_v<T, std::string>) {
    if (auto range = RangeOf(expr)) {
      Tighten(condition.lower, SystemBound<T>{std::move(range->lower), true},
              true);
      Tighten(condition.upper, SystemBound<T>{std::move(range->upper), false},
              false);
      return true;
    }
  }
  if (const auto null = NullCheckOf(expr)) {
    (*null ? condition.null : condition.not_null) = true;
    return true;
  }
  if (type == duckdb::ExpressionType::OPERATOR_NOT) {
    const auto& child =
      *expr.Cast<duckdb::BoundOperatorExpression>().GetChildren()[0];
    bool nulls = false;
    duckdb::ExpressionIterator::VisitExpression<
      duckdb::BoundConstantExpression>(
      child, [&](const duckdb::BoundConstantExpression& constant) {
        nulls = nulls || constant.GetValue().IsNull();
      });
    SystemCondition<T> negated;
    if (nulls || !Capture(child, negated) || !negated.KeysOnly() ||
        negated.not_null) {
      return false;
    }
    absl::c_move(*negated.keys, std::back_inserter(condition.excluded));
    return true;
  }
  return false;
}

template<typename T>
void Seal(SystemCondition<T>& condition) {
  std::optional<std::vector<T>> keys;
  std::swap(keys, condition.keys);
  if (!keys && condition.lower && condition.upper &&
      condition.lower->inclusive && condition.upper->inclusive &&
      condition.lower->value == condition.upper->value) {
    keys.emplace().emplace_back(condition.lower->value);
  }
  if (!keys) {
    return;
  }
  std::erase_if(*keys, [&](const T& key) { return !condition.Passes(key); });
  condition = {.keys = std::move(keys),
               .null = condition.null,
               .not_null = condition.not_null};
}

struct SystemClaim {
  bool claimed;
  bool visibility;
};

SystemClaim Claim(const SystemTable& table, duckdb::column_t column,
                  const duckdb::Expression& expr, SystemFilter& compiled) {
  SDB_ASSERT(column < table.Columns().LogicalColumnCount());
  if (const auto& visibility = table.Visibility();
      visibility && visibility->column == column &&
      IsVisibility(expr, *visibility)) {
    return {.claimed = true, .visibility = true};
  }
  if (expr.GetExpressionType() == duckdb::ExpressionType::CONJUNCTION_AND) {
    SystemClaim claim{.claimed = true, .visibility = false};
    for (const auto& child :
         expr.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
      const auto part = Claim(table, column, *child, compiled);
      if (!part.claimed &&
          !duckdb::ExpressionFilter::IsRootOptionalExpression(*child)) {
        return part;
      }
      claim.visibility = claim.visibility || part.visibility;
    }
    return claim;
  }
  const auto& type =
    table.Columns().GetColumn(duckdb::LogicalIndex{column}).Type();
  if (type.id() == duckdb::LogicalTypeId::VARCHAR) {
    return {.claimed = Capture(expr, compiled.texts), .visibility = false};
  }
  return {.claimed = IsNumber(type) && Capture(expr, compiled.numbers),
          .visibility = false};
}

void Finish(const SystemSqlColumn& meta, SystemFilter& compiled) {
  compiled.texts.not_null = compiled.texts.not_null && !meta.not_null;
  compiled.numbers.not_null = compiled.numbers.not_null && !meta.not_null;
  Seal(compiled.texts);
  Seal(compiled.numbers);
  if (meta.type == kChar) {
    for (uint32_t c = 0; c < 256; ++c) {
      const auto ch = static_cast<char>(c);
      compiled.chars[c] = compiled.texts.Passes(std::string_view{&ch, c != 0});
    }
  }
}

}  // namespace

bool SystemScan::Overlaps(uint32_t column, int64_t min, int64_t max) const {
  if (const auto* filter = FilterOf(column);
      filter && !filter->numbers.Overlaps(min, max)) {
    return false;
  }
  return absl::c_none_of(_prunes, [&](const SystemPrune& prune) {
    if (prune.column != column) {
      return false;
    }
    duckdb::NumericStats::SetMin<int64_t>(*prune.stats, min);
    duckdb::NumericStats::SetMax<int64_t>(*prune.stats, max);
    const auto result =
      duckdb::ExpressionFilter::GetExpressionFilter(*prune.filter, "SystemScan")
        .CheckStatistics(*prune.stats, *prune.state);
    return result == duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE ||
           result == duckdb::FilterPropagateResult::FILTER_FALSE_OR_NULL;
  });
}

bool SystemScan::Possible(std::span<const SystemFact> facts) const {
  uint64_t constrained = 0;
  uint64_t possible = 0;
  for (const auto& fact : facts) {
    const auto bit = uint64_t{1} << fact.column;
    constrained |= bit;
    if (Allows(fact.column, fact.min, fact.max)) {
      possible |= bit;
    }
  }
  return possible == constrained;
}

duckdb::TableFilterPushdown SystemPushdown(const SystemTable& table,
                                           duckdb::column_t column,
                                           const duckdb::Expression& expr) {
  if (duckdb::ExpressionFilter::IsRootOptionalExpression(expr)) {
    return duckdb::TableFilterPushdown::BeforeLimit;
  }
  SystemFilter compiled;
  return Claim(table, column, expr, compiled).claimed
           ? duckdb::TableFilterPushdown::BeforeLimit
           : duckdb::TableFilterPushdown::Reject;
}

void SystemScan::Compile(duckdb::column_t column,
                         const duckdb::TableFilter& filter) {
  const auto& expr =
    *duckdb::ExpressionFilter::GetExpressionFilter(filter, "SystemScan").expr;
  SystemFilter compiled;
  const auto claim = Claim(_table, column, expr, compiled);
  if (claim.visibility) {
    SDB_ASSERT(!_session);
    _session.emplace(MakeSession(&_context));
  }
  const bool optional =
    duckdb::ExpressionFilter::IsRootOptionalExpression(expr);
  SDB_ASSERT(claim.claimed || optional);
  SDB_ASSERT(column < _table.Columns().LogicalColumnCount());
  const auto& meta = _table.Sql().columns[column];
  Finish(meta, compiled);
  if (const auto& type =
        _table.Columns().GetColumn(duckdb::LogicalIndex{column}).Type();
      optional && !compiled.numbers.keys && type.IsIntegral()) {
    _pruned |= uint64_t{1} << column;
    _prunes.emplace_back(static_cast<uint32_t>(column), &filter,
                         duckdb::TableFilterState::Initialize(_context, filter),
                         duckdb::BaseStatistics::FromConstant(
                           duckdb::Value::BIGINT(0).DefaultCastAs(type))
                           .ToUnique());
  }
  if (compiled.texts.Empty() && compiled.numbers.Empty()) {
    return;
  }
  const bool passes =
    !compiled.texts.Empty()
      ? (meta.zero_default ? compiled.texts.Passes(std::string_view{})
                           : compiled.texts.PassesNull())
      : (meta.zero_default ? compiled.numbers.Passes(int64_t{0})
                           : compiled.numbers.PassesNull());
  _filtered |= uint64_t{1} << column;
  if (!passes) {
    _rejecting |= uint64_t{1} << column;
  }
  _filter_index[column] = static_cast<uint8_t>(_filters.size());
  _filters.emplace_back(std::move(compiled));
}

}  // namespace sdb::pg
