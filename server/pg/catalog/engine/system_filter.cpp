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
#include <ranges>
#include <utf8proc_wrapper.hpp>

#include "pg/catalog/engine/system_table.h"

namespace sdb::pg {
namespace {

bool IsNumber(const duckdb::LogicalType& type) {
  return type.id() == duckdb::LogicalTypeId::BOOLEAN ||
         (type.IsIntegral() &&
          type.InternalType() != duckdb::PhysicalType::UINT64 &&
          duckdb::GetTypeIdSize(type.InternalType()) <= sizeof(int64_t));
}

template<typename T>
std::optional<T> Constant(const duckdb::Value& value) {
  if constexpr (std::is_same_v<T, std::string>) {
    if (value.type().id() != duckdb::LogicalTypeId::VARCHAR) {
      return std::nullopt;
    }
    return duckdb::StringValue::Get(value);
  } else {
    if (!IsNumber(value.type())) {
      return std::nullopt;
    }
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
  const auto& children = function.GetChildren();
  if ((name != "prefix" && name != "starts_with" && name != "^@") ||
      children.size() != 2) {
    return std::nullopt;
  }
  const auto* column = children[0].get();
  if (duckdb::BoundCastExpression::IsCast(*column)) {
    const auto& cast = column->Cast<duckdb::BoundFunctionExpression>();
    if (duckdb::BoundCastExpression::SourceType(cast).id() !=
          duckdb::LogicalTypeId::VARCHAR ||
        cast.GetReturnType().id() != duckdb::LogicalTypeId::VARCHAR) {
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
  if (!duckdb::ExpressionFilter::IsSimpleFilterColumnRef(operand) ||
      operand.GetReturnType().id() != duckdb::LogicalTypeId::BOOLEAN) {
    return std::nullopt;
  }
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
  const auto& children = function.GetChildren();
  return function.Function().GetName() == visibility.function &&
         children.size() == 1 &&
         duckdb::ExpressionFilter::IsSimpleFilterColumnRef(*children[0]);
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
  set.erase(std::unique(set.begin(), set.end()), set.end());
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
  if (type == COMPARE_EQUAL) {
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
  using enum duckdb::ExpressionClass;
  if (expr.GetExpressionClass() == BOUND_OPERATOR &&
      (expr.GetExpressionType() == duckdb::ExpressionType::COMPARE_IN ||
       expr.GetExpressionType() == duckdb::ExpressionType::COMPARE_NOT_IN)) {
    const auto& children =
      expr.Cast<duckdb::BoundOperatorExpression>().GetChildren();
    if (children.empty() ||
        !duckdb::ExpressionFilter::IsSimpleFilterColumnRef(*children[0])) {
      return false;
    }
    const bool excluded =
      expr.GetExpressionType() == duckdb::ExpressionType::COMPARE_NOT_IN;
    std::vector<T> set;
    for (const auto& child : children | std::views::drop(1)) {
      if (child->GetExpressionType() !=
          duckdb::ExpressionType::VALUE_CONSTANT) {
        return false;
      }
      const auto& value =
        child->Cast<duckdb::BoundConstantExpression>().GetValue();
      if (value.IsNull()) {
        if (excluded) {
          return false;
        }
        continue;
      }
      auto constant = Constant<T>(value);
      if (!constant) {
        return false;
      }
      set.emplace_back(std::move(*constant));
    }
    if (excluded) {
      condition.excluded.insert(condition.excluded.end(),
                                std::make_move_iterator(set.begin()),
                                std::make_move_iterator(set.end()));
    } else {
      Intersect(condition.keys, std::move(set));
    }
    return true;
  }
  if (duckdb::BoundComparisonExpression::IsComparison(expr)) {
    auto type = duckdb::ExpressionType::INVALID;
    const auto constant =
      duckdb::ExpressionFilter::TryGetColumnConstantComparison(
        expr.Cast<duckdb::BoundFunctionExpression>(), type);
    if (!constant) {
      return false;
    }
    auto value = Constant<T>(*constant);
    return value && Restrict(condition, type, std::move(*value));
  }
  if (expr.GetExpressionClass() == BOUND_CONJUNCTION &&
      expr.GetExpressionType() == duckdb::ExpressionType::CONJUNCTION_AND) {
    return absl::c_all_of(
      expr.Cast<duckdb::BoundConjunctionExpression>().GetChildren(),
      [&](const duckdb::unique_ptr<duckdb::Expression>& child) {
        return Capture(*child, condition);
      });
  }
  if (expr.GetExpressionClass() == BOUND_CONJUNCTION &&
      expr.GetExpressionType() == duckdb::ExpressionType::CONJUNCTION_OR) {
    std::vector<T> set;
    for (const auto& child :
         expr.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
      SystemCondition<T> branch;
      if (!Capture(*child, branch) || !branch.keys ||
          !branch.excluded.empty() || branch.lower || branch.upper ||
          branch.null) {
        return false;
      }
      set.insert(set.end(), branch.keys->begin(), branch.keys->end());
    }
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
  if (expr.GetExpressionType() == duckdb::ExpressionType::OPERATOR_NOT) {
    const auto& child =
      *expr.Cast<duckdb::BoundOperatorExpression>().GetChildren()[0];
    bool nulls = false;
    duckdb::ExpressionIterator::VisitExpression<
      duckdb::BoundConstantExpression>(
      child, [&](const duckdb::BoundConstantExpression& constant) {
        nulls = nulls || constant.GetValue().IsNull();
      });
    SystemCondition<T> negated;
    if (nulls || !Capture(child, negated) || !negated.keys ||
        !negated.excluded.empty() || negated.lower || negated.upper ||
        negated.null || negated.not_null) {
      return false;
    }
    condition.excluded.insert(condition.excluded.end(),
                              std::make_move_iterator(negated.keys->begin()),
                              std::make_move_iterator(negated.keys->end()));
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
               .excluded = {},
               .lower = {},
               .upper = {},
               .null = condition.null,
               .not_null = condition.not_null};
}

enum class SystemClaim : uint8_t {
  None,
  Filter,
  Visibility,
};

SystemClaim Claim(const SystemTable& table, duckdb::column_t column,
                  const duckdb::Expression& expr, SystemFilter& compiled) {
  if (column >= table.Columns().LogicalColumnCount()) {
    return SystemClaim::None;
  }
  if (const auto& visibility = table.Visibility();
      visibility && visibility->column == column &&
      IsVisibility(expr, *visibility)) {
    return SystemClaim::Visibility;
  }
  const auto& type =
    table.Columns().GetColumn(duckdb::LogicalIndex{column}).Type();
  if (type.id() == duckdb::LogicalTypeId::VARCHAR) {
    return Capture(expr, compiled.texts) ? SystemClaim::Filter
                                         : SystemClaim::None;
  }
  return IsNumber(type) && Capture(expr, compiled.numbers) ? SystemClaim::Filter
                                                           : SystemClaim::None;
}

void Finish(const SystemSqlColumn& meta, SystemFilter& compiled) {
  compiled.texts.not_null = compiled.texts.not_null && !meta.not_null;
  compiled.numbers.not_null = compiled.numbers.not_null && !meta.not_null;
  Seal(compiled.texts);
  Seal(compiled.numbers);
  if (meta.type == kChar) {
    compiled.chars[0] = compiled.texts.Passes(std::string_view{});
    for (uint32_t c = 1; c < 256; ++c) {
      const auto ch = static_cast<char>(c);
      compiled.chars[c] = compiled.texts.Passes(std::string_view{&ch, 1});
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

bool SystemScan::AllowsSchema(std::optional<uint32_t> column,
                              const duckdb::SchemaCatalogEntry& schema) const {
  if (!column) {
    return true;
  }
  if (_table.Sql().columns[*column].physical == duckdb::PhysicalType::VARCHAR) {
    return Allows(*column, std::string_view{schema.name.GetIdentifierName()});
  }
  return Allows(*column, static_cast<int64_t>(schema.oid));
}

duckdb::TableFilterPushdown SystemPushdown(const SystemTable& table,
                                           duckdb::column_t column,
                                           const duckdb::Expression& expr) {
  if (duckdb::ExpressionFilter::IsRootOptionalExpression(expr)) {
    return duckdb::TableFilterPushdown::BeforeLimit;
  }
  SystemFilter compiled;
  return Claim(table, column, expr, compiled) != SystemClaim::None
           ? duckdb::TableFilterPushdown::BeforeLimit
           : duckdb::TableFilterPushdown::Reject;
}

void SystemScan::Compile(duckdb::column_t column,
                         const duckdb::TableFilter& filter) {
  const auto& expr =
    *duckdb::ExpressionFilter::GetExpressionFilter(filter, "SystemScan").expr;
  SystemFilter compiled;
  const auto claim = Claim(_table, column, expr, compiled);
  if (claim == SystemClaim::Visibility) {
    _visible_only = true;
    return;
  }
  const bool optional =
    duckdb::ExpressionFilter::IsRootOptionalExpression(expr);
  SDB_ASSERT(claim == SystemClaim::Filter || optional);
  if (column >= _table.Columns().LogicalColumnCount()) {
    return;
  }
  const auto& meta = _table.Sql().columns[column];
  Finish(meta, compiled);
  if (const auto& type =
        _table.Columns().GetColumn(duckdb::LogicalIndex{column}).Type();
      optional && !compiled.numbers.keys && type.IsIntegral()) {
    _pruned |= uint64_t{1} << column;
    _prunes.emplace_back(SystemPrune{
      .column = static_cast<uint32_t>(column),
      .filter = &filter,
      .state = duckdb::TableFilterState::Initialize(_context, filter),
      .stats = duckdb::BaseStatistics::FromConstant(
                 duckdb::Value::BIGINT(0).DefaultCastAs(type))
                 .ToUnique()});
  }
  if (compiled.texts.Empty() && compiled.numbers.Empty()) {
    return;
  }
  const auto& fallback = meta.default_value;
  const bool passes =
    !compiled.texts.Empty()
      ? (fallback ? compiled.texts.Passes(*fallback)
                  : compiled.texts.PassesNull())
      : (fallback ? compiled.numbers.Passes(SystemNumber(*fallback))
                  : compiled.numbers.PassesNull());
  _filtered |= uint64_t{1} << column;
  if (!passes) {
    _rejecting |= uint64_t{1} << column;
  }
  _filter_index[column] = static_cast<uint8_t>(_filters.size());
  _filters.emplace_back(std::move(compiled));
}

}  // namespace sdb::pg
