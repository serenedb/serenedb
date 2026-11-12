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

#include "connector/curve_filter_builder.h"

#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression_binder.hpp>
#include <iresearch/search/filters/curve_box_filter.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "connector/curve_index.h"
#include "connector/functions/search.h"
#include "connector/functions/ts_common.hpp"
#include "connector/functions/ts_query_codec.h"
#include "connector/inet_filter_builder.hpp"
#include "pg/types/logical.h"

namespace sdb::connector {
namespace {

bool IsWideningCast(const duckdb::Expression& expression) {
  if (!duckdb::BoundCastExpression::IsCast(expression)) {
    return false;
  }
  const auto& source = duckdb::BoundCastExpression::Child(
                         expression.Cast<duckdb::BoundFunctionExpression>())
                         .GetReturnType();
  const auto& target = expression.GetReturnType();
  if (source.id() == duckdb::LogicalTypeId::FLOAT) {
    return target.id() == duckdb::LogicalTypeId::DOUBLE;
  }
  return source.IsIntegral() && target.IsIntegral() &&
         source.IsSigned() == target.IsSigned() &&
         duckdb::GetTypeIdSize(source.InternalType()) <
           duckdb::GetTypeIdSize(target.InternalType());
}

void AddBoxCandidates(irs::BooleanFilter& root, const SearchColumnInfo& info,
                      const irs::curve::Box& box) {
  auto filter = std::make_unique<irs::ByCurveBox>();
  *filter->mutable_field_id() = info.field_id;
  filter->SetScorer(&irs::ForceConstScore());
  auto& options = *filter->mutable_options();
  options.box = box;
  options.curve =
    irs::utils::downCast<CurveTokenizer>(*info.tokenizer.analyzer).Options();
  root.Add(std::move(filter), irs::Occur::Must);
}

void RaiseMin(uint64_t& min, CurveKey bound, bool inclusive, bool& empty) {
  if (!inclusive && bound.exact) {
    if (bound.key > std::numeric_limits<uint64_t>::max() - bound.step) {
      empty = true;
      return;
    }
    bound.key += bound.step;
  }
  min = std::max(min, bound.key);
}

void LowerMax(uint64_t& max, CurveKey bound, bool inclusive, bool& empty) {
  if (!inclusive && bound.exact) {
    if (bound.key < bound.step) {
      empty = true;
      return;
    }
    bound.key -= bound.step;
  }
  max = std::min(max, bound.key);
}

[[noreturn]] void ThrowUnsupportedQuery() {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
    ERR_MSG("curve() @@ supports only ts_between(ROW(...), ROW(...), "
            "lower_inclusive, upper_inclusive) with constant arguments"));
}

const duckdb::Value* Component(const duckdb::Value& bounds, uint32_t axis,
                               uint32_t dimensions) {
  if (bounds.IsNull()) {
    return nullptr;
  }
  if (!duckdb::StructType::IsStruct(bounds.type()) ||
      pg::IsInet(bounds.type()) ||
      duckdb::StructType::GetChildCount(bounds.type()) != dimensions) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
      ERR_MSG("ts_between bounds of a curve() must be ROWs of ", dimensions,
              " values, got ", bounds.type().ToString()));
  }
  const auto& component = duckdb::StructValue::GetChildren(bounds)[axis];
  return component.IsNull() ? nullptr : &component;
}

CurveKey QueryKey(duckdb::ClientContext& context, const CurveAxis& axis,
                  const duckdb::Value& bound, uint32_t index) {
  auto key = CurveBoundKey(axis, bound);
  if (key && axis.family == CurveFamily::Float) {
    const auto common = duckdb::BoundComparisonExpression::BindComparison(
      context, axis.type, bound.type(), duckdb::ExpressionType::COMPARE_EQUAL);
    if (common == axis.type) {
      key = CurveBoundKey(axis, bound.DefaultCastAs(common));
    }
  }
  if (!key) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
      ERR_MSG("ts_between: bound ", bound.type().ToString(),
              " does not match coordinate ", index + 1, " of type ",
              axis.type.ToString(), "; cast the bound explicitly"));
  }
  return *key;
}

duckdb::unique_ptr<duckdb::Expression> Compare(
  duckdb::ClientContext& context, duckdb::ExpressionType type,
  const duckdb::Expression& coordinate, const duckdb::Value& bound) {
  const auto common = duckdb::BoundComparisonExpression::BindComparison(
    context, coordinate.GetReturnType(), bound.type(), type);
  auto left = duckdb::BoundCastExpression::AddCastToType(
    context, coordinate.Copy(), common);
  auto right = duckdb::BoundCastExpression::AddCastToType(
    context, duckdb::make_uniq<duckdb::BoundConstantExpression>(bound), common);
  const auto collation =
    duckdb::ExpressionBinder::ComparisonCollationType(type);
  duckdb::ExpressionBinder::PushCollation(context, left, common, collation);
  duckdb::ExpressionBinder::PushCollation(context, right, common, collation);
  return duckdb::BoundComparisonExpression::Create(type, std::move(left),
                                                   std::move(right));
}

duckdb::unique_ptr<duckdb::Expression> IsNotNull(
  const duckdb::Expression& coordinate) {
  auto check = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
    duckdb::ExpressionType::OPERATOR_IS_NOT_NULL, duckdb::LogicalType::BOOLEAN);
  check->GetChildrenMutable().emplace_back(coordinate.Copy());
  return check;
}

void CollectRanges(const duckdb::Expression& expression,
                   absl::FunctionRef<duckdb::unique_ptr<duckdb::Expression>(
                     const duckdb::Expression&)>
                     normalize,
                   size_t conjunct, std::vector<CurveRange>& ranges) {
  using enum duckdb::ExpressionType;
  if (expression.GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_FUNCTION ||
      expression.HasParameter()) {
    return;
  }
  const auto add =
    [&](const duckdb::Expression& component, const duckdb::Expression* lower,
        const duckdb::Expression* upper, bool lower_inclusive = true,
        bool upper_inclusive = true) {
      if (auto normalized = normalize(PeelInetSortKey(component))) {
        ranges.emplace_back(std::move(normalized), lower, upper,
                            lower_inclusive, upper_inclusive, conjunct);
      }
    };
  const auto& function = expression.Cast<duckdb::BoundFunctionExpression>();
  if (expression.GetExpressionType() == COMPARE_BETWEEN) {
    add(duckdb::BoundBetweenExpression::Input(function),
        &duckdb::BoundBetweenExpression::LowerBound(function),
        &duckdb::BoundBetweenExpression::UpperBound(function),
        duckdb::BoundBetweenExpression::LowerInclusive(function),
        duckdb::BoundBetweenExpression::UpperInclusive(function));
    return;
  }
  if (!duckdb::BoundComparisonExpression::IsComparison(expression)) {
    return;
  }
  const auto& left = duckdb::BoundComparisonExpression::Left(function);
  const auto& right = duckdb::BoundComparisonExpression::Right(function);
  switch (expression.GetExpressionType()) {
    case COMPARE_EQUAL:
      add(left, &right, &right);
      add(right, &left, &left);
      break;
    case COMPARE_GREATERTHAN:
    case COMPARE_GREATERTHANOREQUALTO:
      add(left, &right, nullptr,
          expression.GetExpressionType() == COMPARE_GREATERTHANOREQUALTO);
      add(right, nullptr, &left, true,
          expression.GetExpressionType() == COMPARE_GREATERTHANOREQUALTO);
      break;
    case COMPARE_LESSTHAN:
    case COMPARE_LESSTHANOREQUALTO:
      add(left, nullptr, &right, true,
          expression.GetExpressionType() == COMPARE_LESSTHANOREQUALTO);
      add(right, &left, nullptr,
          expression.GetExpressionType() == COMPARE_LESSTHANOREQUALTO);
      break;
    default:
      break;
  }
}

}  // namespace

bool ClaimCurveMatch(
  duckdb::ClientContext& context, irs::BooleanFilter& root,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters, size_t index,
  const ExpressionGetter& expressions, CurveChecks checks) {
  const auto& conjunct = *filters[index];
  if (conjunct.GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_FUNCTION ||
      conjunct.HasParameter()) {
    return false;
  }
  const auto& match = conjunct.Cast<duckdb::BoundFunctionExpression>();
  if (match.Function().GetName().GetIdentifierName() != kTSQueryMatch) {
    return false;
  }
  const auto& curve = *match.GetChildren()[0];
  const auto* spec = TryGetCurveSpec(curve);
  if (!spec) {
    return false;
  }
  const auto info = expressions(curve);
  if (!info) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("curve() @@ needs an index on the same curve() call"),
      ERR_HINT("Query the index relation and repeat the indexed call with the "
               "same coordinates and options."));
  }
  const auto* query = &UnwrapTSQueryCast(*match.GetChildren()[1]);
  duckdb::unique_ptr<duckdb::Expression> parsed;
  if (query->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONSTANT) {
    const auto parts = TryGetTSQueryParts(
      query->Cast<duckdb::BoundConstantExpression>().GetValue());
    if (parts && !HasModifiers(*parts)) {
      parsed = TryParseStructuredTSQueryText(parts->text, context);
    }
    if (!parsed) {
      ThrowUnsupportedQuery();
    }
    query = &UnwrapTSQueryCast(*parsed);
  }
  if (query->GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
    ThrowUnsupportedQuery();
  }
  const auto& between = query->Cast<duckdb::BoundFunctionExpression>();
  const auto& args = between.GetChildren();
  if (between.Function().GetName().GetIdentifierName() != kTSQBetween) {
    ThrowUnsupportedQuery();
  }
  const auto lower = TryEvaluateScalar(context, *args[0]);
  const auto upper = TryEvaluateScalar(context, *args[1]);
  const auto lower_inclusive = TryEvaluateScalar(context, *args[2]);
  const auto upper_inclusive = TryEvaluateScalar(context, *args[3]);
  if (!lower || !upper || !lower_inclusive || !upper_inclusive ||
      lower_inclusive->IsNull() || upper_inclusive->IsNull()) {
    ThrowUnsupportedQuery();
  }
  const auto include_lower = lower_inclusive->GetValue<bool>();
  const auto include_upper = upper_inclusive->GetValue<bool>();

  const auto dimensions = spec->options.dimensions;
  const auto coordinates = CurveCoordinates(curve);
  if (coordinates.size() != dimensions) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("curve() @@ needs the point written as ROW(...) "
                            "of its coordinates"));
  }
  irs::curve::Box box;
  bool empty = false;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> rechecks;
  for (uint32_t axis = 0; axis < dimensions; ++axis) {
    const auto& target = spec->axes[axis];
    const auto& coordinate = *coordinates[axis];
    box.max[axis] = std::numeric_limits<uint64_t>::max();
    const auto* lo = Component(*lower, axis, dimensions);
    const auto* hi = Component(*upper, axis, dimensions);
    const bool lossless = checks == CurveChecks::Lossy &&
                          CurveAxisIsExact(target, spec->options.max_level);
    if (target.family == CurveFamily::Null) {
      empty = true;
    }
    if (lo) {
      bool exact = false;
      if (target.family != CurveFamily::Null) {
        const auto bound = QueryKey(context, target, *lo, axis);
        exact = lossless && bound.exact;
        if (!empty) {
          RaiseMin(box.min[axis], bound, include_lower, empty);
        }
      }
      if (!exact) {
        rechecks.emplace_back(Compare(
          context,
          include_lower ? duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO
                        : duckdb::ExpressionType::COMPARE_GREATERTHAN,
          coordinate, *lo));
      }
    }
    if (hi) {
      bool exact = false;
      if (target.family != CurveFamily::Null) {
        const auto bound = QueryKey(context, target, *hi, axis);
        exact = lossless && bound.exact;
        if (!empty) {
          LowerMax(box.max[axis], bound, include_upper, empty);
        }
      }
      if (!exact) {
        rechecks.emplace_back(Compare(
          context,
          include_upper ? duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO
                        : duckdb::ExpressionType::COMPARE_LESSTHAN,
          coordinate, *hi));
      }
    }
    if (checks == CurveChecks::All && !lo && !hi) {
      rechecks.emplace_back(IsNotNull(coordinate));
    }
    box.max[axis] |= CurveKeyPadding(target);
  }
  if (empty) {
    root.Add(std::make_unique<irs::Empty>(), irs::Occur::Must);
  } else {
    AddBoxCandidates(root, *info, box);
  }
  if (rechecks.empty()) {
    std::swap(filters[index], filters.back());
    filters.pop_back();
    return true;
  }
  filters[index] = std::move(rechecks.front());
  for (size_t i = 1; i < rechecks.size(); ++i) {
    filters.emplace_back(std::move(rechecks[i]));
  }
  return true;
}

std::vector<CurveRange> CollectCurveRanges(
  std::span<const duckdb::unique_ptr<duckdb::Expression>> conjuncts,
  absl::FunctionRef<
    duckdb::unique_ptr<duckdb::Expression>(const duckdb::Expression&)>
    normalize) {
  std::vector<CurveRange> ranges;
  for (size_t i = 0; i < conjuncts.size(); ++i) {
    CollectRanges(*conjuncts[i], normalize, i, ranges);
  }
  return ranges;
}

bool AddCurveRangeCandidates(duckdb::ClientContext& context,
                             irs::BooleanFilter& root,
                             std::span<const CurveRange> ranges,
                             const duckdb::Expression& key,
                             const SearchColumnInfo& info,
                             std::vector<size_t>& answered) {
  const auto* spec = TryGetCurveSpec(key);
  SDB_ASSERT(spec);
  const auto coordinates = CurveCoordinates(key);
  if (coordinates.size() != spec->options.dimensions) {
    return false;
  }
  irs::curve::Box box;
  bool empty = false;
  std::vector<size_t> exact;
  for (uint32_t axis = 0; axis < spec->options.dimensions; ++axis) {
    const auto& target = spec->axes[axis];
    bool has_lower = false;
    bool has_upper = false;
    box.max[axis] = std::numeric_limits<uint64_t>::max();
    const bool lossless = CurveAxisIsExact(target, spec->options.max_level);
    const auto& indexed = *coordinates[axis];
    for (const auto& range : ranges) {
      const auto* component = range.component.get();
      bool matched = component->Equals(indexed);
      while (!matched && IsWideningCast(*component)) {
        component = &duckdb::BoundCastExpression::Child(
          component->Cast<duckdb::BoundFunctionExpression>());
        matched = component->Equals(indexed);
      }
      if (!matched) {
        continue;
      }
      std::optional<CurveKey> lower;
      std::optional<CurveKey> upper;
      if (range.lower) {
        const auto value = TryEvaluateScalar(context, *range.lower);
        if (!value || !(lower = CurveBoundKey(target, *value))) {
          continue;
        }
      }
      if (range.upper) {
        const auto value = TryEvaluateScalar(context, *range.upper);
        if (!value || !(upper = CurveBoundKey(target, *value))) {
          continue;
        }
      }
      if (lower) {
        RaiseMin(box.min[axis], *lower, range.lower_inclusive, empty);
        has_lower = true;
      }
      if (upper) {
        LowerMax(box.max[axis], *upper, range.upper_inclusive, empty);
        has_upper = true;
      }
      if (lossless && (!lower || lower->exact) && (!upper || upper->exact)) {
        exact.emplace_back(range.conjunct);
      }
    }
    if (!has_lower || !has_upper) {
      return false;
    }
    box.max[axis] |= CurveKeyPadding(target);
  }
  if (empty) {
    root.Add(std::make_unique<irs::Empty>(), irs::Occur::Must);
    return true;
  }
  AddBoxCandidates(root, info, box);
  answered.insert(answered.end(), exact.begin(), exact.end());
  return true;
}

}  // namespace sdb::connector
