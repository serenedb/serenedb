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

#include <absl/algorithm/container.h>

#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "connector/curve_index.h"
#include "connector/functions/search.h"
#include "connector/functions/ts_query_codec.h"
#include "connector/geo_validate.h"
#include "connector/pg_logical_types.h"

namespace sdb::connector {
namespace {

const duckdb::Expression& PeelCast(const duckdb::Expression& expr) {
  if (duckdb::BoundCastExpression::IsCast(expr)) {
    const auto& child = duckdb::BoundCastExpression::Child(
      expr.Cast<duckdb::BoundFunctionExpression>());
    if (child.GetReturnType() == expr.GetReturnType() ||
        (child.GetReturnType().id() == duckdb::LogicalTypeId::GEOMETRY &&
         expr.GetReturnType().id() == duckdb::LogicalTypeId::GEOMETRY)) {
      return PeelCast(child);
    }
  }
  return expr;
}

const duckdb::Expression& PeelCasts(const duckdb::Expression& expr) {
  const auto* result = &expr;
  while (duckdb::BoundCastExpression::IsCast(*result)) {
    result = &duckdb::BoundCastExpression::Child(
      result->Cast<duckdb::BoundFunctionExpression>());
  }
  return *result;
}

uint32_t IntegerWidth(duckdb::LogicalTypeId type) {
  using enum duckdb::LogicalTypeId;
  switch (type) {
    case TINYINT:
    case UTINYINT:
      return 8;
    case SMALLINT:
    case USMALLINT:
      return 16;
    case INTEGER:
    case UINTEGER:
      return 32;
    case BIGINT:
    case UBIGINT:
      return 64;
    default:
      return 0;
  }
}

bool IsUnsigned(duckdb::LogicalTypeId type) {
  using enum duckdb::LogicalTypeId;
  return type == UTINYINT || type == USMALLINT || type == UINTEGER ||
         type == UBIGINT;
}

bool IsWideningCast(const duckdb::Expression& expression) {
  if (!duckdb::BoundCastExpression::IsCast(expression)) {
    return false;
  }
  const auto& child = duckdb::BoundCastExpression::Child(
    expression.Cast<duckdb::BoundFunctionExpression>());
  const auto source = child.GetReturnType().id();
  const auto target = expression.GetReturnType().id();
  return (source == duckdb::LogicalTypeId::FLOAT &&
          target == duckdb::LogicalTypeId::DOUBLE) ||
         (IntegerWidth(source) != 0 &&
          IntegerWidth(source) < IntegerWidth(target) &&
          IsUnsigned(source) == IsUnsigned(target));
}

std::optional<duckdb::Value> Fold(duckdb::ClientContext& context,
                                  const duckdb::Expression& expression) {
  if (!expression.IsFoldable() || expression.HasParameter()) {
    return std::nullopt;
  }
  auto copy = expression.Copy();
  duckdb::Value value;
  if (!duckdb::ExpressionExecutor::TryEvaluateScalar(context, *copy, value)) {
    return std::nullopt;
  }
  return value;
}

void AddTermCandidates(irs::BooleanFilter& root, irs::field_id field_id,
                       std::span<const std::string> terms) {
  auto candidates = std::make_unique<irs::BooleanFilter>();
  candidates->SetScorer(&irs::ForceConstScore());
  for (const auto& term : terms) {
    AddTerm({candidates.get(), irs::Occur::Should}, field_id,
            irs::ViewCast<irs::byte_type>(std::string_view{term}),
            irs::kNoBoost, &irs::ForceConstScore());
  }
  SetMinMatch(*candidates, 1);
  root.Add(std::move(candidates), irs::Occur::Must);
}

void AddBoxCandidates(irs::BooleanFilter& root, const SearchColumnInfo& info,
                      const irs::curve::Box& box) {
  const auto& options =
    irs::utils::downCast<CurveTokenizer>(*info.tokenizer.analyzer).Options();
  const auto cells = irs::curve::CoverBox(box, options);
  if (cells.empty()) {
    root.Add(std::make_unique<irs::Empty>(), irs::Occur::Must);
    return;
  }
  AddTermCandidates(root, info.field_id,
                    irs::curve::PointQueryTerms(cells, box, options));
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
  if (key && axis.type.id() == duckdb::LogicalTypeId::FLOAT) {
    const auto common = duckdb::BoundComparisonExpression::BindComparison(
      context, axis.type, bound.type(), duckdb::ExpressionType::COMPARE_EQUAL);
    if (common.id() == duckdb::LogicalTypeId::FLOAT) {
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
  const bool inet = pg::IsInet(coordinate.GetReturnType());
  const auto comparison =
    inet ? duckdb::NegateComparisonExpression(type) : type;
  const auto common = duckdb::BoundComparisonExpression::BindComparison(
    context, coordinate.GetReturnType(), bound.type(), comparison);
  auto result = duckdb::BoundComparisonExpression::Create(
    comparison,
    duckdb::BoundCastExpression::AddCastToType(context, coordinate.Copy(),
                                               common),
    duckdb::BoundCastExpression::AddCastToType(
      context, duckdb::make_uniq<duckdb::BoundConstantExpression>(bound),
      common));
  if (!inet) {
    return result;
  }
  auto negation = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
    duckdb::ExpressionType::OPERATOR_NOT, duckdb::LogicalType::BOOLEAN);
  negation->GetChildrenMutable().push_back(std::move(result));
  return negation;
}

duckdb::unique_ptr<duckdb::Expression> IsNotNull(
  const duckdb::Expression& coordinate) {
  auto check = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
    duckdb::ExpressionType::OPERATOR_IS_NOT_NULL, duckdb::LogicalType::BOOLEAN);
  check->GetChildrenMutable().push_back(coordinate.Copy());
  return check;
}

void CollectRanges(const duckdb::Expression& expression,
                   absl::FunctionRef<duckdb::unique_ptr<duckdb::Expression>(
                     const duckdb::Expression&)>
                     normalize,
                   std::vector<CurveRange>& ranges) {
  using enum duckdb::ExpressionType;
  if (expression.GetExpressionType() == CONJUNCTION_AND) {
    for (const auto& child :
         expression.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
      CollectRanges(*child, normalize, ranges);
    }
    return;
  }
  if (expression.GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_FUNCTION ||
      expression.HasParameter()) {
    return;
  }
  const auto add =
    [&](const duckdb::Expression& component, const duckdb::Expression* lower,
        const duckdb::Expression* upper, bool lower_inclusive = true,
        bool upper_inclusive = true) {
      if (auto normalized = normalize(PeelCast(component))) {
        ranges.push_back({std::move(normalized), lower, upper, lower_inclusive,
                          upper_inclusive});
      }
    };
  const auto& function = expression.Cast<duckdb::BoundFunctionExpression>();
  if (expression.GetExpressionType() == COMPARE_BETWEEN) {
    if (duckdb::BoundBetweenExpression::HasValidBindData(function)) {
      add(duckdb::BoundBetweenExpression::Input(function),
          &duckdb::BoundBetweenExpression::LowerBound(function),
          &duckdb::BoundBetweenExpression::UpperBound(function),
          duckdb::BoundBetweenExpression::LowerInclusive(function),
          duckdb::BoundBetweenExpression::UpperInclusive(function));
    }
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
  irs::BooleanFilter& root,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters, size_t index,
  const ExpressionGetter& expressions, duckdb::ClientContext& context) {
  const auto& conjunct = *filters[index];
  if (conjunct.GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_FUNCTION ||
      conjunct.HasParameter()) {
    return false;
  }
  const auto& match = conjunct.Cast<duckdb::BoundFunctionExpression>();
  if (match.Function().GetName().GetIdentifierName() != kTSQueryMatch ||
      match.GetChildren().size() != 2) {
    return false;
  }
  const auto& curve = *match.GetChildren()[0];
  const auto* spec = TryGetCurveSpec(curve);
  if (!spec) {
    return false;
  }
  const auto info = expressions(curve);
  if (!info || !info->tokenizer.analyzer ||
      info->tokenizer.analyzer->type() != irs::Type<CurveTokenizer>::id()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("curve() @@ needs an index on the same curve() call"),
      ERR_HINT("Query the index relation and repeat the indexed call with the "
               "same coordinates and options."));
  }
  const auto* query = &PeelCasts(*match.GetChildren()[1]);
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
    query = &PeelCasts(*parsed);
  }
  if (query->GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
    ThrowUnsupportedQuery();
  }
  const auto& between = query->Cast<duckdb::BoundFunctionExpression>();
  const auto& args = between.GetChildren();
  if (between.Function().GetName().GetIdentifierName() != kTSQBetween ||
      args.size() != 4) {
    ThrowUnsupportedQuery();
  }
  const auto lower = Fold(context, *args[0]);
  const auto upper = Fold(context, *args[1]);
  const auto lower_inclusive = Fold(context, *args[2]);
  const auto upper_inclusive = Fold(context, *args[3]);
  if (!lower || !upper || !lower_inclusive || !upper_inclusive ||
      lower_inclusive->IsNull() || upper_inclusive->IsNull()) {
    ThrowUnsupportedQuery();
  }
  const auto include_lower = lower_inclusive->GetValue<bool>();
  const auto include_upper = upper_inclusive->GetValue<bool>();

  const auto dimensions = spec->options.dimensions;
  const auto& coordinates =
    curve.Cast<duckdb::BoundFunctionExpression>().GetChildren();
  irs::curve::Box box;
  bool empty = false;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> rechecks;
  for (uint32_t axis = 0; axis < dimensions; ++axis) {
    const auto& target = spec->axes[axis];
    const auto& coordinate = *coordinates[axis];
    const auto limits = CurveLimits(target);
    box.min[axis] = limits.min;
    box.max[axis] = limits.max;
    const auto* lo = Component(*lower, axis, dimensions);
    const auto* hi = Component(*upper, axis, dimensions);
    if (target.family == CurveFamily::Null) {
      empty = true;
    }
    if (lo) {
      if (target.family != CurveFamily::Null) {
        const auto bound = QueryKey(context, target, *lo, axis);
        if (!empty) {
          RaiseMin(box.min[axis], bound, include_lower, empty);
        }
      }
      rechecks.push_back(Compare(
        context,
        include_lower ? duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO
                      : duckdb::ExpressionType::COMPARE_GREATERTHAN,
        coordinate, *lo));
    }
    if (hi) {
      if (target.family != CurveFamily::Null) {
        const auto bound = QueryKey(context, target, *hi, axis);
        if (!empty) {
          LowerMax(box.max[axis], bound, include_upper, empty);
        }
      }
      rechecks.push_back(Compare(
        context,
        include_upper ? duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO
                      : duckdb::ExpressionType::COMPARE_LESSTHAN,
        coordinate, *hi));
    }
    if (!lo && !hi) {
      rechecks.push_back(IsNotNull(coordinate));
    }
  }
  if (empty) {
    root.Add(std::make_unique<irs::Empty>(), irs::Occur::Must);
  } else {
    AddBoxCandidates(root, *info, box);
  }
  filters[index] = std::move(rechecks.front());
  for (size_t i = 1; i < rechecks.size(); ++i) {
    filters.push_back(std::move(rechecks[i]));
  }
  return true;
}

std::vector<CurveRange> CollectCurveRanges(
  std::span<const duckdb::unique_ptr<duckdb::Expression>> conjuncts,
  absl::FunctionRef<
    duckdb::unique_ptr<duckdb::Expression>(const duckdb::Expression&)>
    normalize) {
  std::vector<CurveRange> ranges;
  for (const auto& conjunct : conjuncts) {
    CollectRanges(*conjunct, normalize, ranges);
  }
  return ranges;
}

bool AddCurveRangeCandidates(irs::BooleanFilter& root,
                             std::span<const CurveRange> ranges,
                             const duckdb::Expression& key,
                             const SearchColumnInfo& info,
                             duckdb::ClientContext& context) {
  const auto* spec = TryGetCurveSpec(key);
  if (!spec) {
    return false;
  }
  const auto& coordinates =
    key.Cast<duckdb::BoundFunctionExpression>().GetChildren();
  irs::curve::Box box;
  bool empty = false;
  for (uint32_t axis = 0; axis < spec->options.dimensions; ++axis) {
    const auto& target = spec->axes[axis];
    bool has_lower = false;
    bool has_upper = false;
    const auto limits = CurveLimits(target);
    box.min[axis] = limits.min;
    box.max[axis] = limits.max;
    const auto& indexed = PeelCast(*coordinates[axis]);
    for (const auto& range : ranges) {
      const auto* component = &PeelCast(*range.component);
      bool matched = component->Equals(indexed);
      while (!matched && IsWideningCast(*component)) {
        component = &PeelCast(duckdb::BoundCastExpression::Child(
          component->Cast<duckdb::BoundFunctionExpression>()));
        matched = component->Equals(indexed);
      }
      if (!matched) {
        continue;
      }
      std::optional<CurveKey> lower;
      std::optional<CurveKey> upper;
      if (range.lower) {
        const auto value = Fold(context, *range.lower);
        if (!value || value->IsNull() ||
            !(lower = CurveBoundKey(target, *value))) {
          continue;
        }
      }
      if (range.upper) {
        const auto value = Fold(context, *range.upper);
        if (!value || value->IsNull() ||
            !(upper = CurveBoundKey(target, *value))) {
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
    }
    if (!has_lower || !has_upper) {
      return false;
    }
  }
  if (empty) {
    root.Add(std::make_unique<irs::Empty>(), irs::Occur::Must);
    return true;
  }
  AddBoxCandidates(root, info, box);
  return true;
}

bool AddCartesianCandidates(irs::BooleanFilter& root,
                            const duckdb::Expression& expression,
                            const ColumnGetter& columns,
                            const ExpressionGetter& expressions,
                            duckdb::ClientContext& context) {
  constexpr std::array<std::string_view, 8> kPredicates{
    "ST_Intersects", "ST_Contains", "ST_Within",  "ST_Covers",
    "ST_CoveredBy",  "ST_Touches",  "ST_Crosses", "ST_Overlaps",
  };
  if (expression.GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_FUNCTION ||
      expression.HasParameter()) {
    return false;
  }
  const auto& function = expression.Cast<duckdb::BoundFunctionExpression>();
  const auto& args = function.GetChildren();
  const auto& name = function.Function().GetName();
  if (args.size() != 2 ||
      absl::c_none_of(kPredicates, [&](std::string_view predicate) {
        return name == predicate;
      })) {
    return false;
  }
  const auto find =
    [&](const duckdb::Expression& argument) -> std::optional<SearchColumnInfo> {
    const auto& expr = PeelCast(argument);
    auto result =
      expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF
        ? columns(expr.Cast<duckdb::BoundColumnRefExpression>())
        : expressions(expr);
    if (!result || !result->tokenizer.analyzer ||
        result->tokenizer.analyzer->type() != irs::Type<CurveTokenizer>::id() ||
        !irs::utils::downCast<CurveTokenizer>(*result->tokenizer.analyzer)
           .Options()
           .cartesian) {
      return std::nullopt;
    }
    return result;
  };
  size_t field = 0;
  auto info = find(*args[field]);
  if (!info) {
    field = 1;
    info = find(*args[field]);
  }
  if (!info) {
    return false;
  }
  const auto shape = Fold(context, *args[1 - field]);
  if (!shape || shape->IsNull() ||
      shape->type().id() != duckdb::LogicalTypeId::GEOMETRY) {
    return false;
  }
  ValidateGeometryCartesian(info->logical_type, "Cartesian index");
  if (duckdb::GeoType::HasCRS(shape->type())) {
    ValidateGeometryCartesian(shape->type(), "Cartesian query");
  }
  const auto& options =
    irs::utils::downCast<CurveTokenizer>(*info->tokenizer.analyzer).Options();
  const auto cells = CoverCartesian(duckdb::StringValue::Get(*shape), options);
  if (cells.empty()) {
    return false;
  }
  AddTermCandidates(root, info->field_id,
                    irs::curve::Terms(cells, options, true));
  return true;
}

}  // namespace sdb::connector
