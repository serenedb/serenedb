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

#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/utils/down_cast.hpp>

#include "connector/curve_index.h"
#include "connector/geo_validate.h"

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

bool Fold(duckdb::ClientContext& context, const duckdb::Expression& expression,
          duckdb::Value& value) {
  if (!expression.IsFoldable() || expression.HasParameter()) {
    return false;
  }
  auto copy = expression.Copy();
  return duckdb::ExpressionExecutor::TryEvaluateScalar(context, *copy, value) &&
         !value.IsNull();
}

bool IsClosedBox(const duckdb::Value& lower, const duckdb::Value& upper) {
  for (const auto* bound : {&lower, &upper}) {
    for (const auto& component : duckdb::StructValue::GetChildren(*bound)) {
      if (component.IsNull()) {
        return false;
      }
    }
  }
  return true;
}

}  // namespace

bool AddCurveCandidates(irs::BooleanFilter& root,
                        const duckdb::Expression& expression,
                        const ColumnGetter& columns,
                        const ExpressionGetter& expressions,
                        duckdb::ClientContext& context) {
  if (expression.GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_FUNCTION ||
      expression.HasParameter()) {
    return false;
  }
  const auto& function = expression.Cast<duckdb::BoundFunctionExpression>();
  const auto& name = function.Function().GetName();
  const bool point = name == "sdb_box_contains";
  const bool geometry = name == "st_intersects" || name == "st_contains" ||
                        name == "st_within" || name == "st_covers" ||
                        name == "st_coveredby" || name == "st_touches" ||
                        name == "st_crosses" || name == "st_overlaps";
  const auto& args = function.GetChildren();
  if ((!point && !geometry) || args.size() != (point ? 3 : 2)) {
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
        result->tokenizer.analyzer->type() != irs::Type<CurveTokenizer>::id()) {
      return std::nullopt;
    }
    return result;
  };
  size_t field = 0;
  auto info = find(*args[field]);
  if (!info && geometry) {
    field = 1;
    info = find(*args[field]);
  }
  if (!info) {
    return false;
  }
  const auto& options =
    irs::utils::downCast<CurveTokenizer>(*info->tokenizer.analyzer).Options();
  if (options.cartesian != geometry) {
    return false;
  }
  std::vector<irs::curve::Cell> cells;
  irs::curve::Box box;
  if (point) {
    duckdb::Value lower, upper;
    if (!Fold(context, *args[1], lower) || !Fold(context, *args[2], upper)) {
      return false;
    }
    ValidateCurveBounds(info->logical_type, lower.type(), upper.type());
    if (!IsClosedBox(lower, upper)) {
      return false;
    }
    box = CurveBox(info->logical_type, lower, upper);
    cells = irs::curve::CoverBox(box, options);
  } else {
    duckdb::Value shape;
    if (!Fold(context, *args[1 - field], shape) ||
        shape.type().id() != duckdb::LogicalTypeId::GEOMETRY) {
      return false;
    }
    ValidateGeometryCartesian(info->logical_type, "Cartesian index");
    if (duckdb::GeoType::HasCRS(shape.type())) {
      ValidateGeometryCartesian(shape.type(), "Cartesian query");
    }
    cells = CoverCartesian(duckdb::StringValue::Get(shape), options);
    if (cells.empty()) {
      return false;
    }
  }
  if (cells.empty()) {
    root.Add(std::make_unique<irs::Empty>(), irs::Occur::Must);
    return true;
  }
  auto terms = point ? irs::curve::PointQueryTerms(cells, box, options)
                     : irs::curve::Terms(cells, options, true);
  auto candidates = std::make_unique<irs::BooleanFilter>();
  candidates->SetScorer(&irs::ForceConstScore());
  for (const auto& term : terms) {
    AddTerm({candidates.get(), irs::Occur::Should}, info->field_id,
            irs::ViewCast<irs::byte_type>(std::string_view{term}),
            irs::kNoBoost, &irs::ForceConstScore());
  }
  SetMinMatch(*candidates, 1);
  root.Add(std::move(candidates), irs::Occur::Must);
  return true;
}

}  // namespace sdb::connector
