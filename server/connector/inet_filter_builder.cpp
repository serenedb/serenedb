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

#include "connector/inet_filter_builder.hpp"

#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/granular_range_filter.hpp>
#include <iresearch/search/filters/prefix_filter.hpp>
#include <iresearch/search/filters/range_filter.hpp>
#include <iresearch/search/scorers/constant_score.hpp>
#include <string_view>
#include <vector>

#include "connector/functions/ts_common.hpp"
#include "connector/inet_terms.h"
#include "connector/pg_logical_types.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kSortKeyFunction = "inet_sort_key";

const duckdb::Expression& PeelCidrCast(const duckdb::Expression& expr) {
  if (!duckdb::BoundCastExpression::IsCast(expr) ||
      !pg::IsInet(expr.GetReturnType())) {
    return expr;
  }
  const auto& child = duckdb::BoundCastExpression::Child(
    expr.Cast<duckdb::BoundFunctionExpression>());
  return pg::IsCidr(child.GetReturnType()) ? child : expr;
}

const duckdb::Expression& PeelSortKey(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return expr;
  }
  const auto& func = expr.Cast<duckdb::BoundFunctionExpression>();
  if (func.Function().GetName().GetIdentifierName() != kSortKeyFunction ||
      func.GetChildren().size() != 1) {
    return PeelCidrCast(expr);
  }
  return PeelCidrCast(*func.GetChildren()[0]);
}

const SearchColumnInfo* FindInetColumn(const FilterContext& ctx,
                                       const duckdb::Expression& expr) {
  const auto& column = PeelSortKey(expr);
  if (!pg::IsInetOrCidr(column.GetReturnType())) {
    return nullptr;
  }
  const auto* info = FindColumnInfoForExpr(ctx, column);
  return info && pg::IsInetOrCidr(info->logical_type) ? info : nullptr;
}

const duckdb::Value* FindInetConstant(const duckdb::Expression& expr) {
  if (const auto* value = TryGetConstant(expr)) {
    return value;
  }
  return TryGetConstant(PeelSortKey(expr));
}

struct Operands {
  const SearchColumnInfo* column;
  const duckdb::Value* constant;
  bool swapped;
};

std::optional<Operands> ResolveOperands(const FilterContext& ctx,
                                        const duckdb::Expression& left,
                                        const duckdb::Expression& right) {
  if (const auto* column = FindInetColumn(ctx, left)) {
    if (const auto* constant = FindInetConstant(right)) {
      return Operands{column, constant, false};
    }
    return std::nullopt;
  }
  if (const auto* column = FindInetColumn(ctx, right)) {
    if (const auto* constant = FindInetConstant(left)) {
      return Operands{column, constant, true};
    }
  }
  return std::nullopt;
}

absl::Status Unsupported() {
  return absl::InvalidArgumentError(
    "INET constant can not be encoded as an index term");
}

void AddGranularRange(BoolTarget target, const SearchColumnInfo& column,
                      const inet_terms::Inet& min, irs::BoundType min_type,
                      const inet_terms::Inet& max, irs::BoundType max_type) {
  auto& range = AddFilter<irs::ByGranularRange>(target);
  *range.mutable_field_id() = column.field_id;
  range.SetScorer(&irs::ForceConstScore());
  auto& bounds = range.mutable_options()->range;
  bounds.min = inet_terms::GranularTermsOf(min);
  bounds.min_type = min_type;
  bounds.max = inet_terms::GranularTermsOf(max);
  bounds.max_type = max_type;
}

void AddByteRange(BoolTarget target, irs::field_id field, irs::bstring min,
                  irs::bstring max) {
  auto& range = AddFilter<irs::ByRange>(target);
  *range.mutable_field_id() = field;
  range.SetScorer(&irs::ForceConstScore());
  auto& bounds = range.mutable_options()->range;
  bounds.min = std::move(min);
  bounds.min_type = irs::BoundType::Inclusive;
  bounds.max = std::move(max);
  bounds.max_type = irs::BoundType::Inclusive;
}

void AddBytePrefix(BoolTarget target, irs::field_id field,
                   irs::bstring prefix) {
  auto& filter = AddFilter<irs::ByPrefix>(target);
  *filter.mutable_field_id() = field;
  filter.SetScorer(&irs::ForceConstScore());
  filter.mutable_options()->term = std::move(prefix);
}

void AddComparison(BoolTarget target, const SearchColumnInfo& column,
                   const inet_terms::Inet& value, ComparisonOp op) {
  const auto group = AddGroup(target, irs::Occur::Should);
  for (const bool v6 : {false, true}) {
    const bool below = v6 < value.v6;
    const bool above = v6 > value.v6;
    switch (op) {
      case ComparisonOp::Lt:
      case ComparisonOp::Le:
        if (below) {
          AddBytePrefix(group, column.field_id,
                        inet_terms::CoarsestLevelPrefixOf(v6));
        } else if (!above) {
          AddGranularRange(group, column, inet_terms::FamilyMin(v6),
                           irs::BoundType::Inclusive, value,
                           op == ComparisonOp::Lt ? irs::BoundType::Exclusive
                                                  : irs::BoundType::Inclusive);
        }
        break;
      case ComparisonOp::Gt:
      case ComparisonOp::Ge:
        if (above) {
          AddBytePrefix(group, column.field_id,
                        inet_terms::CoarsestLevelPrefixOf(v6));
        } else if (!below) {
          AddGranularRange(group, column, value,
                           op == ComparisonOp::Gt ? irs::BoundType::Exclusive
                                                  : irs::BoundType::Inclusive,
                           inet_terms::FamilyMax(v6),
                           irs::BoundType::Inclusive);
        }
        break;
      case ComparisonOp::None:
        break;
    }
  }
  SetMinMatch(*group.node, 1);
}

void AddContainedBy(BoolTarget target, const SearchColumnInfo& column,
                    const inet_terms::Inet& value) {
  const auto field = column.field_id;
  const auto bits = value.NetworkBits();
  const auto group = AddGroup(target, irs::Occur::Must);
  if (bits == 0) {
    AddBytePrefix(group, field, inet_terms::CoarsestLevelPrefixOf(value.v6));
    return;
  }
  const auto network = inet_terms::Network(value, bits);
  const auto broadcast = inet_terms::Broadcast(value, bits);
  uint32_t level = inet_terms::LevelCount(value.v6);
  while (level != 0 && inet_terms::LevelBits(value.v6, level) < bits) {
    --level;
  }
  if (level == 0) {
    AddByteRange(group, field, inet_terms::ExactPrefixOf(value.v6, network),
                 inet_terms::ExactUpperOf(value.v6, broadcast));
  } else if (inet_terms::LevelBits(value.v6, level) == bits) {
    AddTerm(group, field, inet_terms::LevelTermOf(value.v6, level, network),
            irs::kNoBoost, &irs::ForceConstScore());
  } else {
    AddByteRange(group, field,
                 inet_terms::LevelTermOf(value.v6, level, network),
                 inet_terms::LevelTermOf(value.v6, level, broadcast));
  }
  AddByteRange(
    {group.node, irs::Occur::MustNot}, field,
    inet_terms::MaskTermOf(value.v6, 0),
    inet_terms::MaskTermOf(value.v6, static_cast<uint8_t>(bits - 1)));
}

void AddContains(BoolTarget target, const SearchColumnInfo& column,
                 const inet_terms::Inet& value) {
  auto& filter = AddFilter<irs::ByPrefixes>(target);
  *filter.mutable_field_id() = column.field_id;
  filter.SetScorer(&irs::ForceConstScore());
  auto& prefixes = filter.mutable_options()->prefixes;
  prefixes.reserve(value.NetworkBits() + 1);
  for (uint32_t bits = 0; bits <= value.NetworkBits(); ++bits) {
    prefixes.push_back(inet_terms::ExactPrefixOf(
      value.v6, inet_terms::Network(value, bits), static_cast<uint16_t>(bits)));
  }
}

}  // namespace

std::optional<absl::Status> TryFromInetEquality(BoolTarget filter,
                                                const FilterContext& ctx,
                                                const duckdb::Expression& left,
                                                const duckdb::Expression& right,
                                                bool not_equal) {
  const auto operands = ResolveOperands(ctx, left, right);
  if (!operands) {
    return std::nullopt;
  }
  if (operands->constant->IsNull()) {
    AddFilter<irs::Empty>(filter);
    return absl::OkStatus();
  }
  const auto value = inet_terms::FromValue(*operands->constant);
  if (!value) {
    return Unsupported();
  }
  const auto target = (ctx.negated != not_equal)
                        ? NegateScoped(filter, *operands->column)
                        : filter;
  AddTerm(target, operands->column->field_id, inet_terms::ExactTermOf(*value),
          ctx.boost, &irs::ForceConstScore());
  return absl::OkStatus();
}

std::optional<absl::Status> TryFromInetComparison(
  BoolTarget filter, const FilterContext& ctx, const duckdb::Expression& left,
  const duckdb::Expression& right, ComparisonOp op) {
  const auto operands = ResolveOperands(ctx, left, right);
  if (!operands) {
    return std::nullopt;
  }
  if (operands->swapped) {
    op = SwapComparisonOp(op);
  }
  if (ctx.negated) {
    op = InvertComparisonOp(op);
  }
  if (operands->constant->IsNull()) {
    AddFilter<irs::Empty>(filter);
    return absl::OkStatus();
  }
  const auto value = inet_terms::FromValue(*operands->constant);
  if (!value) {
    return Unsupported();
  }
  AddComparison(filter, *operands->column, *value, op);
  return absl::OkStatus();
}

std::optional<absl::Status> TryFromInetIn(
  BoolTarget filter, const FilterContext& ctx,
  const duckdb::BoundOperatorExpression& op_expr) {
  const auto& children = op_expr.GetChildren();
  const auto* column = FindInetColumn(ctx, *children[0]);
  if (!column) {
    return std::nullopt;
  }
  std::vector<irs::bstring> terms;
  terms.reserve(children.size() - 1);
  bool has_null = false;
  for (size_t i = 1; i < children.size(); ++i) {
    const auto* constant = FindInetConstant(*children[i]);
    if (!constant) {
      return absl::InvalidArgumentError(
        "Failed to evaluate IN value as constant");
    }
    if (constant->IsNull()) {
      has_null = true;
      continue;
    }
    const auto value = inet_terms::FromValue(*constant);
    if (!value) {
      return Unsupported();
    }
    terms.push_back(inet_terms::ExactTermOf(*value));
  }
  if ((ctx.negated && has_null) || terms.empty()) {
    AddFilter<irs::Empty>(filter);
    return absl::OkStatus();
  }
  auto& node =
    AddTermSet(MaybeNegated(filter, ctx, *column), column->field_id, terms, 1);
  node.SetBoost(ctx.boost);
  node.SetScorer(&irs::ForceConstScore());
  return absl::OkStatus();
}

std::optional<absl::Status> TryFromInetFunction(
  BoolTarget filter, const FilterContext& ctx,
  const duckdb::BoundFunctionExpression& func) {
  const std::string_view name = func.Function().GetName().GetIdentifierName();
  bool contained;
  if (name == "<<=" || name == "subnet_contained_by_or_equals") {
    contained = true;
  } else if (name == ">>=" || name == "subnet_contains_or_equals") {
    contained = false;
  } else {
    return std::nullopt;
  }
  const auto& args = func.GetChildren();
  if (args.size() != 2) {
    return std::nullopt;
  }
  const auto operands = ResolveOperands(ctx, *args[0], *args[1]);
  if (!operands) {
    return std::nullopt;
  }
  if (operands->swapped) {
    contained = !contained;
  }
  if (operands->constant->IsNull()) {
    AddFilter<irs::Empty>(filter);
    return absl::OkStatus();
  }
  const auto value = inet_terms::FromValue(*operands->constant);
  if (!value) {
    return Unsupported();
  }
  const auto target = MaybeNegated(filter, ctx, *operands->column);
  if (contained) {
    AddContainedBy(target, *operands->column, *value);
  } else {
    AddContains(target, *operands->column, *value);
  }
  return absl::OkStatus();
}

}  // namespace sdb::connector
