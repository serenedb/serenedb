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

#include <absl/algorithm/container.h>

#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
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
constexpr std::string_view kContainedByOp = "<<=";
constexpr std::string_view kContainedByFunction =
  "subnet_contained_by_or_equals";
constexpr std::string_view kContainsOp = ">>=";
constexpr std::string_view kContainsFunction = "subnet_contains_or_equals";
constexpr std::string_view kStrictlyContainedByOp = "<<";
constexpr std::string_view kStrictlyContainsOp = ">>";
constexpr std::string_view kOverlapsOp = "&&";

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

const duckdb::Value* FindInetConstant(const duckdb::Expression& expr) {
  if (const auto* value = TryGetConstant(expr)) {
    return value;
  }
  return TryGetConstant(PeelSortKey(expr));
}

std::optional<InetNetworkOp> NetworkOpOf(std::string_view name) {
  if (name == kContainedByOp || name == kContainedByFunction) {
    return InetNetworkOp::ContainedBy;
  }
  if (name == kContainsOp || name == kContainsFunction) {
    return InetNetworkOp::Contains;
  }
  if (name == kStrictlyContainedByOp) {
    return InetNetworkOp::StrictlyContainedBy;
  }
  if (name == kStrictlyContainsOp) {
    return InetNetworkOp::StrictlyContains;
  }
  if (name == kOverlapsOp) {
    return InetNetworkOp::Overlaps;
  }
  return std::nullopt;
}

InetNetworkOp SwapNetworkOp(InetNetworkOp op) {
  switch (op) {
    case InetNetworkOp::ContainedBy:
      return InetNetworkOp::Contains;
    case InetNetworkOp::Contains:
      return InetNetworkOp::ContainedBy;
    case InetNetworkOp::StrictlyContainedBy:
      return InetNetworkOp::StrictlyContains;
    case InetNetworkOp::StrictlyContains:
      return InetNetworkOp::StrictlyContainedBy;
    case InetNetworkOp::Overlaps:
      return InetNetworkOp::Overlaps;
  }
  SDB_UNREACHABLE();
}

absl::Status Unsupported() {
  return absl::InvalidArgumentError(
    "INET constant can not be encoded as an index term");
}

void AddByteRange(BoolTarget target, irs::field_id field, irs::bstring min,
                  irs::BoundType min_type, irs::bstring max,
                  irs::BoundType max_type) {
  auto& range = AddFilter<irs::ByRange>(target);
  *range.mutable_field_id() = field;
  range.SetScorer(&irs::ForceConstScore());
  auto& bounds = range.mutable_options()->range;
  bounds.min = std::move(min);
  bounds.min_type = min_type;
  bounds.max = std::move(max);
  bounds.max_type = max_type;
}

void AddByteRange(BoolTarget target, irs::field_id field, irs::bstring min,
                  irs::bstring max) {
  AddByteRange(target, field, std::move(min), irs::BoundType::Inclusive,
               std::move(max), irs::BoundType::Inclusive);
}

void AddBytePrefix(BoolTarget target, irs::field_id field,
                   irs::bstring prefix) {
  auto& filter = AddFilter<irs::ByPrefix>(target);
  *filter.mutable_field_id() = field;
  filter.SetScorer(&irs::ForceConstScore());
  filter.mutable_options()->term = std::move(prefix);
}

struct InetBound {
  inet_terms::Inet value;
  irs::BoundType type = irs::BoundType::Inclusive;
};

BoolTarget Boosted(BoolTarget target, irs::score_t boost) {
  if (boost == irs::kNoBoost) {
    return target;
  }
  const auto group = AddGroup(target, irs::Occur::Must);
  group.node->SetBoost(boost);
  return group;
}

irs::BoundType Flip(irs::BoundType type) {
  return type == irs::BoundType::Inclusive ? irs::BoundType::Exclusive
                                           : irs::BoundType::Inclusive;
}

void AddFamilyRange(BoolTarget target, const SearchColumnInfo& column,
                    const InetBound& min, const InetBound& max) {
  SDB_ASSERT(min.value.family == max.value.family);
  const auto family = min.value.family;
  const auto field = column.field_id;
  const auto first = inet_terms::Network(min.value, min.value.mask);
  const auto last = inet_terms::Network(max.value, max.value.mask);
  if (first > last) {
    AddFilter<irs::Empty>(target);
    return;
  }
  const auto pieces = inet_terms::SplitNetworks(family, first, last);
  const auto exact = [](const inet_terms::NetworkPiece& piece) {
    return piece.level == 0;
  };
  const bool exact_first = absl::c_any_of(pieces, [&](const auto& piece) {
    return exact(piece) && piece.first == first;
  });
  const bool exact_last = absl::c_any_of(pieces, [&](const auto& piece) {
    return exact(piece) && piece.last == last;
  });
  const auto min_term = inet_terms::ExactTermOf(min.value);
  const auto max_term = inet_terms::ExactTermOf(max.value);
  const bool exclude_head =
    !exact_first && (min.type == irs::BoundType::Exclusive ||
                     min_term != inet_terms::FirstExactTermOf(family, first));
  const bool exclude_tail =
    !exact_last && (max.type == irs::BoundType::Exclusive ||
                    max_term != inet_terms::LastExactTermOf(family, last));

  const auto group =
    exclude_head || exclude_tail ? AddGroup(target, irs::Occur::Must) : target;
  const auto pieces_target =
    pieces.size() == 1 ? group : AddGroup(group, irs::Occur::Should);
  for (const auto& piece : pieces) {
    if (exact(piece)) {
      const bool clip_first = piece.first == first;
      const bool clip_last = piece.last == last;
      AddByteRange(
        pieces_target, field,
        clip_first ? min_term
                   : inet_terms::FirstExactTermOf(family, piece.first),
        clip_first ? min.type : irs::BoundType::Inclusive,
        clip_last ? max_term : inet_terms::LastExactTermOf(family, piece.last),
        clip_last ? max.type : irs::BoundType::Inclusive);
    } else if (piece.first == piece.last) {
      AddTerm(pieces_target, field,
              inet_terms::LevelTermOf(family, piece.level, piece.first),
              irs::kNoBoost, &irs::ForceConstScore());
    } else {
      AddByteRange(pieces_target, field,
                   inet_terms::LevelTermOf(family, piece.level, piece.first),
                   inet_terms::LevelTermOf(family, piece.level, piece.last));
    }
  }
  if (pieces.size() != 1) {
    SetMinMatch(*pieces_target.node, 1);
  }
  if (exclude_head) {
    AddByteRange({group.node, irs::Occur::MustNot}, field,
                 inet_terms::FirstExactTermOf(family, first),
                 irs::BoundType::Inclusive, min_term, Flip(min.type));
  }
  if (exclude_tail) {
    AddByteRange({group.node, irs::Occur::MustNot}, field, max_term,
                 Flip(max.type), inet_terms::LastExactTermOf(family, last),
                 irs::BoundType::Inclusive);
  }
}

void AddRange(BoolTarget target, const SearchColumnInfo& column,
              const std::optional<InetBound>& min,
              const std::optional<InetBound>& max) {
  if (min && max && min->value.family > max->value.family) {
    AddFilter<irs::Empty>(target);
    return;
  }
  const auto group = AddGroup(target, irs::Occur::Should);
  for (const auto family : {inet_terms::Family::V4, inet_terms::Family::V6}) {
    if ((min && family < min->value.family) ||
        (max && family > max->value.family)) {
      continue;
    }
    const bool from_family_min = !min || family > min->value.family;
    const bool to_family_max = !max || family < max->value.family;
    if (from_family_min && to_family_max) {
      AddBytePrefix(group, column.field_id,
                    inet_terms::CoarsestLevelPrefixOf(family));
      continue;
    }
    AddFamilyRange(
      group, column,
      from_family_min ? InetBound{.value = inet_terms::FamilyMin(family)}
                      : *min,
      to_family_max ? InetBound{.value = inet_terms::FamilyMax(family)} : *max);
  }
  SetMinMatch(*group.node, 1);
}

void AddComparison(BoolTarget target, const SearchColumnInfo& column,
                   const inet_terms::Inet& value, ComparisonOp op) {
  SDB_ASSERT(op != ComparisonOp::None);
  const InetBound bound{
    .value = value,
    .type = op == ComparisonOp::Lt || op == ComparisonOp::Gt
              ? irs::BoundType::Exclusive
              : irs::BoundType::Inclusive,
  };
  if (op == ComparisonOp::Lt || op == ComparisonOp::Le) {
    AddRange(target, column, std::nullopt, bound);
  } else {
    AddRange(target, column, bound, std::nullopt);
  }
}

void AddContainedBy(BoolTarget target, const SearchColumnInfo& column,
                    const inet_terms::Inet& value) {
  const auto field = column.field_id;
  const uint32_t bits = value.mask;
  const auto group = AddGroup(target, irs::Occur::Must);
  if (bits == 0) {
    AddBytePrefix(group, field,
                  inet_terms::CoarsestLevelPrefixOf(value.family));
    return;
  }
  const auto network = inet_terms::Network(value, bits);
  const auto broadcast = inet_terms::Broadcast(value, bits);
  uint32_t level = inet_terms::LevelCount(value.family);
  while (level != 0 && inet_terms::LevelBits(value.family, level) < bits) {
    --level;
  }
  if (level == 0) {
    AddByteRange(group, field,
                 inet_terms::ExactPrefixOf(value.family, network,
                                           static_cast<uint16_t>(bits)),
                 inet_terms::ExactUpperOf(value.family, broadcast));
    return;
  }
  if (inet_terms::LevelBits(value.family, level) == bits) {
    AddTerm(group, field, inet_terms::LevelTermOf(value.family, level, network),
            irs::kNoBoost, &irs::ForceConstScore());
  } else {
    AddByteRange(group, field,
                 inet_terms::LevelTermOf(value.family, level, network),
                 inet_terms::LevelTermOf(value.family, level, broadcast));
  }
  const auto min_mask = inet_terms::MinMaskOf(value.family, network);
  if (min_mask < bits) {
    AddByteRange(
      {group.node, irs::Occur::MustNot}, field,
      inet_terms::MaskTermOf(value.family, static_cast<uint8_t>(min_mask)),
      inet_terms::MaskTermOf(value.family, static_cast<uint8_t>(bits - 1)));
  }
}

void AddStrictlyContainedBy(BoolTarget target, const SearchColumnInfo& column,
                            const inet_terms::Inet& value) {
  const auto group = AddGroup(target, irs::Occur::Must);
  AddContainedBy(group, column, value);
  AddBytePrefix(
    {group.node, irs::Occur::MustNot}, column.field_id,
    inet_terms::ExactPrefixOf(
      value.family, inet_terms::Network(value, value.mask), value.mask));
}

void AddContains(BoolTarget target, const SearchColumnInfo& column,
                 const inet_terms::Inet& value, uint32_t max_mask) {
  auto& filter = AddFilter<irs::ByPrefixes>(target);
  *filter.mutable_field_id() = column.field_id;
  filter.SetScorer(&irs::ForceConstScore());
  std::vector<irs::bstring> prefixes;
  prefixes.reserve(max_mask + 1);
  for (uint32_t bits = 0; bits <= max_mask; ++bits) {
    prefixes.push_back(
      inet_terms::ExactPrefixOf(value.family, inet_terms::Network(value, bits),
                                static_cast<uint16_t>(bits)));
  }
  filter.mutable_options()->SetPrefixes(std::move(prefixes));
}

}  // namespace

const SearchColumnInfo* FindInetColumn(const FilterContext& ctx,
                                       const duckdb::Expression& expr) {
  const auto& column = PeelSortKey(expr);
  if (!pg::IsInetOrCidr(column.GetReturnType())) {
    return nullptr;
  }
  const auto* info = FindColumnInfoForExpr(ctx, column);
  return info && pg::IsInetOrCidr(info->logical_type) ? info : nullptr;
}

std::optional<InetOperands> ResolveInetOperands(
  const FilterContext& ctx, const duckdb::Expression& left,
  const duckdb::Expression& right) {
  if (const auto* column = FindInetColumn(ctx, left)) {
    if (const auto* constant = FindInetConstant(right)) {
      return InetOperands{.column = column, .constant = constant};
    }
    return std::nullopt;
  }
  if (const auto* column = FindInetColumn(ctx, right)) {
    if (const auto* constant = FindInetConstant(left)) {
      return InetOperands{
        .column = column, .constant = constant, .swapped = true};
    }
  }
  return std::nullopt;
}

std::optional<InetNetworkPredicate> ResolveInetNetworkOp(
  const FilterContext& ctx, const duckdb::BoundFunctionExpression& func) {
  const auto op = NetworkOpOf(func.Function().GetName().GetIdentifierName());
  const auto& args = func.GetChildren();
  if (!op || args.size() != 2) {
    return std::nullopt;
  }
  const auto operands = ResolveInetOperands(ctx, *args[0], *args[1]);
  if (!operands) {
    return std::nullopt;
  }
  return InetNetworkPredicate{
    .column = operands->column,
    .constant = operands->constant,
    .op = operands->swapped ? SwapNetworkOp(*op) : *op,
  };
}

absl::Status FromInetEquality(BoolTarget filter, const FilterContext& ctx,
                              const InetOperands& operands, bool not_equal) {
  if (operands.constant->IsNull()) {
    AddFilter<irs::Empty>(filter);
    return absl::OkStatus();
  }
  const auto value = inet_terms::FromValue(*operands.constant);
  if (!value) {
    return Unsupported();
  }
  const auto target = (ctx.negated != not_equal)
                        ? NegateScoped(filter, *operands.column)
                        : filter;
  AddTerm(target, operands.column->field_id, inet_terms::ExactTermOf(*value),
          ctx.boost, &irs::ForceConstScore());
  return absl::OkStatus();
}

absl::Status FromInetComparison(BoolTarget filter, const FilterContext& ctx,
                                const InetOperands& operands, ComparisonOp op) {
  if (operands.swapped) {
    op = SwapComparisonOp(op);
  }
  if (ctx.negated) {
    op = InvertComparisonOp(op);
  }
  if (operands.constant->IsNull()) {
    AddFilter<irs::Empty>(filter);
    return absl::OkStatus();
  }
  const auto value = inet_terms::FromValue(*operands.constant);
  if (!value) {
    return Unsupported();
  }
  AddComparison(Boosted(filter, ctx.boost), *operands.column, *value, op);
  return absl::OkStatus();
}

absl::Status FromInetBetween(BoolTarget filter, const FilterContext& ctx,
                             const SearchColumnInfo& column,
                             const duckdb::BoundFunctionExpression& between) {
  const auto* lower =
    FindInetConstant(duckdb::BoundBetweenExpression::LowerBound(between));
  const auto* upper =
    FindInetConstant(duckdb::BoundBetweenExpression::UpperBound(between));
  if (!lower || !upper) {
    return absl::InvalidArgumentError("BETWEEN bounds must be constants");
  }
  if (lower->IsNull() || upper->IsNull()) {
    AddFilter<irs::Empty>(filter);
    return absl::OkStatus();
  }
  const auto min = inet_terms::FromValue(*lower);
  const auto max = inet_terms::FromValue(*upper);
  if (!min || !max) {
    return Unsupported();
  }
  const auto bound_type = [](bool inclusive) {
    return inclusive ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
  };
  AddRange(
    Boosted(filter, ctx.boost), column,
    InetBound{.value = *min,
              .type = bound_type(
                duckdb::BoundBetweenExpression::LowerInclusive(between))},
    InetBound{.value = *max,
              .type = bound_type(
                duckdb::BoundBetweenExpression::UpperInclusive(between))});
  return absl::OkStatus();
}

absl::Status FromInetIn(BoolTarget filter, const FilterContext& ctx,
                        const SearchColumnInfo& column,
                        const duckdb::BoundOperatorExpression& op_expr) {
  const auto& children = op_expr.GetChildren();
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
    AddTermSet(MaybeNegated(filter, ctx, column), column.field_id, terms, 1);
  node.SetBoost(ctx.boost);
  node.SetScorer(&irs::ForceConstScore());
  return absl::OkStatus();
}

absl::Status FromInetNetworkOp(BoolTarget filter, const FilterContext& ctx,
                               const InetNetworkPredicate& predicate) {
  if (predicate.constant->IsNull()) {
    AddFilter<irs::Empty>(filter);
    return absl::OkStatus();
  }
  const auto value = inet_terms::FromValue(*predicate.constant);
  if (!value) {
    return Unsupported();
  }
  const auto& column = *predicate.column;
  const auto target = Boosted(MaybeNegated(filter, ctx, column), ctx.boost);
  switch (predicate.op) {
    case InetNetworkOp::ContainedBy:
      AddContainedBy(target, column, *value);
      break;
    case InetNetworkOp::Contains:
      AddContains(target, column, *value, value->mask);
      break;
    case InetNetworkOp::StrictlyContainedBy:
      AddStrictlyContainedBy(target, column, *value);
      break;
    case InetNetworkOp::StrictlyContains:
      if (value->mask == 0) {
        AddFilter<irs::Empty>(target);
      } else {
        AddContains(target, column, *value, value->mask - 1u);
      }
      break;
    case InetNetworkOp::Overlaps: {
      const auto group = AddGroup(target, irs::Occur::Should);
      AddContainedBy(group, column, *value);
      AddContains(group, column, *value, value->mask);
      SetMinMatch(*group.node, 1);
      break;
    }
  }
  return absl::OkStatus();
}

}  // namespace sdb::connector
