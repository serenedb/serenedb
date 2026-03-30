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

#include "rocksdb_filter.hpp"

#include <absl/container/flat_hash_set.h>
#include <absl/strings/str_cat.h>
#include <velox/expression/ExprConstants.h>
#include <velox/vector/ConstantVector.h>

#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"

namespace sdb::connector {
namespace {

std::vector<KeyConstraint> MergeKeyConstraintsPrecise(
  std::vector<KeyConstraint>);
[[maybe_unused]] std::vector<KeyConstraint> MergeKeyConstraintsEager(
  std::vector<KeyConstraint>);

std::vector<KeyConstraint> AnyKeyConstraint(
  const std::span<const std::string> names) {
  return {KeyConstraint(names)};
}

std::vector<KeyConstraint> ExtractFilterEq(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names) {
  SDB_ASSERT(func_call->inputs().size() == 2);
  if (!func_call->inputs()[0]->isFieldAccessKind() ||
      !func_call->inputs()[1]->isConstantKind()) {
    return AnyKeyConstraint(pk_names);
  }
  auto field_access =
    basics::downCast<velox::core::FieldAccessTypedExpr>(func_call->inputs()[0]);
  auto const_val =
    basics::downCast<velox::core::ConstantTypedExpr>(func_call->inputs()[1]);

  if (!absl::c_linear_search(pk_names, field_access->name())) {
    return AnyKeyConstraint(pk_names);
  }
  KeyConstraint p{pk_names};
  p.AddEqFilter(field_access->name(), *const_val, func_call);
  return {p};
}

std::vector<KeyConstraint> ExtractFilterComparison(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names) {
  SDB_ASSERT(func_call->inputs().size() == 2);

  const bool field_left = func_call->inputs()[0]->isFieldAccessKind() &&
                          func_call->inputs()[1]->isConstantKind();
  const bool field_right = func_call->inputs()[1]->isFieldAccessKind() &&
                           func_call->inputs()[0]->isConstantKind();
  if (!field_left && !field_right) {
    return AnyKeyConstraint(pk_names);
  }

  const auto& field_input =
    field_left ? func_call->inputs()[0] : func_call->inputs()[1];
  const auto& const_input =
    field_left ? func_call->inputs()[1] : func_call->inputs()[0];
  auto field_access =
    basics::downCast<velox::core::FieldAccessTypedExpr>(field_input);
  auto const_val =
    basics::downCast<velox::core::ConstantTypedExpr>(const_input);

  if (!absl::c_linear_search(pk_names, field_access->name())) {
    return AnyKeyConstraint(pk_names);
  }

  // Determine the op from the function name, flipping if field is on the right.
  // e.g. `5 > a` with field_right means `a < 5`.
  ComparisonOp op;
  if (IsCallOf(func_call, "_gt")) {
    op = field_left ? ComparisonOp::Gt : ComparisonOp::Lt;
  } else if (IsCallOf(func_call, "_gte")) {
    op = field_left ? ComparisonOp::Ge : ComparisonOp::Le;
  } else if (IsCallOf(func_call, "_lt")) {
    op = field_left ? ComparisonOp::Lt : ComparisonOp::Gt;
  } else {  // _lte
    op = field_left ? ComparisonOp::Le : ComparisonOp::Ge;
  }

  KeyConstraint kc{pk_names};
  kc.AddComparisonFilter(field_access->name(), *const_val, op, func_call);
  return {std::move(kc)};
}

std::vector<KeyConstraint> ExtractFilterIn(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names) {
  if (!func_call->inputs()[0]->isFieldAccessKind() ||
      !func_call->inputs()[1]->isConstantKind()) {
    return AnyKeyConstraint(pk_names);
  }
  auto field_access =
    basics::downCast<velox::core::FieldAccessTypedExpr>(func_call->inputs()[0]);
  auto array_const =
    basics::downCast<velox::core::ConstantTypedExpr>(func_call->inputs()[1]);
  if (array_const->type()->kind() != velox::TypeKind::ARRAY) {
    return AnyKeyConstraint(pk_names);
  }
  if (!absl::c_linear_search(pk_names, field_access->name())) {
    return AnyKeyConstraint(pk_names);
  }

  SDB_ASSERT(array_const->valueVector()->typeKind() == velox::TypeKind::ARRAY);
  auto const_vec = basics::downCast<velox::ConstantVector<velox::ComplexType>>(
    array_const->valueVector());
  auto array_vec =
    basics::downCast<velox::ArrayVector>(const_vec->valueVector());
  const auto offset = array_vec->offsetAt(const_vec->index());
  const auto size = array_vec->sizeAt(const_vec->index());
  const auto& elements = array_vec->elements();

  std::vector<KeyConstraint> points;
  points.reserve(size);
  for (velox::vector_size_t i = 0; i < size; ++i) {
    velox::core::ConstantTypedExpr elem_const{
      velox::BaseVector::wrapInConstant(1, offset + i, elements)};
    KeyConstraint p{pk_names};
    // If all points that uses func_call source expr become specific, it means
    // that this node maybe replaced with constant true, as specific points will
    // be processed in point lookup data source
    p.AddEqFilter(field_access->name(), elem_const, func_call);
    points.push_back(std::move(p));
  }
  return points;
}

std::vector<KeyConstraint> ExtractFilterAnd(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names) {
  SDB_ASSERT(!func_call->inputs().empty());

  // Cartesian product of all children's point sets, intersecting each tuple.
  // An unconstrained child (AnyKeyConstraint — empty filters) acts as identity
  // because Intersect(P, {}) == P, so no special-casing is needed.
  std::vector<KeyConstraint> result =
    ExtractFilterExpr(func_call->inputs()[0], pk_names);
  for (size_t i = 1; i < func_call->inputs().size(); ++i) {
    const auto rhs_pts = ExtractFilterExpr(func_call->inputs()[i], pk_names);
    std::vector<KeyConstraint> next;
    next.reserve(result.size() * rhs_pts.size());
    for (const auto& lhs : result) {
      for (const auto& rhs : rhs_pts) {
        if (auto merged = KeyConstraint::Intersect(lhs, rhs);
            merged && !merged->IsUnconstrained()) {
          next.push_back(std::move(*merged));
        }
      }
    }
    result = std::move(next);
  }
  return result;
}

std::vector<KeyConstraint> ExtractFilterNot(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names);

std::vector<KeyConstraint> ExtractFilterOr(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names) {
  SDB_ASSERT(!func_call->inputs().empty());

  std::vector<KeyConstraint> result;
  for (const auto& input : func_call->inputs()) {
    auto pts = ExtractFilterExpr(input, pk_names);
    // If any point in the child result is unconstrained, the OR is
    // unconstrained.
    if (absl::c_any_of(
          pts, [](const KeyConstraint& p) { return p.IsUnconstrained(); })) {
      return AnyKeyConstraint(pk_names);
    }
    result.insert(result.end(), std::make_move_iterator(pts.begin()),
                  std::make_move_iterator(pts.end()));
  }
  return MergeKeyConstraintsPrecise(std::move(result));
}

// Negates a comparison op for NOT push-down.
ComparisonOp NegateOp(ComparisonOp op) {
  switch (op) {
    case ComparisonOp::Gt:
      return ComparisonOp::Le;
    case ComparisonOp::Ge:
      return ComparisonOp::Lt;
    case ComparisonOp::Lt:
      return ComparisonOp::Ge;
    case ComparisonOp::Le:
      return ComparisonOp::Gt;
    case ComparisonOp::None:
      return ComparisonOp::None;
  }
  SDB_ASSERT(false, "unreachable");
}

std::vector<KeyConstraint> ExtractFilterNot(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names) {
  SDB_ASSERT(func_call->inputs().size() == 1);
  const auto& child_expr = func_call->inputs()[0];
  if (!child_expr->isCallKind()) {
    return AnyKeyConstraint(pk_names);
  }
  const auto* child = child_expr->asUnchecked<velox::core::CallTypedExpr>();

  // not(not(x)) → x
  if (IsCallOf(child, "_not")) {
    auto inner = child->asUnchecked<velox::core::CallTypedExpr>();
    SDB_ASSERT(inner->inputs().size() == 1);
    return ExtractFilterExpr(inner->inputs()[0], pk_names);
  }

  // not(and(a, b)) → or(not(a), not(b))  — De Morgan
  if (child->name() == velox::expression::kAnd) {
    std::vector<KeyConstraint> result;
    for (const auto& input : child->inputs()) {
      auto negated_expr = std::make_shared<velox::core::CallTypedExpr>(
        velox::BOOLEAN(), std::vector<velox::core::TypedExprPtr>{input}, "not");
      auto negated = ExtractFilterNot(
        negated_expr->asUnchecked<velox::core::CallTypedExpr>(), pk_names);
      if (absl::c_any_of(negated, [](const KeyConstraint& p) {
            return p.IsUnconstrained();
          })) {
        return AnyKeyConstraint(pk_names);
      }
      result.insert(result.end(), std::make_move_iterator(negated.begin()),
                    std::make_move_iterator(negated.end()));
    }
    return MergeKeyConstraintsPrecise(std::move(result));
  }

  // not(or(a, b)) → and(not(a), not(b))  — De Morgan
  if (child->name() == velox::expression::kOr) {
    std::vector<KeyConstraint> result;
    bool first = true;
    for (const auto& input : child->inputs()) {
      auto negated_expr = std::make_shared<velox::core::CallTypedExpr>(
        velox::BOOLEAN(), std::vector<velox::core::TypedExprPtr>{input}, "not");
      auto negated = ExtractFilterNot(
        negated_expr->asUnchecked<velox::core::CallTypedExpr>(), pk_names);
      if (first) {
        result = std::move(negated);
        first = false;
        continue;
      }
      std::vector<KeyConstraint> next;
      next.reserve(result.size() * negated.size());
      for (const auto& lhs : result) {
        for (const auto& rhs : negated) {
          if (auto merged = KeyConstraint::Intersect(lhs, rhs);
              merged && !merged->IsUnconstrained()) {
            next.push_back(std::move(*merged));
          }
        }
      }
      result = std::move(next);
    }
    return result;
  }

  // not(a > v) → a <= v, etc.
  if (IsCallOf(child, "_gt") || IsCallOf(child, "_gte") ||
      IsCallOf(child, "_lt") || IsCallOf(child, "_lte")) {
    if (child->inputs().size() != 2) {
      return AnyKeyConstraint(pk_names);
    }
    const bool field_left = child->inputs()[0]->isFieldAccessKind() &&
                            child->inputs()[1]->isConstantKind();
    const bool field_right = child->inputs()[1]->isFieldAccessKind() &&
                             child->inputs()[0]->isConstantKind();
    if (!field_left && !field_right) {
      return AnyKeyConstraint(pk_names);
    }
    const auto& field_input =
      field_left ? child->inputs()[0] : child->inputs()[1];
    const auto& const_input =
      field_left ? child->inputs()[1] : child->inputs()[0];
    auto field_access =
      basics::downCast<velox::core::FieldAccessTypedExpr>(field_input);
    auto const_val =
      basics::downCast<velox::core::ConstantTypedExpr>(const_input);
    if (!absl::c_linear_search(pk_names, field_access->name())) {
      return AnyKeyConstraint(pk_names);
    }

    ComparisonOp op;
    if (IsCallOf(child, "_gt")) {
      op = field_left ? ComparisonOp::Gt : ComparisonOp::Lt;
    } else if (IsCallOf(child, "_gte")) {
      op = field_left ? ComparisonOp::Ge : ComparisonOp::Le;
    } else if (IsCallOf(child, "_lt")) {
      op = field_left ? ComparisonOp::Lt : ComparisonOp::Gt;
    } else {
      op = field_left ? ComparisonOp::Le : ComparisonOp::Ge;
    }

    KeyConstraint kc{pk_names};
    kc.AddComparisonFilter(field_access->name(), *const_val, NegateOp(op),
                           func_call);
    return {std::move(kc)};
  }

  // not(a = v) and not(a in (...)) cannot be expressed as a useful range.
  return AnyKeyConstraint(pk_names);
}

// Recursively rewrites `expr`, replacing any node whose address appears in
// `sources` with a constant `true`. Returns the original pointer when nothing
// changed (avoids allocations on the happy path).
velox::core::TypedExprPtr RewriteExpr(
  const velox::core::TypedExprPtr& expr,
  const containers::FlatHashSet<const velox::core::ITypedExpr*>& sources) {
  if (!expr->isCallKind()) {
    return expr;
  }
  if (sources.contains(expr.get())) {
    return {};
  }

  const auto* call = expr->asUnchecked<velox::core::CallTypedExpr>();
  std::vector<velox::core::TypedExprPtr> new_inputs;
  bool changed = false;
  for (const auto& input : call->inputs()) {
    auto new_input = RewriteExpr(input, sources);
    if (new_input.get() != input.get()) {
      changed = true;
    }
    new_inputs.push_back(std::move(new_input));
  }
  if (!changed) {
    return expr;
  }

  if (call->name() == "and") {
    std::erase_if(new_inputs, [](auto expr) { return expr == nullptr; });
    if (new_inputs.size() == 0) {
      return {};
    }
    if (new_inputs.size() == 1) {
      return new_inputs[0];
    }
  }

  if (call->name() == "or") {
    // If any branch became true (null), the whole OR is trivially true.
    if (absl::c_any_of(new_inputs,
                       [](const auto& e) { return e == nullptr; })) {
      return {};
    }
  }

  // For any other call (e.g. "not"), if any input was rewritten to null (i.e.
  // fully captured), treat the whole expression as captured too.
  if (absl::c_any_of(new_inputs, [](const auto& e) { return e == nullptr; })) {
    return {};
  }

  return std::make_shared<velox::core::CallTypedExpr>(
    expr->type(), std::move(new_inputs), call->name());
}

// Returns the more restrictive (greater) lower bound.
std::optional<Boundary> MaxLeft(const std::optional<Boundary>& a,
                                const std::optional<Boundary>& b) {
  if (!a) {
    return b;
  }
  if (!b) {
    return a;
  }
  if (a->value < b->value) {
    return b;
  }
  if (b->value < a->value) {
    return a;
  }
  // Same value: exclusive is more restrictive for a left bound.
  return a->inclusive ? b : a;
}

// Returns the more restrictive (lesser) upper bound.
std::optional<Boundary> MinRight(const std::optional<Boundary>& a,
                                 const std::optional<Boundary>& b) {
  if (!a) {
    return b;
  }
  if (!b) {
    return a;
  }
  if (a->value < b->value) {
    return a;
  }
  if (b->value < a->value) {
    return b;
  }
  // Same value: exclusive is more restrictive for a right bound.
  return a->inclusive ? b : a;
}

std::optional<Range> IntersectRange(const Range& lhs, const Range& rhs) {
  auto new_left = MaxLeft(lhs.left, rhs.left);
  auto new_right = MinRight(lhs.right, rhs.right);
  if (new_left && new_right) {
    if (new_right->value < new_left->value) {
      return std::nullopt;
    }
    // Same boundary value but at least one side exclusive: empty interval.
    if (new_left->value == new_right->value &&
        (!new_left->inclusive || !new_right->inclusive)) {
      return std::nullopt;
    }
  }
  return Range{std::move(new_left), std::move(new_right)};
}

// Less restrictive (smaller) lower bound — for union.
std::optional<Boundary> MinLeft(const std::optional<Boundary>& a,
                                const std::optional<Boundary>& b) {
  if (!a || !b) {
    return std::nullopt;
  }
  if (a->value < b->value) {
    return a;
  }
  if (b->value < a->value) {
    return b;
  }
  // Same value: inclusive is less restrictive for a left bound.
  return a->inclusive ? a : b;
}

// Less restrictive (larger) upper bound — for union.
std::optional<Boundary> MaxRight(const std::optional<Boundary>& a,
                                 const std::optional<Boundary>& b) {
  if (!a || !b) {
    return std::nullopt;
  }
  if (b->value < a->value) {
    return a;
  }
  if (a->value < b->value) {
    return b;
  }
  // Same value: inclusive is less restrictive for a right bound.
  return a->inclusive ? a : b;
}

bool BoundaryEqual(const std::optional<Boundary>& a,
                   const std::optional<Boundary>& b) {
  if (!a && !b) {
    return true;
  }
  if (!a || !b) {
    return false;
  }
  return a->value == b->value && a->inclusive == b->inclusive;
}

bool RangesEqual(const Range& a, const Range& b) {
  return BoundaryEqual(a.left, b.left) && BoundaryEqual(a.right, b.right);
}

// Unions two ranges if they overlap; returns nullopt if there is a gap.
std::optional<Range> UnionRangeIfOverlapping(const Range& lhs,
                                             const Range& rhs) {
  if (!IntersectRange(lhs, rhs)) {
    return std::nullopt;
  }
  return Range{MinLeft(lhs.left, rhs.left), MaxRight(lhs.right, rhs.right)};
}

// Lexicographic ordering on a column's left bound for sorting purposes.
// Unbounded (-inf) sorts first; among bounded, lower value sorts first;
// ties broken by inclusive < exclusive (inclusive starts earlier).
bool LeftBoundLess(const std::optional<Boundary>& a,
                   const std::optional<Boundary>& b) {
  if (!a && !b) {
    return false;
  }
  if (!a) {
    return true;  // -inf < anything
  }
  if (!b) {
    return false;  // anything >= -inf
  }
  if (a->value < b->value) {
    return true;
  }
  if (b->value < a->value) {
    return false;
  }
  // Same value: inclusive (starts earlier) sorts before exclusive.
  return a->inclusive && !b->inclusive;
}

// Sorts constraints lexicographically by each PK column's left bound in order.
// This groups constraints that share the same prefix columns and orders the
// differing column so overlapping ranges become adjacent.
bool KeyConstraintLess(const KeyConstraint& a, const KeyConstraint& b) {
  for (std::string_view name : a.PkNames()) {
    const Range* ra = a.FindFilter(name);
    const Range* rb = b.FindFilter(name);
    const auto& la = ra ? ra->left : std::optional<Boundary>{};
    const auto& lb = rb ? rb->left : std::optional<Boundary>{};
    if (LeftBoundLess(la, lb)) {
      return true;
    }
    if (LeftBoundLess(lb, la)) {
      return false;
    }
  }
  return false;
}

// Reduces constraints by sorting then doing a single linear scan.
// Leaves constraints with gaps between them as separate entries.
std::vector<KeyConstraint> MergeKeyConstraintsPrecise(
  std::vector<KeyConstraint> constraints) {
  if (constraints.size() <= 1) {
    return constraints;
  }

  absl::c_sort(constraints, KeyConstraintLess);

  std::vector<KeyConstraint> result;
  result.push_back(std::move(constraints[0]));
  for (size_t i = 1; i < constraints.size(); ++i) {
    if (auto merged = KeyConstraint::TryUnion(result.back(), constraints[i])) {
      result.back() = std::move(*merged);
    } else {
      result.push_back(std::move(constraints[i]));
    }
  }
  return result;
}

// Merges all per-column source expression sets from `src` into `dst`.
void MergeSourceExprs(KeyConstraint::SourceExprsMap& dst,
                      const KeyConstraint::SourceExprsMap& src) {
  for (const auto& [col, exprs] : src) {
    auto& dst_set = dst[col];
    dst_set.insert(exprs.begin(), exprs.end());
  }
}

// Folds all constraints into one by taking the bounding range per column.
// Always produces a single constraint; may over-approximate gaps.
std::vector<KeyConstraint> MergeKeyConstraintsEager(
  std::vector<KeyConstraint> constraints) {
  if (constraints.size() <= 1) {
    return constraints;
  }
  KeyConstraint merged = std::move(constraints[0]);
  for (size_t i = 1; i < constraints.size(); ++i) {
    merged = KeyConstraint::ForceUnion(merged, constraints[i]);
  }
  return {std::move(merged)};
}

}  // namespace

template<velox::TypeKind Kind>
velox::variant ExtractScalarVariant(const velox::BaseVector& vec) {
  using T = typename velox::TypeTraits<Kind>::NativeType;
  const auto& cv = static_cast<const velox::ConstantVector<T>&>(vec);
  return velox::variant(cv.valueAt(0));
}

velox::variant ToVariant(const velox::core::ConstantTypedExpr& expr) {
  if (!expr.hasValueVector()) {
    return expr.value();
  }
  if (expr.isNull()) {
    return velox::variant(expr.type()->kind());
  }
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
    ExtractScalarVariant, expr.type()->kind(), *expr.valueVector());
}

std::string ToString(const Range& range) {
  std::string result;
  auto variant_str = [](const velox::variant& v) {
    return v.toString(velox::createScalarType(v.kind()));
  };
  if (!range.left) {
    absl::StrAppend(&result, "(-inf");
  } else {
    absl::StrAppend(&result, range.left->inclusive ? "[" : "(",
                    variant_str(range.left->value));
  }
  absl::StrAppend(&result, ", ");
  if (!range.right) {
    absl::StrAppend(&result, "+inf)");
  } else {
    absl::StrAppend(&result, variant_str(range.right->value),
                    range.right->inclusive ? "]" : ")");
  }
  return result;
}

std::string KeyConstraint::ToString() const {
  if (_column_filters.empty()) {
    return "{}";
  }
  std::string result = "{";
  bool first = true;
  for (std::string_view name : _pk_names) {
    auto it = _column_filters.find(name);
    if (it == _column_filters.end()) {
      continue;
    }
    if (!first) {
      absl::StrAppend(&result, ", ");
    }
    first = false;
    absl::StrAppend(&result, name, ": ", sdb::connector::ToString(*it->second));
  }
  absl::StrAppend(&result, "}");
  return result;
}

size_t KeyConstraint::PrefixSize() const noexcept {
  // Count K: leading exact-equality (point) columns.
  size_t k = 0;
  for (; k < _pk_names.size(); ++k) {
    const Range* r = FindFilter(_pk_names[k]);
    if (r == nullptr) {
      return k;
    }
    if (!r->IsPoint()) {
      // Range column at position k. Valid only if K ≥ 1 (equality prefix
      // required); prefix covers k equality columns + this range column.
      return k + 1;
    }
  }

  return k;
}

bool KeyConstraint::IsSpecific() const {
  return absl::c_all_of(_pk_names, [&](std::string_view name) {
    auto it = _column_filters.find(name);
    if (it == _column_filters.end()) {
      return false;
    }
    return it->second->IsPoint();
  });
}

const containers::FlatHashSet<const velox::core::ITypedExpr*>&
KeyConstraint::GetSourceExprs(std::string_view column_name) const noexcept {
  static const containers::FlatHashSet<const velox::core::ITypedExpr*> kEmpty;
  auto it = _source_exprs.find(column_name);
  return it != _source_exprs.end() ? it->second : kEmpty;
}

const Range* KeyConstraint::FindFilter(std::string_view column_name) const {
  auto it = _column_filters.find(column_name);
  return it != _column_filters.end() ? it->second.get() : nullptr;
}

void KeyConstraint::AddEqFilter(std::string_view column_name,
                                const velox::core::ConstantTypedExpr& value,
                                const velox::core::ITypedExpr* source_expr) {
  SDB_ASSERT(!_column_filters.contains(column_name));
  auto v = ToVariant(value);
  _column_filters.emplace(
    column_name,
    std::make_unique<Range>(Range{Boundary{v, true}, Boundary{v, true}}));
  _source_exprs[std::string(column_name)].insert(source_expr);
}

void KeyConstraint::AddComparisonFilter(
  std::string_view column_name, const velox::core::ConstantTypedExpr& value,
  ComparisonOp op, const velox::core::ITypedExpr* source_expr) {
  SDB_ASSERT(!_column_filters.contains(column_name));
  auto v = ToVariant(value);
  Range range;
  switch (op) {
    case ComparisonOp::Gt:
      range = Range{Boundary{v, false}, std::nullopt};
      break;
    case ComparisonOp::Ge:
      range = Range{Boundary{v, true}, std::nullopt};
      break;
    case ComparisonOp::Lt:
      range = Range{std::nullopt, Boundary{v, false}};
      break;
    case ComparisonOp::Le:
      range = Range{std::nullopt, Boundary{v, true}};
      break;
    case ComparisonOp::None:
      SDB_ASSERT(false, "AddComparisonFilter called with ComparisonOp::None");
      break;
  }
  _column_filters.emplace(column_name, std::make_unique<Range>(range));
  _source_exprs[std::string(column_name)].insert(source_expr);
}

std::optional<KeyConstraint> KeyConstraint::Intersect(
  const KeyConstraint& lhs, const KeyConstraint& rhs) {
  SDB_ASSERT(lhs._pk_names.data() == rhs._pk_names.data());
  KeyConstraint result{lhs._pk_names};
  for (std::string_view pk_name : lhs._pk_names) {
    const auto* lhs_f = lhs.FindFilter(pk_name);
    const auto* rhs_f = rhs.FindFilter(pk_name);
    if (!lhs_f && !rhs_f) {
      continue;
    }
    if (!lhs_f) {
      result._column_filters.emplace(pk_name, std::make_unique<Range>(*rhs_f));
      continue;
    }
    if (!rhs_f) {
      result._column_filters.emplace(pk_name, std::make_unique<Range>(*lhs_f));
      continue;
    }
    auto merged_range = IntersectRange(*lhs_f, *rhs_f);
    if (!merged_range) {
      // e.g. [1,1] AND [2,2] -> contradiction
      return {};
    }
    result._column_filters.emplace(pk_name,
                                   std::make_unique<Range>(*merged_range));
  }
  MergeSourceExprs(result._source_exprs, lhs._source_exprs);
  MergeSourceExprs(result._source_exprs, rhs._source_exprs);
  return result;
}

std::optional<KeyConstraint> KeyConstraint::TryUnion(const KeyConstraint& lhs,
                                                     const KeyConstraint& rhs) {
  SDB_ASSERT(lhs._pk_names.data() == rhs._pk_names.data());

  std::string_view differing_col;
  int diff_count = 0;
  for (std::string_view pk_name : lhs._pk_names) {
    const Range* lf = lhs.FindFilter(pk_name);
    const Range* rf = rhs.FindFilter(pk_name);
    if (!lf && !rf) {
      continue;
    }
    if (!lf || !rf || !RangesEqual(*lf, *rf)) {
      if (++diff_count > 1) {
        return std::nullopt;
      }
      differing_col = pk_name;
    }
  }

  // Start from lhs and merge source exprs from rhs.
  KeyConstraint result{lhs};
  MergeSourceExprs(result._source_exprs, rhs._source_exprs);

  if (diff_count == 0) {
    return result;  // identical constraints
  }

  const Range* lf = lhs.FindFilter(differing_col);
  const Range* rf = rhs.FindFilter(differing_col);

  // One side unconstrained on this column → union removes the constraint.
  if (!lf || !rf) {
    result._column_filters.erase(differing_col);
    return result;
  }

  // Both have a range → merge only if they overlap (no gap).
  auto merged = UnionRangeIfOverlapping(*lf, *rf);
  if (!merged) {
    return std::nullopt;
  }

  auto it = result._column_filters.find(differing_col);
  SDB_ASSERT(it != result._column_filters.end());
  it->second = std::make_unique<Range>(*merged);
  return result;
}

KeyConstraint KeyConstraint::ForceUnion(const KeyConstraint& lhs,
                                        const KeyConstraint& rhs) {
  SDB_ASSERT(lhs._pk_names.data() == rhs._pk_names.data());
  KeyConstraint result{lhs._pk_names};
  MergeSourceExprs(result._source_exprs, lhs._source_exprs);
  MergeSourceExprs(result._source_exprs, rhs._source_exprs);
  for (std::string_view pk_name : lhs._pk_names) {
    const Range* lf = lhs.FindFilter(pk_name);
    const Range* rf = rhs.FindFilter(pk_name);
    if (!lf && !rf) {
      continue;
    }
    // One side unconstrained → union is unconstrained; omit filter.
    if (!lf || !rf) {
      continue;
    }
    result._column_filters.emplace(
      pk_name, std::make_unique<Range>(Range{MinLeft(lf->left, rf->left),
                                             MaxRight(lf->right, rf->right)}));
  }
  return result;
}

std::vector<SpecificPoint> ToSpecificPoints(
  const std::vector<KeyConstraint>& points, const velox::RowType& pk_type) {
  std::vector<SpecificPoint> result;
  result.reserve(points.size());
  for (const auto& p : points) {
    SpecificPoint sp;
    sp.reserve(pk_type.size());
    for (const auto& name : pk_type.names()) {
      const auto* filter = p.FindFilter(name);
      SDB_ASSERT(filter != nullptr, "pk column not found in specific point");
      SDB_ASSERT(filter->left.has_value());
      sp.push_back(filter->left->value);
    }
    result.push_back(std::move(sp));
  }
  return result;
}

std::vector<SpecificRange> ToSpecificRanges(
  const std::vector<KeyConstraint>& ranges, const velox::RowType& pk_type) {
  std::vector<SpecificRange> result;
  result.reserve(ranges.size());
  for (const auto& kc : ranges) {
    const size_t prefix_size = kc.PrefixSize();
    SDB_ASSERT(prefix_size > 0,
               "ToSpecificRanges: constraint must have PrefixSize() >= 1");
    SpecificRange sr;

    // Find the index of the range column: walk leading equality columns until
    // the first non-point column or the end. If all prefix_size columns are
    // equalities, the last one becomes the range column (as a [v, v] point
    // range), so the prefix contains only the columns before it.
    size_t range_col_idx = 0;
    for (; range_col_idx < prefix_size; ++range_col_idx) {
      const Range* r = kc.FindFilter(pk_type.nameOf(range_col_idx));
      SDB_ASSERT(r != nullptr);
      if (!r->IsPoint()) {
        break;
      }
    }
    if (range_col_idx == prefix_size) {
      // All prefix_size columns are equalities; use the last one as range_col.
      range_col_idx = prefix_size - 1;
    }

    // Columns 0..range_col_idx-1 form the equality prefix.
    for (size_t i = 0; i < range_col_idx; ++i) {
      const Range* r = kc.FindFilter(pk_type.nameOf(i));
      SDB_ASSERT(r != nullptr && r->left.has_value());
      sr.prefix.push_back(r->left->value);
    }

    // Column range_col_idx is the range column.
    const Range* r = kc.FindFilter(pk_type.nameOf(range_col_idx));
    SDB_ASSERT(r != nullptr);
    sr.range_col = *r;

    result.push_back(std::move(sr));
  }
  return result;
}

std::vector<KeyConstraint> ExtractFilterExpr(
  const velox::core::TypedExprPtr& expr,
  std::span<const std::string> pk_names) {
  std::vector<KeyConstraint> pts;
  if (!expr->isCallKind()) {
    pts = AnyKeyConstraint(pk_names);
  } else {
    const auto* func_call = expr->asUnchecked<velox::core::CallTypedExpr>();
    if (IsCallOf(func_call, "_eq")) {
      pts = ExtractFilterEq(func_call, pk_names);
    } else if (IsCallOf(func_call, "_in")) {
      pts = ExtractFilterIn(func_call, pk_names);
    } else if (IsCallOf(func_call, "_gt") || IsCallOf(func_call, "_gte") ||
               IsCallOf(func_call, "_lt") || IsCallOf(func_call, "_lte")) {
      pts = ExtractFilterComparison(func_call, pk_names);
    } else if (func_call->name() == velox::expression::kAnd) {
      pts = ExtractFilterAnd(func_call, pk_names);
    } else if (func_call->name() == velox::expression::kOr) {
      pts = ExtractFilterOr(func_call, pk_names);
    } else if (IsCallOf(func_call, "_not")) {
      pts = ExtractFilterNot(func_call, pk_names);
    } else {
      pts = AnyKeyConstraint(pk_names);
    }
  }
  return pts;
}

ExtractAndRewriteResult ExtractAndRewriteFilterExpr(
  const velox::core::TypedExprPtr& expr,
  std::span<const std::string> pk_names) {
  auto constraints = ExtractFilterExpr(expr, pk_names);

  if (absl::c_all_of(constraints,
                     [](const KeyConstraint& p) { return p.IsSpecific(); })) {
    // All constraints are point lookups: rewrite all of their sources.
    containers::FlatHashSet<const velox::core::ITypedExpr*> sources;
    for (const auto& p : constraints) {
      for (const auto& [col, exprs] : p.GetSourceExprs()) {
        sources.insert(exprs.begin(), exprs.end());
      }
    }
    return {ConstraintKind::Points, std::move(constraints),
            RewriteExpr(expr, sources)};
  }

  // Not all specific: fall back to range scan on the first PK column.
  // Rewrite sources of every constraint that has a filter on pk_names[0].
  // The returned constraints carry all filter info; remaining_filter covers
  // predicates whose sources were not captured by any such constraint.
  if (pk_names.empty()) {
    return {ConstraintKind::Empty, {}, expr};
  }

  // Every constraint must form a valid key prefix: first K columns are exact
  // points, the (K+1)-th is a range, and nothing is constrained after that.
  // This excludes cases like `b > 5` (first PK column unconstrained) or
  // `a > 3 AND c > 1` (gap on b) that cannot be represented as a range scan.
  if (absl::c_any_of(constraints, [](const KeyConstraint& c) {
        return c.PrefixSize() == 0;
      })) {
    return {ConstraintKind::Empty, {}, expr};
  }

  // Collect rewrite sources from every column in each constraint's prefix
  // (all K equality columns + the range column), not just the first PK column.
  containers::FlatHashSet<const velox::core::ITypedExpr*> sources;
  for (const auto& c : constraints) {
    const size_t prefix = c.PrefixSize();
    for (size_t i = 0; i < prefix; ++i) {
      const auto& col_exprs = c.GetSourceExprs(pk_names[i]);
      sources.insert(col_exprs.begin(), col_exprs.end());
    }
  }

  if (sources.empty()) {
    return {ConstraintKind::Empty, {}, expr};
  }

  return {ConstraintKind::Ranges, std::move(constraints),
          RewriteExpr(expr, sources)};
}

void SortPoints(std::vector<SpecificPoint>& points) {
  absl::c_sort(points, [](const SpecificPoint& lhs, const SpecificPoint& rhs) {
    for (size_t i = 0; i < lhs.size(); ++i) {
      if (lhs[i] != rhs[i]) {
        return lhs[i] < rhs[i];
      }
    }
    return false;
  });
}

}  // namespace sdb::connector
