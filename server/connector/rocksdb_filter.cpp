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

std::vector<KeyConstraint> MergeKeyConstraintsPrecise(
  std::vector<KeyConstraint>);

void MergeSourceExprs(KeyConstraint::SourceExprsMap& dst,
                      const KeyConstraint::SourceExprsMap& src);

std::vector<KeyConstraint> AnyKeyConstraint(
  const std::span<const std::string> names) {
  return {KeyConstraint::MakeAny(names)};
}

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
  SDB_UNREACHABLE();
}

// Helper: produces two KCs for a != v -> a < v OR a > v.
std::vector<KeyConstraint> MakeNeqConstraints(
  std::string_view col_name, const velox::core::ConstantTypedExpr& val,
  const velox::core::ITypedExpr* source,
  std::span<const std::string> pk_names) {
  auto less_constraint = KeyConstraint::MakeAny(pk_names);
  less_constraint.AddComparisonFilter(col_name, val, ComparisonOp::Lt, source);

  auto greater_constraint = KeyConstraint::MakeAny(pk_names);
  greater_constraint.AddComparisonFilter(col_name, val, ComparisonOp::Gt,
                                         source);
  return {std::move(less_constraint), std::move(greater_constraint)};
}

std::vector<KeyConstraint> ExtractFilterEq(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names, bool negated) {
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
  if (negated) {
    // NOT(a = v) -> a < v OR a > v
    return MakeNeqConstraints(field_access->name(), *const_val, func_call,
                              pk_names);
  }
  auto p = KeyConstraint::MakeAny(pk_names);
  p.AddEqFilter(field_access->name(), *const_val, func_call);
  return {p};
}

// a != v  ->  a < v  OR  a > v
std::vector<KeyConstraint> ExtractFilterNeq(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names, bool negated) {
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
  if (negated) {
    // NOT(a != v) -> a = v
    auto kc = KeyConstraint::MakeAny(pk_names);
    kc.AddEqFilter(field_access->name(), *const_val, func_call);
    return {std::move(kc)};
  }
  return MakeNeqConstraints(field_access->name(), *const_val, func_call,
                            pk_names);
}

std::vector<KeyConstraint> ExtractFilterComparison(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names, bool negated) {
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

  // Base op (field on left), then flip direction for field_right (Gt<->Lt,
  // Ge<->Le), then flip inclusivity for negated (Gt<->Le, Ge<->Lt). e.g. `5 >
  // a` with field_right -> a < 5; negated `a > 5` -> a <= 5.
  auto flip_direction = [](ComparisonOp o) {
    switch (o) {
      case ComparisonOp::Gt:
        return ComparisonOp::Lt;
      case ComparisonOp::Lt:
        return ComparisonOp::Gt;
      case ComparisonOp::Ge:
        return ComparisonOp::Le;
      case ComparisonOp::Le:
        return ComparisonOp::Ge;
      case ComparisonOp::None:
        return ComparisonOp::None;
    }
    SDB_UNREACHABLE();
  };

  ComparisonOp op;
  if (IsCallOf(func_call, "_gt")) {
    op = ComparisonOp::Gt;
  } else if (IsCallOf(func_call, "_gte")) {
    op = ComparisonOp::Ge;
  } else if (IsCallOf(func_call, "_lt")) {
    op = ComparisonOp::Lt;
  } else {
    op = ComparisonOp::Le;  // _lte
  }

  if (!field_left) {
    op = flip_direction(op);
  }
  if (negated) {
    op = NegateOp(op);
  }

  auto constraint = KeyConstraint::MakeAny(pk_names);
  constraint.AddComparisonFilter(field_access->name(), *const_val, op,
                                 func_call);
  return {std::move(constraint)};
}

std::vector<KeyConstraint> ExtractFilterIn(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names, bool negated) {
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
  SDB_ASSERT(size > 0);
  const auto& elements = array_vec->elements();

  const std::string& col = field_access->name();

  if (!negated) {
    // a IN (x1, ..., xn)  ->  n equality constaints
    std::vector<KeyConstraint> points;
    points.reserve(size);
    for (velox::vector_size_t i = 0; i < size; ++i) {
      velox::core::ConstantTypedExpr elem_const{
        velox::BaseVector::wrapInConstant(1, offset + i, elements)};
      auto p = KeyConstraint::MakeAny(pk_names);
      // If any point is used for range scan or point lookup, it means
      // this filter fully utilized in table scan, so it's OK
      // to replace with true.
      p.AddEqFilter(col, elem_const, func_call);
      points.push_back(std::move(p));
    }
    return points;
  }

  // NOT IN (x1, ..., xn)  ->  n+1 open intervals between sorted values
  // Sort element indices by value so we can build ordered complement intervals.
  std::vector<velox::vector_size_t> order(size);
  std::iota(order.begin(), order.end(), 0);
  absl::c_sort(order, [&](velox::vector_size_t a, velox::vector_size_t b) {
    velox::core::ConstantTypedExpr ea{
      velox::BaseVector::wrapInConstant(1, offset + a, elements)};
    velox::core::ConstantTypedExpr eb{
      velox::BaseVector::wrapInConstant(1, offset + b, elements)};
    return ToVariant(ea) < ToVariant(eb);
  });

  auto elem = [&](velox::vector_size_t i) {
    return velox::core::ConstantTypedExpr{
      velox::BaseVector::wrapInConstant(1, offset + order[i], elements)};
  };
  auto make_constraint = [&](ComparisonOp op,
                             const velox::core::ConstantTypedExpr& val) {
    auto constraint = KeyConstraint::MakeAny(pk_names);
    constraint.AddComparisonFilter(col, val, op, func_call);
    return constraint;
  };

  std::vector<KeyConstraint> result;
  result.reserve(size + 1);
  result.push_back(make_constraint(ComparisonOp::Lt, elem(0)));
  for (velox::vector_size_t i = 1; i < size; ++i) {
    if (auto kc = KeyConstraint::TryIntersect(
          make_constraint(ComparisonOp::Gt, elem(i - 1)),
          make_constraint(ComparisonOp::Lt, elem(i)))) {
      result.push_back(std::move(*kc));
    }
  }
  result.push_back(make_constraint(ComparisonOp::Gt, elem(size - 1)));
  return result;
}

std::vector<KeyConstraint> ExtractFilterAnd(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names, bool negated) {
  SDB_ASSERT(!func_call->inputs().empty());

  // Cartesian product of all children's point sets, intersecting each tuple.
  // An unconstrained child (AnyPoint -- empty filters) acts as identity because
  // Intersect(P, {}) == P, so no special-casing is needed.
  std::vector<KeyConstraint> result =
    ExtractFilterExpr(func_call->inputs()[0], pk_names, negated);
  for (size_t i = 1; i < func_call->inputs().size(); ++i) {
    // Propagate contradictory: AND(contradictory, anything) = contradictory.
    if (absl::c_all_of(
          result, [](const KeyConstraint& c) { return c.IsContradictory(); })) {
      return result;
    }
    const auto rhs_pts =
      ExtractFilterExpr(func_call->inputs()[i], pk_names, negated);
    std::vector<KeyConstraint> next;
    next.reserve(result.size() * rhs_pts.size());
    bool had_contradiction = false;
    for (const auto& lhs : result) {
      for (const auto& rhs : rhs_pts) {
        if (auto merged = KeyConstraint::TryIntersect(lhs, rhs)) {
          if (!merged->IsUnconstrained()) {
            next.push_back(std::move(*merged));
          }
        } else {
          had_contradiction = true;
        }
      }
    }
    result = std::move(next);
    if (result.empty()) {
      if (had_contradiction) {
        // All pairs were contradictions: this AND can never be satisfied.
        return {KeyConstraint::MakeContradictory(pk_names)};
      }
      // All intersections produced unconstrained results: AND is unconstrained.
      return AnyKeyConstraint(pk_names);
    }
  }
  return result;
}

std::vector<KeyConstraint> ExtractFilterOr(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names, bool negated) {
  SDB_ASSERT(!func_call->inputs().empty());

  std::vector<KeyConstraint> result;
  for (const auto& input : func_call->inputs()) {
    auto constraints = ExtractFilterExpr(input, pk_names, negated);

    if (constraints.empty() ||
        absl::c_any_of(constraints, [](const KeyConstraint& p) {
          return p.IsUnconstrained();
        })) {
      return AnyKeyConstraint(pk_names);
    }

    if (absl::c_all_of(constraints, [](const KeyConstraint& p) {
          return p.IsContradictory();
        })) {
      continue;
    }

    result.insert(result.end(), std::make_move_iterator(constraints.begin()),
                  std::make_move_iterator(constraints.end()));
  }
  // All branches were contradictory -> OR is itself contradictory.
  if (result.empty()) {
    return {KeyConstraint::MakeContradictory(pk_names)};
  }
  return MergeKeyConstraintsPrecise(std::move(result));
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

// Sorts constraints lexicographically by each PK column's left bound in order.
// This groups constraints that share the same prefix columns and orders the
// differing column so overlapping ranges become adjacent.
// ── Sweep helpers ────────────────────────────────────────────────────────────

// One atomic segment on a single dimension's axis.
// is_point=false -> open interval (left, right); has_left/has_right mark
// whether the endpoint is finite. is_point=true -> closed singleton {left}.
struct Atom {
  bool is_point{false};
  bool has_left{false};
  bool has_right{false};
  velox::variant left;   // valid iff has_left
  velox::variant right;  // valid iff has_right (== left when is_point)

  // Closed singleton {v}.
  [[nodiscard]] static Atom Point(velox::variant v) {
    return {.is_point = true,
            .has_left = true,
            .has_right = true,
            .left = v,
            .right = std::move(v)};
  }

  // Open interval (l, r); pass std::nullopt for -inf / +inf.
  [[nodiscard]] static Atom Interval(std::optional<velox::variant> l,
                                     std::optional<velox::variant> r) {
    return {.is_point = false,
            .has_left = l.has_value(),
            .has_right = r.has_value(),
            .left = l ? std::move(*l) : velox::variant{},
            .right = r ? std::move(*r) : velox::variant{}};
  }

  bool operator==(const Atom&) const = default;
};

// Lightweight constraint representation used during the recursive sweep.
struct SweepRange {
  std::span<const std::string> pk_names;
  containers::FlatHashMap<std::string, ColumnRange> col_ranges;
  KeyConstraint::SourceExprsMap source_exprs;
};

// Builds the atom sequence for a sorted, deduplicated list of event points.
// Emits: (-inf, p0), {p0}, (p0,p1), {p1}, ..., {pN}, (pN, +inf).
// If event_points is empty, emits one unconstrained open atom.
std::vector<Atom> BuildAtoms(const std::vector<velox::variant>& pts) {
  if (pts.empty()) {
    return {Atom::Interval(std::nullopt,
                           std::nullopt)};  // unconstrained (-inf, +inf)
  }
  std::vector<Atom> atoms;
  atoms.reserve(2 * pts.size() + 1);
  atoms.push_back(Atom::Interval(std::nullopt, pts[0]));
  for (size_t i = 0; i + 1 < pts.size(); ++i) {
    atoms.push_back(Atom::Point(pts[i]));
    atoms.push_back(Atom::Interval(pts[i], pts[i + 1]));
  }
  atoms.push_back(Atom::Point(pts.back()));
  atoms.push_back(Atom::Interval(pts.back(), std::nullopt));
  return atoms;
}

// Returns true iff ColumnRange cr contains atom a.
// cr with no bounds is treated as (-inf, +inf) and covers every atom.
bool AtomContainedBy(const Atom& a, const ColumnRange& cr) {
  if (a.is_point) {
    const velox::variant& point = a.left;
    const bool left_ok = !cr.HasLeft() || cr.left_value < point ||
                         (cr.left_value == point && cr.IsLeftInclusive());
    const bool right_ok = !cr.HasRight() || point < cr.right_value ||
                          (cr.right_value == point && cr.IsRightInclusive());
    return left_ok && right_ok;
  } else {
    // Open interval atom (p, q): cr.left < q  AND  cr.right > p.
    // Infinite atom ends are handled by short-circuit.
    const bool left_ok =
      !a.has_right ||   // atom right is +inf -> cr always satisfies
      !cr.HasLeft() ||  // cr left is -inf -> always ok
      cr.left_value < a.right;
    const bool right_ok = !a.has_left ||     // atom left is -inf -> always ok
                          !cr.HasRight() ||  // cr right is +inf -> always ok
                          a.left < cr.right_value;
    return left_ok && right_ok;
  }
}

// Converts an Atom to its ColumnRange representation.
ColumnRange AtomToColumnRange(const Atom& a) {
  if (a.is_point) {
    return ColumnRange::Point(a.left);
  }
  if (!a.has_left && !a.has_right) {
    return ColumnRange{};  // fully unconstrained
  }
  if (!a.has_left) {
    return ColumnRange::RightBound(a.right, false);
  }
  if (!a.has_right) {
    return ColumnRange::LeftBound(a.left, false);
  }
  return ColumnRange::Bounded(a.left, false, a.right, false);
}

// Reconstructs the merged ColumnRange for a contiguous run of atoms.
// Left bound comes from the first atom, right bound from the last.
// For a single-atom run (first == last), delegates to AtomToColumnRange.
ColumnRange FuseAtomRange(const Atom& first, const Atom& last) {
  if (first == last) {
    return AtomToColumnRange(first);
  }

  // Left: point atom -> inclusive; open atom -> exclusive; no left ->
  // unbounded.
  const bool left_inc = first.is_point;
  // Right: point atom -> inclusive; open atom -> exclusive; no right ->
  // unbounded.
  const bool right_inc = last.is_point;

  const bool has_l = first.has_left;
  const bool has_r = last.has_right;

  if (!has_l && !has_r) {
    return ColumnRange{};
  }
  if (!has_l) {
    return ColumnRange::RightBound(last.is_point ? last.left : last.right,
                                   right_inc);
  }
  if (!has_r) {
    return ColumnRange::LeftBound(first.left, left_inc);
  }
  return ColumnRange::Bounded(
    first.left, left_inc, last.is_point ? last.left : last.right, right_inc);
}

// Collects, sorts, and deduplicates all boundary values from `ranges` on
// dimension `dim_col`. Missing column (unconstrained) contributes no events.
std::vector<velox::variant> CollectEventPoints(
  const std::vector<SweepRange>& ranges, std::string_view dim_col) {
  std::vector<velox::variant> pts;
  for (const auto& sr : ranges) {
    auto it = sr.col_ranges.find(dim_col);
    if (it == sr.col_ranges.end()) {
      continue;
    }
    const ColumnRange& cr = it->second;
    if (cr.HasLeft()) {
      pts.push_back(cr.left_value);
    }
    if (cr.HasRight()) {
      pts.push_back(cr.right_value);
    }
  }
  absl::c_sort(pts);
  pts.erase(std::unique(pts.begin(), pts.end()), pts.end());
  return pts;
}

// Returns a copy of sr with dim_col removed from col_ranges.
SweepRange ProjectAwayDim(const SweepRange& sr, std::string_view dim_col) {
  SweepRange out;
  out.pk_names = sr.pk_names;
  out.source_exprs = sr.source_exprs;
  for (const auto& [k, v] : sr.col_ranges) {
    if (k != dim_col) {
      out.col_ranges.emplace(k, v);
    }
  }
  return out;
}

// Two SweepRanges are equal on all dimensions except dim_col.
bool EqualExceptDim(const SweepRange& a, const SweepRange& b,
                    std::string_view dim_col) {
  for (const auto& name : a.pk_names) {
    if (name == dim_col) {
      continue;
    }
    const auto* ca =
      a.col_ranges.count(name) ? &a.col_ranges.at(name) : nullptr;
    const auto* cb =
      b.col_ranges.count(name) ? &b.col_ranges.at(name) : nullptr;
    if (!ca && !cb) {
      continue;
    }
    if (!ca || !cb) {
      return false;
    }
    if (!(*ca == *cb)) {
      return false;
    }
  }
  return true;
}

// Fuses adjacent atom-result pairs that are consecutive and share the same
// sub-result on all other dimensions. Reconstructs ColumnRange for dim_col.
std::vector<SweepRange> MergeAlongDim0(
  std::vector<std::pair<Atom, SweepRange>> atom_results,
  std::string_view dim_col) {
  if (atom_results.empty()) {
    return {};
  }
  std::vector<SweepRange> result;
  size_t run_start = 0;
  for (size_t i = 1; i <= atom_results.size(); ++i) {
    bool merge = false;
    if (i < atom_results.size()) {
      // Atoms are consecutive if they share an endpoint and the sub-results
      // agree on all other dimensions.
      const Atom& prev = atom_results[i - 1].first;
      const Atom& curr = atom_results[i].first;
      const SweepRange& prev_sr = atom_results[i - 1].second;
      const SweepRange& curr_sr = atom_results[i].second;
      // Consecutive: end of prev == start of curr.
      const velox::variant& prev_end =
        prev.is_point ? prev.left
                      : (prev.has_right ? prev.right : velox::variant{});
      const velox::variant& curr_start =
        curr.is_point ? curr.left
                      : (curr.has_left ? curr.left : velox::variant{});
      // In BuildAtoms the sequence strictly alternates open/point/open/..., so
      // two open-interval atoms are never adjacent: there is always a point
      // atom between them. If that point atom was skipped (not covered by any
      // range) there is a gap at the shared boundary and we must NOT merge.
      const bool alternating = (prev.is_point != curr.is_point);
      const bool endpoints_touch = alternating &&
                                   (prev.has_right == curr.has_left) &&
                                   (!prev.has_right || prev_end == curr_start);
      merge = endpoints_touch && EqualExceptDim(prev_sr, curr_sr, dim_col);
    }
    if (!merge) {
      // Flush the run [run_start, i).
      SweepRange out = atom_results[run_start].second;
      const ColumnRange fused =
        FuseAtomRange(atom_results[run_start].first, atom_results[i - 1].first);
      if (fused.flags != ColumnRange::kZero) {
        out.col_ranges[std::string(dim_col)] = fused;
      } else {
        out.col_ranges.erase(dim_col);
      }
      // Union source_exprs from the entire run.
      for (size_t j = run_start + 1; j < i; ++j) {
        MergeSourceExprs(out.source_exprs, atom_results[j].second.source_exprs);
      }
      result.push_back(std::move(out));
      run_start = i;
    }
  }
  return result;
}

// Recursively sweeps dimensions pk_names[dim_idx..K-1].
// Returns SweepRanges with ColumnRange entries for those dimensions.
std::vector<SweepRange> SweepDims(std::vector<SweepRange> ranges,
                                  std::span<const std::string> pk_names,
                                  size_t dim_idx) {
  if (dim_idx == pk_names.size()) {
    // Base case: all dimensions projected away. Merge source exprs.
    SweepRange out;
    out.pk_names = pk_names;
    for (const auto& sr : ranges) {
      MergeSourceExprs(out.source_exprs, sr.source_exprs);
    }
    return {std::move(out)};
  }

  const std::string& dim_col = pk_names[dim_idx];
  const auto events = CollectEventPoints(ranges, dim_col);
  const auto atoms = BuildAtoms(events);

  static const ColumnRange kUnconstrained{};
  std::vector<std::pair<Atom, SweepRange>> atom_results;
  for (const auto& a : atoms) {
    std::vector<SweepRange> active;
    for (const auto& sr : ranges) {
      auto it = sr.col_ranges.find(dim_col);
      const ColumnRange& cr =
        (it != sr.col_ranges.end()) ? it->second : kUnconstrained;
      if (AtomContainedBy(a, cr)) {
        active.push_back(ProjectAwayDim(sr, dim_col));
      }
    }
    if (active.empty()) {
      continue;
    }
    auto sub = SweepDims(std::move(active), pk_names, dim_idx + 1);
    const ColumnRange atom_cr = AtomToColumnRange(a);
    for (auto& s : sub) {
      // Only insert a ColumnRange for this dimension if it actually constrains
      // something (flags != 0). Unconstrained open atoms produced for
      // dimensions with no event points represent (-inf, +inf) and should be
      // absent from col_ranges so FindColumnRange returns nullptr for them.
      if (atom_cr.flags != ColumnRange::kZero) {
        s.col_ranges[std::string(dim_col)] = atom_cr;
      }
      atom_results.emplace_back(a, std::move(s));
    }
  }
  return MergeAlongDim0(std::move(atom_results), dim_col);
}

// Produces a set of disjoint KeyConstraints covering exactly the union of the
// input constraints' key-spaces, using the recursive atomic-sweep algorithm.
std::vector<KeyConstraint> MergeKeyConstraintsPrecise(
  std::vector<KeyConstraint> constraints) {
  if (constraints.empty()) {
    return {};
  }
  const auto pk_names = constraints[0].PKNames();

  for (const auto& c : constraints) {
    if (c.IsUnconstrained()) {
      return {KeyConstraint::MakeAny(pk_names)};
    }
  }
  std::erase_if(constraints,
                [](const KeyConstraint& c) { return c.IsContradictory(); });
  SDB_ASSERT(!constraints.empty());

  if (constraints.size() == 1) {
    return constraints;
  }

  std::vector<SweepRange> inputs;
  inputs.reserve(constraints.size());
  for (const auto& kc : constraints) {
    SweepRange sr;
    sr.pk_names = pk_names;
    for (const auto& name : pk_names) {
      if (const auto* cr = kc.FindColumnRange(name)) {
        sr.col_ranges.emplace(name, *cr);
      }
    }
    sr.source_exprs = kc.GetSourceExprs();
    inputs.push_back(std::move(sr));
  }

  auto swept = SweepDims(std::move(inputs), pk_names, 0);

  std::vector<KeyConstraint> result;
  result.reserve(swept.size());
  for (auto& sr : swept) {
    result.push_back(KeyConstraint::BuildFromRanges(
      pk_names, std::move(sr.col_ranges), std::move(sr.source_exprs)));
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

}  // namespace

std::string ColumnRange::toString() const {
  auto variant_str = [](const velox::variant& v) {
    return v.toString(velox::createScalarType(v.kind()));
  };
  if (IsPoint()) {
    return variant_str(left_value);
  }
  std::string result;
  if (!HasLeft()) {
    absl::StrAppend(&result, "(-inf");
  } else {
    absl::StrAppend(&result, IsLeftInclusive() ? "[" : "(",
                    variant_str(left_value));
  }
  absl::StrAppend(&result, ", ");
  if (!HasRight()) {
    absl::StrAppend(&result, "+inf)");
  } else {
    absl::StrAppend(&result, variant_str(right_value),
                    IsRightInclusive() ? "]" : ")");
  }
  return result;
}

std::string KeyConstraint::toString() const {
  if (_column_ranges.empty()) {
    return "{}";
  }
  std::string result = "{";
  bool first = true;
  for (std::string_view name : _pk_names) {
    auto it = _column_ranges.find(name);
    if (it == _column_ranges.end()) {
      continue;
    }
    if (!first) {
      absl::StrAppend(&result, ", ");
    }
    first = false;
    absl::StrAppend(&result, name, ": ", it->second.toString());
  }
  absl::StrAppend(&result, "}");
  return result;
}

size_t KeyConstraint::RangePrefixSize() const noexcept {
  for (size_t k = 0; k < _pk_names.size(); ++k) {
    const ColumnRange* column_range = FindColumnRange(_pk_names[k]);
    if (column_range == nullptr) {
      return k;
    }
    if (!column_range->IsPoint()) {
      // k specific points that defines prefix and
      // one on-column range that defines a rocksdb key range.
      return k + 1;
    }
  }

  return _pk_names.size();
}

bool KeyConstraint::IsSpecificPoint() const {
  return absl::c_all_of(_pk_names, [&](std::string_view name) {
    auto it = _column_ranges.find(name);
    if (it == _column_ranges.end()) {
      return false;
    }
    return it->second.IsPoint();
  });
}

KeyConstraint KeyConstraint::BuildFromRanges(
  std::span<const std::string> pk_names,
  containers::FlatHashMap<std::string, ColumnRange> ranges,
  SourceExprsMap source_exprs) {
  KeyConstraint kc{pk_names};
  kc._column_ranges = std::move(ranges);
  kc._source_exprs = std::move(source_exprs);
  return kc;
}

bool ColumnRange::operator==(const ColumnRange& other) const noexcept {
  return flags == other.flags && left_value == other.left_value &&
         right_value == other.right_value;
}

std::optional<ColumnRange> ColumnRange::IntersectWith(
  const ColumnRange& other) const {
  // Tightest left: greater (more restrictive) lower bound.
  // Unbounded (-inf) loses; on equal value, exclusive wins.
  auto pick_tighter_left = [](const ColumnRange& a,
                              const ColumnRange& b) -> const ColumnRange& {
    if (!a.HasLeft()) {
      return b;
    }
    if (!b.HasLeft()) {
      return a;
    }
    if (a.left_value < b.left_value) {
      return b;
    }
    if (b.left_value < a.left_value) {
      return a;
    }
    return a.IsLeftInclusive() ? b : a;  // exclusive is more restrictive
  };

  // Tightest right: lesser (more restrictive) upper bound.
  // Unbounded (+inf) loses; on equal value, exclusive wins.
  auto pick_tighter_right = [](const ColumnRange& a,
                               const ColumnRange& b) -> const ColumnRange& {
    if (!a.HasRight()) {
      return b;
    }
    if (!b.HasRight()) {
      return a;
    }
    if (b.right_value < a.right_value) {
      return b;
    }
    if (a.right_value < b.right_value) {
      return a;
    }
    return a.IsRightInclusive() ? b : a;  // exclusive is more restrictive
  };

  const ColumnRange& ls = pick_tighter_left(*this, other);
  const ColumnRange& rs = pick_tighter_right(*this, other);

  if (ls.HasLeft() && rs.HasRight()) {
    if (rs.right_value < ls.left_value) {
      return std::nullopt;
    }
    if (ls.left_value == rs.right_value &&
        (!ls.IsLeftInclusive() || !rs.IsRightInclusive())) {
      return std::nullopt;
    }
  }

  // Mask the relevant flag bits from each source and copy the values.
  // Unset left_value/right_value are always default-constructed, so
  // copying them unconditionally is safe.
  ColumnRange result;
  result.flags |= ls.flags & (kLeftBounded | kLeftInclusive);
  result.flags |= rs.flags & (kRightBounded | kRightInclusive);
  result.left_value = ls.left_value;
  result.right_value = rs.right_value;
  return result;
}

bool ColumnRange::OverlapsWith(const ColumnRange& other) const {
  return IntersectWith(other).has_value();
}

ColumnRange ColumnRange::UnionWith(const ColumnRange& other) const {
  // Widest left bound: pick the lesser (less restrictive) lower bound.
  // If either is unbounded, result is unbounded.
  ColumnRange result;
  if (HasLeft() && other.HasLeft()) {
    bool use_other_left;
    if (left_value < other.left_value) {
      use_other_left = false;
    } else if (other.left_value < left_value) {
      use_other_left = true;
    } else {
      // Same value: inclusive is less restrictive.
      use_other_left = !IsLeftInclusive() && other.IsLeftInclusive();
    }
    const ColumnRange& left_src = use_other_left ? other : *this;
    result.flags |=
      kLeftBounded | (left_src.IsLeftInclusive() ? kLeftInclusive : kZero);
    result.left_value = left_src.left_value;
  }
  // else: at least one side is unbounded -> result has no left bound.

  // Widest right bound: pick the greater (less restrictive) upper bound.
  if (HasRight() && other.HasRight()) {
    bool use_other_right;
    if (other.right_value < right_value) {
      use_other_right = false;
    } else if (right_value < other.right_value) {
      use_other_right = true;
    } else {
      // Same value: inclusive is less restrictive.
      use_other_right = !IsRightInclusive() && other.IsRightInclusive();
    }
    const ColumnRange& right_src = use_other_right ? other : *this;
    result.flags |=
      kRightBounded | (right_src.IsRightInclusive() ? kRightInclusive : kZero);
    result.right_value = right_src.right_value;
  }
  // else: at least one side is unbounded -> result has no right bound.

  return result;
}

bool ColumnRange::LeftBoundLessThan(const ColumnRange& other) const noexcept {
  if (!HasLeft()) {
    return other.HasLeft();  // -inf < bounded, -inf == -inf
  }
  if (!other.HasLeft()) {
    return false;  // bounded >= -inf
  }
  if (left_value < other.left_value) {
    return true;
  }
  if (other.left_value < left_value) {
    return false;
  }
  // Same value: inclusive (starts earlier) sorts before exclusive.
  return IsLeftInclusive() && !other.IsLeftInclusive();
}

const containers::FlatHashSet<const velox::core::ITypedExpr*>&
KeyConstraint::GetSourceExprs(std::string_view column_name) const noexcept {
  static const containers::FlatHashSet<const velox::core::ITypedExpr*> kEmpty;
  auto it = _source_exprs.find(column_name);
  return it != _source_exprs.end() ? it->second : kEmpty;
}

const ColumnRange* KeyConstraint::FindColumnRange(
  std::string_view column_name) const {
  auto it = _column_ranges.find(column_name);
  return it != _column_ranges.end() ? &it->second : nullptr;
}

void KeyConstraint::AddEqFilter(std::string_view column_name,
                                const velox::core::ConstantTypedExpr& value,
                                const velox::core::ITypedExpr* source_expr) {
  SDB_ASSERT(!_column_ranges.contains(column_name));
  auto v = ToVariant(value);
  _column_ranges.emplace(column_name, ColumnRange::Point(std::move(v)));
  _source_exprs[std::string(column_name)].insert(source_expr);
}

void KeyConstraint::AddComparisonFilter(
  std::string_view column_name, const velox::core::ConstantTypedExpr& value,
  ComparisonOp op, const velox::core::ITypedExpr* source_expr) {
  SDB_ASSERT(!_column_ranges.contains(column_name));
  auto v = ToVariant(value);
  ColumnRange range;
  switch (op) {
    case ComparisonOp::Gt:
      range = ColumnRange::LeftBound(v, false);
      break;
    case ComparisonOp::Ge:
      range = ColumnRange::LeftBound(v, true);
      break;
    case ComparisonOp::Lt:
      range = ColumnRange::RightBound(v, false);
      break;
    case ComparisonOp::Le:
      range = ColumnRange::RightBound(v, true);
      break;
    case ComparisonOp::None:
      SDB_ASSERT(false, "AddComparisonFilter called with ComparisonOp::None");
      break;
  }
  _column_ranges.emplace(column_name, std::move(range));
  _source_exprs[std::string(column_name)].insert(source_expr);
}

std::optional<KeyConstraint> KeyConstraint::TryIntersect(
  const KeyConstraint& lhs, const KeyConstraint& rhs) {
  SDB_ASSERT(lhs._pk_names.data() == rhs._pk_names.data());
  auto result = KeyConstraint::MakeAny(lhs._pk_names);
  for (std::string_view pk_name : lhs._pk_names) {
    const auto* lhs_f = lhs.FindColumnRange(pk_name);
    const auto* rhs_f = rhs.FindColumnRange(pk_name);
    if (!lhs_f && !rhs_f) {
      continue;
    }
    if (!lhs_f) {
      result._column_ranges.emplace(pk_name, *rhs_f);
      continue;
    }
    if (!rhs_f) {
      result._column_ranges.emplace(pk_name, *lhs_f);
      continue;
    }
    auto merged_range = lhs_f->IntersectWith(*rhs_f);
    if (!merged_range) {
      // e.g. [1,1] AND [2,2] -> contradiction
      return {};
    }
    result._column_ranges.emplace(pk_name, *merged_range);
  }
  MergeSourceExprs(result._source_exprs, lhs._source_exprs);
  MergeSourceExprs(result._source_exprs, rhs._source_exprs);
  return result;
}

// TODO looks like wrong
std::optional<KeyConstraint> KeyConstraint::TryUnion(const KeyConstraint& lhs,
                                                     const KeyConstraint& rhs) {
  SDB_ASSERT(lhs._pk_names.data() == rhs._pk_names.data());

  std::string_view differing_col;
  int diff_count = 0;
  for (std::string_view pk_name : lhs._pk_names) {
    const ColumnRange* lf = lhs.FindColumnRange(pk_name);
    const ColumnRange* rf = rhs.FindColumnRange(pk_name);
    if (!lf && !rf) {
      continue;
    }
    if (!lf || !rf || *lf != *rf) {
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

  const ColumnRange* lf = lhs.FindColumnRange(differing_col);
  const ColumnRange* rf = rhs.FindColumnRange(differing_col);

  // One side unconstrained on this column -> union removes the constraint.
  if (!lf || !rf) {
    result._column_ranges.erase(differing_col);
    return result;
  }

  // Both have a range -> merge only if they overlap (no gap).
  auto merged =
    lf->OverlapsWith(*rf) ? std::optional{lf->UnionWith(*rf)} : std::nullopt;
  if (!merged) {
    return std::nullopt;
  }

  auto it = result._column_ranges.find(differing_col);
  SDB_ASSERT(it != result._column_ranges.end());
  it->second = *merged;
  return result;
}

std::vector<ResolvedPoint> ToResolvedPoints(
  const std::vector<KeyConstraint>& points, const velox::RowType& pk_type) {
  std::vector<ResolvedPoint> result;
  result.reserve(points.size());
  for (const auto& p : points) {
    ResolvedPoint sp;
    sp.reserve(pk_type.size());
    for (const auto& name : pk_type.names()) {
      const auto* filter = p.FindColumnRange(name);
      SDB_ASSERT(filter != nullptr, "pk column not found in specific point");
      SDB_ASSERT(filter->HasLeft());
      sp.push_back(filter->left_value);
    }
    result.push_back(std::move(sp));
  }
  return result;
}

std::vector<ResolvedRange> ToResolvedRanges(
  const std::vector<KeyConstraint>& ranges, const velox::RowType& pk_type) {
#ifdef SDB_DEV
  for (size_t i = 0; i < ranges.size(); ++i) {
    for (size_t j = i + 1; j < ranges.size(); ++j) {
      SDB_ASSERT(!KeyConstraint::TryIntersect(ranges[i], ranges[j]).has_value(),
                 "Resolved Ranges must be non-overlapping");
    }
  }

  SDB_ASSERT(
    !absl::c_all_of(
      ranges, [](const KeyConstraint& kc) { return kc.IsSpecificPoint(); }),
    "Specific points should prepared separately for efficiency reason");
#endif

  std::vector<ResolvedRange> result;
  result.reserve(ranges.size());
  for (const auto& key_contraint : ranges) {
    const size_t prefix_size = key_contraint.RangePrefixSize();
    SDB_ASSERT(prefix_size > 0,
               "ToSpecificRanges: constraint must have PrefixSize() >= 1");
    const size_t range_column_index = prefix_size - 1;

    ResolvedRange resolved_range;

    // Columns 0..range_column_index-1 form the equality prefix.
    for (size_t i = 0; i < range_column_index; ++i) {
      const ColumnRange* column_range =
        key_contraint.FindColumnRange(pk_type.nameOf(i));
      SDB_ASSERT(column_range && column_range->IsPoint());
      resolved_range.prefix.push_back(column_range->left_value);
    }

    // Column range_column_index is the range column.
    const ColumnRange* column_range =
      key_contraint.FindColumnRange(pk_type.nameOf(range_column_index));
    SDB_ASSERT(column_range);
    resolved_range.range_col = *column_range;

    result.push_back(std::move(resolved_range));
  }

  return result;
}

std::vector<KeyConstraint> ExtractFilterExpr(
  const velox::core::TypedExprPtr& expr, std::span<const std::string> pk_names,
  bool negated) {
  std::vector<KeyConstraint> pts;
  if (!expr->isCallKind()) {
    pts = AnyKeyConstraint(pk_names);
  } else {
    const auto* func_call = expr->asUnchecked<velox::core::CallTypedExpr>();
    if (IsCallOf(func_call, "_eq")) {
      pts = ExtractFilterEq(func_call, pk_names, negated);
    } else if (IsCallOf(func_call, "_neq")) {
      pts = ExtractFilterNeq(func_call, pk_names, negated);
    } else if (IsCallOf(func_call, "_in")) {
      pts = ExtractFilterIn(func_call, pk_names, negated);
    } else if (IsCallOf(func_call, "_gt") || IsCallOf(func_call, "_gte") ||
               IsCallOf(func_call, "_lt") || IsCallOf(func_call, "_lte")) {
      pts = ExtractFilterComparison(func_call, pk_names, negated);
    } else if (func_call->name() == velox::expression::kAnd) {
      // De Morgan: NOT(A AND B) = NOT(A) OR NOT(B)
      pts = negated ? ExtractFilterOr(func_call, pk_names, negated)
                    : ExtractFilterAnd(func_call, pk_names, negated);
    } else if (func_call->name() == velox::expression::kOr) {
      // De Morgan: NOT(A OR B) = NOT(A) AND NOT(B)
      pts = negated ? ExtractFilterAnd(func_call, pk_names, negated)
                    : ExtractFilterOr(func_call, pk_names, negated);
    } else if (IsCallOf(func_call, "_not")) {
      pts = ExtractFilterExpr(func_call->inputs()[0], pk_names, !negated);
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

  if (constraints.empty()) {
    return {ConstraintKind::None, {}, expr};
  }

  // Contradictory predicate (e.g. a < 1 AND a > 1): produce a range scan
  // with zero ranges so no rows are returned without a full scan.
  if (absl::c_all_of(constraints, [](const KeyConstraint& c) {
        return c.IsContradictory();
      })) {
    return {ConstraintKind::Ranges, {}, nullptr};
  }

  // If any constraint does not form a valid key prefix, use full scan
  if (absl::c_any_of(constraints, [](const KeyConstraint& c) {
        return c.RangePrefixSize() == 0;
      })) {
    return {ConstraintKind::None, {}, expr};
  }

  if (absl::c_all_of(constraints, [](const KeyConstraint& p) {
        return p.IsSpecificPoint();
      })) {
    containers::FlatHashSet<const velox::core::ITypedExpr*> sources;
    for (const auto& point : constraints) {
      for (const auto& [column_name, source_exprs] : point.GetSourceExprs()) {
        sources.insert(source_exprs.begin(), source_exprs.end());
      }
    }
    return {ConstraintKind::Points, std::move(constraints),
            RewriteExpr(expr, sources)};
  }

  // Normalize: the sweep may produce multiple KCs that share the same scan
  // prefix (equality columns 0..K-1 plus the range column K) but differ only
  // in trailing columns beyond K.  All such KCs map to the identical RocksDB
  // range scan; keep one representative and let remaining_filter handle the
  // rest.  Two KCs are "scan-equivalent" iff their RangePrefixSize() agrees
  // and the ColumnRange for every column in 0..prefix_size-1 is equal.
  {
    static const ColumnRange kUnconstr{};
    auto scan_equivalent = [&](const KeyConstraint& a, const KeyConstraint& b) {
      const size_t n = a.RangePrefixSize();
      if (n != b.RangePrefixSize()) {
        return false;
      }
      for (size_t i = 0; i < n; ++i) {
        const ColumnRange* ca = a.FindColumnRange(pk_names[i]);
        const ColumnRange* cb = b.FindColumnRange(pk_names[i]);
        const ColumnRange& ra = ca ? *ca : kUnconstr;
        const ColumnRange& rb = cb ? *cb : kUnconstr;
        if (!(ra == rb)) {  // TODO implement three way comparison
          return false;
        }
      }
      return true;
    };
    constraints.erase(
      std::unique(constraints.begin(), constraints.end(), scan_equivalent),
      constraints.end());
  }

  // SDB_PRINT("-------------");
  // for (const auto& c : constraints) {
  //   SDB_PRINT("  ", c.toString());
  // }

  // If the constraints' first-column ranges together cover (-inf, +inf) with
  // no gap, the range scan reads every row -- equivalent to a full scan.
  // Detect this by checking that sorted disjoint ranges tile the whole axis:
  // first has no left bound, last has no right bound, and every consecutive
  // pair is adjacent (right of prev and left of next share a value with
  // complementary or equal-inclusive endpoints, leaving no uncovered point).
  {
    std::vector<const ColumnRange*> first_column_ranges;
    first_column_ranges.reserve(constraints.size());
    for (const auto& constraint : constraints) {
      first_column_ranges.push_back(constraint.FindColumnRange(pk_names[0]));
    }

    const bool first_unbounded =
      !first_column_ranges.front() || !first_column_ranges.front()->HasLeft();
    const bool last_unbounded =
      !first_column_ranges.back() || !first_column_ranges.back()->HasRight();
    bool contiguous = first_unbounded && last_unbounded;
    for (size_t i = 1; contiguous && i < first_column_ranges.size(); ++i) {
      const ColumnRange* prev = first_column_ranges[i - 1];
      const ColumnRange* curr = first_column_ranges[i];
      // prev must have a right bound and curr a left bound with the same
      // value; together they must not leave the shared point uncovered
      // (i.e. not both exclusive).
      if (!prev || !prev->HasRight() || !curr || !curr->HasLeft() ||
          !(prev->right_value == curr->left_value) ||
          (!prev->IsRightInclusive() && !curr->IsLeftInclusive())) {
        contiguous = false;
      }
    }
    if (contiguous) {
      return {ConstraintKind::None, {}, expr};
    }
  }

  // Collect rewrite sources from every column in each constraint's prefix
  // (all K equality columns + the range column), not just the first PK column.
  containers::FlatHashSet<const velox::core::ITypedExpr*> sources;
  for (const auto& contraint : constraints) {
    const size_t prefix = contraint.RangePrefixSize();

    // Check if any constrains columns beyond its range prefix. If so, the
    // scan covers a superset of the intended rows, and the beyond-prefix
    // predicates must be post-filtered.  Because different KCs may have
    // different beyond-prefix conditions, we cannot form a single shared
    // remaining filter by stripping only the prefix sources -- the stripped
    // expression would combine conditions from different ranges incorrectly
    // (e.g. OR(b<2, b>2) instead of (a<2 AND b<2) OR (a>2 AND b>2)).
    // Keep the original expression as remaining_filter in this case.
    const bool has_beyond_prefix_constraints = absl::c_any_of(
      pk_names.subspan(prefix), [&](const std::string& column_name) {
        return contraint.FindColumnRange(column_name) != nullptr;
      });
    if (!has_beyond_prefix_constraints) {
      for (size_t i = 0; i < prefix; ++i) {
        const auto& col_exprs = contraint.GetSourceExprs(pk_names[i]);
        sources.insert(col_exprs.begin(), col_exprs.end());
      }
    }
  }

  velox::core::TypedExprPtr remaining = RewriteExpr(expr, sources);
  return {ConstraintKind::Ranges, std::move(constraints), std::move(remaining)};
}

}  // namespace sdb::connector
