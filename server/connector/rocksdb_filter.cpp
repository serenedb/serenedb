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
#include "basics/system-compiler.h"

namespace sdb::connector {
namespace {

velox::variant ToVariant(const velox::core::ConstantTypedExpr& expr) {
  if (!expr.hasValueVector()) {
    return expr.value();
  }
  return expr.valueVector()->variantAt(0);
}

std::vector<KeyBounds> MergeKeyConstraints(std::vector<KeyBounds>);

void MergeSourceExprs(KeyBounds::SourceExprsMap& dst,
                      const KeyBounds::SourceExprsMap& src);

std::vector<KeyBounds> AnyKeyConstraint(
  const std::span<const std::string> names) {
  return {KeyBounds::MakeAny(names)};
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

// Helper: produces two key bounds for a != v -> a < v OR a > v.
std::vector<KeyBounds> MakeNeqConstraints(
  std::string_view col_name, const velox::core::ConstantTypedExpr& val,
  const velox::core::ITypedExpr* source,
  std::span<const std::string> pk_names) {
  auto less_constraint = KeyBounds::MakeAny(pk_names);
  less_constraint.AddComparisonFilter(col_name, val, ComparisonOp::Lt, source);

  auto greater_constraint = KeyBounds::MakeAny(pk_names);
  greater_constraint.AddComparisonFilter(col_name, val, ComparisonOp::Gt,
                                         source);
  return {std::move(less_constraint), std::move(greater_constraint)};
}

std::vector<KeyBounds> ExtractFilterEq(
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
  auto p = KeyBounds::MakeAny(pk_names);
  p.AddEqFilter(field_access->name(), *const_val, func_call);
  return {p};
}

std::vector<KeyBounds> ExtractFilterIsNull(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names) {
  SDB_ASSERT(func_call->inputs().size() == 1);
  if (!func_call->inputs()[0]->isFieldAccessKind()) {
    return AnyKeyConstraint(pk_names);
  }
  auto field_access =
    basics::downCast<velox::core::FieldAccessTypedExpr>(func_call->inputs()[0]);
  if (!absl::c_linear_search(pk_names, field_access->name())) {
    return AnyKeyConstraint(pk_names);
  }
  velox::core::ConstantTypedExpr null_val{
    field_access->type(), velox::variant::null(field_access->type()->kind())};
  auto p = KeyBounds::MakeAny(pk_names);
  p.AddEqFilter(field_access->name(), null_val, func_call);
  return {p};
}

std::vector<KeyBounds> ExtractFilterNeq(
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
    auto kc = KeyBounds::MakeAny(pk_names);
    kc.AddEqFilter(field_access->name(), *const_val, func_call);
    return {std::move(kc)};
  }
  return MakeNeqConstraints(field_access->name(), *const_val, func_call,
                            pk_names);
}

std::vector<KeyBounds> ExtractFilterComparison(
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

  auto constraint = KeyBounds::MakeAny(pk_names);
  constraint.AddComparisonFilter(field_access->name(), *const_val, op,
                                 func_call);
  return {std::move(constraint)};
}

std::vector<KeyBounds> ExtractFilterIn(
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
    std::vector<KeyBounds> points;
    points.reserve(size);
    for (velox::vector_size_t i = 0; i < size; ++i) {
      velox::core::ConstantTypedExpr elem_const{
        velox::BaseVector::wrapInConstant(1, offset + i, elements)};
      auto p = KeyBounds::MakeAny(pk_names);
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
    auto constraint = KeyBounds::MakeAny(pk_names);
    constraint.AddComparisonFilter(col, val, op, func_call);
    return constraint;
  };

  std::vector<KeyBounds> result;
  result.reserve(size + 1);
  result.push_back(make_constraint(ComparisonOp::Lt, elem(0)));
  for (velox::vector_size_t i = 1; i < size; ++i) {
    if (auto kc = KeyBounds::TryIntersect(
          make_constraint(ComparisonOp::Gt, elem(i - 1)),
          make_constraint(ComparisonOp::Lt, elem(i)))) {
      result.push_back(std::move(*kc));
    }
  }
  result.push_back(make_constraint(ComparisonOp::Gt, elem(size - 1)));
  return result;
}

std::vector<KeyBounds> ExtractFilterAnd(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names, bool negated) {
  SDB_ASSERT(!func_call->inputs().empty());

  // Cartesian product of all children's point sets, intersecting each tuple.
  // An unconstrained child (AnyPoint -- empty filters) acts as identity because
  // Intersect(P, {}) == P, so no special-casing is needed.
  std::vector<KeyBounds> result =
    ExtractFilterExpr(func_call->inputs()[0], pk_names, negated);
  for (size_t i = 1; i < func_call->inputs().size(); ++i) {
    // Propagate contradictory: AND(contradictory, anything) = contradictory.
    if (absl::c_all_of(result,
                       [](const KeyBounds& c) { return c.IsEmpty(); })) {
      return result;
    }
    const auto rhs_pts =
      ExtractFilterExpr(func_call->inputs()[i], pk_names, negated);
    std::vector<KeyBounds> next;
    next.reserve(result.size() * rhs_pts.size());
    bool had_contradiction = false;
    for (const auto& lhs : result) {
      for (const auto& rhs : rhs_pts) {
        if (auto merged = KeyBounds::TryIntersect(lhs, rhs)) {
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
        return {KeyBounds::MakeContradictory(pk_names)};
      }
      // All intersections produced unconstrained results: AND is unconstrained.
      return AnyKeyConstraint(pk_names);
    }
  }
  return result;
}

std::vector<KeyBounds> ExtractFilterOr(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names, bool negated) {
  SDB_ASSERT(!func_call->inputs().empty());

  std::vector<KeyBounds> result;
  for (const auto& input : func_call->inputs()) {
    auto constraints = ExtractFilterExpr(input, pk_names, negated);

    if (constraints.empty() ||
        absl::c_any_of(constraints, [](const KeyBounds& p) {
          return p.IsUnconstrained();
        })) {
      return AnyKeyConstraint(pk_names);
    }

    if (absl::c_all_of(constraints,
                       [](const KeyBounds& p) { return p.IsEmpty(); })) {
      continue;
    }

    result.insert(result.end(), std::make_move_iterator(constraints.begin()),
                  std::make_move_iterator(constraints.end()));
  }
  // All branches were contradictory -> OR is itself contradictory.
  if (result.empty()) {
    return {KeyBounds::MakeContradictory(pk_names)};
  }
  return MergeKeyConstraints(std::move(result));
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

  if (call->name() == velox::expression::kAnd) {
    std::erase_if(new_inputs, [](auto expr) { return !expr; });
    if (new_inputs.size() == 0) {
      return {};
    }
    if (new_inputs.size() == 1) {
      return new_inputs[0];
    }
  }

  if (call->name() == velox::expression::kOr) {
    // If ALL branches became true (null), the whole OR is trivially true.
    if (absl::c_all_of(new_inputs, [](const auto& e) { return !e; })) {
      return {};
    }
    // For OR, we cannot safely produce a simplified remaining filter when some
    // but not all branches were captured: different branches select different
    // row subsets, so stripping captured branches would lose per-branch
    // conditions (e.g. `score=200` from `(city='berlin' AND score=200) OR
    // city='tokyo'`). Return the original expression as the post-filter.
    return expr;
  }

  // For any other call (e.g. "not"), if any input was rewritten to null (i.e.
  // fully captured), treat the whole expression as captured too.
  if (absl::c_any_of(new_inputs, [](const auto& e) { return !e; })) {
    return {};
  }

  return std::make_shared<velox::core::CallTypedExpr>(
    expr->type(), std::move(new_inputs), call->name());
}

// TODO(mkornaukhov) separate this algorithms and add unit tests for it.
// Sorts constraints lexicographically by each PK column's left bound in order.
// This groups constraints that share the same prefix columns and orders the
// differing column so overlapping ranges become adjacent.
// ── Sweep helpers ────────────────────────────────────────────────────────────

// One atomic segment on a single dimension's axis.
// is_point=false -> open interval (left, right); has_left/has_right mark
// whether the endpoint is finite. is_point=true -> closed singleton {left}.
// Atoms are represented as ColumnRanges: points use Point(), open intervals
// use Bounded/LeftBound/RightBound with exclusive endpoints, and the fully
// unconstrained atom is an empty ColumnRange{} (no flags set).
using Atom = ColumnRange;

// Lightweight constraint representation used during the recursive sweep.
struct SweepRegion {
  std::span<const std::string> pk_names;
  containers::FlatHashMap<std::string, ColumnRange> column_ranges;
  KeyBounds::SourceExprsMap source_exprs;
};

// Builds the atom sequence for a sorted, deduplicated list of event points.
// Emits: (-inf, p0), {p0}, (p0,p1), {p1}, ..., {pN}, (pN, +inf).
// If event_points is empty, emits one unconstrained open atom.
std::vector<Atom> BuildAtoms(const std::vector<velox::variant>& pts) {
  if (pts.empty()) {
    return {ColumnRange{}};  // unconstrained (-inf, +inf)
  }
  std::vector<Atom> atoms;
  atoms.reserve(2 * pts.size() + 1);
  atoms.push_back(ColumnRange::RightBound(pts[0], false));
  for (size_t i = 0; i + 1 < pts.size(); ++i) {
    atoms.push_back(ColumnRange::Point(pts[i]));
    atoms.push_back(ColumnRange::Bounded(pts[i], false, pts[i + 1], false));
  }
  atoms.push_back(ColumnRange::Point(pts.back()));
  atoms.push_back(ColumnRange::LeftBound(pts.back(), false));
  return atoms;
}

// Returns true iff column_range contains atom.
// Fast path: by sweep invariant, overlap <-> full containment.
// Under SDB_DEV the full check is also
// performed and must agree.
bool AtomContainedBy(const Atom& atom, const ColumnRange& column_range) {
#ifdef SDB_DEV
  bool fully_contained;
  if (atom.IsPoint()) {
    const velox::variant& p = atom.LeftValue();
    fully_contained =
      (!column_range.HasLeft() || column_range.LeftValue() < p ||
       (column_range.LeftValue() == p && column_range.IsLeftInclusive())) &&
      (!column_range.HasRight() || p < column_range.RightValue() ||
       (column_range.RightValue() == p && column_range.IsRightInclusive()));
  } else {
    // Open interval (l, r) ⊆ column_range iff column_range starts at or
    // before l AND ends at or after r. If atom is unbounded on a side,
    // column_range must also be unbounded on that side.
    fully_contained =
      (!atom.HasLeft() ? !column_range.HasLeft()
                       : (!column_range.HasLeft() ||
                          !(atom.LeftValue() < column_range.LeftValue()))) &&
      (!atom.HasRight() ? !column_range.HasRight()
                        : (!column_range.HasRight() ||
                           !(column_range.RightValue() < atom.RightValue())));
  }
  const bool overlaps = atom.OverlapsWith(column_range);
  SDB_ASSERT(overlaps == fully_contained,
             "AtomContainedBy: sweep invariant violated");
  return overlaps;
#else
  return atom.OverlapsWith(column_range);
#endif
}

// Reconstructs the merged ColumnRange for a contiguous run of atoms.
// Left bound comes from the first atom, right bound from the last.
ColumnRange UniteAtomsRange(const Atom& first, const Atom& last) {
  if (first == last) {
    return first;
  }

  const bool left_inclusive = first.IsLeftInclusive();
  const bool right_inclusive = last.IsRightInclusive();

  if (!first.HasLeft() && !last.HasRight()) {
    return ColumnRange{};
  }
  if (!first.HasLeft()) {
    return ColumnRange::RightBound(last.RightValue(), right_inclusive);
  }
  if (!last.HasRight()) {
    return ColumnRange::LeftBound(first.LeftValue(), left_inclusive);
  }
  return ColumnRange::Bounded(first.LeftValue(), left_inclusive,
                              last.RightValue(), right_inclusive);
}

// Collects, sorts, and deduplicates all boundary values from `ranges` on
// dimension `dim_col`. Missing column (unconstrained) contributes no events.
std::vector<velox::variant> CollectEventPoints(
  const std::vector<SweepRegion>& ranges, std::string_view dim_col) {
  std::vector<velox::variant> pts;
  for (const auto& key_range : ranges) {
    auto it = key_range.column_ranges.find(dim_col);
    if (it == key_range.column_ranges.end()) {
      continue;
    }
    const ColumnRange& column_range = it->second;
    if (column_range.HasLeft()) {
      pts.push_back(column_range.LeftValue());
    }
    if (column_range.HasRight()) {
      pts.push_back(column_range.RightValue());
    }
  }
  absl::c_sort(pts);
  pts.erase(std::unique(pts.begin(), pts.end()), pts.end());
  return pts;
}

// Returns a copy of sweep range with dim_col removed from col_ranges.
SweepRegion ProjectAwayDim(const SweepRegion& sweep_range,
                           std::string_view dimension_column) {
  SweepRegion out;
  out.pk_names = sweep_range.pk_names;
  out.source_exprs = sweep_range.source_exprs;
  for (const auto& [k, v] : sweep_range.column_ranges) {
    if (k != dimension_column) {
      out.column_ranges.emplace(k, v);
    }
  }
  return out;
}

// Two SweepRegions are equal on all dimensions except dim_col.
bool EqualExceptDim(const SweepRegion& a, const SweepRegion& b,
                    std::string_view dim_col) {
  for (const auto& name : a.pk_names) {
    if (name == dim_col) {
      continue;
    }
    const auto* a_cur_column =
      a.column_ranges.contains(name) ? &a.column_ranges.at(name) : nullptr;
    const auto* b_cur_column =
      b.column_ranges.contains(name) ? &b.column_ranges.at(name) : nullptr;
    if (!a_cur_column && !b_cur_column) {
      continue;
    }
    if (!a_cur_column || !b_cur_column) {
      return false;
    }
    if (!(*a_cur_column == *b_cur_column)) {
      return false;
    }
  }
  return true;
}

// Fuses adjacent atom-result pairs that are consecutive and share the same
// sub-result on all other dimensions. Reconstructs ColumnRange for dim_col.
std::vector<SweepRegion> FuseAdjacentAtoms(
  std::vector<std::pair<Atom, SweepRegion>> atom_results,
  std::string_view dim_col) {
  // Two adjacent entries can extend the same run when:
  //  - atoms alternate point/open (a skipped point means a gap -- do not
  //  merge),
  //  - they share a boundary value (or both are unbounded on the touching
  //  side),
  //  - their sub-results agree on all other dimensions.
  // Since a point has LeftValue() == RightValue(), prev.RightValue() and
  // curr.LeftValue() give the shared endpoint for both point and open atoms.
  auto can_extend_run = [&](size_t i) {
    const Atom& prev = atom_results[i - 1].first;
    const Atom& curr = atom_results[i].first;
    return prev.IsPoint() != curr.IsPoint() &&
           prev.HasRight() == curr.HasLeft() &&
           (!prev.HasRight() || prev.RightValue() == curr.LeftValue()) &&
           EqualExceptDim(atom_results[i - 1].second, atom_results[i].second,
                          dim_col);
  };

  std::vector<SweepRegion> result;
  for (size_t run_start = 0; run_start < atom_results.size();) {
    size_t run_end = run_start + 1;
    while (run_end < atom_results.size() && can_extend_run(run_end)) {
      ++run_end;
    }
    // Fuse the run [run_start, run_end).
    SweepRegion out = atom_results[run_start].second;
    const ColumnRange fused = UniteAtomsRange(atom_results[run_start].first,
                                              atom_results[run_end - 1].first);
    if (fused.HasLeft() || fused.HasRight()) {
      out.column_ranges[std::string(dim_col)] = fused;
    } else {
      out.column_ranges.erase(dim_col);
    }
    for (size_t j = run_start + 1; j < run_end; ++j) {
      MergeSourceExprs(out.source_exprs, atom_results[j].second.source_exprs);
    }
    result.push_back(std::move(out));
    run_start = run_end;
  }
  return result;
}

// Returns SweepRegions with ColumnRange entries for those dimensions.
std::vector<SweepRegion> SweepDimensions(std::vector<SweepRegion> ranges,
                                         std::span<const std::string> pk_names,
                                         size_t dim_idx) {
  if (dim_idx == pk_names.size()) {
    // Base case: all dimensions projected away. Merge source exprs.
    SweepRegion out;
    out.pk_names = pk_names;
    for (const auto& range : ranges) {
      MergeSourceExprs(out.source_exprs, range.source_exprs);
    }
    return {std::move(out)};
  }

  const std::string& dim_col = pk_names[dim_idx];
  const auto events = CollectEventPoints(ranges, dim_col);
  const auto atoms = BuildAtoms(events);

  std::vector<std::pair<Atom, SweepRegion>> atom_results;
  for (const auto& atom : atoms) {
    std::vector<SweepRegion> active;
    for (const auto& sweep_range : ranges) {
      auto it = sweep_range.column_ranges.find(dim_col);
      // Absent from map means unconstrained (-inf, +inf): always contains atom.
      if (it == sweep_range.column_ranges.end() ||
          AtomContainedBy(atom, it->second)) {
        active.push_back(ProjectAwayDim(sweep_range, dim_col));
      }
    }
    if (active.empty()) {
      continue;
    }
    auto sub = SweepDimensions(std::move(active), pk_names, dim_idx + 1);
    for (auto& s : sub) {
      // (+inf, -inf) is encoded as an absence in a map
      if (atom.HasLeft() || atom.HasRight()) {
        s.column_ranges[std::string(dim_col)] = atom;
      }
      atom_results.emplace_back(atom, std::move(s));
    }
  }
  return FuseAdjacentAtoms(std::move(atom_results), dim_col);
}

// Produces a set of disjoint KeyConstraints covering exactly the union of the
// input constraints' key-spaces, using the recursive atomic-sweep algorithm.
std::vector<KeyBounds> MergeKeyConstraints(std::vector<KeyBounds> constraints) {
  if (constraints.empty()) {
    return {};
  }
  const auto pk_names = constraints[0].PKNames();

  for (const auto& c : constraints) {
    if (c.IsUnconstrained()) {
      return {KeyBounds::MakeAny(pk_names)};
    }
  }
  std::erase_if(constraints, [](const KeyBounds& c) { return c.IsEmpty(); });
  SDB_ASSERT(!constraints.empty());

  if (constraints.size() == 1) {
    return constraints;
  }

  std::vector<SweepRegion> inputs;
  inputs.reserve(constraints.size());
  for (const auto& kc : constraints) {
    SweepRegion sr;
    sr.pk_names = pk_names;
    for (const auto& name : pk_names) {
      if (const auto* cr = kc.FindColumnRange(name)) {
        sr.column_ranges.emplace(name, *cr);
      }
    }
    sr.source_exprs = kc.GetSourceExprs();
    inputs.push_back(std::move(sr));
  }

  auto swept = SweepDimensions(std::move(inputs), pk_names, 0);

  std::vector<KeyBounds> result;
  result.reserve(swept.size());
  for (auto& sr : swept) {
    result.push_back(KeyBounds::BuildFromRanges(
      pk_names, std::move(sr.column_ranges), std::move(sr.source_exprs)));
  }
  return result;
}

// Merges all per-column source expression sets from `src` into `dst`.
void MergeSourceExprs(KeyBounds::SourceExprsMap& dst,
                      const KeyBounds::SourceExprsMap& src) {
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
    return variant_str(_left_value);
  }
  std::string result;
  if (!HasLeft()) {
    absl::StrAppend(&result, "(-inf");
  } else {
    absl::StrAppend(&result, IsLeftInclusive() ? "[" : "(",
                    variant_str(_left_value));
  }
  absl::StrAppend(&result, ", ");
  if (!HasRight()) {
    absl::StrAppend(&result, "+inf)");
  } else {
    absl::StrAppend(&result, variant_str(_right_value),
                    IsRightInclusive() ? "]" : ")");
  }
  return result;
}

std::string KeyBounds::toString() const {
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

size_t KeyBounds::RangePrefixSize() const noexcept {
  for (size_t k = 0; k < _pk_names.size(); ++k) {
    const ColumnRange* column_range = FindColumnRange(_pk_names[k]);
    if (!column_range) {
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

bool KeyBounds::IsResolvedPoint() const {
  return absl::c_all_of(_pk_names, [&](std::string_view name) {
    auto it = _column_ranges.find(name);
    if (it == _column_ranges.end()) {
      return false;
    }
    return it->second.IsPoint();
  });
}

KeyBounds KeyBounds::BuildFromRanges(
  std::span<const std::string> pk_names,
  containers::FlatHashMap<std::string, ColumnRange> ranges,
  SourceExprsMap source_exprs) {
  KeyBounds constraint{pk_names};
  constraint._column_ranges = std::move(ranges);
  constraint._source_exprs = std::move(source_exprs);
  return constraint;
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
    if (a._left_value < b._left_value) {
      return b;
    }
    if (b._left_value < a._left_value) {
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
    if (b._right_value < a._right_value) {
      return b;
    }
    if (a._right_value < b._right_value) {
      return a;
    }
    return a.IsRightInclusive() ? b : a;  // exclusive is more restrictive
  };

  const ColumnRange& ls = pick_tighter_left(*this, other);
  const ColumnRange& rs = pick_tighter_right(*this, other);

  if (ls.HasLeft() && rs.HasRight()) {
    if (rs._right_value < ls._left_value) {
      return std::nullopt;
    }
    if (ls._left_value == rs._right_value &&
        (!ls.IsLeftInclusive() || !rs.IsRightInclusive())) {
      return std::nullopt;
    }
  }

  // Mask the relevant flag bits from each source and copy the values.
  // Unset left_value/right_value are always default-constructed, so
  // copying them unconditionally is safe.
  ColumnRange result;
  result._flags |= ls._flags & (kLeftBounded | kLeftInclusive);
  result._flags |= rs._flags & (kRightBounded | kRightInclusive);
  result._left_value = ls._left_value;
  result._right_value = rs._right_value;
  return result;
}

bool ColumnRange::OverlapsWith(const ColumnRange& other) const {
  return IntersectWith(other).has_value();
}

bool ColumnRange::LeftBoundLessThan(const ColumnRange& other) const noexcept {
  if (!HasLeft()) {
    return other.HasLeft();  // -inf < bounded, -inf == -inf
  }
  if (!other.HasLeft()) {
    return false;  // bounded >= -inf
  }
  if (_left_value < other._left_value) {
    return true;
  }
  if (other._left_value < _left_value) {
    return false;
  }
  // Same value: inclusive (starts earlier) sorts before exclusive.
  return IsLeftInclusive() && !other.IsLeftInclusive();
}

const containers::FlatHashSet<const velox::core::ITypedExpr*>&
KeyBounds::GetSourceExprs(std::string_view column_name) const noexcept {
  static const containers::FlatHashSet<const velox::core::ITypedExpr*> kEmpty;
  auto it = _source_exprs.find(column_name);
  return it != _source_exprs.end() ? it->second : kEmpty;
}

const ColumnRange* KeyBounds::FindColumnRange(
  std::string_view column_name) const {
  auto it = _column_ranges.find(column_name);
  return it != _column_ranges.end() ? &it->second : nullptr;
}

void KeyBounds::AddEqFilter(std::string_view column_name,
                            const velox::core::ConstantTypedExpr& value,
                            const velox::core::ITypedExpr* source_expr) {
  SDB_ASSERT(!_column_ranges.contains(column_name));
  auto v = ToVariant(value);
  _column_ranges.emplace(column_name, ColumnRange::Point(std::move(v)));
  _source_exprs[std::string(column_name)].insert(source_expr);
}

void KeyBounds::AddComparisonFilter(
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

std::optional<KeyBounds> KeyBounds::TryIntersect(const KeyBounds& lhs,
                                                 const KeyBounds& rhs) {
  SDB_ASSERT(lhs._pk_names.data() == rhs._pk_names.data());
  auto result = KeyBounds::MakeAny(lhs._pk_names);
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

std::vector<ResolvedPoint> ToResolvedPoints(
  const std::vector<KeyBounds>& points,
  std::span<const std::string> column_names) {
  std::vector<ResolvedPoint> result;
  result.reserve(points.size());
  for (const auto& p : points) {
    ResolvedPoint sp;
    sp.reserve(column_names.size());
    for (const auto& name : column_names) {
      const auto* filter = p.FindColumnRange(name);
      SDB_ASSERT(filter != nullptr, "pk column not found in specific point");
      SDB_ASSERT(filter->HasLeft());
      sp.push_back(filter->LeftValue());
    }
    result.push_back(std::move(sp));
  }
  return result;
}

std::vector<ResolvedRange> ToDisjointRanges(
  const std::vector<KeyBounds>& ranges, const velox::RowType& pk_type) {
  if (ranges.empty()) {
    return {ResolvedRange::Conflicting()};
  }
#ifdef SDB_DEV
  for (size_t i = 0; i < ranges.size(); ++i) {
    for (size_t j = i + 1; j < ranges.size(); ++j) {
      SDB_ASSERT(!KeyBounds::TryIntersect(ranges[i], ranges[j]),
                 "Resolved ranges must be non-overlapping");
    }
  }

  SDB_ASSERT(
    !absl::c_all_of(ranges,
                    [](const KeyBounds& kc) { return kc.IsResolvedPoint(); }),
    "Specific points should prepared separately for efficiency reason");
#endif

  std::vector<ResolvedRange> result;
  result.reserve(ranges.size());
  for (const auto& key_contraint : ranges) {
    const auto prefix_size = key_contraint.RangePrefixSize();
    SDB_ASSERT(prefix_size > 0);
    const auto range_column_index = prefix_size - 1;

    ResolvedRange resolved_range;

    // Columns 0..range_column_index-1 form the equality prefix.
    for (size_t i = 0; i < range_column_index; ++i) {
      const auto* column_range =
        key_contraint.FindColumnRange(pk_type.nameOf(i));
      SDB_ASSERT(column_range && column_range->IsPoint());
      resolved_range.prefix.push_back(column_range->LeftValue());
    }

    // Column range_column_index is the range column.
    const auto* column_range =
      key_contraint.FindColumnRange(pk_type.nameOf(range_column_index));
    SDB_ASSERT(column_range);
    resolved_range.range_column = *column_range;

    result.push_back(std::move(resolved_range));
  }

  return result;
}

std::vector<KeyBounds> ExtractFilterExpr(const velox::core::TypedExprPtr& expr,
                                         std::span<const std::string> pk_names,
                                         bool negated) {
  std::vector<KeyBounds> key_bounds;
  if (!expr->isCallKind()) {
    key_bounds = AnyKeyConstraint(pk_names);
  } else {
    const auto* func_call = expr->asUnchecked<velox::core::CallTypedExpr>();
    if (IsCallOf(func_call, "_eq")) {
      key_bounds = ExtractFilterEq(func_call, pk_names, negated);
    } else if (IsCallOf(func_call, "_neq")) {
      key_bounds = ExtractFilterNeq(func_call, pk_names, negated);
    } else if (IsCallOf(func_call, "_in")) {
      key_bounds = ExtractFilterIn(func_call, pk_names, negated);
    } else if (IsCallOf(func_call, "_gt") || IsCallOf(func_call, "_gte") ||
               IsCallOf(func_call, "_lt") || IsCallOf(func_call, "_lte")) {
      key_bounds = ExtractFilterComparison(func_call, pk_names, negated);
    } else if (IsCallOf(func_call, "_isnull") ||
               IsCallOf(func_call, "_is_null")) {
      // TODO: NOT NULL
      key_bounds = ExtractFilterIsNull(func_call, pk_names);
    } else if (func_call->name() == velox::expression::kAnd) {
      // De Morgan: NOT(A AND B) = NOT(A) OR NOT(B)
      key_bounds = negated ? ExtractFilterOr(func_call, pk_names, negated)
                           : ExtractFilterAnd(func_call, pk_names, negated);
    } else if (func_call->name() == velox::expression::kOr) {
      // De Morgan: NOT(A OR B) = NOT(A) AND NOT(B)
      key_bounds = negated ? ExtractFilterAnd(func_call, pk_names, negated)
                           : ExtractFilterOr(func_call, pk_names, negated);
    } else if (IsCallOf(func_call, "_not")) {
      key_bounds =
        ExtractFilterExpr(func_call->inputs()[0], pk_names, !negated);
    } else {
      key_bounds = AnyKeyConstraint(pk_names);
    }
  }
  return key_bounds;
}

ExtractAndRewriteResult ExtractAndRewriteFilterExpr(
  const velox::core::TypedExprPtr& expr,
  std::span<const std::string> pk_names) {
  auto constraints = ExtractFilterExpr(expr, pk_names);

  if (constraints.empty()) {
    return {ConstraintKind::None, {}, expr};
  }

  // Contradictory predicate (e.g. a < 1 AND a > 1): no rows can match,
  // so produce zero ranges to skip reading entirely.
  if (absl::c_all_of(constraints,
                     [](const KeyBounds& c) { return c.IsEmpty(); })) {
    return {ConstraintKind::Ranges, {}, nullptr};
  }

  // If any constraint does not form a valid key prefix, use full scan
  if (absl::c_any_of(constraints, [](const KeyBounds& c) {
        return c.RangePrefixSize() == 0;
      })) {
    return {ConstraintKind::None, {}, expr};
  }

  if (absl::c_all_of(constraints,
                     [](const KeyBounds& p) { return p.IsResolvedPoint(); })) {
    containers::FlatHashSet<const velox::core::ITypedExpr*> sources;
    for (const auto& point : constraints) {
      for (const auto& [column_name, source_exprs] : point.GetSourceExprs()) {
        sources.insert(source_exprs.begin(), source_exprs.end());
      }
    }
    return {ConstraintKind::Points, std::move(constraints),
            RewriteExpr(expr, sources)};
  }

  // Normalize: multiple constraints may share the same prefix ranges but
  // differ in suffix PK columns. They produce identical RocksDB range scans,
  // so keep one representative and let remaining_filter handle the rest.
  // Two constraints are scan-equivalent iff their prefix size matches and
  // every column range in the prefix is equal.
  auto scan_equivalent = [&](const KeyBounds& left_bound,
                             const KeyBounds& right_bound) {
    const size_t prefix_size = left_bound.RangePrefixSize();
    if (prefix_size != right_bound.RangePrefixSize()) {
      return false;
    }
    for (size_t i = 0; i < prefix_size; ++i) {
      const ColumnRange* left_column_range =
        left_bound.FindColumnRange(pk_names[i]);
      const ColumnRange* right_column_range =
        right_bound.FindColumnRange(pk_names[i]);
      if (left_column_range == right_column_range) {
        continue;
      }
      if (!left_column_range || !right_column_range ||
          *left_column_range != *right_column_range) {
        return false;
      }
    }
    return true;
  };
  constraints.erase(
    std::unique(constraints.begin(), constraints.end(), scan_equivalent),
    constraints.end());

  // If the prefix ranges together cover (-inf, +inf) with no gaps,
  // the scan reads every row -- just do a full scan instead.
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
    const auto* prev = first_column_ranges[i - 1];
    const auto* curr = first_column_ranges[i];
    // Adjacent ranges must meet at the same value with no gap
    // (at least one endpoint must include the shared point).
    if (!prev || !prev->HasRight() || !curr || !curr->HasLeft() ||
        prev->RightValue() != curr->LeftValue() ||
        (!prev->IsRightInclusive() && !curr->IsLeftInclusive())) {
      contiguous = false;
    }
  }
  if (contiguous) {
    return {ConstraintKind::None, {}, expr};
  }

  // Collect source expressions from prefix columns so they can be stripped
  // from the remaining filter (the scan will enforce them).
  // If a constraint also covers suffix PK columns, skip it -- stripping
  // prefix sources would break correlations between prefix and suffix
  // conditions, e.g. OR(not_pk<2, not_pk>2) instead of (pk<2 AND not_pk<2) OR
  // (pk>2 AND not_pk>2))
  containers::FlatHashSet<const velox::core::ITypedExpr*> sources;
  for (const auto& constraint : constraints) {
    const auto prefix = constraint.RangePrefixSize();
    const bool has_suffix_constraint = absl::c_any_of(
      pk_names.subspan(prefix), [&](const std::string& column_name) {
        return constraint.FindColumnRange(column_name) != nullptr;
      });
    if (!has_suffix_constraint) {
      for (size_t i = 0; i < prefix; ++i) {
        const auto& col_exprs = constraint.GetSourceExprs(pk_names[i]);
        sources.insert(col_exprs.begin(), col_exprs.end());
      }
    }
  }

  auto remaining = RewriteExpr(expr, sources);
  return {ConstraintKind::Ranges, std::move(constraints), std::move(remaining)};
}

void SortAndDedupPoints(std::vector<ResolvedPoint>& points) {
  absl::c_sort(points, [](const ResolvedPoint& lhs, const ResolvedPoint& rhs) {
    for (size_t i = 0; i < lhs.size(); ++i) {
      if (lhs[i] != rhs[i]) {
        return lhs[i] < rhs[i];
      }
    }
    return false;
  });
  auto [first, last] = std::ranges::unique(points);
  points.erase(first, last);
}

}  // namespace sdb::connector
