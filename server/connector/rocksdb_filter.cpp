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

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_set.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <limits>
#include <numeric>

#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/system-compiler.h"

namespace sdb::connector {
namespace {

constexpr catalog::Column::Id kInvalidId =
  std::numeric_limits<catalog::Column::Id>::max();

std::vector<KeyBounds> MergeKeyConstraints(std::vector<KeyBounds>);

void MergeSourceExprs(KeyBounds::SourceExprsMap& dst,
                      const KeyBounds::SourceExprsMap& src);

std::vector<KeyBounds> AnyKeyConstraint(
  std::span<const catalog::Column::Id> pk_ids) {
  return {KeyBounds::MakeAny(pk_ids)};
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

// Resolve a BoundColumnRefExpression to a catalog::Column::Id via the
// ColumnResolver. Returns kInvalidId if the expression does not reference
// our scan.
catalog::Column::Id TryResolveColumn(const duckdb::Expression& expr,
                                     const ColumnResolver& resolver) {
  if (expr.expression_class != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return kInvalidId;
  }
  return resolver.Resolve(expr.Cast<duckdb::BoundColumnRefExpression>());
}

// Returns the constant Value if expr is a BoundConstantExpression, nullopt
// otherwise.
const duckdb::Value* TryGetConstant(const duckdb::Expression& expr) {
  if (expr.expression_class != duckdb::ExpressionClass::BOUND_CONSTANT) {
    return nullptr;
  }
  return &expr.Cast<duckdb::BoundConstantExpression>().value;
}

// Returns true if col_id is one of the PK columns.
bool IsPKColumn(catalog::Column::Id col_id,
                std::span<const catalog::Column::Id> pk_ids) {
  return absl::c_linear_search(pk_ids, col_id);
}

// Helper: produces two key bounds for a != v -> a < v OR a > v.
std::vector<KeyBounds> MakeNeqConstraints(
  catalog::Column::Id col_id, duckdb::Value val,
  const duckdb::Expression* source,
  std::span<const catalog::Column::Id> pk_ids) {
  auto less_constraint = KeyBounds::MakeAny(pk_ids);
  less_constraint.AddComparisonFilter(col_id, val, ComparisonOp::Lt, source);

  auto greater_constraint = KeyBounds::MakeAny(pk_ids);
  greater_constraint.AddComparisonFilter(col_id, std::move(val),
                                         ComparisonOp::Gt, source);
  return {std::move(less_constraint), std::move(greater_constraint)};
}

// ── Comparison expression dispatch ──────────────────────────────────────────

std::vector<KeyBounds> ExtractFilterEq(
  const duckdb::BoundComparisonExpression& cmp,
  std::span<const catalog::Column::Id> pk_ids, const ColumnResolver& resolver,
  bool negated) {
  // Either side can be the column.
  catalog::Column::Id col_id = TryResolveColumn(*cmp.left, resolver);
  const duckdb::Value* const_val = TryGetConstant(*cmp.right);
  if (col_id == kInvalidId || !const_val) {
    col_id = TryResolveColumn(*cmp.right, resolver);
    const_val = TryGetConstant(*cmp.left);
  }
  if (col_id == kInvalidId || !const_val) {
    return AnyKeyConstraint(pk_ids);
  }
  if (!IsPKColumn(col_id, pk_ids)) {
    return AnyKeyConstraint(pk_ids);
  }
  if (negated) {
    // NOT(a = v) -> a < v OR a > v
    return MakeNeqConstraints(col_id, *const_val, &cmp, pk_ids);
  }
  auto p = KeyBounds::MakeAny(pk_ids);
  p.AddEqFilter(col_id, *const_val, &cmp);
  return {p};
}

std::vector<KeyBounds> ExtractFilterNeq(
  const duckdb::BoundComparisonExpression& cmp,
  std::span<const catalog::Column::Id> pk_ids, const ColumnResolver& resolver,
  bool negated) {
  catalog::Column::Id col_id = TryResolveColumn(*cmp.left, resolver);
  const duckdb::Value* const_val = TryGetConstant(*cmp.right);
  if (col_id == kInvalidId || !const_val) {
    col_id = TryResolveColumn(*cmp.right, resolver);
    const_val = TryGetConstant(*cmp.left);
  }
  if (col_id == kInvalidId || !const_val) {
    return AnyKeyConstraint(pk_ids);
  }
  if (!IsPKColumn(col_id, pk_ids)) {
    return AnyKeyConstraint(pk_ids);
  }
  if (negated) {
    // NOT(a != v) -> a = v
    auto kc = KeyBounds::MakeAny(pk_ids);
    kc.AddEqFilter(col_id, *const_val, &cmp);
    return {std::move(kc)};
  }
  return MakeNeqConstraints(col_id, *const_val, &cmp, pk_ids);
}

std::vector<KeyBounds> ExtractFilterComparison(
  const duckdb::BoundComparisonExpression& cmp,
  std::span<const catalog::Column::Id> pk_ids, const ColumnResolver& resolver,
  bool negated) {
  const bool field_left = TryResolveColumn(*cmp.left, resolver) != kInvalidId &&
                          TryGetConstant(*cmp.right) != nullptr;
  const bool field_right =
    TryResolveColumn(*cmp.right, resolver) != kInvalidId &&
    TryGetConstant(*cmp.left) != nullptr;
  if (!field_left && !field_right) {
    return AnyKeyConstraint(pk_ids);
  }

  const auto& field_expr = field_left ? *cmp.left : *cmp.right;
  const auto& const_expr = field_left ? *cmp.right : *cmp.left;
  auto col_id = TryResolveColumn(field_expr, resolver);
  const auto* const_val = TryGetConstant(const_expr);

  if (!IsPKColumn(col_id, pk_ids)) {
    return AnyKeyConstraint(pk_ids);
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
  switch (cmp.type) {
    case duckdb::ExpressionType::COMPARE_GREATERTHAN:
      op = ComparisonOp::Gt;
      break;
    case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      op = ComparisonOp::Ge;
      break;
    case duckdb::ExpressionType::COMPARE_LESSTHAN:
      op = ComparisonOp::Lt;
      break;
    case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
      op = ComparisonOp::Le;
      break;
    default:
      SDB_UNREACHABLE();
  }

  if (!field_left) {
    op = flip_direction(op);
  }
  if (negated) {
    op = NegateOp(op);
  }

  auto constraint = KeyBounds::MakeAny(pk_ids);
  constraint.AddComparisonFilter(col_id, *const_val, op, &cmp);
  return {std::move(constraint)};
}

// ── IS NULL ─────────────────────────────────────────────────────────────────

std::vector<KeyBounds> ExtractFilterIsNull(
  const duckdb::BoundOperatorExpression& op_expr,
  std::span<const catalog::Column::Id> pk_ids, const ColumnResolver& resolver) {
  SDB_ASSERT(op_expr.children.size() == 1);
  auto col_id = TryResolveColumn(*op_expr.children[0], resolver);
  if (col_id == kInvalidId || !IsPKColumn(col_id, pk_ids)) {
    return AnyKeyConstraint(pk_ids);
  }
  // PK / SK columns are NOT NULL by storage contract -- `col IS NULL`
  // matches zero rows. Model as a contradictory KeyBounds so downstream
  // collapses to an empty-scan result (ExtractAndRewriteFilterExpr's
  // "all constraints empty" branch). Never feeds a NULL value into a
  // ColumnRange, so the sweep-merge comparisons stay safe.
  (void)col_id;
  return {KeyBounds::MakeContradictory(pk_ids)};
}

// ── IN / NOT IN ─────────────────────────────────────────────────────────────

std::vector<KeyBounds> ExtractFilterIn(
  const duckdb::BoundOperatorExpression& op_expr,
  std::span<const catalog::Column::Id> pk_ids, const ColumnResolver& resolver,
  bool negated) {
  if (op_expr.children.empty()) {
    return AnyKeyConstraint(pk_ids);
  }
  auto col_id = TryResolveColumn(*op_expr.children[0], resolver);
  if (col_id == kInvalidId || !IsPKColumn(col_id, pk_ids)) {
    return AnyKeyConstraint(pk_ids);
  }

  // children[1..] are the constant values in the IN list.
  const auto size = op_expr.children.size() - 1;
  if (size == 0) {
    return AnyKeyConstraint(pk_ids);
  }

  // Verify all children[1..] are constants.
  for (size_t i = 1; i < op_expr.children.size(); ++i) {
    if (!TryGetConstant(*op_expr.children[i])) {
      return AnyKeyConstraint(pk_ids);
    }
  }

  if (!negated) {
    // a IN (x1, ..., xn) -> n equality constraints
    std::vector<KeyBounds> points;
    points.reserve(size);
    for (size_t i = 1; i < op_expr.children.size(); ++i) {
      const auto* val = TryGetConstant(*op_expr.children[i]);
      auto p = KeyBounds::MakeAny(pk_ids);
      // If any point is used for range scan or point lookup, it means
      // this filter fully utilized in table scan, so it's OK
      // to replace with true.
      p.AddEqFilter(col_id, *val, &op_expr);
      points.push_back(std::move(p));
    }
    return points;
  }

  // NOT IN (x1, ..., xn) -> n+1 open intervals between sorted values.
  // Sort element indices by value so we can build ordered complement intervals.
  std::vector<size_t> order(size);
  std::iota(order.begin(), order.end(), 0);
  absl::c_sort(order, [&](size_t a, size_t b) {
    return *TryGetConstant(*op_expr.children[a + 1]) <
           *TryGetConstant(*op_expr.children[b + 1]);
  });

  auto elem = [&](size_t i) -> const duckdb::Value& {
    return *TryGetConstant(*op_expr.children[order[i] + 1]);
  };
  auto make_constraint = [&](ComparisonOp comp_op, const duckdb::Value& val) {
    auto constraint = KeyBounds::MakeAny(pk_ids);
    constraint.AddComparisonFilter(col_id, val, comp_op, &op_expr);
    return constraint;
  };

  std::vector<KeyBounds> result;
  result.reserve(size + 1);
  result.push_back(make_constraint(ComparisonOp::Lt, elem(0)));
  for (size_t i = 1; i < size; ++i) {
    if (auto kc = KeyBounds::TryIntersect(
          make_constraint(ComparisonOp::Gt, elem(i - 1)),
          make_constraint(ComparisonOp::Lt, elem(i)))) {
      result.push_back(std::move(*kc));
    }
  }
  result.push_back(make_constraint(ComparisonOp::Gt, elem(size - 1)));
  return result;
}

// ── BETWEEN ─────────────────────────────────────────────────────────────────

std::vector<KeyBounds> ExtractFilterBetween(
  const duckdb::BoundBetweenExpression& between,
  std::span<const catalog::Column::Id> pk_ids, const ColumnResolver& resolver,
  bool negated) {
  auto col_id = TryResolveColumn(*between.input, resolver);
  if (col_id == kInvalidId || !IsPKColumn(col_id, pk_ids)) {
    return AnyKeyConstraint(pk_ids);
  }
  const auto* lower_val = TryGetConstant(*between.lower);
  const auto* upper_val = TryGetConstant(*between.upper);
  if (!lower_val || !upper_val) {
    return AnyKeyConstraint(pk_ids);
  }

  if (!negated) {
    // a BETWEEN lower AND upper -> lower_op(a, lower) AND upper_op(a, upper)
    auto lower_op =
      between.lower_inclusive ? ComparisonOp::Ge : ComparisonOp::Gt;
    auto upper_op =
      between.upper_inclusive ? ComparisonOp::Le : ComparisonOp::Lt;

    auto lower_kc = KeyBounds::MakeAny(pk_ids);
    lower_kc.AddComparisonFilter(col_id, *lower_val, lower_op, &between);

    auto upper_kc = KeyBounds::MakeAny(pk_ids);
    upper_kc.AddComparisonFilter(col_id, *upper_val, upper_op, &between);

    auto merged = KeyBounds::TryIntersect(lower_kc, upper_kc);
    if (!merged) {
      return {KeyBounds::MakeContradictory(pk_ids)};
    }
    return {std::move(*merged)};
  }

  // NOT BETWEEN: De Morgan -> a < lower OR a > upper
  auto lower_op = between.lower_inclusive ? ComparisonOp::Lt : ComparisonOp::Le;
  auto upper_op = between.upper_inclusive ? ComparisonOp::Gt : ComparisonOp::Ge;

  auto lt_constraint = KeyBounds::MakeAny(pk_ids);
  lt_constraint.AddComparisonFilter(col_id, *lower_val, lower_op, &between);

  auto gt_constraint = KeyBounds::MakeAny(pk_ids);
  gt_constraint.AddComparisonFilter(col_id, *upper_val, upper_op, &between);

  return {std::move(lt_constraint), std::move(gt_constraint)};
}

// ── AND / OR ────────────────────────────────────────────────────────────────

std::vector<KeyBounds> ExtractFilterAnd(
  const duckdb::BoundConjunctionExpression& conj,
  std::span<const catalog::Column::Id> pk_ids, const ColumnResolver& resolver,
  bool negated) {
  SDB_ASSERT(!conj.children.empty());

  // Cartesian product of all children's point sets, intersecting each tuple.
  // An unconstrained child (AnyPoint -- empty filters) acts as identity because
  // Intersect(P, {}) == P, so no special-casing is needed.
  std::vector<KeyBounds> result =
    ExtractFilterExpr(*conj.children[0], pk_ids, resolver, negated);
  for (size_t i = 1; i < conj.children.size(); ++i) {
    // Propagate contradictory: AND(contradictory, anything) = contradictory.
    if (absl::c_all_of(result,
                       [](const KeyBounds& c) { return c.IsEmpty(); })) {
      return result;
    }
    const auto rhs_pts =
      ExtractFilterExpr(*conj.children[i], pk_ids, resolver, negated);
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
        return {KeyBounds::MakeContradictory(pk_ids)};
      }
      // All intersections produced unconstrained results: AND is unconstrained.
      return AnyKeyConstraint(pk_ids);
    }
  }
  return result;
}

std::vector<KeyBounds> ExtractFilterOr(
  const duckdb::BoundConjunctionExpression& conj,
  std::span<const catalog::Column::Id> pk_ids, const ColumnResolver& resolver,
  bool negated) {
  SDB_ASSERT(!conj.children.empty());

  std::vector<KeyBounds> result;
  for (const auto& child : conj.children) {
    auto constraints = ExtractFilterExpr(*child, pk_ids, resolver, negated);

    if (constraints.empty() ||
        absl::c_any_of(constraints, [](const KeyBounds& p) {
          return p.IsUnconstrained();
        })) {
      return AnyKeyConstraint(pk_ids);
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
    return {KeyBounds::MakeContradictory(pk_ids)};
  }
  return MergeKeyConstraints(std::move(result));
}

// ── Rewrite ─────────────────────────────────────────────────────────────────

// Recursively rewrites `expr`, replacing any node whose address appears in
// `sources` with nullptr (= true). Returns a copy when the tree changed,
// nullptr when the whole expression was claimed.
duckdb::unique_ptr<duckdb::Expression> RewriteExpr(
  const duckdb::Expression& expr,
  const containers::FlatHashSet<const duckdb::Expression*>& sources) {
  if (sources.contains(&expr)) {
    return nullptr;
  }

  if (expr.expression_class == duckdb::ExpressionClass::BOUND_CONJUNCTION) {
    const auto& conj = expr.Cast<duckdb::BoundConjunctionExpression>();
    std::vector<duckdb::unique_ptr<duckdb::Expression>> new_children;
    bool changed = false;
    for (const auto& child : conj.children) {
      auto new_child = RewriteExpr(*child, sources);
      if (!new_child && child) {
        changed = true;
      } else if (new_child && new_child.get() != child.get()) {
        changed = true;
      }
      new_children.push_back(std::move(new_child));
    }
    if (!changed) {
      return expr.Copy();
    }

    if (expr.type == duckdb::ExpressionType::CONJUNCTION_AND) {
      std::erase_if(new_children, [](const auto& e) { return !e; });
      if (new_children.empty()) {
        return nullptr;
      }
      if (new_children.size() == 1) {
        return std::move(new_children[0]);
      }
      auto result = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
        duckdb::ExpressionType::CONJUNCTION_AND);
      result->children = std::move(new_children);
      return result;
    }

    if (expr.type == duckdb::ExpressionType::CONJUNCTION_OR) {
      // If ALL branches became true (null), the whole OR is trivially true.
      if (absl::c_all_of(new_children, [](const auto& e) { return !e; })) {
        return nullptr;
      }
      // For OR, we cannot safely produce a simplified remaining filter when
      // some but not all branches were captured: different branches select
      // different row subsets, so stripping captured branches would lose
      // per-branch conditions. Return the original expression as the
      // post-filter.
      return expr.Copy();
    }
  }

  if (expr.expression_class == duckdb::ExpressionClass::BOUND_OPERATOR) {
    const auto& op = expr.Cast<duckdb::BoundOperatorExpression>();
    std::vector<duckdb::unique_ptr<duckdb::Expression>> new_children;
    bool changed = false;
    for (const auto& child : op.children) {
      auto new_child = RewriteExpr(*child, sources);
      if (!new_child && child) {
        changed = true;
      } else if (new_child && new_child.get() != child.get()) {
        changed = true;
      }
      new_children.push_back(std::move(new_child));
    }
    if (!changed) {
      return expr.Copy();
    }
    // For NOT/IS_NULL, if any input was fully captured, treat the whole
    // expression as captured too.
    if (absl::c_any_of(new_children, [](const auto& e) { return !e; })) {
      return nullptr;
    }
    // Rebuild operator with new children.
    auto result = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
      op.type, op.return_type);
    result->children = std::move(new_children);
    return result;
  }

  if (expr.expression_class == duckdb::ExpressionClass::BOUND_COMPARISON) {
    const auto& cmp = expr.Cast<duckdb::BoundComparisonExpression>();
    auto new_left = RewriteExpr(*cmp.left, sources);
    auto new_right = RewriteExpr(*cmp.right, sources);
    bool left_changed = !new_left || (new_left.get() != cmp.left.get());
    bool right_changed = !new_right || (new_right.get() != cmp.right.get());
    if (!left_changed && !right_changed) {
      return expr.Copy();
    }
    if (!new_left || !new_right) {
      return nullptr;
    }
    return duckdb::make_uniq<duckdb::BoundComparisonExpression>(
      cmp.type, std::move(new_left), std::move(new_right));
  }

  if (expr.expression_class == duckdb::ExpressionClass::BOUND_BETWEEN) {
    // BoundBetweenExpression: if claimed, return nullptr.
    // Otherwise return a copy.
    return expr.Copy();
  }

  // Leaf / unknown expression: return a copy.
  return expr.Copy();
}

// ── Sweep-based OR merge ────────────────────────────────────────────────────
// TODO(mkornaukhov) separate this algorithms and add unit tests for it.
// Sorts constraints lexicographically by each PK column's left bound in order.
// This groups constraints that share the same prefix columns and orders the
// differing column so overlapping ranges become adjacent.

// One atomic segment on a single dimension's axis.
// is_point=false -> open interval (left, right); has_left/has_right mark
// whether the endpoint is finite. is_point=true -> closed singleton {left}.
// Atoms are represented as ColumnRanges: points use Point(), open intervals
// use Bounded/LeftBound/RightBound with exclusive endpoints, and the fully
// unconstrained atom is an empty ColumnRange{} (no flags set).
using Atom = ColumnRange;

// Lightweight constraint representation used during the recursive sweep.
struct SweepRegion {
  std::span<const catalog::Column::Id> pk_ids;
  KeyBounds::ColumnRangeMap column_ranges;
  KeyBounds::SourceExprsMap source_exprs;
};

// Builds the atom sequence for a sorted, deduplicated list of event points.
// Emits: (-inf, p0), {p0}, (p0,p1), {p1}, ..., {pN}, (pN, +inf).
// If event_points is empty, emits one unconstrained open atom.
std::vector<Atom> BuildAtoms(const std::vector<duckdb::Value>& pts) {
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
    const duckdb::Value& p = atom.LeftValue();
    fully_contained =
      (!column_range.HasLeft() || column_range.LeftValue() < p ||
       (column_range.LeftValue() == p && column_range.IsLeftInclusive())) &&
      (!column_range.HasRight() || p < column_range.RightValue() ||
       (column_range.RightValue() == p && column_range.IsRightInclusive()));
  } else {
    // Open interval (l, r) is a subset of column_range iff column_range starts
    // at or before l AND ends at or after r. If atom is unbounded on a side,
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
std::vector<duckdb::Value> CollectEventPoints(
  const std::vector<SweepRegion>& ranges, catalog::Column::Id dim_col) {
  std::vector<duckdb::Value> pts;
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
                           catalog::Column::Id dimension_column) {
  SweepRegion out;
  out.pk_ids = sweep_range.pk_ids;
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
                    catalog::Column::Id dim_col) {
  for (auto col_id : a.pk_ids) {
    if (col_id == dim_col) {
      continue;
    }
    const auto* a_cur_column =
      a.column_ranges.contains(col_id) ? &a.column_ranges.at(col_id) : nullptr;
    const auto* b_cur_column =
      b.column_ranges.contains(col_id) ? &b.column_ranges.at(col_id) : nullptr;
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
  catalog::Column::Id dim_col) {
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
      out.column_ranges[dim_col] = fused;
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
std::vector<SweepRegion> SweepDimensions(
  std::vector<SweepRegion> ranges, std::span<const catalog::Column::Id> pk_ids,
  size_t dim_idx) {
  if (dim_idx == pk_ids.size()) {
    // Base case: all dimensions projected away. Merge source exprs.
    SweepRegion out;
    out.pk_ids = pk_ids;
    for (const auto& range : ranges) {
      MergeSourceExprs(out.source_exprs, range.source_exprs);
    }
    return {std::move(out)};
  }

  const auto dim_col = pk_ids[dim_idx];
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
    auto sub = SweepDimensions(std::move(active), pk_ids, dim_idx + 1);
    for (auto& s : sub) {
      // (+inf, -inf) is encoded as an absence in a map
      if (atom.HasLeft() || atom.HasRight()) {
        s.column_ranges[dim_col] = atom;
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
  const auto pk_ids = constraints[0].PKColumns();

  for (const auto& c : constraints) {
    if (c.IsUnconstrained()) {
      return {KeyBounds::MakeAny(pk_ids)};
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
    sr.pk_ids = pk_ids;
    for (auto col_id : pk_ids) {
      if (const auto* cr = kc.FindColumnRange(col_id)) {
        sr.column_ranges.emplace(col_id, *cr);
      }
    }
    sr.source_exprs = kc.GetSourceExprs();
    inputs.push_back(std::move(sr));
  }

  auto swept = SweepDimensions(std::move(inputs), pk_ids, 0);

  std::vector<KeyBounds> result;
  result.reserve(swept.size());
  for (auto& sr : swept) {
    result.push_back(KeyBounds::BuildFromRanges(
      pk_ids, std::move(sr.column_ranges), std::move(sr.source_exprs)));
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

// ── ColumnResolver ──────────────────────────────────────────────────────────

catalog::Column::Id ColumnResolver::Resolve(
  const duckdb::BoundColumnRefExpression& ref) const {
  if (ref.binding.table_index != table_index) {
    return kInvalidId;
  }
  auto col_idx = ref.binding.column_index;
  if (col_idx >= projected_column_ids.size()) {
    return kInvalidId;
  }
  return projected_column_ids[col_idx];
}

// ── ColumnRange ─────────────────────────────────────────────────────────────

bool ColumnRange::operator==(const ColumnRange& other) const {
  if (_flags != other._flags) {
    return false;
  }
  if (HasLeft() && !(_left_value == other._left_value)) {
    return false;
  }
  if (HasRight() && !(_right_value == other._right_value)) {
    return false;
  }
  return true;
}

std::string ColumnRange::toString() const {
  // Distinguish a contradictory range (kEmptyRange) from a fully
  // unconstrained range -- both have no Left / no Right bounds, but
  // the former represents "no value matches" while the latter means
  // "any value". Without this split EXPLAIN shows both as
  // `(-inf, +inf)`, which is actively misleading for a contradictory
  // IS NULL-on-PK claim.
  if (IsEmpty()) {
    return "empty";
  }
  if (IsPoint()) {
    return _left_value.ToString();
  }
  std::string result;
  if (!HasLeft()) {
    absl::StrAppend(&result, "(-inf");
  } else {
    absl::StrAppend(&result, IsLeftInclusive() ? "[" : "(",
                    _left_value.ToString());
  }
  absl::StrAppend(&result, ", ");
  if (!HasRight()) {
    absl::StrAppend(&result, "+inf)");
  } else {
    absl::StrAppend(&result, _right_value.ToString(),
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
  for (auto col_id : _pk_ids) {
    auto it = _column_ranges.find(col_id);
    if (it == _column_ranges.end()) {
      continue;
    }
    if (!first) {
      absl::StrAppend(&result, ", ");
    }
    first = false;
    absl::StrAppend(&result, "col", col_id, ": ", it->second.toString());
  }
  absl::StrAppend(&result, "}");
  return result;
}

size_t KeyBounds::RangePrefixSize() const noexcept {
  for (size_t k = 0; k < _pk_ids.size(); ++k) {
    const ColumnRange* column_range = FindColumnRange(_pk_ids[k]);
    if (!column_range) {
      return k;
    }
    if (!column_range->IsPoint()) {
      // k specific points that defines prefix and
      // one on-column range that defines a rocksdb key range.
      return k + 1;
    }
  }

  return _pk_ids.size();
}

bool KeyBounds::IsResolvedPoint() const {
  return absl::c_all_of(_pk_ids, [&](catalog::Column::Id col_id) {
    auto it = _column_ranges.find(col_id);
    if (it == _column_ranges.end()) {
      return false;
    }
    return it->second.IsPoint();
  });
}

KeyBounds KeyBounds::BuildFromRanges(
  std::span<const catalog::Column::Id> pk_ids, ColumnRangeMap ranges,
  SourceExprsMap source_exprs) {
  KeyBounds constraint{pk_ids};
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

const containers::FlatHashSet<const duckdb::Expression*>&
KeyBounds::GetSourceExprs(catalog::Column::Id col_id) const noexcept {
  static const containers::FlatHashSet<const duckdb::Expression*> kEmpty;
  auto it = _source_exprs.find(col_id);
  return it != _source_exprs.end() ? it->second : kEmpty;
}

const ColumnRange* KeyBounds::FindColumnRange(
  catalog::Column::Id col_id) const {
  auto it = _column_ranges.find(col_id);
  return it != _column_ranges.end() ? &it->second : nullptr;
}

void KeyBounds::AddEqFilter(catalog::Column::Id col_id, duckdb::Value value,
                            const duckdb::Expression* source_expr) {
  SDB_ASSERT(!_column_ranges.contains(col_id));
  _column_ranges.emplace(col_id, ColumnRange::Point(std::move(value)));
  _source_exprs[col_id].insert(source_expr);
}

void KeyBounds::AddComparisonFilter(catalog::Column::Id col_id,
                                    duckdb::Value value, ComparisonOp op,
                                    const duckdb::Expression* source_expr) {
  SDB_ASSERT(!_column_ranges.contains(col_id));
  ColumnRange range;
  switch (op) {
    case ComparisonOp::Gt:
      range = ColumnRange::LeftBound(std::move(value), false);
      break;
    case ComparisonOp::Ge:
      range = ColumnRange::LeftBound(std::move(value), true);
      break;
    case ComparisonOp::Lt:
      range = ColumnRange::RightBound(std::move(value), false);
      break;
    case ComparisonOp::Le:
      range = ColumnRange::RightBound(std::move(value), true);
      break;
    case ComparisonOp::None:
      SDB_ASSERT(false, "AddComparisonFilter called with ComparisonOp::None");
      break;
  }
  _column_ranges.emplace(col_id, std::move(range));
  _source_exprs[col_id].insert(source_expr);
}

std::optional<KeyBounds> KeyBounds::TryIntersect(const KeyBounds& lhs,
                                                 const KeyBounds& rhs) {
  SDB_ASSERT(lhs._pk_ids.data() == rhs._pk_ids.data());
  auto result = KeyBounds::MakeAny(lhs._pk_ids);
  for (auto pk_id : lhs._pk_ids) {
    const auto* lhs_f = lhs.FindColumnRange(pk_id);
    const auto* rhs_f = rhs.FindColumnRange(pk_id);
    if (!lhs_f && !rhs_f) {
      continue;
    }
    if (!lhs_f) {
      result._column_ranges.emplace(pk_id, *rhs_f);
      continue;
    }
    if (!rhs_f) {
      result._column_ranges.emplace(pk_id, *lhs_f);
      continue;
    }
    auto merged_range = lhs_f->IntersectWith(*rhs_f);
    if (!merged_range) {
      // e.g. [1,1] AND [2,2] -> contradiction
      return {};
    }
    result._column_ranges.emplace(pk_id, *merged_range);
  }
  MergeSourceExprs(result._source_exprs, lhs._source_exprs);
  MergeSourceExprs(result._source_exprs, rhs._source_exprs);
  return result;
}

std::vector<ResolvedPoint> ToResolvedPoints(
  const std::vector<KeyBounds>& points,
  std::span<const catalog::Column::Id> column_ids) {
  std::vector<ResolvedPoint> result;
  result.reserve(points.size());
  for (const auto& p : points) {
    ResolvedPoint sp;
    sp.reserve(column_ids.size());
    for (auto col_id : column_ids) {
      const auto* filter = p.FindColumnRange(col_id);
      SDB_ASSERT(filter != nullptr, "pk column not found in specific point");
      SDB_ASSERT(filter->HasLeft());
      sp.push_back(filter->LeftValue());
    }
    result.push_back(std::move(sp));
  }
  return result;
}

std::vector<ResolvedRange> ToDisjointRanges(
  const std::vector<KeyBounds>& ranges,
  std::span<const catalog::Column::Id> pk_ids) {
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
      const auto* column_range = key_contraint.FindColumnRange(pk_ids[i]);
      SDB_ASSERT(column_range && column_range->IsPoint());
      resolved_range.prefix.push_back(column_range->LeftValue());
    }

    // Column range_column_index is the range column.
    const auto* column_range =
      key_contraint.FindColumnRange(pk_ids[range_column_index]);
    SDB_ASSERT(column_range);
    resolved_range.range_column = *column_range;

    result.push_back(std::move(resolved_range));
  }

  return result;
}

std::vector<KeyBounds> ExtractFilterExpr(
  const duckdb::Expression& expr, std::span<const catalog::Column::Id> pk_ids,
  const ColumnResolver& resolver, bool negated) {
  std::vector<KeyBounds> key_bounds;

  switch (expr.expression_class) {
    case duckdb::ExpressionClass::BOUND_COMPARISON: {
      const auto& cmp = expr.Cast<duckdb::BoundComparisonExpression>();
      switch (cmp.type) {
        case duckdb::ExpressionType::COMPARE_EQUAL:
          key_bounds = ExtractFilterEq(cmp, pk_ids, resolver, negated);
          break;
        case duckdb::ExpressionType::COMPARE_NOTEQUAL:
          key_bounds = ExtractFilterNeq(cmp, pk_ids, resolver, negated);
          break;
        case duckdb::ExpressionType::COMPARE_GREATERTHAN:
        case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
        case duckdb::ExpressionType::COMPARE_LESSTHAN:
        case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
          key_bounds = ExtractFilterComparison(cmp, pk_ids, resolver, negated);
          break;
        default:
          key_bounds = AnyKeyConstraint(pk_ids);
          break;
      }
      break;
    }

    case duckdb::ExpressionClass::BOUND_CONJUNCTION: {
      const auto& conj = expr.Cast<duckdb::BoundConjunctionExpression>();
      if (conj.type == duckdb::ExpressionType::CONJUNCTION_AND) {
        // De Morgan: NOT(A AND B) = NOT(A) OR NOT(B)
        key_bounds = negated
                       ? ExtractFilterOr(conj, pk_ids, resolver, negated)
                       : ExtractFilterAnd(conj, pk_ids, resolver, negated);
      } else if (conj.type == duckdb::ExpressionType::CONJUNCTION_OR) {
        // De Morgan: NOT(A OR B) = NOT(A) AND NOT(B)
        key_bounds = negated ? ExtractFilterAnd(conj, pk_ids, resolver, negated)
                             : ExtractFilterOr(conj, pk_ids, resolver, negated);
      } else {
        key_bounds = AnyKeyConstraint(pk_ids);
      }
      break;
    }

    case duckdb::ExpressionClass::BOUND_OPERATOR: {
      const auto& op_expr = expr.Cast<duckdb::BoundOperatorExpression>();
      switch (op_expr.type) {
        case duckdb::ExpressionType::OPERATOR_NOT:
          SDB_ASSERT(op_expr.children.size() == 1);
          key_bounds =
            ExtractFilterExpr(*op_expr.children[0], pk_ids, resolver, !negated);
          break;
        case duckdb::ExpressionType::OPERATOR_IS_NULL:
          if (!negated) {
            key_bounds = ExtractFilterIsNull(op_expr, pk_ids, resolver);
          } else {
            // NOT IS NULL == IS NOT NULL, which is unconstrained for PK
            // extraction purposes.
            key_bounds = AnyKeyConstraint(pk_ids);
          }
          break;
        case duckdb::ExpressionType::OPERATOR_IS_NOT_NULL:
          if (negated) {
            // NOT IS NOT NULL == IS NULL
            key_bounds = ExtractFilterIsNull(op_expr, pk_ids, resolver);
          } else {
            key_bounds = AnyKeyConstraint(pk_ids);
          }
          break;
        case duckdb::ExpressionType::COMPARE_IN:
          key_bounds = ExtractFilterIn(op_expr, pk_ids, resolver, negated);
          break;
        case duckdb::ExpressionType::COMPARE_NOT_IN:
          key_bounds = ExtractFilterIn(op_expr, pk_ids, resolver, !negated);
          break;
        default:
          key_bounds = AnyKeyConstraint(pk_ids);
          break;
      }
      break;
    }

    case duckdb::ExpressionClass::BOUND_BETWEEN: {
      const auto& between = expr.Cast<duckdb::BoundBetweenExpression>();
      key_bounds = ExtractFilterBetween(between, pk_ids, resolver, negated);
      break;
    }

    default:
      key_bounds = AnyKeyConstraint(pk_ids);
      break;
  }

  return key_bounds;
}

ExtractAndRewriteResult ExtractAndRewriteFilterExpr(
  const duckdb::Expression& expr, std::span<const catalog::Column::Id> pk_ids,
  const ColumnResolver& resolver) {
  auto constraints = ExtractFilterExpr(expr, pk_ids, resolver);

  if (constraints.empty()) {
    return {ConstraintKind::None, {}, expr.Copy()};
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
    return {ConstraintKind::None, {}, expr.Copy()};
  }

  if (absl::c_all_of(constraints,
                     [](const KeyBounds& p) { return p.IsResolvedPoint(); })) {
    containers::FlatHashSet<const duckdb::Expression*> sources;
    for (const auto& point : constraints) {
      for (const auto& [col_id, source_exprs] : point.GetSourceExprs()) {
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
        left_bound.FindColumnRange(pk_ids[i]);
      const ColumnRange* right_column_range =
        right_bound.FindColumnRange(pk_ids[i]);
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
    first_column_ranges.push_back(constraint.FindColumnRange(pk_ids[0]));
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
    return {ConstraintKind::None, {}, expr.Copy()};
  }

  // Collect source expressions from prefix columns so they can be stripped
  // from the remaining filter (the scan will enforce them).
  // If a constraint also covers suffix PK columns, skip it -- stripping
  // prefix sources would break correlations between prefix and suffix
  // conditions, e.g. OR(not_pk<2, not_pk>2) instead of (pk<2 AND not_pk<2) OR
  // (pk>2 AND not_pk>2))
  containers::FlatHashSet<const duckdb::Expression*> sources;
  for (const auto& constraint : constraints) {
    const auto prefix = constraint.RangePrefixSize();
    const bool has_suffix_constraint =
      absl::c_any_of(pk_ids.subspan(prefix), [&](catalog::Column::Id col_id) {
        return constraint.FindColumnRange(col_id) != nullptr;
      });
    if (!has_suffix_constraint) {
      for (size_t i = 0; i < prefix; ++i) {
        const auto& col_exprs = constraint.GetSourceExprs(pk_ids[i]);
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
