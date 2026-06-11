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

constexpr catalog::Column::Id kInvalidId = catalog::Column::kInvalidId;

std::vector<KeyBounds> MergeKeyConstraints(std::vector<KeyBounds>);

void MergeSourceExprs(KeyBounds::SourceExprsMap& dst,
                      const KeyBounds::SourceExprsMap& src);

// Context threaded through all filter-extraction helpers.
// is_primary_key distinguishes PK columns (always NOT NULL by storage contract)
// from SK columns (may store NULLs), allowing ExtractFilterIsNull to choose
// the correct KeyBounds model.
struct FilterExtractCtx {
  std::span<const catalog::Column::Id> key_ids;
  const ColumnResolver& resolver;
  bool negated = false;
  bool is_primary_key = true;
  // ExtractFilterOr writes source expressions from all-contradictory OR
  // branches into this set so RewriteExpr can strip them.
  containers::FlatHashSet<const duckdb::Expression*>& dead_sources;
};

std::vector<KeyBounds> ExtractFilterExprImpl(const duckdb::Expression& expr,
                                             const FilterExtractCtx& ctx);

std::vector<KeyBounds> AnyKeyConstraint(
  std::span<const catalog::Column::Id> key_ids) {
  return {KeyBounds::MakeAny(key_ids)};
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
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return kInvalidId;
  }
  return resolver.Resolve(expr.Cast<duckdb::BoundColumnRefExpression>());
}

// Returns the constant Value if expr is a BoundConstantExpression, nullopt
// otherwise.
const duckdb::Value* TryGetConstant(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
    return nullptr;
  }
  return &expr.Cast<duckdb::BoundConstantExpression>().value;
}

// Returns true if col_id is one of the PK columns.
bool IsKeyColumn(catalog::Column::Id col_id,
                 std::span<const catalog::Column::Id> key_ids) {
  return absl::c_linear_search(key_ids, col_id);
}

// Helper: produces two key bounds for a != v -> a < v OR a > v.
std::vector<KeyBounds> MakeNeqConstraints(
  catalog::Column::Id col_id, duckdb::Value val,
  const duckdb::Expression* source,
  std::span<const catalog::Column::Id> key_ids) {
  auto less_constraint = KeyBounds::MakeAny(key_ids);
  less_constraint.AddComparisonFilter(col_id, val, ComparisonOp::Lt, source);

  auto greater_constraint = KeyBounds::MakeAny(key_ids);
  greater_constraint.AddComparisonFilter(col_id, std::move(val),
                                         ComparisonOp::Gt, source);
  return {std::move(less_constraint), std::move(greater_constraint)};
}

// ── Comparison expression dispatch ──────────────────────────────────────────

std::vector<KeyBounds> ExtractFilterEq(
  const duckdb::BoundFunctionExpression& cmp, const FilterExtractCtx& ctx) {
  const auto& left = duckdb::BoundComparisonExpression::Left(cmp);
  const auto& right = duckdb::BoundComparisonExpression::Right(cmp);
  // Either side can be the column.
  catalog::Column::Id col_id = TryResolveColumn(left, ctx.resolver);
  const duckdb::Value* const_val = TryGetConstant(right);
  if (col_id == kInvalidId || !const_val) {
    col_id = TryResolveColumn(right, ctx.resolver);
    const_val = TryGetConstant(left);
  }
  if (col_id == kInvalidId || !const_val) {
    return AnyKeyConstraint(ctx.key_ids);
  }
  if (!IsKeyColumn(col_id, ctx.key_ids)) {
    return AnyKeyConstraint(ctx.key_ids);
  }
  if (ctx.negated) {
    // NOT(a = v) -> a < v OR a > v
    return MakeNeqConstraints(col_id, *const_val, &cmp, ctx.key_ids);
  }
  auto p = KeyBounds::MakeAny(ctx.key_ids);
  p.AddEqFilter(col_id, *const_val, &cmp);
  return {p};
}

std::vector<KeyBounds> ExtractFilterNeq(
  const duckdb::BoundFunctionExpression& cmp, const FilterExtractCtx& ctx) {
  const auto& left = duckdb::BoundComparisonExpression::Left(cmp);
  const auto& right = duckdb::BoundComparisonExpression::Right(cmp);
  catalog::Column::Id col_id = TryResolveColumn(left, ctx.resolver);
  const duckdb::Value* const_val = TryGetConstant(right);
  if (col_id == kInvalidId || !const_val) {
    col_id = TryResolveColumn(right, ctx.resolver);
    const_val = TryGetConstant(left);
  }
  if (col_id == kInvalidId || !const_val) {
    return AnyKeyConstraint(ctx.key_ids);
  }
  if (!IsKeyColumn(col_id, ctx.key_ids)) {
    return AnyKeyConstraint(ctx.key_ids);
  }
  if (ctx.negated) {
    // NOT(a != v) -> a = v
    auto kc = KeyBounds::MakeAny(ctx.key_ids);
    kc.AddEqFilter(col_id, *const_val, &cmp);
    return {std::move(kc)};
  }
  return MakeNeqConstraints(col_id, *const_val, &cmp, ctx.key_ids);
}

std::vector<KeyBounds> ExtractFilterComparison(
  const duckdb::BoundFunctionExpression& cmp, const FilterExtractCtx& ctx) {
  const auto& left = duckdb::BoundComparisonExpression::Left(cmp);
  const auto& right = duckdb::BoundComparisonExpression::Right(cmp);
  const bool field_left = TryResolveColumn(left, ctx.resolver) != kInvalidId &&
                          TryGetConstant(right) != nullptr;
  const bool field_right =
    TryResolveColumn(right, ctx.resolver) != kInvalidId &&
    TryGetConstant(left) != nullptr;
  if (!field_left && !field_right) {
    return AnyKeyConstraint(ctx.key_ids);
  }

  const auto& field_expr = field_left ? left : right;
  const auto& const_expr = field_left ? right : left;
  auto col_id = TryResolveColumn(field_expr, ctx.resolver);
  const auto* const_val = TryGetConstant(const_expr);

  if (!IsKeyColumn(col_id, ctx.key_ids)) {
    return AnyKeyConstraint(ctx.key_ids);
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
  switch (cmp.GetExpressionType()) {
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
  if (ctx.negated) {
    op = NegateOp(op);
  }

  auto constraint = KeyBounds::MakeAny(ctx.key_ids);
  constraint.AddComparisonFilter(col_id, *const_val, op, &cmp);
  return {std::move(constraint)};
}

// ── IS NULL ─────────────────────────────────────────────────────────────────

std::vector<KeyBounds> ExtractFilterIsNull(
  const duckdb::BoundOperatorExpression& op_expr, const FilterExtractCtx& ctx) {
  SDB_ASSERT(op_expr.children.size() == 1);
  auto col_id = TryResolveColumn(*op_expr.children[0], ctx.resolver);
  if (col_id == kInvalidId || !IsKeyColumn(col_id, ctx.key_ids)) {
    return AnyKeyConstraint(ctx.key_ids);
  }

  if (ctx.is_primary_key) {
    if (ctx.negated) {
      // PK IS NOT NULL -> always true (PK is NOT NULL by storage contract).
      return AnyKeyConstraint(ctx.key_ids);
    }
    // PK IS NULL -> always false.
    return {KeyBounds::MakeContradictory(ctx.key_ids)};
  }

  // SK column: may store NULLs via the 0x01 sentinel prefix.
  // ctx.negated reflects the effective polarity (OPERATOR_IS_NOT_NULL already
  // flips it before calling here).
  auto kc = KeyBounds::MakeAny(ctx.key_ids);

  if (!ctx.negated) {
    kc.AddNullFilter(col_id, &op_expr);
  } else {
    kc.AddNotNullFilter(col_id, &op_expr);
  }
  return {kc};
}

// ── IN / NOT IN ─────────────────────────────────────────────────────────────

std::vector<KeyBounds> ExtractFilterIn(
  const duckdb::BoundOperatorExpression& op_expr, const FilterExtractCtx& ctx) {
  if (op_expr.children.empty()) {
    return AnyKeyConstraint(ctx.key_ids);
  }
  auto col_id = TryResolveColumn(*op_expr.children[0], ctx.resolver);
  if (col_id == kInvalidId || !IsKeyColumn(col_id, ctx.key_ids)) {
    return AnyKeyConstraint(ctx.key_ids);
  }

  // children[1..] are the constant values in the IN list.
  const auto size = op_expr.children.size() - 1;
  if (size == 0) {
    return AnyKeyConstraint(ctx.key_ids);
  }

  // Verify all children[1..] are constants.
  for (size_t i = 1; i < op_expr.children.size(); ++i) {
    if (!TryGetConstant(*op_expr.children[i])) {
      return AnyKeyConstraint(ctx.key_ids);
    }
  }

  if (!ctx.negated) {
    // a IN (x1, ..., xn) -> n equality constraints
    std::vector<KeyBounds> points;
    points.reserve(size);
    for (size_t i = 1; i < op_expr.children.size(); ++i) {
      const auto* val = TryGetConstant(*op_expr.children[i]);
      auto p = KeyBounds::MakeAny(ctx.key_ids);
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
    auto constraint = KeyBounds::MakeAny(ctx.key_ids);
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
  const duckdb::BoundFunctionExpression& between, const FilterExtractCtx& ctx) {
  auto col_id = TryResolveColumn(duckdb::BoundBetweenExpression::Input(between),
                                 ctx.resolver);
  if (col_id == kInvalidId || !IsKeyColumn(col_id, ctx.key_ids)) {
    return AnyKeyConstraint(ctx.key_ids);
  }
  const auto* lower_val =
    TryGetConstant(duckdb::BoundBetweenExpression::LowerBound(between));
  const auto* upper_val =
    TryGetConstant(duckdb::BoundBetweenExpression::UpperBound(between));
  if (!lower_val || !upper_val) {
    return AnyKeyConstraint(ctx.key_ids);
  }
  const bool lower_inclusive =
    duckdb::BoundBetweenExpression::LowerInclusive(between);
  const bool upper_inclusive =
    duckdb::BoundBetweenExpression::UpperInclusive(between);

  if (!ctx.negated) {
    // a BETWEEN lower AND upper -> lower_op(a, lower) AND upper_op(a, upper)
    auto lower_op = lower_inclusive ? ComparisonOp::Ge : ComparisonOp::Gt;
    auto upper_op = upper_inclusive ? ComparisonOp::Le : ComparisonOp::Lt;

    auto lower_kc = KeyBounds::MakeAny(ctx.key_ids);
    lower_kc.AddComparisonFilter(col_id, *lower_val, lower_op, &between);

    auto upper_kc = KeyBounds::MakeAny(ctx.key_ids);
    upper_kc.AddComparisonFilter(col_id, *upper_val, upper_op, &between);

    auto merged = KeyBounds::TryIntersect(lower_kc, upper_kc);
    if (!merged) {
      return {KeyBounds::MakeContradictory(ctx.key_ids)};
    }
    return {std::move(*merged)};
  }

  // NOT BETWEEN: De Morgan -> a < lower OR a > upper
  auto lower_op = lower_inclusive ? ComparisonOp::Lt : ComparisonOp::Le;
  auto upper_op = upper_inclusive ? ComparisonOp::Gt : ComparisonOp::Ge;

  auto lt_constraint = KeyBounds::MakeAny(ctx.key_ids);
  lt_constraint.AddComparisonFilter(col_id, *lower_val, lower_op, &between);

  auto gt_constraint = KeyBounds::MakeAny(ctx.key_ids);
  gt_constraint.AddComparisonFilter(col_id, *upper_val, upper_op, &between);

  return {std::move(lt_constraint), std::move(gt_constraint)};
}

// ── AND / OR ────────────────────────────────────────────────────────────────

std::vector<KeyBounds> ExtractFilterAnd(
  const duckdb::BoundConjunctionExpression& conj, const FilterExtractCtx& ctx) {
  SDB_ASSERT(!conj.children.empty());

  // Cartesian product of all children's point sets, intersecting each tuple.
  // An unconstrained child (AnyPoint -- empty filters) acts as identity because
  // Intersect(P, {}) == P, so no special-casing is needed.
  std::vector<KeyBounds> result = ExtractFilterExprImpl(*conj.children[0], ctx);
  for (size_t i = 1; i < conj.children.size(); ++i) {
    // Propagate contradictory: AND(contradictory, anything) = contradictory.
    if (absl::c_all_of(result,
                       [](const KeyBounds& c) { return c.IsEmpty(); })) {
      return result;
    }
    const auto rhs_pts = ExtractFilterExprImpl(*conj.children[i], ctx);
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
        return {KeyBounds::MakeContradictory(ctx.key_ids)};
      }
      // All intersections produced unconstrained results: AND is unconstrained.
      return AnyKeyConstraint(ctx.key_ids);
    }
  }
  return result;
}

std::vector<KeyBounds> ExtractFilterOr(
  const duckdb::BoundConjunctionExpression& conj, const FilterExtractCtx& ctx) {
  SDB_ASSERT(!conj.children.empty());

  std::vector<KeyBounds> result;
  for (const auto& child : conj.children) {
    auto constraints = ExtractFilterExprImpl(*child, ctx);

    if (constraints.empty() ||
        absl::c_any_of(constraints, [](const KeyBounds& p) {
          return p.IsUnconstrained();
        })) {
      return AnyKeyConstraint(ctx.key_ids);
    }

    if (absl::c_all_of(constraints,
                       [](const KeyBounds& p) { return p.IsEmpty(); })) {
      for (const auto& c : constraints) {
        for (const auto& [col_id, src] : c.GetSourceExprs()) {
          ctx.dead_sources.insert(src.begin(), src.end());
        }
      }
      continue;
    }

    result.insert(result.end(), std::make_move_iterator(constraints.begin()),
                  std::make_move_iterator(constraints.end()));
  }
  // All branches were contradictory -> OR is itself contradictory.
  if (result.empty()) {
    return {KeyBounds::MakeContradictory(ctx.key_ids)};
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

  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONJUNCTION) {
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

    if (expr.GetExpressionType() == duckdb::ExpressionType::CONJUNCTION_AND) {
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

    if (expr.GetExpressionType() == duckdb::ExpressionType::CONJUNCTION_OR) {
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

  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_OPERATOR) {
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
      op.GetExpressionType(), op.GetReturnType());
    result->children = std::move(new_children);
    return result;
  }

  if (duckdb::BoundComparisonExpression::IsComparison(expr)) {
    const auto& cmp = expr.Cast<duckdb::BoundFunctionExpression>();
    const auto& cmp_left = duckdb::BoundComparisonExpression::Left(cmp);
    const auto& cmp_right = duckdb::BoundComparisonExpression::Right(cmp);
    auto new_left = RewriteExpr(cmp_left, sources);
    auto new_right = RewriteExpr(cmp_right, sources);
    bool left_changed = !new_left || (new_left.get() != &cmp_left);
    bool right_changed = !new_right || (new_right.get() != &cmp_right);
    if (!left_changed && !right_changed) {
      return expr.Copy();
    }
    if (!new_left || !new_right) {
      return nullptr;
    }
    return duckdb::BoundComparisonExpression::Create(
      cmp.GetExpressionType(), std::move(new_left), std::move(new_right));
  }

  if (expr.GetExpressionType() == duckdb::ExpressionType::COMPARE_BETWEEN) {
    // BETWEEN function expression: if claimed, return nullptr.
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
  std::span<const catalog::Column::Id> key_ids;
  KeyBounds::ColumnRangeMap column_ranges;
  KeyBounds::SourceExprsMap source_exprs;
};

// Builds the atom sequence for a sorted, deduplicated list of event points.
// When has_null_ranges is true, prepends a NullOnly() atom for the null bucket.
// Value atoms: (-inf, p0), {p0}, (p0,p1), {p1}, ..., {pN}, (pN, +inf).
// If pts is empty and has_null_ranges is false, emits one unconstrained atom.
std::vector<Atom> BuildAtoms(const std::vector<duckdb::Value>& pts,
                             bool has_null_ranges) {
  std::vector<Atom> atoms;
  if (has_null_ranges) {
    atoms.push_back(ColumnRange::Null());
  }
  if (pts.empty()) {
    atoms.push_back(ColumnRange{});  // unconstrained (-inf, +inf)
    return atoms;
  }
  atoms.reserve(atoms.size() + 2 * pts.size() + 1);
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
  if (column_range.IsEmpty()) {
    fully_contained = false;
  } else if (atom.IsNull()) {
    fully_contained = column_range.IsNull();
  } else if (column_range.IsNull()) {
    // Non-null atom cannot be contained in a null-only range.
    fully_contained = false;
  } else if (atom.IsNonNullPoint()) {
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
                          atom.LeftValue() >= column_range.LeftValue())) &&
      (!atom.HasRight() ? !column_range.HasRight()
                        : (!column_range.HasRight() ||
                           column_range.RightValue() >= atom.RightValue()));
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
  out.key_ids = sweep_range.key_ids;
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
  for (auto col_id : a.key_ids) {
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
  // An atom that produced multiple sub-regions at this dim (e.g. a point
  // atom where two deeper-dim atoms survived with disjoint sub-results)
  // has more than one entry in atom_results with the SAME first-field.
  // Fusing across such an atom would extend an adjacent open range to
  // cover that atom value, while the sibling duplicate entry also
  // covers the atom value -- producing overlapping scan ranges and
  // emitting the same row twice. Flag those entries so can_extend_run
  // bails when either side sits on a multi-occurrence atom.
  std::vector<bool> atom_has_duplicates(atom_results.size(), false);
  for (size_t i = 0; i < atom_results.size(); ++i) {
    if (atom_has_duplicates[i]) {
      continue;
    }
    for (size_t j = i + 1; j < atom_results.size(); ++j) {
      if (atom_results[j].first == atom_results[i].first) {
        atom_has_duplicates[i] = true;
        atom_has_duplicates[j] = true;
      }
    }
  }

  // Two adjacent entries can extend the same run when:
  //  - atoms alternate point/open (a skipped point means a gap -- do not
  //  merge),
  //  - they share a boundary value (or both are unbounded on the touching
  //  side),
  //  - their sub-results agree on all other dimensions,
  //  - neither side is an atom with multiple sub-regions at this dim.
  // Since a point has LeftValue() == RightValue(), prev.RightValue() and
  // curr.LeftValue() give the shared endpoint for both point and open atoms.
  auto can_extend_run = [&](size_t i) {
    if (atom_has_duplicates[i - 1] || atom_has_duplicates[i]) {
      return false;
    }
    const Atom& prev = atom_results[i - 1].first;
    const Atom& curr = atom_results[i].first;
    return prev.IsNonNullPoint() != curr.IsNonNullPoint() &&
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
    if (fused.HasLeft() || fused.HasRight() || fused.IsNull()) {
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
  std::vector<SweepRegion> ranges, std::span<const catalog::Column::Id> key_ids,
  size_t dim_idx) {
  if (dim_idx == key_ids.size()) {
    // Base case: all dimensions projected away. Merge source exprs.
    SweepRegion out;
    out.key_ids = key_ids;
    for (const auto& range : ranges) {
      MergeSourceExprs(out.source_exprs, range.source_exprs);
    }
    return {std::move(out)};
  }

  const auto dim_col = key_ids[dim_idx];
  const auto events = CollectEventPoints(ranges, dim_col);
  const bool has_null_ranges =
    absl::c_any_of(ranges, [&](const SweepRegion& sr) {
      auto it = sr.column_ranges.find(dim_col);
      return it != sr.column_ranges.end() && it->second.IsNull();
    });
  const auto atoms = BuildAtoms(events, has_null_ranges);

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
    auto sub = SweepDimensions(std::move(active), key_ids, dim_idx + 1);
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
  const auto key_ids = constraints[0].KeyColumns();

  for (const auto& c : constraints) {
    if (c.IsUnconstrained()) {
      return {KeyBounds::MakeAny(key_ids)};
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
    sr.key_ids = key_ids;
    for (auto col_id : key_ids) {
      if (const auto* cr = kc.FindColumnRange(col_id)) {
        sr.column_ranges.emplace(col_id, *cr);
      }
    }
    sr.source_exprs = kc.GetSourceExprs();
    inputs.push_back(std::move(sr));
  }

  auto swept = SweepDimensions(std::move(inputs), key_ids, 0);

  std::vector<KeyBounds> result;
  result.reserve(swept.size());
  for (auto& sr : swept) {
    result.push_back(KeyBounds::BuildFromRanges(
      key_ids, std::move(sr.column_ranges), std::move(sr.source_exprs)));
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

std::vector<KeyBounds> ExtractFilterExprImpl(const duckdb::Expression& expr,
                                             const FilterExtractCtx& ctx) {
  std::vector<KeyBounds> key_bounds;

  if (duckdb::BoundComparisonExpression::IsComparison(expr)) {
    const auto& cmp = expr.Cast<duckdb::BoundFunctionExpression>();
    switch (cmp.GetExpressionType()) {
      case duckdb::ExpressionType::COMPARE_EQUAL:
        return ExtractFilterEq(cmp, ctx);
      case duckdb::ExpressionType::COMPARE_NOTEQUAL:
        return ExtractFilterNeq(cmp, ctx);
      case duckdb::ExpressionType::COMPARE_GREATERTHAN:
      case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      case duckdb::ExpressionType::COMPARE_LESSTHAN:
      case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
        return ExtractFilterComparison(cmp, ctx);
      default:
        return AnyKeyConstraint(ctx.key_ids);
    }
  }

  if (expr.GetExpressionType() == duckdb::ExpressionType::COMPARE_BETWEEN) {
    const auto& between = expr.Cast<duckdb::BoundFunctionExpression>();
    return ExtractFilterBetween(between, ctx);
  }

  switch (expr.GetExpressionClass()) {
    case duckdb::ExpressionClass::BOUND_CONJUNCTION: {
      const auto& conj = expr.Cast<duckdb::BoundConjunctionExpression>();
      if (conj.GetExpressionType() == duckdb::ExpressionType::CONJUNCTION_AND) {
        // De Morgan: NOT(A AND B) = NOT(A) OR NOT(B)
        key_bounds = ctx.negated ? ExtractFilterOr(conj, ctx)
                                 : ExtractFilterAnd(conj, ctx);
      } else if (conj.GetExpressionType() ==
                 duckdb::ExpressionType::CONJUNCTION_OR) {
        // De Morgan: NOT(A OR B) = NOT(A) AND NOT(B)
        key_bounds = ctx.negated ? ExtractFilterAnd(conj, ctx)
                                 : ExtractFilterOr(conj, ctx);
      } else {
        key_bounds = AnyKeyConstraint(ctx.key_ids);
      }
      break;
    }

    case duckdb::ExpressionClass::BOUND_OPERATOR: {
      const auto& op_expr = expr.Cast<duckdb::BoundOperatorExpression>();
      switch (op_expr.GetExpressionType()) {
        case duckdb::ExpressionType::OPERATOR_NOT: {
          SDB_ASSERT(op_expr.children.size() == 1);
          auto negated_ctx = ctx;
          negated_ctx.negated = !ctx.negated;
          key_bounds = ExtractFilterExprImpl(*op_expr.children[0], negated_ctx);
        } break;
        case duckdb::ExpressionType::OPERATOR_IS_NULL:
          key_bounds = ExtractFilterIsNull(op_expr, ctx);
          break;
        case duckdb::ExpressionType::OPERATOR_IS_NOT_NULL: {
          auto negated_ctx = ctx;
          negated_ctx.negated = !ctx.negated;
          key_bounds = ExtractFilterIsNull(op_expr, negated_ctx);
        } break;
        case duckdb::ExpressionType::COMPARE_IN:
          key_bounds = ExtractFilterIn(op_expr, ctx);
          break;
        case duckdb::ExpressionType::COMPARE_NOT_IN: {
          auto negated_ctx = ctx;
          negated_ctx.negated = !ctx.negated;
          key_bounds = ExtractFilterIn(op_expr, negated_ctx);
          break;
        }
        default:
          key_bounds = AnyKeyConstraint(ctx.key_ids);
          break;
      }
      break;
    }

    default:
      key_bounds = AnyKeyConstraint(ctx.key_ids);
      break;
  }

  return key_bounds;
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
  if (IsEmpty()) {
    return "empty";
  }
  if (IsNull()) {
    return "null";
  }
  if (IsNonNullPoint()) {
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
  for (auto col_id : _key_ids) {
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
  for (size_t k = 0; k < _key_ids.size(); ++k) {
    const ColumnRange* column_range = FindColumnRange(_key_ids[k]);
    if (!column_range) {
      return k;
    }
    if (!column_range->IsNonNullPoint()) {
      // k specific points that defines prefix and
      // one on-column range that defines a rocksdb key range.
      return k + 1;
    }
  }

  return _key_ids.size();
}

bool KeyBounds::IsResolvedNonNullPoint() const {
  return absl::c_all_of(_key_ids, [&](catalog::Column::Id col_id) {
    auto it = _column_ranges.find(col_id);
    if (it == _column_ranges.end()) {
      return false;
    }
    return it->second.IsNonNullPoint();
  });
}

KeyBounds KeyBounds::BuildFromRanges(
  std::span<const catalog::Column::Id> key_ids, ColumnRangeMap ranges,
  SourceExprsMap source_exprs) {
  KeyBounds constraint{key_ids};
  constraint._column_ranges = std::move(ranges);
  constraint._source_exprs = std::move(source_exprs);
  return constraint;
}

std::optional<ColumnRange> ColumnRange::IntersectWith(
  const ColumnRange& other) const {
  // kEmptyRange means "nothing at all, not even null". Either side being empty
  // makes the intersection contradictory.
  if (IsEmpty() || other.IsEmpty()) {
    return std::nullopt;
  }
  // kIsNull is exclusive -- a null-only range only overlaps with another
  // null-only range. A null-only and a value range are disjoint.
  if (IsNull() || other.IsNull()) {
    if (IsNull() && other.IsNull()) {
      return Null();
    }
    return std::nullopt;
  }

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

std::strong_ordering ColumnRange::operator<=>(
  const ColumnRange& other) const noexcept {
  // Empty sorts first.
  if (IsEmpty() != other.IsEmpty()) {
    return IsEmpty() ? std::strong_ordering::less
                     : std::strong_ordering::greater;
  }
  if (IsEmpty()) {
    return std::strong_ordering::equal;
  }
  // Null sorts before any value range.
  if (IsNull() != other.IsNull()) {
    return IsNull() ? std::strong_ordering::less
                    : std::strong_ordering::greater;
  }
  if (IsNull()) {
    return std::strong_ordering::equal;
  }

  // Note: (0, +inf) < [0, +inf) for integers
  // Left bound: unbounded = -inf < any finite value; inclusive < exclusive.
  if (HasLeft() != other.HasLeft()) {
    return HasLeft() ? std::strong_ordering::greater
                     : std::strong_ordering::less;
  }
  if (HasLeft()) {
    if (_left_value < other._left_value) {
      return std::strong_ordering::less;
    }
    if (other._left_value < _left_value) {
      return std::strong_ordering::greater;
    }
    if (IsLeftInclusive() != other.IsLeftInclusive()) {
      return IsLeftInclusive() ? std::strong_ordering::less
                               : std::strong_ordering::greater;
    }
  }

  // Right bound: unbounded = +inf > any finite value; exclusive < inclusive.
  if (HasRight() != other.HasRight()) {
    return HasRight() ? std::strong_ordering::less
                      : std::strong_ordering::greater;
  }
  if (HasRight()) {
    if (_right_value < other._right_value) {
      return std::strong_ordering::less;
    }
    if (other._right_value < _right_value) {
      return std::strong_ordering::greater;
    }
    if (IsRightInclusive() != other.IsRightInclusive()) {
      return IsRightInclusive() ? std::strong_ordering::greater
                                : std::strong_ordering::less;
    }
  }
  return std::strong_ordering::equal;
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

void KeyBounds::AddNullFilter(catalog::Column::Id col_id,
                              const duckdb::Expression* source_expr) {
  SDB_ASSERT(!_column_ranges.contains(col_id));
  _column_ranges.emplace(col_id, ColumnRange::Null());
  _source_exprs[col_id].insert(source_expr);
}

void KeyBounds::AddNotNullFilter(catalog::Column::Id col_id,
                                 const duckdb::Expression* source_expr) {
  SDB_ASSERT(!_column_ranges.contains(col_id));
  _column_ranges.emplace(col_id, ColumnRange::AnyNotNull());
  _source_exprs[col_id].insert(source_expr);
}

std::optional<KeyBounds> KeyBounds::TryIntersect(const KeyBounds& lhs,
                                                 const KeyBounds& rhs) {
  SDB_ASSERT(lhs._key_ids.data() == rhs._key_ids.data());
  auto result = KeyBounds::MakeAny(lhs._key_ids);
  for (auto pk_id : lhs._key_ids) {
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

std::vector<ResolvedPoint> ToSortedResolvedPoints(
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
  absl::c_sort(result, [](const ResolvedPoint& lhs, const ResolvedPoint& rhs) {
    for (size_t i = 0; i < lhs.size(); ++i) {
      if (lhs[i] != rhs[i]) {
        return lhs[i] < rhs[i];
      }
    }
    return false;
  });
  auto [first, last] = std::ranges::unique(result);
  result.erase(first, last);
  return result;
}

std::vector<ResolvedRange> ToSortedDisjointRanges(
  const std::vector<KeyBounds>& ranges,
  std::span<const catalog::Column::Id> key_ids) {
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
      const auto* column_range = key_contraint.FindColumnRange(key_ids[i]);
      SDB_ASSERT(column_range && column_range->IsNonNullPoint());
      resolved_range.prefix.push_back(column_range->LeftValue());
    }

    const auto* column_range =
      key_contraint.FindColumnRange(key_ids[range_column_index]);
    SDB_ASSERT(column_range);
    resolved_range.range_column = *column_range;

    result.push_back(std::move(resolved_range));
  }

  absl::c_sort(result);
  auto [first, last] = std::ranges::unique(result);
  result.erase(first, last);
  return result;
}

// Thin reference wrapper for scan-equivalence dedup. Holds only a reference
// to a KeyBounds -- no per-entry heap allocation. Key ids are read from the
// bound KeyBounds itself.
// Two ScanKeyRefs are equal iff their prefix_size matches and every prefix
// column range compares equal (via ColumnRange::operator==).
struct ScanKeyRef {
  const KeyBounds& key_bounds;

  bool operator==(const ScanKeyRef& other) const noexcept {
    const auto key_ids = key_bounds.KeyColumns();
    SDB_ASSERT(std::ranges::equal(key_ids, other.key_bounds.KeyColumns()),
               "ScanKeyRefs must share the same key schema");
    const size_t prefix_size = key_bounds.RangePrefixSize();
    if (prefix_size != other.key_bounds.RangePrefixSize()) {
      return false;
    }
    for (size_t i = 0; i < prefix_size; ++i) {
      const auto* l = key_bounds.FindColumnRange(key_ids[i]);
      const auto* r = other.key_bounds.FindColumnRange(key_ids[i]);
      if (l != r && (!l || !r || *l != *r)) {
        return false;
      }
    }
    return true;
  }

  template<typename H>
  friend H AbslHashValue(H h, const ScanKeyRef& self) {
    const auto key_ids = self.key_bounds.KeyColumns();
    const size_t prefix_size = self.key_bounds.RangePrefixSize();
    h = H::combine(std::move(h), prefix_size);
    for (size_t i = 0; i < prefix_size; ++i) {
      if (const auto* range_column =
            self.key_bounds.FindColumnRange(key_ids[i])) {
        h = H::combine(std::move(h), *range_column);
      }
    }
    return h;
  }
};

ExtractAndRewriteResult ExtractAndRewriteFilterExpr(
  const duckdb::Expression& expr, std::span<const catalog::Column::Id> key_ids,
  const ColumnResolver& resolver, bool is_primary_key, bool is_unique) {
  containers::FlatHashSet<const duckdb::Expression*> dead_sources;
  auto constraints = ExtractFilterExprImpl(
    expr, {key_ids, resolver, false, is_primary_key, dead_sources});

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

  if (absl::c_all_of(constraints, [](const KeyBounds& p) {
        return p.IsResolvedNonNullPoint();
      })) {
    // Non-unique SK: point lookup is unsafe (multiple rows share the key).
    // Fall through to the range scan path -- a point [v,v] is a valid range
    // that SkRangeScan handles by scanning the [v_encoded, v_encoded+1) slice.
    if (is_primary_key || is_unique) {
      containers::FlatHashSet<const duckdb::Expression*> sources =
        std::move(dead_sources);
      for (const auto& point : constraints) {
        for (const auto& [col_id, source_exprs] : point.GetSourceExprs()) {
          sources.insert(source_exprs.begin(), source_exprs.end());
        }
      }
      return {ConstraintKind::Points, std::move(constraints),
              RewriteExpr(expr, sources)};
    }
    // else: non-unique SK -- fall through to Ranges below
  }

  // Normalize: multiple constraints may share the same prefix ranges but
  // differ in suffix PK columns. They produce identical RocksDB range scans,
  // so keep one representative and let remaining_filter handle the rest.
  // Two constraints are scan-equivalent iff their prefix size matches and
  // every column range in the prefix is equal. SweepDimensions can emit
  // equivalent pieces non-adjacently (one block per trailing-col value),
  // so O(n) hash-set dedup on a prefix key beats std::unique.
  {
    containers::FlatHashSet<ScanKeyRef> seen;
    seen.reserve(constraints.size());

    for (size_t i = 0; i < constraints.size();) {
      if (seen.insert(ScanKeyRef{constraints[i]}).second) {
        ++i;
        continue;
      }
      if (i + 1 != constraints.size()) {
        constraints[i] = std::move(constraints.back());
      }
      constraints.pop_back();
    }
  }

  // If the prefix ranges together cover (-inf, +inf) with no gaps, the scan
  // reads every row -- just do a full scan instead. Only meaningful for PK:
  // PK columns are non-null.
  if (is_primary_key) {
    std::vector<const ColumnRange*> first_column_ranges;
    first_column_ranges.reserve(constraints.size());
    for (const auto& constraint : constraints) {
      first_column_ranges.push_back(constraint.FindColumnRange(key_ids[0]));
    }
    // A nullptr means the constraint doesn't restrict key_ids[0] -- treat it
    // as fully unbounded (sorts first).
    absl::c_sort(first_column_ranges,
                 [](const ColumnRange* a, const ColumnRange* b) {
                   if (a == b) {
                     return false;
                   }
                   if (!a || !b) {
                     return !a;
                   }
                   return *a < *b;
                 });

    const bool first_unbounded =
      !first_column_ranges.front() || !first_column_ranges.front()->HasLeft();
    const bool last_unbounded =
      !first_column_ranges.back() || !first_column_ranges.back()->HasRight();
    bool contiguous = first_unbounded && last_unbounded;
    for (size_t i = 1; contiguous && i < first_column_ranges.size(); ++i) {
      const auto* prev = first_column_ranges[i - 1];
      const auto* curr = first_column_ranges[i];
      // Adjacent ranges must meet at the same value with no gap (at least
      // one endpoint must include the shared point).
      if (!prev || !prev->HasRight() || !curr || !curr->HasLeft() ||
          prev->RightValue() != curr->LeftValue() ||
          (!prev->IsRightInclusive() && !curr->IsLeftInclusive())) {
        contiguous = false;
      }
    }
    if (contiguous) {
      return {ConstraintKind::None, {}, expr.Copy()};
    }
  }

  // Collect source expressions from prefix columns so they can be stripped
  // from the remaining filter (the scan will enforce them).
  // A source is safe to strip only when it appears in some constraint's
  // prefix AND in NO constraint's suffix. SweepDimensions decomposes
  // `(AND all_pk) OR (IN on trailing)` into a mix of fully-prefixed
  // pieces (e.g. `{a=v1, b=v2, ..., z=Point(k)}`, whose source is `&IN`
  // via the `z` prefix column) and sweep-split pieces (e.g.
  // `{a=(-inf, v1), z=Point(k)}`, whose `z=Point(k)` is in the SUFFIX --
  // past the scan's range column). The scan only enforces up to the
  // range column, so the sweep-split piece does NOT enforce `z=k`; the
  // scan's union of ranges over-reads. If we strip `&IN` anyway (because
  // the fully-prefixed piece puts it in the prefix bucket), the over-read
  // goes uncorrected. Subtracting suffix sources from the strippable set
  // keeps the trailing predicate in remaining_filter whenever any sibling
  // piece leaves it unenforced.
  containers::FlatHashSet<const duckdb::Expression*> prefix_sources;
  containers::FlatHashSet<const duckdb::Expression*> suffix_sources;
  for (const auto& constraint : constraints) {
    const auto prefix = constraint.RangePrefixSize();
    for (size_t i = 0; i < prefix; ++i) {
      const auto& col_exprs = constraint.GetSourceExprs(key_ids[i]);
      prefix_sources.insert(col_exprs.begin(), col_exprs.end());
    }
    for (auto col_id : key_ids.subspan(prefix)) {
      const auto& col_exprs = constraint.GetSourceExprs(col_id);
      suffix_sources.insert(col_exprs.begin(), col_exprs.end());
    }
  }
  for (const auto* s : suffix_sources) {
    prefix_sources.erase(s);
  }
  containers::FlatHashSet<const duckdb::Expression*> sources =
    std::move(dead_sources);
  sources.insert(prefix_sources.begin(), prefix_sources.end());

  auto remaining = RewriteExpr(expr, sources);
  return {ConstraintKind::Ranges, std::move(constraints), std::move(remaining)};
}

}  // namespace sdb::connector
