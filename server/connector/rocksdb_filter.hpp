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

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <velox/core/Expressions.h>
#include <velox/core/ITypedExpr.h>
#include <velox/type/Variant.h>
#include <velox/vector/ComplexVector.h>

#include <cassert>
#include <span>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/fwd.h"

namespace sdb::connector {

enum class ComparisonOp { None, Lt, Le, Gt, Ge };

// A possibly-bounded interval on a single column value.
struct ColumnRange {
  enum Flags : uint8_t {
    kZero = 0x0,
    kLeftBounded = 0x1,
    kLeftInclusive = 0x2,
    kRightBounded = 0x4,
    kRightInclusive = 0x8,
    // The range is empty (no value satisfies it, e.g. pk > 5 AND
    // pk < 3). We need to keep it to distiguish between filter absence and
    // contradictory filters, like a>5 AND pk < 3.
    kEmptyRange = 0x10,
  };

  [[nodiscard]] bool HasLeft() const noexcept { return _flags & kLeftBounded; }
  [[nodiscard]] bool IsLeftInclusive() const noexcept {
    return _flags & kLeftInclusive;
  }
  [[nodiscard]] bool HasRight() const noexcept {
    return _flags & kRightBounded;
  }
  [[nodiscard]] bool IsRightInclusive() const noexcept {
    return _flags & kRightInclusive;
  }
  [[nodiscard]] bool IsEmpty() const noexcept { return _flags & kEmptyRange; }

  [[nodiscard]] const velox::variant& LeftValue() const noexcept {
    return _left_value;
  }
  [[nodiscard]] const velox::variant& RightValue() const noexcept {
    return _right_value;
  }

  // [v, v]
  [[nodiscard]] static ColumnRange Point(velox::variant v) {
    return Make(kLeftBounded | kLeftInclusive | kRightBounded | kRightInclusive,
                v, std::move(v));
  }

  // (v, +inf) or [v, +inf)
  [[nodiscard]] static ColumnRange LeftBound(velox::variant v, bool inclusive) {
    return Make(kLeftBounded | (inclusive ? kLeftInclusive : kZero),
                std::move(v), {});
  }

  // (-inf, v) or (-inf, v]
  [[nodiscard]] static ColumnRange RightBound(velox::variant v,
                                              bool inclusive) {
    return Make(kRightBounded | (inclusive ? kRightInclusive : kZero), {},
                std::move(v));
  }

  // Empty range -- matches no value.
  [[nodiscard]] static ColumnRange Empty() { return Make(kEmptyRange, {}, {}); }

  // Full two-sided interval
  [[nodiscard]] static ColumnRange Bounded(velox::variant left_value,
                                           bool left_inclusive,
                                           velox::variant right_value,
                                           bool right_inclusive) {
    return Make(kLeftBounded | (left_inclusive ? kLeftInclusive : kZero) |
                  kRightBounded | (right_inclusive ? kRightInclusive : kZero),
                std::move(left_value), std::move(right_value));
  }

  // Returns true when the range represents a single exact value [v, v].
  [[nodiscard]] bool IsPoint() const noexcept {
    return HasLeft() && HasRight() && IsLeftInclusive() && IsRightInclusive() &&
           _left_value == _right_value;
  }

  // True iff both ranges represent the exact same interval.
  [[nodiscard]] bool operator==(const ColumnRange& other) const noexcept =
    default;

  // Tightest interval covering only keys in both ranges.
  // Returns nullopt when the result is empty (e.g. [1,1] ∩ [2,2]).
  [[nodiscard]] std::optional<ColumnRange> IntersectWith(
    const ColumnRange& other) const;

  // True iff the two ranges share at least one point.
  [[nodiscard]] bool OverlapsWith(const ColumnRange& other) const;

  // True iff *this starts strictly before other by left bound.
  // Unbounded left (-inf) sorts first; ties broken by inclusive < exclusive.
  [[nodiscard]] bool LeftBoundLessThan(const ColumnRange& other) const noexcept;

  // e.g. "[1, 5)", "(-inf, +inf)", "[3, +inf)"
  // NOLINTNEXTLINE(readability-identifier-naming)
  [[nodiscard]] std::string toString() const;

 private:
  [[nodiscard]] static ColumnRange Make(uint8_t f, velox::variant l,
                                        velox::variant r) {
    ColumnRange cr;
    cr._flags = f;
    cr._left_value = std::move(l);
    cr._right_value = std::move(r);
    return cr;
  }

  velox::variant _left_value;   // valid iff HasLeft()
  velox::variant _right_value;  // valid iff HasRight()
  uint8_t _flags{kZero};
};

// A set of per-column range bound constraints over PK columns.
class KeyBounds {
 public:
  [[nodiscard]] static KeyBounds MakeAny(
    std::span<const std::string> pk_names) {
    return KeyBounds{pk_names};
  }

  [[nodiscard]] static KeyBounds MakeContradictory(
    std::span<const std::string> pk_names) {
    KeyBounds kc{pk_names};
    kc._is_empty = true;
    return kc;
  }

  [[nodiscard]] bool IsResolvedPoint() const;

  [[nodiscard]] bool IsUnconstrained() const noexcept {
    return !_is_empty && _column_ranges.empty();
  }

  [[nodiscard]] bool IsEmpty() const noexcept { return _is_empty; }

  // Returns the number of PK columns covered by the constraint's leading
  // prefix, or 0 if the constraint cannot drive a useful range scan.
  [[nodiscard]] size_t RangePrefixSize() const noexcept;

  // Returns nullptr if the column has no filter range (matches any value).
  [[nodiscard]] const ColumnRange* FindColumnRange(
    std::string_view column_name) const;

  [[nodiscard]] std::span<const std::string> PKNames() const noexcept {
    return _pk_names;
  }

  using SourceExprsMap = containers::FlatHashMap<
    std::string, containers::FlatHashSet<const velox::core::ITypedExpr*>>;

  // Returns all source expressions grouped by the column they constrain.
  [[nodiscard]] const SourceExprsMap& GetSourceExprs() const noexcept {
    return _source_exprs;
  }

  // Returns source expressions for a single column (empty set if
  // unconstrained).
  [[nodiscard]] const containers::FlatHashSet<const velox::core::ITypedExpr*>&
  GetSourceExprs(std::string_view column_name) const noexcept;

  // e.g. "{a: [1, 5), b: (-inf, +inf)}" or "{}" when unconstrained.
  // NOLINTNEXTLINE(readability-identifier-naming)
  [[nodiscard]] std::string toString() const;

  void AddEqFilter(std::string_view column_name,
                   const velox::core::ConstantTypedExpr& value,
                   const velox::core::ITypedExpr* source_expr);

  void AddComparisonFilter(std::string_view column_name,
                           const velox::core::ConstantTypedExpr& value,
                           ComparisonOp op,
                           const velox::core::ITypedExpr* source_expr);

  // Returns nullopt when the two constraints are contradictory (e.g. a=1 AND
  // a=2).
  [[nodiscard]] static std::optional<KeyBounds> TryIntersect(
    const KeyBounds& lhs, const KeyBounds& rhs);

  // Constructs a KeyConstraint directly from pre-built column ranges and source
  // expressions. All columns absent from `ranges` are treated as unconstrained.
  // Used by the sweep-based merge algorithm.
  [[nodiscard]] static KeyBounds BuildFromRanges(
    std::span<const std::string> pk_names,
    containers::FlatHashMap<std::string, ColumnRange> ranges,
    SourceExprsMap source_exprs);

 private:
  explicit KeyBounds(std::span<const std::string> pk_names)
    : _pk_names{pk_names} {}

  std::span<const std::string> _pk_names;
  // true -> contradictory predicate, matches no rows
  bool _is_empty = false;

  containers::FlatHashMap<std::string, ColumnRange> _column_ranges;
  SourceExprsMap _source_exprs;
};

// A fully resolved point: one variant per PK column, ordered by pk_type.
// Used after filter extraction -- no expression metadata, no names.
using ResolvedPoint = std::vector<velox::variant>;

// Converts specific (fully constrained) Points to Resolved, ordered by
// pk_type column order.
std::vector<ResolvedPoint> ToResolvedPoints(
  const std::vector<KeyBounds>& points,
  std::span<const std::string> column_names);

// A fully resolved range: first K exact PK column values (the equality prefix),
// followed by a Range for the (K+1)-th column.
// prefix.size() == K; K may be 0 if the range column is the first PK column.
struct ResolvedRange {
  std::vector<velox::variant> prefix;  // exact values for columns 0..K-1
  ColumnRange range_column;            // constraint on column K

  // A sentinel that represents a contradictory predicate (no rows match).
  [[nodiscard]] static ResolvedRange Conflicting() {
    return {{}, ColumnRange::Empty()};
  }

  [[nodiscard]] bool IsEmpty() const noexcept { return range_column.IsEmpty(); }

  // Ordering: compare the leftmost key covered by each range.
  // Contradictory ranges sort before all real ranges.
  // Walk column by column; a prefix value is exact (inclusive point), and at
  // the range column we compare the range_col left bound.  When the two ranges
  // have different prefix depths but share the common prefix, the shorter
  // range's range_col is compared against the exact prefix value of the longer
  // range at that position.
  bool operator<(const ResolvedRange& other) const {
    if (IsEmpty() || other.IsEmpty()) {
      return IsEmpty() && !other.IsEmpty();
    }
    const auto min_depth = std::min(prefix.size(), other.prefix.size());
    for (size_t i = 0; i < min_depth; ++i) {
      if (prefix[i] != other.prefix[i]) {
        return prefix[i] < other.prefix[i];
      }
    }

    const auto left_at_split = (prefix.size() == min_depth)
                                 ? range_column
                                 : ColumnRange::Point(prefix[min_depth]);
    const auto right_at_split = (other.prefix.size() == min_depth)
                                  ? other.range_column
                                  : ColumnRange::Point(other.prefix[min_depth]);
    return left_at_split.LeftBoundLessThan(right_at_split);
  }
};

// Converts range KeyConstraints to ResolvedRange, ordered by pk_type column
// order. Each constraint must have PrefixSize() >= 1.
[[nodiscard]] std::vector<ResolvedRange> ToDisjointRanges(
  const std::vector<KeyBounds>& ranges, const velox::RowType& pk_type);

[[nodiscard]] std::vector<KeyBounds> ExtractFilterExpr(
  const velox::core::TypedExprPtr& expr, std::span<const std::string> pk_names,
  bool negated = false);

enum class ConstraintKind {
  // All constraints are fully-specified equality points; use point lookup.
  Points,
  // At least one constraint is a range; use range scan on the range prefix.
  Ranges,
  // No constraints, use full scan.
  None,
};

struct ExtractAndRewriteResult {
  ConstraintKind kind;
  std::vector<KeyBounds> constraints;
  // Rewritten filter with captured PK predicates removed; null if the entire
  // expression reduced to true.
  velox::core::TypedExprPtr remaining_filter;
};

[[nodiscard]] ExtractAndRewriteResult ExtractAndRewriteFilterExpr(
  const velox::core::TypedExprPtr& expr,
  std::span<const std::string> column_names);

// Sorts and deduplicates points in-place by key order. Column order matches
// the pk_type used during ToResolvedPoints.
void SortAndDedupPoints(std::vector<ResolvedPoint>& points);

// Returns true if `call` matches a velox function named either `suffix[1:]`
// (bare name, e.g. "eq") or anything ending with `suffix` (prefixed name, e.g.
// "presto_eq"). `suffix` must start with '_'.
[[nodiscard]] inline bool IsCallOf(const velox::core::CallTypedExpr* call,
                                   std::string_view suffix) {
  SDB_ASSERT(!suffix.empty() && suffix[0] == '_');
  const auto& name = call->name();
  return name == suffix.substr(1) || name.ends_with(suffix);
}

}  // namespace sdb::connector
