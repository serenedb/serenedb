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
#include "basics/system-compiler.h"

namespace sdb::connector {

enum class ComparisonOp { None, Lt, Le, Gt, Ge };

// TODO make empty range transparent
// A possibly-bounded interval on a single column value.
struct ColumnRange {
  static constexpr uint8_t kZero = 0x0;
  static constexpr uint8_t kLeftBounded = 0x1;
  static constexpr uint8_t kLeftInclusive = 0x2;
  static constexpr uint8_t kRightBounded = 0x4;
  static constexpr uint8_t kRightInclusive = 0x8;

  velox::variant left_value;   // valid iff HasLeft()
  velox::variant right_value;  // valid iff HasRight()
  uint8_t flags{kZero};

  [[nodiscard]] bool HasLeft() const noexcept { return flags & kLeftBounded; }
  [[nodiscard]] bool IsLeftInclusive() const noexcept {
    return flags & kLeftInclusive;
  }
  [[nodiscard]] bool HasRight() const noexcept { return flags & kRightBounded; }
  [[nodiscard]] bool IsRightInclusive() const noexcept {
    return flags & kRightInclusive;
  }

  // [v, v]
  [[nodiscard]] static ColumnRange Point(velox::variant v) {
    ColumnRange r;
    r.flags = kLeftBounded | kLeftInclusive | kRightBounded | kRightInclusive;
    r.left_value = v;
    r.right_value = std::move(v);
    return r;
  }

  // (v, +inf) or [v, +inf)
  [[nodiscard]] static ColumnRange LeftBound(velox::variant v, bool inclusive) {
    ColumnRange r;
    r.flags = kLeftBounded | (inclusive ? kLeftInclusive : kZero);
    r.left_value = std::move(v);
    return r;
  }

  // (-inf, v) or (-inf, v]
  [[nodiscard]] static ColumnRange RightBound(velox::variant v,
                                              bool inclusive) {
    ColumnRange r;
    r.flags = kRightBounded | (inclusive ? kRightInclusive : kZero);
    r.right_value = std::move(v);
    return r;
  }

  // Full two-sided interval
  [[nodiscard]] static ColumnRange Bounded(velox::variant left_value,
                                           bool left_inclusive,
                                           velox::variant right_value,
                                           bool right_inclusive) {
    ColumnRange r;
    r.flags = kLeftBounded | (left_inclusive ? kLeftInclusive : kZero) |
              kRightBounded | (right_inclusive ? kRightInclusive : kZero);
    r.left_value = std::move(left_value);
    r.right_value = std::move(right_value);
    return r;
  }

  // Returns true when the range represents a single exact value [v, v].
  [[nodiscard]] bool IsPoint() const noexcept {
    return HasLeft() && HasRight() && IsLeftInclusive() && IsRightInclusive() &&
           left_value == right_value;
  }

  // True iff both ranges represent the exact same interval.
  [[nodiscard]] bool operator==(const ColumnRange& other) const noexcept;

  // Tightest interval covering only keys in both ranges.
  // Returns nullopt when the result is empty (e.g. [1,1] ∩ [2,2]).
  [[nodiscard]] std::optional<ColumnRange> IntersectWith(
    const ColumnRange& other) const;

  // True iff the two ranges share at least one point.
  [[nodiscard]] bool OverlapsWith(const ColumnRange& other) const;

  // Smallest interval covering all keys in both ranges (may over-approximate
  // gaps).
  [[nodiscard]] ColumnRange UnionWith(const ColumnRange& other) const;

  // True iff *this starts strictly before other by left bound.
  // Unbounded left (-inf) sorts first; ties broken by inclusive < exclusive.
  [[nodiscard]] bool LeftBoundLessThan(const ColumnRange& other) const noexcept;

  // e.g. "[1, 5)", "(-inf, +inf)", "[3, +inf)"
  // NOLINTNEXTLINE(readability-identifier-naming)
  [[nodiscard]] std::string toString() const;
};

// A set of per-column range constraints over PK columns.
class KeyConstraint {
 public:
  [[nodiscard]] static KeyConstraint MakeAny(
    std::span<const std::string> pk_names) {
    return KeyConstraint{pk_names};
  }

  [[nodiscard]] static KeyConstraint MakeContradictory(
    std::span<const std::string> pk_names) {
    KeyConstraint kc{pk_names};
    kc._contradictory = true;
    return kc;
  }

  [[nodiscard]] bool IsSpecificPoint() const;

  [[nodiscard]] bool IsUnconstrained() const noexcept {
    return !_contradictory && _column_ranges.empty();
  }

  [[nodiscard]] bool IsContradictory() const noexcept { return _contradictory; }

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
  [[nodiscard]] static std::optional<KeyConstraint> TryIntersect(
    const KeyConstraint& lhs, const KeyConstraint& rhs);

  // Fast-path approximation: merges two constraints only when they differ on
  // exactly one column and the ranges on that column overlap. Returns nullopt
  // when the constraints differ on more than one column or have a gap on the
  // differing column. Use MergeKeyConstraintsPrecise for the general case.
  [[nodiscard]] static std::optional<KeyConstraint> TryUnion(
    const KeyConstraint& lhs, const KeyConstraint& rhs);

  // Constructs a KeyConstraint directly from pre-built column ranges and source
  // expressions. All columns absent from `ranges` are treated as unconstrained.
  // Used by the sweep-based merge algorithm.
  [[nodiscard]] static KeyConstraint BuildFromRanges(
    std::span<const std::string> pk_names,
    containers::FlatHashMap<std::string, ColumnRange> ranges,
    SourceExprsMap source_exprs);

 private:
  explicit KeyConstraint(std::span<const std::string> pk_names)
    : _pk_names{pk_names} {}

  std::span<const std::string> _pk_names;
  bool _contradictory =
    false;  // true -> contradictory predicate, matches no rows

  containers::FlatHashMap<std::string, ColumnRange> _column_ranges;
  SourceExprsMap _source_exprs;
};

// A fully resolved point: one variant per PK column, ordered by pk_type.
// Used after filter extraction -- no expression metadata, no names.
using ResolvedPoint = std::vector<velox::variant>;

// Converts specific (fully constrained) KeyConstraints to SpecificPoint,
// ordered by pk_type column order.
[[nodiscard]] std::vector<ResolvedPoint> ToResolvedPoints(
  const std::vector<KeyConstraint>& points, const velox::RowType& pk_type);

// A fully resolved range: first K exact PK column values (the equality prefix),
// followed by a Range for the (K+1)-th column.
// prefix.size() == K; K may be 0 if the range column is the first PK column.
struct ResolvedRange {
  std::vector<velox::variant> prefix;  // exact values for columns 0..K-1
  ColumnRange range_col;               // constraint on column K

  // Ordering: compare the effective (K+1)-element sequence element by element.
  // Each element is either a prefix value (exact) or, at position K, the
  // range_col left boundary (nullopt = -inf, sorts before any concrete value).
  // When two ranges share all elements of the shorter one -> ranges overlap,
  // which must not happen -- asserts.
  bool operator<(const ResolvedRange& other) const {
    // Compare prefix values (exact equality columns) first.
    const size_t common_prefix = std::min(prefix.size(), other.prefix.size());
    for (size_t i = 0; i < common_prefix; ++i) {
      if (prefix[i] < other.prefix[i]) {
        return true;
      }
      if (other.prefix[i] < prefix[i]) {
        return false;
      }
    }
    if (prefix.size() != other.prefix.size()) {
      // Unequal prefix lengths with a shared prefix means ranges overlap.
      SDB_UNREACHABLE();
    }

    // Same prefix length: compare left bounds of range_col.
    // Use LeftBoundLessThan so that ties on value are broken by inclusivity:
    // [v, ...] < (v, ...) because [v starts earlier.
    // Two ranges with identical left bounds overlap -- asserts.
    if (range_col.LeftBoundLessThan(other.range_col)) {
      return true;
    }
    if (other.range_col.LeftBoundLessThan(range_col)) {
      return false;
    }

    // Identical left bounds -> ranges overlap.
    SDB_UNREACHABLE();
  }
};

// Converts range KeyConstraints to ResolvedRange, ordered by pk_type column
// order. Each constraint must have PrefixSize() >= 1.
[[nodiscard]] std::vector<ResolvedRange> ToResolvedRanges(
  const std::vector<KeyConstraint>& ranges, const velox::RowType& pk_type);

[[nodiscard]] std::vector<KeyConstraint> ExtractFilterExpr(
  const velox::core::TypedExprPtr& expr, std::span<const std::string> pk_names);

enum class ConstraintKind {
  // All constraints are fully-specified equality points; use point lookup.
  Points,
  // At least one constraint is a range; use range scan on the range prefix.
  Ranges,
  // No constraints, use full scan.
  None
};

struct ExtractAndRewriteResult {
  ConstraintKind kind;
  std::vector<KeyConstraint> constraints;
  // Rewritten filter with captured PK predicates removed; null if the entire
  // expression reduced to true.
  velox::core::TypedExprPtr remaining_filter;
};

[[nodiscard]] ExtractAndRewriteResult ExtractAndRewriteFilterExpr(
  const velox::core::TypedExprPtr& expr, std::span<const std::string> pk_names);

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
