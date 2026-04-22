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

#include <cassert>
#include <duckdb/common/types/value.hpp>
#include <duckdb/planner/expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/containers/node_hash_map.h"
#include "catalog/table.h"

namespace sdb::connector {

enum class ComparisonOp { None, Lt, Le, Gt, Ge };

// A possibly-bounded interval on a single column value.
struct ColumnRange {
  enum Flags : uint8_t {
    kZero = 0x00,
    kLeftBounded = 0x01,
    kLeftInclusive = 0x02,
    kRightBounded = 0x04,
    kRightInclusive = 0x08,
    // The range is empty (no value satisfies it, e.g. pk > 5 AND pk < 3),
    // matching absolutely no rows -- not even NULL.
    // Invariant: kEmptyRange and kMaybeNull are never both set.
    kEmptyRange = 0x10,
    kMaybeNull = 0x20,
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
  // kEmptyRange matches nothing, not even NULL.
  [[nodiscard]] bool IsEmpty() const noexcept { return _flags & kEmptyRange; }

  [[nodiscard]] bool MaybeNull() const noexcept { return _flags & kMaybeNull; }

  // True iff this is exactly the null sentinel bucket with no value interval
  // (i.e. kMaybeNull is set and no bound flags are present).
  [[nodiscard]] bool IsNullOnly() const noexcept {
    return MaybeNull() && !HasLeft() && !HasRight();
  }

  [[nodiscard]] const duckdb::Value& LeftValue() const noexcept {
    return _left_value;
  }
  [[nodiscard]] const duckdb::Value& RightValue() const noexcept {
    return _right_value;
  }

  // [v, v]
  [[nodiscard]] static ColumnRange Point(duckdb::Value v) {
    return Make(kLeftBounded | kLeftInclusive | kRightBounded | kRightInclusive,
                v, std::move(v));
  }

  // (v, +inf) or [v, +inf)
  [[nodiscard]] static ColumnRange LeftBound(duckdb::Value v, bool inclusive) {
    return Make(kLeftBounded | (inclusive ? kLeftInclusive : kZero),
                std::move(v), duckdb::Value{});
  }

  // (-inf, v) or (-inf, v]
  [[nodiscard]] static ColumnRange RightBound(duckdb::Value v, bool inclusive) {
    return Make(kRightBounded | (inclusive ? kRightInclusive : kZero),
                duckdb::Value{}, std::move(v));
  }

  // Empty range -- matches no value.
  [[nodiscard]] static ColumnRange Empty() {
    return Make(kEmptyRange, duckdb::Value{}, duckdb::Value{});
  }

  // Matches only the NULL sentinel bucket (0x01 prefix): col IS NULL.
  // kMaybeNull set, no bound flags -- no value interval.
  [[nodiscard]] static ColumnRange NullOnly() {
    return Make(kMaybeNull, duckdb::Value{}, duckdb::Value{});
  }

  // Unbounded non-null range: any non-NULL value. Same encoding as a default
  // ColumnRange{} -- named explicitly for clarity in SK contexts.
  [[nodiscard]] static ColumnRange AnyNotNull() { return ColumnRange{}; }

  // Full two-sided interval
  [[nodiscard]] static ColumnRange Bounded(duckdb::Value left_value,
                                           bool left_inclusive,
                                           duckdb::Value right_value,
                                           bool right_inclusive) {
    return Make(kLeftBounded | (left_inclusive ? kLeftInclusive : kZero) |
                  kRightBounded | (right_inclusive ? kRightInclusive : kZero),
                std::move(left_value), std::move(right_value));
  }

  // Returns true when the range is a closed non-null singleton [v, v].
  // Returns false for NullOnly() and Empty().
  [[nodiscard]] bool IsNonNullPoint() const noexcept {
    return !(_flags & (kEmptyRange | kMaybeNull)) && HasLeft() && HasRight() &&
           IsLeftInclusive() && IsRightInclusive() &&
           _left_value == _right_value;
  }

  // True iff both ranges represent the exact same interval.
  [[nodiscard]] bool operator==(const ColumnRange& other) const;

  // Tightest interval covering only keys in both ranges.
  // Returns nullopt when the result is empty (e.g. [1,1] /\ [2,2]).
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

  template<typename H>
  friend H AbslHashValue(H h, const ColumnRange& cr) {
    h = H::combine(std::move(h), cr._flags);
    if (cr.HasLeft()) {
      h = H::combine(std::move(h), cr._left_value.Hash());
    }
    if (cr.HasRight()) {
      h = H::combine(std::move(h), cr._right_value.Hash());
    }
    return h;
  }

 private:
  [[nodiscard]] static ColumnRange Make(uint8_t f, duckdb::Value l,
                                        duckdb::Value r) {
    ColumnRange cr;
    cr._flags = f;
    cr._left_value = std::move(l);
    cr._right_value = std::move(r);
    return cr;
  }

  duckdb::Value _left_value;   // valid iff HasLeft()
  duckdb::Value _right_value;  // valid iff HasRight()
  uint8_t _flags{kZero};
};

// A set of per-column range bound constraints over PK columns.
class KeyBounds {
 public:
  [[nodiscard]] static KeyBounds MakeAny(
    std::span<const catalog::Column::Id> pk_ids) {
    return KeyBounds{pk_ids};
  }

  [[nodiscard]] static KeyBounds MakeContradictory(
    std::span<const catalog::Column::Id> pk_ids) {
    KeyBounds kc{pk_ids};
    kc._is_empty = true;
    return kc;
  }

  // Returns true only when every key column has a non-null singleton range.
  // A NullOnly constraint causes this to return false.
  [[nodiscard]] bool IsResolvedNonNullPoint() const;

  [[nodiscard]] bool IsUnconstrained() const noexcept {
    return !_is_empty && _column_ranges.empty();
  }

  [[nodiscard]] bool IsEmpty() const noexcept { return _is_empty; }

  // Returns the number of PK columns covered by the constraint's leading
  // prefix, or 0 if the constraint cannot drive a useful range scan.
  [[nodiscard]] size_t RangePrefixSize() const noexcept;

  // Returns nullptr if the column has no filter range (matches any value).
  [[nodiscard]] const ColumnRange* FindColumnRange(
    catalog::Column::Id col_id) const;

  [[nodiscard]] std::span<const catalog::Column::Id> PKColumns()
    const noexcept {
    return _pk_ids;
  }

  using SourceExprsMap =
    containers::FlatHashMap<catalog::Column::Id,
                            containers::FlatHashSet<const duckdb::Expression*>>;

  // Returns all source expressions grouped by the column they constrain.
  [[nodiscard]] const SourceExprsMap& GetSourceExprs() const noexcept {
    return _source_exprs;
  }

  // Returns source expressions for a single column (empty set if
  // unconstrained).
  [[nodiscard]] const containers::FlatHashSet<const duckdb::Expression*>&
  GetSourceExprs(catalog::Column::Id col_id) const noexcept;

  // e.g. "{col42: [1, 5), col7: (-inf, +inf)}" or "{}" when unconstrained.
  // NOLINTNEXTLINE(readability-identifier-naming)
  [[nodiscard]] std::string toString() const;

  void AddEqFilter(catalog::Column::Id col_id, duckdb::Value value,
                   const duckdb::Expression* source_expr);

  // SK IS NULL: pins col_id to the null sentinel bucket.
  void AddNullFilter(catalog::Column::Id col_id,
                     const duckdb::Expression* source_expr);

  // SK IS NOT NULL: restricts col_id to non-null values only.
  void AddNotNullFilter(catalog::Column::Id col_id,
                        const duckdb::Expression* source_expr);

  void AddComparisonFilter(catalog::Column::Id col_id, duckdb::Value value,
                           ComparisonOp op,
                           const duckdb::Expression* source_expr);

  // Returns nullopt when the two constraints are contradictory (e.g. a=1 AND
  // a=2).
  [[nodiscard]] static std::optional<KeyBounds> TryIntersect(
    const KeyBounds& lhs, const KeyBounds& rhs);

  // Constructs a KeyConstraint directly from pre-built column ranges and source
  // expressions. All columns absent from `ranges` are treated as unconstrained.
  // Used by the sweep-based merge algorithm.
  // NodeHashMap keeps node pointers stable and sidesteps FlatHashMap's
  // sizeof(K)+sizeof(V) <= 80 limit: ColumnRange holds two duckdb::Value
  // (~100+ bytes total), too large for a flat bucket.
  using ColumnRangeMap =
    containers::NodeHashMap<catalog::Column::Id, ColumnRange>;

  [[nodiscard]] static KeyBounds BuildFromRanges(
    std::span<const catalog::Column::Id> pk_ids, ColumnRangeMap ranges,
    SourceExprsMap source_exprs);

 private:
  explicit KeyBounds(std::span<const catalog::Column::Id> pk_ids)
    : _pk_ids{pk_ids} {}

  std::span<const catalog::Column::Id> _pk_ids;
  // true -> contradictory predicate, matches no rows
  bool _is_empty = false;

  ColumnRangeMap _column_ranges;
  SourceExprsMap _source_exprs;
};

// A fully resolved point: one Value per PK column, ordered by pk_ids order.
// Used after filter extraction -- no expression metadata, no names.
using ResolvedPoint = std::vector<duckdb::Value>;

// Converts specific (fully constrained) Points to Resolved, ordered by
// pk_ids column order.
std::vector<ResolvedPoint> ToResolvedPoints(
  const std::vector<KeyBounds>& points,
  std::span<const catalog::Column::Id> column_ids);

// A fully resolved range: first K exact PK column values (the equality prefix),
// followed by a Range for the (K+1)-th column.
// prefix.size() == K; K may be 0 if the range column is the first PK column.
struct ResolvedRange {
  std::vector<duckdb::Value> prefix;  // exact values for columns 0..K-1
  ColumnRange range_column;           // constraint on column K

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

// Column resolver: maps DuckDB BoundColumnRefExpression bindings to
// catalog::Column::Id.
//
// `projected_column_ids` is indexed by the filter expression's
// `binding.column_index` and returns the catalog Column::Id of that
// projected column. The caller is responsible for pre-translating through
// any projection-pushdown reordering -- i.e. it should compose
// `bind_data.column_ids` with `LogicalGet::GetColumnIds()` so the span
// matches the bindings seen in filter expressions.
struct ColumnResolver {
  duckdb::TableIndex table_index;
  std::span<const catalog::Column::Id> projected_column_ids;

  // Returns std::numeric_limits<catalog::Column::Id>::max() if ref isn't a
  // column of our scan.
  [[nodiscard]] catalog::Column::Id Resolve(
    const duckdb::BoundColumnRefExpression& ref) const;
};

// Converts range KeyConstraints to ResolvedRange, ordered by pk_ids column
// order. Each constraint must have PrefixSize() >= 1.
[[nodiscard]] std::vector<ResolvedRange> ToDisjointRanges(
  const std::vector<KeyBounds>& ranges,
  std::span<const catalog::Column::Id> pk_ids);

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
  duckdb::unique_ptr<duckdb::Expression> remaining_filter;
};

[[nodiscard]] ExtractAndRewriteResult ExtractAndRewriteFilterExpr(
  const duckdb::Expression& expr, std::span<const catalog::Column::Id> pk_ids,
  const ColumnResolver& resolver, bool is_primary_key, bool is_unique);

// Sorts and deduplicates points in-place by key order. Column order matches
// the pk_ids used during ToResolvedPoints.
void SortAndDedupPoints(std::vector<ResolvedPoint>& points);

}  // namespace sdb::connector
