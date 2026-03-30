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
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/fwd.h"

namespace sdb::connector {

struct Boundary {
  velox::variant value;
  bool inclusive;
};

enum class ComparisonOp { None, Lt, Le, Gt, Ge };

// nullopt left/right means unbounded on that side.
struct Range {
  std::optional<Boundary> left;
  std::optional<Boundary> right;

  // Returns true when the range represents a single exact value [v, v].
  [[nodiscard]] bool IsPoint() const noexcept {
    return left.has_value() && right.has_value() &&
           left->value == right->value && left->inclusive && right->inclusive;
  }
};

// e.g. "[1, 5)", "(-inf, +inf)", "[3, +inf)"
[[nodiscard]] std::string ToString(const Range& range);

// A set of per-column range constraints over PK columns.
// Absent column in _column_filters means "any value" (unconstrained).
// A column with equal inclusive left/right bounds represents an equality
// predicate; asymmetric or half-open bounds represent range predicates.
class KeyConstraint {
 public:
  explicit KeyConstraint(std::span<const std::string> pk_names)
    : _pk_names{pk_names} {}

  KeyConstraint(const KeyConstraint& other)
    : _pk_names{other._pk_names},
      _contradictory{other._contradictory},
      _source_exprs{other._source_exprs} {
    for (const auto& [k, v] : other._column_filters) {
      _column_filters.emplace(k, std::make_unique<Range>(*v));
    }
  }

  KeyConstraint& operator=(const KeyConstraint& other) {
    if (this != &other) {
      _pk_names = other._pk_names;
      _contradictory = other._contradictory;
      _source_exprs = other._source_exprs;
      _column_filters.clear();
      for (const auto& [k, v] : other._column_filters) {
        _column_filters.emplace(k, std::make_unique<Range>(*v));
      }
    }
    return *this;
  }

  KeyConstraint(KeyConstraint&&) = default;
  KeyConstraint& operator=(KeyConstraint&&) = default;

  [[nodiscard]] bool IsSpecific() const;

  [[nodiscard]] bool IsUnconstrained() const noexcept {
    return !_contradictory && _column_filters.empty();
  }

  // True when the constraint is contradictory (e.g. a < 1 AND a > 1) and
  // can never be satisfied — the scan should produce zero rows.
  [[nodiscard]] bool IsContradictory() const noexcept { return _contradictory; }

  // Creates a void (impossible) constraint.
  [[nodiscard]] static KeyConstraint MakeContradictory(
    std::span<const std::string> pk_names) {
    KeyConstraint kc{pk_names};
    kc._contradictory = true;
    return kc;
  }

  // Returns the number of PK columns covered by the constraint's leading
  // prefix, or 0 if the constraint cannot drive a useful range scan. The prefix
  // is: K equality-point columns followed by at most one range column. K must
  // be ≥ 1 (a bare range on the first column with no equality prefix yields 0).
  // Examples (PK = a, b, c):
  //   b > 5              → 0  (a unconstrained → no usable prefix)
  //   a > 5              → 0  (K=0, no equality prefix)
  //   a = 1              → 1  (one equality, nothing after)
  //   a = 1 AND b > 5    → 2  (a point, b range)
  //   a = 1 AND b = 2    → 2  (both points; note: IsSpecific handles all-point)
  //   a = 1 AND b = 2 AND c > 3 → 3
  [[nodiscard]] size_t PrefixSize() const noexcept;

  // Returns nullptr if the column has no filter (matches any value).
  [[nodiscard]] const Range* FindFilter(std::string_view column_name) const;

  [[nodiscard]] std::span<const std::string> PkNames() const noexcept {
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
  [[nodiscard]] std::string ToString() const;

  void AddEqFilter(std::string_view column_name,
                   const velox::core::ConstantTypedExpr& value,
                   const velox::core::ITypedExpr* source_expr);

  void AddComparisonFilter(std::string_view column_name,
                           const velox::core::ConstantTypedExpr& value,
                           ComparisonOp op,
                           const velox::core::ITypedExpr* source_expr);

  // Returns nullopt when the two constraints are contradictory (e.g. a=1 AND
  // a=2).
  [[nodiscard]] static std::optional<KeyConstraint> Intersect(
    const KeyConstraint& lhs, const KeyConstraint& rhs);

  // Returns nullopt when the constraints cannot be merged into one — i.e. they
  // differ on more than one column, or their ranges on the differing column
  // have a gap. Otherwise returns a single constraint covering the union of
  // both key-spaces.
  [[nodiscard]] static std::optional<KeyConstraint> TryUnion(
    const KeyConstraint& lhs, const KeyConstraint& rhs);

  // Always merges two constraints into one by taking the widest bounding range
  // per column. May over-approximate: the result can cover key-space not in
  // either input (e.g. [2,10) ∪ (40,44] → [2,44]).
  [[nodiscard]] static KeyConstraint ForceUnion(const KeyConstraint& lhs,
                                                const KeyConstraint& rhs);

 private:
  std::span<const std::string> _pk_names;
  bool _contradictory =
    false;  // true → contradictory predicate, matches no rows

  // TODO(mkornaukhov)
  // Range should really be much smaller, for now it's 64 bytes
  // But it should be just 2 varints + 4 bools -> 16 * 2 + 1 byte + alignment ->
  // 40 bytes. It should be OK to paste it into flat hash map
  containers::FlatHashMap<std::string, std::unique_ptr<Range>> _column_filters;
  SourceExprsMap _source_exprs;
};

// A fully resolved point: one variant per PK column, ordered by pk_type.
// Used after filter extraction — no expression metadata, no names.
using SpecificPoint = std::vector<velox::variant>;

// Converts specific (fully constrained) KeyConstraints to SpecificPoint,
// ordered by pk_type column order.
[[nodiscard]] std::vector<SpecificPoint> ToSpecificPoints(
  const std::vector<KeyConstraint>& points, const velox::RowType& pk_type);

// A fully resolved range: first K exact PK column values (the equality prefix),
// followed by a Range for the (K+1)-th column.
// prefix.size() == K; K may be 0 if the range column is the first PK column.
// Analogous to SpecificPoint but for range scans.
struct SpecificRange {
  std::vector<velox::variant> prefix;  // exact values for columns 0..K-1
  Range range_col;                     // constraint on column K

  // Ordering: compare the effective (K+1)-element sequence element by element.
  // Each element is either a prefix value (exact) or, at position K, the
  // range_col left boundary (nullopt = -inf, sorts before any concrete value).
  // When two ranges share all elements of the shorter one → ranges overlap,
  // which must not happen — asserts.
  bool operator<(const SpecificRange& other) const {
    // Value at position i: prefix[i] for i < K, or range_col.left for i == K.
    auto val_at = [](const SpecificRange& sr,
                     size_t i) -> const velox::variant* {
      if (i < sr.prefix.size()) {
        return &sr.prefix[i];
      }
      return sr.range_col.left ? &sr.range_col.left->value : nullptr;
    };

    const size_t len = prefix.size() + 1;  // this range's sequence length
    const size_t other_len = other.prefix.size() + 1;
    const size_t common = std::min(len, other_len);

    for (size_t i = 0; i < common; ++i) {
      const velox::variant* a = val_at(*this, i);
      const velox::variant* b = val_at(other, i);
      const bool a_inf = (a == nullptr);
      const bool b_inf = (b == nullptr);
      if (a_inf != b_inf) {
        return a_inf;  // -inf < concrete value
      }
      if (a_inf) {
        return false;  // both -inf at same pos: equal here
      }
      if (*a < *b) {
        return true;
      }
      if (*b < *a) {
        return false;
      }
    }

    // All common elements equal — one range is a structural prefix of the
    // other, meaning they overlap. This is an invalid sorted range list.
    SDB_ASSERT(false,
               "SpecificRange ordering: one range is a prefix of another");
    return false;
  }
};

// Converts range KeyConstraints to SpecificRange, ordered by pk_type column
// order. Each constraint must have PrefixSize() >= 1.
[[nodiscard]] std::vector<SpecificRange> ToSpecificRanges(
  const std::vector<KeyConstraint>& ranges, const velox::RowType& pk_type);

[[nodiscard]] std::vector<KeyConstraint> ExtractFilterExpr(
  const velox::core::TypedExprPtr& expr, std::span<const std::string> pk_names);

enum class ConstraintKind {
  // All constraints are fully-specified equality points; use point lookup.
  Points,
  // At least one constraint is a range; use range scan on the first PK column.
  Ranges,
  Empty
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

// Sorts points in-place by PK key order. Column order matches the pk_type used
// during ToSpecificPoints. Comparison uses velox::variant::operator<.
void SortPoints(std::vector<SpecificPoint>& points);

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
