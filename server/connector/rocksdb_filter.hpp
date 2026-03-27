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
};

// A set of per-column range constraints over PK columns.
// Absent column in _column_filters means "any value" (unconstrained).
// A column with equal inclusive left/right bounds represents an equality
// predicate; asymmetric or half-open bounds represent range predicates.
class KeyConstraint {
 public:
  explicit KeyConstraint(std::span<const std::string> pk_names)
    : _pk_names{pk_names} {}

  KeyConstraint(const KeyConstraint& other)
    : _pk_names{other._pk_names}, _source_exprs{other._source_exprs} {
    for (const auto& [k, v] : other._column_filters) {
      _column_filters.emplace(k, std::make_unique<Range>(*v));
    }
  }

  KeyConstraint& operator=(const KeyConstraint& other) {
    if (this != &other) {
      _pk_names = other._pk_names;
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
    return _column_filters.empty();
  }

  // Returns nullptr if the column has no filter (matches any value).
  [[nodiscard]] const Range* FindFilter(std::string_view column_name) const;

  [[nodiscard]] std::span<const std::string> PkNames() const noexcept {
    return _pk_names;
  }

  // The expression nodes that produced this constraint.
  [[nodiscard]] const auto& GetSourceExprs() const noexcept {
    return _source_exprs;
  }

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

 private:
  std::span<const std::string> _pk_names;

  // TODO(mkornaukhov)
  // Range should really be much smaller, for now it's 64 bytes
  // But it should be just 2 varints + 4 bools -> 16 * 2 + 1 byte + alignment ->
  // 40 bytes. It should be OK to paste it into flat hash map
  containers::FlatHashMap<std::string, std::unique_ptr<Range>> _column_filters;
  containers::FlatHashSet<const velox::core::ITypedExpr*> _source_exprs;
};

// A fully resolved point: one variant per PK column, ordered by pk_type.
// Used after filter extraction — no expression metadata, no names.
using SpecificPoint = std::vector<velox::variant>;

// Converts specific (fully constrained) KeyConstraints to SpecificPoint,
// ordered by pk_type column order.
[[nodiscard]] std::vector<SpecificPoint> ToSpecificPoints(
  const std::vector<KeyConstraint>& points, const velox::RowType& pk_type);

[[nodiscard]] std::vector<KeyConstraint> ExtractFilterExpr(
  const velox::core::TypedExprPtr& expr, std::span<const std::string> pk_names);

struct ExtractAndRewriteResult {
  std::vector<KeyConstraint> points;
  // Rewritten filter with PK predicates replaced by true; null if the entire
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
