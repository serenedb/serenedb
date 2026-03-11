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
#include <velox/vector/ComplexVector.h>

#include <span>
#include <string>
#include <vector>

#include "basics/fwd.h"

namespace sdb::connector {

// A point in PK-space. Absent column in _column_filters means "any value".
class Point {
 public:
  explicit Point(std::span<const std::string> pk_names) : _pk_names{pk_names} {}

  [[nodiscard]] bool IsSpecific() const;

  // Returns true when the point carries no column constraints (matches any
  // row).
  [[nodiscard]] bool IsUnconstrained() const noexcept {
    return _column_filters.empty();
  }

  // Returns nullptr if the column has no filter (matches any value).
  [[nodiscard]] const velox::core::ConstantTypedExpr* FindFilter(
    std::string_view column_name) const;

  [[nodiscard]] std::span<const std::string> PkNames() const noexcept {
    return _pk_names;
  }

  // The expression nodes that specify this point
  [[nodiscard]] const auto& GetSourceExprs() const noexcept {
    return _source_exprs;
  }

  std::string ToString() const {
    std::string resp;

    // for (std::string_view column_fi)

    return resp;
  }

  void AddEqFilter(std::string_view column_name,
                   velox::core::ConstantTypedExprPtr value,
                   const velox::core::ITypedExpr* source_expr);

  // Returns nullopt when the two points are contradictory (e.g. a=1 AND a=2).
  [[nodiscard]] static std::optional<Point> Intersect(const Point& lhs,
                                                      const Point& rhs);

 private:
  std::span<const std::string> _pk_names;
  // absl::flat_hash_map<std::string_view, velox::core::ConstantTypedExprPtr>
  absl::flat_hash_map<std::string, velox::core::ConstantTypedExprPtr>
    _column_filters;
  absl::flat_hash_set<const velox::core::ITypedExpr*> _source_exprs;
};

// Materialises a point set into a velox RowVector using the supplied memory
// pool.
[[nodiscard]] velox::RowVectorPtr PointsToRowVector(
  const std::vector<Point>& points, velox::RowTypePtr pk_type,
  velox::memory::MemoryPool* pool);

// Extracts PK points from a filter expression. Returns a vector with a single
// unconstrained Point (IsUnconstrained() == true) when the expression cannot be
// reduced to a finite point set — callers should fall back to a full scan.
[[nodiscard]] std::vector<Point> ExtractFilterExpr(
  const velox::core::TypedExprPtr& expr, std::span<const std::string> pk_names);

// Calls ExtractFilterExpr. When all returned points are specific, rewrites
// `expr` by replacing each point's source sub-expression with a constant `true`
// (the PK lookup covers those predicates) and returns the rewritten tree.
// When not all points are specific, returns `expr` unchanged.
// The returned TypedExprPtr is null when the rewritten filter is trivially
// true.
struct ExtractAndRewriteResult {
  std::vector<Point> points;
  // Rewritten filter with PK predicates replaced by true; null if the entire
  // expression reduced to true.
  velox::core::TypedExprPtr remaining_filter;
};
[[nodiscard]] ExtractAndRewriteResult ExtractAndRewriteFilterExpr(
  const velox::core::TypedExprPtr& expr, std::span<const std::string> pk_names);

// Sorts points in-place by serialized PK key order using pk_type for
// authoritative column ordering. Comparison uses velox::variant::operator<,
// which matches RocksDB byte ordering for all supported PK types.
void SortPoints(std::vector<Point>& points, const velox::RowType& pk_type);

}  // namespace sdb::connector
