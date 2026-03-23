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

#include <span>
#include <string>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/fwd.h"

namespace sdb::connector {

// A point in PK-space. Absent column in _column_filters means "any value".
class Point {
 public:
  explicit Point(std::span<const std::string> pk_names) : _pk_names{pk_names} {}

  [[nodiscard]] bool IsSpecific() const;

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

  void AddEqFilter(std::string_view column_name,
                   velox::core::ConstantTypedExprPtr value,
                   const velox::core::ITypedExpr* source_expr);

  // Returns nullopt when the two points are contradictory (e.g. a=1 AND a=2).
  [[nodiscard]] static std::optional<Point> Intersect(const Point& lhs,
                                                      const Point& rhs);

 private:
  std::span<const std::string> _pk_names;
  containers::FlatHashMap<std::string, velox::core::ConstantTypedExprPtr>
    _column_filters;
  containers::FlatHashSet<const velox::core::ITypedExpr*> _source_exprs;
};

// A fully resolved point: one variant per PK column, ordered by pk_type.
// Used after filter extraction — no expression metadata, no names.
using SpecificPoint = std::vector<velox::variant>;

// Converts specific (fully constrained) Points to SpecificPoint, ordered by
// pk_type column order.
[[nodiscard]] std::vector<SpecificPoint> ToSpecificPoints(
  const std::vector<Point>& points, const velox::RowType& pk_type);

[[nodiscard]] std::vector<Point> ExtractFilterExpr(
  const velox::core::TypedExprPtr& expr, std::span<const std::string> pk_names);

struct ExtractAndRewriteResult {
  std::vector<Point> points;
  // Rewritten filter with PK predicates replaced by true; null if the entire
  // expression reduced to true.
  velox::core::TypedExprPtr remaining_filter;
};

[[nodiscard]] ExtractAndRewriteResult ExtractAndRewriteFilterExpr(
  const velox::core::TypedExprPtr& expr, std::span<const std::string> pk_names);

// Sorts points in-place by PK key order. Column order matches the pk_type used
// during ToSpecificPoints. Comparison uses velox::variant::operator<.
void SortPoints(std::vector<SpecificPoint>& points);

}  // namespace sdb::connector
