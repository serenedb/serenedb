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

  void AddEqFilter(std::string_view column_name,
                   velox::core::ConstantTypedExprPtr value);

  // Returns nullopt when the two points are contradictory (e.g. a=1 AND a=2).
  [[nodiscard]] static std::optional<Point> Intersect(const Point& lhs,
                                                      const Point& rhs);

 private:
  std::span<const std::string> _pk_names;
  absl::flat_hash_map<std::string_view, velox::core::ConstantTypedExprPtr>
    _column_filters;
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

}  // namespace sdb::connector
