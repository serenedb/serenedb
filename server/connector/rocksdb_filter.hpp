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

#include <memory>
#include <optional>
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

  // Returns nullptr if the column has no filter (matches any value).
  [[nodiscard]] const velox::core::ConstantTypedExpr* FindFilter(
    std::string_view column_name) const;

  [[nodiscard]] std::span<const std::string> PkNames() const noexcept {
    return _pk_names;
  }

  void AddEqFilter(std::string_view column_name,
                   const velox::core::ConstantTypedExpr* value);

  // Returns nullopt when the two points are contradictory (e.g. a=1 AND a=2).
  [[nodiscard]] static std::optional<Point> Intersect(const Point& lhs,
                                                      const Point& rhs);

 private:
  std::span<const std::string> _pk_names;
  absl::flat_hash_map<std::string_view, const velox::core::ConstantTypedExpr*>
    _column_filters;
};

class FilterNode {
 public:
  virtual ~FilterNode() = default;

  [[nodiscard]] virtual std::optional<Point> NextPoint() = 0;

 protected:
  FilterNode() = default;
};

class EqFilterNode final : public FilterNode {
 public:
  EqFilterNode(std::string column_name, velox::core::ConstantTypedExprPtr value,
               std::span<const std::string> pk_names);

  [[nodiscard]] std::optional<Point> NextPoint() final;

 private:
  std::string _column_name;
  velox::core::ConstantTypedExprPtr _value;
  std::span<const std::string> _pk_names;
  bool _sent = false;
};

class AndFilterNode final : public FilterNode {
 public:
  explicit AndFilterNode(std::vector<std::unique_ptr<FilterNode>> filters);

  [[nodiscard]] std::optional<Point> NextPoint() final;

 private:
  // Ensure _all_points[child_idx] has an entry at needed_idx (lazy drain).
  // Returns false if the child is exhausted before reaching that index.
  bool EnsureChildPoint(size_t child_idx, size_t needed_idx);

  // Advance the Cartesian-product index; returns false when exhausted.
  bool Advance();

  // Intersect points at the current indices; returns nullopt on conflict.
  [[nodiscard]] std::optional<Point> TryMerge() const;

  std::vector<std::unique_ptr<FilterNode>> _filters;

  enum class State : uint8_t { NotStarted, Running, Done };
  State _state = State::NotStarted;

  std::vector<std::vector<Point>>
    _all_points;                 // lazy per-child cache [filter_idx][point_idx]
  std::vector<bool> _exhausted;  // true when child fully drained
  std::vector<size_t> _indices;  // current index per filter
};

class OrFilterNode final : public FilterNode {
 public:
  explicit OrFilterNode(std::vector<std::unique_ptr<FilterNode>> filters);

  [[nodiscard]] std::optional<Point> NextPoint() final;

 private:
  std::vector<std::unique_ptr<FilterNode>> _filters;
  size_t _current = 0;
};

// Returns nullptr for expressions that cannot be parsed as a specific filter.
// Callers treat nullptr as "any" (identity for AND, short-circuit for OR).
[[nodiscard]] std::unique_ptr<FilterNode> ParseFilters(
  const std::vector<velox::core::TypedExprPtr>& filters,
  std::span<const std::string> pk_names);

// Returns a RowVector with one row per specific point, or nullptr if the filter
// is not a set of specific PK points (→ caller should use full scan).
[[nodiscard]] velox::RowVectorPtr TryGetPoints(FilterNode& filter,
                                               velox::RowTypePtr pk_type,
                                               velox::memory::MemoryPool* pool);

}  // namespace sdb::connector
