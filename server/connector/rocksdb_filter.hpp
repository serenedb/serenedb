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

#include <velox/core/ExpressionEvaluator.h>
#include <velox/core/Expressions.h>
#include <velox/vector/ComplexVector.h>
#include <velox/vector/ConstantVector.h>

#include "axiom/connectors/ConnectorMetadata.h"
#include "basics/fwd.h"
#include "basics/result.h"
#include "iresearch/search/boolean_filter.hpp"
#include "magic_enum/magic_enum.hpp"
#include "velox/core/ITypedExpr.h"

namespace sdb::connector {

// TODO improve performance

// How to handle
// If something like
// t=(a int PK, b int PK)
// select * from t where (a = 1 and b = 2 ) or (a = 10 and b = 20)?

enum class FilterNodeKind : uint8_t {
  Eq,
  // TODO
  //   Less,
  //   Leq,
  //   Greater,
  //   Geq,
  And,
  Or,
  Any  // maybe any value
};

struct ColumnFilterEq {
  velox::core::ConstantTypedExprPtr value;
};

using ColumnFilterAny = std::monostate;
using ColumnFilter = std::variant<ColumnFilterAny, ColumnFilterEq>;

// todo store vector?
struct Point {
  template<typename It>
  Point(It begin, It end) : pk_names{begin, end} {}
  const absl::flat_hash_set<std::string> pk_names;
  absl::flat_hash_map<std::string, ColumnFilter> column_filters;
  bool IsNoFilters() const {  // todo is required?
    return absl::c_none_of(pk_names, [&](std::string_view column_name) {
      return column_filters.contains(column_name);
    });
  }

  bool IsSpecific() const {
    return absl::c_all_of(pk_names, [&](std::string_view column_name) {
      auto it = column_filters.find(column_name);
      if (it == column_filters.end())
        return false;
      return std::holds_alternative<ColumnFilterEq>(it->second);
    });
  }

  void AddColumnFilter(std::string_view column_name, ColumnFilter filter) {
    SDB_ASSERT(!column_filters.contains(column_name));
    column_filters.emplace(column_name, filter);
  }

  static std::optional<Point> Intersect(const Point& lhs, const Point& rhs) {
    SDB_ASSERT(lhs.pk_names == rhs.pk_names);
    Point resp{lhs.pk_names.begin(), lhs.pk_names.end()};
    for (std::string_view pk_name : lhs.pk_names) {
      auto lhs_it = lhs.column_filters.find(pk_name);
      auto rhs_it = rhs.column_filters.find(pk_name);
      // TODO rewrite shit
      if (lhs_it == lhs.column_filters.end()) {
        if (rhs_it == rhs.column_filters.end()) {
          // no filter
          continue;
        }
        resp.column_filters.emplace(pk_name, rhs_it->second);
        continue;
      }
      if (rhs_it == lhs.column_filters.end()) {
        SDB_ASSERT(lhs_it != lhs.column_filters.end());
        resp.column_filters.emplace(pk_name, lhs_it->second);
        continue;
      }
      const auto& lhs_filter = lhs_it->second;
      const auto& rhs_filter = rhs_it->second;
      if (std::holds_alternative<ColumnFilterAny>(lhs_filter)) {
        resp.column_filters.emplace(pk_name, rhs_filter);
        continue;
      }
      if (std::holds_alternative<ColumnFilterAny>(rhs_filter)) {
        resp.column_filters.emplace(pk_name, lhs_filter);
        continue;
      }

      SDB_ASSERT(std::holds_alternative<ColumnFilterEq>(lhs_filter));
      SDB_ASSERT(std::holds_alternative<ColumnFilterEq>(rhs_filter));
      auto lhs_val = std::get<ColumnFilterEq>(lhs_filter).value;
      auto rhs_val = std::get<ColumnFilterEq>(rhs_filter).value;

      if (lhs_val->value() != rhs_val->value()) {
        // a = 1 and a = 2 -> no point
        return {};
      }
      resp.column_filters.emplace(pk_name, lhs_filter);
    }
    return resp;
  }

  static Point CreateAny(std::span<const std::string> pk_names) {
    return Point(pk_names.begin(), pk_names.end());
  }
};

struct FilterNode {
  FilterNode(FilterNodeKind kind) : kind{kind} {}
  FilterNodeKind kind;
  virtual ~FilterNode() = default;
  virtual std::optional<Point> NextPoint(
    std::span<const std::string> pk_names) const = 0;
};

struct EqFilterNode final : FilterNode {
  EqFilterNode(std::string column_name, velox::core::ConstantTypedExprPtr value)
    : FilterNode{FilterNodeKind::Eq},
      column_name{std::move(column_name)},
      value{std::move(value)} {}

  std::optional<Point> NextPoint(
    std::span<const std::string> pk_names) const final {
    if (sent)
      return {};
    sent = true;
    Point p{pk_names.begin(), pk_names.end()};
    p.AddColumnFilter(column_name, ColumnFilterEq{value});
    return p;
  }

  std::string column_name;
  velox::core::ConstantTypedExprPtr value;
  mutable bool sent = false;
};

struct AnyFilterNode final : FilterNode {
  virtual std::optional<Point> NextPoint(
    std::span<const std::string> pk_names) const final {
    if (sent)
      return {};
    sent = true;
    return Point::CreateAny(pk_names);
  }
  mutable bool sent = false;
};

struct AndFilter final : FilterNode {
  AndFilter(std::vector<std::unique_ptr<FilterNode>> filters)
    : FilterNode{FilterNodeKind::And}, filters{std::move(filters)} {}
  std::vector<std::unique_ptr<FilterNode>> filters;

  std::optional<Point> NextPoint(
    std::span<const std::string> pk_names) const final {
    if (filters.empty()) {
      return {};
    }

    if (_stop)
      return {};

    // On first call: drain every subfilter into _all_points and set _indices to
    // (0, 0, ..., 0).  Subsequent calls just advance the index array.
    if (!_initialized) {
      _initialized = true;
      _all_points.resize(filters.size());
      for (size_t i = 0; i < filters.size(); ++i) {
        while (auto p = filters[i]->NextPoint(pk_names))  // todo make it lazy?
          _all_points[i].push_back(std::move(*p));
        if (_all_points[i].empty()) {
          _stop = true;
          return {};
        }
      }
      _indices.assign(filters.size(), 0);
    } else {
      if (!Advance()) {
        _stop = true;
        return {};
      }
    }

    // Skip combinations where columns conflict (Intersect returns nullopt).
    while (true) {
      if (auto result = TryMerge())
        return result;
      if (!Advance()) {
        _stop = true;
        return {};
      }
    }
  }

 private:
  // Increment the rightmost index; carry left on overflow (like a counter).
  // Returns false when all combinations are exhausted.
  bool Advance() const {
    for (int i = static_cast<int>(_indices.size()) - 1; i >= 0; --i) {
      if (++_indices[i] < _all_points[i].size())
        return true;
      _indices[i] = 0;
    }
    return false;
  }

  // Merge the points at the current indices; returns nullopt on conflict.
  // Uses emplace() instead of assignment because Point has a const member.
  std::optional<Point> TryMerge() const {
    std::optional<Point> result{_all_points[0][_indices[0]]};
    for (size_t i = 1; i < _all_points.size(); ++i) {
      auto merged = Point::Intersect(*result, _all_points[i][_indices[i]]);
      if (!merged)
        return {};
      result.emplace(std::move(*merged));
    }
    return result;
  }

  mutable bool _initialized = false;
  mutable bool _stop = false;
  mutable std::vector<std::vector<Point>>
    _all_points;                         // [filter_idx][point_idx]
  mutable std::vector<size_t> _indices;  // current index per filter
};

struct OrFilter final : FilterNode {
  OrFilter(std::vector<std::unique_ptr<FilterNode>> filters)
    : FilterNode{FilterNodeKind::Or}, filters{std::move(filters)} {}
  std::vector<std::unique_ptr<FilterNode>> filters;

  std::optional<Point> NextPoint(
    std::span<const std::string> pk_names) const final {
    while (_current < filters.size()) {
      if (auto p = filters[_current]->NextPoint(pk_names)) {
        if (p->IsNoFilters()) {
          // Unconstrained point matches everything — no further points needed.
          _current = filters.size();
        }
        return p;
      }
      ++_current;
    }
    return {};
  }

 private:
  mutable size_t _current = 0;
};

inline std::unique_ptr<FilterNode> ParseFilter(
  const velox::core::TypedExprPtr& filter) {
  if (!filter->isCallKind()) {
    return {};
  }
  auto* func_call = filter->asUnchecked<velox::core::CallTypedExpr>();
  if (func_call->name() == "presto_eq") {
    SDB_ASSERT(func_call->inputs().size() == 2);
    if (func_call->inputs()[0]->kind() != velox::core::ExprKind::kFieldAccess ||
        func_call->inputs()[1]->kind() != velox::core::ExprKind::kConstant) {
      return {};
    }
    auto field_access = basics::downCast<velox::core::FieldAccessTypedExpr>(
      func_call->inputs()[0]);
    auto const_val =
      basics::downCast<velox::core::ConstantTypedExpr>(func_call->inputs()[1]);
    return std::make_unique<EqFilterNode>(field_access->name(), const_val);
  }
  if (func_call->name() == "in") {
    if (func_call->inputs()[0]->kind() != velox::core::ExprKind::kFieldAccess) {
      return {};
    }
    auto field_access = basics::downCast<velox::core::FieldAccessTypedExpr>(
      func_call->inputs()[0]);

    SDB_ASSERT(func_call->inputs().size() == 2,
               "in() expected exactly 2 inputs: field + array constant");
    if (func_call->inputs()[1]->kind() != velox::core::ExprKind::kConstant) {
      SDB_PRINT("[mkornaukhov] in() second arg is not a constant");
      return {};
    }
    auto array_const =
      basics::downCast<velox::core::ConstantTypedExpr>(func_call->inputs()[1]);
    if (array_const->type()->kind() != velox::TypeKind::ARRAY) {
      SDB_PRINT("[mkornaukhov] in() second arg is not an array constant");
      return {};
    }

    const auto elem_type = array_const->type()->childAt(0);
    SDB_ASSERT(array_const->valueVector()->typeKind() ==
               velox::TypeKind::ARRAY);
    auto const_vec =
      basics::downCast<velox::ConstantVector<velox::ComplexType>>(
        array_const->valueVector());
    auto array_vec =
      basics::downCast<velox::ArrayVector>(const_vec->valueVector());
    const auto offset = array_vec->offsetAt(const_vec->index());
    const auto size = array_vec->sizeAt(const_vec->index());
    const auto& elements = array_vec->elements();

    std::vector<std::unique_ptr<FilterNode>> eqs;
    eqs.reserve(size);
    for (velox::vector_size_t i = 0; i < size; ++i) {
      auto elem_const = std::make_shared<velox::core::ConstantTypedExpr>(
        velox::BaseVector::wrapInConstant(1, offset + i, elements));
      eqs.emplace_back(
        std::make_unique<EqFilterNode>(field_access->name(), elem_const));
    }
    return std::make_unique<OrFilter>(std::move(eqs));
  }
  return {};
}

inline std::unique_ptr<FilterNode> ParseFilters(
  const std::vector<velox::core::TypedExprPtr>& filters) {
  std::vector<std::unique_ptr<FilterNode>> resp;
  for (const auto& filter : filters) {
    SDB_PRINT("filter=", filter->toString());
    if (auto parsed = ParseFilter(filter)) {
      resp.emplace_back(std::move(parsed));
    }
  }

  return std::make_unique<AndFilter>(std::move(resp));
}

// Somehow need to pass kind of row {a: 42, b: "lol"}  into data source
inline std::shared_ptr<velox::RowVector> TryGetPoint2(
  const FilterNode& filter, velox::RowTypePtr pk_type,
  velox::memory::MemoryPool* pool) {
  SDB_ASSERT(pk_type->size() != 0);
  SDB_ASSERT(filter.kind == FilterNodeKind::And);
  auto& and_filter = basics::downCast<AndFilter>(filter);
  absl::flat_hash_map<std::string, EqFilterNode*> cname2filter;
  for (const auto& sub_filter : and_filter.filters) {
    if (sub_filter->kind != FilterNodeKind::Eq)
      continue;
    auto* as_eq = basics::downCast<EqFilterNode>(sub_filter.get());
    auto res = cname2filter.emplace(as_eq->column_name, as_eq);
    SDB_ASSERT(res.second);
  }

  std::vector<velox::VectorPtr> children;
  for (std::string_view pk_col_name : pk_type->names()) {
    if (auto it = cname2filter.find(pk_col_name); it != cname2filter.end()) {
      children.emplace_back(it->second->value->toConstantVector(pool));
    } else {
      return {};
    }
  }
  return std::make_shared<velox::RowVector>(pool, pk_type, velox::BufferPtr{},
                                            1, std::move(children));
}

inline std::vector<velox::RowVectorPtr> TryGetPoints(
  const FilterNode& filter, velox::RowTypePtr pk_type,
  velox::memory::MemoryPool* pool) {
  SDB_ASSERT(pk_type->size() != 0);

  std::vector<velox::RowVectorPtr> result;
  while (auto point = filter.NextPoint(pk_type->names())) {
    if (!point->IsSpecific()) {
      SDB_PRINT("[mkornaukhov] point filter is not specific, too wide");
      return {};
    }

    const auto& cname2filter = point->column_filters;
    std::vector<velox::VectorPtr> children;
    for (std::string_view pk_col_name : pk_type->names()) {
      auto it = cname2filter.find(pk_col_name);
      SDB_ASSERT(it != cname2filter.end(),
                 "[mkornaukhov] pk column name not found in filter");
      SDB_ASSERT(std::holds_alternative<ColumnFilterEq>(it->second));

      const auto& as_eq = std::get<ColumnFilterEq>(it->second);
      children.emplace_back(as_eq.value->toConstantVector(pool));
    }
    result.emplace_back(std::make_shared<velox::RowVector>(
      pool, pk_type, velox::BufferPtr{}, 1, std::move(children)));
  }
  return result;
}

}  // namespace sdb::connector
