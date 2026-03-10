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

#include "rocksdb_filter.hpp"

#include <velox/vector/ConstantVector.h>
#include <velox/vector/FlatVector.h>

#include "basics/assert.h"
#include "basics/down_cast.h"

namespace sdb::connector {

namespace {

template<velox::TypeKind Kind>
velox::VectorPtr BuildPointColumn(const std::string& col_name,
                                  const std::vector<Point>& points,
                                  velox::memory::MemoryPool* pool) {
  using T = typename velox::TypeTraits<Kind>::NativeType;
  const auto n = static_cast<velox::vector_size_t>(points.size());
  auto result = velox::BaseVector::create<velox::FlatVector<T>>(
    velox::Type::create<Kind>(), n, pool);
  for (velox::vector_size_t row_idx = 0; row_idx < n; ++row_idx) {
    const auto* eq = points[static_cast<size_t>(row_idx)].FindFilter(col_name);
    SDB_ASSERT(eq != nullptr, "pk column not found in filter");
    auto src_vec = eq->toConstantVector(pool);
    result->copy(src_vec.get(), row_idx, 0, 1);
  }
  return result;
}

std::vector<Point> AnyPoint(const std::span<const std::string> names) {
  return {Point(names)};
}

std::vector<Point> ExtractFilterEq(const velox::core::CallTypedExpr* func_call,
                                   std::span<const std::string> pk_names) {
  SDB_ASSERT(func_call->inputs().size() == 2);
  if (!func_call->inputs()[0]->isFieldAccessKind() ||
      !func_call->inputs()[1]->isConcatKind()) {
    return AnyPoint(pk_names);
  }
  auto field_access =
    basics::downCast<velox::core::FieldAccessTypedExpr>(func_call->inputs()[0]);
  auto const_val =
    basics::downCast<velox::core::ConstantTypedExpr>(func_call->inputs()[1]);

  Point p{pk_names};
  p.AddEqFilter(field_access->name(), const_val);
  return {p};
}

std::vector<Point> ExtractFilterIn(const velox::core::CallTypedExpr* func_call,
                                   std::span<const std::string> pk_names) {
  if (!func_call->inputs()[0]->isFieldAccessKind() ||
      !func_call->inputs()[1]->isConcatKind()) {
    return AnyPoint(pk_names);
  }
  auto field_access =
    basics::downCast<velox::core::FieldAccessTypedExpr>(func_call->inputs()[0]);
  auto array_const =
    basics::downCast<velox::core::ConstantTypedExpr>(func_call->inputs()[1]);
  if (array_const->type()->kind() != velox::TypeKind::ARRAY) {
    return AnyPoint(pk_names);
  }

  SDB_ASSERT(array_const->valueVector()->typeKind() == velox::TypeKind::ARRAY);
  auto const_vec = basics::downCast<velox::ConstantVector<velox::ComplexType>>(
    array_const->valueVector());
  auto array_vec =
    basics::downCast<velox::ArrayVector>(const_vec->valueVector());
  const auto offset = array_vec->offsetAt(const_vec->index());
  const auto size = array_vec->sizeAt(const_vec->index());
  const auto& elements = array_vec->elements();

  std::vector<Point> points;
  points.reserve(size);
  for (velox::vector_size_t i = 0; i < size; ++i) {
    auto elem_const = std::make_shared<velox::core::ConstantTypedExpr>(
      velox::BaseVector::wrapInConstant(1, offset + i, elements));
    Point p{pk_names};
    p.AddEqFilter(field_access->name(), std::move(elem_const));
    points.push_back(std::move(p));
  }
  return points;
}

std::vector<Point> ExtractFilterAnd(const velox::core::CallTypedExpr* func_call,
                                    std::span<const std::string> pk_names) {
  SDB_ASSERT(!func_call->inputs().empty());

  // Cartesian product of all children's point sets, intersecting each tuple.
  // An unconstrained child (AnyPoint — empty filters) acts as identity because
  // Intersect(P, {}) == P, so no special-casing is needed.
  std::vector<Point> result =
    ExtractFilterExpr(func_call->inputs()[0], pk_names);
  for (size_t i = 1; i < func_call->inputs().size(); ++i) {
    const auto rhs_pts = ExtractFilterExpr(func_call->inputs()[i], pk_names);
    std::vector<Point> next;
    next.reserve(result.size() * rhs_pts.size());
    for (const auto& lhs : result) {
      for (const auto& rhs : rhs_pts) {
        if (auto merged = Point::Intersect(lhs, rhs);
            merged && !merged->IsUnconstrained()) {
          next.push_back(std::move(*merged));
        }
      }
    }
    result = std::move(next);
  }
  return result;
}

std::vector<Point> ExtractFilterOr(const velox::core::CallTypedExpr* func_call,
                                   std::span<const std::string> pk_names) {
  SDB_ASSERT(!func_call->inputs().empty());

  std::vector<Point> result;
  for (const auto& input : func_call->inputs()) {
    auto pts = ExtractFilterExpr(input, pk_names);
    // If any point in the child result is unconstrained, the OR is
    // unconstrained.
    if (absl::c_any_of(pts,
                       [](const Point& p) { return p.IsUnconstrained(); })) {
      return AnyPoint(pk_names);
    }
    result.insert(result.end(), std::make_move_iterator(pts.begin()),
                  std::make_move_iterator(pts.end()));
  }
  return result;
}

}  // namespace

bool Point::IsSpecific() const {
  return absl::c_all_of(_pk_names, [&](std::string_view name) {
    return _column_filters.contains(name);
  });
}

const velox::core::ConstantTypedExpr* Point::FindFilter(
  std::string_view column_name) const {
  auto it = _column_filters.find(column_name);
  return it != _column_filters.end() ? it->second.get() : nullptr;
}

void Point::AddEqFilter(std::string_view column_name,
                        velox::core::ConstantTypedExprPtr value) {
  SDB_ASSERT(!_column_filters.contains(column_name));
  _column_filters.emplace(column_name, std::move(value));
}

std::optional<Point> Point::Intersect(const Point& lhs, const Point& rhs) {
  SDB_ASSERT(lhs._pk_names.data() == rhs._pk_names.data());
  Point result{lhs._pk_names};
  for (std::string_view pk_name : lhs._pk_names) {
    const auto* lhs_f = lhs.FindFilter(pk_name);
    const auto* rhs_f = rhs.FindFilter(pk_name);
    if (!lhs_f && !rhs_f) {
      continue;
    }
    if (!lhs_f) {
      result._column_filters.emplace(pk_name, rhs._column_filters.at(pk_name));
      continue;
    }
    if (!rhs_f) {
      result._column_filters.emplace(pk_name, lhs._column_filters.at(pk_name));
      continue;
    }
    if (lhs_f->value() != rhs_f->value()) {
      // a = 1 AND a = 2 -> contradiction
      return {};
    }
    result._column_filters.emplace(pk_name, lhs._column_filters.at(pk_name));
  }
  return result;
}

velox::RowVectorPtr PointsToRowVector(const std::vector<Point>& points,
                                      velox::RowTypePtr pk_type,
                                      velox::memory::MemoryPool* pool) {
  // Build one column vector per PK column, each with one row per point.
  std::vector<velox::VectorPtr> columns;
  columns.reserve(pk_type->size());
  for (size_t col_idx = 0; col_idx < pk_type->size(); ++col_idx) {
    const auto& col_name = pk_type->nameOf(col_idx);
    const auto& col_type = pk_type->childAt(col_idx);
    columns.emplace_back(VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      BuildPointColumn, col_type->kind(), col_name, points, pool));
  }
  return std::make_shared<velox::RowVector>(pool, pk_type, velox::BufferPtr{},
                                            points.size(), std::move(columns));
}

std::vector<Point> ExtractFilterExpr(const velox::core::TypedExprPtr& expr,
                                     std::span<const std::string> pk_names) {
  if (!expr->isCallKind()) {
    return AnyPoint(pk_names);
  }
  const auto* func_call = expr->asUnchecked<velox::core::CallTypedExpr>();
  if (func_call->name() == "presto_eq") {
    return ExtractFilterEq(func_call, pk_names);
  }
  if (func_call->name() == "in") {
    return ExtractFilterIn(func_call, pk_names);
  }
  if (func_call->name() == "and") {
    return ExtractFilterAnd(func_call, pk_names);
  }
  if (func_call->name() == "or") {
    return ExtractFilterOr(func_call, pk_names);
  }
  return AnyPoint(pk_names);
}

}  // namespace sdb::connector
