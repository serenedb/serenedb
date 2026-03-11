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

#include <absl/container/flat_hash_set.h>
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
      !func_call->inputs()[1]->isConstantKind()) {
    return AnyPoint(pk_names);
  }
  auto field_access =
    basics::downCast<velox::core::FieldAccessTypedExpr>(func_call->inputs()[0]);
  auto const_val =
    basics::downCast<velox::core::ConstantTypedExpr>(func_call->inputs()[1]);

  if (!absl::c_linear_search(pk_names, field_access->name())) {
    return AnyPoint(pk_names);
  }
  Point p{pk_names};
  p.AddEqFilter(field_access->name(), const_val, func_call);
  return {p};
}

std::vector<Point> ExtractFilterIn(const velox::core::CallTypedExpr* func_call,
                                   std::span<const std::string> pk_names) {
  if (!func_call->inputs()[0]->isFieldAccessKind() ||
      !func_call->inputs()[1]->isConstantKind()) {
    return AnyPoint(pk_names);
  }
  auto field_access =
    basics::downCast<velox::core::FieldAccessTypedExpr>(func_call->inputs()[0]);
  auto array_const =
    basics::downCast<velox::core::ConstantTypedExpr>(func_call->inputs()[1]);
  if (array_const->type()->kind() != velox::TypeKind::ARRAY) {
    return AnyPoint(pk_names);
  }
  if (!absl::c_linear_search(pk_names, field_access->name())) {
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
    // If all points that uses func_call source expr become specific, it means
    // that this node maybe replaced with constant true, as specific points will
    // be processed in point lookup datas ource
    p.AddEqFilter(field_access->name(), std::move(elem_const), func_call);
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

// Recursively rewrites `expr`, replacing any node whose address appears in
// `sources` with a constant `true`. Returns the original pointer when nothing
// changed (avoids allocations on the happy path).
velox::core::TypedExprPtr RewriteExpr(
  const velox::core::TypedExprPtr& expr,
  const absl::flat_hash_set<const velox::core::ITypedExpr*>& sources) {
  if (!expr->isCallKind()) {
    return expr;
  }
  if (sources.contains(expr.get())) {
    return {};
  }

  const auto* call = expr->asUnchecked<velox::core::CallTypedExpr>();
  std::vector<velox::core::TypedExprPtr> new_inputs;
  bool changed = false;
  for (const auto& input : call->inputs()) {
    auto new_input = RewriteExpr(input, sources);
    if (new_input.get() != input.get()) {
      changed = true;
    }
    new_inputs.push_back(std::move(new_input));
  }
  if (!changed) {
    return expr;
  }

  if (call->name() == "and") {
    std::erase_if(new_inputs, [](auto expr) { return expr == nullptr; });
    if (new_inputs.size() == 0) {
      return {};
    }
    if (new_inputs.size() == 1) {
      return new_inputs[0];
    }
  }

  if (call->name() == "or") {
    SDB_ASSERT(
      absl::c_all_of(new_inputs, [](auto expr) { return expr == nullptr; }));
    return {};
  }

  return std::make_shared<velox::core::CallTypedExpr>(
    expr->type(), std::move(new_inputs), call->name());
}

template<velox::TypeKind Kind>
velox::variant ExtractScalarVariant(const velox::BaseVector& vec) {
  using T = typename velox::TypeTraits<Kind>::NativeType;
  const auto& cv = static_cast<const velox::ConstantVector<T>&>(vec);
  return velox::variant(cv.valueAt(0));
}

velox::variant ToVariant(const velox::core::ConstantTypedExpr& expr) {
  if (!expr.hasValueVector())
    return expr.value();
  if (expr.isNull())
    return velox::variant(expr.type()->kind());
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
    ExtractScalarVariant, expr.type()->kind(), *expr.valueVector());
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
                        velox::core::ConstantTypedExprPtr value,
                        const velox::core::ITypedExpr* source_expr) {
  SDB_ASSERT(!_column_filters.contains(column_name));
  _column_filters.emplace(column_name, std::move(value));
  _source_exprs.insert(source_expr);
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
  result._source_exprs.insert(
    lhs._source_exprs.begin(),
    lhs._source_exprs.end());  // TODO moving iterator?

  result._source_exprs.insert(
    rhs._source_exprs.begin(),
    rhs._source_exprs.end());  // TODO moving iterator?

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

// problem
// a, b -- PK
// a = 1 and b = 2 and c = 3 --> true and c = 3
std::vector<Point> ExtractFilterExpr(const velox::core::TypedExprPtr& expr,
                                     std::span<const std::string> pk_names) {
  std::vector<Point> pts;
  if (!expr->isCallKind()) {
    pts = AnyPoint(pk_names);
  } else {
    const auto* func_call = expr->asUnchecked<velox::core::CallTypedExpr>();
    if (func_call->name() == "presto_eq") {
      pts = ExtractFilterEq(func_call, pk_names);
    } else if (func_call->name() == "in") {
      pts = ExtractFilterIn(func_call, pk_names);
    } else if (func_call->name() == "and") {
      pts = ExtractFilterAnd(func_call, pk_names);
    } else if (func_call->name() == "or") {
      pts = ExtractFilterOr(func_call, pk_names);
    } else {
      pts = AnyPoint(pk_names);
    }
  }
  return pts;
}

ExtractAndRewriteResult ExtractAndRewriteFilterExpr(
  const velox::core::TypedExprPtr& expr,
  std::span<const std::string> pk_names) {
  auto pts = ExtractFilterExpr(expr, pk_names);
  if (!absl::c_all_of(pts, [](const Point& p) { return p.IsSpecific(); })) {
    return {{}, expr};
  }

  // Collect the unique source sub-expressions to replace with `true`.
  absl::flat_hash_set<const velox::core::ITypedExpr*> sources;
  for (const auto& p : pts) {
    sources.insert(p.GetSourceExprs().begin(),
                   p.GetSourceExprs().end());  // todo moving iterator?
  }

  auto rewritten = RewriteExpr(expr, sources);

  return {std::move(pts), std::move(rewritten)};
}

void SortPoints(std::vector<Point>& points, const velox::RowType& pk_type) {
  absl::c_sort(points, [&](const Point& a, const Point& b) {
    for (size_t i = 0; i < pk_type.size(); ++i) {
      const auto& col_name = pk_type.nameOf(i);
      const auto* af = a.FindFilter(col_name);
      const auto* bf = b.FindFilter(col_name);
      SDB_ASSERT(af && bf);
      const auto av = ToVariant(*af);
      const auto bv = ToVariant(*bf);
      if (av < bv)
        return true;
      if (bv < av)
        return false;
    }
    return false;
  });
}

}  // namespace sdb::connector
