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
#include "basics/logger/logger.h"

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

std::unique_ptr<FilterNode> ParseFilter(const velox::core::TypedExprPtr& expr,
                                        std::span<const std::string> pk_names);

std::unique_ptr<FilterNode> ParseEq(const velox::core::CallTypedExpr* func_call,
                                    std::span<const std::string> pk_names) {
  SDB_ASSERT(func_call->inputs().size() == 2);
  if (func_call->inputs()[0]->kind() != velox::core::ExprKind::kFieldAccess ||
      func_call->inputs()[1]->kind() != velox::core::ExprKind::kConstant) {
    return nullptr;
  }
  auto field_access =
    basics::downCast<velox::core::FieldAccessTypedExpr>(func_call->inputs()[0]);
  auto const_val =
    basics::downCast<velox::core::ConstantTypedExpr>(func_call->inputs()[1]);
  return std::make_unique<EqFilterNode>(field_access->name(), const_val,
                                        pk_names);
}

std::unique_ptr<FilterNode> ParseIn(const velox::core::CallTypedExpr* func_call,
                                    std::span<const std::string> pk_names) {
  if (func_call->inputs().size() != 2) {
    return nullptr;
  }
  if (func_call->inputs()[0]->kind() != velox::core::ExprKind::kFieldAccess ||
      func_call->inputs()[1]->kind() != velox::core::ExprKind::kConstant) {
    return nullptr;
  }
  auto field_access =
    basics::downCast<velox::core::FieldAccessTypedExpr>(func_call->inputs()[0]);
  auto array_const =
    basics::downCast<velox::core::ConstantTypedExpr>(func_call->inputs()[1]);
  if (array_const->type()->kind() != velox::TypeKind::ARRAY) {
    return nullptr;
  }
  SDB_ASSERT(array_const->valueVector()->typeKind() == velox::TypeKind::ARRAY);
  auto const_vec = basics::downCast<velox::ConstantVector<velox::ComplexType>>(
    array_const->valueVector());
  auto array_vec =
    basics::downCast<velox::ArrayVector>(const_vec->valueVector());
  const auto offset = array_vec->offsetAt(const_vec->index());
  const auto size = array_vec->sizeAt(const_vec->index());
  const auto& elements = array_vec->elements();

  std::vector<std::unique_ptr<FilterNode>> children;
  children.reserve(size);
  for (velox::vector_size_t i = 0; i < size; ++i) {
    auto elem_const = std::make_shared<velox::core::ConstantTypedExpr>(
      velox::BaseVector::wrapInConstant(1, offset + i, elements));
    children.emplace_back(std::make_unique<EqFilterNode>(
      field_access->name(), std::move(elem_const), pk_names));
  }
  return std::make_unique<OrFilterNode>(std::move(children));
}

std::unique_ptr<FilterNode> ParseAndOr(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names) {
  SDB_ASSERT(!func_call->inputs().empty(), "and/or must have at least 1 child");
  const bool is_or = func_call->name() == "or";

  std::vector<std::unique_ptr<FilterNode>> children;
  children.reserve(func_call->inputs().size());
  for (const auto& input : func_call->inputs()) {
    auto child = ParseFilter(input, pk_names);
    if (is_or && !child) {
      // Unconstrained child makes the entire OR unconstrained.
      return nullptr;
    }
    if (child) {
      children.emplace_back(std::move(child));
    }
  }
  if (children.empty()) {
    return nullptr;
  }
  if (!is_or) {
    return std::make_unique<AndFilterNode>(std::move(children));
  }
  return std::make_unique<OrFilterNode>(std::move(children));
}

std::unique_ptr<FilterNode> ParseFilter(const velox::core::TypedExprPtr& expr,
                                        std::span<const std::string> pk_names) {
  if (!expr->isCallKind()) {
    return nullptr;
  }
  const auto* func_call = expr->asUnchecked<velox::core::CallTypedExpr>();

  if (func_call->name() == "presto_eq") {
    return ParseEq(func_call, pk_names);
  }
  if (func_call->name() == "in") {
    return ParseIn(func_call, pk_names);
  }
  if (func_call->name() == "and" || func_call->name() == "or") {
    return ParseAndOr(func_call, pk_names);
  }
  return nullptr;
}

constexpr size_t kMaxPoints = 16 * 1024;  // TODO(mkornaukhov) tune

}  // namespace

bool Point::IsSpecific() const {
  return absl::c_all_of(_pk_names, [&](std::string_view name) {
    return _column_filters.contains(name);
  });
}

const velox::core::ConstantTypedExpr* Point::FindFilter(
  std::string_view column_name) const {
  auto it = _column_filters.find(column_name);
  return it != _column_filters.end() ? it->second : nullptr;
}

void Point::AddEqFilter(std::string_view column_name,
                        const velox::core::ConstantTypedExpr* value) {
  SDB_ASSERT(!_column_filters.contains(column_name));
  _column_filters.emplace(column_name, value);
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
      result._column_filters.emplace(pk_name, rhs_f);
      continue;
    }
    if (!rhs_f) {
      result._column_filters.emplace(pk_name, lhs_f);
      continue;
    }
    if (lhs_f->value() != rhs_f->value()) {
      // a = 1 AND a = 2 -> contradiction
      return {};
    }
    result._column_filters.emplace(pk_name, lhs_f);
  }
  return result;
}

EqFilterNode::EqFilterNode(std::string column_name,
                           velox::core::ConstantTypedExprPtr value,
                           std::span<const std::string> pk_names)
  : _column_name{std::move(column_name)},
    _value{std::move(value)},
    _pk_names{pk_names} {}

std::vector<Point> EqFilterNode::NextPoints() {
  if (_sent)
    return {};
  _sent = true;
  Point p{_pk_names};
  p.AddEqFilter(_column_name, _value.get());
  return {std::move(p)};
}

AndFilterNode::AndFilterNode(std::vector<std::unique_ptr<FilterNode>> filters)
  : _children{std::move(filters)} {}

bool AndFilterNode::EnsureChildPoint(size_t child_idx, size_t needed_idx) {
  auto& child_points = _children_points[child_idx];
  while (child_points.size() <= needed_idx) {
    if (_exhausted[child_idx])
      return false;
    auto pts = _children[child_idx]->NextPoints();
    if (pts.empty()) {
      _exhausted[child_idx] = true;
      return false;
    }
    std::ranges::move(pts, std::back_inserter(child_points));
  }
  return true;
}

bool AndFilterNode::Advance() {
  for (int child_idx = static_cast<int>(_indices.size()) - 1; child_idx >= 0;
       --child_idx) {
    ++_indices[child_idx];
    if (EnsureChildPoint(child_idx, _indices[child_idx])) {
      return true;
    }
    // Child exhausted at this depth: carry left, reset right.
    _indices[child_idx] = 0;
  }
  return false;
}

std::optional<Point> AndFilterNode::TryMerge() const {
  std::optional<Point> result{_children_points[0][_indices[0]]};
  for (size_t i = 1; i < _children_points.size(); ++i) {
    result = Point::Intersect(*result, _children_points[i][_indices[i]]);
    if (!result)
      return {};
  }
  return result;
}

std::vector<Point> AndFilterNode::NextPoints() {
  if (_children.empty() || _state == State::Done)
    return {};

  if (_state == State::NotStarted) {
    _state = State::Running;
    _children_points.resize(_children.size());
    _exhausted.resize(_children.size(), false);
    _indices.resize(_children.size(), 0);
    for (size_t child_idx = 0; child_idx < _children.size(); ++child_idx) {
      if (!EnsureChildPoint(child_idx, 0)) {
        _state = State::Done;
        return {};
      }
    }
  } else {
    if (!Advance()) {
      _state = State::Done;
      return {};
    }
  }

  // Skip conflicting combinations.
  while (true) {
    if (auto result = TryMerge()) {
      return {std::move(*result)};
    }
    if (!Advance()) {
      _state = State::Done;
      return {};
    }
  }
}

OrFilterNode::OrFilterNode(std::vector<std::unique_ptr<FilterNode>> filters)
  : _children{std::move(filters)} {}

std::vector<Point> OrFilterNode::NextPoints() {
  while (_current < _children.size()) {
    auto pts = _children[_current]->NextPoints();
    if (!pts.empty()) {
      return pts;
    }
    ++_current;
  }
  return {};
}

std::unique_ptr<FilterNode> ParseFilters(
  const std::vector<velox::core::TypedExprPtr>& filters,
  std::span<const std::string> pk_names) {
  std::vector<std::unique_ptr<FilterNode>> nodes;
  nodes.reserve(filters.size());
  for (const auto& filter : filters) {
    if (auto node = ParseFilter(filter, pk_names)) {
      nodes.emplace_back(std::move(node));
    }
  }
  return std::make_unique<AndFilterNode>(std::move(nodes));
}

velox::RowVectorPtr TryGetPoints(FilterNode& filter, velox::RowTypePtr pk_type,
                                 velox::memory::MemoryPool* pool) {
  SDB_ASSERT(pk_type->size() != 0);

  // Drain all specific points first.
  std::vector<Point> points;
  while (true) {
    auto pts = filter.NextPoints();
    if (pts.empty())
      break;
    for (auto& point : pts) {
      if (!point.IsSpecific()) {
        return nullptr;
      }
      points.emplace_back(std::move(point));
    }
  }

  if (points.empty() || points.size() > kMaxPoints) {
    // At the moment, if there are no filtered specific points, we assume that
    // there is at least one row. But in fact, this may mean that there is
    // a contradiction and we know that *no* rows will be processed.
    return nullptr;
  }

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

}  // namespace sdb::connector
