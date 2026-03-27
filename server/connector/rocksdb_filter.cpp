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
#include <velox/expression/ExprConstants.h>
#include <velox/vector/ConstantVector.h>

#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"

namespace sdb::connector {
namespace {

std::vector<KeyConstraint> AnyKeyConstraint(
  const std::span<const std::string> names) {
  return {KeyConstraint(names)};
}

std::vector<KeyConstraint> ExtractFilterEq(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names) {
  SDB_ASSERT(func_call->inputs().size() == 2);
  if (!func_call->inputs()[0]->isFieldAccessKind() ||
      !func_call->inputs()[1]->isConstantKind()) {
    return AnyKeyConstraint(pk_names);
  }
  auto field_access =
    basics::downCast<velox::core::FieldAccessTypedExpr>(func_call->inputs()[0]);
  auto const_val =
    basics::downCast<velox::core::ConstantTypedExpr>(func_call->inputs()[1]);

  if (!absl::c_linear_search(pk_names, field_access->name())) {
    return AnyKeyConstraint(pk_names);
  }
  KeyConstraint p{pk_names};
  p.AddEqFilter(field_access->name(), *const_val, func_call);
  return {p};
}

std::vector<KeyConstraint> ExtractFilterComparison(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names) {
  SDB_ASSERT(func_call->inputs().size() == 2);

  const bool field_left = func_call->inputs()[0]->isFieldAccessKind() &&
                          func_call->inputs()[1]->isConstantKind();
  const bool field_right = func_call->inputs()[1]->isFieldAccessKind() &&
                           func_call->inputs()[0]->isConstantKind();
  if (!field_left && !field_right) {
    return AnyKeyConstraint(pk_names);
  }

  const auto& field_input =
    field_left ? func_call->inputs()[0] : func_call->inputs()[1];
  const auto& const_input =
    field_left ? func_call->inputs()[1] : func_call->inputs()[0];
  auto field_access =
    basics::downCast<velox::core::FieldAccessTypedExpr>(field_input);
  auto const_val =
    basics::downCast<velox::core::ConstantTypedExpr>(const_input);

  if (!absl::c_linear_search(pk_names, field_access->name())) {
    return AnyKeyConstraint(pk_names);
  }

  // Determine the op from the function name, flipping if field is on the right.
  // e.g. `5 > a` with field_right means `a < 5`.
  ComparisonOp op;
  if (IsCallOf(func_call, "_gt")) {
    op = field_left ? ComparisonOp::Gt : ComparisonOp::Lt;
  } else if (IsCallOf(func_call, "_gte")) {
    op = field_left ? ComparisonOp::Ge : ComparisonOp::Le;
  } else if (IsCallOf(func_call, "_lt")) {
    op = field_left ? ComparisonOp::Lt : ComparisonOp::Gt;
  } else {  // _lte
    op = field_left ? ComparisonOp::Le : ComparisonOp::Ge;
  }

  KeyConstraint kc{pk_names};
  kc.AddComparisonFilter(field_access->name(), *const_val, op, func_call);
  return {std::move(kc)};
}

std::vector<KeyConstraint> ExtractFilterIn(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names) {
  if (!func_call->inputs()[0]->isFieldAccessKind() ||
      !func_call->inputs()[1]->isConstantKind()) {
    return AnyKeyConstraint(pk_names);
  }
  auto field_access =
    basics::downCast<velox::core::FieldAccessTypedExpr>(func_call->inputs()[0]);
  auto array_const =
    basics::downCast<velox::core::ConstantTypedExpr>(func_call->inputs()[1]);
  if (array_const->type()->kind() != velox::TypeKind::ARRAY) {
    return AnyKeyConstraint(pk_names);
  }
  if (!absl::c_linear_search(pk_names, field_access->name())) {
    return AnyKeyConstraint(pk_names);
  }

  SDB_ASSERT(array_const->valueVector()->typeKind() == velox::TypeKind::ARRAY);
  auto const_vec = basics::downCast<velox::ConstantVector<velox::ComplexType>>(
    array_const->valueVector());
  auto array_vec =
    basics::downCast<velox::ArrayVector>(const_vec->valueVector());
  const auto offset = array_vec->offsetAt(const_vec->index());
  const auto size = array_vec->sizeAt(const_vec->index());
  const auto& elements = array_vec->elements();

  std::vector<KeyConstraint> points;
  points.reserve(size);
  for (velox::vector_size_t i = 0; i < size; ++i) {
    velox::core::ConstantTypedExpr elem_const{
      velox::BaseVector::wrapInConstant(1, offset + i, elements)};
    KeyConstraint p{pk_names};
    // If all points that uses func_call source expr become specific, it means
    // that this node maybe replaced with constant true, as specific points will
    // be processed in point lookup data source
    p.AddEqFilter(field_access->name(), elem_const, func_call);
    points.push_back(std::move(p));
  }
  return points;
}

std::vector<KeyConstraint> ExtractFilterAnd(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names) {
  SDB_ASSERT(!func_call->inputs().empty());

  // Cartesian product of all children's point sets, intersecting each tuple.
  // An unconstrained child (AnyKeyConstraint — empty filters) acts as identity
  // because Intersect(P, {}) == P, so no special-casing is needed.
  std::vector<KeyConstraint> result =
    ExtractFilterExpr(func_call->inputs()[0], pk_names);
  for (size_t i = 1; i < func_call->inputs().size(); ++i) {
    const auto rhs_pts = ExtractFilterExpr(func_call->inputs()[i], pk_names);
    std::vector<KeyConstraint> next;
    next.reserve(result.size() * rhs_pts.size());
    for (const auto& lhs : result) {
      for (const auto& rhs : rhs_pts) {
        if (auto merged = KeyConstraint::Intersect(lhs, rhs);
            merged && !merged->IsUnconstrained()) {
          next.push_back(std::move(*merged));
        }
      }
    }
    result = std::move(next);
  }
  return result;
}

std::vector<KeyConstraint> ExtractFilterOr(
  const velox::core::CallTypedExpr* func_call,
  std::span<const std::string> pk_names) {
  SDB_ASSERT(!func_call->inputs().empty());

  std::vector<KeyConstraint> result;
  for (const auto& input : func_call->inputs()) {
    auto pts = ExtractFilterExpr(input, pk_names);
    // If any point in the child result is unconstrained, the OR is
    // unconstrained.
    if (absl::c_any_of(
          pts, [](const KeyConstraint& p) { return p.IsUnconstrained(); })) {
      return AnyKeyConstraint(pk_names);
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
  const containers::FlatHashSet<const velox::core::ITypedExpr*>& sources) {
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

// Returns the more restrictive (greater) lower bound.
std::optional<Boundary> MaxLeft(const std::optional<Boundary>& a,
                                const std::optional<Boundary>& b) {
  if (!a) {
    return b;
  }
  if (!b) {
    return a;
  }
  if (a->value < b->value) {
    return b;
  }
  if (b->value < a->value) {
    return a;
  }
  // Same value: exclusive is more restrictive for a left bound.
  return a->inclusive ? b : a;
}

// Returns the more restrictive (lesser) upper bound.
std::optional<Boundary> MinRight(const std::optional<Boundary>& a,
                                 const std::optional<Boundary>& b) {
  if (!a) {
    return b;
  }
  if (!b) {
    return a;
  }
  if (a->value < b->value) {
    return a;
  }
  if (b->value < a->value) {
    return b;
  }
  // Same value: exclusive is more restrictive for a right bound.
  return a->inclusive ? b : a;
}

std::optional<Range> IntersectRange(const Range& lhs, const Range& rhs) {
  auto new_left = MaxLeft(lhs.left, rhs.left);
  auto new_right = MinRight(lhs.right, rhs.right);
  if (new_left && new_right) {
    if (new_right->value < new_left->value) {
      return std::nullopt;
    }
    // Same boundary value but at least one side exclusive: empty interval.
    if (new_left->value == new_right->value &&
        (!new_left->inclusive || !new_right->inclusive)) {
      return std::nullopt;
    }
  }
  return Range{std::move(new_left), std::move(new_right)};
}

}  // namespace

template<velox::TypeKind Kind>
velox::variant ExtractScalarVariant(const velox::BaseVector& vec) {
  using T = typename velox::TypeTraits<Kind>::NativeType;
  const auto& cv = static_cast<const velox::ConstantVector<T>&>(vec);
  return velox::variant(cv.valueAt(0));
}

velox::variant ToVariant(const velox::core::ConstantTypedExpr& expr) {
  if (!expr.hasValueVector()) {
    return expr.value();
  }
  if (expr.isNull()) {
    return velox::variant(expr.type()->kind());
  }
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
    ExtractScalarVariant, expr.type()->kind(), *expr.valueVector());
}

bool KeyConstraint::IsSpecific() const {
  return absl::c_all_of(_pk_names, [&](std::string_view name) {
    auto it = _column_filters.find(name);
    if (it == _column_filters.end()) {
      return false;
    }
    const Range& r = *it->second;
    return r.left.has_value() && r.right.has_value() &&
           r.left->value == r.right->value && r.left->inclusive &&
           r.right->inclusive;
  });
}

const Range* KeyConstraint::FindFilter(std::string_view column_name) const {
  auto it = _column_filters.find(column_name);
  return it != _column_filters.end() ? it->second.get() : nullptr;
}

void KeyConstraint::AddEqFilter(std::string_view column_name,
                                const velox::core::ConstantTypedExpr& value,
                                const velox::core::ITypedExpr* source_expr) {
  SDB_ASSERT(!_column_filters.contains(column_name));
  auto v = ToVariant(value);
  _column_filters.emplace(
    column_name,
    std::make_unique<Range>(Range{Boundary{v, true}, Boundary{v, true}}));
  _source_exprs.insert(source_expr);
}

void KeyConstraint::AddComparisonFilter(
  std::string_view column_name, const velox::core::ConstantTypedExpr& value,
  ComparisonOp op, const velox::core::ITypedExpr* source_expr) {
  SDB_ASSERT(!_column_filters.contains(column_name));
  auto v = ToVariant(value);
  Range range;
  switch (op) {
    case ComparisonOp::Gt:
      range = Range{Boundary{v, false}, std::nullopt};
      break;
    case ComparisonOp::Ge:
      range = Range{Boundary{v, true}, std::nullopt};
      break;
    case ComparisonOp::Lt:
      range = Range{std::nullopt, Boundary{v, false}};
      break;
    case ComparisonOp::Le:
      range = Range{std::nullopt, Boundary{v, true}};
      break;
    case ComparisonOp::None:
      SDB_ASSERT(false, "AddComparisonFilter called with ComparisonOp::None");
      break;
  }
  _column_filters.emplace(column_name, std::make_unique<Range>(range));
  _source_exprs.insert(source_expr);
}

std::optional<KeyConstraint> KeyConstraint::Intersect(
  const KeyConstraint& lhs, const KeyConstraint& rhs) {
  SDB_ASSERT(lhs._pk_names.data() == rhs._pk_names.data());
  KeyConstraint result{lhs._pk_names};
  for (std::string_view pk_name : lhs._pk_names) {
    const auto* lhs_f = lhs.FindFilter(pk_name);
    const auto* rhs_f = rhs.FindFilter(pk_name);
    if (!lhs_f && !rhs_f) {
      continue;
    }
    if (!lhs_f) {
      result._column_filters.emplace(pk_name, std::make_unique<Range>(*rhs_f));
      continue;
    }
    if (!rhs_f) {
      result._column_filters.emplace(pk_name, std::make_unique<Range>(*lhs_f));
      continue;
    }
    auto merged_range = IntersectRange(*lhs_f, *rhs_f);
    if (!merged_range) {
      // e.g. [1,1] AND [2,2] -> contradiction
      return {};
    }
    result._column_filters.emplace(pk_name,
                                   std::make_unique<Range>(*merged_range));
  }
  result._source_exprs.insert(lhs._source_exprs.begin(),
                              lhs._source_exprs.end());

  result._source_exprs.insert(rhs._source_exprs.begin(),
                              rhs._source_exprs.end());

  return result;
}

std::vector<SpecificPoint> ToSpecificPoints(
  const std::vector<KeyConstraint>& points, const velox::RowType& pk_type) {
  std::vector<SpecificPoint> result;
  result.reserve(points.size());
  for (const auto& p : points) {
    SpecificPoint sp;
    sp.reserve(pk_type.size());
    for (const auto& name : pk_type.names()) {
      const auto* filter = p.FindFilter(name);
      SDB_ASSERT(filter != nullptr, "pk column not found in specific point");
      SDB_ASSERT(filter->left.has_value());
      sp.push_back(filter->left->value);
    }
    result.push_back(std::move(sp));
  }
  return result;
}

std::vector<KeyConstraint> ExtractFilterExpr(
  const velox::core::TypedExprPtr& expr,
  std::span<const std::string> pk_names) {
  std::vector<KeyConstraint> pts;
  if (!expr->isCallKind()) {
    pts = AnyKeyConstraint(pk_names);
  } else {
    const auto* func_call = expr->asUnchecked<velox::core::CallTypedExpr>();
    if (IsCallOf(func_call, "_eq")) {
      pts = ExtractFilterEq(func_call, pk_names);
    } else if (IsCallOf(func_call, "_in")) {
      pts = ExtractFilterIn(func_call, pk_names);
    } else if (IsCallOf(func_call, "_gt") || IsCallOf(func_call, "_gte") ||
               IsCallOf(func_call, "_lt") || IsCallOf(func_call, "_lte")) {
      pts = ExtractFilterComparison(func_call, pk_names);
    } else if (func_call->name() == velox::expression::kAnd) {
      pts = ExtractFilterAnd(func_call, pk_names);
    } else if (func_call->name() == velox::expression::kOr) {
      pts = ExtractFilterOr(func_call, pk_names);
    } else {
      pts = AnyKeyConstraint(pk_names);
    }
  }
  return pts;
}

ExtractAndRewriteResult ExtractAndRewriteFilterExpr(
  const velox::core::TypedExprPtr& expr,
  std::span<const std::string> pk_names) {
  auto pts = ExtractFilterExpr(expr, pk_names);
  if (!absl::c_all_of(pts,
                      [](const KeyConstraint& p) { return p.IsSpecific(); })) {
    return {{}, expr};
  }
  // TODO extract ranges here

  // Collect the unique source sub-expressions to get rid of them.
  containers::FlatHashSet<const velox::core::ITypedExpr*> sources;
  for (const auto& p : pts) {
    sources.insert(p.GetSourceExprs().begin(), p.GetSourceExprs().end());
  }

  return {std::move(pts), RewriteExpr(expr, sources)};
}

void SortPoints(std::vector<SpecificPoint>& points) {
  absl::c_sort(points, [](const SpecificPoint& lhs, const SpecificPoint& rhs) {
    for (size_t i = 0; i < lhs.size(); ++i) {
      if (lhs[i] != rhs[i]) {
        return lhs[i] < rhs[i];
      }
    }
    return false;
  });
}

}  // namespace sdb::connector
