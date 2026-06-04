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

#include <duckdb/planner/expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <optional>

#include "basics/result.h"
#include "rocksdb_filter.hpp"  // ComparisonOp

namespace sdb::connector {

struct FilterContext;        // ts_common.hpp
class BooleanFilterBuilder;  // ts_common.hpp

// Returns the inner expression as a ST_Distance_Centroid(field, centroid)
// call -- or its `<->` operator-form synonym -- when it matches that
// exact shape, or nullptr otherwise. `<->` shares its operator name with
// the vector L2 op, so disambiguation needs the catalog (FilterContext).
const duckdb::BoundFunctionExpression* TryGetGeoDistanceCall(
  const FilterContext& ctx, const duckdb::Expression& expr);

// ST_Distance_Centroid(field, centroid) = / != distance  ->  point range.
Result FromGeoDistanceBinaryEq(BooleanFilterBuilder& filter,
                               const FilterContext& ctx,
                               const duckdb::BoundFunctionExpression& geo_call,
                               const duckdb::Expression& dist_expr);

// ST_Distance_Centroid(field, centroid) </<=/>/>= distance  ->  one-sided.
Result FromGeoDistanceComparison(
  BooleanFilterBuilder& filter, const FilterContext& ctx,
  const duckdb::BoundFunctionExpression& geo_call,
  const duckdb::Expression& dist_expr, ComparisonOp op);

// Dispatch a function expression that names a geo predicate
// (ST_Distance_Between / ST_Intersects / ST_Contains). Returns nullopt
// if `func` is not a geo function so the caller can fall through.
std::optional<Result> TryDispatchGeoFunction(
  BooleanFilterBuilder& filter, const FilterContext& ctx,
  const duckdb::BoundFunctionExpression& func);

}  // namespace sdb::connector
