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

#include <absl/status/status.h>

#include <optional>

#include "connector/comparison_op.hpp"
#include "connector/search_filter_builder.hpp"

namespace duckdb {

class Expression;
class BoundFunctionExpression;
class BoundOperatorExpression;

}  // namespace duckdb
namespace sdb::connector {

struct FilterContext;

std::optional<absl::Status> TryFromInetEquality(BoolTarget filter,
                                                const FilterContext& ctx,
                                                const duckdb::Expression& left,
                                                const duckdb::Expression& right,
                                                bool not_equal);

std::optional<absl::Status> TryFromInetComparison(
  BoolTarget filter, const FilterContext& ctx, const duckdb::Expression& left,
  const duckdb::Expression& right, ComparisonOp op);

std::optional<absl::Status> TryFromInetIn(
  BoolTarget filter, const FilterContext& ctx,
  const duckdb::BoundOperatorExpression& op_expr);

std::optional<absl::Status> TryFromInetFunction(
  BoolTarget filter, const FilterContext& ctx,
  const duckdb::BoundFunctionExpression& func);

}  // namespace sdb::connector
