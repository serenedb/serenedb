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

#include <cstdint>
#include <duckdb/common/types/value.hpp>
#include <duckdb/planner/expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <optional>

#include "connector/comparison_op.hpp"
#include "connector/functions/ts_common.hpp"
#include "connector/search_filter_builder.hpp"

namespace sdb::connector {

struct InetOperands {
  const SearchColumnInfo* column = nullptr;
  const duckdb::Value* constant = nullptr;
  bool swapped = false;
};

enum class InetNetworkOp : uint8_t {
  ContainedBy,
  Contains,
  StrictlyContainedBy,
  StrictlyContains,
  Overlaps,
};

struct InetNetworkPredicate {
  const SearchColumnInfo* column = nullptr;
  const duckdb::Value* constant = nullptr;
  InetNetworkOp op = InetNetworkOp::ContainedBy;
};

const SearchColumnInfo* FindInetColumn(const FilterContext& ctx,
                                       const duckdb::Expression& expr);

std::optional<InetOperands> ResolveInetOperands(
  const FilterContext& ctx, const duckdb::Expression& left,
  const duckdb::Expression& right);

std::optional<InetNetworkPredicate> ResolveInetNetworkOp(
  const FilterContext& ctx, const duckdb::BoundFunctionExpression& func);

absl::Status FromInetEquality(BoolTarget filter, const FilterContext& ctx,
                              const InetOperands& operands, bool not_equal);

absl::Status FromInetComparison(BoolTarget filter, const FilterContext& ctx,
                                const InetOperands& operands, ComparisonOp op);

// That's required because IPv4 and IPv6 comparable,
// so AndRangeMergeRule cannot merge them
absl::Status FromInetBetween(BoolTarget filter, const FilterContext& ctx,
                             const SearchColumnInfo& column,
                             const duckdb::BoundFunctionExpression& between);

absl::Status FromInetIn(BoolTarget filter, const FilterContext& ctx,
                        const SearchColumnInfo& column,
                        const duckdb::BoundOperatorExpression& op_expr);

absl::Status FromInetNetworkOp(BoolTarget filter, const FilterContext& ctx,
                               const InetNetworkPredicate& predicate);

}  // namespace sdb::connector
