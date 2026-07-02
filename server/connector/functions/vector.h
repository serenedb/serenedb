////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/operator/logical_order.hpp>
#include <iresearch/index/column_info.hpp>
#include <optional>
#include <string>

#include "connector/duckdb_table_function.h"

namespace sdb::connector {

struct AnnFunctionInfo : public duckdb::ScalarFunctionInfo {
  irs::VectorMetric metric;
  duckdb::OrderType order;
  bool is_norm;  // unary norm form (l2_norm/l1_norm) vs binary distance
  ScoreEmit score_emit;

  AnnFunctionInfo(irs::VectorMetric m, duckdb::OrderType o, bool n, ScoreEmit e)
    : metric(m), order(o), is_norm(n), score_emit(e) {}
};

inline std::optional<AnnFunctionInfo> GetAnnFunctionInfo(
  const duckdb::BoundFunctionExpression& func) {
  if (!func.function.HasExtraFunctionInfo()) {
    return std::nullopt;
  }
  if (const auto* info = dynamic_cast<const AnnFunctionInfo*>(
        &func.function.GetExtraFunctionInfo())) {
    return *info;
  }
  return std::nullopt;
}

inline constexpr std::string_view kL2Distance = "l2_distance";
inline constexpr std::string_view kL2DistanceOp = "<->";
inline constexpr std::string_view kL2SqrDistance = "l2_sqr_distance";
inline constexpr std::string_view kL1Distance = "l1_distance";
inline constexpr std::string_view kL1DistanceOp = "<+>";
inline constexpr std::string_view kCosineDistanceOp = "<=>";
inline constexpr std::string_view kCosineDistance = "cosine_distance";
inline constexpr std::string_view kCosineSimilarity = "cosine_similarity";
inline constexpr std::string_view kIP = "inner_product";
inline constexpr std::string_view kNegativeIP = "negative_inner_product";
inline constexpr std::string_view kNegativeIPDistanceOp = "<#>";
inline constexpr std::string_view kL1Norm = "l1_norm";
inline constexpr std::string_view kL2Norm = "l2_norm";
inline constexpr std::string_view kL1Normalize = "l1_normalize";
inline constexpr std::string_view kL2Normalize = "l2_normalize";

void RegisterVectorFunctions(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector
