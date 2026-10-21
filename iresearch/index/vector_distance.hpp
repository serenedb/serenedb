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

#include <core_functions/array_kernels.hpp>

#include "iresearch/index/column_info.hpp"

namespace irs {

template<VectorMetric Metric>
float ComputeDistance(const float* l, const float* r, uint16_t d) {
  if constexpr (Metric == VectorMetric::L2Sqr) {
    return -duckdb::DistanceSquaredOp::Operation(l, r, d);
  } else if constexpr (Metric == VectorMetric::L1) {
    return -duckdb::L1DistanceOp::Operation(l, r, d);
  } else if constexpr (Metric == VectorMetric::InnerProduct) {
    return duckdb::InnerProductOp::Operation(l, r, d);
  } else if constexpr (Metric == VectorMetric::Cosine) {
    return duckdb::CosineSimilarityOp::Operation(l, r, d);
  }
  SDB_UNREACHABLE();
}

template<VectorMetric Metric>
float ComputeDistance(const byte_type* l, const byte_type* r, uint16_t d) {
  return ComputeDistance<Metric>(reinterpret_cast<const float*>(l),
                                 reinterpret_cast<const float*>(r), d);
}

}  // namespace irs
