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

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <duckdb/common/numeric_utils.hpp>
#include <duckdb/common/typedefs.hpp>
#include <duckdb/common/vector_size.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <type_traits>

#include "iresearch/utils/assert.hpp"

namespace irs::codecs::alp {

inline constexpr uint32_t kVectorSize = 1024;
inline constexpr uint32_t kRowGroupSamples = 8;
inline constexpr uint16_t kSamplesPerVector = 32;
inline constexpr uint32_t kRowGroupSamplesJump =
  (DEFAULT_ROW_GROUP_SIZE / kRowGroupSamples) / STANDARD_VECTOR_SIZE;
inline constexpr double kCompactBlockThreshold = 0.80;

template<typename T>
using ExactType =
  std::conditional_t<std::is_same_v<T, double>, uint64_t, uint32_t>;

struct SamplingParameters {
  uint32_t n_lookup_values = 0;
  uint32_t n_sampled_increments = 0;
  uint32_t n_sampled_values = 0;
};

inline SamplingParameters GetSamplingParameters(
  duckdb::idx_t current_vector_n_values) {
  const auto n_lookup_values = duckdb::NumericCast<uint32_t>(
    std::min<duckdb::idx_t>(current_vector_n_values, kVectorSize));
  const auto n_sampled_increments = std::max<uint32_t>(
    1, duckdb::ExactNumericCast<uint32_t>(
         std::ceil(static_cast<double>(n_lookup_values) / kSamplesPerVector)));
  const auto n_sampled_values = duckdb::ExactNumericCast<uint32_t>(
    std::ceil(static_cast<double>(n_lookup_values) / n_sampled_increments));
  SDB_ASSERT(n_sampled_values <= kSamplesPerVector);
  return {
    .n_lookup_values = n_lookup_values,
    .n_sampled_increments = n_sampled_increments,
    .n_sampled_values = n_sampled_values,
  };
}

inline bool MustSkipSamplingFromCurrentVector(
  duckdb::idx_t vectors_count, duckdb::idx_t vectors_sampled_count,
  duckdb::idx_t current_vector_n_values) {
  if (vectors_count % kRowGroupSamplesJump) {
    return true;
  }
  return current_vector_n_values < kSamplesPerVector && vectors_sampled_count;
}

template<typename T>
void FindAndReplaceNullsInVector(T* input_vector,
                                 const uint16_t* vector_null_positions,
                                 duckdb::idx_t values_count,
                                 duckdb::idx_t nulls_count) {
  if (!nulls_count) {
    return;
  }
  T a_non_null_value{};
  duckdb::idx_t null_idx = 0;
  for (duckdb::idx_t i = 0; i < values_count; ++i) {
    if (null_idx < nulls_count && vector_null_positions[null_idx] == i) {
      ++null_idx;
      continue;
    }
    a_non_null_value = input_vector[i];
    break;
  }
  for (duckdb::idx_t i = 0; i < nulls_count; ++i) {
    input_vector[vector_null_positions[i]] = a_non_null_value;
  }
}

}  // namespace irs::codecs::alp
