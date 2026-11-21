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

#include <absl/algorithm/container.h>
#include <absl/base/optimization.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/numeric_utils.hpp>
#include <duckdb/common/operator/comparison_operators.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/planner/filter/zonemap_checker.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <limits>
#include <memory>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/numeric/alp_primitives.hpp"
#include "iresearch/formats/column/codecs/numeric/alp_scan.tpp"
#include "iresearch/formats/column/codecs/numeric_axis.hpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs {
namespace {

using alp::kMetadataPointerSize;
using alp::kVectorSize;
using duckdb::idx_t;

constexpr uint8_t kUncompressedModeSentinel =
  std::numeric_limits<uint8_t>::max();
constexpr uint32_t kHeaderSize = sizeof(uint32_t);
constexpr uint32_t kExponentSize = sizeof(uint8_t);
constexpr uint32_t kFactorSize = sizeof(uint8_t);
constexpr uint32_t kExceptionsCountSize = sizeof(uint16_t);
constexpr uint32_t kExceptionPositionSize = sizeof(uint16_t);
constexpr uint32_t kForSize = sizeof(uint64_t);
constexpr uint32_t kBitWidthSize = sizeof(uint8_t);
constexpr uint32_t kVectorHeaderSize =
  kExponentSize + kFactorSize + kExceptionsCountSize + kForSize + kBitWidthSize;
constexpr uint8_t kMaxBitWidth = sizeof(uint64_t) * 8;
constexpr idx_t kSamplingEarlyExitThreshold = 2;
constexpr double kEncodingUpperLimit = 9223372036854774784.0;
constexpr double kEncodingLowerLimit = -9223372036854774784.0;
constexpr auto kEncodedUpperLimit = static_cast<int64_t>(kEncodingUpperLimit);
constexpr size_t kMaxCombinations = 5;

constexpr int64_t kFactArr[] = {
  1,
  10,
  100,
  1000,
  10000,
  100000,
  1000000,
  10000000,
  100000000,
  1000000000,
  10000000000,
  100000000000,
  1000000000000,
  10000000000000,
  100000000000000,
  1000000000000000,
  10000000000000000,
  100000000000000000,
  1000000000000000000,
};

template<typename T>
struct AlpTypedConstants;

template<>
struct AlpTypedConstants<float> {
  static constexpr float kMagicNumber = 12582912.0F;
  static constexpr uint8_t kMaxExponent = 10;
  static constexpr float kExpArr[] = {
    1.0F,         10.0F,         100.0F,         1000.0F,
    10000.0F,     100000.0F,     1000000.0F,     10000000.0F,
    100000000.0F, 1000000000.0F, 10000000000.0F,
  };
  static constexpr float kFracArr[] = {
    1.0F,      0.1F,       0.01F,       0.001F,       0.0001F,       0.00001F,
    0.000001F, 0.0000001F, 0.00000001F, 0.000000001F, 0.0000000001F,
  };
};

template<>
struct AlpTypedConstants<double> {
  static constexpr double kMagicNumber = 6755399441055744.0;
  static constexpr uint8_t kMaxExponent = 18;
  static constexpr double kExpArr[] = {
    1.0,
    10.0,
    100.0,
    1000.0,
    10000.0,
    100000.0,
    1000000.0,
    10000000.0,
    100000000.0,
    1000000000.0,
    10000000000.0,
    100000000000.0,
    1000000000000.0,
    10000000000000.0,
    100000000000000.0,
    1000000000000000.0,
    10000000000000000.0,
    100000000000000000.0,
    1000000000000000000.0,
    10000000000000000000.0,
    100000000000000000000.0,
    1000000000000000000000.0,
    10000000000000000000000.0,
    100000000000000000000000.0,
  };
  static constexpr double kFracArr[] = {
    1.0,
    0.1,
    0.01,
    0.001,
    0.0001,
    0.00001,
    0.000001,
    0.0000001,
    0.00000001,
    0.000000001,
    0.0000000001,
    0.00000000001,
    0.000000000001,
    0.0000000000001,
    0.00000000000001,
    0.000000000000001,
    0.0000000000000001,
    0.00000000000000001,
    0.000000000000000001,
    0.0000000000000000001,
    0.00000000000000000001,
  };
};

[[noreturn]] IRS_NO_INLINE void ThrowAlpVectorOutOfRange() {
  ThrowCorrupted("ALP", "vector exceeds its data range");
}

[[noreturn]] IRS_NO_INLINE void ThrowAlpExponentOutOfRange(
  uint8_t exponent, uint8_t max_exponent) {
  ThrowCorrupted("ALP", absl::StrCat("exponent ", exponent,
                                     " exceeds the maximum ", max_exponent));
}

[[noreturn]] IRS_NO_INLINE void ThrowAlpFactorOutOfRange(uint8_t factor,
                                                         uint8_t exponent) {
  ThrowCorrupted(
    "ALP", absl::StrCat("factor ", factor, " exceeds exponent ", exponent));
}

[[noreturn]] IRS_NO_INLINE void ThrowAlpExceptionCountOutOfRange(
  uint16_t exception_count, idx_t vector_size) {
  ThrowCorrupted("ALP", absl::StrCat("exception count ", exception_count,
                                     " exceeds vector size ", vector_size));
}

[[noreturn]] IRS_NO_INLINE void ThrowAlpBitWidthOutOfRange(uint8_t bit_width) {
  ThrowCorrupted(
    "ALP", absl::StrCat("bit width ", bit_width, " exceeds ", kMaxBitWidth));
}

[[noreturn]] IRS_NO_INLINE void ThrowAlpExceptionPositionOutOfRange(
  uint16_t position, idx_t vector_size) {
  ThrowCorrupted("ALP", absl::StrCat("exception position ", position,
                                     " is outside vector size ", vector_size));
}

template<typename T>
[[noreturn]] IRS_NO_INLINE void ThrowAlpVector(uint8_t exponent, uint8_t factor,
                                               uint16_t exceptions_count,
                                               uint8_t bit_width,
                                               idx_t vector_size) {
  if (exponent > AlpTypedConstants<T>::kMaxExponent) {
    ThrowAlpExponentOutOfRange(exponent, AlpTypedConstants<T>::kMaxExponent);
  }
  if (exceptions_count > vector_size) {
    ThrowAlpExceptionCountOutOfRange(exceptions_count, vector_size);
  }
  if (factor > exponent) {
    ThrowAlpFactorOutOfRange(factor, exponent);
  }
  if (bit_width > kMaxBitWidth) {
    ThrowAlpBitWidthOutOfRange(bit_width);
  }
  ThrowAlpVectorOutOfRange();
}

struct AlpEncodingIndices {
  uint8_t exponent = 0;
  uint8_t factor = 0;
};

struct AlpCombination {
  AlpEncodingIndices encoding_indices;
  uint64_t n_appearances = 0;
  uint64_t estimated_compression_size = 0;
};

template<typename T>
struct AlpCompressionData {
  AlpEncodingIndices vector_encoding_indices;
  uint16_t exceptions_count = 0;
  uint16_t bit_width = 0;
  uint64_t bp_size = 0;
  uint64_t frame_of_reference = 0;
  int64_t encoded_integers[kVectorSize] = {};
  T exceptions[kVectorSize] = {};
  uint16_t exceptions_positions[kVectorSize] = {};
  std::vector<AlpCombination> best_k_combinations;
  uint32_t values_encoded[kVectorSize * 2] = {};
};

template<typename T>
idx_t RequiredSpace(const AlpCompressionData<T>& data) {
  return data.bp_size +
         (data.exceptions_count *
          (sizeof(alp::ExactType<T>) + kExceptionPositionSize)) +
         kVectorHeaderSize;
}

template<typename T>
bool IsImpossibleToEncode(T n) {
  return !std::isfinite(n) || n > kEncodingUpperLimit ||
         n < kEncodingLowerLimit || (n == 0.0 && std::signbit(n));
}

template<typename T>
int64_t NumberToInt64(T n) {
  if (IsImpossibleToEncode(n)) {
    return kEncodedUpperLimit;
  }
  n =
    n + AlpTypedConstants<T>::kMagicNumber - AlpTypedConstants<T>::kMagicNumber;
  if (IsImpossibleToEncode(n)) {
    return kEncodedUpperLimit;
  }
  return static_cast<int64_t>(n);
}

template<typename T>
int64_t EncodeValue(T value, AlpEncodingIndices encoding_indices) {
  const T tmp_encoded_value =
    value * AlpTypedConstants<T>::kExpArr[encoding_indices.exponent] *
    AlpTypedConstants<T>::kFracArr[encoding_indices.factor];
  return NumberToInt64(tmp_encoded_value);
}

template<typename T>
T DecodeValue(int64_t encoded_value, AlpEncodingIndices encoding_indices) {
  return static_cast<T>(encoded_value) *
         static_cast<T>(kFactArr[encoding_indices.factor]) *
         AlpTypedConstants<T>::kFracArr[encoding_indices.exponent];
}

bool CompareAlpCombinations(const AlpCombination& c1,
                            const AlpCombination& c2) {
  return (c1.n_appearances > c2.n_appearances) ||
         (c1.n_appearances == c2.n_appearances &&
          (c1.estimated_compression_size < c2.estimated_compression_size)) ||
         ((c1.n_appearances == c2.n_appearances &&
           c1.estimated_compression_size == c2.estimated_compression_size) &&
          (c2.encoding_indices.exponent < c1.encoding_indices.exponent)) ||
         ((c1.n_appearances == c2.n_appearances &&
           c1.estimated_compression_size == c2.estimated_compression_size &&
           c2.encoding_indices.exponent == c1.encoding_indices.exponent) &&
          (c2.encoding_indices.factor < c1.encoding_indices.factor));
}

template<typename T, bool kPenalizeExceptions>
uint64_t DryCompressToEstimateSize(std::span<const T> input_vector,
                                   AlpEncodingIndices encoding_indices,
                                   uint64_t limit) {
  constexpr uint64_t kExceptionBits =
    sizeof(T) * 8 + kExceptionPositionSize * 8;
  constexpr auto kOverLimit = std::numeric_limits<uint64_t>::max();
  constexpr size_t kProbe = 8;
  const idx_t n_values = input_vector.size();
  idx_t exceptions_count = 0;
  idx_t non_exceptions_count = 0;
  int64_t max_encoded_value = std::numeric_limits<int64_t>::min();
  int64_t min_encoded_value = std::numeric_limits<int64_t>::max();
  const auto encode = [&](std::span<const T> values) {
    for (const T value : values) {
      const int64_t encoded_value = EncodeValue(value, encoding_indices);
      const T decoded_value = DecodeValue<T>(encoded_value, encoding_indices);
      if (decoded_value == value) {
        ++non_exceptions_count;
        max_encoded_value = std::max(encoded_value, max_encoded_value);
        min_encoded_value = std::min(encoded_value, min_encoded_value);
        continue;
      }
      ++exceptions_count;
    }
  };
  const auto probe = input_vector.first(limit < n_values * kExceptionBits / 2
                                          ? std::min<size_t>(n_values, kProbe)
                                          : 0);
  encode(probe);
  uint64_t at_least = exceptions_count * kExceptionBits;
  if (non_exceptions_count) {
    const uint64_t delta = static_cast<uint64_t>(max_encoded_value) -
                           static_cast<uint64_t>(min_encoded_value);
    at_least += n_values * (std::bit_width(delta | 1) - 1);
  }
  if (at_least > limit) {
    return kOverLimit;
  }
  encode(input_vector.subspan(probe.size()));
  if constexpr (kPenalizeExceptions) {
    if (non_exceptions_count < 2) {
      return kOverLimit;
    }
  }
  const uint64_t delta = static_cast<uint64_t>(max_encoded_value) -
                         static_cast<uint64_t>(min_encoded_value);
  const auto estimated_bits_per_value =
    duckdb::ExactNumericCast<uint32_t>(std::ceil(std::log2(delta + 1)));
  return n_values * estimated_bits_per_value +
         exceptions_count * kExceptionBits;
}

template<typename T>
void FindTopKCombinations(const std::vector<std::vector<T>>& vectors_sampled,
                          AlpCompressionData<T>& compression_data) {
  constexpr uint8_t kMaxExponent = AlpTypedConstants<T>::kMaxExponent;
  constexpr idx_t kExactTypeBitSize = sizeof(T) * 8;
  compression_data.best_k_combinations.clear();

  std::array<std::array<uint64_t, kMaxExponent + 1>, kMaxExponent + 1>
    appearances{};
  AlpEncodingIndices previous_best{.exponent = kMaxExponent,
                                   .factor = kMaxExponent};
  for (const auto& sampled_vector : vectors_sampled) {
    const idx_t n_samples = sampled_vector.size();
    const idx_t best_total_bits =
      (n_samples * (kExactTypeBitSize + kExceptionPositionSize * 8)) +
      (n_samples * kExactTypeBitSize);
    AlpCombination best_combination{
      .encoding_indices = {.exponent = kMaxExponent, .factor = kMaxExponent},
      .n_appearances = 0,
      .estimated_compression_size = best_total_bits,
    };
    const auto consider = [&](AlpEncodingIndices encoding_indices) {
      const AlpCombination combination{
        .encoding_indices = encoding_indices,
        .n_appearances = 0,
        .estimated_compression_size = DryCompressToEstimateSize<T, true>(
          sampled_vector, encoding_indices,
          best_combination.estimated_compression_size),
      };
      if (CompareAlpCombinations(combination, best_combination)) {
        best_combination = combination;
      }
    };
    consider(previous_best);
    for (int32_t exp_idx = kMaxExponent; exp_idx >= 0; --exp_idx) {
      for (int32_t factor_idx = exp_idx; factor_idx >= 0; --factor_idx) {
        consider({.exponent = static_cast<uint8_t>(exp_idx),
                  .factor = static_cast<uint8_t>(factor_idx)});
      }
    }
    previous_best = best_combination.encoding_indices;
    ++appearances[best_combination.encoding_indices.exponent]
                 [best_combination.encoding_indices.factor];
  }

  std::vector<AlpCombination> best_k_combinations;
  for (uint8_t exponent = 0; exponent <= kMaxExponent; ++exponent) {
    for (uint8_t factor = 0; factor <= exponent; ++factor) {
      if (const auto n_appearances = appearances[exponent][factor]) {
        best_k_combinations.emplace_back(
          AlpEncodingIndices{.exponent = exponent, .factor = factor},
          n_appearances, 0);
      }
    }
  }
  absl::c_sort(best_k_combinations, CompareAlpCombinations);
  best_k_combinations.resize(
    std::min(kMaxCombinations, best_k_combinations.size()));
  compression_data.best_k_combinations = std::move(best_k_combinations);
}

template<typename T>
void FindBestFactorAndExponent(const T* input_vector, idx_t n_values,
                               AlpCompressionData<T>& compression_data) {
  T vector_sample[alp::kSamplesPerVector] = {};
  idx_t n_samples = 0;
  const auto idx_increments = std::max<uint32_t>(
    1, duckdb::ExactNumericCast<uint32_t>(
         std::ceil(static_cast<double>(n_values) / alp::kSamplesPerVector)));
  for (idx_t i = 0; i < n_values; i += idx_increments) {
    vector_sample[n_samples++] = input_vector[i];
  }

  AlpEncodingIndices best_encoding_indices;
  uint64_t best_total_bits = std::numeric_limits<uint64_t>::max();
  idx_t worse_total_bits_counter = 0;
  for (const auto& combination : compression_data.best_k_combinations) {
    const uint64_t estimated_compression_size =
      DryCompressToEstimateSize<T, false>({vector_sample, n_samples},
                                          combination.encoding_indices,
                                          best_total_bits);
    if (estimated_compression_size >= best_total_bits) {
      ++worse_total_bits_counter;
      if (worse_total_bits_counter == kSamplingEarlyExitThreshold) {
        break;
      }
      continue;
    }
    best_total_bits = estimated_compression_size;
    best_encoding_indices = combination.encoding_indices;
    worse_total_bits_counter = 0;
  }
  compression_data.vector_encoding_indices = best_encoding_indices;
}

template<typename T, bool kEmpty>
void Compress(const T* input_vector, idx_t n_values,
              const uint16_t* vector_null_positions, idx_t nulls_count,
              AlpCompressionData<T>& compression_data) {
  if (compression_data.best_k_combinations.size() > 1) {
    FindBestFactorAndExponent(input_vector, n_values, compression_data);
  } else {
    compression_data.vector_encoding_indices =
      compression_data.best_k_combinations[0].encoding_indices;
  }

  uint16_t exceptions_idx = 0;
  for (idx_t i = 0; i < n_values; ++i) {
    const T actual_value = input_vector[i];
    const int64_t encoded_value =
      EncodeValue(actual_value, compression_data.vector_encoding_indices);
    const T decoded_value =
      DecodeValue<T>(encoded_value, compression_data.vector_encoding_indices);
    compression_data.encoded_integers[i] = encoded_value;
    const bool is_exception = decoded_value != actual_value;
    compression_data.exceptions_positions[exceptions_idx] =
      static_cast<uint16_t>(i);
    exceptions_idx += is_exception;
  }

  int64_t a_non_exception_value = 0;
  for (idx_t i = 0; i < n_values; ++i) {
    if (i != compression_data.exceptions_positions[i]) {
      a_non_exception_value = compression_data.encoded_integers[i];
      break;
    }
  }
  for (idx_t i = 0; i < exceptions_idx; ++i) {
    const idx_t exception_pos = compression_data.exceptions_positions[i];
    const T actual_value = input_vector[exception_pos];
    compression_data.encoded_integers[exception_pos] = a_non_exception_value;
    compression_data.exceptions[i] = actual_value;
  }
  compression_data.exceptions_count = exceptions_idx;

  for (idx_t i = 0; i < nulls_count; ++i) {
    compression_data.encoded_integers[vector_null_positions[i]] =
      a_non_exception_value;
  }

  auto min_value = std::numeric_limits<int64_t>::max();
  auto max_value = std::numeric_limits<int64_t>::min();
  for (idx_t i = 0; i < n_values; ++i) {
    max_value = std::max(max_value, compression_data.encoded_integers[i]);
    min_value = std::min(min_value, compression_data.encoded_integers[i]);
  }
  const uint64_t min_max_diff =
    static_cast<uint64_t>(max_value) - static_cast<uint64_t>(min_value);

  auto* u_encoded_integers =
    reinterpret_cast<uint64_t*>(compression_data.encoded_integers);
  const auto u_min_value = static_cast<uint64_t>(min_value);
  if constexpr (!kEmpty) {
    for (idx_t i = 0; i < n_values; ++i) {
      u_encoded_integers[i] -= u_min_value;
    }
  }

  const auto bit_width =
    duckdb::BitpackingPrimitives::MinimumBitWidth<uint64_t, false>(
      min_max_diff);
  const auto bp_size =
    duckdb::BitpackingPrimitives::GetRequiredSize(n_values, bit_width);
  if constexpr (!kEmpty) {
    if (bit_width) {
      duckdb::BitpackingPrimitives::PackBuffer<uint64_t, false>(
        reinterpret_cast<duckdb::data_ptr_t>(compression_data.values_encoded),
        u_encoded_integers, n_values, bit_width);
    }
  }
  compression_data.bit_width = bit_width;
  compression_data.bp_size = bp_size;
  compression_data.frame_of_reference = static_cast<uint64_t>(min_value);
}

template<typename T>
void Decompress(duckdb::const_data_ptr_t for_encoded, T* output, idx_t count,
                uint8_t vector_factor, uint8_t vector_exponent,
                uint16_t exceptions_count, const T* exceptions,
                const uint16_t* exceptions_positions,
                uint64_t frame_of_reference, uint8_t bit_width) {
  const AlpEncodingIndices encoding_indices{
    .exponent = vector_exponent,
    .factor = vector_factor,
  };
  if (const auto base = static_cast<int64_t>(frame_of_reference);
      std::is_same_v<T, double> && bit_width != 0 && bit_width < 32 &&
      base >= std::numeric_limits<int32_t>::min() &&
      base <=
        std::numeric_limits<int32_t>::max() - ((int64_t{1} << bit_width) - 1)) {
    ABSL_CACHELINE_ALIGNED uint32_t narrow[kVectorSize];
    duckdb::BitpackingPrimitives::UnPackBuffer<uint32_t>(
      reinterpret_cast<duckdb::data_ptr_t>(narrow), for_encoded, count,
      bit_width);
    const auto first = static_cast<uint32_t>(base);
    const auto factor = static_cast<T>(kFactArr[vector_factor]);
    const auto fraction = AlpTypedConstants<T>::kFracArr[vector_exponent];
#pragma clang loop vectorize_width(2)
    for (idx_t i = 0; i < count; ++i) {
      output[i] = static_cast<T>(static_cast<int32_t>(narrow[i] + first)) *
                  factor * fraction;
    }
  } else {
    ABSL_CACHELINE_ALIGNED uint64_t for_decoded[kVectorSize] = {};
    if (bit_width) {
      duckdb::BitpackingPrimitives::UnPackBuffer<uint64_t>(
        reinterpret_cast<duckdb::data_ptr_t>(for_decoded), for_encoded, count,
        bit_width);
    }
    for (idx_t i = 0; i < count; ++i) {
      for_decoded[i] += frame_of_reference;
    }
    for (idx_t i = 0; i < count; ++i) {
      const auto encoded_integer = static_cast<int64_t>(for_decoded[i]);
      output[i] = DecodeValue<T>(encoded_integer, encoding_indices);
    }
  }
  for (idx_t i = 0; i < exceptions_count; ++i) {
    output[exceptions_positions[i]] = exceptions[i];
  }
}

template<typename T>
struct AlpAnalyzeState {
  idx_t vectors_sampled_count = 0;
  idx_t total_values_count = 0;
  idx_t vectors_count = 0;
  std::vector<std::vector<T>> rowgroup_sample;
  std::vector<std::vector<T>> complete_vectors_sampled;
  AlpCompressionData<T> compression_data;
};

template<typename T>
void AlpSample(AlpAnalyzeState<T>& analyze_state, const duckdb::Vector& input) {
  const auto count = input.size();
  const bool must_skip_current_vector = alp::MustSkipSamplingFromCurrentVector(
    analyze_state.vectors_count, analyze_state.vectors_sampled_count, count);
  analyze_state.vectors_count += 1;
  analyze_state.total_values_count += count;
  if (must_skip_current_vector) {
    return;
  }

  const auto values = input.Values<T>();
  const auto sampling_params = alp::GetSamplingParameters(count);

  uint16_t current_vector_null_positions[kVectorSize] = {};
  std::vector<T> current_vector_values(sampling_params.n_lookup_values);
  std::vector<T> current_vector_sample(sampling_params.n_sampled_values);

  idx_t nulls_idx = 0;
  if (!values.CanHaveNull()) {
    for (idx_t i = 0; i < sampling_params.n_lookup_values; ++i) {
      current_vector_values[i] = values.GetValueUnsafe(i);
    }
  } else {
    for (idx_t i = 0; i < sampling_params.n_lookup_values; ++i) {
      const auto entry = values[i];
      current_vector_values[i] = entry.GetValueUnsafe();
      current_vector_null_positions[nulls_idx] = static_cast<uint16_t>(i);
      nulls_idx += !entry.IsValid();
    }
    alp::FindAndReplaceNullsInVector<T>(
      current_vector_values.data(), current_vector_null_positions,
      sampling_params.n_lookup_values, nulls_idx);
  }

  idx_t sample_idx = 0;
  for (idx_t i = 0; i < sampling_params.n_lookup_values;
       i += sampling_params.n_sampled_increments) {
    current_vector_sample[sample_idx++] = current_vector_values[i];
  }
  SDB_ASSERT(sample_idx == sampling_params.n_sampled_values);

  analyze_state.complete_vectors_sampled.push_back(
    std::move(current_vector_values));
  analyze_state.rowgroup_sample.push_back(std::move(current_vector_sample));
  ++analyze_state.vectors_sampled_count;
}

template<typename T>
std::unique_ptr<AlpAnalyzeState<T>> AlpSampleRowGroup(
  std::span<const WriteChunk> chunks) {
  auto analyze_state = std::make_unique<AlpAnalyzeState<T>>();
  for (const auto& chunk : chunks) {
    AlpSample(*analyze_state, chunk.data);
  }
  FindTopKCombinations(analyze_state->rowgroup_sample,
                       analyze_state->compression_data);
  return analyze_state;
}

template<typename T>
idx_t AlpAnalyzeChunks(std::span<const WriteChunk> chunks) {
  const auto sampled = AlpSampleRowGroup<T>(chunks);
  auto& analyze_state = *sampled;

  idx_t total_bytes_used = 0;
  idx_t current_bytes_used_in_segment = 0;
  idx_t compressed_values = 0;
  for (const auto& vector_to_compress :
       analyze_state.complete_vectors_sampled) {
    Compress<T, true>(vector_to_compress.data(), vector_to_compress.size(),
                      nullptr, 0, analyze_state.compression_data);
    const idx_t uncompressed_size =
      kExponentSize + sizeof(T) * vector_to_compress.size();
    const idx_t compressed_size = RequiredSpace(analyze_state.compression_data);
    const idx_t vector_size =
      compressed_size < uncompressed_size ? compressed_size : uncompressed_size;
    if (duckdb::AlignValue(current_bytes_used_in_segment + vector_size) >
        kBlockSize - kMetadataPointerSize) {
      total_bytes_used += current_bytes_used_in_segment + kMetadataPointerSize;
      current_bytes_used_in_segment = 0;
    }
    current_bytes_used_in_segment += vector_size;
    compressed_values += vector_to_compress.size();
  }
  total_bytes_used += current_bytes_used_in_segment + kMetadataPointerSize;

  if (!compressed_values) {
    return duckdb::DConstants::INVALID_INDEX;
  }
  const auto factor_of_sampling =
    analyze_state.total_values_count / compressed_values;
  return duckdb::AlignValue(total_bytes_used) * factor_of_sampling;
}

template<typename T>
class AlpCompressionState {
 public:
  AlpCompressionState(ColumnDataCheckpointData& checkpoint_data,
                      AlpAnalyzeState<T>& analyze_state)
    : _checkpoint_data{checkpoint_data},
      _segment{CodecType::Alp, checkpoint_data.type} {
    _compression_data.best_k_combinations =
      std::move(analyze_state.compression_data.best_k_combinations);
  }

  void Append(const duckdb::Vector& input) {
    const auto values = input.Values<T>();
    idx_t values_left_in_data = input.size();
    idx_t offset_in_data = 0;
    while (values_left_in_data) {
      const auto values_to_fill_alp_input =
        std::min<idx_t>(kVectorSize - _vector_idx, values_left_in_data);
      if (!values.CanHaveNull()) {
        for (idx_t i = 0; i < values_to_fill_alp_input; ++i) {
          _input_vector[_vector_idx + i] =
            values.GetValueUnsafe(offset_in_data + i);
        }
      } else {
        for (idx_t i = 0; i < values_to_fill_alp_input; ++i) {
          const auto entry = values[offset_in_data + i];
          _vector_null_positions[_nulls_idx] =
            static_cast<uint16_t>(_vector_idx + i);
          _nulls_idx += !entry.IsValid();
          _input_vector[_vector_idx + i] = entry.GetValueUnsafe();
        }
      }
      offset_in_data += values_to_fill_alp_input;
      values_left_in_data -= values_to_fill_alp_input;
      _vector_idx += values_to_fill_alp_input;
      if (_vector_idx == kVectorSize) {
        CompressVector();
      }
    }
  }

  void Finalize() {
    if (_vector_idx) {
      CompressVector();
    }
    FlushSegment();
  }

 private:
  idx_t UsedSpace() const noexcept {
    return kMetadataPointerSize + _data_bytes_used;
  }

  bool HasEnoughSpace(idx_t vector_size) const noexcept {
    return duckdb::AlignValue(UsedSpace() + vector_size) <
           _metadata_offset - kMetadataPointerSize;
  }

  void CompressVector() {
    if (_nulls_idx) {
      alp::FindAndReplaceNullsInVector<T>(_input_vector, _vector_null_positions,
                                          _vector_idx, _nulls_idx);
    }
    Compress<T, false>(_input_vector, _vector_idx, _vector_null_positions,
                       _nulls_idx, _compression_data);
    const idx_t uncompressed_size = kExponentSize + sizeof(T) * _vector_idx;
    const idx_t compressed_size = RequiredSpace(_compression_data);
    const bool should_compress = compressed_size < uncompressed_size;
    const idx_t vector_size =
      should_compress ? compressed_size : uncompressed_size;

    if (!HasEnoughSpace(vector_size)) {
      FlushSegment();
    }

    if (_nulls_idx) {
      _stats_writer.SetHasNull();
    }
    if (_vector_idx != _nulls_idx) {
      _stats_writer.SetHasValid();
      for (idx_t i = 0; i < _vector_idx; ++i) {
        _stats_writer.UpdateMinMax(_input_vector[i]);
      }
    }
    _segment.count += _vector_idx;

    if (should_compress) {
      FlushCompressedVector();
    } else {
      FlushUncompressedVector();
    }
  }

  void FlushCompressedVector() {
    auto* data_ptr = _segment.Data() + UsedSpace();
    duckdb::Store<uint8_t>(_compression_data.vector_encoding_indices.exponent,
                           data_ptr);
    data_ptr += kExponentSize;
    duckdb::Store<uint8_t>(_compression_data.vector_encoding_indices.factor,
                           data_ptr);
    data_ptr += kFactorSize;
    duckdb::Store<uint16_t>(_compression_data.exceptions_count, data_ptr);
    data_ptr += kExceptionsCountSize;
    duckdb::Store<uint64_t>(_compression_data.frame_of_reference, data_ptr);
    data_ptr += kForSize;
    duckdb::Store<uint8_t>(static_cast<uint8_t>(_compression_data.bit_width),
                           data_ptr);
    data_ptr += kBitWidthSize;

    SDB_ASSERT(_compression_data.bp_size <= kVectorSize * 8);
    std::memcpy(data_ptr, _compression_data.values_encoded,
                _compression_data.bp_size);
    data_ptr += _compression_data.bp_size;

    if (_compression_data.exceptions_count) {
      const auto exceptions_bytes =
        sizeof(alp::ExactType<T>) * _compression_data.exceptions_count;
      std::memcpy(data_ptr, _compression_data.exceptions, exceptions_bytes);
      data_ptr += exceptions_bytes;
      const auto positions_bytes =
        kExceptionPositionSize * _compression_data.exceptions_count;
      std::memcpy(data_ptr, _compression_data.exceptions_positions,
                  positions_bytes);
    }

    _data_bytes_used += RequiredSpace(_compression_data);
    FlushVectorMetadata();
  }

  void FlushUncompressedVector() {
    auto* data_ptr = _segment.Data() + UsedSpace();
    duckdb::Store<uint8_t>(kUncompressedModeSentinel, data_ptr);
    data_ptr += kExponentSize;
    std::memcpy(data_ptr, _input_vector, sizeof(T) * _vector_idx);

    _data_bytes_used += kExponentSize + (sizeof(T) * _vector_idx);
    FlushVectorMetadata();
  }

  void FlushVectorMetadata() {
    _metadata_offset -= kMetadataPointerSize;
    duckdb::Store<uint32_t>(_next_vector_byte_index_start,
                            _segment.Data() + _metadata_offset);
    _next_vector_byte_index_start = duckdb::NumericCast<uint32_t>(UsedSpace());
    _vector_idx = 0;
    _nulls_idx = 0;
  }

  void FlushSegment() {
    auto* dataptr = _segment.Data();
    const idx_t metadata_offset = duckdb::AlignValue(UsedSpace());
    SDB_ASSERT(metadata_offset <= _metadata_offset);
    const idx_t bytes_used_by_metadata = kBlockSize - _metadata_offset;
    idx_t total_segment_size = kBlockSize;

    const auto used_space_percentage =
      static_cast<float>(metadata_offset + bytes_used_by_metadata) /
      static_cast<float>(total_segment_size);
    if (used_space_percentage < alp::kCompactBlockThreshold) {
      std::memmove(dataptr + metadata_offset, dataptr + _metadata_offset,
                   bytes_used_by_metadata);
      total_segment_size = metadata_offset + bytes_used_by_metadata;
    }
    std::memset(dataptr + UsedSpace(), 0,
                total_segment_size - bytes_used_by_metadata - UsedSpace());
    duckdb::Store<uint32_t>(duckdb::NumericCast<uint32_t>(total_segment_size),
                            dataptr);

    _stats_writer.Merge(_segment.stats);
    _segment.Flush(_checkpoint_data, total_segment_size);
    _stats_writer.Clear();

    _data_bytes_used = 0;
    _metadata_offset = kBlockSize;
    _next_vector_byte_index_start = kHeaderSize;
  }

  ColumnDataCheckpointData& _checkpoint_data;
  CompressedSegment _segment;
  duckdb::StatsWriter<T> _stats_writer;
  idx_t _vector_idx = 0;
  idx_t _nulls_idx = 0;
  idx_t _data_bytes_used = 0;
  idx_t _metadata_offset = kBlockSize;
  uint32_t _next_vector_byte_index_start = kHeaderSize;
  T _input_vector[kVectorSize] = {};
  uint16_t _vector_null_positions[kVectorSize] = {};
  AlpCompressionData<T> _compression_data;
};

template<typename T>
void AlpCompressChunks(ColumnDataCheckpointData& checkpoint_data,
                       std::span<const WriteChunk> chunks) {
  AlpCompressionState<T> state{checkpoint_data, *AlpSampleRowGroup<T>(chunks)};
  for (const auto& chunk : chunks) {
    state.Append(chunk.data);
  }
  state.Finalize();
}

template<typename T>
struct AlpVectorState {
  idx_t index = 0;
  T decoded_values[kVectorSize];
  T exceptions[kVectorSize];
  uint16_t exceptions_positions[kVectorSize];
  uint64_t for_encoded[kVectorSize];
};

struct AlpVector {
  duckdb::const_data_ptr_t values = nullptr;
  duckdb::const_data_ptr_t packed = nullptr;
  duckdb::const_data_ptr_t exceptions = nullptr;
  duckdb::const_data_ptr_t positions = nullptr;
  uint64_t frame_of_reference = 0;
  uint16_t exceptions_count = 0;
  uint8_t exponent = 0;
  uint8_t factor = 0;
  uint8_t bit_width = 0;
};

template<typename T>
IRS_FORCE_INLINE AlpVector OpenVector(const alp::SegmentLayout& layout,
                                      idx_t vector_index, idx_t vector_size) {
  const auto bytes =
    alp::VectorBytes(layout, vector_index, kExponentSize, "ALP");
  const auto* at = bytes.data();
  AlpVector vector{.exponent = duckdb::Load<uint8_t>(at)};
  if (vector.exponent == kUncompressedModeSentinel) {
    if (bytes.size() < kExponentSize + vector_size * sizeof(T)) [[unlikely]] {
      ThrowAlpVectorOutOfRange();
    }
    vector.values = at + kExponentSize;
    return vector;
  }
  if (bytes.size() < kVectorHeaderSize) [[unlikely]] {
    ThrowAlpVectorOutOfRange();
  }
  at += kExponentSize;
  vector.factor = duckdb::Load<uint8_t>(at);
  at += kFactorSize;
  vector.exceptions_count = duckdb::Load<uint16_t>(at);
  at += kExceptionsCountSize;
  vector.frame_of_reference = duckdb::Load<uint64_t>(at);
  at += kForSize;
  vector.bit_width = duckdb::Load<uint8_t>(at);
  at += kBitWidthSize;
  const idx_t packed_size = duckdb::BitpackingPrimitives::GetRequiredSize(
    vector_size, vector.bit_width);
  const idx_t exceptions_size = vector.exceptions_count * sizeof(T);
  if (!(vector.exponent <= AlpTypedConstants<T>::kMaxExponent &&
        vector.factor <= vector.exponent && vector.bit_width <= kMaxBitWidth &&
        vector.exceptions_count <= vector_size &&
        kVectorHeaderSize + packed_size + exceptions_size +
            vector.exceptions_count * kExceptionPositionSize <=
          bytes.size())) [[unlikely]] {
    ThrowAlpVector<T>(vector.exponent, vector.factor, vector.exceptions_count,
                      vector.bit_width, vector_size);
  }
  vector.packed = at;
  vector.exceptions = at + packed_size;
  vector.positions = vector.exceptions + exceptions_size;
  return vector;
}

alp::SegmentLayout ReadAlpSegmentLayout(duckdb::const_data_ptr_t base,
                                        idx_t size, idx_t count) {
  return alp::ReadLayout(base, kHeaderSize,
                         alp::MetadataEnd(base, size, kHeaderSize, "ALP"),
                         count, "ALP");
}

template<typename T>
struct AlpScanState final : SegmentScanState {
  using Value = T;
  using Vector = AlpVector;
  using VectorState = AlpVectorState<T>;

  AlpScanState(duckdb::BufferHandle handle_p, const ColumnSegment& segment,
               std::unique_ptr<VectorState> vector_state_p = nullptr)
    : handle{std::move(handle_p)},
      count{segment.count},
      layout{ReadAlpSegmentLayout(handle.Ptr(), segment.size, count)},
      vector_state{std::move(vector_state_p)} {}

  duckdb::BufferHandle handle;
  idx_t count = 0;
  alp::SegmentLayout layout;
  idx_t total_value_count = 0;
  std::unique_ptr<VectorState> vector_state;
  duckdb::unique_ptr<duckdb::BaseStatistics> filter_group_stats;
};

template<typename T>
AlpVector OpenVector(const AlpScanState<T>& state, idx_t vector_index) {
  return OpenVector<T>(
    state.layout, vector_index,
    std::min<idx_t>(kVectorSize, state.count - vector_index * kVectorSize));
}

template<typename T>
T* ResultValues(const AlpScanState<T>&, duckdb::Vector& result) {
  return duckdb::FlatVector::GetDataMutable<T>(result);
}

template<typename T>
void LoadVector(AlpScanState<T>& state, T* value_buffer) {
  auto& vector_state = *state.vector_state;
  vector_state.index = 0;

  const idx_t vector_size =
    std::min<idx_t>(kVectorSize, state.count - state.total_value_count);
  const auto vector = OpenVector<T>(
    state.layout, state.total_value_count / kVectorSize, vector_size);
  if (vector.exponent == kUncompressedModeSentinel) {
    std::memcpy(value_buffer, vector.values, sizeof(T) * vector_size);
    return;
  }

  static_assert(sizeof(vector_state.for_encoded) ==
                kVectorSize * kMaxBitWidth / 8);
  std::memcpy(vector_state.for_encoded, vector.packed,
              duckdb::BitpackingPrimitives::GetRequiredSize(vector_size,
                                                            vector.bit_width));
  std::memcpy(vector_state.exceptions, vector.exceptions,
              vector.exceptions_count * sizeof(T));
  std::memcpy(vector_state.exceptions_positions, vector.positions,
              vector.exceptions_count * kExceptionPositionSize);
  for (idx_t i = 0; i < vector.exceptions_count; ++i) {
    if (vector_state.exceptions_positions[i] >= vector_size) {
      ThrowAlpExceptionPositionOutOfRange(vector_state.exceptions_positions[i],
                                          vector_size);
    }
  }

  Decompress<T>(
    reinterpret_cast<duckdb::const_data_ptr_t>(vector_state.for_encoded),
    value_buffer, vector_size, vector.factor, vector.exponent,
    vector.exceptions_count, vector_state.exceptions,
    vector_state.exceptions_positions, vector.frame_of_reference,
    vector.bit_width);
}

template<typename T>
bool AlpVectorBounds(const AlpScanState<T>& scan_state, idx_t vector_idx,
                     idx_t vector_rows, T& minimum, T& maximum) {
  const auto vector = OpenVector<T>(scan_state.layout, vector_idx, vector_rows);
  if (vector.exponent == kUncompressedModeSentinel ||
      vector.bit_width >= sizeof(uint64_t) * 8) {
    return false;
  }

  const uint64_t packed_span =
    vector.bit_width ? (uint64_t{1} << vector.bit_width) - 1 : uint64_t{0};
  const auto lo_digit = static_cast<int64_t>(vector.frame_of_reference);
  const auto hi_digit =
    static_cast<int64_t>(vector.frame_of_reference + packed_span);
  if (lo_digit > hi_digit) {
    return false;
  }
  const AlpEncodingIndices encoding_indices{
    .exponent = vector.exponent,
    .factor = vector.factor,
  };
  minimum = DecodeValue<T>(lo_digit, encoding_indices);
  maximum = DecodeValue<T>(hi_digit, encoding_indices);

  for (idx_t i = 0; i < vector.exceptions_count; ++i) {
    const T exception = duckdb::Load<T>(vector.exceptions + i * sizeof(T));
    if (duckdb::LessThan::Operation(exception, minimum)) {
      minimum = exception;
    }
    if (duckdb::GreaterThan::Operation(exception, maximum)) {
      maximum = exception;
    }
  }
  return true;
}

template<typename T>
void AlpFilter(const ColumnSegment& segment, ColumnScanState& state,
               idx_t vector_count, duckdb::Vector& result,
               duckdb::SelectionVector& sel, idx_t& sel_count,
               const duckdb::TableFilter&,
               duckdb::TableFilterState& filter_state) {
  auto& scan_state = state.scan_state->Cast<AlpScanState<T>>();
  const auto& checker =
    *filter_state.Cast<duckdb::ExpressionFilterState>().zonemap_checker;
  auto verdict = duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
  if (checker.IsFullyCompiled()) {
    if (!scan_state.filter_group_stats) {
      scan_state.filter_group_stats =
        duckdb::NumericStats::CreateEmpty(segment.type).ToUnique();
      scan_state.filter_group_stats->SetHasNoNullFast();
    }
    auto& group_stats = *scan_state.filter_group_stats;
    const idx_t total_rows = scan_state.count;
    idx_t vector_idx = scan_state.total_value_count / kVectorSize;
    idx_t offset_in_vector = scan_state.total_value_count % kVectorSize;
    idx_t remaining = vector_count;
    bool first = true;
    while (remaining) {
      const idx_t vector_rows =
        std::min<idx_t>(kVectorSize, total_rows - vector_idx * kVectorSize);
      auto group_verdict = duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
      T minimum{};
      T maximum{};
      if (AlpVectorBounds<T>(scan_state, vector_idx, vector_rows, minimum,
                             maximum)) {
        duckdb::NumericStats::SetMin<T>(group_stats, minimum);
        duckdb::NumericStats::SetMax<T>(group_stats, maximum);
        group_verdict = checker.Check(group_stats, nullptr);
      }
      if (first) {
        verdict = group_verdict;
        first = false;
      } else if (group_verdict != verdict) {
        verdict = duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
      }
      if (verdict == duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE) {
        break;
      }
      remaining -= std::min<idx_t>(remaining, vector_rows - offset_in_vector);
      ++vector_idx;
      offset_in_vector = 0;
    }
  }
  if (verdict == duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE ||
      verdict == duckdb::FilterPropagateResult::FILTER_FALSE_OR_NULL) {
    alp::SkipValues(scan_state, vector_count);
    sel_count = 0;
    return;
  }
  alp::Scan<AlpScanState<T>>(segment, state, vector_count, result, 0,
                             duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
  duckdb::FlatVector::SetSize(result, duckdb::count_t(vector_count));
  if (verdict == duckdb::FilterPropagateResult::FILTER_ALWAYS_TRUE) {
    return;
  }
  duckdb::ColumnSegment::FilterSelection(sel, result, filter_state,
                                         vector_count, sel_count);
}

template<typename T>
IRS_FORCE_INLINE T ValueAt(const AlpScanState<T>&, const AlpVector& vector,
                           idx_t index) {
  if (vector.exponent == kUncompressedModeSentinel) {
    return duckdb::Load<T>(vector.values + index * sizeof(T));
  }
  if (vector.exceptions_count) {
    const auto position_at = [&](idx_t i) {
      return duckdb::Load<uint16_t>(vector.positions +
                                    i * kExceptionPositionSize);
    };
    const auto found =
      PartitionIndex(0, vector.exceptions_count,
                     [&](size_t i) { return position_at(i) < index; });
    if (found < vector.exceptions_count && position_at(found) == index) {
      return duckdb::Load<T>(vector.exceptions + found * sizeof(T));
    }
  }
  static_assert(kHeaderSize + kVectorHeaderSize >= alp::kWordReach);
  const auto encoded =
    alp::PackedValue<uint64_t>(vector.packed, index, vector.bit_width);
  return DecodeValue<T>(
    static_cast<int64_t>(encoded + vector.frame_of_reference),
    {.exponent = vector.exponent, .factor = vector.factor});
}

template<typename T>
void AlpFetchRows(const ColumnSegment& segment, ColumnFetchState& state,
                  const idx_t* rows, idx_t count, duckdb::Vector& result,
                  const duckdb::SelectionVector& result_sel,
                  idx_t result_offset) {
  SDB_ASSERT(!count || rows[count - 1] < segment.count);
  auto& fetch_state =
    state.GetOrCreateSegmentState<alp::FetchState<AlpScanState<T>>>([&] {
      return std::make_unique<alp::FetchState<AlpScanState<T>>>(segment.Pin(),
                                                                segment);
    });
  auto* result_data =
    duckdb::FlatVector::GetDataMutableUnsafe<T>(result) + result_offset;
  if (alp::CopyOneDecoded(fetch_state, rows, count, result_data, result_sel)) {
    return;
  }
  const idx_t last_vector = count ? rows[count - 1] / kVectorSize : 0;
  for (idx_t i = 0; i < count;) {
    if (rows[i] / kVectorSize == fetch_state.decoded_vector ||
        alp::Dense(fetch_state, rows, i, count, last_vector)) {
      i =
        alp::CopyDecoded(fetch_state, rows, i, count, result_data, result_sel);
      continue;
    }
    result_data[result_sel.get_index(i)] =
      alp::FetchValue(fetch_state, rows[i]);
    ++i;
  }
}

template<typename T>
constexpr DecompressionFunction kDecompression{
  .init_scan = alp::InitScan<AlpScanState<T>>,
  .scan = alp::Scan<AlpScanState<T>>,
  .filter = AlpFilter<T>,
  .fetch_rows = AlpFetchRows<T>,
  .skip = alp::Skip<AlpScanState<T>>,
};

template<typename T>
constexpr Codec kCodec{
  .type = CodecType::Alp,
  .compression = &kStreamed<CodecType::Alp, AlpAnalyzeChunks<T>,
                            AlpCompressChunks<T>, HasRows>,
  .decompression = &kDecompression<T>,
};

}  // namespace

const Codec* AlpCodec(duckdb::PhysicalType physical) {
  return Resolve<NumericAxis>(physical, [&]<typename T>() -> const Codec* {
    if constexpr (std::is_floating_point_v<T>) {
      return &kCodec<T>;
    } else {
      return nullptr;
    }
  });
}

}  // namespace irs::codecs
