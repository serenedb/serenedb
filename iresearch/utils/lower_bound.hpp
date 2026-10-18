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
/// Copyright holder is SereneDB GmbH
////////////////////////////////////////////////////////////////////////////////

#pragma once

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <type_traits>

#include "iresearch/utils/shared.hpp"

namespace irs {

template<size_t W>
IRS_FORCE_INLINE uint32_t CountLess(const uint32_t* begin,
                                    uint32_t value) noexcept {
  static_assert(W % 16 == 0);
#ifdef __AVX2__
  const __m256i bias = _mm256_set1_epi32(std::numeric_limits<int32_t>::min());
  const __m256i target =
    _mm256_xor_si256(_mm256_set1_epi32(static_cast<int32_t>(value)), bias);
  const auto less = [&](size_t j) IRS_FORCE_INLINE {
    return _mm256_cmpgt_epi32(
      target,
      _mm256_xor_si256(
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(begin + j)), bias));
  };
  uint32_t count = 0;
  size_t i = 0;
  for (; i + 32 <= W; i += 32) {
    const __m256i low = _mm256_packs_epi32(less(i), less(i + 8));
    const __m256i high = _mm256_packs_epi32(less(i + 16), less(i + 24));
    count += std::popcount(static_cast<uint32_t>(
      _mm256_movemask_epi8(_mm256_packs_epi16(low, high))));
  }
  if constexpr (W % 32 != 0) {
    count += std::popcount(static_cast<uint32_t>(_mm256_movemask_epi8(
               _mm256_packs_epi32(less(i), less(i + 8))))) /
             2;
  }
  return count;
#else
  using U32x8 = uint32_t __attribute__((vector_size(32)));
  using I32x8 = int32_t __attribute__((vector_size(32)));
  const U32x8 target = U32x8{} + value;
  I32x8 acc{};
  for (size_t j = 0; j != W; j += 8) {
    U32x8 v;
    std::memcpy(&v, begin + j, sizeof(v));
    acc += (I32x8)(v < target);
  }
  int32_t sum = 0;
  for (size_t lane = 0; lane != 8; ++lane) {
    sum += acc[lane];
  }
  return static_cast<uint32_t>(-sum);
#endif
}

template<size_t N, typename It, typename T, typename Cmp = std::less<>>
IRS_FORCE_INLINE It BranchlessLowerBound(It begin, const T& value,
                                         Cmp&& compare = {}) {
  static_assert(std::has_single_bit(N));
  constexpr size_t kGroup = 32;
  if constexpr (N > kGroup && std::is_pointer_v<It> &&
                std::is_same_v<std::remove_const_t<std::remove_pointer_t<It>>,
                               uint32_t> &&
                std::is_same_v<T, uint32_t> &&
                std::is_same_v<std::remove_cvref_t<Cmp>, std::less<>>) {
    size_t group = 0;
    for (size_t g = 1; g != N / kGroup; ++g) {
      group += static_cast<size_t>(begin[g * kGroup - 1] < value);
    }
    begin += group * kGroup;
    return begin + CountLess<kGroup>(begin, value);
  } else {
    for (size_t step = N / 2; step != 0; step /= 2) {
      begin =
        IRS_UNPREDICTABLE(compare(begin[step], value)) ? begin + step : begin;
    }
    return begin + compare(*begin, value);
  }
}

template<typename It, typename Pred>
IRS_FORCE_INLINE It PartitionPoint(It begin, size_t len, Pred&& pred) {
  if (len == 0) {
    return begin;
  }
  const auto end = begin + len;
  size_t step = std::bit_floor(len);
  if (step != len && pred(begin[step])) {
    const auto rest = len - step - 1;
    if (rest == 0) {
      return end;
    }
    step = std::bit_ceil(rest);
    begin = end - step;
  }
  for (step /= 2; step != 0; step /= 2) {
    if (pred(begin[step])) {
      begin += step;
    }
  }
  return begin + pred(*begin);
}

template<typename It, typename Pred>
IRS_FORCE_INLINE It BranchlessPartitionPoint(It begin, size_t len,
                                             Pred&& pred) {
  if (len == 0) {
    return begin;
  }
  const auto end = begin + len;
  size_t step = std::bit_floor(len);
  if (step != len) {
    const bool right = IRS_UNPREDICTABLE(pred(begin[step]));
    const size_t tail = std::bit_ceil(len - step - 1);
    begin = right ? end - tail : begin;
    step = right ? tail : step;
  }
  for (step /= 2; step != 0; step /= 2) {
    __builtin_prefetch(&begin[step / 4]);
    __builtin_prefetch(&begin[step / 2 + step / 4]);
    __builtin_prefetch(&begin[step + step / 4]);
    __builtin_prefetch(&begin[step + step / 2 + step / 4]);
    begin = IRS_UNPREDICTABLE(pred(begin[step])) ? begin + step : begin;
  }
  return begin + pred(*begin);
}

}  // namespace irs
