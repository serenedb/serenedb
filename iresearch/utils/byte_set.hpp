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

#if defined(__SSE4_1__)
#include <smmintrin.h>
#endif

#include <array>
#include <cstddef>
#include <cstdint>

#include "iresearch/types.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {

struct ByteSet {
  IRS_FORCE_INLINE void Add(byte_type b) noexcept {
    words[b >> 6] |= uint64_t{1} << (b & 63);
  }
  IRS_FORCE_INLINE bool Contains(byte_type b) const noexcept {
    return ((words[b >> 6] >> (b & 63)) & 1) != 0;
  }

  alignas(16) std::array<uint64_t, 4> words{};
};

IRS_FORCE_INLINE inline uint32_t ClassifyByteSet16(
  const byte_type* block, const ByteSet& set) noexcept {
#if defined(__SSE4_1__)
  const auto* table = reinterpret_cast<const __m128i*>(set.words.data());
  const auto bytes = _mm_loadu_si128(reinterpret_cast<const __m128i*>(block));
  const auto nibble = _mm_set1_epi8(0x0F);
  const auto at = _mm_and_si128(_mm_srli_epi16(bytes, 3), nibble);
  const auto rows =
    _mm_blendv_epi8(_mm_shuffle_epi8(_mm_load_si128(table), at),
                    _mm_shuffle_epi8(_mm_load_si128(table + 1), at), bytes);
  const auto bits = _mm_shuffle_epi8(
    _mm_setr_epi8(1, 2, 4, 8, 16, 32, 64, -128, 1, 2, 4, 8, 16, 32, 64, -128),
    _mm_and_si128(bytes, nibble));
  return static_cast<uint32_t>(
    _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_and_si128(rows, bits), bits)));
#else
  uint32_t found = 0;
  for (size_t i = 0; i != 16; ++i) {
    found |= uint32_t{set.Contains(block[i])} << i;
  }
  return found;
#endif
}

}  // namespace irs
