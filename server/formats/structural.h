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

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iresearch/utils/shared.hpp>
#include <utility>

#if defined(__x86_64__)
#include <immintrin.h>
#elif defined(__aarch64__)
#include <arm_neon.h>
#endif

namespace sdb::formats {

inline constexpr size_t kBlockBytes = 64;

template<size_t N>
struct BlockMasks {
  std::array<uint64_t, N> masks;
};

#if defined(__x86_64__)

template<size_t N>
IRS_FORCE_INLINE BlockMasks<N> MatchBlock(const char* block,
                                          const std::array<char, N>& chars) {
  const auto lo = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block));
  const auto hi =
    _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block + 32));
  BlockMasks<N> result;
  for (size_t i = 0; i < N; ++i) {
    const auto c = _mm256_set1_epi8(chars[i]);
    const auto l =
      static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, c)));
    const auto h =
      static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, c)));
    result.masks[i] = l | (static_cast<uint64_t>(h) << 32);
  }
  return result;
}

#elif defined(__aarch64__)

IRS_FORCE_INLINE inline uint64_t ToBitmask(uint8x16_t a, uint8x16_t b,
                                           uint8x16_t c, uint8x16_t d) {
  const uint8x16_t bits = {0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80,
                           0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80};
  auto sum0 = vpaddq_u8(vandq_u8(a, bits), vandq_u8(b, bits));
  const auto sum1 = vpaddq_u8(vandq_u8(c, bits), vandq_u8(d, bits));
  sum0 = vpaddq_u8(sum0, sum1);
  sum0 = vpaddq_u8(sum0, sum0);
  return vgetq_lane_u64(vreinterpretq_u64_u8(sum0), 0);
}

template<size_t N>
IRS_FORCE_INLINE BlockMasks<N> MatchBlock(const char* block,
                                          const std::array<char, N>& chars) {
  const auto* bytes = reinterpret_cast<const uint8_t*>(block);
  const auto v0 = vld1q_u8(bytes);
  const auto v1 = vld1q_u8(bytes + 16);
  const auto v2 = vld1q_u8(bytes + 32);
  const auto v3 = vld1q_u8(bytes + 48);
  BlockMasks<N> result;
  for (size_t i = 0; i < N; ++i) {
    const auto c = vdupq_n_u8(static_cast<uint8_t>(chars[i]));
    result.masks[i] = ToBitmask(vceqq_u8(v0, c), vceqq_u8(v1, c),
                                vceqq_u8(v2, c), vceqq_u8(v3, c));
  }
  return result;
}

#else

template<size_t N>
IRS_FORCE_INLINE BlockMasks<N> MatchBlock(const char* block,
                                          const std::array<char, N>& chars) {
  BlockMasks<N> result{};
  for (size_t byte = 0; byte < kBlockBytes; ++byte) {
    for (size_t i = 0; i < N; ++i) {
      result.masks[i] |= static_cast<uint64_t>(block[byte] == chars[i]) << byte;
    }
  }
  return result;
}

#endif

template<size_t N>
IRS_FORCE_INLINE BlockMasks<N> MatchTail(const char* data, size_t size,
                                         const std::array<char, N>& chars) {
  alignas(kBlockBytes) char block[kBlockBytes];
  std::memset(block, 0, sizeof(block));
  std::memcpy(block, data, size);
  auto result = MatchBlock(block, chars);
  const auto valid =
    size == kBlockBytes ? ~uint64_t{0} : (uint64_t{1} << size) - 1;
  for (auto& mask : result.masks) {
    mask &= valid;
  }
  return result;
}

IRS_FORCE_INLINE inline uint64_t PrefixXor(uint64_t bits) noexcept {
#if defined(__x86_64__)
  return static_cast<uint64_t>(_mm_cvtsi128_si64(_mm_clmulepi64_si128(
    _mm_set_epi64x(0, static_cast<int64_t>(bits)), _mm_set1_epi8(-1), 0)));
#elif defined(__aarch64__)
  return vgetq_lane_u64(vreinterpretq_u64_p128(vmull_p64(bits, ~uint64_t{0})),
                        0);
#else
  bits ^= bits << 1;
  bits ^= bits << 2;
  bits ^= bits << 4;
  bits ^= bits << 8;
  bits ^= bits << 16;
  bits ^= bits << 32;
  return bits;
#endif
}

class EscapeScanner {
 public:
  IRS_FORCE_INLINE uint64_t Next(uint64_t backslash) noexcept {
    if (!backslash) {
      return std::exchange(_next_escaped, 0);
    }
    constexpr uint64_t kOddBits = 0xAAAAAAAAAAAAAAAAULL;
    const auto potential = backslash & ~_next_escaped;
    const auto maybe_escaped_and_odd = (potential << 1) | kOddBits;
    const auto codes = (maybe_escaped_and_odd - potential) ^ kOddBits;
    const auto escaped = codes ^ (backslash | _next_escaped);
    const auto escape = codes & backslash;
    _next_escaped = escape >> 63;
    return escaped;
  }

  bool NextEscaped() const noexcept { return _next_escaped != 0; }

 private:
  uint64_t _next_escaped = 0;
};

}  // namespace sdb::formats
