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

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/vector_size.hpp>
#include <span>
#include <type_traits>
#include <utility>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/dispatch.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::numeric {

template<size_t W>
using Bits = std::conditional_t<
  W == 1, uint8_t,
  std::conditional_t<W == 2, uint16_t,
                     std::conditional_t<W == 4, uint32_t, uint64_t>>>;

template<typename U>
constexpr uint8_t BytesFor(U max) noexcept {
  if (max <= 0xFF) {
    return 1;
  }
  if (max <= 0xFFFF) {
    return 2;
  }
  if (static_cast<uint64_t>(max) <= 0xFFFFFFFFULL) {
    return 4;
  }
  return 8;
}

template<size_t kMax>
consteval auto StoredWidths() noexcept {
  std::array<uint8_t, std::bit_width(kMax)> widths{};
  for (size_t i = 0; i != widths.size(); ++i) {
    widths[i] = static_cast<uint8_t>(size_t{1} << i);
  }
  return widths;
}

template<size_t kMax>
struct StoredAxis : ArrayAxis<StoredWidths<kMax>()> {
  template<uint8_t K, typename F>
  IRS_FORCE_INLINE static decltype(auto) Call(F&& f) {
    return std::forward<F>(f).template operator()<Bits<K>>();
  }
};

template<typename S, typename F>
void NarrowAs(size_t n, uint8_t* out, F value) noexcept {
  for (size_t i = 0; i < n; ++i) {
    duckdb::Store(static_cast<S>(value(i)), out + i * sizeof(S));
  }
}

template<size_t kMax, typename F>
void Narrow(size_t n, uint8_t stored, uint8_t* out, F value) noexcept {
  Resolve<StoredAxis<kMax>>(
    stored, [&]<typename S>() IRS_FORCE_INLINE { NarrowAs<S>(n, out, value); });
}

template<typename U, typename S>
void WidenAs(const uint8_t* in, size_t n, U* out) noexcept {
  for (size_t i = 0; i < n; ++i) {
    out[i] = static_cast<U>(duckdb::Load<S>(in + i * sizeof(S)));
  }
}

template<typename U>
void Widen(const uint8_t* in, size_t n, uint8_t stored, U* out) noexcept {
  Resolve<StoredAxis<sizeof(U)>>(
    stored, [&]<typename S>() IRS_FORCE_INLINE { WidenAs<U, S>(in, n, out); });
}

inline uint64_t ShuffledItem(const uint8_t* in, size_t n, uint8_t stored,
                             size_t at) noexcept {
  uint64_t v = 0;
  for (uint8_t b = 0; b < stored; ++b) {
    v |= uint64_t{in[b * n + at]} << (8 * b);
  }
  return v;
}

inline uint64_t ShuffledSum(const uint8_t* in, size_t n, uint8_t stored,
                            size_t begin, size_t end) noexcept {
  uint64_t sum = 0;
  for (uint8_t b = 0; b < stored; ++b) {
    const auto* lane = in + b * n;
    uint64_t bytes = 0;
    for (size_t i = begin; i < end; ++i) {
      bytes += lane[i];
    }
    sum += bytes << (8 * b);
  }
  return sum;
}

using U8x16 = uint8_t __attribute__((vector_size(16)));

IRS_FORCE_INLINE inline U8x16 Bytes16(const uint8_t* in) noexcept {
  U8x16 v;
  std::memcpy(&v, in, sizeof(v));
  return v;
}

IRS_FORCE_INLINE inline U8x16 Lane(const uint8_t* in, size_t n, size_t b,
                                   size_t i) noexcept {
  return Bytes16(in + b * n + i);
}

inline void Put(uint8_t* out, size_t at, U8x16 v) noexcept {
  std::memcpy(out + at, &v, sizeof(v));
}

inline U8x16 Bytes8Lo(U8x16 a, U8x16 b) noexcept {
  return __builtin_shufflevector(a, b, 0, 16, 1, 17, 2, 18, 3, 19, 4, 20, 5, 21,
                                 6, 22, 7, 23);
}

inline U8x16 Bytes8Hi(U8x16 a, U8x16 b) noexcept {
  return __builtin_shufflevector(a, b, 8, 24, 9, 25, 10, 26, 11, 27, 12, 28, 13,
                                 29, 14, 30, 15, 31);
}

inline U8x16 Bytes16Lo(U8x16 a, U8x16 b) noexcept {
  return __builtin_shufflevector(a, b, 0, 1, 16, 17, 2, 3, 18, 19, 4, 5, 20, 21,
                                 6, 7, 22, 23);
}

inline U8x16 Bytes16Hi(U8x16 a, U8x16 b) noexcept {
  return __builtin_shufflevector(a, b, 8, 9, 24, 25, 10, 11, 26, 27, 12, 13, 28,
                                 29, 14, 15, 30, 31);
}

inline U8x16 Bytes32Lo(U8x16 a, U8x16 b) noexcept {
  return __builtin_shufflevector(a, b, 0, 1, 2, 3, 16, 17, 18, 19, 4, 5, 6, 7,
                                 20, 21, 22, 23);
}

inline U8x16 Bytes32Hi(U8x16 a, U8x16 b) noexcept {
  return __builtin_shufflevector(a, b, 8, 9, 10, 11, 24, 25, 26, 27, 12, 13, 14,
                                 15, 28, 29, 30, 31);
}

inline U8x16 EvenBytes(U8x16 a, U8x16 b) noexcept {
  return __builtin_shufflevector(a, b, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20,
                                 22, 24, 26, 28, 30);
}

inline U8x16 OddBytes(U8x16 a, U8x16 b) noexcept {
  return __builtin_shufflevector(a, b, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21,
                                 23, 25, 27, 29, 31);
}

inline size_t Shuffle2(const uint8_t* in, size_t n, uint8_t* out) noexcept {
  size_t i = 0;
  for (; i + 16 <= n; i += 16) {
    const auto a = Bytes16(in + 2 * i);
    const auto b = Bytes16(in + 2 * i + 16);
    Put(out, i, EvenBytes(a, b));
    Put(out, n + i, OddBytes(a, b));
  }
  return i;
}

inline size_t Shuffle4(const uint8_t* in, size_t n, uint8_t* out) noexcept {
  size_t i = 0;
  for (; i + 16 <= n; i += 16) {
    const auto* row = in + 4 * i;
    const auto even_lo = EvenBytes(Bytes16(row), Bytes16(row + 16));
    const auto odd_lo = OddBytes(Bytes16(row), Bytes16(row + 16));
    const auto even_hi = EvenBytes(Bytes16(row + 32), Bytes16(row + 48));
    const auto odd_hi = OddBytes(Bytes16(row + 32), Bytes16(row + 48));
    Put(out, i, EvenBytes(even_lo, even_hi));
    Put(out, n + i, EvenBytes(odd_lo, odd_hi));
    Put(out, 2 * n + i, OddBytes(even_lo, even_hi));
    Put(out, 3 * n + i, OddBytes(odd_lo, odd_hi));
  }
  return i;
}

inline size_t Shuffle8(const uint8_t* in, size_t n, uint8_t* out) noexcept {
  size_t i = 0;
  for (; i + 16 <= n; i += 16) {
    const auto* row = in + 8 * i;
    U8x16 even[4];
    U8x16 odd[4];
    for (size_t p = 0; p < 4; ++p) {
      const auto a = Bytes16(row + 32 * p);
      const auto b = Bytes16(row + 32 * p + 16);
      even[p] = EvenBytes(a, b);
      odd[p] = OddBytes(a, b);
    }
    const U8x16 pairs[8] = {
      EvenBytes(even[0], even[1]), EvenBytes(even[2], even[3]),
      EvenBytes(odd[0], odd[1]),   EvenBytes(odd[2], odd[3]),
      OddBytes(even[0], even[1]),  OddBytes(even[2], even[3]),
      OddBytes(odd[0], odd[1]),    OddBytes(odd[2], odd[3]),
    };
    for (size_t b = 0; b < 4; ++b) {
      Put(out, b * n + i, EvenBytes(pairs[2 * b], pairs[2 * b + 1]));
      Put(out, (b + 4) * n + i, OddBytes(pairs[2 * b], pairs[2 * b + 1]));
    }
  }
  return i;
}

inline void Shuffle(const uint8_t* in, size_t n, uint8_t stored,
                    uint8_t* out) noexcept {
  size_t done = 0;
  switch (stored) {
    case 2:
      done = Shuffle2(in, n, out);
      break;
    case 4:
      done = Shuffle4(in, n, out);
      break;
    case 8:
      done = Shuffle8(in, n, out);
      break;
  }
  for (size_t i = done; i < n; ++i) {
    for (uint8_t b = 0; b < stored; ++b) {
      out[b * n + i] = in[i * stored + b];
    }
  }
}

inline size_t Unshuffle2(const uint8_t* in, size_t n, uint8_t* out) noexcept {
  size_t i = 0;
  for (; i + 16 <= n; i += 16) {
    const auto a = Lane(in, n, 0, i);
    const auto b = Lane(in, n, 1, i);
    Put(out, 2 * i, Bytes8Lo(a, b));
    Put(out, 2 * i + 16, Bytes8Hi(a, b));
  }
  return i;
}

inline size_t Unshuffle4(const uint8_t* in, size_t n, uint8_t* out) noexcept {
  size_t i = 0;
  for (; i + 16 <= n; i += 16) {
    const auto ab_lo = Bytes8Lo(Lane(in, n, 0, i), Lane(in, n, 1, i));
    const auto ab_hi = Bytes8Hi(Lane(in, n, 0, i), Lane(in, n, 1, i));
    const auto cd_lo = Bytes8Lo(Lane(in, n, 2, i), Lane(in, n, 3, i));
    const auto cd_hi = Bytes8Hi(Lane(in, n, 2, i), Lane(in, n, 3, i));
    auto* row = out + 4 * i;
    Put(row, 0, Bytes16Lo(ab_lo, cd_lo));
    Put(row, 16, Bytes16Hi(ab_lo, cd_lo));
    Put(row, 32, Bytes16Lo(ab_hi, cd_hi));
    Put(row, 48, Bytes16Hi(ab_hi, cd_hi));
  }
  return i;
}

inline size_t Unshuffle8(const uint8_t* in, size_t n, uint8_t* out) noexcept {
  size_t i = 0;
  for (; i + 16 <= n; i += 16) {
    U8x16 lo[4];
    U8x16 hi[4];
    for (size_t p = 0; p < 4; ++p) {
      const auto a = Lane(in, n, 2 * p, i);
      const auto b = Lane(in, n, 2 * p + 1, i);
      lo[p] = Bytes8Lo(a, b);
      hi[p] = Bytes8Hi(a, b);
    }
    const U8x16 quads[8] = {
      Bytes16Lo(lo[0], lo[1]), Bytes16Hi(lo[0], lo[1]), Bytes16Lo(hi[0], hi[1]),
      Bytes16Hi(hi[0], hi[1]), Bytes16Lo(lo[2], lo[3]), Bytes16Hi(lo[2], lo[3]),
      Bytes16Lo(hi[2], hi[3]), Bytes16Hi(hi[2], hi[3]),
    };
    auto* row = out + 8 * i;
    for (size_t q = 0; q < 4; ++q) {
      Put(row, 32 * q, Bytes32Lo(quads[q], quads[q + 4]));
      Put(row, 32 * q + 16, Bytes32Hi(quads[q], quads[q + 4]));
    }
  }
  return i;
}

inline void Unshuffle(const uint8_t* in, size_t n, uint8_t stored,
                      uint8_t* out) noexcept {
  size_t done = 0;
  switch (stored) {
    case 2:
      done = Unshuffle2(in, n, out);
      break;
    case 4:
      done = Unshuffle4(in, n, out);
      break;
    case 8:
      done = Unshuffle8(in, n, out);
      break;
  }
  for (uint8_t b = 0; b < stored; ++b) {
    const auto* lane = in + b * n;
    for (size_t i = done; i < n; ++i) {
      out[i * stored + b] = lane[i];
    }
  }
}

template<typename U, typename S>
void WidenAddAs(const uint8_t* in, size_t n, U base, U* out) noexcept {
  for (size_t i = 0; i < n; ++i) {
    out[i] = static_cast<U>(
      static_cast<U>(duckdb::Load<S>(in + i * sizeof(S))) + base);
  }
}

template<typename U>
void WidenAdd(const uint8_t* in, size_t n, uint8_t stored, U base,
              U* out) noexcept {
  Resolve<StoredAxis<sizeof(U)>>(stored, [&]<typename S>() IRS_FORCE_INLINE {
    WidenAddAs<U, S>(in, n, base, out);
  });
}

template<typename U, typename S>
void PutWidened(U8x16 items, U base, U* out) noexcept {
  using Stored = S __attribute__((vector_size(16)));
  using Wide = U __attribute__((vector_size(16 / sizeof(S) * sizeof(U))));
  const Wide wide =
    __builtin_convertvector(std::bit_cast<Stored>(items), Wide) + base;
  std::memcpy(out, &wide, sizeof(wide));
}

template<typename U>
size_t UnshuffleWidenAdd2(const uint8_t* in, size_t n, U base,
                          U* out) noexcept {
  size_t i = 0;
  for (; i + 16 <= n; i += 16) {
    const auto a = Lane(in, n, 0, i);
    const auto b = Lane(in, n, 1, i);
    PutWidened<U, uint16_t>(Bytes8Lo(a, b), base, out + i);
    PutWidened<U, uint16_t>(Bytes8Hi(a, b), base, out + i + 8);
  }
  return i;
}

template<typename U>
size_t UnshuffleWidenAdd4(const uint8_t* in, size_t n, U base,
                          U* out) noexcept {
  size_t i = 0;
  for (; i + 16 <= n; i += 16) {
    const auto ab_lo = Bytes8Lo(Lane(in, n, 0, i), Lane(in, n, 1, i));
    const auto ab_hi = Bytes8Hi(Lane(in, n, 0, i), Lane(in, n, 1, i));
    const auto cd_lo = Bytes8Lo(Lane(in, n, 2, i), Lane(in, n, 3, i));
    const auto cd_hi = Bytes8Hi(Lane(in, n, 2, i), Lane(in, n, 3, i));
    PutWidened<U, uint32_t>(Bytes16Lo(ab_lo, cd_lo), base, out + i);
    PutWidened<U, uint32_t>(Bytes16Hi(ab_lo, cd_lo), base, out + i + 4);
    PutWidened<U, uint32_t>(Bytes16Lo(ab_hi, cd_hi), base, out + i + 8);
    PutWidened<U, uint32_t>(Bytes16Hi(ab_hi, cd_hi), base, out + i + 12);
  }
  return i;
}

template<typename U>
void UnshuffleWidenAdd(const uint8_t* in, size_t n, uint8_t stored, U base,
                       U* out) noexcept {
  SDB_ASSERT(stored < sizeof(U));
  size_t done = 0;
  if constexpr (sizeof(U) > 2) {
    if (stored == 2) {
      done = UnshuffleWidenAdd2(in, n, base, out);
    }
  }
  if constexpr (sizeof(U) > 4) {
    if (stored == 4) {
      done = UnshuffleWidenAdd4(in, n, base, out);
    }
  }
  for (size_t i = done; i < n; ++i) {
    out[i] =
      static_cast<U>(static_cast<U>(ShuffledItem(in, n, stored, i)) + base);
  }
}

#ifdef __AVX2__
template<typename S>
__m256i WidenLanes(const uint8_t* in) noexcept {
  if constexpr (sizeof(S) == 8) {
    return _mm256_loadu_si256(reinterpret_cast<const __m256i*>(in));
  } else if constexpr (sizeof(S) == 4) {
    return _mm256_cvtepu32_epi64(
      _mm_loadu_si128(reinterpret_cast<const __m128i*>(in)));
  } else if constexpr (sizeof(S) == 2) {
    return _mm256_cvtepu16_epi64(
      _mm_loadl_epi64(reinterpret_cast<const __m128i*>(in)));
  } else {
    return _mm256_cvtepu8_epi64(_mm_cvtsi32_si128(duckdb::Load<int32_t>(in)));
  }
}

template<typename S>
size_t WidenPrefixSumLanes(const uint8_t* in, size_t n, uint64_t& acc,
                           uint64_t offset, uint64_t* out) noexcept {
  const auto off = _mm256_set1_epi64x(static_cast<int64_t>(offset));
  auto carry = _mm256_set1_epi64x(static_cast<int64_t>(acc));
  size_t i = 0;
  for (; i + 4 <= n; i += 4) {
    auto x = _mm256_add_epi64(WidenLanes<S>(in + i * sizeof(S)), off);
    x = _mm256_add_epi64(x, _mm256_slli_si256(x, 8));
    x =
      _mm256_add_epi64(x, _mm256_blend_epi32(_mm256_permute4x64_epi64(x, 0x55),
                                             _mm256_setzero_si256(), 0x0F));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(out + i),
                        _mm256_add_epi64(x, carry));
    carry = _mm256_add_epi64(carry, _mm256_permute4x64_epi64(x, 0xFF));
  }
  acc = static_cast<uint64_t>(_mm256_extract_epi64(carry, 0));
  return i;
}

template<typename S>
__m256i WidenHalfLanes(const uint8_t* in) noexcept {
  if constexpr (sizeof(S) == 4) {
    return _mm256_loadu_si256(reinterpret_cast<const __m256i*>(in));
  } else if constexpr (sizeof(S) == 2) {
    return _mm256_cvtepu16_epi32(
      _mm_loadu_si128(reinterpret_cast<const __m128i*>(in)));
  } else {
    return _mm256_cvtepu8_epi32(
      _mm_loadl_epi64(reinterpret_cast<const __m128i*>(in)));
  }
}

template<typename S>
size_t WidenPrefixSumLanes(const uint8_t* in, size_t n, uint32_t& acc,
                           uint32_t offset, uint32_t* out) noexcept {
  const auto off = _mm256_set1_epi32(static_cast<int32_t>(offset));
  const auto last = _mm256_set1_epi32(7);
  auto carry = _mm256_set1_epi32(static_cast<int32_t>(acc));
  size_t i = 0;
  for (; i + 8 <= n; i += 8) {
    auto x = _mm256_add_epi32(WidenHalfLanes<S>(in + i * sizeof(S)), off);
    x = _mm256_add_epi32(x, _mm256_slli_si256(x, 4));
    x = _mm256_add_epi32(x, _mm256_slli_si256(x, 8));
    x = _mm256_add_epi32(
      x, _mm256_shuffle_epi32(_mm256_permute2x128_si256(x, x, 0x08), 0xFF));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(out + i),
                        _mm256_add_epi32(x, carry));
    carry = _mm256_add_epi32(carry, _mm256_permutevar8x32_epi32(x, last));
  }
  acc = static_cast<uint32_t>(_mm256_extract_epi32(carry, 0));
  return i;
}
#endif

template<typename U, typename S, bool kWrite>
U WidenPrefixSumAs(const uint8_t* in, size_t n, U acc, U offset,
                   U* out) noexcept {
  size_t i = 0;
#ifdef __AVX2__
  if constexpr (kWrite && sizeof(U) >= sizeof(uint32_t)) {
    i = WidenPrefixSumLanes<S>(in, n, acc, offset, out);
  }
#endif
  for (; i < n; ++i) {
    const auto s = duckdb::Load<S>(in + i * sizeof(S));
    acc = static_cast<U>(acc + static_cast<U>(static_cast<U>(s) + offset));
    if constexpr (kWrite) {
      out[i] = acc;
    }
  }
  return acc;
}

template<bool kWrite, typename U>
U WidenPrefixSum(const uint8_t* in, size_t n, uint8_t stored, U acc, U offset,
                 U* out) noexcept {
  return Resolve<StoredAxis<sizeof(U)>>(
    stored, [&]<typename S>() IRS_FORCE_INLINE {
      return WidenPrefixSumAs<U, S, kWrite>(in, n, acc, offset, out);
    });
}

template<typename U>
using WideWord = std::conditional_t<(sizeof(U) < 4), uint32_t, U>;

template<typename U>
U OddInverse(U odd) noexcept {
  using W = WideWord<U>;
  W inverse = odd;
  for (int i = 0; i < 5; ++i) {
    inverse = static_cast<W>(inverse * static_cast<W>(2 - odd * inverse));
  }
  return static_cast<U>(inverse);
}

template<bool kSigned, typename U>
void DivideExact(const U* in, size_t n, U divisor, U* out) noexcept {
  using S = std::make_signed_t<U>;
  using W = WideWord<U>;
  const auto shift = std::countr_zero(divisor);
  const W inverse = OddInverse(static_cast<U>(divisor >> shift));
  for (size_t i = 0; i < n; ++i) {
    U x;
    if constexpr (kSigned) {
      x = static_cast<U>(static_cast<S>(in[i]) >> shift);
    } else {
      x = static_cast<U>(in[i] >> shift);
    }
    out[i] = static_cast<U>(static_cast<W>(x) * inverse);
  }
}

template<typename U>
void MultiplyBy(U* v, size_t n, U factor) noexcept {
  using W = WideWord<U>;
  for (size_t i = 0; i < n; ++i) {
    v[i] = static_cast<U>(static_cast<W>(v[i]) * static_cast<W>(factor));
  }
}

inline constexpr size_t kRunPad = 8;

#ifdef __AVX2__
inline constexpr bool kExpandsRuns = true;

inline constexpr auto kRunPicks = [] {
  std::array<uint64_t, 256> picks{};
  for (uint32_t ended = 0; ended < picks.size(); ++ended) {
    uint64_t run = 0;
    for (uint32_t row = 0; row < 8; ++row) {
      picks[ended] |= run << (8 * row);
      run += (ended >> row) & 1;
    }
  }
  return picks;
}();

inline constexpr auto kRunWordPicks = [] {
  std::array<std::array<uint8_t, 16>, 256> picks{};
  for (uint32_t ended = 0; ended < picks.size(); ++ended) {
    uint8_t run = 0;
    for (uint32_t row = 0; row < 8; ++row) {
      picks[ended][2 * row] = static_cast<uint8_t>(2 * run);
      picks[ended][2 * row + 1] = static_cast<uint8_t>(2 * run + 1);
      run += (ended >> row) & 1;
    }
  }
  return picks;
}();

inline constexpr auto kRunWidePicks = [] {
  std::array<uint64_t, 16> picks{};
  for (uint32_t ended = 0; ended < picks.size(); ++ended) {
    uint64_t run = 0;
    for (uint32_t row = 0; row < 4; ++row) {
      picks[ended] |= (2 * run) << (16 * row);
      picks[ended] |= (2 * run + 1) << (16 * row + 8);
      run += (ended >> row) & 1;
    }
  }
  return picks;
}();

template<typename U>
IRS_FORCE_INLINE void ExpandRunRows(const U* runs, uint32_t ended,
                                    void* out) noexcept {
  if constexpr (sizeof(U) == 8) {
    const auto low = ended & 15;
    _mm256_storeu_si256(
      static_cast<__m256i*>(out),
      _mm256_permutevar8x32_epi32(
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(runs)),
        _mm256_cvtepu8_epi32(_mm_loadl_epi64(
          reinterpret_cast<const __m128i*>(&kRunWidePicks[low])))));
    _mm256_storeu_si256(
      static_cast<__m256i*>(out) + 1,
      _mm256_permutevar8x32_epi32(
        _mm256_loadu_si256(
          reinterpret_cast<const __m256i*>(runs + std::popcount(low))),
        _mm256_cvtepu8_epi32(_mm_loadl_epi64(
          reinterpret_cast<const __m128i*>(&kRunWidePicks[ended >> 4])))));
  } else if constexpr (sizeof(U) == 4) {
    _mm256_storeu_si256(
      static_cast<__m256i*>(out),
      _mm256_permutevar8x32_epi32(
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(runs)),
        _mm256_cvtepu8_epi32(_mm_loadl_epi64(
          reinterpret_cast<const __m128i*>(&kRunPicks[ended])))));
  } else if constexpr (sizeof(U) == 2) {
    _mm_storeu_si128(
      static_cast<__m128i*>(out),
      _mm_shuffle_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(runs)),
                       _mm_loadu_si128(reinterpret_cast<const __m128i*>(
                         kRunWordPicks[ended].data()))));
  } else {
    _mm_storel_epi64(
      static_cast<__m128i*>(out),
      _mm_shuffle_epi8(
        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(runs)),
        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&kRunPicks[ended]))));
  }
}
#else
inline constexpr bool kExpandsRuns = false;
#endif

template<typename U, typename T>
void ExpandMarkedRuns(const uint8_t* ended, const U* runs, size_t rows,
                      T* out) noexcept {
  size_t i = 0;
#ifdef __AVX2__
  for (; i + 16 <= rows; i += 16) {
    const auto mask = static_cast<uint32_t>(_mm_movemask_epi8(
      _mm_loadu_si128(reinterpret_cast<const __m128i*>(ended + i))));
    const auto low = mask & 0xFF;
    const auto* next = runs + std::popcount(low);
    ExpandRunRows(runs, low, out + i);
    ExpandRunRows(next, mask >> 8, out + i + 8);
    runs = next + std::popcount(mask >> 8);
  }
  if (i + 8 <= rows) {
    const auto mask = static_cast<uint32_t>(_mm_movemask_epi8(
      _mm_loadl_epi64(reinterpret_cast<const __m128i*>(ended + i))));
    ExpandRunRows(runs, mask, out + i);
    runs += std::popcount(mask);
    i += 8;
  }
#endif
  for (; i < rows; ++i) {
    out[i] = std::bit_cast<T>(*runs);
    runs += ended[i] & 1;
  }
}

template<typename U, typename T>
uint32_t ExpandRuns(std::span<const uint32_t> ends, const U* values,
                    uint32_t run, uint32_t row, uint64_t count,
                    T* out) noexcept {
  uint8_t ended[STANDARD_VECTOR_SIZE];
  while (count != 0) {
    const auto rows =
      static_cast<uint32_t>(std::min<uint64_t>(count, STANDARD_VECTOR_SIZE));
    const auto stop = row + rows;
    std::memset(ended, 0, rows);
    auto next = run;
    for (; next < ends.size() && ends[next] <= stop; ++next) {
      ended[ends[next] - row - 1] = 0xFF;
    }
    ExpandMarkedRuns(ended, values + run, rows, out);
    run = next;
    row = stop;
    out += rows;
    count -= rows;
  }
  return run;
}

inline constexpr size_t kBlockValues = 1024;

template<typename U>
using LaneWord = std::conditional_t<sizeof(U) == 8, uint64_t, uint32_t>;

template<typename U>
inline constexpr unsigned kLanes = 1024 / (8 * sizeof(LaneWord<U>));

template<typename U>
inline constexpr unsigned kMaxBits = 8 * sizeof(U);

constexpr size_t PackedBytes(unsigned bits) noexcept {
  return kBlockValues / 8 * bits;
}

template<typename U>
constexpr size_t PackedBytes(unsigned bits, size_t values) noexcept {
  constexpr size_t kWordBits = 8 * sizeof(LaneWord<U>);
  const size_t rows = (values + kLanes<U> - 1) / kLanes<U>;
  return (rows * bits + kWordBits - 1) / kWordBits * (kBlockValues / 8);
}

template<typename U, unsigned W, unsigned J>
IRS_FORCE_INLINE void PackStep(const U* IRS_RESTRICT in,
                               LaneWord<U>* IRS_RESTRICT out, U base,
                               unsigned l, LaneWord<U>& acc) noexcept {
  using Word = LaneWord<U>;
  constexpr unsigned kWordBits = 8 * sizeof(Word);
  constexpr unsigned kBit = J * W;
  constexpr unsigned kShift = kBit % kWordBits;
  const auto v =
    static_cast<Word>(static_cast<U>(in[J * kLanes<U> + l] - base));
  if constexpr (kShift == 0) {
    acc = v;
  } else {
    acc |= static_cast<Word>(v << kShift);
  }
  if constexpr (kShift + W >= kWordBits) {
    out[kBit / kWordBits * kLanes<U> + l] = acc;
    if constexpr (kShift + W > kWordBits) {
      acc = static_cast<Word>(v >> (kWordBits - kShift));
    }
  }
}

template<typename U, unsigned W, unsigned... J>
IRS_FORCE_INLINE void PackLane(const U* IRS_RESTRICT in,
                               LaneWord<U>* IRS_RESTRICT out, U base,
                               unsigned l,
                               std::integer_sequence<unsigned, J...>) noexcept {
  LaneWord<U> acc = 0;
  (PackStep<U, W, J>(in, out, base, l, acc), ...);
}

template<typename U, unsigned W>
void PackBlock(const U* IRS_RESTRICT in, LaneWord<U>* IRS_RESTRICT out,
               U base) noexcept {
  if constexpr (W == 8 * sizeof(LaneWord<U>)) {
    for (unsigned i = 0; i < kBlockValues; ++i) {
      out[i] = static_cast<LaneWord<U>>(static_cast<U>(in[i] - base));
    }
  } else if constexpr (W != 0) {
    constexpr unsigned kRows = 8 * sizeof(LaneWord<U>);
    for (unsigned l = 0; l < kLanes<U>; ++l) {
      PackLane<U, W>(in, out, base, l,
                     std::make_integer_sequence<unsigned, kRows>{});
    }
  }
}

template<typename U, unsigned W, unsigned J>
IRS_FORCE_INLINE void UnpackStep(const LaneWord<U>* IRS_RESTRICT in,
                                 U* IRS_RESTRICT out, U base,
                                 unsigned l) noexcept {
  using Word = LaneWord<U>;
  constexpr unsigned kWordBits = 8 * sizeof(Word);
  constexpr unsigned kBit = J * W;
  constexpr unsigned kWord = kBit / kWordBits;
  constexpr unsigned kShift = kBit % kWordBits;
  constexpr auto kMask = static_cast<Word>(~Word{0} >> (kWordBits - W));
  Word v = in[kWord * kLanes<U> + l] >> kShift;
  if constexpr (kShift + W > kWordBits) {
    v |= in[(kWord + 1) * kLanes<U> + l] << (kWordBits - kShift);
  }
  out[J * kLanes<U> + l] = static_cast<U>(static_cast<U>(v & kMask) + base);
}

template<typename U, unsigned W, unsigned... J>
IRS_FORCE_INLINE void UnpackLane(
  const LaneWord<U>* IRS_RESTRICT in, U* IRS_RESTRICT out, U base, unsigned l,
  std::integer_sequence<unsigned, J...>) noexcept {
  (UnpackStep<U, W, J>(in, out, base, l), ...);
}

template<typename U, unsigned W, unsigned J>
IRS_FORCE_INLINE void UnpackRow(const LaneWord<U>* IRS_RESTRICT in,
                                U* IRS_RESTRICT out, U base) noexcept {
  for (unsigned l = 0; l < kLanes<U>; ++l) {
    UnpackStep<U, W, J>(in, out, base, l);
  }
}

template<typename U, unsigned W, unsigned... J>
IRS_FORCE_INLINE void UnpackRows(
  const LaneWord<U>* IRS_RESTRICT in, U* IRS_RESTRICT out, U base,
  std::integer_sequence<unsigned, J...>) noexcept {
  (UnpackRow<U, W, J>(in, out, base), ...);
}

template<typename U, unsigned W>
void UnpackBlock(const LaneWord<U>* IRS_RESTRICT in, U* IRS_RESTRICT out,
                 U base) noexcept {
  constexpr unsigned kRows = 8 * sizeof(LaneWord<U>);
  if constexpr (W == 0) {
    std::fill_n(out, kBlockValues, base);
  } else if constexpr (W == kRows) {
    for (unsigned i = 0; i < kBlockValues; ++i) {
      out[i] = static_cast<U>(in[i] + base);
    }
  } else if constexpr (kRows == 64 && W > 32) {
    UnpackRows<U, W>(in, out, base,
                     std::make_integer_sequence<unsigned, kRows>{});
  } else {
    for (unsigned l = 0; l < kLanes<U>; ++l) {
      UnpackLane<U, W>(in, out, base, l,
                       std::make_integer_sequence<unsigned, kRows>{});
    }
  }
}

template<typename U>
IRS_FORCE_INLINE U UnpackValue(const uint8_t* packed, unsigned bits,
                               uint32_t index, U base) noexcept {
  using Word = LaneWord<U>;
  constexpr unsigned kWordBits = 8 * sizeof(Word);
  if (bits == 0) {
    return base;
  }
  const unsigned bit = index / kLanes<U> * bits;
  const auto* at = packed + (size_t{bit / kWordBits} * kLanes<U> +
                             index % kLanes<U>)*sizeof(Word);
  const unsigned shift = bit % kWordBits;
  const bool split = shift + bits > kWordBits;
  const auto low = duckdb::Load<Word>(at);
  const auto high =
    duckdb::Load<Word>(at + (split ? kLanes<U> * sizeof(Word) : 0));
  const auto carried =
    static_cast<Word>(static_cast<Word>(high << 1) << (kWordBits - 1 - shift));
  const auto v = static_cast<Word>(
    (low >> shift) | (carried & static_cast<Word>(-static_cast<Word>(split))));
  const Word mask =
    bits == kWordBits ? ~Word{0} : static_cast<Word>((Word{1} << bits) - 1);
  return static_cast<U>(static_cast<U>(v & mask) + base);
}

template<typename U>
IRS_FORCE_INLINE void Pack(unsigned bits, const U* IRS_RESTRICT in,
                           LaneWord<U>* IRS_RESTRICT out, U base) noexcept {
  [&]<unsigned... W>(std::integer_sequence<unsigned, W...>) IRS_FORCE_INLINE {
    (void)((bits == W && (PackBlock<U, W>(in, out, base), true)) || ...);
  }(std::make_integer_sequence<unsigned, kMaxBits<U> + 1>{});
}

template<typename U>
IRS_FORCE_INLINE void Unpack(unsigned bits, const LaneWord<U>* IRS_RESTRICT in,
                             U* IRS_RESTRICT out, U base) noexcept {
  [&]<unsigned... W>(std::integer_sequence<unsigned, W...>) IRS_FORCE_INLINE {
    (void)((bits == W && (UnpackBlock<U, W>(in, out, base), true)) || ...);
  }(std::make_integer_sequence<unsigned, kMaxBits<U> + 1>{});
}

}  // namespace irs::codecs::numeric
