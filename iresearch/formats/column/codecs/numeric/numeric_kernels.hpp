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
#include <limits>
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

template<size_t N>
using U8x16s = std::array<U8x16, N>;

template<typename Unit, bool kHigh, size_t... I>
IRS_FORCE_INLINE U8x16 Zip(U8x16 a, U8x16 b,
                           std::index_sequence<I...>) noexcept {
  using Units = Unit __attribute__((vector_size(16)));
  constexpr size_t kUnits = sizeof...(I);
  return std::bit_cast<U8x16>(__builtin_shufflevector(
    std::bit_cast<Units>(a), std::bit_cast<Units>(b),
    ((kHigh ? kUnits / 2 : 0) + I / 2 + I % 2 * kUnits)...));
}

inline U8x16 EvenBytes(U8x16 a, U8x16 b) noexcept {
  return __builtin_shufflevector(a, b, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20,
                                 22, 24, 26, 28, 30);
}

inline U8x16 OddBytes(U8x16 a, U8x16 b) noexcept {
  return __builtin_shufflevector(a, b, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21,
                                 23, 25, 27, 29, 31);
}

template<size_t kStored>
IRS_FORCE_INLINE U8x16s<kStored> ShuffleItems(
  const U8x16s<kStored>& items) noexcept {
  if constexpr (kStored == 1) {
    return items;
  } else {
    constexpr size_t kHalf = kStored / 2;
    U8x16s<kHalf> even;
    U8x16s<kHalf> odd;
    for (size_t k = 0; k < kHalf; ++k) {
      even[k] = EvenBytes(items[2 * k], items[2 * k + 1]);
      odd[k] = OddBytes(items[2 * k], items[2 * k + 1]);
    }
    even = ShuffleItems<kHalf>(even);
    odd = ShuffleItems<kHalf>(odd);
    U8x16s<kStored> lanes;
    for (size_t k = 0; k < kHalf; ++k) {
      lanes[2 * k] = even[k];
      lanes[2 * k + 1] = odd[k];
    }
    return lanes;
  }
}

template<size_t kStored>
size_t ShuffleAs(const uint8_t* in, size_t n, uint8_t* out) noexcept {
  size_t i = 0;
  for (; i + 16 <= n; i += 16) {
    U8x16s<kStored> items;
    std::memcpy(&items, in + kStored * i, sizeof(items));
    const auto lanes = ShuffleItems<kStored>(items);
    for (size_t b = 0; b < kStored; ++b) {
      std::memcpy(out + b * n + i, &lanes[b], sizeof(U8x16));
    }
  }
  return i;
}

inline void Shuffle(const uint8_t* in, size_t n, uint8_t stored,
                    uint8_t* out) noexcept {
  const size_t done =
    Resolve<StoredAxis<8>>(stored, [&]<typename S>() IRS_FORCE_INLINE {
      return ShuffleAs<sizeof(S)>(in, n, out);
    });
  for (size_t i = done; i < n; ++i) {
    for (uint8_t b = 0; b < stored; ++b) {
      out[b * n + i] = in[i * stored + b];
    }
  }
}

template<size_t kStored>
IRS_FORCE_INLINE U8x16s<kStored> UnshuffledItems(const uint8_t* in, size_t n,
                                                 size_t i) noexcept {
  if constexpr (kStored == 1) {
    return {Bytes16(in + i)};
  } else {
    constexpr size_t kHalf = kStored / 2;
    using Unit = Bits<kHalf>;
    const auto low = UnshuffledItems<kHalf>(in, n, i);
    const auto high = UnshuffledItems<kHalf>(in + kHalf * n, n, i);
    U8x16s<kStored> items;
    for (size_t k = 0; k < kHalf; ++k) {
      items[2 * k] = Zip<Unit, false>(low[k], high[k],
                                      std::make_index_sequence<16 / kHalf>{});
      items[2 * k + 1] = Zip<Unit, true>(
        low[k], high[k], std::make_index_sequence<16 / kHalf>{});
    }
    return items;
  }
}

template<size_t kStored>
size_t UnshuffleAs(const uint8_t* in, size_t n, uint8_t* out) noexcept {
  size_t i = 0;
  for (; i + 16 <= n; i += 16) {
    const auto items = UnshuffledItems<kStored>(in, n, i);
    std::memcpy(out + kStored * i, &items, sizeof(items));
  }
  return i;
}

inline void Unshuffle(const uint8_t* in, size_t n, uint8_t stored,
                      uint8_t* out) noexcept {
  const size_t done =
    Resolve<StoredAxis<8>>(stored, [&]<typename S>() IRS_FORCE_INLINE {
      return UnshuffleAs<sizeof(S)>(in, n, out);
    });
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
size_t UnshuffleWidenAddAs(const uint8_t* in, size_t n, U base,
                           U* out) noexcept {
  using Stored = S __attribute__((vector_size(16)));
  using Wide = U __attribute__((vector_size(16 / sizeof(S) * sizeof(U))));
  size_t i = 0;
  for (; i + 16 <= n; i += 16) {
    const auto items = UnshuffledItems<sizeof(S)>(in, n, i);
    for (size_t k = 0; k < sizeof(S); ++k) {
      const Wide wide =
        __builtin_convertvector(std::bit_cast<Stored>(items[k]), Wide) + base;
      std::memcpy(out + i + k * (16 / sizeof(S)), &wide, sizeof(wide));
    }
  }
  return i;
}

template<typename U>
void UnshuffleWidenAdd(const uint8_t* in, size_t n, uint8_t stored, U base,
                       U* out) noexcept {
  SDB_ASSERT(stored < sizeof(U));
  const size_t done = Resolve<StoredAxis<sizeof(U)>>(
    stored, [&]<typename S>() IRS_FORCE_INLINE -> size_t {
      if constexpr (sizeof(S) < sizeof(U)) {
        return UnshuffleWidenAddAs<U, S>(in, n, base, out);
      } else {
        return 0;
      }
    });
  for (size_t i = done; i < n; ++i) {
    out[i] =
      static_cast<U>(static_cast<U>(ShuffledItem(in, n, stored, i)) + base);
  }
}

#ifdef __AVX2__
struct LanePacks {
  alignas(32) uint32_t narrow[256][8];
  alignas(32) uint32_t wide[16][8];
};

consteval LanePacks MakeLanePacks() {
  LanePacks packs{};
  for (uint32_t mask = 0; mask < 256; ++mask) {
    uint32_t out = 0;
    for (uint32_t lane = 0; lane < 8; ++lane) {
      if ((mask >> lane & 1) == 0) {
        continue;
      }
      packs.narrow[mask][out] = lane;
      if (mask < 16) {
        packs.wide[mask][2 * out] = 2 * lane;
        packs.wide[mask][2 * out + 1] = 2 * lane + 1;
      }
      ++out;
    }
  }
  return packs;
}

inline constexpr LanePacks kLanePacks = MakeLanePacks();

template<typename U>
size_t LocateChangesLanes(const U* values, size_t n, uint32_t first,
                          U* out_values, uint32_t* out_rows,
                          uint32_t& run) noexcept {
  static_assert(sizeof(U) == 4 || sizeof(U) == 8);
  constexpr uint32_t kLanes = 32 / sizeof(U);
  auto rows = _mm256_add_epi32(_mm256_set1_epi32(static_cast<int32_t>(first)),
                               _mm256_setr_epi32(1, 2, 3, 4, 5, 6, 7, 8));
  const auto step = _mm256_set1_epi32(static_cast<int32_t>(kLanes));
  size_t i = 1;
  for (; i + kLanes <= n; i += kLanes) {
    const auto now =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(values + i));
    const auto before =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(values + i - 1));
    uint32_t mask = 0;
    const uint32_t* lanes = nullptr;
    if constexpr (sizeof(U) == 8) {
      mask = ~static_cast<uint32_t>(_mm256_movemask_pd(
               _mm256_castsi256_pd(_mm256_cmpeq_epi64(now, before)))) &
             0xF;
      lanes = kLanePacks.wide[mask];
    } else {
      mask = ~static_cast<uint32_t>(_mm256_movemask_ps(
               _mm256_castsi256_ps(_mm256_cmpeq_epi32(now, before)))) &
             0xFF;
      lanes = kLanePacks.narrow[mask];
    }
    _mm256_storeu_si256(
      reinterpret_cast<__m256i*>(out_values + run),
      _mm256_permutevar8x32_epi32(
        now, _mm256_load_si256(reinterpret_cast<const __m256i*>(lanes))));
    const auto packed = _mm256_permutevar8x32_epi32(
      rows, _mm256_load_si256(
              reinterpret_cast<const __m256i*>(kLanePacks.narrow[mask])));
    if constexpr (sizeof(U) == 8) {
      _mm_storeu_si128(reinterpret_cast<__m128i*>(out_rows + run),
                       _mm256_castsi256_si128(packed));
    } else {
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(out_rows + run), packed);
    }
    rows = _mm256_add_epi32(rows, step);
    run += static_cast<uint32_t>(std::popcount(mask));
  }
  return i;
}

#endif

template<typename U>
using Lanes = U __attribute__((vector_size(32)));

template<typename U>
inline constexpr size_t kLaneCount = sizeof(Lanes<U>) / sizeof(U);

template<typename U, typename S>
IRS_FORCE_INLINE Lanes<U> WidenLanes(const uint8_t* in) noexcept {
  using Stored = S __attribute__((vector_size(kLaneCount<U> * sizeof(S))));
  Stored stored;
  std::memcpy(&stored, in, sizeof(stored));
  return __builtin_convertvector(stored, Lanes<U>);
}

IRS_FORCE_INLINE inline Lanes<uint32_t> PrefixSums(Lanes<uint32_t> x) noexcept {
  const Lanes<uint32_t> zero{};
  x += __builtin_shufflevector(zero, x, 0, 8, 9, 10, 0, 12, 13, 14);
  x += __builtin_shufflevector(zero, x, 0, 0, 8, 9, 0, 0, 12, 13);
  return x + __builtin_shufflevector(zero, x, 0, 0, 0, 0, 11, 11, 11, 11);
}

IRS_FORCE_INLINE inline Lanes<uint64_t> PrefixSums(Lanes<uint64_t> x) noexcept {
  const Lanes<uint64_t> zero{};
  x += __builtin_shufflevector(zero, x, 0, 4, 0, 6);
  return x + __builtin_shufflevector(zero, x, 0, 0, 5, 5);
}

template<typename S, typename U>
size_t WidenPrefixSumLanes(const uint8_t* in, size_t n, U& acc, U offset,
                           U* out) noexcept {
  Lanes<U> carry = Lanes<U>{} + acc;
  size_t i = 0;
  for (; i + kLaneCount<U> <= n; i += kLaneCount<U>) {
    const auto x = PrefixSums(WidenLanes<U, S>(in + i * sizeof(S)) + offset);
    const Lanes<U> sums = x + carry;
    std::memcpy(out + i, &sums, sizeof(sums));
    carry += x[kLaneCount<U> - 1];
  }
  acc = carry[0];
  return i;
}

template<typename U, typename S, bool kWrite>
U WidenPrefixSumAs(const uint8_t* in, size_t n, U acc, U offset,
                   U* out) noexcept {
  size_t i = 0;
  if constexpr (kWrite && sizeof(U) >= sizeof(uint32_t)) {
    i = WidenPrefixSumLanes<S>(in, n, acc, offset, out);
  }
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

inline constexpr uint64_t kBrokenRuns = std::numeric_limits<uint64_t>::max();

struct RunEnds {
  uint64_t total = 0;
  uint32_t shortest = 0;
};

template<typename S>
size_t PrefixRunEndsLanes(const uint8_t* in, size_t n, uint32_t* out,
                          uint32_t& acc, uint32_t& shortest,
                          uint32_t& longest) noexcept {
  using Lengths = Lanes<uint32_t>;
  Lengths carry{};
  Lengths low = ~Lengths{};
  Lengths high{};
  size_t i = 0;
  for (; i + kLaneCount<uint32_t> <= n; i += kLaneCount<uint32_t>) {
    const auto length = WidenLanes<uint32_t, S>(in + i * sizeof(S));
    low = __builtin_elementwise_min(low, length);
    high = __builtin_elementwise_max(high, length);
    const auto x = PrefixSums(length);
    const Lengths ends = x + carry;
    std::memcpy(out + i, &ends, sizeof(ends));
    carry += x[kLaneCount<uint32_t> - 1];
  }
  shortest = __builtin_reduce_min(low);
  longest = __builtin_reduce_max(high);
  acc = carry[0];
  return i;
}

template<typename S>
RunEnds PrefixRunEndsAs(const uint8_t* in, size_t n, uint32_t* out) noexcept {
  uint32_t acc = 0;
  uint32_t shortest = std::numeric_limits<uint32_t>::max();
  uint32_t longest = 0;
  size_t i = PrefixRunEndsLanes<S>(in, n, out, acc, shortest, longest);
  for (; i < n; ++i) {
    const auto length =
      static_cast<uint32_t>(duckdb::Load<S>(in + i * sizeof(S)));
    shortest = std::min(shortest, length);
    longest = std::max(longest, length);
    acc += length;
    out[i] = acc;
  }
  if (n == 0) {
    return {};
  }
  if (uint64_t{longest} * n > std::numeric_limits<uint32_t>::max()) {
    for (size_t k = 1; k < n; ++k) {
      if (out[k] <= out[k - 1]) {
        return {.total = kBrokenRuns, .shortest = shortest};
      }
    }
  }
  return {.total = acc, .shortest = shortest};
}

inline RunEnds PrefixRunEnds(const uint8_t* in, size_t n, uint8_t width,
                             uint32_t* out) noexcept {
  return Resolve<StoredAxis<sizeof(uint32_t)>>(
    width, [&]<typename S>()
             IRS_FORCE_INLINE { return PrefixRunEndsAs<S>(in, n, out); });
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
  SDB_ASSERT(count == 0 || row + count <= ends.back());
  constexpr uint32_t kMarks = 4;
  uint8_t ended[STANDARD_VECTOR_SIZE + 1];
  while (count != 0) {
    const auto rows =
      static_cast<uint32_t>(std::min<uint64_t>(count, STANDARD_VECTOR_SIZE));
    const auto stop = row + rows;
    std::memset(ended, 0, rows);
    auto next = run;
    for (; next + kMarks <= ends.size() && ends[next + kMarks - 1] < stop;
         next += kMarks) {
      for (uint32_t k = 0; k < kMarks; ++k) {
        ended[ends[next + k] - row - 1] = 0xFF;
      }
    }
    for (uint32_t k = 1; k < kMarks; ++k) {
      const auto end = ends[next];
      const bool inside = end < stop;
      ended[inside ? end - row - 1 : rows] = 0xFF;
      next += inside;
    }
    ExpandMarkedRuns(ended, values + run, rows, out);
    run = next + (ends[next] == stop);
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
