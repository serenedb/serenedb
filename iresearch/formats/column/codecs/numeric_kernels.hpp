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

#include <emmintrin.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <utility>

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

template<typename U, typename S>
void NarrowAs(const U* in, size_t n, uint8_t* out) noexcept {
  for (size_t i = 0; i < n; ++i) {
    const auto s = static_cast<S>(in[i]);
    std::memcpy(out + i * sizeof(S), &s, sizeof(S));
  }
}

template<typename U>
void Narrow(const U* in, size_t n, uint8_t stored, uint8_t* out) noexcept {
  switch (stored) {
    case 1:
      return NarrowAs<U, uint8_t>(in, n, out);
    case 2:
      return NarrowAs<U, uint16_t>(in, n, out);
    case 4:
      return NarrowAs<U, uint32_t>(in, n, out);
    default:
      return NarrowAs<U, uint64_t>(in, n, out);
  }
}

template<typename U, typename S>
void WidenAs(const uint8_t* in, size_t n, U* out) noexcept {
  for (size_t i = 0; i < n; ++i) {
    S s;
    std::memcpy(&s, in + i * sizeof(S), sizeof(S));
    out[i] = static_cast<U>(s);
  }
}

template<typename U>
void Widen(const uint8_t* in, size_t n, uint8_t stored, U* out) noexcept {
  switch (stored) {
    case 1:
      return WidenAs<U, uint8_t>(in, n, out);
    case 2:
      return WidenAs<U, uint16_t>(in, n, out);
    case 4:
      return WidenAs<U, uint32_t>(in, n, out);
    default:
      return WidenAs<U, uint64_t>(in, n, out);
  }
}

inline void Shuffle(const uint8_t* in, size_t n, uint8_t stored,
                    uint8_t* out) noexcept {
  for (size_t i = 0; i < n; ++i) {
    for (uint8_t b = 0; b < stored; ++b) {
      out[b * n + i] = in[i * stored + b];
    }
  }
}

inline __m128i Lane(const uint8_t* in, size_t n, size_t b, size_t i) noexcept {
  return _mm_loadu_si128(reinterpret_cast<const __m128i*>(in + b * n + i));
}

inline void Put(uint8_t* out, size_t at, __m128i v) noexcept {
  _mm_storeu_si128(reinterpret_cast<__m128i*>(out + at), v);
}

inline size_t Unshuffle2(const uint8_t* in, size_t n, uint8_t* out) noexcept {
  size_t i = 0;
  for (; i + 16 <= n; i += 16) {
    const auto a = Lane(in, n, 0, i);
    const auto b = Lane(in, n, 1, i);
    Put(out, 2 * i, _mm_unpacklo_epi8(a, b));
    Put(out, 2 * i + 16, _mm_unpackhi_epi8(a, b));
  }
  return i;
}

inline size_t Unshuffle4(const uint8_t* in, size_t n, uint8_t* out) noexcept {
  size_t i = 0;
  for (; i + 16 <= n; i += 16) {
    const auto ab_lo = _mm_unpacklo_epi8(Lane(in, n, 0, i), Lane(in, n, 1, i));
    const auto ab_hi = _mm_unpackhi_epi8(Lane(in, n, 0, i), Lane(in, n, 1, i));
    const auto cd_lo = _mm_unpacklo_epi8(Lane(in, n, 2, i), Lane(in, n, 3, i));
    const auto cd_hi = _mm_unpackhi_epi8(Lane(in, n, 2, i), Lane(in, n, 3, i));
    auto* row = out + 4 * i;
    Put(row, 0, _mm_unpacklo_epi16(ab_lo, cd_lo));
    Put(row, 16, _mm_unpackhi_epi16(ab_lo, cd_lo));
    Put(row, 32, _mm_unpacklo_epi16(ab_hi, cd_hi));
    Put(row, 48, _mm_unpackhi_epi16(ab_hi, cd_hi));
  }
  return i;
}

inline size_t Unshuffle8(const uint8_t* in, size_t n, uint8_t* out) noexcept {
  size_t i = 0;
  for (; i + 16 <= n; i += 16) {
    __m128i lo[4];
    __m128i hi[4];
    for (size_t p = 0; p < 4; ++p) {
      const auto a = Lane(in, n, 2 * p, i);
      const auto b = Lane(in, n, 2 * p + 1, i);
      lo[p] = _mm_unpacklo_epi8(a, b);
      hi[p] = _mm_unpackhi_epi8(a, b);
    }
    const __m128i quads[8] = {
      _mm_unpacklo_epi16(lo[0], lo[1]), _mm_unpackhi_epi16(lo[0], lo[1]),
      _mm_unpacklo_epi16(hi[0], hi[1]), _mm_unpackhi_epi16(hi[0], hi[1]),
      _mm_unpacklo_epi16(lo[2], lo[3]), _mm_unpackhi_epi16(lo[2], lo[3]),
      _mm_unpacklo_epi16(hi[2], hi[3]), _mm_unpackhi_epi16(hi[2], hi[3]),
    };
    auto* row = out + 8 * i;
    for (size_t q = 0; q < 4; ++q) {
      Put(row, 32 * q, _mm_unpacklo_epi32(quads[q], quads[q + 4]));
      Put(row, 32 * q + 16, _mm_unpackhi_epi32(quads[q], quads[q + 4]));
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
    default:
      break;
  }
  for (uint8_t b = 0; b < stored; ++b) {
    const auto* lane = in + b * n;
    for (size_t i = done; i < n; ++i) {
      out[i * stored + b] = lane[i];
    }
  }
}

template<typename U>
void AddBase(U* v, size_t n, U base) noexcept {
  for (size_t i = 0; i < n; ++i) {
    v[i] = static_cast<U>(v[i] + base);
  }
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
void DivideExact(U* v, size_t n, U divisor) noexcept {
  using S = std::make_signed_t<U>;
  using W = WideWord<U>;
  const auto shift = std::countr_zero(divisor);
  const W inverse = OddInverse(static_cast<U>(divisor >> shift));
  for (size_t i = 0; i < n; ++i) {
    U x;
    if constexpr (kSigned) {
      x = static_cast<U>(static_cast<S>(v[i]) >> shift);
    } else {
      x = static_cast<U>(v[i] >> shift);
    }
    v[i] = static_cast<U>(static_cast<W>(x) * inverse);
  }
}

template<typename U>
void MultiplyBy(U* v, size_t n, U factor) noexcept {
  using W = WideWord<U>;
  for (size_t i = 0; i < n; ++i) {
    v[i] = static_cast<U>(static_cast<W>(v[i]) * static_cast<W>(factor));
  }
}

template<typename U>
void PrefixSum(U* v, size_t n, U base, U offset) noexcept {
  U acc = base;
  for (size_t i = 0; i < n; ++i) {
    acc = static_cast<U>(acc + v[i] + offset);
    v[i] = acc;
  }
}

inline constexpr size_t kBlockValues = 1024;

template<typename U>
using LaneWord = std::conditional_t<sizeof(U) == 8, uint64_t, uint32_t>;

template<typename U>
inline constexpr unsigned kLanes = 1024 / (8 * sizeof(LaneWord<U>));

template<typename U>
inline constexpr unsigned kMaxBits = 8 * sizeof(U);

template<typename U>
constexpr unsigned BitsFor(U range) noexcept {
  unsigned bits = 0;
  while (bits < kMaxBits<U> && (range >> bits) != 0) {
    ++bits;
  }
  return bits;
}

constexpr size_t PackedBytes(unsigned bits) noexcept {
  return kBlockValues / 8 * bits;
}

template<typename U, unsigned W, unsigned J>
IRS_FORCE_INLINE void PackStep(const U* IRS_RESTRICT in,
                               LaneWord<U>* IRS_RESTRICT out, unsigned l,
                               LaneWord<U>& acc) noexcept {
  using Word = LaneWord<U>;
  constexpr unsigned kWordBits = 8 * sizeof(Word);
  constexpr unsigned kBit = J * W;
  constexpr unsigned kShift = kBit % kWordBits;
  const auto v = static_cast<Word>(in[J * kLanes<U> + l]);
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
                               LaneWord<U>* IRS_RESTRICT out, unsigned l,
                               std::integer_sequence<unsigned, J...>) noexcept {
  LaneWord<U> acc = 0;
  (PackStep<U, W, J>(in, out, l, acc), ...);
}

template<typename U, unsigned W>
void PackBlock(const U* IRS_RESTRICT in,
               LaneWord<U>* IRS_RESTRICT out) noexcept {
  if constexpr (W != 0) {
    constexpr unsigned kRows = 8 * sizeof(LaneWord<U>);
    for (unsigned l = 0; l < kLanes<U>; ++l) {
      PackLane<U, W>(in, out, l, std::make_integer_sequence<unsigned, kRows>{});
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
  constexpr Word kMask =
    W == kWordBits ? ~Word{0} : static_cast<Word>((Word{1} << W) - 1);
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

template<typename U, unsigned W>
void UnpackBlock(const LaneWord<U>* IRS_RESTRICT in, U* IRS_RESTRICT out,
                 U base) noexcept {
  if constexpr (W == 0) {
    std::fill_n(out, kBlockValues, base);
  } else {
    constexpr unsigned kRows = 8 * sizeof(LaneWord<U>);
    for (unsigned l = 0; l < kLanes<U>; ++l) {
      UnpackLane<U, W>(in, out, base, l,
                       std::make_integer_sequence<unsigned, kRows>{});
    }
  }
}

template<typename U>
using PackFn = void (*)(const U*, LaneWord<U>*) noexcept;

template<typename U>
using UnpackFn = void (*)(const LaneWord<U>*, U*, U) noexcept;

template<typename U>
inline constexpr auto kPack =
  []<unsigned... W>(std::integer_sequence<unsigned, W...>) {
    return std::array<PackFn<U>, sizeof...(W)>{&PackBlock<U, W>...};
  }(std::make_integer_sequence<unsigned, kMaxBits<U> + 1>{});

template<typename U>
inline constexpr auto kUnpack =
  []<unsigned... W>(std::integer_sequence<unsigned, W...>) {
    return std::array<UnpackFn<U>, sizeof...(W)>{&UnpackBlock<U, W>...};
  }(std::make_integer_sequence<unsigned, kMaxBits<U> + 1>{});

}  // namespace irs::codecs::numeric
