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

#include <absl/base/internal/endian.h>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <utility>

#include "iresearch/types.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::block_codec {

inline constexpr uint32_t kRows = 32;
inline constexpr uint32_t kLanes = 4;
inline constexpr uint32_t kWideLanes = 8;
inline constexpr uint32_t kMaxWidth = 31;

template<uint32_t L>
inline constexpr uint32_t kBlockOf = kRows * L;

template<uint32_t L>
inline constexpr uint32_t kSlotBitsOf = std::bit_width(kBlockOf<L> - 1);

inline constexpr uint32_t kBlock = kBlockOf<kLanes>;
inline constexpr uint32_t kWideBlock = kBlockOf<kWideLanes>;

using U32x4 = uint32_t __attribute__((vector_size(16)));
using I32x8 = int32_t __attribute__((vector_size(32)));

static_assert(std::endian::native == std::endian::little,
              "vector loads and stores read the on-disk words natively");

template<uint32_t B>
consteval uint32_t LowMask() {
  static_assert(B <= kMaxWidth);
  return (uint32_t{1} << B) - 1;
}

template<uint32_t L = kLanes>
IRS_FORCE_INLINE constexpr uint32_t PackedSize(uint32_t len,
                                               uint32_t bits) noexcept {
  return len == kBlockOf<L> ? 4 * L * bits : (len * bits + 7) / 8;
}

template<uint32_t L>
using LaneVector = uint32_t __attribute__((vector_size(L * sizeof(uint32_t))));

template<typename Vector>
IRS_FORCE_INLINE Vector Opaque(Vector v) noexcept {
#if defined(__x86_64__)
  asm("" : "+x"(v));
#elif defined(__aarch64__)
  asm("" : "+w"(v));
#endif
  return v;
}

template<uint32_t B, uint32_t L, bool Mask, uint32_t S>
IRS_FORCE_INLINE void PackLanes(const uint32_t* IRS_RESTRICT in,
                                byte_type* IRS_RESTRICT out,
                                LaneVector<L>& word,
                                LaneVector<L> mask) noexcept {
  constexpr uint32_t kBit = S * B;
  constexpr uint32_t kShift = kBit % 32;
  LaneVector<L> v;
  std::memcpy(&v, in + S * L, sizeof(v));
  if constexpr (Mask) {
    v &= mask;
  }
  if constexpr (kShift == 0) {
    word = v;
  } else {
    word |= v << kShift;
  }
  if constexpr (kShift + B >= 32) {
    std::memcpy(out + kBit / 32 * sizeof(word), &word, sizeof(word));
    if constexpr (kShift + B > 32) {
      word = v >> (32 - kShift);
    }
  }
}

template<uint32_t B, uint32_t L = kLanes, bool Mask = true>
IRS_FORCE_INLINE void PackVertical(const uint32_t* IRS_RESTRICT in,
                                   byte_type* IRS_RESTRICT out) noexcept {
  static_assert(B <= kMaxWidth);
  if constexpr (B != 0) {
    LaneVector<L> word{};
    const auto mask = Opaque(LaneVector<L>{} + LowMask<B>());
    [&]<uint32_t... S>(std::integer_sequence<uint32_t, S...>) IRS_FORCE_INLINE {
      (PackLanes<B, L, Mask, S>(in, out, word, mask), ...);
    }(std::make_integer_sequence<uint32_t, kRows>{});
  }
}

template<uint32_t B, uint32_t Add, uint32_t S, uint32_t L = kLanes>
IRS_FORCE_INLINE void UnpackRow(const uint32_t* IRS_RESTRICT words,
                                uint32_t* IRS_RESTRICT out) noexcept {
  constexpr uint32_t kBit = S * B;
  constexpr uint32_t kWord = kBit / 32;
  constexpr uint32_t kShift = kBit % 32;
  for (uint32_t lane = 0; lane != L; ++lane) {
    uint32_t v = words[kWord * L + lane] >> kShift;
    if constexpr (kShift + B > 32) {
      v |= words[(kWord + 1) * L + lane] << (32 - kShift);
    }
    out[S * L + lane] = (v & LowMask<B>()) + Add;
  }
}

template<uint32_t B, uint32_t Add, uint32_t L = kLanes>
IRS_FORCE_INLINE void UnpackVertical(const byte_type* IRS_RESTRICT in,
                                     uint32_t* IRS_RESTRICT out) noexcept {
  static_assert(B <= kMaxWidth);
  if constexpr (B == 0) {
    std::fill_n(out, kRows * L, Add);
  } else {
    uint32_t words[L * B];
    std::memcpy(words, in, sizeof(words));
    [&]<uint32_t... S>(std::integer_sequence<uint32_t, S...>) IRS_FORCE_INLINE {
      (UnpackRow<B, Add, S, L>(words, out), ...);
    }(std::make_integer_sequence<uint32_t, kRows>{});
  }
}

using U32x8 = uint32_t __attribute__((vector_size(32)));
using U64x4 = uint64_t __attribute__((vector_size(32)));

inline constexpr uint32_t kGroup = 8;
inline constexpr uint32_t kMaxPackGroupBits = 16;

template<uint32_t B>
IRS_FORCE_INLINE void PackGroup(const uint32_t* IRS_RESTRICT in,
                                byte_type* IRS_RESTRICT out) noexcept {
  static_assert(0 < B && B <= kMaxPackGroupBits);
  U32x8 v;
  std::memcpy(&v, in, sizeof(v));
  const auto x = std::bit_cast<U64x4>(v & LowMask<B>());
  const U64x4 pairs = ((x >> 32) << B) | (x & 0xFFFFFFFF);
  const U64x4 shifted = pairs << U64x4{0, 2 * B, 0, 2 * B};
  const uint64_t low = shifted[0] | shifted[1];
  const uint64_t high = shifted[2] | shifted[3];
  if constexpr (B == kMaxPackGroupBits) {
    std::memcpy(out, &low, sizeof(low));
    std::memcpy(out + sizeof(low), &high, sizeof(high));
  } else if constexpr (B > 8) {
    const uint64_t first = low | (high << (4 * B));
    const uint64_t rest = high >> (64 - 4 * B);
    std::memcpy(out, &first, sizeof(first));
    std::memcpy(out + sizeof(first), &rest, B - sizeof(first));
  } else {
    const uint64_t all = low | (high << (4 * B));
    std::memcpy(out, &all, B);
  }
}

template<uint32_t B>
void PackHorizontal(const uint32_t* IRS_RESTRICT in, uint32_t len,
                    byte_type* IRS_RESTRICT out) noexcept {
  static_assert(B <= kMaxWidth);
  if constexpr (B != 0) {
    uint32_t i = 0;
    if constexpr (B <= kMaxPackGroupBits) {
      for (; i + kGroup <= len; i += kGroup, out += B) {
        PackGroup<B>(in + i, out);
      }
    }
    uint64_t acc = 0;
    uint32_t bits = 0;
    for (; i != len; ++i) {
      acc |= uint64_t{in[i] & LowMask<B>()} << bits;
      bits += B;
      if (bits >= 32) {
        absl::little_endian::Store32(out, static_cast<uint32_t>(acc));
        out += sizeof(uint32_t);
        acc >>= 32;
        bits -= 32;
      }
    }
    for (; bits > 0; bits = bits > 8 ? bits - 8 : 0) {
      *out++ = static_cast<byte_type>(acc);
      acc >>= 8;
    }
  }
}

template<bool Wide>
IRS_FORCE_INLINE void PackGroupBits(uint32_t bits, U32x8 values,
                                    byte_type* IRS_RESTRICT out) noexcept {
  const auto x = std::bit_cast<U64x4>(values);
  const uint64_t pair = 2 * bits;
  const U64x4 pairs = ((x >> 32) << bits) | (x & 0xFFFFFFFF);
  const U64x4 shifted = pairs << U64x4{0, pair, 0, pair};
  const uint64_t low = shifted[0] | shifted[1];
  const uint64_t high = shifted[2] | shifted[3];
  if constexpr (Wide) {
    const U64x4 spill = pairs >> (64 - pair);
    const uint64_t shift = 2 * pair - 64;
    const uint64_t words[] = {low, spill[1] | (high << shift),
                              (high >> (64 - shift)) | (spill[3] << shift),
                              spill[3] >> (64 - shift)};
    std::memcpy(out, words, sizeof(words));
  } else {
    const uint64_t words[] = {low | ((high << (2 * pair - 1)) << 1),
                              high >> (64 - 2 * pair)};
    std::memcpy(out, words, sizeof(words));
  }
}

template<uint32_t B, uint32_t K>
IRS_FORCE_INLINE uint32_t ExtractGroup(const byte_type* in) noexcept {
  constexpr uint32_t kBit = K * B;
  return static_cast<uint32_t>(absl::little_endian::Load64(in + kBit / 8) >>
                               (kBit % 8)) &
         LowMask<B>();
}

using U8x16 = uint8_t __attribute__((vector_size(16)));
using U8x32 = uint8_t __attribute__((vector_size(32)));

inline constexpr uint32_t kMaxGroupBits = 25;

template<uint32_t B>
consteval uint32_t GroupHighByte() {
  return B <= 16 ? 0 : B / 2;
}

template<uint32_t B>
consteval int GroupByte(uint32_t j) {
  const uint32_t k = j / 4;
  const uint32_t first = k * B / 8;
  const uint32_t last = (k * B + B - 1) / 8;
  if (first + j % 4 > last) {
    return -1;
  }
  return static_cast<int>(k < 4 ? first + j % 4
                                : 16 + first + j % 4 - GroupHighByte<B>());
}

template<uint32_t B>
IRS_FORCE_INLINE U32x8 UnpackGroup(const byte_type* IRS_RESTRICT in) noexcept {
  static_assert(0 < B && B <= kMaxGroupBits);
  U8x16 lo;
  U8x16 hi;
  std::memcpy(&lo, in, sizeof(lo));
  std::memcpy(&hi, in + GroupHighByte<B>(), sizeof(hi));
  const U8x32 bytes = __builtin_shufflevector(
    lo, hi, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
    19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31);
  const U8x32 gathered =
    [&]<uint32_t... J>(std::integer_sequence<uint32_t, J...>) IRS_FORCE_INLINE {
      return __builtin_shufflevector(bytes, bytes, GroupByte<B>(J)...);
    }(std::make_integer_sequence<uint32_t, 32>{});
  U32x8 v;
  std::memcpy(&v, &gathered, sizeof(v));
  constexpr U32x8 kShifts = {0,         B % 8,     2 * B % 8, 3 * B % 8,
                             4 * B % 8, 5 * B % 8, 6 * B % 8, 7 * B % 8};
  return (v >> kShifts) & LowMask<B>();
}

template<uint32_t B>
IRS_FORCE_INLINE uint32_t VectorGroups(uint32_t len) noexcept {
  return B <= kMaxPackGroupBits ? (len + kGroup - 1) / kGroup : len / kGroup;
}

template<uint32_t B, uint32_t Add>
IRS_FORCE_INLINE void UnpackRest(const byte_type* IRS_RESTRICT in,
                                 uint32_t from, uint32_t len,
                                 uint32_t* IRS_RESTRICT out) noexcept {
  for (uint32_t i = from, bit = from * B; i != len; ++i, bit += B) {
    out[i] = (static_cast<uint32_t>(absl::little_endian::Load64(in + bit / 8) >>
                                    (bit % 8)) &
              LowMask<B>()) +
             Add;
  }
}

template<uint32_t B, uint32_t Add>
void UnpackHorizontal(const byte_type* IRS_RESTRICT in, uint32_t len,
                      uint32_t* IRS_RESTRICT out) noexcept {
  static_assert(B <= kMaxWidth);
  if constexpr (B == 0) {
    std::fill_n(out, len, Add);
  } else if constexpr (B <= kMaxGroupBits) {
    const uint32_t groups = VectorGroups<B>(len);
    for (uint32_t g = 0; g != groups; ++g) {
      const U32x8 v = UnpackGroup<B>(in + g * B) + Add;
      std::memcpy(out + g * kGroup, &v, sizeof(v));
    }
    if constexpr (B > kMaxPackGroupBits) {
      UnpackRest<B, Add>(in, groups * kGroup, len, out);
    }
  } else {
    const uint32_t groups = len / kGroup;
    for (uint32_t g = 0; g != groups; ++g) {
      [&]<uint32_t... K>(std::integer_sequence<uint32_t, K...>)
        IRS_FORCE_INLINE {
          ((out[g * kGroup + K] = ExtractGroup<B, K>(in + g * B) + Add), ...);
        }(std::make_integer_sequence<uint32_t, kGroup>{});
    }
    UnpackRest<B, Add>(in, groups * kGroup, len, out);
  }
}

inline constexpr U32x8 kSteps = {1, 2, 3, 4, 5, 6, 7, 8};

inline IRS_FORCE_INLINE U32x8 SpreadLane(U32x8 v, uint32_t lane) noexcept {
#ifdef __AVX2__
  const U32x8 index = Opaque(U32x8{} + lane);
  return std::bit_cast<U32x8>(_mm256_permutevar8x32_epi32(
    std::bit_cast<__m256i>(v), std::bit_cast<__m256i>(index)));
#else
  return U32x8{} + v[lane];
#endif
}

inline IRS_FORCE_INLINE U32x8 InclusiveScan(U32x8 v) noexcept {
  const U32x8 zero = {0, 0, 0, 0, 0, 0, 0, 0};
  v += __builtin_shufflevector(zero, v, 0, 8, 9, 10, 0, 12, 13, 14);
  v += __builtin_shufflevector(zero, v, 0, 0, 8, 9, 0, 0, 12, 13);
  v += SpreadLane(v, 3) & U32x8{0, 0, 0, 0, ~0U, ~0U, ~0U, ~0U};
  return v;
}

inline IRS_FORCE_INLINE U32x8 BroadcastLast(U32x8 v) noexcept {
  return SpreadLane(v, 7);
}

inline IRS_FORCE_INLINE void ScanDocs(doc_id_t* IRS_RESTRICT docs, uint32_t len,
                                      doc_id_t prev) noexcept {
  U32x8 carry = U32x8{} + prev;
  uint32_t i = 0;
  for (; i + kWideLanes <= len; i += kWideLanes) {
    U32x8 v;
    std::memcpy(&v, docs + i, sizeof(v));
    v = InclusiveScan(v) + kSteps;
    const U32x8 out = v + carry;
    std::memcpy(docs + i, &out, sizeof(out));
    carry += BroadcastLast(v);
  }
  prev = carry[0];
  for (; i != len; ++i) {
    prev += docs[i] + 1;
    docs[i] = prev;
  }
}

template<uint32_t B>
void UnpackHorizontalDelta(const byte_type* IRS_RESTRICT in, uint32_t len,
                           doc_id_t prev, doc_id_t* IRS_RESTRICT out) noexcept {
  if constexpr (0 < B && B <= kMaxGroupBits) {
    U32x8 carry = U32x8{} + prev;
    const uint32_t groups = VectorGroups<B>(len);
    for (uint32_t g = 0; g != groups; ++g) {
      const U32x8 v = InclusiveScan(UnpackGroup<B>(in + g * B)) + kSteps;
      const U32x8 docs = v + carry;
      std::memcpy(out + g * kGroup, &docs, sizeof(docs));
      carry += BroadcastLast(v);
    }
    if constexpr (B > kMaxPackGroupBits) {
      const uint32_t from = groups * kGroup;
      UnpackRest<B, 0>(in, from, len, out);
      prev = carry[0];
      for (uint32_t i = from; i != len; ++i) {
        prev += out[i] + 1;
        out[i] = prev;
      }
    }
  } else {
    UnpackHorizontal<B, 0>(in, len, out);
    ScanDocs(out, len, prev);
  }
}

inline constexpr uint32_t kPatchGroup = 4;

template<uint32_t B, typename Entry, uint32_t L = kLanes>
IRS_FORCE_INLINE void PatchGroup(const byte_type* IRS_RESTRICT p,
                                 uint32_t count,
                                 uint32_t* IRS_RESTRICT out) noexcept {
  for (uint32_t k = 0; k != kPatchGroup; ++k) {
    const auto* entry = p + k * sizeof(Entry);
    uint32_t slot;
    uint32_t high;
    if constexpr (kSlotBitsOf<L> == 8) {
      slot = entry[0];
      if constexpr (sizeof(Entry) == sizeof(uint16_t)) {
        high = entry[1];
      } else {
        high = absl::little_endian::Load32(entry) >> 8;
      }
    } else {
      const uint32_t e = absl::little_endian::Load<Entry>(entry);
      slot = e & (kBlockOf<L> - 1);
      high = e >> kSlotBitsOf<L>;
    }
    out[slot] += k < count ? high << B : 0;
  }
}

inline IRS_FORCE_INLINE U32x4 WordRow(const uint32_t* words,
                                      uint32_t row) noexcept {
  U32x4 v;
  std::memcpy(&v, words + row * kLanes, sizeof(v));
  return v;
}

inline IRS_FORCE_INLINE U32x8 Concat(U32x4 lo, U32x4 hi) noexcept {
  return __builtin_shufflevector(lo, hi, 0, 1, 2, 3, 4, 5, 6, 7);
}

template<uint32_t B, uint32_t S>
IRS_FORCE_INLINE U32x8 UnpackPair(const uint32_t* words) noexcept {
  constexpr uint32_t kBit0 = S * B;
  constexpr uint32_t kBit1 = (S + 1) * B;
  constexpr uint32_t kShift0 = kBit0 % 32;
  constexpr uint32_t kShift1 = kBit1 % 32;
  constexpr bool kSpan0 = kShift0 + B > 32;
  constexpr bool kSpan1 = kShift1 + B > 32;
  U32x8 v = Concat(WordRow(words, kBit0 / 32), WordRow(words, kBit1 / 32)) >>
            U32x8{kShift0, kShift0, kShift0, kShift0,
                  kShift1, kShift1, kShift1, kShift1};
  if constexpr (kSpan0 || kSpan1) {
    constexpr uint32_t kBack0 = kSpan0 ? 32 - kShift0 : 0;
    constexpr uint32_t kBack1 = kSpan1 ? 32 - kShift1 : 0;
    const U32x4 next0 = kSpan0 ? WordRow(words, kBit0 / 32 + 1) : U32x4{};
    const U32x4 next1 = kSpan1 ? WordRow(words, kBit1 / 32 + 1) : U32x4{};
    v |= Concat(next0, next1) << U32x8{kBack0, kBack0, kBack0, kBack0,
                                       kBack1, kBack1, kBack1, kBack1};
  }
  return v & LowMask<B>();
}

inline IRS_FORCE_INLINE U32x8 LoadWide(const byte_type* in,
                                       uint32_t word) noexcept {
  U32x8 v;
  std::memcpy(&v, in + word * sizeof(U32x8), sizeof(v));
  return v;
}

template<uint32_t B, typename Row>
IRS_FORCE_INLINE void WideRows(const byte_type* IRS_RESTRICT in,
                               Row&& row) noexcept {
  static_assert(B <= kMaxWidth);
  if constexpr (B == 0) {
    [&]<uint32_t... S>(std::integer_sequence<uint32_t, S...>) IRS_FORCE_INLINE {
      (row.template operator()<S>(U32x8{}), ...);
    }(std::make_integer_sequence<uint32_t, kRows>{});
  } else {
    U32x8 word = LoadWide(in, 0);
    [&]<uint32_t... S>(std::integer_sequence<uint32_t, S...>) IRS_FORCE_INLINE {
      (([&] IRS_FORCE_INLINE {
         constexpr uint32_t kBit = S * B;
         constexpr uint32_t kShift = kBit % 32;
         U32x8 v = word >> kShift;
         if constexpr (kShift + B >= 32 && S + 1 != kRows) {
           word = LoadWide(in, kBit / 32 + 1);
         }
         if constexpr (kShift + B > 32) {
           v |= word << (32 - kShift);
         }
         if constexpr (kShift + B != 32) {
           v &= LowMask<B>();
         }
         row.template operator()<S>(v);
       }()),
       ...);
    }(std::make_integer_sequence<uint32_t, kRows>{});
  }
}

template<uint32_t B, uint32_t Add>
IRS_FORCE_INLINE void UnpackWide(const byte_type* IRS_RESTRICT in,
                                 uint32_t* IRS_RESTRICT out) noexcept {
  const U32x8 add = B == 0 ? Opaque(U32x8{} + Add) : U32x8{} + Add;
  WideRows<B>(in, [&]<uint32_t S>(U32x8 v) IRS_FORCE_INLINE {
    v += add;
    std::memcpy(out + S * kWideLanes, &v, sizeof(v));
  });
}

inline constexpr uint64_t kNoLane = 0xFF;

inline constexpr auto kExpandIndices = [] {
  std::array<uint64_t, 256> table{};
  for (uint32_t mask = 0; mask != table.size(); ++mask) {
    uint32_t rank = 0;
    for (uint32_t lane = 0; lane != kWideLanes; ++lane) {
      table[mask] |= ((mask >> lane) & 1 ? uint64_t{rank++} : kNoLane)
                     << (8 * lane);
    }
  }
  return table;
}();

inline constexpr auto kLeftPackIndices = [] {
  std::array<uint64_t, 256> table{};
  for (uint32_t mask = 0; mask != table.size(); ++mask) {
    uint32_t rank = 0;
    for (uint32_t lane = 0; lane != kWideLanes; ++lane) {
      if ((mask >> lane) & 1) {
        table[mask] |= uint64_t{lane} << (8 * rank++);
      }
    }
  }
  return table;
}();

inline IRS_FORCE_INLINE U32x8 LeftPack(U32x8 v, uint32_t mask) noexcept {
#ifdef __AVX2__
  const __m256i indices = _mm256_cvtepu8_epi32(
    _mm_cvtsi64_si128(static_cast<int64_t>(kLeftPackIndices[mask])));
  return std::bit_cast<U32x8>(
    _mm256_permutevar8x32_epi32(std::bit_cast<__m256i>(v), indices));
#else
  U32x8 packed{};
  for (uint32_t lane = 0; lane != kWideLanes; ++lane) {
    packed[lane] = v[(kLeftPackIndices[mask] >> (8 * lane)) & 0xFF];
  }
  return packed;
#endif
}

inline IRS_FORCE_INLINE U32x8 ExpandLanes(const uint32_t* IRS_RESTRICT values,
                                          uint32_t mask) noexcept {
  U32x8 v;
  std::memcpy(&v, values, sizeof(v));
#ifdef __AVX2__
  const __m256i indices = _mm256_cvtepi8_epi32(
    _mm_cvtsi64_si128(static_cast<int64_t>(kExpandIndices[mask])));
  return std::bit_cast<U32x8>(_mm256_and_si256(
    _mm256_permutevar8x32_epi32(std::bit_cast<__m256i>(v), indices),
    _mm256_cmpgt_epi32(indices, _mm256_set1_epi32(-1))));
#else
  U32x8 expanded{};
  for (uint32_t lane = 0; lane != kWideLanes; ++lane) {
    const auto index = (kExpandIndices[mask] >> (8 * lane)) & kNoLane;
    expanded[lane] = index < kWideLanes ? v[index] : 0;
  }
  return expanded;
#endif
}

template<uint32_t B, uint32_t L>
IRS_FORCE_INLINE void UnpackVerticalDelta(const byte_type* IRS_RESTRICT in,
                                          doc_id_t prev,
                                          doc_id_t* IRS_RESTRICT out) noexcept {
  static_assert(B <= kMaxWidth);
  constexpr uint32_t kChunk = sizeof(U32x8) / sizeof(uint32_t);
  U32x8 carry = U32x8{} + prev;
  const auto chunk = [&]<uint32_t C>(U32x8 gaps) IRS_FORCE_INLINE {
    const U32x8 v = InclusiveScan(gaps) + kSteps;
    const U32x8 docs = v + carry;
    std::memcpy(out + C * kChunk, &docs, sizeof(docs));
    carry += BroadcastLast(v);
  };
  if constexpr (L == kWideLanes) {
    WideRows<B>(in, chunk);
  } else {
    static_assert(L == kLanes);
    [&]<uint32_t... C>(std::integer_sequence<uint32_t, C...>) IRS_FORCE_INLINE {
      (chunk.template operator()<C>(
         B == 0 ? U32x8{}
                : UnpackPair<B, 2 * C>(reinterpret_cast<const uint32_t*>(in))),
       ...);
    }(std::make_integer_sequence<uint32_t, kBlockOf<L> / kChunk>{});
  }
}

using U32x16 = uint32_t __attribute__((vector_size(64)));

inline IRS_FORCE_INLINE void ScanRow16(U32x16& v, U32x16& carry,
                                       doc_id_t* out) noexcept {
  const U32x16 zero{};
  v += __builtin_shufflevector(zero, v, 0, 16, 17, 18, 19, 20, 21, 22, 23, 24,
                               25, 26, 27, 28, 29, 30);
  v += __builtin_shufflevector(zero, v, 0, 1, 16, 17, 18, 19, 20, 21, 22, 23,
                               24, 25, 26, 27, 28, 29);
  v += __builtin_shufflevector(zero, v, 0, 1, 2, 3, 16, 17, 18, 19, 20, 21, 22,
                               23, 24, 25, 26, 27);
  v += __builtin_shufflevector(zero, v, 0, 1, 2, 3, 4, 5, 6, 7, 16, 17, 18, 19,
                               20, 21, 22, 23);
  v += U32x16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  const U32x16 docs = v + carry;
  std::memcpy(out, &docs, sizeof(docs));
  carry += __builtin_shufflevector(v, v, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
                                   15, 15, 15, 15, 15, 15);
}

template<uint32_t B>
IRS_FORCE_INLINE void UnpackVerticalDelta16(
  const byte_type* IRS_RESTRICT in, doc_id_t prev,
  doc_id_t* IRS_RESTRICT out) noexcept {
  U32x16 carry = U32x16{} + prev;
  U32x8 low{};
  WideRows<B>(in, [&]<uint32_t S>(U32x8 v) IRS_FORCE_INLINE {
    if constexpr (S % 2 == 0) {
      low = v;
    } else {
      U32x16 gaps = __builtin_shufflevector(low, v, 0, 1, 2, 3, 4, 5, 6, 7, 8,
                                            9, 10, 11, 12, 13, 14, 15);
      ScanRow16(gaps, carry, out + (S - 1) * kWideLanes);
    }
  });
}

template<uint32_t B>
IRS_FORCE_INLINE void UnpackHorizontalDelta16(
  const byte_type* IRS_RESTRICT in, uint32_t len, doc_id_t prev,
  doc_id_t* IRS_RESTRICT out) noexcept {
  static_assert(0 < B && B <= kMaxPackGroupBits);
  U32x16 carry = U32x16{} + prev;
  uint32_t i = 0;
  for (; i + kGroup < len; i += 2 * kGroup) {
    U32x16 gaps = __builtin_shufflevector(
      UnpackGroup<B>(in + i * B / 8), UnpackGroup<B>(in + i * B / 8 + B), 0, 1,
      2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    ScanRow16(gaps, carry, out + i);
  }
  if (i < len) {
    const U32x8 v = InclusiveScan(UnpackGroup<B>(in + i * B / 8)) + kSteps;
    const U32x8 docs = v + U32x8{carry[0], carry[0], carry[0], carry[0],
                                 carry[0], carry[0], carry[0], carry[0]};
    std::memcpy(out + i, &docs, sizeof(docs));
  }
}

inline IRS_FORCE_INLINE void ScanDocs16(doc_id_t* docs,
                                        doc_id_t prev) noexcept {
  U32x16 carry = U32x16{} + prev;
  for (uint32_t i = 0; i != kWideBlock; i += 16) {
    U32x16 v;
    std::memcpy(&v, docs + i, sizeof(v));
    ScanRow16(v, carry, docs + i);
  }
}

template<uint32_t B, typename Entry, uint32_t L = kLanes>
IRS_FORCE_INLINE const byte_type* PatchValues(
  const byte_type* IRS_RESTRICT p, uint32_t count,
  uint32_t* IRS_RESTRICT out) noexcept {
  static_assert(B < kMaxWidth);
  PatchGroup<B, Entry, L>(p, count, out);
  for (uint32_t i = kPatchGroup; i < count; i += kPatchGroup) {
    PatchGroup<B, Entry, L>(p + i * sizeof(Entry), count - i, out);
  }
  return p + count * sizeof(Entry);
}

}  // namespace irs::block_codec
