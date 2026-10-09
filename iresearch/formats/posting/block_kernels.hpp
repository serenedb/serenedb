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
inline constexpr uint32_t kLanes = 8;
inline constexpr uint32_t kBlock = kRows * kLanes;
inline constexpr uint32_t kMaxWidth = 31;

using U32x8 = uint32_t __attribute__((vector_size(32)));
using I32x8 = int32_t __attribute__((vector_size(32)));
using U64x4 = uint64_t __attribute__((vector_size(32)));

static_assert(std::endian::native == std::endian::little,
              "vector loads and stores read the on-disk words natively");

template<uint32_t B>
consteval uint32_t LowMask() {
  static_assert(B <= kMaxWidth);
  return (uint32_t{1} << B) - 1;
}

IRS_FORCE_INLINE constexpr uint32_t PackedSize(uint32_t len,
                                               uint32_t bits) noexcept {
  return (len * bits + 7) / 8;
}

template<typename Vector>
IRS_FORCE_INLINE Vector Opaque(Vector v) noexcept {
#if defined(__x86_64__)
  asm("" : "+x"(v));
#elif defined(__aarch64__)
  asm("" : "+w"(v));
#endif
  return v;
}

template<uint32_t B, bool Mask, uint32_t S>
IRS_FORCE_INLINE void PackLanes(const uint32_t* IRS_RESTRICT in,
                                byte_type* IRS_RESTRICT out, U32x8& word,
                                U32x8 mask) noexcept {
  constexpr uint32_t kBit = S * B;
  constexpr uint32_t kShift = kBit % 32;
  U32x8 v;
  std::memcpy(&v, in + S * kLanes, sizeof(v));
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

template<uint32_t B, bool Mask = true>
IRS_FORCE_INLINE void PackVertical(const uint32_t* IRS_RESTRICT in,
                                   byte_type* IRS_RESTRICT out) noexcept {
  static_assert(B <= kMaxWidth);
  if constexpr (B != 0) {
    U32x8 word{};
    const auto mask = Opaque(U32x8{} + LowMask<B>());
    [&]<uint32_t... S>(std::integer_sequence<uint32_t, S...>) IRS_FORCE_INLINE {
      (PackLanes<B, Mask, S>(in, out, word, mask), ...);
    }(std::make_integer_sequence<uint32_t, kRows>{});
  }
}

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
  for (; i + kLanes <= len; i += kLanes) {
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

inline IRS_FORCE_INLINE U32x8 LoadRow(const byte_type* in,
                                      uint32_t word) noexcept {
  U32x8 v;
  std::memcpy(&v, in + word * sizeof(U32x8), sizeof(v));
  return v;
}

template<uint32_t B, typename Row>
IRS_FORCE_INLINE void VerticalRows(const byte_type* IRS_RESTRICT in,
                                   Row&& row) noexcept {
  static_assert(B <= kMaxWidth);
  if constexpr (B == 0) {
    [&]<uint32_t... S>(std::integer_sequence<uint32_t, S...>) IRS_FORCE_INLINE {
      (row.template operator()<S>(U32x8{}), ...);
    }(std::make_integer_sequence<uint32_t, kRows>{});
  } else {
    U32x8 word = LoadRow(in, 0);
    [&]<uint32_t... S>(std::integer_sequence<uint32_t, S...>) IRS_FORCE_INLINE {
      (([&] IRS_FORCE_INLINE {
         constexpr uint32_t kBit = S * B;
         constexpr uint32_t kShift = kBit % 32;
         U32x8 v = word >> kShift;
         if constexpr (kShift + B >= 32 && S + 1 != kRows) {
           word = LoadRow(in, kBit / 32 + 1);
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
IRS_FORCE_INLINE void UnpackVertical(const byte_type* IRS_RESTRICT in,
                                     uint32_t* IRS_RESTRICT out) noexcept {
  const U32x8 add = B == 0 ? Opaque(U32x8{} + Add) : U32x8{} + Add;
  VerticalRows<B>(in, [&]<uint32_t S>(U32x8 v) IRS_FORCE_INLINE {
    v += add;
    std::memcpy(out + S * kLanes, &v, sizeof(v));
  });
}

template<uint32_t B>
IRS_FORCE_INLINE void UnpackVerticalDelta(const byte_type* IRS_RESTRICT in,
                                          doc_id_t prev,
                                          doc_id_t* IRS_RESTRICT out) noexcept {
  U32x8 carry = U32x8{} + prev;
  VerticalRows<B>(in, [&]<uint32_t S>(U32x8 gaps) IRS_FORCE_INLINE {
    const U32x8 v = InclusiveScan(gaps) + kSteps;
    const U32x8 docs = v + carry;
    std::memcpy(out + S * kLanes, &docs, sizeof(docs));
    carry += BroadcastLast(v);
  });
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
  VerticalRows<B>(in, [&]<uint32_t S>(U32x8 v) IRS_FORCE_INLINE {
    if constexpr (S % 2 == 0) {
      low = v;
    } else {
      U32x16 gaps = __builtin_shufflevector(low, v, 0, 1, 2, 3, 4, 5, 6, 7, 8,
                                            9, 10, 11, 12, 13, 14, 15);
      ScanRow16(gaps, carry, out + (S - 1) * kLanes);
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
  for (uint32_t i = 0; i != kBlock; i += 16) {
    U32x16 v;
    std::memcpy(&v, docs + i, sizeof(v));
    ScanRow16(v, carry, docs + i);
  }
}

}  // namespace irs::block_codec
