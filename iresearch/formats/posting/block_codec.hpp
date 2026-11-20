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
#include <limits>
#include <type_traits>
#include <utility>

#include "iresearch/formats/posting/block_kernels.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/dispatch.hpp"
#include "iresearch/utils/system_compiler.hpp"

namespace irs::block_codec {

#ifdef __AVX2__
#define IRS_BLOCK_CODEC_AVX512 \
  __attribute__((target("avx512f,avx512vl,avx512bw,avx512dq")))
#define IRS_BLOCK_CODEC_VBMI2 \
  __attribute__((target("avx512f,avx512vl,avx512bw,avx512dq,avx512vbmi2")))
#else
#define IRS_BLOCK_CODEC_AVX512
#define IRS_BLOCK_CODEC_VBMI2
#endif

inline constexpr uint32_t kMaxBitsetWords = 32;

enum class DeltaEncoding : byte_type {
  Run = 0,
  Same08,
  Same16,
  Same32,
  Pack,
  PatchByte = Pack + kMaxWidth,
  PatchBit = PatchByte + kMaxWidth,
  BitsetWords = PatchBit + kMaxWidth,
  End = BitsetWords + kMaxBitsetWords,
};

enum class ValueEncoding : byte_type {
  Zero = 0,
  Same08,
  Same16,
  Same32,
  Pack,
  PatchByte = Pack + kMaxWidth,
  PatchBit = PatchByte + kMaxWidth,
  End = PatchBit + kMaxWidth,
};

inline constexpr uint32_t kInSlack = 16;
inline constexpr uint32_t kOutSlack = 16;

inline constexpr uint32_t kMaxBlockBytes = 1 + PackedSize(kBlock, kMaxWidth);

template<typename E>
constexpr uint32_t Code(E e) noexcept {
  return static_cast<uint32_t>(e);
}

constexpr bool IsTokenBitset(uint32_t token) noexcept {
  return token >= Code(DeltaEncoding::BitsetWords);
}

constexpr uint32_t TokenBitsetWords(uint32_t token) noexcept {
  return token - Code(DeltaEncoding::BitsetWords) + 1;
}

struct BitsetView {
  const byte_type* bits;
  uint32_t words;
};

inline IRS_FORCE_INLINE BitsetView ParseBitset(const byte_type* in) noexcept {
  return {in + 1, TokenBitsetWords(in[0])};
}

#define IRS_BLOCK_CODEC_CASE(V)                                   \
  case (V):                                                       \
    if constexpr ((V) < N) {                                      \
      return std::forward<Func>(func).template operator()<(V)>(); \
    }                                                             \
    break;
#define IRS_BLOCK_CODEC_CASES4(V) \
  IRS_BLOCK_CODEC_CASE(V)         \
  IRS_BLOCK_CODEC_CASE(V + 1)     \
  IRS_BLOCK_CODEC_CASE(V + 2)     \
  IRS_BLOCK_CODEC_CASE(V + 3)
#define IRS_BLOCK_CODEC_CASES16(V) \
  IRS_BLOCK_CODEC_CASES4(V)        \
  IRS_BLOCK_CODEC_CASES4(V + 4)    \
  IRS_BLOCK_CODEC_CASES4(V + 8)    \
  IRS_BLOCK_CODEC_CASES4(V + 12)
#define IRS_BLOCK_CODEC_CASES64(V) \
  IRS_BLOCK_CODEC_CASES16(V)       \
  IRS_BLOCK_CODEC_CASES16(V + 16)  \
  IRS_BLOCK_CODEC_CASES16(V + 32)  \
  IRS_BLOCK_CODEC_CASES16(V + 48)

template<uint32_t N, typename = std::make_integer_sequence<uint32_t, N>>
struct ByteKeys;

template<uint32_t N, uint32_t... V>
struct ByteKeys<N, std::integer_sequence<uint32_t, V...>> : Axis<V...> {};

template<uint32_t N>
struct ByteAxis : ByteKeys<N> {
  static_assert(N <= 256);

  static constexpr uint32_t Of(uint32_t value) noexcept {
    SDB_ASSERT(value < N);
    return value;
  }

  template<typename Func>
  IRS_FORCE_INLINE static decltype(auto) Dispatch(uint32_t value, Func&& func) {
    switch (value) {
      IRS_BLOCK_CODEC_CASES64(0)
      IRS_BLOCK_CODEC_CASES64(64)
      IRS_BLOCK_CODEC_CASES64(128)
      IRS_BLOCK_CODEC_CASES64(192)
    }
    SDB_UNREACHABLE();
  }
};

#undef IRS_BLOCK_CODEC_CASES64
#undef IRS_BLOCK_CODEC_CASES16
#undef IRS_BLOCK_CODEC_CASES4
#undef IRS_BLOCK_CODEC_CASE

enum class Family : uint8_t {
  Pack,
  PatchByte,
  PatchBit,
};

struct TokenShape {
  Family family = Family::Pack;
  uint32_t bits = 0;
};

template<typename Encoding>
constexpr TokenShape ShapeOf(uint32_t token) noexcept {
  if (token >= Code(Encoding::PatchBit)) {
    return {Family::PatchBit, token - Code(Encoding::PatchBit)};
  }
  if (token >= Code(Encoding::PatchByte)) {
    return {Family::PatchByte, token - Code(Encoding::PatchByte)};
  }
  return {Family::Pack, token - Code(Encoding::Pack) + 1};
}

inline IRS_FORCE_INLINE void FillProgression(doc_id_t* out, uint32_t len,
                                             doc_id_t prev,
                                             uint32_t gap) noexcept {
  for (uint32_t i = 0; i != len; ++i) {
    out[i] = prev + gap * (i + 1);
  }
}

template<uint32_t B, uint32_t Add, bool Full>
IRS_FORCE_INLINE void Unpack(const byte_type* in, uint32_t len,
                             uint32_t* out) noexcept {
  if constexpr (Full) {
    UnpackVertical<B, Add>(in, out);
  } else {
    UnpackHorizontal<B, Add>(in, len, out);
  }
}

struct SizeShape {
  uint16_t fixed = 0;
  uint8_t bits = 0;
  uint8_t per = 0;
  uint8_t highs = 0;
};

template<typename Encoding>
inline constexpr auto kSizeShapes = [] {
  std::array<SizeShape, 256> shapes{};
  for (uint32_t token = 0; token != Code(Encoding::End); ++token) {
    auto& s = shapes[token];
    if constexpr (std::is_same_v<Encoding, DeltaEncoding>) {
      if (IsTokenBitset(token)) {
        s.fixed =
          static_cast<uint16_t>(1 + TokenBitsetWords(token) * sizeof(uint64_t));
        continue;
      }
    }
    if (token < Code(Encoding::Pack)) {
      constexpr uint16_t kSame[] = {1, 2, 3, 5};
      s.fixed = kSame[token];
      continue;
    }
    const auto shape = ShapeOf<Encoding>(token);
    s.bits = static_cast<uint8_t>(shape.bits);
    switch (shape.family) {
      case Family::Pack:
        s.fixed = 1;
        break;
      case Family::PatchByte:
        s.fixed = 2;
        s.per = 2;
        break;
      case Family::PatchBit:
        s.fixed = 3;
        s.per = 1;
        s.highs = std::numeric_limits<byte_type>::max();
        break;
    }
  }
  return shapes;
}();

template<typename Encoding>
IRS_FORCE_INLINE uint32_t BlockBytes(const byte_type* in,
                                     uint32_t len) noexcept {
  const auto& s = kSizeShapes<Encoding>[in[0]];
  const uint32_t n = in[1];
  return s.fixed + PackedSize(len, s.bits) + n * s.per +
         PackedSize(n, in[2] & s.highs);
}

inline constexpr uint32_t kPatchGroup = 4;

static_assert(kOutSlack >= kPatchGroup);

template<bool Full>
IRS_FORCE_INLINE void AddHigh(uint32_t* out, uint32_t len, uint32_t k,
                              bool keep, byte_type slot,
                              uint32_t value) noexcept {
  if constexpr (Full) {
    out[slot] += keep ? value : 0;
  } else {
    out[keep ? slot : len + k] += value;
  }
}

template<typename Group>
IRS_FORCE_INLINE void PatchGroups(const byte_type* first, const byte_type* last,
                                  uint32_t stride, Group&& group) noexcept {
  const auto* p = first;
  group(p, static_cast<uint32_t>(last - p) / stride);
  for (p += kPatchGroup * stride; p < last; p += kPatchGroup * stride) {
    group(p, static_cast<uint32_t>(last - p) / stride);
  }
}

template<uint32_t W, uint32_t Add, bool Full>
IRS_FORCE_INLINE const byte_type* DecodePairs(const byte_type* in, uint32_t len,
                                              uint32_t* out) noexcept {
  if constexpr (Full) {
    len = kBlock;
  }
  const uint32_t n = in[1];
  Unpack<W, Add, Full>(in + 2, len, out);
  const auto* p = in + 2 + PackedSize(len, W);
  const auto* end = p + 2 * n;
  const auto group = [&](const byte_type* e, uint32_t left) IRS_FORCE_INLINE {
    for (uint32_t k = 0; k != kPatchGroup; ++k) {
      AddHigh<Full>(out, len, k, k < left, e[2 * k],
                    uint32_t{e[2 * k + 1]} << W);
    }
  };
  PatchGroups(p, end, 2, group);
  return end;
}

template<uint32_t W, uint32_t Add, bool Full>
IRS_FORCE_INLINE const byte_type* DecodePacked(const byte_type* in,
                                               uint32_t len,
                                               uint32_t* out) noexcept {
  if constexpr (Full) {
    len = kBlock;
  }
  const uint32_t n = in[1];
  const uint32_t high_bits = in[2];
  Unpack<W, Add, Full>(in + 3, len, out);
  const auto* slots = in + 3 + PackedSize(len, W);
  const auto* highs = slots + n;
  const uint32_t mask = (1U << high_bits) - 1;
  const auto group = [&](const byte_type* s, uint32_t left) IRS_FORCE_INLINE {
    const auto at = static_cast<uint32_t>(s - slots) * high_bits;
    for (uint32_t k = 0; k != kPatchGroup; ++k) {
      const uint32_t bit = at + k * high_bits;
      const auto word =
        absl::little_endian::Load64(highs + bit / 8) >> (bit % 8);
      const auto high = (static_cast<uint32_t>(word) & mask) + 1;
      AddHigh<Full>(out, len, k, k < left, s[k], high << W);
    }
  };
  PatchGroups(slots, highs, 1, group);
  return highs + PackedSize(n, high_bits);
}

inline IRS_FORCE_INLINE doc_id_t* MaterializeScalar(doc_id_t offset,
                                                    uint64_t word,
                                                    doc_id_t* out) noexcept {
  for (; word != 0; word &= word - 1) {
    *out++ = offset + static_cast<doc_id_t>(std::countr_zero(word));
  }
  return out;
}

inline IRS_FORCE_INLINE doc_id_t* MaterializeBits(doc_id_t offset,
                                                  uint64_t word,
                                                  doc_id_t* out) noexcept {
#ifdef __AVX2__
  uint64_t counts = word - ((word >> 1) & 0x5555555555555555ULL);
  counts =
    (counts & 0x3333333333333333ULL) + ((counts >> 2) & 0x3333333333333333ULL);
  counts = (counts + (counts >> 4)) & 0x0F0F0F0F0F0F0F0FULL;
  const uint64_t prefix = (counts * 0x0101010101010101ULL) << 8;
  for (uint32_t b = 0; b != 8; ++b) {
    const __m256i docs = _mm256_add_epi32(
      _mm256_set1_epi32(static_cast<int>(offset + b * 8)),
      LeftPackControl(static_cast<uint32_t>(word >> (8 * b)) & 0xFF));
    _mm256_storeu_si256(
      reinterpret_cast<__m256i*>(out + ((prefix >> (8 * b)) & 0xFF)), docs);
  }
  return out + std::popcount(word);
#else
  return MaterializeScalar(offset, word, out);
#endif
}

#ifdef __AVX2__
inline IRS_FORCE_INLINE IRS_BLOCK_CODEC_AVX512 __m512i OpaqueZero() noexcept {
  __m512i zero = _mm512_setzero_si512();
  asm("" : "+v"(zero));
  return zero;
}
#endif

inline IRS_FORCE_INLINE IRS_BLOCK_CODEC_AVX512 doc_id_t* MaterializeWord16(
  doc_id_t offset, uint64_t word, doc_id_t* out) noexcept {
#ifdef __AVX2__
  static_assert(kOutSlack >= 16);
  const __m512i lanes =
    _mm512_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
  const __m512i zero = OpaqueZero();
  for (uint32_t q = 0; q != 4; ++q) {
    const auto mask = static_cast<__mmask16>(word >> (16 * q));
    _mm512_storeu_si512(
      out, _mm512_mask_compress_epi32(
             zero, mask,
             _mm512_add_epi32(
               lanes, _mm512_set1_epi32(static_cast<int>(offset + 16 * q)))));
    out += std::popcount(mask);
  }
  return out;
#else
  return MaterializeScalar(offset, word, out);
#endif
}

inline IRS_FORCE_INLINE IRS_BLOCK_CODEC_AVX512 const byte_type* DecodeBitset16(
  const byte_type* in, uint32_t len, doc_id_t prev, doc_id_t* out) noexcept {
  const auto [bits, words] = ParseBitset(in);
  auto* p = out;
  for (uint32_t w = 0; w != words; ++w) {
    p = MaterializeWord16(
      prev + 1 + w * BitsRequired<uint64_t>(),
      absl::little_endian::Load64(bits + w * sizeof(uint64_t)), p);
  }
  SDB_ASSERT(p == out + len);
  return bits + words * sizeof(uint64_t);
}

#ifdef __AVX2__
template<uint32_t Q>
inline IRS_FORCE_INLINE IRS_BLOCK_CODEC_VBMI2 void StoreDocs64(
  doc_id_t* out, __m512i positions, __m512i base) noexcept {
  _mm512_storeu_si512(
    out + 16 * Q,
    _mm512_add_epi32(
      _mm512_cvtepu8_epi32(_mm512_extracti32x4_epi32(positions, Q)), base));
}

template<uint32_t G>
inline IRS_FORCE_INLINE IRS_BLOCK_CODEC_VBMI2 doc_id_t* MaterializeWord64(
  doc_id_t offset, uint64_t word, doc_id_t* out) noexcept {
  static_assert(kOutSlack >= 16);
  const __m512i positions = _mm512_mask_compress_epi8(
    OpaqueZero(), word,
    _mm512_set_epi8(63, 62, 61, 60, 59, 58, 57, 56, 55, 54, 53, 52, 51, 50, 49,
                    48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 37, 36, 35, 34,
                    33, 32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19,
                    18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2,
                    1, 0));
  const __m512i base = _mm512_set1_epi32(static_cast<int>(offset));
  const auto count = static_cast<uint32_t>(std::popcount(word));
  StoreDocs64<0>(out, positions, base);
  if (G > 1 || count > 16) {
    StoreDocs64<1>(out, positions, base);
  }
  if (G > 2 || count > 32) {
    StoreDocs64<2>(out, positions, base);
  }
  if (G > 3 || count > 48) {
    StoreDocs64<3>(out, positions, base);
  }
  return out + count;
}

template<uint32_t G>
IRS_FORCE_INLINE IRS_BLOCK_CODEC_VBMI2 const byte_type* DecodeBitsetWords64(
  const byte_type* in, uint32_t len, doc_id_t prev, doc_id_t* out) noexcept {
  const auto [bits, words] = ParseBitset(in);
  auto* p = out;
  const auto* const end = out + len + kOutSlack;
  uint32_t w = 0;
  const auto word = [&] IRS_FORCE_INLINE {
    return absl::little_endian::Load64(bits + w * sizeof(uint64_t));
  };
  for (; w != words && end - p >= 16 * G; ++w) {
    p =
      MaterializeWord64<G>(prev + 1 + w * BitsRequired<uint64_t>(), word(), p);
  }
  for (; w != words; ++w) {
    p =
      MaterializeWord64<1>(prev + 1 + w * BitsRequired<uint64_t>(), word(), p);
  }
  SDB_ASSERT(p == out + len);
  return bits + words * sizeof(uint64_t);
}
#endif

inline IRS_FORCE_INLINE IRS_BLOCK_CODEC_VBMI2 const byte_type* DecodeBitset64(
  const byte_type* in, uint32_t len, doc_id_t prev, doc_id_t* out) noexcept {
#ifdef __AVX2__
  constexpr uint32_t kSparse = 10;
  constexpr uint32_t kMedium = 24;
  const auto words = ParseBitset(in).words;
  if (len <= kSparse * words) {
    return DecodeBitsetWords64<1>(in, len, prev, out);
  }
  if (len <= kMedium * words) {
    return DecodeBitsetWords64<2>(in, len, prev, out);
  }
  return DecodeBitsetWords64<4>(in, len, prev, out);
#else
  return DecodeBitset16(in, len, prev, out);
#endif
}

inline IRS_NO_INLINE IRS_BLOCK_CODEC_VBMI2 const byte_type* DecodeBitsetBlock64(
  const byte_type* in, doc_id_t prev, doc_id_t* out) noexcept {
  return DecodeBitset64(in, kBlock, prev, out);
}

inline IRS_NO_INLINE IRS_BLOCK_CODEC_VBMI2 const byte_type* DecodeBitsetTail64(
  const byte_type* in, uint32_t len, doc_id_t prev, doc_id_t* out) noexcept {
  return DecodeBitset64(in, len, prev, out);
}

template<uint32_t Token, bool Full, bool Avx512 = false>
IRS_FORCE_INLINE const byte_type* DecodeDeltaBody(const byte_type* in,
                                                  uint32_t len, doc_id_t prev,
                                                  doc_id_t* out) noexcept {
  if constexpr (Full) {
    len = kBlock;
  }
  if constexpr (Token == Code(DeltaEncoding::Run)) {
    FillProgression(out, len, prev, 1);
    return in + 1;
  } else if constexpr (Token == Code(DeltaEncoding::Same08)) {
    FillProgression(out, len, prev, in[1]);
    return in + 2;
  } else if constexpr (Token == Code(DeltaEncoding::Same16)) {
    FillProgression(out, len, prev, absl::little_endian::Load16(in + 1));
    return in + 1 + sizeof(uint16_t);
  } else if constexpr (Token == Code(DeltaEncoding::Same32)) {
    FillProgression(out, len, prev, absl::little_endian::Load32(in + 1));
    return in + 1 + sizeof(uint32_t);
  } else if constexpr (IsTokenBitset(Token)) {
    const auto [bits, words] = ParseBitset(in);
    auto* p = out;
    for (uint32_t w = 0; w != words; ++w) {
      p = MaterializeBits(
        prev + 1 + w * BitsRequired<uint64_t>(),
        absl::little_endian::Load64(bits + w * sizeof(uint64_t)), p);
    }
    SDB_ASSERT(p == out + len);
    return bits + words * sizeof(uint64_t);
  } else {
    constexpr auto kShape = ShapeOf<DeltaEncoding>(Token);
    if constexpr (Full && kShape.family == Family::Pack) {
      if constexpr (Avx512) {
        UnpackVerticalDelta16<kShape.bits>(in + 1, prev, out);
      } else {
        UnpackVerticalDelta<kShape.bits>(in + 1, prev, out);
      }
      return in + 1 + PackedSize(kBlock, kShape.bits);
    } else if constexpr (kShape.family == Family::Pack) {
      if constexpr (Avx512 && kShape.bits <= kMaxPackGroupBits) {
        UnpackHorizontalDelta16<kShape.bits>(in + 1, len, prev, out);
      } else {
        UnpackHorizontalDelta<kShape.bits>(in + 1, len, prev, out);
      }
      return in + 1 + PackedSize(len, kShape.bits);
    } else {
      const byte_type* end;
      if constexpr (kShape.family == Family::PatchByte) {
        end = DecodePairs<kShape.bits, 0, Full>(in, len, out);
      } else {
        end = DecodePacked<kShape.bits, 0, Full>(in, len, out);
      }
      if constexpr (Avx512 && Full) {
        ScanDocs16(out, prev);
      } else {
        ScanDocs(out, len, prev);
      }
      return end;
    }
  }
}

using DeltaBlockDecoder = const byte_type* (*)(const byte_type*, doc_id_t,
                                               doc_id_t*) noexcept;
using DeltaTailDecoder = const byte_type* (*)(const byte_type*, uint32_t,
                                              doc_id_t, doc_id_t*) noexcept;

template<uint32_t Token>
IRS_NO_INLINE const byte_type* DecodeDeltaBlockToken(const byte_type* in,
                                                     doc_id_t prev,
                                                     doc_id_t* out) noexcept {
  return DecodeDeltaBody<Token, true>(in, kBlock, prev, out);
}

template<uint32_t Token>
IRS_NO_INLINE const byte_type* DecodeDeltaTailToken(const byte_type* in,
                                                    uint32_t len, doc_id_t prev,
                                                    doc_id_t* out) noexcept {
  return DecodeDeltaBody<Token, false>(in, len, prev, out);
}

template<uint32_t Token>
IRS_NO_INLINE IRS_BLOCK_CODEC_AVX512 const byte_type*
DecodeDeltaBlockTokenAvx512(const byte_type* in, doc_id_t prev,
                            doc_id_t* out) noexcept {
  if constexpr (IsTokenBitset(Token)) {
    return DecodeBitset16(in, kBlock, prev, out);
  } else {
    return DecodeDeltaBody<Token, true, true>(in, kBlock, prev, out);
  }
}

template<uint32_t Token>
IRS_NO_INLINE IRS_BLOCK_CODEC_AVX512 const byte_type*
DecodeDeltaTailTokenAvx512(const byte_type* in, uint32_t len, doc_id_t prev,
                           doc_id_t* out) noexcept {
  if constexpr (IsTokenBitset(Token)) {
    return DecodeBitset16(in, len, prev, out);
  } else {
    return DecodeDeltaBody<Token, false, true>(in, len, prev, out);
  }
}

consteval bool Avx512Tail(uint32_t token) noexcept {
  if (IsTokenBitset(token)) {
    return true;
  }
  if (token < Code(DeltaEncoding::Pack)) {
    return false;
  }
  const auto shape = ShapeOf<DeltaEncoding>(token);
  return shape.family == Family::Pack && shape.bits <= kMaxPackGroupBits;
}

template<uint32_t Token, bool Avx512, bool Vbmi2>
consteval DeltaBlockDecoder DeltaBlockDecoderOf() noexcept {
  if constexpr (IsTokenBitset(Token) &&
                Token != Code(DeltaEncoding::BitsetWords)) {
    return DeltaBlockDecoderOf<Code(DeltaEncoding::BitsetWords), Avx512,
                               Vbmi2>();
  } else if constexpr (Vbmi2 && IsTokenBitset(Token)) {
    return &DecodeBitsetBlock64;
  } else if constexpr (Avx512 && Token >= Code(DeltaEncoding::Pack)) {
    return &DecodeDeltaBlockTokenAvx512<Token>;
  } else {
    return &DecodeDeltaBlockToken<Token>;
  }
}

template<uint32_t Token, bool Avx512, bool Vbmi2>
consteval DeltaTailDecoder DeltaTailDecoderOf() noexcept {
  if constexpr (IsTokenBitset(Token) &&
                Token != Code(DeltaEncoding::BitsetWords)) {
    return DeltaTailDecoderOf<Code(DeltaEncoding::BitsetWords), Avx512,
                              Vbmi2>();
  } else if constexpr (Vbmi2 && IsTokenBitset(Token)) {
    return &DecodeBitsetTail64;
  } else if constexpr (Avx512 && Avx512Tail(Token)) {
    return &DecodeDeltaTailTokenAvx512<Token>;
  } else {
    return &DecodeDeltaTailToken<Token>;
  }
}

template<bool Avx512, bool Vbmi2 = false>
inline constexpr auto kDeltaBlockDecoders =
  []<uint32_t... Token>(std::integer_sequence<uint32_t, Token...>) {
    return std::array<DeltaBlockDecoder, sizeof...(Token)>{
      DeltaBlockDecoderOf<Token, Avx512, Vbmi2>()...};
  }(std::make_integer_sequence<uint32_t, Code(DeltaEncoding::End)>{});

template<bool Avx512, bool Vbmi2 = false>
inline constexpr auto kDeltaTailDecoders =
  []<uint32_t... Token>(std::integer_sequence<uint32_t, Token...>) {
    return std::array<DeltaTailDecoder, sizeof...(Token)>{
      DeltaTailDecoderOf<Token, Avx512, Vbmi2>()...};
  }(std::make_integer_sequence<uint32_t, Code(DeltaEncoding::End)>{});

struct DeltaDecoders {
  const DeltaBlockDecoder* blocks;
  const DeltaTailDecoder* tails;
};

template<bool Avx512, bool Vbmi2 = false>
inline constexpr DeltaDecoders kDeltaDecodersOf{
  .blocks = kDeltaBlockDecoders<Avx512, Vbmi2>.data(),
  .tails = kDeltaTailDecoders<Avx512, Vbmi2>.data(),
};

extern const DeltaDecoders kDeltaDecoders;

template<uint32_t Token, uint32_t Add, bool Full>
IRS_FORCE_INLINE const byte_type* DecodeValuesBody(const byte_type* in,
                                                   uint32_t len,
                                                   uint32_t* out) noexcept {
  if constexpr (Full) {
    len = kBlock;
  }
  if constexpr (Token == Code(ValueEncoding::Zero)) {
    Unpack<0, Add, Full>(in + 1, len, out);
    return in + 1;
  } else if constexpr (Token == Code(ValueEncoding::Same08)) {
    std::fill_n(out, len, in[1] + Add);
    return in + 2;
  } else if constexpr (Token == Code(ValueEncoding::Same16)) {
    std::fill_n(out, len, absl::little_endian::Load16(in + 1) + Add);
    return in + 1 + sizeof(uint16_t);
  } else if constexpr (Token == Code(ValueEncoding::Same32)) {
    std::fill_n(out, len, absl::little_endian::Load32(in + 1) + Add);
    return in + 1 + sizeof(uint32_t);
  } else {
    constexpr auto kShape = ShapeOf<ValueEncoding>(Token);
    if constexpr (kShape.family == Family::Pack) {
      Unpack<kShape.bits, Add, Full>(in + 1, len, out);
      return in + 1 + PackedSize(len, kShape.bits);
    } else if constexpr (kShape.family == Family::PatchByte) {
      return DecodePairs<kShape.bits, Add, Full>(in, len, out);
    } else {
      return DecodePacked<kShape.bits, Add, Full>(in, len, out);
    }
  }
}

template<uint32_t Token, uint32_t Add, bool Full>
IRS_NO_INLINE const byte_type* DecodeValuesToken(const byte_type* in,
                                                 uint32_t len,
                                                 uint32_t* out) noexcept {
  return DecodeValuesBody<Token, Add, Full>(in, len, out);
}

using ValueDecoder = const byte_type* (*)(const byte_type*, uint32_t,
                                          uint32_t*);

template<uint32_t Add, bool Full>
inline constexpr auto kValueDecodersOf =
  []<uint32_t... Token>(std::integer_sequence<uint32_t, Token...>) {
    return std::array<ValueDecoder, sizeof...(Token)>{
      &DecodeValuesToken<Token, Add, Full>...};
  }(std::make_integer_sequence<uint32_t, Code(ValueEncoding::End)>{});

struct ValueDecoders {
  const ValueDecoder* blocks;
  const ValueDecoder* tails;
};

inline constexpr uint32_t kValueBiases = 2;

extern const ValueDecoders kValueDecoders[kValueBiases];

struct BlockCodec {
  IRS_FORCE_INLINE static const byte_type* DecodeDeltaBlock(const byte_type* in,
                                                            doc_id_t prev,
                                                            doc_id_t* out) {
    SDB_ASSERT(in[0] < Code(DeltaEncoding::End));
    return kDeltaDecoders.blocks[in[0]](in, prev, out);
  }

  IRS_FORCE_INLINE static const byte_type* DecodeDeltaTail(const byte_type* in,
                                                           uint32_t len,
                                                           doc_id_t prev,
                                                           doc_id_t* out) {
    SDB_ASSERT(1 <= len && len < kBlock);
    SDB_ASSERT(in[0] < Code(DeltaEncoding::End));
    return kDeltaDecoders.tails[in[0]](in, len, prev, out);
  }

  static uint32_t DeltaBlockSize(const byte_type* in) {
    return BlockBytes<DeltaEncoding>(in, kBlock);
  }

  static uint32_t DeltaTailSize(const byte_type* in, uint32_t len) {
    SDB_ASSERT(1 <= len && len < kBlock);
    return BlockBytes<DeltaEncoding>(in, len);
  }

  template<uint32_t Add = 0>
  IRS_FORCE_INLINE static const byte_type* DecodeValuesBlock(
    const byte_type* in, uint32_t* out) {
    static_assert(Add < kValueBiases);
    SDB_ASSERT(in[0] < Code(ValueEncoding::End));
    return kValueDecoders[Add].blocks[in[0]](in, kBlock, out);
  }

  template<uint32_t Add = 0>
  IRS_FORCE_INLINE static const byte_type* DecodeValuesTail(const byte_type* in,
                                                            uint32_t len,
                                                            uint32_t* out) {
    static_assert(Add < kValueBiases);
    SDB_ASSERT(1 <= len && len < kBlock);
    SDB_ASSERT(in[0] < Code(ValueEncoding::End));
    return kValueDecoders[Add].tails[in[0]](in, len, out);
  }

  static uint32_t ValuesBlockSize(const byte_type* in) {
    return BlockBytes<ValueEncoding>(in, kBlock);
  }

  static uint32_t ValuesTailSize(const byte_type* in, uint32_t len) {
    SDB_ASSERT(1 <= len && len < kBlock);
    return BlockBytes<ValueEncoding>(in, len);
  }

  static uint32_t DeltaPrefix(uint32_t token) {
    return kSizeShapes<DeltaEncoding>[token].fixed;
  }

  static uint32_t ValuesPrefix(uint32_t token) {
    return kSizeShapes<ValueEncoding>[token].fixed;
  }
};

}  // namespace irs::block_codec
