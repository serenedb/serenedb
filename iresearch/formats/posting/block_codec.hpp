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

inline constexpr uint32_t kMaxExceptions =
  std::numeric_limits<byte_type>::max();
inline constexpr uint32_t kBitsetMarginPercent = 60;
inline constexpr uint32_t kMaxBlockBytes = 1 + PackedSize(kBlock, kMaxWidth);

static_assert(kBlock == kMaxExceptions + 1);

struct EncodeOptions {
  uint32_t exception_cost_eighths = 0;
  bool narrow_highs = false;
};

inline constexpr uint32_t kMaskBits = BitsRequired<uint64_t>();

inline constexpr uint32_t PaddedLen(uint32_t len) noexcept {
  return (len + kMaskBits - 1) / kMaskBits * kMaskBits;
}

inline IRS_FORCE_INLINE U32x8 LoadFirst(const uint32_t* in,
                                        uint32_t count) noexcept {
  SDB_ASSERT(count < kLanes);
#ifdef __AVX2__
  const __m256i lanes = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7);
  const __m256i mask =
    _mm256_cmpgt_epi32(_mm256_set1_epi32(static_cast<int>(count)), lanes);
  return std::bit_cast<U32x8>(
    _mm256_maskload_epi32(reinterpret_cast<const int*>(in), mask));
#else
  U32x8 v{};
  for (uint32_t k = 0; k != count; ++k) {
    v[k] = in[k];
  }
  return v;
#endif
}

inline IRS_FORCE_INLINE void PadTail(const uint32_t* in, uint32_t len,
                                     uint32_t* out) noexcept {
  uint32_t i = 0;
  for (; i + kLanes <= len; i += kLanes) {
    U32x8 v;
    std::memcpy(&v, in + i, sizeof(v));
    v = Opaque(v);
    std::memcpy(out + i, &v, sizeof(v));
  }
  if (i != len) {
    const U32x8 v = LoadFirst(in + i, len - i);
    std::memcpy(out + i, &v, sizeof(v));
    i += kLanes;
  }
  const U32x8 zero = Opaque(U32x8{});
  for (const uint32_t end = PaddedLen(len); i != end; i += kLanes) {
    std::memcpy(out + i, &zero, sizeof(zero));
  }
}

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
  IRS_BLOCK_CODEC_CASE(V + 2) IRS_BLOCK_CODEC_CASE(V + 3)
#define IRS_BLOCK_CODEC_CASES16(V) \
  IRS_BLOCK_CODEC_CASES4(V)        \
  IRS_BLOCK_CODEC_CASES4(V + 4)    \
  IRS_BLOCK_CODEC_CASES4(V + 8) IRS_BLOCK_CODEC_CASES4(V + 12)
#define IRS_BLOCK_CODEC_CASES64(V) \
  IRS_BLOCK_CODEC_CASES16(V)       \
  IRS_BLOCK_CODEC_CASES16(V + 16)  \
  IRS_BLOCK_CODEC_CASES16(V + 32) IRS_BLOCK_CODEC_CASES16(V + 48)

template<uint32_t N, typename Func>
IRS_FORCE_INLINE decltype(auto) ResolveByte(uint32_t value, Func&& func) {
  static_assert(N <= 256);
  SDB_ASSERT(value < N);
  switch (value) {
    IRS_BLOCK_CODEC_CASES64(0)
    IRS_BLOCK_CODEC_CASES64(64)
    IRS_BLOCK_CODEC_CASES64(128)
    IRS_BLOCK_CODEC_CASES64(192)
  }
  SDB_UNREACHABLE();
}

#undef IRS_BLOCK_CODEC_CASES64
#undef IRS_BLOCK_CODEC_CASES16
#undef IRS_BLOCK_CODEC_CASES4
#undef IRS_BLOCK_CODEC_CASE

enum class Family : uint8_t {
  Pack,
  PatchByte,
  PatchBit,
};

struct Plan {
  Family family = Family::Pack;
  uint32_t bits = 0;
  uint32_t count = 0;
  uint32_t high = 0;
  uint32_t size = std::numeric_limits<uint32_t>::max();
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

using I8x8 = int8_t __attribute__((vector_size(8)));
using I8x16 = int8_t __attribute__((vector_size(16)));
using I8x32 = int8_t __attribute__((vector_size(32)));
using F32x8 = float __attribute__((vector_size(32)));

inline constexpr uint32_t kFloatExactWidth = 24;

inline IRS_FORCE_INLINE uint32_t MoveMask8(I8x32 lanes) noexcept {
#ifdef __AVX2__
  return static_cast<uint32_t>(
    _mm256_movemask_epi8(std::bit_cast<__m256i>(lanes)));
#else
  uint32_t mask = 0;
  for (uint32_t k = 0; k != sizeof(lanes); ++k) {
    mask |= uint32_t{lanes[k] < 0} << k;
  }
  return mask;
#endif
}

inline IRS_FORCE_INLINE uint32_t SumBytes(I8x32 lanes) noexcept {
#ifdef __AVX2__
  const __m256i sums =
    _mm256_sad_epu8(std::bit_cast<__m256i>(lanes), _mm256_setzero_si256());
  const __m128i half = _mm_add_epi64(_mm256_castsi256_si128(sums),
                                     _mm256_extracti128_si256(sums, 1));
  return static_cast<uint32_t>(_mm_cvtsi128_si64(half) +
                               _mm_extract_epi64(half, 1));
#else
  const auto bytes = std::bit_cast<U8x32>(lanes);
  const U8x16 lo = __builtin_shufflevector(bytes, bytes, 0, 1, 2, 3, 4, 5, 6, 7,
                                           8, 9, 10, 11, 12, 13, 14, 15);
  const U8x16 hi =
    __builtin_shufflevector(bytes, bytes, 16, 17, 18, 19, 20, 21, 22, 23, 24,
                            25, 26, 27, 28, 29, 30, 31);
  return uint32_t{__builtin_reduce_add(lo)} + __builtin_reduce_add(hi);
#endif
}

template<bool Exact>
IRS_FORCE_INLINE I8x8 LaneWidths(I32x8 lanes) noexcept {
  if constexpr (Exact) {
    lanes &= ~((lanes > (1 << kFloatExactWidth) - 1) &
               ((1 << (kMaxWidth - kFloatExactWidth)) - 1));
  }
  I32x8 width =
    (std::bit_cast<I32x8>(__builtin_convertvector(lanes, F32x8)) >> 23) - 126;
  width &= ~(width >> 31);
  return __builtin_convertvector(width, I8x8);
}

template<bool Exact>
IRS_FORCE_INLINE I8x8 BitWidths(const uint32_t* values) noexcept {
  I32x8 lanes;
  std::memcpy(&lanes, values, sizeof(lanes));
  return LaneWidths<Exact>(lanes);
}

template<typename Lanes>
IRS_FORCE_INLINE I8x32 Gather32(const uint32_t* values, Lanes lanes) noexcept {
  const I8x16 lo =
    __builtin_shufflevector(lanes(values), lanes(values + 8), 0, 1, 2, 3, 4, 5,
                            6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
  const I8x16 hi =
    __builtin_shufflevector(lanes(values + 16), lanes(values + 24), 0, 1, 2, 3,
                            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
  return __builtin_shufflevector(lo, hi, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
                                 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                                 24, 25, 26, 27, 28, 29, 30, 31);
}

template<bool Full>
class Stats {
 public:
  static constexpr uint32_t kVectors = kBlock / sizeof(I8x32);

  IRS_FORCE_INLINE Stats(const uint32_t* values, uint32_t len,
                         uint32_t width) noexcept
    : _len{len}, _width{width} {
    SDB_ASSERT(width <= kMaxWidth);
    if constexpr (!Full) {
      _vectors = (len + sizeof(I8x32) - 1) / sizeof(I8x32);
    }
    if (width > kFloatExactWidth) {
      Fill<true>(values);
    } else {
      Fill<false>(values);
    }
  }

  uint32_t Len() const noexcept { return _len; }
  uint32_t Width() const noexcept { return _width; }

  IRS_FORCE_INLINE uint32_t Vectors() const noexcept {
    if constexpr (Full) {
      return kVectors;
    } else {
      return _vectors;
    }
  }

  IRS_FORCE_INLINE uint32_t Words() const noexcept {
    return (Vectors() * sizeof(I8x32) + kMaskBits - 1) / kMaskBits;
  }

  IRS_FORCE_INLINE uint32_t Above(uint32_t bits) const noexcept {
    SDB_ASSERT(bits < _width);
    const auto threshold = static_cast<int8_t>(bits);
    if constexpr (Full) {
      I8x32 count{};
      for (const auto& widths : _widths) {
        count -= widths > threshold;
      }
      return SumBytes(count);
    } else {
      uint32_t count = 0;
      for (uint32_t j = 0; j != Vectors(); ++j) {
        count += static_cast<uint32_t>(
          std::popcount(MoveMask8(_widths[j] > threshold)));
      }
      return count;
    }
  }

  uint64_t Mask(uint32_t bits, uint32_t word) const noexcept {
    SDB_ASSERT(bits < kMaxWidth);
    const auto threshold = static_cast<int8_t>(bits);
    return MoveMask8(_widths[2 * word] > threshold) |
           uint64_t{MoveMask8(_widths[2 * word + 1] > threshold)} << 32;
  }

 private:
  template<bool Exact>
  IRS_FORCE_INLINE void Fill(const uint32_t* values) noexcept {
    for (uint32_t j = 0; j != Vectors(); ++j) {
      _widths[j] = Gather32(values + j * sizeof(I8x32), &BitWidths<Exact>);
    }
    if constexpr (!Full) {
      if (_vectors % 2 != 0) {
        _widths[_vectors] = I8x32{};
      }
    }
  }

  uint32_t _len;
  uint32_t _vectors = kVectors;
  uint32_t _width;
  I8x32 _widths[kVectors];
};

template<bool Full>
Plan ChoosePlan(const Stats<Full>& stats, uint32_t max,
                const EncodeOptions& options) noexcept {
  constexpr uint32_t kByteMax = std::numeric_limits<byte_type>::max();
  const uint32_t len = stats.Len();
  const uint32_t width = stats.Width();
  Plan best{
    .family = Family::Pack, .bits = width, .size = 1 + PackedSize(len, width)};
  uint64_t best_cost = uint64_t{best.size} * 8;
  for (uint32_t w = width; w-- != 0;) {
    const uint32_t n = stats.Above(w);
    if (n > kMaxExceptions || uint64_t{2 + n} * 8 >= best_cost) {
      break;
    }
    const uint32_t lows = PackedSize(len, w);
    const uint32_t top = max >> w;
    const uint64_t extra = uint64_t{options.exception_cost_eighths} * n;
    if (top <= kByteMax) {
      const uint32_t size = 2 + lows + 2 * n;
      if (const uint64_t cost = uint64_t{size} * 8 + extra; cost < best_cost) {
        best = {
          .family = Family::PatchByte, .bits = w, .count = n, .size = size};
        best_cost = cost;
      }
    }
    if (options.narrow_highs || top > kByteMax) {
      const auto high = static_cast<uint32_t>(std::bit_width(top - 1));
      const uint32_t size = 3 + lows + n + PackedSize(n, high);
      if (const uint64_t cost = uint64_t{size} * 8 + extra; cost < best_cost) {
        best = {.family = Family::PatchBit,
                .bits = w,
                .count = n,
                .high = high,
                .size = size};
        best_cost = cost;
      }
    }
  }
  return best;
}

template<uint32_t B, bool Full, bool Mask>
IRS_NO_INLINE void PackWidth(const uint32_t* in, uint32_t len,
                             byte_type* out) noexcept {
  if constexpr (Full) {
    PackVertical<B, Mask>(in, out);
  } else {
    PackHorizontal<B>(in, len, out);
  }
}

template<bool Full, bool Mask>
void PackBits(uint32_t bits, const uint32_t* in, uint32_t len,
              byte_type* out) noexcept {
  ResolveByte<kMaxWidth + 1>(bits, [&]<uint32_t B>() IRS_FORCE_INLINE {
    PackWidth<B, Full, Mask>(in, len, out);
  });
}

template<typename Encoding, bool Full>
uint32_t WritePlan(const uint32_t* values, const Stats<Full>& stats,
                   const Plan& plan, byte_type* out) noexcept {
  const uint32_t len = Full ? kBlock : stats.Len();
  if (plan.family == Family::Pack) {
    out[0] = static_cast<byte_type>(Code(Encoding::Pack) + plan.bits - 1);
    PackBits<Full, false>(plan.bits, values, len, out + 1);
    return plan.size;
  }
  const uint32_t w = plan.bits;
  const bool bytes = plan.family == Family::PatchByte;
  out[0] = static_cast<byte_type>(
    Code(bytes ? Encoding::PatchByte : Encoding::PatchBit) + w);
  out[1] = static_cast<byte_type>(plan.count);
  auto* p = out + 2;
  if (!bytes) {
    out[2] = static_cast<byte_type>(plan.high);
    ++p;
  }
  PackBits<Full, true>(w, values, len, p);
  p += PackedSize(len, w);
  uint32_t highs[kMaxExceptions];
  uint32_t n = 0;
  for (uint32_t word = 0; word != stats.Words(); ++word) {
    for (auto mask = stats.Mask(w, word); mask != 0; mask &= mask - 1) {
      const uint32_t i =
        word * kMaskBits + static_cast<uint32_t>(std::countr_zero(mask));
      const uint32_t high = values[i] >> w;
      if (bytes) {
        p[2 * n] = static_cast<byte_type>(i);
        p[2 * n + 1] = static_cast<byte_type>(high);
      } else {
        p[n] = static_cast<byte_type>(i);
        highs[n] = high - 1;
      }
      ++n;
    }
  }
  SDB_ASSERT(n == plan.count);
  if (bytes) {
    p += 2 * n;
  } else {
    p += n;
    PackBits<false, true>(plan.high, highs, n, p);
    p += PackedSize(n, plan.high);
  }
  SDB_ASSERT(static_cast<uint32_t>(p - out) == plan.size);
  return plan.size;
}

template<typename Encoding>
uint32_t WriteSame(uint32_t value, byte_type* out) noexcept {
  if (value <= std::numeric_limits<uint8_t>::max()) {
    out[0] = static_cast<byte_type>(Encoding::Same08);
    out[1] = static_cast<byte_type>(value);
    return 2;
  }
  if (value <= std::numeric_limits<uint16_t>::max()) {
    out[0] = static_cast<byte_type>(Encoding::Same16);
    absl::little_endian::Store16(out + 1, static_cast<uint16_t>(value));
    return 3;
  }
  out[0] = static_cast<byte_type>(Encoding::Same32);
  absl::little_endian::Store32(out + 1, value);
  return 5;
}

template<bool Full>
uint32_t WriteBitset(const doc_id_t* docs, uint32_t len, doc_id_t prev,
                     uint32_t words, byte_type* out) noexcept {
  if constexpr (Full) {
    len = kBlock;
  }
  SDB_ASSERT(1 <= words && words <= kMaxBitsetWords);
  out[0] = static_cast<byte_type>(Code(DeltaEncoding::BitsetWords) + words - 1);
  auto* bits = out + 1;
  const U64x4 zero = Opaque(U64x4{});
  for (uint32_t w = 0; w <= words; w += 4) {
    std::memcpy(bits + w * sizeof(uint64_t), &zero, sizeof(zero));
  }
  uint32_t current = 0;
  uint64_t low = 0;
  uint64_t high = 0;
  const auto deposit = [&](uint32_t at, uint64_t mask) IRS_FORCE_INLINE {
    const uint32_t w = at / BitsRequired<uint64_t>();
    const uint32_t shift = at % BitsRequired<uint64_t>();
    const uint32_t step = w - current;
    low = (step == 0 ? low : step == 1 ? high : 0) | (mask << shift);
    high = (step == 0 ? high : 0) | ((mask >> 1) >> (63 - shift));
    current = w;
    absl::little_endian::Store64(bits + w * sizeof(uint64_t), low);
    absl::little_endian::Store64(bits + (w + 1) * sizeof(uint64_t), high);
  };
  const doc_id_t base = prev + 1;
  uint32_t i = 0;
  for (; i + kLanes <= len; i += kLanes) {
    U32x8 v;
    std::memcpy(&v, docs + i, sizeof(v));
    v -= base;
    const uint32_t first = v[0];
    const U32x8 offsets = v - first;
    if (offsets[kLanes - 1] < BitsRequired<uint64_t>()) {
      const U64x4 one = U64x4{} + 1;
      const U64x4 masks =
        (one << __builtin_convertvector(
           __builtin_shufflevector(offsets, offsets, 0, 1, 2, 3), U64x4)) |
        (one << __builtin_convertvector(
           __builtin_shufflevector(offsets, offsets, 4, 5, 6, 7), U64x4));
      deposit(first, masks[0] | masks[1] | masks[2] | masks[3]);
    } else {
      for (uint32_t k = 0; k != kLanes; ++k) {
        deposit(v[k], 1);
      }
    }
  }
  for (; i != len; ++i) {
    deposit(docs[i] - base, 1);
  }
  return 1 + words * sizeof(uint64_t);
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

template<bool Full>
consteval bool Inlined(uint32_t token) noexcept {
  if (token < Code(ValueEncoding::Pack)) {
    return true;
  }
  const auto family = ShapeOf<ValueEncoding>(token).family;
  return family == Family::Pack || (Full && family == Family::PatchByte);
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

template<uint32_t Add, bool Full>
const byte_type* DispatchValues(const byte_type* in, uint32_t len,
                                uint32_t* out) noexcept {
  return ResolveByte<Code(ValueEncoding::End)>(
    in[0], [&]<uint32_t Token>() IRS_FORCE_INLINE {
      if constexpr (Inlined<Full>(Token)) {
        return DecodeValuesBody<Token, Add, Full>(in, len, out);
      } else {
        return DecodeValuesToken<Token, Add, Full>(in, len, out);
      }
    });
}

inline IRS_FORCE_INLINE uint32_t BitsetWords(const doc_id_t* docs, uint32_t len,
                                             doc_id_t prev,
                                             uint32_t plan_size) noexcept {
  const uint64_t range = uint64_t{docs[len - 1]} - prev;
  const uint64_t words =
    (range + BitsRequired<uint64_t>() - 1) / BitsRequired<uint64_t>();
  if (words <= kMaxBitsetWords &&
      (1 + words * sizeof(uint64_t)) * 100 <=
        uint64_t{plan_size} * (100 + kBitsetMarginPercent)) {
    return static_cast<uint32_t>(words);
  }
  return 0;
}

template<bool Full>
IRS_NO_INLINE uint32_t EncodeDelta(const doc_id_t* docs, uint32_t len,
                                   doc_id_t prev, byte_type* out,
                                   const EncodeOptions& options) noexcept {
  constexpr uint32_t kN = kBlock;
  if constexpr (Full) {
    len = kN;
  }
  SDB_ASSERT(prev < docs[0]);
  SDB_ASSERT(std::adjacent_find(docs, docs + len, std::greater_equal<>{}) ==
             docs + len);
  uint32_t gaps[kN + kLanes];
  const uint32_t first = docs[0] - prev - 1;
  U32x8 before = U32x8{} + prev;
  U32x8 max8{};
  U32x8 diff8{};
  uint32_t i = 0;
  for (; i + 8 <= len; i += 8) {
    U32x8 current;
    std::memcpy(&current, docs + i, sizeof(current));
    const U32x8 gap =
      current - 1 -
      __builtin_shufflevector(before, current, 7, 8, 9, 10, 11, 12, 13, 14);
    std::memcpy(gaps + i, &gap, sizeof(gap));
    max8 = __builtin_elementwise_max(max8, gap);
    diff8 |= gap ^ first;
    before = current;
  }
  if (i != len) {
    const uint32_t rest = len - i;
    const U32x8 current = LoadFirst(docs + i, rest);
    const auto valid = std::bit_cast<U32x8>(I32x8{0, 1, 2, 3, 4, 5, 6, 7} <
                                            static_cast<int32_t>(rest));
    const U32x8 gap =
      (current - 1 -
       __builtin_shufflevector(before, current, 7, 8, 9, 10, 11, 12, 13, 14)) &
      valid;
    std::memcpy(gaps + i, &gap, sizeof(gap));
    max8 = __builtin_elementwise_max(max8, gap);
    diff8 |= (gap ^ first) & valid;
  }
  const uint32_t max = __builtin_reduce_max(max8);
  if (__builtin_reduce_or(diff8) == 0) {
    if (gaps[0] == 0) {
      out[0] = static_cast<byte_type>(DeltaEncoding::Run);
      return 1;
    }
    return WriteSame<DeltaEncoding>(gaps[0] + 1, out);
  }
  if constexpr (!Full) {
    const U32x8 zero = Opaque(U32x8{});
    for (uint32_t i = len; i < PaddedLen(len); i += kLanes) {
      std::memcpy(gaps + i, &zero, sizeof(zero));
    }
  }
  const Stats<Full> stats{gaps, len,
                          static_cast<uint32_t>(std::bit_width(max))};
  const auto plan = ChoosePlan(stats, max, options);
  if (const auto words = BitsetWords(docs, len, prev, plan.size)) {
    return WriteBitset<Full>(docs, len, prev, words, out);
  }
  return WritePlan<DeltaEncoding>(gaps, stats, plan, out);
}

template<bool Full>
IRS_NO_INLINE uint32_t EncodeValues(const uint32_t* values, uint32_t len,
                                    byte_type* out,
                                    const EncodeOptions& options) noexcept {
  constexpr uint32_t kN = kBlock;
  if constexpr (Full) {
    len = kN;
  }
  uint32_t min = values[0];
  uint32_t max = values[0];
  for (uint32_t i = 0; i != len; ++i) {
    min = std::min(min, values[i]);
    max = std::max(max, values[i]);
  }
  if (min == max) {
    if (max == 0) {
      out[0] = static_cast<byte_type>(ValueEncoding::Zero);
      return 1;
    }
    return WriteSame<ValueEncoding>(max, out);
  }
  const uint32_t* raw = values;
  uint32_t padded[kN];
  if constexpr (!Full) {
    PadTail(values, len, padded);
    raw = padded;
  }
  const Stats<Full> stats{raw, len, static_cast<uint32_t>(std::bit_width(max))};
  return WritePlan<ValueEncoding>(raw, stats, ChoosePlan(stats, max, options),
                                  out);
}

struct BlockCodec {
  static uint32_t EncodeDeltaBlock(const doc_id_t* docs, doc_id_t prev,
                                   byte_type* out,
                                   const EncodeOptions& options = {}) {
    return EncodeDelta<true>(docs, kBlock, prev, out, options);
  }

  static uint32_t EncodeDeltaTail(const doc_id_t* docs, uint32_t len,
                                  doc_id_t prev, byte_type* out,
                                  const EncodeOptions& options = {}) {
    SDB_ASSERT(1 <= len && len < kBlock);
    return EncodeDelta<false>(docs, len, prev, out, options);
  }

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

  static uint32_t EncodeValuesBlock(const uint32_t* values, byte_type* out,
                                    const EncodeOptions& options = {}) {
    return EncodeValues<true>(values, kBlock, out, options);
  }

  static uint32_t EncodeValuesTail(const uint32_t* values, uint32_t len,
                                   byte_type* out,
                                   const EncodeOptions& options = {}) {
    SDB_ASSERT(1 <= len && len < kBlock);
    return EncodeValues<false>(values, len, out, options);
  }

  template<uint32_t Add = 0>
  IRS_FORCE_INLINE static const byte_type* DecodeValuesBlock(
    const byte_type* in, uint32_t* out) {
    return DispatchValues<Add, true>(in, kBlock, out);
  }

  template<uint32_t Add = 0>
  IRS_FORCE_INLINE static const byte_type* DecodeValuesTail(const byte_type* in,
                                                            uint32_t len,
                                                            uint32_t* out) {
    SDB_ASSERT(1 <= len && len < kBlock);
    return DispatchValues<Add, false>(in, len, out);
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
