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

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <limits>

#include "iresearch/formats/posting/block_codec.hpp"
#include "iresearch/formats/posting/block_io.hpp"

namespace irs::block_codec {

inline constexpr uint32_t kMaxExceptions =
  std::numeric_limits<byte_type>::max();
inline constexpr uint32_t kBitsetMarginPercent = 60;
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

struct Plan {
  Family family = Family::Pack;
  uint32_t bits = 0;
  uint32_t count = 0;
  uint32_t high = 0;
  uint32_t size = std::numeric_limits<uint32_t>::max();
};

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
    Resolve(width > kFloatExactWidth,
            [&]<bool Wide> IRS_FORCE_INLINE { Fill<Wide>(values); });
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
  Resolve<ByteAxis<kMaxWidth + 1>>(bits, [&]<uint32_t B>() IRS_FORCE_INLINE {
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
    ForEachSetBit(stats.Mask(w, word), [&](uint32_t bit) IRS_FORCE_INLINE {
      const uint32_t i = word * kMaskBits + bit;
      const uint32_t high = values[i] >> w;
      if (bytes) {
        p[2 * n] = static_cast<byte_type>(i);
        p[2 * n + 1] = static_cast<byte_type>(high);
      } else {
        p[n] = static_cast<byte_type>(i);
        highs[n] = high - 1;
      }
      ++n;
    });
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

struct BlockEncoder {
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
};

}  // namespace irs::block_codec
namespace irs::block_io {

using Encoder = block_codec::BlockEncoder;

inline constexpr block_codec::EncodeOptions kFreqOptions{
  .exception_cost_eighths = 12,
  .narrow_highs = true,
};

template<typename Output>
void WriteSlack(Output& out) {
  static constexpr byte_type kZeros[block_codec::kInSlack]{};
  out.WriteData(kZeros, sizeof(kZeros));
}

template<typename Output>
IRS_FORCE_INLINE void WriteTailDelta(uint32_t len, Output& out,
                                     const uint32_t* in, uint32_t prev,
                                     uint32_t* buf) {
  SDB_ASSERT(1 <= len && len <= kBlock);
  SDB_ASSERT(std::is_sorted(in, in + len));
  SDB_ASSERT(std::adjacent_find(in, in + len) == in + len);
  SDB_ASSERT(prev < in[0]);
  auto* const bytes = reinterpret_cast<byte_type*>(buf);
  out.WriteData(bytes, len == kBlock
                         ? Encoder::EncodeDeltaBlock(in, prev, bytes)
                         : Encoder::EncodeDeltaTail(in, len, prev, bytes));
}

IRS_FORCE_INLINE inline uint32_t EncodeBlock(
  const uint32_t* in, uint32_t* buf,
  const block_codec::EncodeOptions& options = {}) {
  return Encoder::EncodeValuesBlock(in, reinterpret_cast<byte_type*>(buf),
                                    options);
}

template<typename Output>
IRS_FORCE_INLINE void WriteTail(
  uint32_t len, Output& out, const uint32_t* in, uint32_t* buf,
  const block_codec::EncodeOptions& options = {}) {
  SDB_ASSERT(1 <= len && len <= kBlock);
  auto* const bytes = reinterpret_cast<byte_type*>(buf);
  out.WriteData(bytes, len == kBlock
                         ? Encoder::EncodeValuesBlock(in, bytes, options)
                         : Encoder::EncodeValuesTail(in, len, bytes, options));
}

}  // namespace irs::block_io
