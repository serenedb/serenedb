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
#else
#define IRS_BLOCK_CODEC_AVX512
#endif

enum class DeltaEncoding : byte_type {
  Run = 0,
  Same08,
  Same16,
  Same32,
  Bitset,
  Pack,
  Patch16 = Pack + kMaxWidth,
  Patch32 = Patch16 + kMaxWidth,
  PatchMixed = Patch32 + kMaxWidth,
  PatchBitmap = PatchMixed + kMaxWidth,
  End = PatchBitmap + kMaxWidth,
};

inline constexpr uint32_t kMinusOneBits = 8;

enum class ValueEncoding : byte_type {
  One = 0,
  Same08,
  Same16,
  Same32,
  Pack,
  Patch16 = Pack + kMaxWidth,
  Patch32 = Patch16 + kMaxWidth,
  PatchMixed = Patch32 + kMaxWidth,
  PatchBitmap = PatchMixed + kMaxWidth,
  PackMinusOne = PatchBitmap + kMaxWidth,
  Patch16MinusOne = PackMinusOne + kMinusOneBits,
  PatchMixedMinusOne = Patch16MinusOne + kMinusOneBits,
  PatchBitmapMinusOne = PatchMixedMinusOne + kMinusOneBits,
  End = PatchBitmapMinusOne + kMinusOneBits,
};

inline constexpr uint32_t kInSlack = 16;
inline constexpr uint32_t kOutSlack = 8;

struct EncodeOptions {
  uint32_t bitset_margin_percent = 28;
  uint32_t bitmap_margin_percent = 25;
  uint32_t min_patch_saving = 1;
  bool patch16 = true;
  bool patch32 = true;
  bool patch_mixed = true;
  bool patch_bitmap = true;
  bool minus_one = true;
  bool bitset = true;
};

inline constexpr uint32_t kMaskBits = BitsRequired<uint64_t>();

inline constexpr uint32_t PaddedLen(uint32_t len) noexcept {
  return (len + kMaskBits - 1) / kMaskBits * kMaskBits;
}

inline IRS_FORCE_INLINE U32x8 LoadFirst(const uint32_t* in,
                                        uint32_t count) noexcept {
  SDB_ASSERT(count < kWideLanes);
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
  for (; i + kWideLanes <= len; i += kWideLanes) {
    U32x8 v;
    std::memcpy(&v, in + i, sizeof(v));
    v = Opaque(v);
    std::memcpy(out + i, &v, sizeof(v));
  }
  if (i != len) {
    const U32x8 v = LoadFirst(in + i, len - i);
    std::memcpy(out + i, &v, sizeof(v));
    i += kWideLanes;
  }
  const U32x8 zero = Opaque(U32x8{});
  for (const uint32_t end = PaddedLen(len); i != end; i += kWideLanes) {
    std::memcpy(out + i, &zero, sizeof(zero));
  }
}
inline constexpr uint32_t kMaxCount = std::numeric_limits<byte_type>::max();

template<typename E>
constexpr uint32_t Code(E e) noexcept {
  return static_cast<uint32_t>(e);
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
  Patch16,
  Patch32,
  PatchMixed,
  PatchBitmap,
};

template<uint32_t L>
struct Layout {
  static constexpr uint32_t kBlock = kBlockOf<L>;
  static constexpr uint32_t kSlotBits = kSlotBitsOf<L>;
  static constexpr uint32_t kHigh16 = 16 - kSlotBits;
  static constexpr uint32_t kHigh32 = 32 - kSlotBits;
  static constexpr uint32_t kMaxBitsetWords = kBlock / 2;
  static constexpr uint32_t kMaskWords = kBlock / kMaskBits;
  static constexpr uint32_t kBitmapBytes = kBlock / 8;
};

struct Plan {
  Family family = Family::Pack;
  uint32_t bits = 0;
  uint32_t add = 0;
  uint32_t count16 = 0;
  uint32_t count32 = 0;
  uint32_t high = 0;
  uint32_t size = std::numeric_limits<uint32_t>::max();
};

struct TokenShape {
  Family family = Family::Pack;
  uint32_t bits = 0;
  uint32_t add = 0;
};

template<typename Encoding>
constexpr TokenShape ShapeOf(uint32_t token) noexcept {
  if constexpr (std::is_same_v<Encoding, ValueEncoding>) {
    if (token >= Code(ValueEncoding::PatchBitmapMinusOne)) {
      return {Family::PatchBitmap,
              token - Code(ValueEncoding::PatchBitmapMinusOne), 1};
    }
    if (token >= Code(ValueEncoding::PatchMixedMinusOne)) {
      return {Family::PatchMixed,
              token - Code(ValueEncoding::PatchMixedMinusOne), 1};
    }
    if (token >= Code(ValueEncoding::Patch16MinusOne)) {
      return {Family::Patch16, token - Code(ValueEncoding::Patch16MinusOne), 1};
    }
    if (token >= Code(ValueEncoding::PackMinusOne)) {
      return {Family::Pack, token - Code(ValueEncoding::PackMinusOne) + 1, 1};
    }
  }
  if (token >= Code(Encoding::PatchBitmap)) {
    return {Family::PatchBitmap, token - Code(Encoding::PatchBitmap), 0};
  }
  if (token >= Code(Encoding::PatchMixed)) {
    return {Family::PatchMixed, token - Code(Encoding::PatchMixed), 0};
  }
  if (token >= Code(Encoding::Patch32)) {
    return {Family::Patch32, token - Code(Encoding::Patch32), 0};
  }
  if (token >= Code(Encoding::Patch16)) {
    return {Family::Patch16, token - Code(Encoding::Patch16), 0};
  }
  return {Family::Pack, token - Code(Encoding::Pack) + 1, 0};
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
  const auto bytes = std::bit_cast<U8x32>(lanes);
  const U8x16 lo = __builtin_shufflevector(bytes, bytes, 0, 1, 2, 3, 4, 5, 6, 7,
                                           8, 9, 10, 11, 12, 13, 14, 15);
  const U8x16 hi =
    __builtin_shufflevector(bytes, bytes, 16, 17, 18, 19, 20, 21, 22, 23, 24,
                            25, 26, 27, 28, 29, 30, 31);
  return uint32_t{__builtin_reduce_add(lo)} + __builtin_reduce_add(hi);
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

inline IRS_FORCE_INLINE I8x8 PowersOfTwo(const uint32_t* values) noexcept {
  I32x8 lanes;
  std::memcpy(&lanes, values, sizeof(lanes));
  const I32x8 less = lanes - 1;
  return __builtin_convertvector((lanes ^ less) > less, I8x8);
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

template<uint32_t L, bool Full>
class Stats {
 public:
  static constexpr uint32_t kVectors = kBlockOf<L> / sizeof(I8x32);
  static constexpr uint32_t kNoWidth = std::numeric_limits<uint32_t>::max();

  IRS_FORCE_INLINE Stats(const uint32_t* values, uint32_t len, uint32_t width,
                         uint32_t less_width) noexcept
    : _len{len}, _width{width, less_width} {
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
  bool HasLess() const noexcept { return _width[1] != kNoWidth; }
  uint32_t Width(bool less) const noexcept { return _width[less]; }

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

  IRS_FORCE_INLINE uint32_t Above(uint32_t bits, bool less) const noexcept {
    SDB_ASSERT(bits < _width[less]);
    const auto threshold = static_cast<int8_t>(bits);
    if constexpr (Full) {
      I8x32 count{};
      for (const auto& widths : _widths) {
        count -= widths[less] > threshold;
      }
      return SumBytes(count);
    } else {
      uint32_t count = 0;
      for (uint32_t j = 0; j != Vectors(); ++j) {
        count += static_cast<uint32_t>(
          std::popcount(MoveMask8(_widths[j][less] > threshold)));
      }
      return count;
    }
  }

  IRS_FORCE_INLINE uint32_t Powers() const noexcept {
    SDB_ASSERT(HasLess());
    I8x32 count{};
    for (uint32_t j = 0; j != Vectors(); ++j) {
      count += _widths[j][0] - _widths[j][1];
    }
    return SumBytes(count);
  }

  uint64_t Mask(uint32_t bits, uint32_t word, bool less) const noexcept {
    SDB_ASSERT(bits < kMaxWidth);
    const auto threshold = static_cast<int8_t>(bits);
    return MoveMask8(_widths[2 * word][less] > threshold) |
           uint64_t{MoveMask8(_widths[2 * word + 1][less] > threshold)} << 32;
  }

 private:
  template<bool Exact>
  IRS_FORCE_INLINE void Fill(const uint32_t* values) noexcept {
    for (uint32_t j = 0; j != Vectors(); ++j) {
      const auto* p = values + j * sizeof(I8x32);
      _widths[j][0] = Gather32(p, &BitWidths<Exact>);
      if (_width[1] != kNoWidth) {
        _widths[j][1] = _widths[j][0] + Gather32(p, &PowersOfTwo);
      }
    }
    if constexpr (!Full) {
      if (_vectors % 2 != 0) {
        _widths[_vectors][0] = I8x32{};
        _widths[_vectors][1] = I8x32{};
      }
    }
  }

  uint32_t _len;
  uint32_t _vectors = kVectors;
  uint32_t _width[2];
  I8x32 _widths[kVectors][2];
};

template<uint32_t L, bool Full, bool All = false>
class Planner {
 public:
  IRS_FORCE_INLINE Planner(uint32_t len, uint32_t width,
                           const EncodeOptions& options, bool less,
                           uint32_t max_bits) noexcept
    : _len{len},
      _width{width},
      _max_bits{max_bits},
      _min_saving{options.min_patch_saving},
      _patch16{options.patch16},
      _patch32{options.patch32 && !less},
      _mixed{options.patch_mixed},
      _bitmap{Full && options.patch_bitmap},
      _less{less} {
    using Shape = Layout<L>;
    if (width <= max_bits) {
      _pack_size = 1 + Packed(width);
    }
    _above_at[width] = 0;
    if constexpr (!Full) {
      _lo16 = width > Shape::kHigh16 ? width - Shape::kHigh16 : 0;
      _floor16 = Packed(_lo16);
      _floor = Packed(width > Shape::kHigh32 ? width - Shape::kHigh32 : 0);
    }
  }

  bool Done() const noexcept { return _done; }
  uint32_t Width() const noexcept { return _width; }

  IRS_FORCE_INLINE void Level(uint32_t bits, uint32_t above) noexcept {
    using Shape = Layout<L>;
    const uint32_t high = _width - bits;
    if (high > Shape::kHigh32) {
      _done = true;
      return;
    }
    _above_at[bits] = above;
    const uint32_t packed = Packed(bits);
    const bool narrow = bits < _max_bits;
    if (All && high <= Shape::kHigh16 && narrow && above <= kMaxCount) {
      Try(Family::Patch16, bits, 2 + packed + 2 * above);
    } else {
      const uint32_t above16 =
        bits + Shape::kHigh16 <= _width ? _above_at[bits + Shape::kHigh16] : 0;
      if (above <= kMaxCount) {
        if (!All && _patch16 && high <= Shape::kHigh16 && narrow) {
          Try(Family::Patch16, bits, 2 + packed + 2 * above);
        }
        if (_patch32) {
          Try(Family::Patch32, bits, 2 + packed + 4 * above);
        }
      }
      if (_mixed && above16 != 0 && above16 != above && narrow &&
          above - above16 <= kMaxCount && above16 <= kMaxCount) {
        Try(Family::PatchMixed, bits,
            3 + packed + 2 * (above - above16) + 4 * above16);
      }
    }
    const uint32_t best = std::min(_pack_size, _patch_size);
    if constexpr (Full) {
      if (_bitmap && narrow) {
        const uint32_t size =
          2 + packed + Shape::kBitmapBytes + (above * high + 7) / 8;
        if (size < _sparse_size) {
          _sparse_size = size;
          _sparse_bits = bits;
        }
      }
      _done = 2 + 2 * above >= best;
    } else {
      uint32_t lower = kNone;
      if (_patch16 && bits > _lo16) {
        lower = 2 + _floor16 + 2 * above;
      }
      if (_patch32) {
        lower = std::min(lower, 2 + _floor + 4 * above);
      }
      if (_mixed) {
        const uint32_t probe = bits + Shape::kHigh16 - 1;
        const uint32_t wide = probe < _width ? _above_at[probe] : 0;
        lower =
          std::min(lower, 3 + _floor + 2 * above + 2 * std::max(wide, 1U));
      }
      _done = lower >= best;
    }
  }

  IRS_FORCE_INLINE Plan Regular() const noexcept {
    using Shape = Layout<L>;
    Plan plan{.family = Family::Pack, .bits = _width, .size = _pack_size};
    if (_patch_size < _pack_size && _pack_size - _patch_size >= _min_saving) {
      const uint32_t bits = _patch_code / kFamilies;
      const auto family = static_cast<Family>(_patch_code % kFamilies);
      const uint32_t above = _above_at[bits];
      plan = {.family = family, .bits = bits, .size = _patch_size};
      if (family == Family::Patch16) {
        plan.count16 = above;
      } else if (family == Family::Patch32) {
        plan.count32 = above;
      } else {
        plan.count32 = _above_at[bits + Shape::kHigh16];
        plan.count16 = above - plan.count32;
      }
    }
    plan.add = _less;
    return plan;
  }

  IRS_FORCE_INLINE Plan Sparse() const noexcept {
    return {
      .family = Family::PatchBitmap,
      .bits = _sparse_bits,
      .add = _less,
      .high = _width - _sparse_bits,
      .size = _sparse_size,
    };
  }

  static IRS_FORCE_INLINE Plan Pick(const Plan& regular, const Plan& sparse,
                                    uint32_t keep) noexcept {
    return uint64_t{sparse.size} * 100 < uint64_t{regular.size} * keep
             ? sparse
             : regular;
  }

 private:
  static constexpr uint32_t kNone = std::numeric_limits<uint32_t>::max();
  static constexpr uint32_t kFamilies = 8;

  IRS_FORCE_INLINE uint32_t Packed(uint32_t bits) const noexcept {
    if constexpr (Full) {
      return PackedSize<L>(kBlockOf<L>, bits);
    } else {
      return (_len * bits + 7) / 8;
    }
  }

  IRS_FORCE_INLINE void Try(Family family, uint32_t bits,
                            uint32_t size) noexcept {
    if (size < _patch_size) {
      _patch_size = size;
      _patch_code = bits * kFamilies + static_cast<uint32_t>(family);
    }
  }

  uint32_t _pack_size = kNone;
  uint32_t _patch_size = kNone;
  uint32_t _patch_code = 0;
  uint32_t _sparse_size = kNone;
  uint32_t _sparse_bits = 0;
  uint32_t _len;
  uint32_t _width;
  uint32_t _max_bits;
  uint32_t _min_saving;
  uint32_t _lo16 = 0;
  uint32_t _floor16 = 0;
  uint32_t _floor = 0;
  bool _patch16;
  bool _patch32;
  bool _mixed;
  bool _bitmap;
  bool _less;
  bool _done = false;
  uint32_t _above_at[kMaxWidth + 1];
};

struct Plans {
  Plan regular;
  Plan sparse;
};

inline IRS_FORCE_INLINE uint32_t TailPacked(uint32_t len,
                                            uint32_t bits) noexcept {
  return (len * bits + 7) / 8;
}

template<uint32_t L, typename Above>
IRS_FORCE_INLINE uint32_t TailPatchBound(uint32_t len, uint32_t width,
                                         uint32_t limit,
                                         Above&& above) noexcept {
  using Shape = Layout<L>;
  const uint32_t top = above(width - 1);
  uint32_t bound = 2 + TailPacked(len, width > 2 ? width - 2 : 0) + 2 * top;
  if (bound < limit) {
    bound = 2 + TailPacked(len, width - 1) + 2 * top;
    if (width != 1) {
      bound =
        std::min(bound, 2 + TailPacked(len, width - 2) + 2 * above(width - 2));
    }
  }
  if (width <= 2) {
    return bound;
  }
  const uint32_t lo16 = width > Shape::kHigh16 ? width - Shape::kHigh16 : 0;
  bound = std::min(bound, 2 + TailPacked(len, lo16) + 2 * above(width - 3));
  const uint32_t lo = width > Shape::kHigh32 ? width - Shape::kHigh32 : 0;
  if (lo16 > lo) {
    const uint32_t low = above(lo16 - 1);
    bound = std::min(bound, TailPacked(len, lo) +
                              std::min(2 + 4 * low, 3 + 2 * low + 2 * top));
  }
  return bound;
}

template<uint32_t L, bool Full>
Plans ChoosePlans(const Stats<L, Full>& stats, const EncodeOptions& options,
                  bool less) noexcept {
  const uint32_t width = stats.Width(less);
  const uint32_t max_bits = less ? kMinusOneBits : kMaxWidth;
  if (!Full && width <= max_bits) {
    const uint32_t pack = 1 + PackedSize<L>(stats.Len(), width);
    const uint32_t bound = TailPatchBound<L>(
      stats.Len(), width, pack,
      [&](uint32_t bits) IRS_FORCE_INLINE { return stats.Above(bits, less); });
    if (bound >= pack) {
      return {
        .regular = {.family = Family::Pack,
                    .bits = width,
                    .add = less,
                    .size = pack},
        .sparse = {.family = Family::PatchBitmap,
                   .add = less,
                   .high = width,
                   .size = std::numeric_limits<uint32_t>::max()},
      };
    }
  }
  const auto walk = [&]<bool All>() IRS_FORCE_INLINE {
    Planner<L, Full, All> planner{stats.Len(), width, options, less, max_bits};
    for (uint32_t bits = planner.Width(); bits-- != 0 && !planner.Done();) {
      planner.Level(bits, stats.Above(bits, less));
    }
    return Plans{planner.Regular(), planner.Sparse()};
  };
  if (Full && options.patch16 && options.patch32 && options.patch_mixed &&
      options.patch_bitmap) {
    return walk.template operator()<true>();
  }
  return walk.template operator()<false>();
}

inline uint32_t BitmapKeep(const EncodeOptions& options) noexcept {
  return 100 - std::min(options.bitmap_margin_percent, 100U);
}

template<uint32_t L, bool Full>
Plan ChoosePlan(const Stats<L, Full>& stats,
                const EncodeOptions& options) noexcept {
  const auto [regular, sparse] = ChoosePlans(stats, options, false);
  return Planner<L, Full>::Pick(regular, sparse, BitmapKeep(options));
}

template<uint32_t L, bool Full, typename Above>
IRS_FORCE_INLINE uint32_t MinusOneBound(uint32_t len, uint32_t width,
                                        uint32_t limit,
                                        Above&& above) noexcept {
  using Shape = Layout<L>;
  constexpr uint32_t kSplit = kMinusOneBits / 2;
  const uint32_t top = std::min(width, kMinusOneBits) - 1;
  const uint32_t exceptions = above(top);
  uint32_t bound = 2 + 2 * exceptions;
  if (top >= kSplit) {
    bound = std::min(2 + PackedSize<L>(len, kSplit) + 2 * exceptions,
                     2 + 2 * above(kSplit - 1));
  }
  if (width <= kMinusOneBits) {
    return std::min(bound, 1 + PackedSize<L>(len, width));
  }
  if constexpr (!Full) {
    if (bound < limit && width > top + Shape::kHigh16) {
      bound = std::max(bound, 3 + 2 * exceptions +
                                2 * std::max(above(top + Shape::kHigh16), 1U));
    }
  }
  return bound;
}

template<uint32_t L, typename Bound>
IRS_FORCE_INLINE bool RawLoses(const Stats<L, true>& stats, const Plans& less,
                               uint32_t keep, Bound&& bound,
                               const EncodeOptions& options) noexcept {
  using Shape = Layout<L>;
  const uint32_t best = less.regular.size;
  const uint32_t spare = less.sparse.size;
  uint32_t regular_floor = best;
  uint32_t sparse_floor = 0;
  if (options.patch_bitmap) {
    sparse_floor = (best * keep + 99) / 100;
    if (spare < sparse_floor) {
      regular_floor = std::max(bound(), spare * 100 / keep);
      sparse_floor = spare + 1;
    }
  }
  const uint32_t width = stats.Width(false);
  if (1 + PackedSize<L>(kBlockOf<L>, width) <= regular_floor) {
    return false;
  }
  for (uint32_t bits = 0; bits != width; ++bits) {
    const uint32_t packed = PackedSize<L>(kBlockOf<L>, bits);
    const bool regular = 4 + packed <= regular_floor;
    const bool bitmap = 3 + packed + Shape::kBitmapBytes < sparse_floor;
    if (!regular && !bitmap) {
      return true;
    }
    const uint32_t above = stats.Above(bits, false);
    const uint32_t high = width - bits;
    if (regular) {
      uint32_t size = 2 + packed + 2 * above;
      if (size <= regular_floor && high > Shape::kHigh16) {
        size = std::min(2 + packed + 4 * above,
                        3 + packed + 2 * above +
                          2 * stats.Above(bits + Shape::kHigh16, false));
      }
      if (size <= regular_floor) {
        return false;
      }
    }
    if (bitmap && 2 + packed + Shape::kBitmapBytes + (above * high + 7) / 8 <
                    sparse_floor) {
      return false;
    }
  }
  return true;
}

template<uint32_t L, bool Full>
Plan ChooseValuePlan(const Stats<L, Full>& stats,
                     const EncodeOptions& options) noexcept {
  const uint32_t keep = BitmapKeep(options);
  const auto less_above = [&](uint32_t bits) IRS_FORCE_INLINE {
    return stats.Above(bits, true);
  };
  if constexpr (Full) {
    if (stats.HasLess() && (stats.Width(true) < stats.Width(false) ||
                            stats.Powers() >= kBlockOf<L> / 4)) {
      const auto less = ChoosePlans(stats, options, true);
      const auto bound = [&] IRS_FORCE_INLINE {
        return MinusOneBound<L, Full>(stats.Len(), stats.Width(true),
                                      less.regular.size, less_above);
      };
      if (RawLoses(stats, less, keep, bound, options)) {
        return Planner<L, Full>::Pick(less.regular, less.sparse, keep);
      }
      auto [regular, sparse] = ChoosePlans(stats, options, false);
      if (bound() < regular.size) {
        if (less.regular.size < regular.size) {
          regular = less.regular;
        }
        if (less.sparse.size < sparse.size) {
          sparse = less.sparse;
        }
      }
      return Planner<L, Full>::Pick(regular, sparse, keep);
    }
  }
  auto [regular, sparse] = ChoosePlans(stats, options, false);
  if (!stats.HasLess()) {
    return Planner<L, Full>::Pick(regular, sparse, keep);
  }
  if (MinusOneBound<L, Full>(stats.Len(), stats.Width(true), regular.size,
                             less_above) < regular.size) {
    const auto less = ChoosePlans(stats, options, true);
    if (less.regular.size < regular.size) {
      regular = less.regular;
    }
    if (less.sparse.size < sparse.size) {
      sparse = less.sparse;
    }
  }
  return Planner<L, Full>::Pick(regular, sparse, keep);
}

template<uint32_t B, bool Full, uint32_t L, bool Mask>
IRS_NO_INLINE void PackWidth(const uint32_t* in, uint32_t len,
                             byte_type* out) noexcept {
  if constexpr (Full) {
    PackVertical<B, L, Mask>(in, out);
  } else {
    PackHorizontal<B>(in, len, out);
  }
}

template<bool Full, uint32_t L, bool Mask>
void PackBits(uint32_t bits, const uint32_t* in, uint32_t len,
              byte_type* out) noexcept {
  ResolveByte<kMaxWidth + 1>(bits, [&]<uint32_t B>() IRS_FORCE_INLINE {
    PackWidth<B, Full, L, Mask>(in, len, out);
  });
}

inline constexpr uint32_t kDenseExceptions = 32;

template<typename Chunk>
IRS_FORCE_INLINE uint32_t LeftPackChunks(const uint64_t* masks, uint32_t words,
                                         Chunk&& chunk) noexcept {
  uint32_t count = 0;
  for (uint32_t w = 0; w != words; ++w) {
    for (uint32_t c = 0; c != kMaskBits / kWideLanes; ++c) {
      const auto mask =
        static_cast<uint32_t>(masks[w] >> (c * kWideLanes)) & 0xFF;
      chunk(w * kMaskBits + c * kWideLanes, mask, count);
      count += static_cast<uint32_t>(std::popcount(mask));
    }
  }
  return count;
}

template<typename Entry, uint32_t L, bool Full>
byte_type* WriteEntries(const uint32_t* values, const uint64_t* masks,
                        uint32_t words, uint32_t bits, uint32_t count,
                        byte_type* out) noexcept {
  if constexpr (Full) {
    words = Layout<L>::kMaskWords;
  }
  if (count < kDenseExceptions) {
    for (uint32_t w = 0; w != words; ++w) {
      for (auto mask = masks[w]; mask != 0; mask &= mask - 1) {
        const uint32_t i =
          w * kMaskBits + static_cast<uint32_t>(std::countr_zero(mask));
        absl::little_endian::Store<Entry>(
          out, static_cast<Entry>(
                 i | ((values[i] >> bits) << Layout<L>::kSlotBits)));
        out += sizeof(Entry);
      }
    }
    return out;
  }
  constexpr U32x8 kIndex = {0, 1, 2, 3, 4, 5, 6, 7};
  Entry entries[Layout<L>::kBlock + kWideLanes];
  LeftPackChunks(
    masks, words,
    [&](uint32_t base, uint32_t mask, uint32_t at) IRS_FORCE_INLINE {
      U32x8 v;
      std::memcpy(&v, values + base, sizeof(v));
      const U32x8 packed =
        LeftPack((kIndex + base) | ((v >> bits) << Layout<L>::kSlotBits), mask);
      if constexpr (sizeof(Entry) == sizeof(uint32_t)) {
        std::memcpy(entries + at, &packed, sizeof(packed));
      } else {
        const auto narrow = __builtin_convertvector(
          packed, uint16_t __attribute__((vector_size(16))));
        std::memcpy(entries + at, &narrow, sizeof(narrow));
      }
    });
  std::memcpy(out, entries, count * sizeof(Entry));
  return out + count * sizeof(Entry);
}

template<bool Full, uint32_t L>
uint32_t WritePlan(const uint32_t* values, const Stats<L, Full>& stats,
                   const Plan& plan, uint32_t token, byte_type* out) noexcept {
  const uint32_t len = Full ? kBlockOf<L> : stats.Len();
  auto* p = out;
  *p++ = static_cast<byte_type>(token);
  if (plan.family == Family::Patch16) {
    *p++ = static_cast<byte_type>(plan.count16);
  } else if (plan.family == Family::Patch32) {
    *p++ = static_cast<byte_type>(plan.count32);
  } else if (plan.family == Family::PatchMixed) {
    *p++ = static_cast<byte_type>(plan.count16);
    *p++ = static_cast<byte_type>(plan.count32);
  } else if (plan.family == Family::PatchBitmap) {
    *p++ = static_cast<byte_type>(plan.high);
  }
  if (!Full || plan.family != Family::Pack) {
    PackBits<Full, L, true>(plan.bits, values, len, p);
  } else {
    PackBits<Full, L, false>(plan.bits, values, len, p);
  }
  p += PackedSize<L>(len, plan.bits);
  const uint32_t words = stats.Words();
  if (plan.family == Family::PatchBitmap) {
    uint64_t masks[Layout<L>::kMaskWords];
    for (uint32_t w = 0; w != words; ++w) {
      masks[w] = stats.Mask(plan.bits, w, plan.add != 0);
      absl::little_endian::Store64(p + w * sizeof(uint64_t), masks[w]);
    }
    uint32_t highs[Layout<L>::kBlock + kWideLanes];
    const uint32_t count = LeftPackChunks(
      masks, words,
      [&](uint32_t base, uint32_t mask, uint32_t at) IRS_FORCE_INLINE {
        U32x8 v;
        std::memcpy(&v, values + base, sizeof(v));
        const U32x8 packed = LeftPack(v >> plan.bits, mask);
        std::memcpy(highs + at, &packed, sizeof(packed));
      });
    p += Layout<L>::kBitmapBytes;
    PackBits<false, L, true>(plan.high, highs, count, p);
    p += (count * plan.high + 7) / 8;
  } else if (plan.family != Family::Pack) {
    const bool less = plan.add != 0;
    uint64_t all[Layout<L>::kMaskWords];
    for (uint32_t w = 0; w != words; ++w) {
      all[w] = stats.Mask(plan.bits, w, less);
    }
    if (plan.family == Family::Patch16) {
      p = WriteEntries<uint16_t, L, Full>(values, all, words, plan.bits,
                                          plan.count16, p);
    } else if (plan.family == Family::Patch32) {
      p = WriteEntries<uint32_t, L, Full>(values, all, words, plan.bits,
                                          plan.count32, p);
    } else {
      uint64_t wide[Layout<L>::kMaskWords];
      for (uint32_t w = 0; w != words; ++w) {
        wide[w] = stats.Mask(plan.bits + Layout<L>::kHigh16, w, less);
        all[w] &= ~wide[w];
      }
      p = WriteEntries<uint16_t, L, Full>(values, all, words, plan.bits,
                                          plan.count16, p);
      p = WriteEntries<uint32_t, L, Full>(values, wide, words, plan.bits,
                                          plan.count32, p);
    }
  }
  SDB_ASSERT(static_cast<uint32_t>(p - out) == plan.size);
  return plan.size;
}

template<typename Encoding>
uint32_t PlanToken(const Plan& plan) noexcept {
  if constexpr (std::is_same_v<Encoding, ValueEncoding>) {
    if (plan.add != 0) {
      switch (plan.family) {
        case Family::Pack:
          return Code(ValueEncoding::PackMinusOne) + plan.bits - 1;
        case Family::Patch16:
          return Code(ValueEncoding::Patch16MinusOne) + plan.bits;
        case Family::PatchMixed:
          return Code(ValueEncoding::PatchMixedMinusOne) + plan.bits;
        case Family::PatchBitmap:
          return Code(ValueEncoding::PatchBitmapMinusOne) + plan.bits;
        case Family::Patch32:
          break;
      }
      SDB_UNREACHABLE();
    }
  }
  switch (plan.family) {
    case Family::Pack:
      return Code(Encoding::Pack) + plan.bits - 1;
    case Family::Patch16:
      return Code(Encoding::Patch16) + plan.bits;
    case Family::Patch32:
      return Code(Encoding::Patch32) + plan.bits;
    case Family::PatchMixed:
      return Code(Encoding::PatchMixed) + plan.bits;
    case Family::PatchBitmap:
      return Code(Encoding::PatchBitmap) + plan.bits;
  }
  SDB_UNREACHABLE();
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

template<bool Full, uint32_t L>
uint32_t WriteBitset(const doc_id_t* docs, uint32_t len, doc_id_t prev,
                     uint32_t words, byte_type* out) noexcept {
  if constexpr (Full) {
    len = kBlockOf<L>;
  }
  out[0] = static_cast<byte_type>(DeltaEncoding::Bitset);
  out[1] = static_cast<byte_type>(words);
  auto* bits = out + 2;
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
  for (; i + kWideLanes <= len; i += kWideLanes) {
    U32x8 v;
    std::memcpy(&v, docs + i, sizeof(v));
    v -= base;
    const uint32_t first = v[0];
    const U32x8 offsets = v - first;
    if (offsets[kWideLanes - 1] < BitsRequired<uint64_t>()) {
      const U64x4 one = U64x4{} + 1;
      const U64x4 masks =
        (one << __builtin_convertvector(
           __builtin_shufflevector(offsets, offsets, 0, 1, 2, 3), U64x4)) |
        (one << __builtin_convertvector(
           __builtin_shufflevector(offsets, offsets, 4, 5, 6, 7), U64x4));
      deposit(first, masks[0] | masks[1] | masks[2] | masks[3]);
    } else {
      for (uint32_t k = 0; k != kWideLanes; ++k) {
        deposit(v[k], 1);
      }
    }
  }
  for (; i != len; ++i) {
    deposit(docs[i] - base, 1);
  }
  return 2 + words * sizeof(uint64_t);
}

inline IRS_FORCE_INLINE void FillProgression(doc_id_t* out, uint32_t len,
                                             doc_id_t prev,
                                             uint32_t gap) noexcept {
  for (uint32_t i = 0; i != len; ++i) {
    out[i] = prev + gap * (i + 1);
  }
}

template<uint32_t B, uint32_t Add, bool Full, uint32_t L>
IRS_FORCE_INLINE void Unpack(const byte_type* in, uint32_t len,
                             uint32_t* out) noexcept {
  if constexpr (Full && L == kWideLanes) {
    UnpackWide<B, Add>(in, out);
  } else if constexpr (Full) {
    UnpackVertical<B, Add, L>(in, out);
  } else {
    UnpackHorizontal<B, Add>(in, len, out);
  }
}

struct Patches {
  uint32_t count16;
  uint32_t count32;
  const byte_type* packed;
};

template<typename Encoding>
IRS_FORCE_INLINE Patches ReadPatches(const byte_type* in) noexcept {
  const auto family = ShapeOf<Encoding>(in[0]).family;
  const uint32_t has16 =
    family == Family::Patch16 || family == Family::PatchMixed;
  const uint32_t has32 =
    family == Family::Patch32 || family == Family::PatchMixed;
  return {
    .count16 = in[1] & (0 - has16),
    .count32 = in[1 + has16] & (0 - has32),
    .packed = in + 1 + has16 + has32,
  };
}

template<Family F>
IRS_FORCE_INLINE Patches ReadPatches(const byte_type* in) noexcept {
  if constexpr (F == Family::Pack) {
    return {.count16 = 0, .count32 = 0, .packed = in + 1};
  } else if constexpr (F == Family::Patch16) {
    return {.count16 = in[1], .count32 = 0, .packed = in + 2};
  } else if constexpr (F == Family::Patch32) {
    return {.count16 = 0, .count32 = in[1], .packed = in + 2};
  } else {
    return {.count16 = in[1], .count32 = in[2], .packed = in + 3};
  }
}

template<uint32_t L>
IRS_FORCE_INLINE uint32_t BitmapCount(const byte_type* bitmap) noexcept {
  uint32_t count = 0;
  for (uint32_t w = 0; w != Layout<L>::kMaskWords; ++w) {
    count += static_cast<uint32_t>(std::popcount(
      absl::little_endian::Load64(bitmap + w * sizeof(uint64_t))));
  }
  return count;
}

template<typename Encoding, uint32_t L>
uint32_t PackedBlockSize(const byte_type* in, uint32_t len) noexcept {
  const auto shape = ShapeOf<Encoding>(in[0]);
  if (shape.family == Family::PatchBitmap) {
    const auto* bitmap = in + 2 + PackedSize<L>(len, shape.bits);
    return static_cast<uint32_t>(bitmap - in) + Layout<L>::kBitmapBytes +
           (BitmapCount<L>(bitmap) * in[1] + 7) / 8;
  }
  const auto [count16, count32, packed] = ReadPatches<Encoding>(in);
  return static_cast<uint32_t>(packed - in) + PackedSize<L>(len, shape.bits) +
         count16 * sizeof(uint16_t) + count32 * sizeof(uint32_t);
}

struct SizeShape {
  uint16_t fixed = 0;
  uint8_t bits = 0;
  uint8_t per1 = 0;
  uint8_t per2 = 0;
  bool bitmap = false;
};

template<typename Encoding>
inline constexpr auto kSizeShapes = [] {
  std::array<SizeShape, 256> shapes{};
  for (uint32_t token = 0; token != Code(Encoding::End); ++token) {
    auto& s = shapes[token];
    if (token < Code(Encoding::Pack)) {
      constexpr uint16_t kSame[] = {1, 2, 3, 5};
      if constexpr (std::is_same_v<Encoding, DeltaEncoding>) {
        if (token == Code(DeltaEncoding::Bitset)) {
          s = {.fixed = 2, .per1 = sizeof(uint64_t)};
          continue;
        }
      }
      s.fixed = kSame[token];
      continue;
    }
    const auto shape = ShapeOf<Encoding>(token);
    s.bits = static_cast<uint8_t>(shape.bits);
    switch (shape.family) {
      case Family::Pack:
        s.fixed = 1;
        break;
      case Family::Patch16:
        s.fixed = 2;
        s.per1 = sizeof(uint16_t);
        break;
      case Family::Patch32:
        s.fixed = 2;
        s.per1 = sizeof(uint32_t);
        break;
      case Family::PatchMixed:
        s.fixed = 3;
        s.per1 = sizeof(uint16_t);
        s.per2 = sizeof(uint32_t);
        break;
      case Family::PatchBitmap:
        s.bitmap = true;
        break;
    }
  }
  return shapes;
}();

template<typename Encoding, uint32_t L>
IRS_FORCE_INLINE uint32_t BlockBytes(const byte_type* in,
                                     uint32_t len) noexcept {
  const auto& s = kSizeShapes<Encoding>[in[0]];
  if (s.bitmap) [[unlikely]] {
    return PackedBlockSize<Encoding, L>(in, len);
  }
  return s.fixed + (len * s.bits + 7) / 8 + in[1] * uint32_t{s.per1} +
         in[2] * uint32_t{s.per2};
}

static_assert(kOutSlack >= kPatchGroup);

inline IRS_NO_INLINE void UnpackBits(uint32_t bits, const byte_type* in,
                                     uint32_t len, uint32_t* out) noexcept {
  ResolveByte<kMaxWidth + 1>(bits, [&]<uint32_t B>() IRS_FORCE_INLINE {
    UnpackHorizontal<B, 0>(in, len, out);
  });
}

template<uint32_t B, uint32_t L>
IRS_FORCE_INLINE const byte_type* PatchBitmapValues(const byte_type* bitmap,
                                                    uint32_t high,
                                                    uint32_t* out) noexcept {
  const auto* highs = bitmap + Layout<L>::kBitmapBytes;
  const uint32_t count = BitmapCount<L>(bitmap);
  uint32_t buffer[kBlockOf<L> + kOutSlack];
  UnpackBits(high, highs, count, buffer);
  uint32_t k = 0;
  for (uint32_t w = 0; w != Layout<L>::kMaskWords; ++w) {
    for (auto word = absl::little_endian::Load64(bitmap + w * sizeof(uint64_t));
         word != 0; word &= word - 1) {
      out[w * kMaskBits + std::countr_zero(word)] += buffer[k++] << B;
    }
  }
  return highs + (count * high + 7) / 8;
}

template<uint32_t B, uint32_t Add>
IRS_FORCE_INLINE const byte_type* DecodeBitmapWide(const byte_type* in,
                                                   uint32_t* out) noexcept {
  const uint32_t high = in[1];
  const auto* packed = in + 2;
  const auto* bitmap = packed + PackedSize<kWideLanes>(kWideBlock, B);
  const auto* highs = bitmap + Layout<kWideLanes>::kBitmapBytes;
  const uint32_t count = BitmapCount<kWideLanes>(bitmap);
  uint32_t buffer[kWideBlock + kWideLanes];
  UnpackBits(high, highs, count, buffer);
  std::fill_n(buffer + count, kWideLanes, 0);
  uint32_t k = 0;
  const U32x8 add = U32x8{} + Add;
  WideRows<B>(packed, [&]<uint32_t S>(U32x8 v) IRS_FORCE_INLINE {
    const uint32_t mask = bitmap[S];
    v += add + (ExpandLanes(buffer + k, mask) << B);
    std::memcpy(out + S * kWideLanes, &v, sizeof(v));
    k += static_cast<uint32_t>(std::popcount(mask));
  });
  return highs + (count * high + 7) / 8;
}

template<Family F, uint32_t B, uint32_t Add, bool Full, uint32_t L>
IRS_FORCE_INLINE const byte_type* DecodeBits(const byte_type* in, uint32_t len,
                                             uint32_t* out) noexcept {
  if constexpr (Full) {
    len = kBlockOf<L>;
  }
  if constexpr (F == Family::PatchBitmap && Full && L == kWideLanes) {
    return DecodeBitmapWide<B, Add>(in, out);
  } else if constexpr (F == Family::PatchBitmap) {
    Unpack<B, Add, Full, L>(in + 2, len, out);
    return PatchBitmapValues<B, L>(in + 2 + PackedSize<L>(len, B), in[1], out);
  } else {
    const auto [count16, count32, packed] = ReadPatches<F>(in);
    Unpack<B, Add, Full, L>(packed, len, out);
    const auto* p = packed + PackedSize<L>(len, B);
    if constexpr (F == Family::Patch16 || F == Family::PatchMixed) {
      p = PatchValues<B, uint16_t, L, Full>(p, count16, len, out);
    }
    if constexpr (F == Family::Patch32 || F == Family::PatchMixed) {
      p = PatchValues<B, uint32_t, L, Full>(p, count32, len, out);
    }
    return p;
  }
}

template<typename Encoding, bool Full>
consteval bool Inlined(uint32_t token) noexcept {
  if (token < Code(Encoding::Pack)) {
    return true;
  }
  const auto family = ShapeOf<Encoding>(token).family;
  return family == Family::Pack || (Full && family != Family::PatchMixed &&
                                    family != Family::PatchBitmap);
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

inline IRS_FORCE_INLINE IRS_BLOCK_CODEC_AVX512 doc_id_t* MaterializeWord16(
  doc_id_t offset, uint64_t word, doc_id_t* out) noexcept {
#ifdef __AVX2__
  const __m512i lanes =
    _mm512_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
  for (uint32_t q = 0; q != 4; ++q) {
    const auto mask = static_cast<__mmask16>(word >> (16 * q));
    const auto count = static_cast<uint32_t>(std::popcount(mask));
    const __m512i docs = _mm512_maskz_compress_epi32(
      mask, _mm512_add_epi32(
              lanes, _mm512_set1_epi32(static_cast<int>(offset + 16 * q))));
    _mm512_mask_storeu_epi32(
      out, static_cast<__mmask16>((uint32_t{1} << count) - 1), docs);
    out += count;
  }
  return out;
#else
  return MaterializeScalar(offset, word, out);
#endif
}

inline IRS_FORCE_INLINE IRS_BLOCK_CODEC_AVX512 const byte_type* DecodeBitset16(
  const byte_type* in, uint32_t len, doc_id_t prev, doc_id_t* out) noexcept {
  const uint32_t words = in[1];
  const auto* bits = in + 2;
  auto* p = out;
  for (uint32_t w = 0; w != words; ++w) {
    p = MaterializeWord16(
      prev + 1 + w * BitsRequired<uint64_t>(),
      absl::little_endian::Load64(bits + w * sizeof(uint64_t)), p);
  }
  SDB_ASSERT(p == out + len);
  return bits + words * sizeof(uint64_t);
}

template<uint32_t Token, bool Full, uint32_t L, bool Wide = false>
IRS_FORCE_INLINE const byte_type* DecodeDeltaBody(const byte_type* in,
                                                  uint32_t len, doc_id_t prev,
                                                  doc_id_t* out) noexcept {
  static_assert(!Wide || L == kWideLanes);
  if constexpr (Full) {
    len = kBlockOf<L>;
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
  } else if constexpr (Token == Code(DeltaEncoding::Bitset)) {
    const uint32_t words = in[1];
    const auto* bits = in + 2;
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
      if constexpr (Wide) {
        UnpackVerticalDelta16<kShape.bits>(in + 1, prev, out);
      } else {
        UnpackVerticalDelta<kShape.bits, L>(in + 1, prev, out);
      }
      return in + 1 + PackedSize<L>(kBlockOf<L>, kShape.bits);
    } else if constexpr (kShape.family == Family::Pack) {
      if constexpr (Wide && kShape.bits <= kMaxPackGroupBits) {
        UnpackHorizontalDelta16<kShape.bits>(in + 1, len, prev, out);
      } else {
        UnpackHorizontalDelta<kShape.bits>(in + 1, len, prev, out);
      }
      return in + 1 + PackedSize<L>(len, kShape.bits);
    } else {
      const auto* end =
        DecodeBits<kShape.family, kShape.bits, 0, Full, L>(in, len, out);
      if constexpr (Wide && Full) {
        ScanDocs16(out, prev);
      } else {
        ScanDocs(out, len, prev);
      }
      return end;
    }
  }
}

template<uint32_t Token, bool Full, uint32_t L>
IRS_NO_INLINE const byte_type* DecodeDeltaToken(const byte_type* in,
                                                uint32_t len, doc_id_t prev,
                                                doc_id_t* out) noexcept {
  return DecodeDeltaBody<Token, Full, L>(in, len, prev, out);
}

using DeltaBlockDecoder = const byte_type* (*)(const byte_type*, doc_id_t,
                                               doc_id_t*) noexcept;
using DeltaTailDecoder = const byte_type* (*)(const byte_type*, uint32_t,
                                              doc_id_t, doc_id_t*) noexcept;

template<uint32_t Token, uint32_t L>
IRS_NO_INLINE const byte_type* DecodeDeltaBlockToken(const byte_type* in,
                                                     doc_id_t prev,
                                                     doc_id_t* out) noexcept {
  return DecodeDeltaBody<Token, true, L>(in, kBlockOf<L>, prev, out);
}

template<uint32_t Token>
IRS_NO_INLINE IRS_BLOCK_CODEC_AVX512 const byte_type*
DecodeDeltaBlockTokenAvx512(const byte_type* in, doc_id_t prev,
                            doc_id_t* out) noexcept {
  if constexpr (Token == Code(DeltaEncoding::Bitset)) {
    return DecodeBitset16(in, kWideBlock, prev, out);
  } else {
    return DecodeDeltaBody<Token, true, kWideLanes, true>(in, kWideBlock, prev,
                                                          out);
  }
}

template<uint32_t Token>
IRS_NO_INLINE IRS_BLOCK_CODEC_AVX512 const byte_type*
DecodeDeltaTailTokenAvx512(const byte_type* in, uint32_t len, doc_id_t prev,
                           doc_id_t* out) noexcept {
  if constexpr (Token == Code(DeltaEncoding::Bitset)) {
    return DecodeBitset16(in, len, prev, out);
  } else {
    return DecodeDeltaBody<Token, false, kWideLanes, true>(in, len, prev, out);
  }
}

consteval bool WideTail(uint32_t token) noexcept {
  if (token < Code(DeltaEncoding::Pack)) {
    return token == Code(DeltaEncoding::Bitset);
  }
  const auto shape = ShapeOf<DeltaEncoding>(token);
  return shape.family == Family::Pack && shape.bits <= kMaxPackGroupBits;
}

template<uint32_t Token, uint32_t L, bool Wide>
consteval DeltaBlockDecoder DeltaBlockDecoderOf() noexcept {
  if constexpr (Wide && Token >= Code(DeltaEncoding::Bitset)) {
    return &DecodeDeltaBlockTokenAvx512<Token>;
  } else {
    return &DecodeDeltaBlockToken<Token, L>;
  }
}

template<uint32_t Token, uint32_t L, bool Wide>
consteval DeltaTailDecoder DeltaTailDecoderOf() noexcept {
  if constexpr (Wide && WideTail(Token)) {
    return &DecodeDeltaTailTokenAvx512<Token>;
  } else {
    return &DecodeDeltaToken<Token, false, L>;
  }
}

template<uint32_t L, bool Wide>
inline constexpr auto kDeltaBlockDecoders =
  []<uint32_t... Token>(std::integer_sequence<uint32_t, Token...>) {
    return std::array<DeltaBlockDecoder, sizeof...(Token)>{
      DeltaBlockDecoderOf<Token, L, Wide>()...};
  }(std::make_integer_sequence<uint32_t, Code(DeltaEncoding::End)>{});

template<uint32_t L, bool Wide>
inline constexpr auto kDeltaTailDecoders =
  []<uint32_t... Token>(std::integer_sequence<uint32_t, Token...>) {
    return std::array<DeltaTailDecoder, sizeof...(Token)>{
      DeltaTailDecoderOf<Token, L, Wide>()...};
  }(std::make_integer_sequence<uint32_t, Code(DeltaEncoding::End)>{});

struct DeltaDecoders {
  const DeltaBlockDecoder* blocks;
  const DeltaTailDecoder* tails;
};

template<bool Wide>
inline constexpr DeltaDecoders kWideDeltaDecodersOf{
  .blocks = kDeltaBlockDecoders<kWideLanes, Wide>.data(),
  .tails = kDeltaTailDecoders<kWideLanes, Wide>.data(),
};

inline const DeltaDecoders kWideDeltaDecoders = [] {
#ifdef __AVX2__
  __builtin_cpu_init();
  if (__builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512vl") &&
      __builtin_cpu_supports("avx512bw") &&
      __builtin_cpu_supports("avx512dq")) {
    return kWideDeltaDecodersOf<true>;
  }
#endif
  return kWideDeltaDecodersOf<false>;
}();

template<uint32_t Token, bool Full, uint32_t L>
IRS_FORCE_INLINE const byte_type* DecodeValuesBody(const byte_type* in,
                                                   uint32_t len,
                                                   uint32_t* out) noexcept {
  if constexpr (Full) {
    len = kBlockOf<L>;
  }
  if constexpr (Token == Code(ValueEncoding::One)) {
    std::fill_n(out, len, 1);
    return in + 1;
  } else if constexpr (Token == Code(ValueEncoding::Same08)) {
    std::fill_n(out, len, in[1]);
    return in + 2;
  } else if constexpr (Token == Code(ValueEncoding::Same16)) {
    std::fill_n(out, len, absl::little_endian::Load16(in + 1));
    return in + 1 + sizeof(uint16_t);
  } else if constexpr (Token == Code(ValueEncoding::Same32)) {
    std::fill_n(out, len, absl::little_endian::Load32(in + 1));
    return in + 1 + sizeof(uint32_t);
  } else {
    constexpr auto kShape = ShapeOf<ValueEncoding>(Token);
    return DecodeBits<kShape.family, kShape.bits, kShape.add, Full, L>(in, len,
                                                                       out);
  }
}

template<uint32_t Token, bool Full, uint32_t L>
IRS_NO_INLINE const byte_type* DecodeValuesToken(const byte_type* in,
                                                 uint32_t len,
                                                 uint32_t* out) noexcept {
  return DecodeValuesBody<Token, Full, L>(in, len, out);
}

template<bool Full, uint32_t L>
const byte_type* DispatchValues(const byte_type* in, uint32_t len,
                                uint32_t* out) noexcept {
  return ResolveByte<Code(ValueEncoding::End)>(
    in[0], [&]<uint32_t Token>() IRS_FORCE_INLINE {
      if constexpr (Inlined<ValueEncoding, Full>(Token)) {
        return DecodeValuesBody<Token, Full, L>(in, len, out);
      } else {
        return DecodeValuesToken<Token, Full, L>(in, len, out);
      }
    });
}

inline constexpr uint32_t kTinyTail = 32;

inline IRS_FORCE_INLINE I8x32 Concat4(const I8x8* parts) noexcept {
  const I8x16 lo = __builtin_shufflevector(parts[0], parts[1], 0, 1, 2, 3, 4, 5,
                                           6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
  const I8x16 hi = __builtin_shufflevector(parts[2], parts[3], 0, 1, 2, 3, 4, 5,
                                           6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
  return __builtin_shufflevector(lo, hi, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
                                 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                                 24, 25, 26, 27, 28, 29, 30, 31);
}

class TinyWidths {
 public:
  IRS_FORCE_INLINE TinyWidths(const uint32_t* values, uint32_t len,
                              uint32_t width) noexcept {
    SDB_ASSERT(len <= kTinyTail);
    if (width > kFloatExactWidth) {
      Fill<true>(values, len);
    } else {
      Fill<false>(values, len);
    }
  }

  IRS_FORCE_INLINE uint32_t Above(uint32_t bits) const noexcept {
    return static_cast<uint32_t>(
      std::popcount(MoveMask8(_widths > static_cast<int8_t>(bits))));
  }

 private:
  template<bool Exact>
  IRS_FORCE_INLINE void Fill(const uint32_t* values, uint32_t len) noexcept {
    I8x8 parts[kTinyTail / kWideLanes] = {};
    for (uint32_t c = 0; c * kWideLanes < len; ++c) {
      parts[c] = BitWidths<Exact>(values + c * kWideLanes);
    }
    _widths = Concat4(parts);
  }

  I8x32 _widths;
};

class TinyValues {
 public:
  IRS_FORCE_INLINE TinyValues(const uint32_t* values, uint32_t len) noexcept {
    SDB_ASSERT(1 <= len && len <= kTinyTail);
    const U32x8 first = U32x8{} + values[0];
    U32x8 any{};
    U32x8 less_any{};
    U32x8 diff{};
    U32x8 zeros{};
    I8x8 raw[kTinyTail / kWideLanes] = {};
    I8x8 less[kTinyTail / kWideLanes] = {};
    for (uint32_t c = 0; c != kTinyTail / kWideLanes; ++c) {
      if (c * kWideLanes >= len) {
        break;
      }
      const uint32_t rest = len - c * kWideLanes;
      U32x8 v;
      U32x8 valid;
      if (rest >= kWideLanes) {
        std::memcpy(&v, values + c * kWideLanes, sizeof(v));
        valid = U32x8{} - 1;
      } else {
        v = LoadFirst(values + c * kWideLanes, rest);
        valid = std::bit_cast<U32x8>(I32x8{0, 1, 2, 3, 4, 5, 6, 7} <
                                     static_cast<int32_t>(rest));
      }
      const U32x8 minus = (v - 1) & valid;
      any |= v;
      less_any |= minus;
      diff |= (v ^ first) & valid;
      zeros |= (v == 0) & valid;
      raw[c] = LaneWidths<true>(std::bit_cast<I32x8>(v));
      less[c] = LaneWidths<false>(std::bit_cast<I32x8>(minus));
    }
    _any = __builtin_reduce_or(any);
    _less_any = __builtin_reduce_or(less_any);
    _same = __builtin_reduce_or(diff) == 0;
    _zeros = __builtin_reduce_or(zeros) != 0;
    _widths[0] = Concat4(raw);
    _widths[1] = Concat4(less);
  }

  uint32_t Any() const noexcept { return _any; }
  uint32_t LessAny() const noexcept { return _less_any; }
  bool Same() const noexcept { return _same; }
  bool HasZero() const noexcept { return _zeros; }

  IRS_FORCE_INLINE uint32_t Above(uint32_t bits, bool less) const noexcept {
    return static_cast<uint32_t>(
      std::popcount(MoveMask8(_widths[less] > static_cast<int8_t>(bits))));
  }

 private:
  uint32_t _any;
  uint32_t _less_any;
  bool _same;
  bool _zeros;
  I8x32 _widths[2];
};

template<uint32_t L>
IRS_FORCE_INLINE bool TinyPackWins(const TinyWidths& widths, uint32_t len,
                                   uint32_t width) noexcept {
  const uint32_t pack = 1 + TailPacked(len, width);
  return TailPatchBound<L>(len, width, pack,
                           [&](uint32_t bits) IRS_FORCE_INLINE {
                             return widths.Above(bits);
                           }) >= pack;
}

template<bool Padded>
IRS_FORCE_INLINE uint32_t WriteTailPack(const uint32_t* values, uint32_t len,
                                        uint32_t bits, uint32_t sub,
                                        uint32_t token,
                                        byte_type* out) noexcept {
  SDB_ASSERT(1 <= len && len <= kTinyTail);
  SDB_ASSERT(0 < bits && bits <= kMaxWidth);
  out[0] = static_cast<byte_type>(token);
  const auto pack = [&]<bool Wide>() IRS_FORCE_INLINE {
    for (uint32_t c = 0; c * kWideLanes < len; ++c) {
      const uint32_t rest = len - c * kWideLanes;
      U32x8 v;
      if (Padded || rest >= kWideLanes) {
        std::memcpy(&v, values + c * kWideLanes, sizeof(v));
        v -= sub;
      } else {
        v = (LoadFirst(values + c * kWideLanes, rest) - sub) &
            std::bit_cast<U32x8>(I32x8{0, 1, 2, 3, 4, 5, 6, 7} <
                                 static_cast<int32_t>(rest));
      }
      PackGroupBits<Wide>(bits, v, out + 1 + c * bits);
    }
  };
  if (bits <= kMaxPackGroupBits) {
    pack.template operator()<false>();
  } else {
    pack.template operator()<true>();
  }
  return 1 + TailPacked(len, bits);
}

template<uint32_t L>
IRS_FORCE_INLINE uint32_t BitsetWords(const doc_id_t* docs, uint32_t len,
                                      doc_id_t prev, uint32_t plan_size,
                                      const EncodeOptions& options) noexcept {
  const uint64_t range = uint64_t{docs[len - 1]} - prev;
  const uint64_t words =
    (range + BitsRequired<uint64_t>() - 1) / BitsRequired<uint64_t>();
  if (options.bitset && words <= Layout<L>::kMaxBitsetWords &&
      (2 + words * sizeof(uint64_t)) * 100 <=
        uint64_t{plan_size} * (100 + options.bitset_margin_percent)) {
    return static_cast<uint32_t>(words);
  }
  return 0;
}

template<uint32_t L>
IRS_FORCE_INLINE uint32_t EncodeTinyValues(const TinyValues& tiny,
                                           const uint32_t* values, uint32_t len,
                                           bool minus_one,
                                           byte_type* out) noexcept {
  const auto width = static_cast<uint32_t>(std::bit_width(tiny.Any()));
  const uint32_t pack = 1 + TailPacked(len, width);
  const auto raw = [&](uint32_t bits)
                     IRS_FORCE_INLINE { return tiny.Above(bits, false); };
  const auto less = [&](uint32_t bits)
                      IRS_FORCE_INLINE { return tiny.Above(bits, true); };
  const auto write_less = [&](uint32_t less_width) IRS_FORCE_INLINE {
    const uint32_t less_pack = 1 + TailPacked(len, less_width);
    if (TailPatchBound<L>(len, less_width, less_pack, less) < less_pack) {
      return 0U;
    }
    return WriteTailPack<false>(
      values, len, less_width, 1,
      Code(ValueEncoding::PackMinusOne) + less_width - 1, out);
  };
  const auto less_width = static_cast<uint32_t>(std::bit_width(tiny.LessAny()));
  if (minus_one) {
    const uint32_t less_pack = 1 + TailPacked(len, less_width);
    if (less_width <= kMinusOneBits && less_pack < pack &&
        less_pack < 2 + 2 * raw(width - 1)) {
      return write_less(less_width);
    }
  }
  const uint32_t patched = TailPatchBound<L>(len, width, pack, raw);
  if (minus_one) {
    const uint32_t regular = std::min(pack, patched);
    if (MinusOneBound<L, false>(len, less_width, regular, less) < regular) {
      if (less_width > kMinusOneBits) {
        return 0;
      }
      const uint32_t less_pack = 1 + TailPacked(len, less_width);
      if (less_pack < regular) {
        return write_less(less_width);
      }
      if (TailPatchBound<L>(len, less_width, less_pack, less) < less_pack) {
        return 0;
      }
    }
  }
  if (patched < pack) {
    return 0;
  }
  return WriteTailPack<false>(values, len, width, 0,
                              Code(ValueEncoding::Pack) + width - 1, out);
}

template<bool Full, uint32_t L>
IRS_NO_INLINE uint32_t EncodeDelta(const doc_id_t* docs, uint32_t len,
                                   doc_id_t prev, byte_type* out,
                                   const EncodeOptions& options) noexcept {
  constexpr uint32_t kN = kBlockOf<L>;
  if constexpr (Full) {
    len = kN;
  }
  SDB_ASSERT(prev < docs[0]);
  SDB_ASSERT(std::adjacent_find(docs, docs + len, std::greater_equal<>{}) ==
             docs + len);
  uint32_t gaps[kN + kWideLanes];
  const uint32_t first = docs[0] - prev - 1;
  U32x8 before = U32x8{} + prev;
  U32x8 any8{};
  U32x8 diff8{};
  uint32_t i = 0;
  for (; i + 8 <= len; i += 8) {
    U32x8 current;
    std::memcpy(&current, docs + i, sizeof(current));
    const U32x8 gap =
      current - 1 -
      __builtin_shufflevector(before, current, 7, 8, 9, 10, 11, 12, 13, 14);
    std::memcpy(gaps + i, &gap, sizeof(gap));
    any8 |= gap;
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
    any8 |= gap;
    diff8 |= (gap ^ first) & valid;
  }
  uint32_t any = 0;
  uint32_t diff = 0;
  for (uint32_t k = 0; k != 8; ++k) {
    any |= any8[k];
    diff |= diff8[k];
  }
  if (diff == 0) {
    if (gaps[0] == 0) {
      out[0] = static_cast<byte_type>(DeltaEncoding::Run);
      return 1;
    }
    return WriteSame<DeltaEncoding>(gaps[0] + 1, out);
  }
  const auto width = static_cast<uint32_t>(std::bit_width(any));
  if constexpr (!Full) {
    if (len <= kTinyTail &&
        TinyPackWins<L>(TinyWidths{gaps, len, width}, len, width)) {
      const uint32_t pack = 1 + TailPacked(len, width);
      if (const auto words = BitsetWords<L>(docs, len, prev, pack, options)) {
        return WriteBitset<false, L>(docs, len, prev, words, out);
      }
      return WriteTailPack<true>(gaps, len, width, 0,
                                 Code(DeltaEncoding::Pack) + width - 1, out);
    }
    const U32x8 zero = Opaque(U32x8{});
    for (uint32_t i = len; i < PaddedLen(len); i += kWideLanes) {
      std::memcpy(gaps + i, &zero, sizeof(zero));
    }
  }
  const Stats<L, Full> stats{gaps, len, width, Stats<L, Full>::kNoWidth};
  const auto plan = ChoosePlan(stats, options);
  if (const auto words = BitsetWords<L>(docs, len, prev, plan.size, options)) {
    return WriteBitset<Full, L>(docs, len, prev, words, out);
  }
  return WritePlan<Full>(gaps, stats, plan, PlanToken<DeltaEncoding>(plan),
                         out);
}

template<bool Full, uint32_t L>
IRS_FORCE_INLINE uint32_t DeltaSize(const byte_type* in,
                                    uint32_t len) noexcept {
  if constexpr (Full) {
    len = kBlockOf<L>;
  }
  return BlockBytes<DeltaEncoding, L>(in, len);
}

inline uint32_t WriteSameValue(uint32_t value, byte_type* out) noexcept {
  if (value == 1) {
    out[0] = static_cast<byte_type>(ValueEncoding::One);
    return 1;
  }
  return WriteSame<ValueEncoding>(value, out);
}

template<bool Full, uint32_t L>
uint32_t EncodeGeneralValues(const uint32_t* values, uint32_t len, uint32_t any,
                             uint32_t less_any, bool minus_one, byte_type* out,
                             const EncodeOptions& options) noexcept {
  constexpr uint32_t kN = kBlockOf<L>;
  const uint32_t* raw = values;
  uint32_t padded[kN];
  const uint32_t padded_len = Full ? kN : PaddedLen(len);
  if constexpr (!Full) {
    PadTail(values, len, padded);
    raw = padded;
  }
  const Stats<L, Full> stats{
    raw, len, static_cast<uint32_t>(std::bit_width(any)),
    minus_one ? static_cast<uint32_t>(std::bit_width(less_any))
              : Stats<L, Full>::kNoWidth};
  const auto plan = ChooseValuePlan(stats, options);
  if (plan.add == 0) {
    return WritePlan<Full>(raw, stats, plan, PlanToken<ValueEncoding>(plan),
                           out);
  }
  uint32_t shifted[kN];
  for (uint32_t i = 0; i != padded_len; ++i) {
    shifted[i] = raw[i] - 1;
  }
  return WritePlan<Full>(shifted, stats, plan, PlanToken<ValueEncoding>(plan),
                         out);
}

template<bool Full, uint32_t L>
IRS_NO_INLINE uint32_t EncodeValues(const uint32_t* values, uint32_t len,
                                    byte_type* out,
                                    const EncodeOptions& options) noexcept {
  if constexpr (Full) {
    len = kBlockOf<L>;
  } else if (len <= kTinyTail) {
    const TinyValues tiny{values, len};
    if (tiny.Same()) {
      return WriteSameValue(values[0], out);
    }
    const bool minus_one = options.minus_one && !tiny.HasZero();
    if (const auto size =
          EncodeTinyValues<L>(tiny, values, len, minus_one, out)) {
      return size;
    }
    return EncodeGeneralValues<Full, L>(values, len, tiny.Any(), tiny.LessAny(),
                                        minus_one, out, options);
  }
  uint32_t any = 0;
  uint32_t less_any = 0;
  uint32_t diff = 0;
  uint32_t zeros = 0;
  for (uint32_t i = 0; i != len; ++i) {
    any |= values[i];
    less_any |= values[i] - 1;
    diff |= values[i] ^ values[0];
    zeros += values[i] == 0;
  }
  if (diff == 0) {
    return WriteSameValue(values[0], out);
  }
  return EncodeGeneralValues<Full, L>(
    values, len, any, less_any, options.minus_one && zeros == 0, out, options);
}

template<bool Full, uint32_t L>
IRS_FORCE_INLINE uint32_t ValuesSize(const byte_type* in,
                                     uint32_t len) noexcept {
  if constexpr (Full) {
    len = kBlockOf<L>;
  }
  return BlockBytes<ValueEncoding, L>(in, len);
}

template<typename Encoding, bool Full, uint32_t L>
uint32_t PrefixSize(uint32_t token, uint32_t len) noexcept {
  if constexpr (Full) {
    len = kBlockOf<L>;
  }
  if (token < Code(Encoding::Pack)) {
    if constexpr (std::is_same_v<Encoding, DeltaEncoding>) {
      if (token == Code(DeltaEncoding::Bitset)) {
        return 2;
      }
    }
    constexpr uint32_t kSame[] = {1, 2, 3, 5};
    return kSame[token];
  }
  const auto shape = ShapeOf<Encoding>(token);
  switch (shape.family) {
    case Family::Pack:
      return 1;
    case Family::Patch16:
    case Family::Patch32:
      return 2;
    case Family::PatchMixed:
      return 3;
    case Family::PatchBitmap:
      return 2 + PackedSize<L>(len, shape.bits) + Layout<L>::kBitmapBytes;
  }
  SDB_UNREACHABLE();
}

template<uint32_t L>
struct BlockCodec {
  static constexpr uint32_t kLanes = L;
  static constexpr uint32_t kBlock = kBlockOf<L>;
  static constexpr uint32_t kMaxBlockBytes = 3 + 4 * L * kMaxWidth + 4 * kBlock;

  static uint32_t EncodeDeltaBlock(const doc_id_t* docs, doc_id_t prev,
                                   byte_type* out,
                                   const EncodeOptions& options = {}) {
    return EncodeDelta<true, L>(docs, kBlock, prev, out, options);
  }

  static uint32_t EncodeDeltaTail(const doc_id_t* docs, uint32_t len,
                                  doc_id_t prev, byte_type* out,
                                  const EncodeOptions& options = {}) {
    SDB_ASSERT(1 <= len && len < kBlock);
    return EncodeDelta<false, L>(docs, len, prev, out, options);
  }

  IRS_FORCE_INLINE static const byte_type* DecodeDeltaBlock(const byte_type* in,
                                                            doc_id_t prev,
                                                            doc_id_t* out) {
    SDB_ASSERT(in[0] < Code(DeltaEncoding::End));
    if constexpr (L == kWideLanes) {
      return kWideDeltaDecoders.blocks[in[0]](in, prev, out);
    } else {
      return kDeltaBlockDecoders<L, false>[in[0]](in, prev, out);
    }
  }

  IRS_FORCE_INLINE static const byte_type* DecodeDeltaTail(const byte_type* in,
                                                           uint32_t len,
                                                           doc_id_t prev,
                                                           doc_id_t* out) {
    SDB_ASSERT(1 <= len && len < kBlock);
    SDB_ASSERT(in[0] < Code(DeltaEncoding::End));
    if constexpr (L == kWideLanes) {
      return kWideDeltaDecoders.tails[in[0]](in, len, prev, out);
    } else {
      return kDeltaTailDecoders<L, false>[in[0]](in, len, prev, out);
    }
  }

  static uint32_t DeltaBlockSize(const byte_type* in) {
    return DeltaSize<true, L>(in, kBlock);
  }

  static uint32_t DeltaTailSize(const byte_type* in, uint32_t len) {
    SDB_ASSERT(1 <= len && len < kBlock);
    return DeltaSize<false, L>(in, len);
  }

  static uint32_t EncodeValuesBlock(const uint32_t* values, byte_type* out,
                                    const EncodeOptions& options = {}) {
    return EncodeValues<true, L>(values, kBlock, out, options);
  }

  static uint32_t EncodeValuesTail(const uint32_t* values, uint32_t len,
                                   byte_type* out,
                                   const EncodeOptions& options = {}) {
    SDB_ASSERT(1 <= len && len < kBlock);
    return EncodeValues<false, L>(values, len, out, options);
  }

  IRS_FORCE_INLINE static const byte_type* DecodeValuesBlock(
    const byte_type* in, uint32_t* out) {
    return DispatchValues<true, L>(in, kBlock, out);
  }

  IRS_FORCE_INLINE static const byte_type* DecodeValuesTail(const byte_type* in,
                                                            uint32_t len,
                                                            uint32_t* out) {
    SDB_ASSERT(1 <= len && len < kBlock);
    return DispatchValues<false, L>(in, len, out);
  }

  static uint32_t ValuesBlockSize(const byte_type* in) {
    return ValuesSize<true, L>(in, kBlock);
  }

  static uint32_t ValuesTailSize(const byte_type* in, uint32_t len) {
    SDB_ASSERT(1 <= len && len < kBlock);
    return ValuesSize<false, L>(in, len);
  }

  static uint32_t DeltaBlockPrefix(uint32_t token) {
    return PrefixSize<DeltaEncoding, true, L>(token, kBlock);
  }

  static uint32_t DeltaTailPrefix(uint32_t token, uint32_t len) {
    SDB_ASSERT(1 <= len && len < kBlock);
    return PrefixSize<DeltaEncoding, false, L>(token, len);
  }

  static uint32_t ValuesBlockPrefix(uint32_t token) {
    return PrefixSize<ValueEncoding, true, L>(token, kBlock);
  }

  static uint32_t ValuesTailPrefix(uint32_t token, uint32_t len) {
    SDB_ASSERT(1 <= len && len < kBlock);
    return PrefixSize<ValueEncoding, false, L>(token, len);
  }
};

using Codec128 = BlockCodec<kLanes>;
using Codec256 = BlockCodec<kWideLanes>;

}  // namespace irs::block_codec
