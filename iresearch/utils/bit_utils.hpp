////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <roaring/bitset_util.h>

#include <array>
#include <bit>
#include <cstddef>
#include <utility>
#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"

#ifdef __AVX2__
#define IRS_AVX512 __attribute__((target("avx512f,avx512vl,avx512bw,avx512dq")))
#define IRS_VBMI2 \
  __attribute__((target("avx512f,avx512vl,avx512bw,avx512dq,avx512vbmi2")))
#else
#define IRS_AVX512
#define IRS_VBMI2
#endif

namespace irs {

using UnalignedWord [[gnu::aligned(1)]] = uint64_t;

template<typename T>
consteval uint32_t BitsRequired() noexcept {
  return sizeof(T) * 8U;
}

template<typename T>
IRS_FORCE_INLINE constexpr size_t BitsRequired(size_t n) noexcept {
  return BitsRequired<T>() * n;
}

template<typename T>
IRS_FORCE_INLINE constexpr T PopBit(T v) noexcept {
  SDB_ASSERT(v);
  return v & (v - 1);
}

IRS_FORCE_INLINE constexpr uint64_t SelectInWord(uint64_t bits,
                                                 uint64_t rank) noexcept {
  SDB_ASSERT(rank < static_cast<uint64_t>(std::popcount(bits)));
  uint64_t at = 0;
  for (uint64_t width = 32; width != 0; width /= 2) {
    const auto low =
      static_cast<uint64_t>(std::popcount(bits & ((uint64_t{1} << width) - 1)));
    if (rank >= low) {
      rank -= low;
      bits >>= width;
      at += width;
    }
  }
  return at;
}

IRS_FORCE_INLINE constexpr uint64_t HeadMask(uint64_t begin) noexcept {
  return ~uint64_t{0} << (begin % BitsRequired<uint64_t>());
}

IRS_FORCE_INLINE constexpr uint64_t TailMask(uint64_t end) noexcept {
  return ~uint64_t{0} >> ((~end + 1) % BitsRequired<uint64_t>());
}

template<typename Visit>
IRS_FORCE_INLINE void ForEachSetBit(uint64_t word, Visit&& visit) {
  for (; word != 0; word = PopBit(word)) {
    visit(static_cast<uint32_t>(std::countr_zero(word)));
  }
}

inline constexpr size_t kHarleySealWords = 256;

#ifdef __AVX2__
inline bool HasAvx512() noexcept {
  static const bool kHas = [] {
    __builtin_cpu_init();
    return __builtin_cpu_supports("avx512f") &&
           __builtin_cpu_supports("avx512vl") &&
           __builtin_cpu_supports("avx512bw") &&
           __builtin_cpu_supports("avx512dq");
  }();
  return kHas;
}

inline bool HasVbmi2() noexcept {
  static const bool kHas = HasAvx512() && __builtin_cpu_supports("avx512vbmi2");
  return kHas;
}

inline bool HasRoaringAvx512() noexcept {
  static const bool kHas = (roaring::internal::croaring_hardware_support() &
                            roaring::internal::ROARING_SUPPORTS_AVX512) != 0;
  return kHas;
}
#endif

inline uint64_t CountBits(const uint64_t* IRS_RESTRICT words,
                          size_t count) noexcept {
  uint64_t total = 0;
  size_t w = 0;
#ifdef __AVX2__
  if (count >= kHarleySealWords) {
    if (HasRoaringAvx512()) {
      w = count / 8 * 8;
      total = roaring::internal::avx512_vpopcount(
        reinterpret_cast<const __m512i*>(words), count / 8);
    } else {
      w = count / 4 * 4;
      total = roaring::internal::avx2_harley_seal_popcount256(
        reinterpret_cast<const __m256i*>(words), count / 4);
    }
  }
#endif
  for (; w != count; ++w) {
    total += static_cast<uint64_t>(std::popcount(words[w]));
  }
  return total;
}

inline IRS_FORCE_INLINE uint64_t CountBitRange(
  const uint64_t* IRS_RESTRICT words, uint64_t begin, uint64_t end) noexcept {
  constexpr auto kBits = BitsRequired<uint64_t>();
  SDB_ASSERT(begin < end);
  const auto first = begin / kBits;
  const auto last = (end - 1) / kBits;
  if (first == last) {
    return static_cast<uint64_t>(
      std::popcount(words[first] & HeadMask(begin) & TailMask(end)));
  }
  return static_cast<uint64_t>(std::popcount(words[first] & HeadMask(begin))) +
         CountBits(words + first + 1, last - first - 1) +
         static_cast<uint64_t>(std::popcount(words[last] & TailMask(end)));
}

inline IRS_FORCE_INLINE uint64_t
NextSetBit(const UnalignedWord* IRS_RESTRICT words, uint64_t from,
           uint64_t count) noexcept {
  constexpr auto kBits = BitsRequired<uint64_t>();
  SDB_ASSERT(from < count * kBits);
  auto w = from / kBits;
  uint64_t bits = words[w] & HeadMask(from);
  while (bits == 0) {
    if (++w == count) {
      return count * kBits;
    }
    bits = words[w];
  }
  return w * kBits + static_cast<uint64_t>(std::countr_zero(bits));
}

template<typename Word, typename Apply>
IRS_FORCE_INLINE void ApplyShifted(Word* IRS_RESTRICT dst, uint64_t begin,
                                   const UnalignedWord* IRS_RESTRICT src,
                                   uint32_t words, Apply&& apply) noexcept {
  constexpr auto kBits = BitsRequired<uint64_t>();
  SDB_ASSERT(words != 0);
  dst += begin / kBits;
  const auto shift = static_cast<uint32_t>(begin % kBits);
  if (shift == 0) {
    for (uint32_t i = 0; i != words; ++i) {
      apply(dst[i], src[i]);
    }
    return;
  }
  uint64_t carry = 0;
  for (uint32_t i = 0; i != words; ++i) {
    const uint64_t word = src[i];
    apply(dst[i], (word << shift) | carry);
    carry = word >> (kBits - shift);
  }
  apply(dst[words], carry);
}

inline IRS_FORCE_INLINE void OrBlock(uint64_t* IRS_RESTRICT dst, uint64_t begin,
                                     const UnalignedWord* IRS_RESTRICT src,
                                     uint32_t words) noexcept {
  ApplyShifted(dst, begin, src, words,
               [](uint64_t& word, uint64_t bits)
                 IRS_FORCE_INLINE { word |= bits; });
}

inline IRS_FORCE_INLINE void ClearBlock(uint64_t* IRS_RESTRICT dst,
                                        uint64_t begin,
                                        const UnalignedWord* IRS_RESTRICT src,
                                        uint32_t words) noexcept {
  ApplyShifted(dst, begin, src, words,
               [](uint64_t& word, uint64_t bits)
                 IRS_FORCE_INLINE { word &= ~bits; });
}

inline IRS_FORCE_INLINE uint64_t
CountBlock(const uint64_t* IRS_RESTRICT dst, uint64_t begin,
           const UnalignedWord* IRS_RESTRICT src, uint32_t words) noexcept {
  uint64_t total = 0;
  ApplyShifted(dst, begin, src, words,
               [&](uint64_t word, uint64_t bits) IRS_FORCE_INLINE {
                 total += static_cast<uint64_t>(std::popcount(word & bits));
               });
  return total;
}

#ifdef __AVX2__
using BitsetByteEntry = std::array<uint8_t, 8>;

inline constexpr std::array<BitsetByteEntry, 256> kBitsetByteTable = [] {
  std::array<BitsetByteEntry, 256> t{};
  for (uint32_t b = 0; b != 256; ++b) {
    uint32_t count = 0;
    for (uint32_t i = 0; i != 8; ++i) {
      if (b & (1 << i)) {
        t[b][count++] = i;
      }
    }
  }
  return t;
}();

inline IRS_FORCE_INLINE __m256i LeftPackControl(uint32_t mask) noexcept {
  SDB_ASSERT(mask < 256);
  return _mm256_cvtepi8_epi32(_mm_loadl_epi64(
    reinterpret_cast<const __m128i*>(kBitsetByteTable[mask].data())));
}
#endif

inline constexpr uint32_t kSparseWordBits = 8;
inline constexpr uint32_t kUnrolledBits = 4;

IRS_FORCE_INLINE constexpr uint64_t BytePrefixes(uint64_t word) noexcept {
  uint64_t counts = word - ((word >> 1) & 0x5555555555555555ULL);
  counts =
    (counts & 0x3333333333333333ULL) + ((counts >> 2) & 0x3333333333333333ULL);
  counts = (counts + (counts >> 4)) & 0x0F0F0F0F0F0F0F0FULL;
  return (counts * 0x0101010101010101ULL) << 8;
}

template<typename T>
IRS_FORCE_INLINE T* MaterializeSparse(T base, uint64_t word,
                                      T* IRS_RESTRICT out) noexcept {
  const auto bits = static_cast<uint32_t>(std::popcount(word));
  for (uint32_t k = 0; k != kUnrolledBits; ++k) {
    out[k] = static_cast<T>(base + std::countr_zero(word));
    word &= word - 1;
  }
  if (bits <= kUnrolledBits) [[likely]] {
    return out + bits;
  }
  out += kUnrolledBits;
  ForEachSetBit(word, [&](uint32_t bit) IRS_FORCE_INLINE {
    *out++ = static_cast<T>(base + bit);
  });
  return out;
}

template<typename T>
IRS_FORCE_INLINE T* MaterializeDense(T base, uint64_t word,
                                     T* IRS_RESTRICT out) noexcept {
#ifdef __AVX2__
  static_assert(sizeof(T) == sizeof(uint16_t) || sizeof(T) == sizeof(uint32_t));
  const auto prefix = BytePrefixes(word);
  for (uint32_t b = 0; b != 8; ++b) {
    auto* const at = out + ((prefix >> (b * 8)) & 0xFF);
    const auto mask = static_cast<uint32_t>(word >> (b * 8)) & 0xFF;
    if constexpr (sizeof(T) == sizeof(uint16_t)) {
      _mm_storeu_si128(
        reinterpret_cast<__m128i*>(at),
        _mm_add_epi16(
          _mm_set1_epi16(static_cast<int16_t>(base + b * 8)),
          _mm_cvtepu8_epi16(_mm_loadl_epi64(
            reinterpret_cast<const __m128i*>(kBitsetByteTable[mask].data())))));
    } else {
      _mm256_storeu_si256(
        reinterpret_cast<__m256i*>(at),
        _mm256_add_epi32(_mm256_set1_epi32(static_cast<int>(base + b * 8)),
                         LeftPackControl(mask)));
    }
  }
  return out + std::popcount(word);
#else
  return MaterializeSparse(base, word, out);
#endif
}

template<typename T>
IRS_FORCE_INLINE T* MaterializeBits(T base, uint64_t word,
                                    T* IRS_RESTRICT out) noexcept {
  if (static_cast<uint32_t>(std::popcount(word)) > kSparseWordBits) {
    return MaterializeDense(base, word, out);
  }
  return MaterializeSparse(base, word, out);
}

template<typename T>
IRS_FORCE_INLINE T* MaterializeWords(T base, const uint64_t* IRS_RESTRICT words,
                                     size_t count,
                                     T* IRS_RESTRICT out) noexcept {
  constexpr auto kBits = BitsRequired<uint64_t>();
  for (size_t chunk = 0; chunk < count; chunk += kBits) {
    const auto* const part = words + chunk;
    const auto n = std::min<size_t>(kBits, count - chunk);
    uint64_t held = 0;
    for (size_t w = 0; w != n; ++w) {
      held |= uint64_t{part[w] != 0} << w;
    }
    for (; held != 0; held = PopBit(held)) {
      const auto w = static_cast<size_t>(std::countr_zero(held));
      out = MaterializeBits(static_cast<T>(base + (chunk + w) * kBits), part[w],
                            out);
    }
  }
  return out;
}

inline IRS_FORCE_INLINE uint32_t* MaterializeWord(uint32_t offset,
                                                  uint64_t word,
                                                  uint32_t* IRS_RESTRICT out) {
  if (word == 0) {
    return out;
  }
#ifdef __AVX2__
  out = MaterializeBits(offset, word, out);
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(out),
                      _mm256_set1_epi32(static_cast<int>(offset)));
  return out;
#else
  auto* const begin = out;
  out = MaterializeBits(offset, word, out);
  for (auto* pad = out; pad != begin + ((out - begin + 7) & ~ptrdiff_t{7});
       ++pad) {
    *pad = offset;
  }
  return out;
#endif
}

inline IRS_FORCE_INLINE uint32_t* MaterializeRange(
  uint32_t offset, const uint64_t* IRS_RESTRICT words, size_t count,
  uint32_t* IRS_RESTRICT out) noexcept {
  out = MaterializeWords(offset, words, count, out);
#ifdef __AVX2__
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(out),
                      _mm256_set1_epi32(static_cast<int>(offset)));
#endif
  return out;
}

#ifdef __AVX2__
inline IRS_FORCE_INLINE IRS_AVX512 __m512i OpaqueZero() noexcept {
  __m512i zero = _mm512_setzero_si512();
  asm("" : "+v"(zero));
  return zero;
}
#endif

inline IRS_FORCE_INLINE IRS_AVX512 uint32_t* MaterializeWord16(
  uint32_t offset, uint64_t word, uint32_t* out) noexcept {
#ifdef __AVX2__
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
  return MaterializeBits(offset, word, out);
#endif
}

template<typename T>
IRS_FORCE_INLINE constexpr void SetBit(T& value, size_t bit) noexcept {
  static_assert(std::is_unsigned_v<T>);
  value |= (T(1) << bit);
}

template<typename T>
IRS_FORCE_INLINE constexpr void UnsetBit(T& value, size_t bit) noexcept {
  static_assert(std::is_unsigned_v<T>);
  auto mask = T(1) << bit;
  if !consteval {
    if (!__builtin_constant_p(bit)) {
      mask = Opaque(mask);
    }
  }
  value &= ~mask;
}

template<typename T>
IRS_FORCE_INLINE constexpr void SetBit(T& value, size_t bit,
                                       bool set) noexcept {
  set ? SetBit(value, bit) : UnsetBit(value, bit);
}

template<typename T>
IRS_FORCE_INLINE constexpr void UnsetBit(T& value, size_t bit,
                                         bool unset) noexcept {
  if (unset) {
    UnsetBit(value, bit);
  }
}

template<typename T>
IRS_FORCE_INLINE constexpr bool CheckBit(T value, size_t bit) noexcept {
  static_assert(std::is_unsigned_v<T>);
  return (value & (T(1) << bit)) != 0;
}

template<unsigned Offset, typename T>
IRS_FORCE_INLINE constexpr T Rol(T value) noexcept {
  static_assert(Offset >= 0 && Offset <= sizeof(T) * 8, "Offset out of range");
  return (value << Offset) | (value >> (sizeof(T) * 8 - Offset));
}

template<unsigned Offset, typename T>
IRS_FORCE_INLINE constexpr T Ror(T value) noexcept {
  static_assert(Offset >= 0 && Offset <= sizeof(T) * 8, "Offset out of range");
  return (value >> Offset) | (value << (sizeof(T) * 8 - Offset));
}

// static_assert that signed right shift works as expected
static_assert(static_cast<uint32_t>(INT32_C(-1) >> 31) == UINT32_C(0xFFFFFFFF));
static_assert(static_cast<uint64_t>(INT64_C(-1) >> 63) ==
              UINT64_C(0xFFFFFFFFFFFFFFFF));

template<typename T>
struct EnumBitwiseTraits {
  static_assert(std::is_enum_v<T>);

  static constexpr T Or(T lhs, T rhs) noexcept {
    return static_cast<T>(std::to_underlying(lhs) | std::to_underlying(rhs));
  }

  static constexpr T Xor(T lhs, T rhs) noexcept {
    return static_cast<T>(std::to_underlying(lhs) ^ std::to_underlying(rhs));
  }

  static constexpr T And(T lhs, T rhs) noexcept {
    return static_cast<T>(std::to_underlying(lhs) & std::to_underlying(rhs));
  }

  static constexpr T Not(T v) noexcept {
    return static_cast<T>(~std::to_underlying(v));
  }
};

template<typename T>
inline constexpr T EnumBitwiseOr(T lhs, T rhs) noexcept {
  return EnumBitwiseTraits<T>::Or(lhs, rhs);
}

template<typename T>
inline constexpr T EnumBitwiseXor(T lhs, T rhs) noexcept {
  return EnumBitwiseTraits<T>::Xor(lhs, rhs);
}

template<typename T>
inline constexpr T EnumBitwiseAnd(T lhs, T rhs) noexcept {
  return EnumBitwiseTraits<T>::And(lhs, rhs);
}

template<typename T>
inline constexpr T EnumBitwiseNot(T v) noexcept {
  return EnumBitwiseTraits<T>::Not(v);
}

}  // namespace irs

// TODO(mbkkt) PLEASE DON'T USE IT!
//  For an example, you have enum with 1 and 2:
//  you make OR, it produces 3 which is not part of enum!
// TODO(mbkkt) Remove all usage, then remove it

#define ENABLE_BITMASK_ENUM(x)                                              \
  [[maybe_unused]] inline constexpr x operator&(x lhs, x rhs) noexcept {    \
    return irs::EnumBitwiseAnd(lhs, rhs);                                   \
  }                                                                         \
  [[maybe_unused]] inline constexpr x& operator&=(x& lhs, x rhs) noexcept { \
    return lhs = irs::EnumBitwiseAnd(lhs, rhs);                             \
  }                                                                         \
  [[maybe_unused]] inline constexpr x operator|(x lhs, x rhs) noexcept {    \
    return irs::EnumBitwiseOr(lhs, rhs);                                    \
  }                                                                         \
  [[maybe_unused]] inline constexpr x& operator|=(x& lhs, x rhs) noexcept { \
    return lhs = irs::EnumBitwiseOr(lhs, rhs);                              \
  }                                                                         \
  [[maybe_unused]] inline constexpr x operator^(x lhs, x rhs) noexcept {    \
    return irs::EnumBitwiseXor(lhs, rhs);                                   \
  }                                                                         \
  [[maybe_unused]] inline constexpr x& operator^=(x& lhs, x rhs) noexcept { \
    return lhs = irs::EnumBitwiseXor(lhs, rhs);                             \
  }                                                                         \
  [[maybe_unused]] inline constexpr x operator~(x v) noexcept {             \
    return irs::EnumBitwiseNot(v);                                          \
  }
