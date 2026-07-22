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

#include <bit>
#include <cstddef>
#include <cstdint>
#include <span>

#if defined(__AVX2__) || defined(__SSE2__)
#include <immintrin.h>
#endif

#include "basics/shared.hpp"
#include "iresearch/types.hpp"

namespace irs {

// The ONE hand-vectorized site in the analysis layer. Byte-classification
// masks are the hot inner primitive of every splitter kernel; the compiler
// auto-vectorizes the scalar loop but cannot form the cmpeq+movemask idiom
// (measured ~2x slower: widened lanes + OR-reduction tree instead of one
// mask extraction). Everything downstream consumes the plain uint32_t mask.
inline constexpr size_t kClassifyBlock =
#if defined(__AVX2__)
  32;
#elif defined(__SSE2__)
  16;
#else
  8;
#endif

// Bit i set iff block[i] == target; block must have kClassifyBlock readable
// bytes.
IRS_FORCE_INLINE inline uint32_t ClassifyEqBlock(const byte_type* block,
                                                 byte_type target) noexcept {
#if defined(__AVX2__)
  const __m256i b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block));
  const __m256i t = _mm256_set1_epi8(static_cast<char>(target));
  return static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, t)));
#elif defined(__SSE2__)
  const __m128i b = _mm_loadu_si128(reinterpret_cast<const __m128i*>(block));
  const __m128i t = _mm_set1_epi8(static_cast<char>(target));
  return static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(b, t)));
#else
  uint32_t bitmask = 0;
  for (size_t i = 0; i < kClassifyBlock; ++i) {
    bitmask |= static_cast<uint32_t>(block[i] == target) << i;
  }
  return bitmask;
#endif
}

// OR of ClassifyEqBlock over every target byte.
IRS_FORCE_INLINE inline uint32_t ClassifyAnyEqBlock(
  const byte_type* block, std::span<const byte_type> targets) noexcept {
#if defined(__AVX2__)
  const __m256i b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block));
  __m256i acc = _mm256_setzero_si256();
  for (const auto target : targets) {
    acc = _mm256_or_si256(
      acc, _mm256_cmpeq_epi8(b, _mm256_set1_epi8(static_cast<char>(target))));
  }
  return static_cast<uint32_t>(_mm256_movemask_epi8(acc));
#elif defined(__SSE2__)
  const __m128i b = _mm_loadu_si128(reinterpret_cast<const __m128i*>(block));
  __m128i acc = _mm_setzero_si128();
  for (const auto target : targets) {
    acc = _mm_or_si128(
      acc, _mm_cmpeq_epi8(b, _mm_set1_epi8(static_cast<char>(target))));
  }
  return static_cast<uint32_t>(_mm_movemask_epi8(acc));
#else
  uint32_t bitmask = 0;
  for (const auto target : targets) {
    bitmask |= ClassifyEqBlock(block, target);
  }
  return bitmask;
#endif
}

// Per-byte membership masks for the ASCII word set: word = [A-Za-z0-9_],
// alpha = [A-Za-z], digit = [0-9]. Range tests, so the movemask idiom is the
// same class the compiler cannot form (find-first-non-word is a data-dependent
// early exit). `block` must have kClassifyBlock readable bytes.
struct WordMasks {
  uint32_t word;
  uint32_t alpha;
  uint32_t digit;
};

IRS_FORCE_INLINE inline WordMasks ClassifyWordBlock(
  const byte_type* block) noexcept {
#if defined(__AVX2__)
  const __m256i b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block));
  const auto in_range = [b](char lo, char hi) {
    const __m256i x = b;
    const __m256i ge =
      _mm256_cmpgt_epi8(x, _mm256_set1_epi8(static_cast<char>(lo - 1)));
    const __m256i gt = _mm256_cmpgt_epi8(x, _mm256_set1_epi8(hi));
    return _mm256_andnot_si256(gt, ge);
  };
  const __m256i digit = in_range('0', '9');
  const __m256i folded = _mm256_or_si256(b, _mm256_set1_epi8(0x20));
  const __m256i ge = _mm256_cmpgt_epi8(folded, _mm256_set1_epi8('a' - 1));
  const __m256i gt = _mm256_cmpgt_epi8(folded, _mm256_set1_epi8('z'));
  const __m256i alpha = _mm256_andnot_si256(gt, ge);
  const __m256i us = _mm256_cmpeq_epi8(b, _mm256_set1_epi8('_'));
  const __m256i word = _mm256_or_si256(_mm256_or_si256(digit, alpha), us);
  return {static_cast<uint32_t>(_mm256_movemask_epi8(word)),
          static_cast<uint32_t>(_mm256_movemask_epi8(alpha)),
          static_cast<uint32_t>(_mm256_movemask_epi8(digit))};
#elif defined(__SSE2__)
  const __m128i b = _mm_loadu_si128(reinterpret_cast<const __m128i*>(block));
  const auto in_range = [b](char lo, char hi) {
    const __m128i ge =
      _mm_cmpgt_epi8(b, _mm_set1_epi8(static_cast<char>(lo - 1)));
    const __m128i gt = _mm_cmpgt_epi8(b, _mm_set1_epi8(hi));
    return _mm_andnot_si128(gt, ge);
  };
  const __m128i digit = in_range('0', '9');
  const __m128i folded = _mm_or_si128(b, _mm_set1_epi8(0x20));
  const __m128i ge = _mm_cmpgt_epi8(folded, _mm_set1_epi8('a' - 1));
  const __m128i gt = _mm_cmpgt_epi8(folded, _mm_set1_epi8('z'));
  const __m128i alpha = _mm_andnot_si128(gt, ge);
  const __m128i us = _mm_cmpeq_epi8(b, _mm_set1_epi8('_'));
  const __m128i word = _mm_or_si128(_mm_or_si128(digit, alpha), us);
  return {static_cast<uint32_t>(_mm_movemask_epi8(word)),
          static_cast<uint32_t>(_mm_movemask_epi8(alpha)),
          static_cast<uint32_t>(_mm_movemask_epi8(digit))};
#else
  WordMasks m{0, 0, 0};
  for (size_t i = 0; i < kClassifyBlock; ++i) {
    const auto c = block[i];
    const bool d = c >= '0' && c <= '9';
    const auto f = static_cast<byte_type>(c | 0x20);
    const bool a = f >= 'a' && f <= 'z';
    m.digit |= static_cast<uint32_t>(d) << i;
    m.alpha |= static_cast<uint32_t>(a) << i;
    m.word |= static_cast<uint32_t>(d || a || c == '_') << i;
  }
  return m;
#endif
}

}  // namespace irs
