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
#include <cstdint>

#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

template<typename Excludes>
IRS_FORCE_INLINE bool IsExcluded(Excludes& excludes, doc_id_t doc) {
  if constexpr (requires { excludes.Test(doc); }) {
    return excludes.Test(doc);
  } else {
    return excludes.Probe(doc) == doc;
  }
}

#ifdef __AVX2__
inline constexpr auto kCompactLanes = [] {
  std::array<std::array<uint8_t, 16>, 16> table{};
  for (uint32_t mask = 0; mask != 16; ++mask) {
    uint32_t out = 0;
    for (uint32_t lane = 0; lane != 4; ++lane) {
      if (((mask >> lane) & 1) != 0) {
        for (uint32_t b = 0; b != 4; ++b) {
          table[mask][out * 4 + b] = static_cast<uint8_t>(lane * 4 + b);
        }
        ++out;
      }
    }
    for (uint32_t b = out * 4; b != 16; ++b) {
      table[mask][b] = 0x80;
    }
  }
  return table;
}();

IRS_FORCE_INLINE inline uint32_t CompactLanes(__m128i lanes, uint32_t mask,
                                              void* out) noexcept {
  const auto control = _mm_loadu_si128(
    reinterpret_cast<const __m128i*>(kCompactLanes[mask].data()));
  _mm_storeu_si128(static_cast<__m128i*>(out),
                   _mm_shuffle_epi8(lanes, control));
  return static_cast<uint32_t>(std::popcount(mask));
}

IRS_FORCE_INLINE inline void CompactBlock(__m256i lanes, uint32_t keep,
                                          uint32_t* out) noexcept {
  const auto low = CompactLanes(_mm256_castsi256_si128(lanes), keep & 15, out);
  CompactLanes(_mm256_extracti128_si256(lanes, 1), keep >> 4, out + low);
}
#endif

template<typename Excludes>
IRS_FORCE_INLINE uint32_t ExcludeWords(const Excludes& excludes,
                                       doc_id_t* IRS_RESTRICT docs,
                                       score_t* IRS_RESTRICT scores, uint32_t i,
                                       uint32_t len) {
  uint32_t kept = i;
#ifdef __AVX2__
  static_assert(Excludes::kBits == 64);
  const auto count = excludes.WordCount();
  SDB_ASSERT(count != 0);
  const auto* const words = excludes.Words();
  const auto low = _mm256_set1_epi64x(Excludes::kBits - 1);
  const auto one = _mm256_set1_epi64x(1);
  auto* const scores_bits = reinterpret_cast<uint32_t*>(scores);
  const auto test = [&](__m128i rel, __m256i word0, __m256i word1) {
    const auto at = _mm256_cvtepu32_epi64(rel);
    const auto word =
      _mm256_blendv_epi8(word0, word1, _mm256_cmpgt_epi64(at, low));
    const auto bit =
      _mm256_and_si256(_mm256_srlv_epi64(word, _mm256_and_si256(at, low)), one);
    return static_cast<uint32_t>(
      _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(bit, one))));
  };
  while (i + 8 <= len) {
    const auto first = docs[i] - Excludes::kMin;
    const auto w = first / Excludes::kBits;
    if ((docs[i + 7] - Excludes::kMin) / Excludes::kBits - w > 1) {
      for (const auto end = i + 8; i != end; ++i) {
        const auto doc = docs[i];
        docs[kept] = doc;
        scores[kept] = scores[i];
        kept += static_cast<uint32_t>(!excludes.Test(doc));
      }
      continue;
    }
    const auto word0 =
      _mm256_set1_epi64x(w < count ? static_cast<long long>(words[w]) : 0);
    const auto word1 = _mm256_set1_epi64x(
      w + 1 < count ? static_cast<long long>(words[w + 1]) : 0);
    const auto doc =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(docs + i));
    const auto score =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(scores_bits + i));
    const auto rel = _mm256_sub_epi32(
      doc, _mm256_set1_epi32(
             static_cast<int>(Excludes::kMin + w * Excludes::kBits)));
    const auto hit = test(_mm256_castsi256_si128(rel), word0, word1) |
                     test(_mm256_extracti128_si256(rel, 1), word0, word1) << 4;
    const auto keep = ~hit & 0xFF;
    CompactBlock(doc, keep, docs + kept);
    CompactBlock(score, keep, scores_bits + kept);
    kept += static_cast<uint32_t>(std::popcount(keep));
    i += 8;
  }
#endif
  for (; i != len; ++i) {
    const auto doc = docs[i];
    docs[kept] = doc;
    scores[kept] = scores[i];
    kept += static_cast<uint32_t>(!excludes.Test(doc));
  }
  return kept;
}

template<typename Excludes>
IRS_FORCE_INLINE uint32_t ExcludeBlock(Excludes& excludes,
                                       doc_id_t* IRS_RESTRICT docs,
                                       score_t* IRS_RESTRICT scores,
                                       uint32_t len) {
  if (len == 0) {
    return len;
  }
  const auto next = excludes.Probe(docs[0]);
  if (next > docs[len - 1]) {
    return len;
  }
  uint32_t i = 0;
  for (auto step = std::bit_floor(len); step != 0; step >>= 1) {
    i += (i + step <= len && docs[i + step - 1] < next) ? step : 0;
  }
  if constexpr (requires { excludes.Words(); }) {
    return ExcludeWords(excludes, docs, scores, i, len);
  }
  uint32_t kept = i;
  for (; i != len; ++i) {
    const auto doc = docs[i];
    docs[kept] = doc;
    scores[kept] = scores[i];
    kept += static_cast<uint32_t>(!IsExcluded(excludes, doc));
  }
  return kept;
}

template<typename Excludes>
IRS_FORCE_INLINE uint32_t CountExcluded(Excludes& excludes,
                                        const doc_id_t* docs, uint32_t len) {
  uint32_t excluded = 0;
  if constexpr (requires { excludes.Words(); }) {
    for (uint32_t i = 0; i != len; ++i) {
      excluded += static_cast<uint32_t>(excludes.Test(docs[i]));
    }
  } else {
    for (uint32_t i = 0; i != len;) {
      const auto next = excludes.Probe(docs[i]);
      if (next == docs[i]) {
        ++excluded;
        ++i;
      } else {
        i = static_cast<uint32_t>(
          std::lower_bound(docs + i + 1, docs + len, next) - docs);
      }
    }
  }
  return excluded;
}

}  // namespace irs::detail
