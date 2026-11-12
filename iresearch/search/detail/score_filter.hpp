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
#include <cstdint>

#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

#ifdef __AVX2__
template<bool Freqs>
IRS_FORCE_INLINE void PackKept(doc_id_t* IRS_RESTRICT docs,
                               [[maybe_unused]] uint32_t* IRS_RESTRICT freqs,
                               score_t* IRS_RESTRICT scores, uint32_t at,
                               __m256 values, __m256 keep,
                               uint32_t& out) noexcept {
  const auto mask = static_cast<uint32_t>(_mm256_movemask_ps(keep));
  const auto control = LeftPackControl(mask);
  const auto ids =
    _mm256_loadu_si256(reinterpret_cast<const __m256i*>(docs + at));
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(docs + out),
                      _mm256_permutevar8x32_epi32(ids, control));
  if constexpr (Freqs) {
    const auto counts =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(freqs + at));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(freqs + out),
                        _mm256_permutevar8x32_epi32(counts, control));
  }
  _mm256_storeu_ps(scores + out, _mm256_permutevar8x32_ps(values, control));
  out += static_cast<uint32_t>(std::popcount(mask));
}
#endif

template<bool Freqs>
IRS_FORCE_INLINE uint32_t FilterScoresImpl(
  doc_id_t* IRS_RESTRICT docs, [[maybe_unused]] uint32_t* IRS_RESTRICT freqs,
  score_t* IRS_RESTRICT scores, uint32_t len, score_t bound) noexcept {
  uint32_t out = 0;
  uint32_t i = 0;

#ifdef __AVX2__
  const auto edge = _mm256_set1_ps(bound);
  for (; i + 32 <= len; i += 32) {
    const auto v0 = _mm256_loadu_ps(scores + i);
    const auto v1 = _mm256_loadu_ps(scores + i + 8);
    const auto v2 = _mm256_loadu_ps(scores + i + 16);
    const auto v3 = _mm256_loadu_ps(scores + i + 24);
    const auto k0 = _mm256_cmp_ps(v0, edge, _CMP_GT_OQ);
    const auto k1 = _mm256_cmp_ps(v1, edge, _CMP_GT_OQ);
    const auto k2 = _mm256_cmp_ps(v2, edge, _CMP_GT_OQ);
    const auto k3 = _mm256_cmp_ps(v3, edge, _CMP_GT_OQ);
    if (_mm256_movemask_ps(
          _mm256_or_ps(_mm256_or_ps(k0, k1), _mm256_or_ps(k2, k3))) == 0) {
      continue;
    }
    PackKept<Freqs>(docs, freqs, scores, i, v0, k0, out);
    PackKept<Freqs>(docs, freqs, scores, i + 8, v1, k1, out);
    PackKept<Freqs>(docs, freqs, scores, i + 16, v2, k2, out);
    PackKept<Freqs>(docs, freqs, scores, i + 24, v3, k3, out);
  }
  for (; i + 8 <= len; i += 8) {
    const auto values = _mm256_loadu_ps(scores + i);
    PackKept<Freqs>(docs, freqs, scores, i, values,
                    _mm256_cmp_ps(values, edge, _CMP_GT_OQ), out);
  }
#endif

  for (; i != len; ++i) {
    const auto score = scores[i];
    docs[out] = docs[i];
    if constexpr (Freqs) {
      freqs[out] = freqs[i];
    }
    scores[out] = score;
    out += static_cast<uint32_t>(score > bound);
  }
  return out;
}

inline IRS_FORCE_INLINE uint32_t FilterScores(doc_id_t* IRS_RESTRICT docs,
                                              score_t* IRS_RESTRICT scores,
                                              uint32_t len,
                                              score_t bound) noexcept {
  return FilterScoresImpl<false>(docs, nullptr, scores, len, bound);
}

inline IRS_FORCE_INLINE uint32_t FilterScores(doc_id_t* IRS_RESTRICT docs,
                                              uint32_t* IRS_RESTRICT freqs,
                                              score_t* IRS_RESTRICT scores,
                                              uint32_t len,
                                              score_t bound) noexcept {
  return FilterScoresImpl<true>(docs, freqs, scores, len, bound);
}

inline IRS_FORCE_INLINE uint32_t FilterFreqs(
  const doc_id_t* IRS_RESTRICT docs, const uint32_t* IRS_RESTRICT freqs,
  uint32_t len, uint32_t least, doc_id_t* IRS_RESTRICT kept_docs,
  uint32_t* IRS_RESTRICT kept_freqs) noexcept {
  uint32_t out = 0;
  uint32_t i = 0;
#ifdef __AVX2__
  const auto edge = _mm256_set1_epi32(static_cast<int>(least - 1));
  for (; i + 8 <= len; i += 8) {
    const auto values =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(freqs + i));
    const auto mask = static_cast<uint32_t>(_mm256_movemask_ps(
      _mm256_castsi256_ps(_mm256_cmpgt_epi32(values, edge))));
    if (mask == 0) {
      continue;
    }
    const auto control = LeftPackControl(mask);
    const auto ids =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(docs + i));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(kept_docs + out),
                        _mm256_permutevar8x32_epi32(ids, control));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(kept_freqs + out),
                        _mm256_permutevar8x32_epi32(values, control));
    out += static_cast<uint32_t>(std::popcount(mask));
  }
#endif
  for (; i != len; ++i) {
    kept_docs[out] = docs[i];
    kept_freqs[out] = freqs[i];
    out += static_cast<uint32_t>(freqs[i] >= least);
  }
  return out;
}

template<typename Docs, typename Scores>
IRS_FORCE_INLINE void MergeHits(bool required, const doc_id_t* hit_docs,
                                const uint32_t* slots,
                                const score_t* hit_scores, uint32_t hits,
                                Docs& docs, Scores& scores, uint32_t& out) {
  if (required) {
    for (uint32_t j = 0; j != hits; ++j) {
      docs[out] = hit_docs[j];
      scores[out] = scores[slots[j]] + hit_scores[j];
      ++out;
    }
  } else {
    for (uint32_t j = 0; j != hits; ++j) {
      scores[slots[j]] += hit_scores[j];
    }
  }
}

}  // namespace irs::detail
