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

#include "basics/bit_utils.hpp"
#include "basics/shared.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

inline IRS_FORCE_INLINE uint32_t FilterScores(doc_id_t* IRS_RESTRICT docs,
                                              score_t* IRS_RESTRICT scores,
                                              uint32_t len,
                                              score_t bound) noexcept {
  uint32_t out = 0;
  uint32_t i = 0;

#ifdef __AVX2__
  const auto edge = _mm256_set1_ps(bound);
  for (; i + 8 <= len; i += 8) {
    const auto values = _mm256_loadu_ps(scores + i);
    const auto keep = _mm256_cmp_ps(values, edge, _CMP_GT_OQ);
    const auto mask = static_cast<uint32_t>(_mm256_movemask_ps(keep));
    const auto control = LeftPackControl(mask);
    const auto ids =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(docs + i));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(docs + out),
                        _mm256_permutevar8x32_epi32(ids, control));
    _mm256_storeu_ps(scores + out, _mm256_permutevar8x32_ps(values, control));
    out += static_cast<uint32_t>(std::popcount(mask));
  }
#endif

  for (; i != len; ++i) {
    const auto score = scores[i];
    docs[out] = docs[i];
    scores[out] = score;
    out += static_cast<uint32_t>(score > bound);
  }
  return out;
}

}  // namespace irs::search
