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

#include "iresearch/search/common/score/root_window.hpp"

#include <bit>

#include "basics/bit_utils.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs::search {
namespace {

template<ScoreMergeType Inner>
IRS_FORCE_INLINE void MergeHeld(score_t* IRS_RESTRICT window,
                                const uint64_t* IRS_RESTRICT mask, size_t words,
                                score_t constant) noexcept {
  for (size_t w = 0; w != words; ++w) {
    auto bits = mask[w];
    const size_t base = w * BitsRequired<uint64_t>();
    while (bits != 0) {
      irs::Merge<Inner>(
        window[base + static_cast<uint32_t>(std::countr_zero(bits))], constant);
      bits = PopBit(bits);
    }
  }
}

}  // namespace

void RootWindowScore::Apply(score_t* IRS_RESTRICT window,
                            const uint64_t* IRS_RESTRICT mask,
                            size_t words) const noexcept {
  if (_constant == 0) {
    return;
  }
  if (_inner == ScoreMergeType::Max) {
    MergeHeld<ScoreMergeType::Max>(window, mask, words, _constant);
  } else {
    MergeHeld<ScoreMergeType::Sum>(window, mask, words, _constant);
  }
}

}  // namespace irs::search
