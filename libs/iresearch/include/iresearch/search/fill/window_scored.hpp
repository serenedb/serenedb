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
#include <utility>

#include "basics/bit_utils.hpp"
#include "basics/shared.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/fill/concept.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

template<ScoredType Child>
class WindowScored {
 public:
  template<typename... Args>
  explicit WindowScored(ScoreMergeType merge, Args&&... args)
    : _child{std::forward<Args>(args)...}, _merge{merge} {}

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                score_t* IRS_RESTRICT scores) {
    const auto next = _child.Fill(min, max, _mask.data(), _scores);
    const auto words = search::WindowWords(min, max);
    irs::ResolveMergeType(
      _merge, [&]<ScoreMergeType Merge> { Fold<Merge>(mask, scores, words); });
    return next;
  }

 private:
  template<ScoreMergeType Merge>
  IRS_FORCE_INLINE void Fold(uint64_t* IRS_RESTRICT mask,
                             score_t* IRS_RESTRICT scores,
                             size_t words) noexcept {
    for (size_t w = 0; w != words; ++w) {
      auto bits = std::exchange(_mask[w], uint64_t{0});
      mask[w] |= bits;
      const size_t base = w * search::kWindowBits;
      while (bits != 0) {
        const auto doc = base + static_cast<uint32_t>(std::countr_zero(bits));
        irs::Merge<Merge>(scores[doc], std::exchange(_scores[doc], 0.f));
        bits = PopBit(bits);
      }
    }
  }

  search::Scratch _mask{};
  ABSL_CACHELINE_ALIGNED score_t _scores[search::kWindowDocs]{};
  Child _child;
  ScoreMergeType _merge;
};

template<typename Child>
using ByWindowScored = Impl<WindowScored<Child>>;

}  // namespace irs::fill
