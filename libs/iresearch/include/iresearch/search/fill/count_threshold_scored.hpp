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
#include <utility>

#include "basics/assert.h"
#include "basics/bit_utils.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

template<typename Leaves>
class CountThresholdScored {
 public:
  template<typename LeavesArgs>
  CountThresholdScored(ScoreMergeType merge, std::piecewise_construct_t,
                       LeavesArgs&& leaves, uint32_t min_match,
                       score_t absorbed = 0)
    : _outer{merge},
      _constant{absorbed},
      _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _min_match{min_match} {
    SDB_ASSERT(_min_match > 1);
    std::fill_n(_window, search::kWindowDocs, _constant);
  }

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                score_t* IRS_RESTRICT scores) {
    if (_leaves.Live() < _min_match) {
      return doc_limits::eof();
    }
    return irs::ResolveMergeType(_outer, [&]<ScoreMergeType Outer> {
      return FillImpl<Outer>(min, max, mask, scores);
    });
  }

 private:
  template<ScoreMergeType Outer>
  doc_id_t FillImpl(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                    score_t* IRS_RESTRICT scores) {
    const auto words = search::WindowWords(min, max);
    const auto next = _leaves.Visit(max, [&](auto& leaf) {
      return leaf.Count(min, max, _counts, _touched, _window);
    });

    for (size_t w = 0; w != words; ++w) {
      const auto base = w * search::kWindowBits;
      auto touched = _touched[w];
      _touched[w] = 0;
      uint64_t answers = 0;
      while (touched != 0) {
        const auto bit = static_cast<uint32_t>(std::countr_zero(touched));
        const auto offset = base + bit;
        if (_counts[offset] >= _min_match) {
          answers |= uint64_t{1} << bit;
          irs::Merge<Outer>(scores[offset], _window[offset]);
        }
        _counts[offset] = 0;
        _window[offset] = _constant;
        touched = PopBit(touched);
      }
      mask[w] |= answers;
    }
    return next;
  }

  ABSL_CACHELINE_ALIGNED uint64_t _touched[search::kWindowWords]{};
  ABSL_CACHELINE_ALIGNED uint32_t _counts[search::kWindowDocs]{};
  ABSL_CACHELINE_ALIGNED score_t _window[search::kWindowDocs]{};
  ScoreMergeType _outer;
  score_t _constant;
  Leaves _leaves;
  uint32_t _min_match;
};

}  // namespace irs::fill
