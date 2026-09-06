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
#include <vector>

#include "basics/assert.h"
#include "basics/bit_utils.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

template<typename Leaves>
class BitsThresholdScored {
 public:
  template<typename LeavesArgs>
  BitsThresholdScored(ScoreMergeType merge, std::piecewise_construct_t,
                      LeavesArgs&& leaves, uint32_t min_match,
                      score_t absorbed = 0)
    : _outer{merge},
      _constant{absorbed},
      _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _planes(size_t{min_match} * search::kWindowWords, 0),
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
    auto* const planes = _planes.data();
    const auto top = size_t{_min_match} - 1;

    const auto next = _leaves.Visit(max, [&](auto& leaf) {
      search::Clear(_scratch.data(), words);
      const auto doc = leaf.Fill(min, max, _scratch.data(), _window);
      search::FoldCarry(planes, _scratch.data(), words, top);
      return doc;
    });

    const auto* const answers = planes + top * search::kWindowWords;
    for (size_t w = 0; w != words; ++w) {
      const auto base = w * search::kWindowBits;
      auto answer = answers[w];
      mask[w] |= answer;
      while (answer != 0) {
        const auto offset =
          base + static_cast<uint32_t>(std::countr_zero(answer));
        irs::Merge<Outer>(scores[offset], _window[offset]);
        answer = PopBit(answer);
      }
      auto touched = planes[w];
      while (touched != 0) {
        _window[base + static_cast<uint32_t>(std::countr_zero(touched))] =
          _constant;
        touched = PopBit(touched);
      }
    }
    std::fill(_planes.begin(), _planes.end(), uint64_t{0});
    return next;
  }

  ABSL_CACHELINE_ALIGNED score_t _window[search::kWindowDocs]{};
  search::Scratch _scratch{};
  ScoreMergeType _outer;
  score_t _constant;
  Leaves _leaves;
  std::vector<uint64_t> _planes;
  uint32_t _min_match;
};

}  // namespace irs::fill
