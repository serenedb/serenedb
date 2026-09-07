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
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "iresearch/search/common/window.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

template<typename Leaves>
class BitsThresholdDocs {
 public:
  template<typename LeavesArgs>
  BitsThresholdDocs(std::piecewise_construct_t, LeavesArgs&& leaves,
                    uint32_t min_match)
    : _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _planes(size_t{min_match} * search::kWindowWords, 0),
      _min_match{min_match} {
    SDB_ASSERT(_min_match > 1);
  }

  doc_id_t FillOr(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    if (_leaves.Live() < _min_match) {
      return doc_limits::eof();
    }
    const auto words = search::WindowWords(min, max);
    auto* const planes = _planes.data();
    const auto top = size_t{_min_match} - 1;

    bool first = true;
    const auto next = _leaves.Visit(max, [&](auto& leaf) {
      if (first) {
        first = false;
        return leaf.FillOr(min, max, planes);
      }
      search::Clear(_window.data(), words);
      const auto doc = leaf.FillOr(min, max, _window.data());
      search::FoldCarry(planes, _window.data(), words, top);
      return doc;
    });

    const auto* const answers = planes + top * search::kWindowWords;
    for (size_t w = 0; w != words; ++w) {
      mask[w] |= answers[w];
    }
    std::fill(_planes.begin(), _planes.end(), uint64_t{0});

    return next;
  }

 private:
  search::Scratch _window{};
  Leaves _leaves;
  std::vector<uint64_t> _planes;
  uint32_t _min_match;
};

}  // namespace irs::fill
