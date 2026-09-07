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
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

template<typename Leaves>
class CountThresholdDocs {
 public:
  template<typename LeavesArgs>
  CountThresholdDocs(std::piecewise_construct_t, LeavesArgs&& leaves,
                     uint32_t min_match)
    : _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _min_match{min_match} {
    SDB_ASSERT(_min_match > 1);
  }

  doc_id_t FillOr(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    if (_leaves.Live() < _min_match) {
      return doc_limits::eof();
    }
    const auto words = search::WindowWords(min, max);
    const auto next = _leaves.Visit(max, [&](auto& leaf) {
      if constexpr (requires { leaf.Count(min, max, _counts); }) {
        return leaf.Count(min, max, _counts);
      } else {
        search::Clear(_own.data(), words);
        const auto doc = leaf.FillOr(min, max, _own.data());
        Tally(words);
        return doc;
      }
    });

    for (size_t w = 0; w != words; ++w) {
      auto* const counts = _counts + w * search::kWindowBits;
      uint64_t word = 0;
      for (uint32_t i = 0; i != search::kWindowBits; ++i) {
        word |= uint64_t{counts[i] >= _min_match} << i;
      }
      std::fill_n(counts, search::kWindowBits, uint32_t{0});
      mask[w] |= word;
    }
    return next;
  }

 private:
  void Tally(size_t words) noexcept {
    for (size_t w = 0; w != words; ++w) {
      auto word = _own[w];
      auto* const base = _counts + w * search::kWindowBits;
      while (word != 0) {
        ++base[static_cast<uint32_t>(std::countr_zero(word))];
        word = PopBit(word);
      }
    }
  }

  ABSL_CACHELINE_ALIGNED uint32_t _counts[search::kWindowDocs]{};
  search::Scratch _own{};
  Leaves _leaves;
  uint32_t _min_match;
};

}  // namespace irs::fill
