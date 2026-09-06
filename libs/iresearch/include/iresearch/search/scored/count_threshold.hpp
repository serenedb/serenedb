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

#include "basics/bit_utils.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/scored/root.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::scored {

template<typename Leaves, typename Table>
class CountThreshold : public Root {
 public:
  static constexpr size_t kNumWords = search::kWindowWords;
  static constexpr doc_id_t kWindow = search::kWindowDocs;

  template<typename LeavesArgs>
  CountThreshold(Table table, std::piecewise_construct_t, LeavesArgs&& leaves,
                 uint32_t min_match, score_t absorbed = 0)
    : _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _min_match{min_match},
      _constant{absorbed},
      _table{table} {
    SDB_ASSERT(_min_match > 1);
    std::fill_n(_window, kWindow, _constant);
  }

  uint32_t Run(doc_id_t* IRS_RESTRICT out, score_t* IRS_RESTRICT scores,
               uint32_t capacity) final {
    SDB_ASSERT(capacity >= doc_limits::kMinCapacity);
    SDB_ASSERT(capacity != 0);
    uint32_t n = 0;

    for (;;) {
      for (; _word != kNumWords; ++_word) {
        auto word = _mask[_word];
        n = DrainWord(word, _word * BitsRequired<uint64_t>(), out, scores, n,
                      capacity);
        if (word != 0) {
          _mask[_word] = word;
          return n;
        }
        _mask[_word] = 0;
      }

      if (_leaves.Live() < _min_match) {
        return n;
      }
      if (!_table.Skip(_next)) {
        return n;
      }
      _min = _next;
      _next = _leaves.Visit(
        _min + kWindow,
        [min = _min, max = _min + kWindow, this](auto& leaf) IRS_FORCE_INLINE {
          return leaf.Count(min, max, _counts, _mask, _window);
        });
      _word = 0;
    }
  }

 private:
  uint32_t DrainWord(uint64_t& word, size_t base, doc_id_t* IRS_RESTRICT out,
                     score_t* IRS_RESTRICT scores, uint32_t n,
                     uint32_t capacity) noexcept {
    while (word != 0) {
      const size_t offset =
        base + static_cast<uint32_t>(std::countr_zero(word));
      if (_counts[offset] >= _min_match) {
        if (n == capacity) {
          return n;
        }
        out[n] = _min + static_cast<doc_id_t>(offset);
        scores[n] = _window[offset];
        ++n;
      }
      _counts[offset] = 0;
      _window[offset] = _constant;
      word = PopBit(word);
    }
    return n;
  }

  ABSL_CACHELINE_ALIGNED uint64_t _mask[kNumWords]{};
  ABSL_CACHELINE_ALIGNED uint32_t _counts[kWindow]{};
  ABSL_CACHELINE_ALIGNED score_t _window[kWindow]{};
  Leaves _leaves;
  doc_id_t _min = 0;
  doc_id_t _next = 0;
  uint32_t _word = kNumWords;
  uint32_t _min_match;
  score_t _constant;
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::scored
