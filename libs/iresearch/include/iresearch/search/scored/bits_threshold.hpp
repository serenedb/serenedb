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

#include "basics/bit_utils.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/scored/root.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::scored {

template<typename Leaves, typename Table>
class BitsThreshold : public Root {
 public:
  template<typename LeavesArgs>
  BitsThreshold(Table table, std::piecewise_construct_t, LeavesArgs&& leaves,
                uint32_t min_match, score_t absorbed = 0)
    : _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _planes(size_t{min_match} * search::kWindowWords, 0),
      _min_match{min_match},
      _constant{absorbed},
      _table{table} {
    SDB_ASSERT(_min_match > 1);
    std::fill_n(_window, search::kWindowDocs, _constant);
  }

  uint32_t Run(doc_id_t* IRS_RESTRICT out, score_t* IRS_RESTRICT scores,
               uint32_t capacity) final {
    SDB_ASSERT(capacity >= doc_limits::kMinCapacity);
    uint32_t n = 0;
    auto* const planes = _planes.data();
    const auto top = size_t{_min_match} - 1;
    auto* const answers = planes + top * search::kWindowWords;

    for (;;) {
      for (; _word != search::kWindowWords; ++_word) {
        auto word = answers[_word];
        const size_t base = _word * search::kWindowBits;
        n = DrainWord(word, base, out, scores, n, capacity);
        if (word != 0) {
          answers[_word] = word;
          return n;
        }
        answers[_word] = 0;
        auto touched = planes[_word];
        planes[_word] = 0;
        while (touched != 0) {
          _window[base + static_cast<uint32_t>(std::countr_zero(touched))] =
            _constant;
          touched = PopBit(touched);
        }
      }

      if (_leaves.Live() < _min_match) {
        return n;
      }
      if (!_table.Skip(_next)) {
        return n;
      }
      _min = _next;
      std::fill(_planes.begin(), _planes.end(), uint64_t{0});
      _next = _leaves.Visit(_min + search::kWindowDocs, [&](auto& leaf) {
        search::Clear(_scratch.data(), search::kWindowWords);
        const auto doc =
          leaf.Fill(_min, _min + search::kWindowDocs, _scratch.data(), _window);
        search::FoldCarry(planes, _scratch.data(), search::kWindowWords, top);
        return doc;
      });
      _word = 0;
    }
  }

 private:
  uint32_t DrainWord(uint64_t& word, size_t base, doc_id_t* IRS_RESTRICT out,
                     score_t* IRS_RESTRICT scores, uint32_t n,
                     uint32_t capacity) noexcept {
    while (word != 0) {
      if (n == capacity) {
        return n;
      }
      const size_t offset =
        base + static_cast<uint32_t>(std::countr_zero(word));
      out[n] = _min + static_cast<doc_id_t>(offset);
      scores[n] = _window[offset];
      ++n;
      word = PopBit(word);
    }
    return n;
  }

  ABSL_CACHELINE_ALIGNED score_t _window[search::kWindowDocs]{};
  search::Scratch _scratch{};
  Leaves _leaves;
  std::vector<uint64_t> _planes;
  doc_id_t _min = 0;
  doc_id_t _next = 0;
  uint32_t _word = search::kWindowWords;
  uint32_t _min_match;
  score_t _constant;
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::scored
