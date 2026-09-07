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
#include <type_traits>
#include <utility>

#include "basics/bit_utils.hpp"
#include "basics/empty.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/scored/root.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::scored {

template<typename Leaves, typename Excludes, typename Table>
class WindowDisjunction : public Root {
 public:
  static constexpr size_t kNumWords = search::kWindowWords;
  static constexpr doc_id_t kWindow = search::kWindowDocs;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;

  template<typename LeavesArgs, typename ExcludesArgs>
  WindowDisjunction(Table table, std::piecewise_construct_t,
                    LeavesArgs&& leaves, ExcludesArgs&& excludes,
                    score_t absorbed = 0)
    : _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))},
      _constant{absorbed},
      _table{table} {
    std::fill_n(_window, kWindow, _constant);
  }

  uint32_t Run(doc_id_t* IRS_RESTRICT out, score_t* IRS_RESTRICT scores,
               uint32_t capacity) final {
    SDB_ASSERT(capacity >= doc_limits::kMinCapacity);
    SDB_ASSERT(capacity >= BitsRequired<uint64_t>());
    uint32_t n = 0;

    for (;;) {
      const score_t* IRS_RESTRICT const window = _window;
      const auto min = _min;
      for (; _word != kNumWords; ++_word) {
        const auto word = _mask[_word];
        if (word == 0) {
          continue;
        }
        if (n + BitsRequired<uint64_t>() > capacity) [[unlikely]] {
          if (n + static_cast<uint32_t>(std::popcount(word)) > capacity) {
            return n;
          }
        }
        _mask[_word] = 0;
        const auto base = _word * BitsRequired<uint64_t>();
        const auto first = n;
        n = static_cast<uint32_t>(
          MaterializeWord(min + static_cast<doc_id_t>(base), word, out + n) -
          out);
        const auto padded = first + ((n - first + 7) & ~uint32_t{7});
        for (auto i = first; i != padded; i += 8) {
          for (uint32_t j = 0; j != 8; ++j) {
            scores[i + j] = window[out[i + j] - min];
          }
        }
        std::fill_n(_window + base, BitsRequired<uint64_t>(), _constant);
      }

      if (_leaves.Empty()) {
        return n;
      }
      if (!_table.Skip(_next)) {
        return n;
      }
      _min = _next;
      _next = _leaves.Visit(
        _min + kWindow,
        [min = _min, max = _min + kWindow, this](auto& leaf)
          IRS_FORCE_INLINE { return leaf.Fill(min, max, _mask, _window); });
      if constexpr (kExcludes) {
        _excludes.Remove(_min, _min + kWindow, _mask, _window, _constant);
      }
      _word = 0;
    }
  }

 private:
  ABSL_CACHELINE_ALIGNED uint64_t _mask[kNumWords]{};
  ABSL_CACHELINE_ALIGNED score_t _window[kWindow];
  Leaves _leaves;
  [[no_unique_address]] Excludes _excludes;
  doc_id_t _min = doc_limits::min();
  doc_id_t _next = 0;
  uint32_t _word = kNumWords;
  score_t _constant;
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::scored
