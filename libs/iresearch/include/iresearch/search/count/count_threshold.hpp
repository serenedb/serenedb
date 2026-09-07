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
#include <cstdint>
#include <utility>

#include "basics/empty.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/count/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::count {

template<typename Leaves, typename Table>
class CountThreshold : public Root {
 public:
  static constexpr bool kTable = !std::is_same_v<Table, utils::Empty>;

  template<typename LeavesArgs>
  CountThreshold(Table table, std::piecewise_construct_t, LeavesArgs&& leaves,
                 uint32_t min_match)
    : _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _min_match{min_match},
      _table{table} {
    SDB_ASSERT(_min_match > 1);
  }

  uint64_t Run() final {
    uint64_t total = 0;
    doc_id_t min = doc_limits::min();

    while (_leaves.Live() >= _min_match) {
      if (!_table.Skip(min)) {
        return total;
      }
      SDB_ASSERT(min <= doc_limits::eof() - search::kWindowDocs);
      const doc_id_t max = min + search::kWindowDocs;

      const auto next = _leaves.Visit(
        max, [&](auto& leaf) { return leaf.Count(min, max, _counts); });

      for (uint32_t w = 0; w != search::kWindowWords; ++w) {
        auto* const counts = _counts + w * search::kWindowBits;
        uint64_t word = 0;
        for (uint32_t i = 0; i != search::kWindowBits; ++i) {
          word |= uint64_t{counts[i] >= _min_match} << i;
        }
        std::fill_n(counts, search::kWindowBits, uint32_t{0});
        if constexpr (kTable) {
          _mask[w] = word;
        } else {
          total += static_cast<uint64_t>(std::popcount(word));
        }
      }
      if constexpr (kTable) {
        total += _table.CountAndClear(min, _mask.data(), search::kWindowWords);
      }

      min = next;
    }

    return total;
  }

 private:
  alignas(64) uint32_t _counts[search::kWindowDocs]{};
  Leaves _leaves;
  uint32_t _min_match;
  [[no_unique_address]] utils::Need<kTable, search::Scratch> _mask;
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::count
