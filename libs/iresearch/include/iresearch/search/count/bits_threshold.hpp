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
#include <cstdint>
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/count/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::count {

template<typename Leaves, typename Table>
class BitsThreshold : public Root {
 public:
  template<typename LeavesArgs>
  BitsThreshold(Table table, std::piecewise_construct_t, LeavesArgs&& leaves,
                uint32_t min_match)
    : _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _planes(size_t{min_match} * search::kWindowWords, 0),
      _min_match{min_match},
      _table{table} {
    SDB_ASSERT(_min_match > 1);
  }

  uint64_t Run() final {
    uint64_t total = 0;
    doc_id_t min = 0;
    auto* const planes = _planes.data();
    const auto top = size_t{_min_match} - 1;

    while (_leaves.Live() >= _min_match) {
      if (!_table.Skip(min)) {
        return total;
      }
      SDB_ASSERT(min <= doc_limits::eof() - search::kWindowDocs);
      const doc_id_t max = min + search::kWindowDocs;

      bool first = true;
      const auto next = _leaves.Visit(max, [&](auto& leaf) {
        if (first) {
          first = false;
          return leaf.FillOr(min, max, planes);
        }
        search::Clear(_window.data(), search::kWindowWords);
        const auto doc = leaf.FillOr(min, max, _window.data());
        search::FoldCarry(planes, _window.data(), search::kWindowWords, top);
        return doc;
      });

      total += _table.Count(min, planes + top * search::kWindowWords,
                            search::kWindowWords);
      std::fill(_planes.begin(), _planes.end(), uint64_t{0});

      min = next;
    }

    return total;
  }

 private:
  search::Scratch _window{};
  Leaves _leaves;
  std::vector<uint64_t> _planes;
  uint32_t _min_match;
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::count
