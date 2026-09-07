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
#include <type_traits>
#include <utility>

#include "basics/empty.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/count/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::count {

template<typename Leaves, typename Table>
class WindowDisjunction : public Root {
 public:
  template<typename LeavesArgs>
  WindowDisjunction(Table table, std::piecewise_construct_t,
                    LeavesArgs&& leaves)
    : _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _table{table} {}

  uint64_t Run() final {
    uint64_t total = 0;
    doc_id_t min = 0;

    while (!_leaves.Empty()) {
      if (!_table.Skip(min)) {
        return total;
      }
      SDB_ASSERT(min <= doc_limits::eof() - search::kWindowDocs);
      const doc_id_t max = min + search::kWindowDocs;

      const auto next = _leaves.Visit(
        max, [&](auto& leaf) { return leaf.FillOr(min, max, _mask.data()); });

      total += _table.CountAndClear(min, _mask.data(), search::kWindowWords);

      min = next;
    }

    return total;
  }

 private:
  search::Scratch _mask{};
  Leaves _leaves;
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::count
