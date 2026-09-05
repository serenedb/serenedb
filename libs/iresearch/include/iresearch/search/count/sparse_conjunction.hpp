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

#include <array>
#include <cstdint>
#include <tuple>
#include <type_traits>
#include <utility>

#include "basics/empty.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/count/root.hpp"
#include "iresearch/search/lead/concept.hpp"
#include "iresearch/search/probe/concept.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::count {

template<lead::Type Head, probe::Type Tail, typename Table>
class SparseConjunction : public Root {
 public:
  static constexpr bool kTable = !std::is_same_v<Table, utils::Empty>;
  static constexpr uint32_t kRun = 2048;

  template<typename HeadArgs, typename TailArgs>
  SparseConjunction(Table table, std::piecewise_construct_t, HeadArgs&& head,
                    TailArgs&& tail)
    : _head{std::make_from_tuple<Head>(std::forward<HeadArgs>(head))},
      _tail{std::make_from_tuple<Tail>(std::forward<TailArgs>(tail))},
      _table{table} {}

  SparseConjunction(SparseConjunction&&) = delete;
  SparseConjunction& operator=(SparseConjunction&&) = delete;

  uint64_t Run() final {
    uint64_t total = 0;
    auto doc = _head.Advance();
    uint32_t n = 0;

    while (!doc_limits::eof(doc)) {
      if constexpr (kTable) {
        const auto live = _table.Live(doc);
        if (live != doc) {
          doc = _head.Seek(live);
          continue;
        }
      }
      const auto probe = _tail.Probe(doc);
      if (probe == doc) {
        if constexpr (kTable) {
          _docs[n++] = doc;
          if (n == kRun) {
            total += _table.Run(_docs.data(), nullptr, n);
            n = 0;
          }
        } else {
          ++total;
        }
        doc = _head.Advance();
      } else {
        doc = _head.Seek(probe);
      }
    }

    if constexpr (kTable) {
      if (n != 0) {
        total += _table.Run(_docs.data(), nullptr, n);
      }
    }
    return total;
  }

 private:
  Head _head;
  Tail _tail;
  [[no_unique_address]] utils::Need<kTable, std::array<doc_id_t, kRun>> _docs;
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::count
