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

#include <tuple>
#include <utility>

#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/docs/root.hpp"
#include "iresearch/search/lead/concept.hpp"
#include "iresearch/search/probe/concept.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

template<lead::Type Head, probe::Type Tail, typename Table>
class SparseConjunction : public Root {
 public:
  static constexpr bool kTable = !std::is_same_v<Table, utils::Empty>;

  template<typename HeadArgs, typename TailArgs>
  SparseConjunction(Table table, std::piecewise_construct_t, HeadArgs&& main,
                    TailArgs&& other)
    : _main{std::make_from_tuple<Head>(std::forward<HeadArgs>(main))},
      _other{std::make_from_tuple<Tail>(std::forward<TailArgs>(other))},
      _table{table} {}

  SparseConjunction(SparseConjunction&&) = delete;
  SparseConjunction& operator=(SparseConjunction&&) = delete;

  uint32_t Run(doc_id_t* IRS_RESTRICT out, uint32_t capacity) final {
    SDB_ASSERT(capacity >= doc_limits::kMinCapacity);
    uint32_t n = 0;
    if (_spent) {
      return 0;
    }
    auto doc = _main.Advance();

    while (!doc_limits::eof(doc)) {
      if constexpr (kTable) {
        if (const auto live = _table.Live(doc); live != doc) {
          doc = _main.Seek(live);
          continue;
        }
      }
      const auto probe = _other.Probe(doc);
      if (probe != doc) {
        doc = _main.Seek(probe);
        continue;
      }
      out[n++] = doc;
      if (n == capacity) {
        return n;
      }
      doc = _main.Advance();
    }

    _spent = true;
    return n;
  }

 private:
  Head _main;
  Tail _other;
  bool _spent = false;
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::docs
