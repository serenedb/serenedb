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

#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/docs/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

template<typename Table>
class All : public Root {
 public:
  static constexpr bool kTable = !std::is_same_v<Table, utils::Empty>;

  All(Table table, doc_id_t count) noexcept
    : _end{doc_limits::min() + count}, _table{table} {}

  uint32_t Run(doc_id_t* IRS_RESTRICT out, uint32_t capacity) final {
    SDB_ASSERT(capacity >= doc_limits::kMinCapacity);
    if constexpr (kTable) {
      _doc = std::min(_table.Live(_doc), _end);
    }
    const auto n = std::min<uint32_t>(capacity, _end - _doc);
    for (uint32_t i = 0; i != n; ++i) {
      out[i] = _doc + i;
    }
    _doc += n;
    return n;
  }

 private:
  doc_id_t _doc = doc_limits::min();
  doc_id_t _end;
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::docs
