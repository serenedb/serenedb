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
#include <type_traits>
#include <utility>

#include "basics/empty.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/count/root.hpp"
#include "iresearch/search/lead/concept.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::count {

template<lead::Type Node, typename Table>
class Walk : public Root {
 public:
  static constexpr bool kTable = !std::is_same_v<Table, utils::Empty>;
  static constexpr uint32_t kRun = 2048;

  template<typename... Args>
  Walk(Table table, Args&&... args)
    : _node{std::forward<Args>(args)...}, _table{table} {}

  uint64_t Run() final {
    uint64_t total = 0;
    if constexpr (kTable) {
      auto doc = _node.Advance();
      while (!doc_limits::eof(doc)) {
        const auto live = _table.Live(doc);
        if (live != doc) {
          doc = _node.Seek(live);
          continue;
        }
        uint32_t n = 0;
        do {
          _docs[n++] = doc;
          doc = _node.Advance();
        } while (n != kRun && !doc_limits::eof(doc));
        total += _table.Run(_docs.data(), nullptr, n);
      }
    } else {
      while (!doc_limits::eof(_node.Advance())) {
        ++total;
      }
    }
    return total;
  }

 private:
  Node _node;
  [[no_unique_address]] utils::Need<kTable, std::array<doc_id_t, kRun>> _docs;
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::count
