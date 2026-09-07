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

template<lead::Type Include, probe::Type Exclude, typename Table>
class SparseExclusion : public Root {
 public:
  static constexpr bool kTable = !std::is_same_v<Table, utils::Empty>;
  static constexpr uint32_t kRun = 2048;

  template<typename IncludeArgs, typename ExcludeArgs>
  SparseExclusion(Table table, std::piecewise_construct_t,
                  IncludeArgs&& include, ExcludeArgs&& exclude)
    : _include{std::make_from_tuple<Include>(
        std::forward<IncludeArgs>(include))},
      _exclude{
        std::make_from_tuple<Exclude>(std::forward<ExcludeArgs>(exclude))},
      _table{table} {}

  SparseExclusion(SparseExclusion&&) = delete;
  SparseExclusion& operator=(SparseExclusion&&) = delete;

  uint64_t Run() final {
    uint64_t total = 0;
    uint32_t n = 0;

    for (auto doc = _include.Advance(); !doc_limits::eof(doc);) {
      if constexpr (kTable) {
        const auto live = _table.Live(doc);
        if (live != doc) {
          doc = _include.Seek(live);
          continue;
        }
      }
      if (_exclude.Probe(doc) != doc) {
        if constexpr (kTable) {
          _docs[n++] = doc;
          if (n == kRun) {
            total += _table.Run(_docs.data(), nullptr, n);
            n = 0;
          }
        } else {
          ++total;
        }
      }
      doc = _include.Advance();
    }

    if constexpr (kTable) {
      if (n != 0) {
        total += _table.Run(_docs.data(), nullptr, n);
      }
    }
    return total;
  }

 private:
  Include _include;
  Exclude _exclude;
  [[no_unique_address]] utils::Need<kTable, std::array<doc_id_t, kRun>> _docs;
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::count
