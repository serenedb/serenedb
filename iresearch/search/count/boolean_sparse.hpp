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

#include "iresearch/search/count/root.hpp"
#include "iresearch/search/detail/exclude_block.hpp"
#include "iresearch/search/detail/table_filter.hpp"
#include "iresearch/search/lead/concept.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::count {

template<lead::Type Lead, typename Probes, typename Excludes, typename Table>
class BooleanSparse : public Root {
 public:
  static constexpr bool kProbes = !std::is_same_v<Probes, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  static constexpr bool kTable = !std::is_same_v<Table, utils::Empty>;
  static constexpr uint32_t kRun = 2048;
  static_assert(kProbes || kExcludes);

  template<typename LeadArgs, typename ProbesArgs, typename ExcludesArgs>
  BooleanSparse(Table table, std::piecewise_construct_t, LeadArgs&& lead,
                ProbesArgs&& probes, ExcludesArgs&& excludes)
    : _lead{std::make_from_tuple<Lead>(std::forward<LeadArgs>(lead))},
      _probes{std::make_from_tuple<Probes>(std::forward<ProbesArgs>(probes))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))},
      _table{table} {}

  BooleanSparse(BooleanSparse&&) = delete;
  BooleanSparse& operator=(BooleanSparse&&) = delete;

  uint64_t Run() final {
    uint64_t total = 0;
    uint32_t n = 0;
    auto doc = _lead.Next();

    while (!doc_limits::eof(doc)) {
      if constexpr (kTable) {
        const auto live = _table.Live(doc);
        if (live != doc) {
          doc = _lead.Seek(live);
          continue;
        }
      }
      auto probe = doc;
      if constexpr (kProbes) {
        probe = _probes.Probe(doc);
      }
      if (probe == doc) {
        bool kept = true;
        if constexpr (kExcludes) {
          kept = !detail::IsExcluded(_excludes, doc);
        }
        if constexpr (kTable) {
          _docs[n] = doc;
          n += static_cast<uint32_t>(kept);
          if (n == kRun) {
            total += _table.Run(_docs.data(), nullptr, n);
            n = 0;
          }
        } else {
          total += static_cast<uint64_t>(kept);
        }
        doc = _lead.Next();
      } else {
        doc = _lead.Seek(probe);
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
  Lead _lead;
  [[no_unique_address]] Probes _probes;
  [[no_unique_address]] Excludes _excludes;
  [[no_unique_address]] utils::Need<kTable, std::array<doc_id_t, kRun>> _docs;
  [[no_unique_address]] detail::Narrowing<Table> _table;
};

}  // namespace irs::count
