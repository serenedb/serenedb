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

#include <cstdint>
#include <tuple>
#include <type_traits>
#include <utility>

#include "basics/empty.hpp"
#include "iresearch/search/common/exclude_block.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/docs/root.hpp"
#include "iresearch/search/lead/concept.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

template<lead::Type Lead, typename Probes, typename Excludes, typename Table>
class BooleanSparse : public Root {
 public:
  static constexpr bool kProbes = !std::is_same_v<Probes, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  static constexpr bool kTable = !std::is_same_v<Table, utils::Empty>;
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

  uint32_t Run(doc_id_t* IRS_RESTRICT out, uint32_t capacity) final {
    SDB_ASSERT(capacity >= doc_limits::kMinCapacity);
    if (_spent) {
      return 0;
    }
    uint32_t n = 0;
    auto doc = _lead.Advance();

    while (!doc_limits::eof(doc)) {
      if constexpr (kTable) {
        const auto live = _table.Live(doc);
        if (live != doc) {
          doc = _lead.Seek(live);
          continue;
        }
      }
      if constexpr (kProbes) {
        const auto probe = _probes.Probe(doc);
        if (probe != doc) {
          doc = _lead.Seek(probe);
          continue;
        }
      }
      out[n] = doc;
      if constexpr (kExcludes) {
        n += static_cast<uint32_t>(!search::IsExcluded(_excludes, doc));
      } else {
        ++n;
      }
      if (n == capacity) {
        return n;
      }
      doc = _lead.Advance();
    }

    _spent = true;
    return n;
  }

 private:
  Lead _lead;
  [[no_unique_address]] Probes _probes;
  [[no_unique_address]] Excludes _excludes;
  bool _spent = false;
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::docs
