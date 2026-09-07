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

template<lead::Type Include, probe::Type Exclude, typename Table>
class SparseExclusion : public Root {
 public:
  static constexpr bool kTable = !std::is_same_v<Table, utils::Empty>;

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

  uint32_t Run(doc_id_t* IRS_RESTRICT out, uint32_t capacity) final {
    SDB_ASSERT(capacity >= doc_limits::kMinCapacity);
    uint32_t n = 0;
    if (_spent) {
      return 0;
    }

    for (auto doc = _include.Advance(); !doc_limits::eof(doc);
         doc = _include.Advance()) {
      if constexpr (kTable) {
        if (const auto live = _table.Live(doc); live != doc) {
          doc = _include.Seek(live);
          if (doc_limits::eof(doc)) {
            break;
          }
        }
      }
      if (_exclude.Probe(doc) == doc) {
        continue;
      }
      out[n++] = doc;
      if (n == capacity) {
        return n;
      }
    }

    _spent = true;
    return n;
  }

 private:
  Include _include;
  Exclude _exclude;
  bool _spent = false;
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::docs
