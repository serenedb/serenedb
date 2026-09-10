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
#include <tuple>
#include <type_traits>
#include <utility>

#include "basics/empty.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/count/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::count {

template<typename Lead, typename Others, typename Optional, typename Excludes,
         typename Table>
class BooleanWindow : public Root {
 public:
  static constexpr bool kLead = !std::is_same_v<Lead, utils::Empty>;
  static constexpr bool kOthers = !std::is_same_v<Others, utils::Empty>;
  static constexpr bool kOptional = !std::is_same_v<Optional, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  static_assert(kLead != kOptional);
  static_assert(kLead || !kOthers);

  template<typename LeadArgs, typename OthersArgs, typename OptionalArgs,
           typename ExcludesArgs>
  BooleanWindow(Table table, std::piecewise_construct_t, LeadArgs&& lead,
                OthersArgs&& others, OptionalArgs&& optional,
                ExcludesArgs&& excludes)
    : _lead{std::make_from_tuple<Lead>(std::forward<LeadArgs>(lead))},
      _others{std::make_from_tuple<Others>(std::forward<OthersArgs>(others))},
      _optional{
        std::make_from_tuple<Optional>(std::forward<OptionalArgs>(optional))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))},
      _table{table} {}

  BooleanWindow(BooleanWindow&&) = delete;
  BooleanWindow& operator=(BooleanWindow&&) = delete;

  uint64_t Run() final {
    uint64_t total = 0;
    doc_id_t min = doc_limits::min();

    for (;;) {
      if constexpr (kOptional) {
        if (_optional.Exhausted()) {
          return total;
        }
      }
      if (!_table.Skip(min)) {
        return total;
      }
      SDB_ASSERT(min <= doc_limits::eof() - search::kWindowDocs);
      const doc_id_t max = min + search::kWindowDocs;

      auto* const words = _mask.data();
      doc_id_t next;
      if constexpr (kLead) {
        next = _lead.FillOr(min, max, words);
        if constexpr (kOthers) {
          next = std::max(next, _others.Restrict(min, max, words));
        }
      } else {
        next = _optional.Fill(min, max, words);
      }
      if constexpr (kExcludes) {
        _excludes.Remove(min, max, words);
      }

      total += _table.CountAndClear(min, words, search::kWindowWords);

      if (doc_limits::eof(next)) {
        return total;
      }
      min = next;
    }
  }

 private:
  search::Scratch _mask{};
  [[no_unique_address]] Lead _lead;
  [[no_unique_address]] Others _others;
  [[no_unique_address]] Optional _optional;
  [[no_unique_address]] Excludes _excludes;
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::count
