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
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/posting_fill.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/count/root.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/states/term_state.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::count {

template<typename Lead, typename Others, typename Excludes, typename Table>
class Window : public Root {
 public:
  static constexpr bool kOthers = !std::is_same_v<Others, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;
  template<typename LeadArgs, typename OthersArgs, typename ExcludesArgs>
  Window(Table table, std::piecewise_construct_t, LeadArgs&& lead,
         OthersArgs&& others, ExcludesArgs&& excludes)
    : _lead{std::make_from_tuple<Lead>(std::forward<LeadArgs>(lead))},
      _others{std::make_from_tuple<Others>(std::forward<OthersArgs>(others))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))},
      _table{table} {}

  uint64_t Run() final {
    uint64_t total = 0;
    doc_id_t min = doc_limits::min();

    for (;;) {
      if (!_table.Skip(min)) {
        return total;
      }
      SDB_ASSERT(min <= doc_limits::eof() - search::kWindowDocs);
      const doc_id_t max = min + search::kWindowDocs;

      auto next = _lead.FillOr(min, max, _mask.data());
      if constexpr (kOthers) {
        next = std::max(next, _others.Restrict(min, max, _mask.data()));
      }
      if constexpr (kExcludes) {
        _excludes.Remove(min, max, _mask.data());
      }

      total += _table.CountAndClear(min, _mask.data(), search::kWindowWords);

      if (doc_limits::eof(next)) {
        return total;
      }
      min = next;
    }
  }

 private:
  search::Scratch _mask{};
  Lead _lead;
  [[no_unique_address]] Others _others;
  [[no_unique_address]] Excludes _excludes;
  [[no_unique_address]] search::Narrowing<Table> _table;
};

template<typename Excludes, typename Term, typename ExcludesArgs>
Root::ptr MakeWindowOfTerms(std::span<const Term> terms,
                            const TermReader* field, const IndexInput& doc,
                            ExcludesArgs&& excludes, const Context& ctx) {
  SDB_ASSERT(terms.size() >= 2);
  return search::ResolveInput(doc, [&]<typename Input> -> Root::ptr {
    using Leaf = search::PostingFill<Input>;
    using Others = fill::AndLeaves<Leaf>;
    const auto& own = search::FieldOf(terms.front(), field);
    const auto& front = search::CookieOf(terms.front());
    return MakeShape<Window, Leaf, Others, Excludes>(
      ctx, std::piecewise_construct,
      std::forward_as_tuple(front, doc,
                            front.docs_count != 1 && search::BoundsOf(own),
                            front.docs_count != 1 && search::FreqOf(own)),
      std::forward_as_tuple(
        terms.size() - 1,
        [&](Leaf& leaf, size_t i) {
          const auto& other = search::FieldOf(terms[i + 1], field);
          const auto& meta = search::CookieOf(terms[i + 1]);
          leaf.Prepare(meta, doc,
                       meta.docs_count != 1 && search::BoundsOf(other),
                       meta.docs_count != 1 && search::FreqOf(other));
        }),
      std::forward<ExcludesArgs>(excludes));
  });
}

}  // namespace irs::count
