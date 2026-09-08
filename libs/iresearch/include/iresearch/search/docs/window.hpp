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
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "iresearch/search/common/posting_fill.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/docs/emit.hpp"
#include "iresearch/search/docs/plan.hpp"
#include "iresearch/search/docs/root.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/fill/window_disjunction.hpp"
#include "iresearch/search/states/term_state.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

template<typename Lead, typename Others, typename Excludes, typename Table>
class Window : public Root {
 public:
  static constexpr bool kOthers = !std::is_same_v<Others, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;

  template<typename LeadArgs, typename OthersArgs, typename ExcludesArgs>
  Window(Table table, std::piecewise_construct_t, LeadArgs&& lead,
         OthersArgs&& others, ExcludesArgs&& excludes)
    : _emit{table},
      _lead{std::make_from_tuple<Lead>(std::forward<LeadArgs>(lead))},
      _others{std::make_from_tuple<Others>(std::forward<OthersArgs>(others))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))} {}

  uint32_t Run(doc_id_t* IRS_RESTRICT out, uint32_t capacity) final {
    SDB_ASSERT(capacity >= doc_limits::kMinCapacity);
    uint32_t n = 0;

    for (;;) {
      if (!_emit.Drain(out, capacity, n)) {
        return n;
      }
      if (!_emit.Skip(_min)) {
        _spent = true;
      }
      if (_spent || n == capacity) {
        return n;
      }
      SDB_ASSERT(_min <= doc_limits::eof() - search::kWindowDocs);
      const doc_id_t max = _min + search::kWindowDocs;

      auto next = _lead.FillOr(_min, max, _emit.Mask());
      if constexpr (kOthers) {
        next = std::max(next, _others.Restrict(_min, max, _emit.Mask()));
      }
      if constexpr (kExcludes) {
        _excludes.Remove(_min, max, _emit.Mask());
      }
      _emit.Opened(_min);

      if (doc_limits::eof(next)) {
        _spent = true;
      } else {
        _min = next;
      }
    }
  }

 private:
  Emit<Table> _emit;
  Lead _lead;
  [[no_unique_address]] Others _others;
  [[no_unique_address]] Excludes _excludes;
  doc_id_t _min = doc_limits::min();
  bool _spent = false;
};

template<typename Set, typename... Args>
Root::ptr MakeWindowOfSet(const Context& ctx, Args&&... args) {
  using Lead = fill::WindowDisjunctionDocs<Set>;
  return MakeShape<Window, Lead, utils::Empty, utils::Empty>(
    ctx, std::piecewise_construct,
    std::forward_as_tuple(std::piecewise_construct,
                          std::forward_as_tuple(std::forward<Args>(args)...)),
    std::forward_as_tuple(), std::forward_as_tuple());
}

template<typename Excludes, typename Term, typename ExcludesArgs>
Root::ptr MakeWindowOfTerms(std::span<const Term> terms,
                            const TermReader* field, const IndexInput& doc,
                            ExcludesArgs&& excludes, const Context& ctx) {
  SDB_ASSERT(terms.size() >= 2);
  return search::ResolveInput(doc, [&]<typename Input> -> Root::ptr {
    using Leaf = search::PostingFill<Input>;
    using Others = fill::AndLeaves<Leaf>;
    const auto& lead = search::FieldOf(terms.front(), field);
    const auto& front = search::CookieOf(terms.front());
    return MakeShape<Window, Leaf, Others, Excludes>(
      ctx, std::piecewise_construct,
      std::forward_as_tuple(front, doc,
                            front.docs_count != 1 && search::BoundsOf(lead),
                            front.docs_count != 1 && search::FreqOf(lead)),
      std::forward_as_tuple(
        terms.size() - 1,
        [&](Leaf& leaf, size_t i) {
          const auto& own = search::FieldOf(terms[i + 1], field);
          const auto& meta = search::CookieOf(terms[i + 1]);
          leaf.Prepare(meta, doc, meta.docs_count != 1 && search::BoundsOf(own),
                       meta.docs_count != 1 && search::FreqOf(own));
        }),
      std::forward<ExcludesArgs>(excludes));
  });
}

}  // namespace irs::docs
