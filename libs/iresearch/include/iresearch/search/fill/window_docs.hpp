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
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/posting_fill.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/fill/concept.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/states/term_state.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

template<Producer Lead, typename Others, typename Excludes>
class WindowDocs {
 public:
  static constexpr bool kOthers = !std::is_same_v<Others, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;

  template<typename LeadArgs, typename OthersArgs, typename ExcludesArgs>
  WindowDocs(std::piecewise_construct_t, LeadArgs&& lead, OthersArgs&& others,
             ExcludesArgs&& excludes)
    : _lead{std::make_from_tuple<Lead>(std::forward<LeadArgs>(lead))},
      _others{std::make_from_tuple<Others>(std::forward<OthersArgs>(others))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))} {}

  doc_id_t FillOr(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    const auto words = search::WindowWords(min, max);
    const auto next = Compute(min, max, words);
    for (size_t w = 0; w != words; ++w) {
      mask[w] |= _own[w];
    }
    return next;
  }

  doc_id_t FillAnd(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    const auto words = search::WindowWords(min, max);
    const auto next = Compute(min, max, words);
    search::FoldAnd(mask, _own.data(), words);
    return next;
  }

  doc_id_t FillAndNot(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    const auto words = search::WindowWords(min, max);
    const auto next = Compute(min, max, words);
    search::FoldAndNot(mask, _own.data(), words);
    return next;
  }

 private:
  doc_id_t Compute(doc_id_t min, doc_id_t max, size_t words) {
    search::Clear(_own.data(), words);
    auto next = _lead.FillOr(min, max, _own.data());
    if constexpr (kOthers) {
      next = std::max(next, _others.Restrict(min, max, _own.data()));
    }
    if constexpr (kExcludes) {
      _excludes.Remove(min, max, _own.data());
    }
    return next;
  }

  search::Scratch _own{};
  Lead _lead;
  [[no_unique_address]] Others _others;
  [[no_unique_address]] Excludes _excludes;
};

template<typename Result, typename Excludes, typename Term,
         typename ExcludesArgs>
Result MakeWindowOfTerms(std::span<const Term> terms, const TermReader* field,
                         const IndexInput& doc, ExcludesArgs&& excludes) {
  SDB_ASSERT(terms.size() >= 2);
  return search::ResolveInput(doc, [&]<typename Input> -> Result {
    using Leaf = search::PostingFill<Input>;
    using Others = AndLeaves<Leaf>;
    using Node = WindowDocs<Leaf, Others, Excludes>;
    const auto& own = search::FieldOf(terms.front(), field);
    const auto& meta = search::CookieOf(terms.front());
    return memory::make_managed<Impl<Node>>(
      std::piecewise_construct,
      std::forward_as_tuple(meta, doc,
                            meta.docs_count != 1 && search::BoundsOf(own),
                            meta.docs_count != 1 && search::FreqOf(own)),
      std::forward_as_tuple(
        terms.size() - 1,
        [&](Leaf& leaf, size_t i) {
          const auto& other = search::FieldOf(terms[i + 1], field);
          const auto& cookie = search::CookieOf(terms[i + 1]);
          leaf.Prepare(cookie, doc,
                       cookie.docs_count != 1 && search::BoundsOf(other),
                       cookie.docs_count != 1 && search::FreqOf(other));
        }),
      std::forward<ExcludesArgs>(excludes));
  });
}

}  // namespace irs::fill
