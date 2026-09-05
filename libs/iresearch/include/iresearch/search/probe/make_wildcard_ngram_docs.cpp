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

#include <utility>

#include "iresearch/search/probe/all_docs.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/two_phase_docs.hpp"
#include "iresearch/search/probe/wildcard_ngram_slots_docs.hpp"
#include "iresearch/search/wildcard_ngram_filter.hpp"

namespace irs::probe {

Node::ptr MakeWildcardNgramDocs(const WildcardNgramQuery& query,
                                uint64_t interrogations) {
  SDB_ASSERT(query.Kind() != QueryKind::Empty);
  const auto& ngrams = query.NGrams();
  SDB_ASSERT(ngrams.Kind() != QueryKind::Empty);
  if (!query.HasMatcher()) {
    return ngrams.PlanProbe({}, interrogations);
  }
  const auto recipe = query.MakeRecipe();

  if (ngrams.Kind() == QueryKind::All) {
    using Slots = WildcardNgramSlotsDocs<AllDocs>;
    return memory::make_managed<Impl<TwoPhaseDocs<Slots>>>(
      std::piecewise_construct, std::forward_as_tuple(query.Segment()), recipe);
  }

  auto node = ngrams.PlanProbe({}, interrogations);
  if (!node) {
    return {};
  }
  using Slots = WildcardNgramSlotsDocs<Erased>;
  return memory::make_managed<Impl<TwoPhaseDocs<Slots>>>(
    std::piecewise_construct, std::forward_as_tuple(std::move(node)), recipe);
}

}  // namespace irs::probe
