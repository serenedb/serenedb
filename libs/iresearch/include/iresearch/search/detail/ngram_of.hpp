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

#include <span>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/ngram_all_slots.hpp"
#include "iresearch/search/detail/ngram_slots.hpp"
#include "iresearch/search/detail/node_of.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/posting_pos.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/lead/two_phase_docs.hpp"
#include "iresearch/search/queries/ngram_similarity_query.hpp"
#include "iresearch/search/probe/two_phase_docs.hpp"

namespace irs::detail {

template<bool Scored = false, typename F>
auto Build(const NGramSimilarityQuery& query, F&& f) {
  const auto& state = query.State();
  const auto& h = state.handles;
  const std::span metas{state.terms.data(), state.terms.size()};
  const auto min_match = static_cast<uint32_t>(query.MinMatchCount());
  return ResolveBounds(h.bounds, [&]<bool Bounds> {
    return ResolveInput(*h.doc, [&]<typename Input> {
      using Leaf = detail::PostingPos<Input, Bounds>;
      return ResolveArity<kSlotArity, kSlotFloor>(metas.size(), [&]<size_t N> {
        return f
          .template operator()<detail::NGramSlots<Leaf, Scored, false, N>>(
            metas.size(),
            [&](Leaf& leaf, size_t i) {
              leaf.Prepare(metas[i], *h.doc, h.Layout(), *h.pos, h.pay);
            },
            min_match, state.total_terms);
      });
    });
  });
}

template<bool Scored = false, typename F>
auto BuildAll(const NGramSimilarityQuery& query, F&& f) {
  const auto& state = query.State();
  const auto& h = state.handles;
  const std::span metas{state.terms.data(), state.terms.size()};
  return ResolveBounds(h.bounds, [&]<bool Bounds> {
    return ResolveInput(*h.doc, [&]<typename Input> {
      using Leaf = detail::PostingPos<Input, Bounds>;
      return ResolveArity<kSlotArity, kSlotFloor>(metas.size(), [&]<size_t N> {
        return f.template operator()<detail::NGramAllSlots<Leaf, N, Scored>>(
          metas, *h.doc, h.Layout(), *h.pos, h.pay, state.total_terms);
      });
    });
  });
}

}  // namespace irs::detail
