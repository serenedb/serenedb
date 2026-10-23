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

#include <optional>
#include <span>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/ngram_all_slots.hpp"
#include "iresearch/search/detail/ngram_slots.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/posting_pos.hpp"
#include "iresearch/search/queries/ngram_similarity_query.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"

namespace irs::detail {

template<typename Result, bool Scored, NGramMatch M, typename F>
Result Build(NGramKind<M>, const NGramSimilarityQuery& query, uint64_t, F&& f) {
  const auto& state = query.State();
  const auto& h = state.handles;
  const std::span metas{state.metas.data(), state.metas.size()};
  return ResolveBounds(h.bounds, [&]<bool Bounds> {
    return ResolveInput(*h.doc, [&]<typename Input> {
      using Leaf = PostingPos<Input, Bounds>;
      return ResolveArity<kSlotArity, kSlotFloor>(metas.size(), [&]<size_t N> {
        if constexpr (M == NGramMatch::All) {
          return f.template operator()<NGramAllSlots<Leaf, N, Scored>>(
            metas, *h.doc, h.Layout(), *h.pos, h.pay, query.TotalTerms());
        } else {
          return f.template operator()<NGramSlots<Leaf, Scored, false, N>>(
            metas.size(),
            [&](Leaf& leaf, size_t i) {
              leaf.Prepare(metas[i], *h.doc, h.Layout(), *h.pos, h.pay);
            },
            static_cast<uint32_t>(query.MinMatchCount()), query.TotalTerms());
        }
      });
    });
  });
}

template<NGramMatch M>
std::optional<score_t> ConstantOf(NGramKind<M>,
                                  const NGramSimilarityQuery& query,
                                  const ScoreArgs& args) {
  const auto& field = *query.State().reader;
  if constexpr (M == NGramMatch::All) {
    const auto value = ConstantOf(query.Segment(), field, args);
    if (!value) {
      return std::nullopt;
    }
    const auto scale = static_cast<score_t>(query.Present()) /
                       static_cast<score_t>(query.TotalTerms());
    return scale * *value;
  } else {
    return ConstantOf(query.Segment(), field, args, true);
  }
}

template<NGramMatch M>
const TermReader& FieldOf(NGramKind<M>, const NGramSimilarityQuery& query) {
  return *query.State().reader;
}

}  // namespace irs::detail
