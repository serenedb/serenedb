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
#include <utility>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/detail/ngram_all_slots.hpp"
#include "iresearch/search/detail/ngram_slots.hpp"
#include "iresearch/search/detail/phrase_of.hpp"
#include "iresearch/search/detail/phrase_slots.hpp"
#include "iresearch/search/detail/posting_pos.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/offsets/impl.hpp"
#include "iresearch/search/offsets/ngram.hpp"
#include "iresearch/search/offsets/phrase.hpp"
#include "iresearch/search/offsets/posting.hpp"
#include "iresearch/search/offsets/root.hpp"
#include "iresearch/search/queries/ngram_similarity_query.hpp"
#include "iresearch/search/queries/phrase_query.hpp"
#include "iresearch/utils/memory.hpp"

namespace irs::offsets {

template<detail::PhraseMatch M, typename Query>
Root::ptr MakePhrase(const Query& query) {
  if (!query.state.handles.HasOffsets()) {
    return {};
  }
  static constexpr bool kCount = M == detail::PhraseMatch::Slop;
  return detail::BuildPhrase<M, kCount, true, false>(
    query, [&]<typename Slots>(auto&&... args) -> Root::ptr {
      return memory::make_managed<Impl<Phrase<Slots>>>(
        std::forward<decltype(args)>(args)...);
    });
}

template<detail::NGramMatch M>
Root::ptr MakeNGram(const NGramSimilarityQuery& query) {
  const auto& state = query.State();
  const auto& h = state.handles;
  if (!h.HasOffsets()) {
    return {};
  }
  const std::span metas{state.metas.data(), state.metas.size()};
  return detail::ResolveBounds(h.bounds, [&]<bool Bounds> -> Root::ptr {
    return detail::ResolveInput(*h.doc, [&]<typename Input> -> Root::ptr {
      using Leaf = detail::PostingPos<Input, Bounds, true>;
      if constexpr (M == detail::NGramMatch::All) {
        using Slots = detail::NGramAllSlots<Leaf, 0, false, true>;
        using Node = Impl<NGram<Slots>>;
        return memory::make_managed<Node>(metas, *h.doc, h.Layout(), *h.pos,
                                          h.pay, query.TotalTerms());
      } else {
        using Slots = detail::NGramSlots<Leaf, false, true>;
        using Node = Impl<NGram<Slots>>;
        return memory::make_managed<Node>(
          metas.size(),
          [&](Leaf& leaf, size_t i) {
            leaf.Prepare(metas[i], *h.doc, h.Layout(), *h.pos, h.pay);
          },
          static_cast<uint32_t>(query.MinMatchCount()), query.TotalTerms());
      }
    });
  });
}

}  // namespace irs::offsets

#define IRS_OFFSETS_PHRASE(Query, Match) \
  template Root::ptr MakePhrase<detail::PhraseMatch::Match>(const Query&)

#define IRS_OFFSETS_NGRAM(Match)                           \
  template Root::ptr MakeNGram<detail::NGramMatch::Match>( \
    const NGramSimilarityQuery&)

namespace irs::detail {

inline offsets::Root::ptr MakePostingOffsets(const PostingMeta& meta,
                                             const PhraseHandles& h) {
  if (!h.HasOffsets() || meta.docs_count == 0) {
    return {};
  }
  return ResolveBounds(h.bounds, [&]<bool Bounds> -> offsets::Root::ptr {
    return ResolveInput(*h.doc, [&]<typename Input> -> offsets::Root::ptr {
      using Leaf = detail::PostingPos<Input, Bounds, true>;
      using Impl = offsets::Impl<offsets::Posting<Leaf>>;
      return memory::make_managed<Impl>(meta, *h.doc, h.Layout(), *h.pos,
                                        h.pay);
    });
  });
}

}  // namespace irs::detail
