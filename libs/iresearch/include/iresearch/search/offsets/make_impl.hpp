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

#include "basics/memory.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/ngram_all_slots.hpp"
#include "iresearch/search/common/ngram_slots.hpp"
#include "iresearch/search/common/phrase_fixed_slots.hpp"
#include "iresearch/search/common/phrase_of.hpp"
#include "iresearch/search/common/phrase_variadic_slots.hpp"
#include "iresearch/search/common/posting_pos.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/ngram_similarity_query.hpp"
#include "iresearch/search/offsets/impl.hpp"
#include "iresearch/search/offsets/ngram.hpp"
#include "iresearch/search/offsets/phrase.hpp"
#include "iresearch/search/offsets/posting.hpp"
#include "iresearch/search/offsets/root.hpp"
#include "iresearch/search/phrase_query.hpp"

namespace irs::search {

template<PhraseMatch M>
inline constexpr bool kOffsetsCount = M == PhraseMatch::Slop;

template<PhraseMatch M>
offsets::Root::ptr MakeFixedPhraseOffsets(const FixedPhraseQuery& query) {
  const auto& state = query.state;
  const auto& h = state.handles;
  if (!h.HasOffsets()) {
    return {};
  }
  const std::span metas{state.metas.data(), state.metas.size()};
  return ResolveBounds(h.bounds, [&]<bool Bounds> -> offsets::Root::ptr {
    return ResolveInput(*h.doc, [&]<typename Input> -> offsets::Root::ptr {
      using Leaf = search::PostingPos<Input, Bounds, true>;
      return ResolveMatcherOf<M, 0, kOffsetsCount<M>, true>(
        query, [&]<typename Matcher>(auto&&... args) -> offsets::Root::ptr {
          using Slots = search::PhraseFixedSlots<Matcher, Leaf>;
          using Impl = offsets::Impl<offsets::Phrase<Slots>>;
          return memory::make_managed<Impl>(
            metas, std::span<const TermInterval>{query.positions}, *h.doc,
            h.Layout(), *h.pos, h.pay, std::forward<decltype(args)>(args)...);
        });
    });
  });
}

template<PhraseMatch M>
offsets::Root::ptr MakeVariadicPhraseOffsets(const VariadicPhraseQuery& query) {
  const auto& state = query.state;
  const auto& h = state.handles;
  if (!h.HasOffsets()) {
    return {};
  }
  const std::span metas{state.metas.data(), state.metas.size()};
  const std::span widths{state.num_terms.data(), state.num_terms.size()};
  const std::span<const TermInterval> intervals{query.positions};
  SDB_ASSERT(metas.size() != widths.size());

  return ResolveBounds(h.bounds, [&]<bool Bounds> -> offsets::Root::ptr {
    return ResolveInput(*h.doc, [&]<typename Input> -> offsets::Root::ptr {
      return ResolveSlotMatcherOf<M, Bounds, Input, kOffsetsCount<M>, true>(
        query,
        [&]<typename Matcher, typename Leaf>(
          auto&&... args) -> offsets::Root::ptr {
          using Slots = search::PhraseVariadicSlots<Matcher, Leaf>;
          using Impl = offsets::Impl<offsets::Phrase<Slots>>;
          return memory::make_managed<Impl>(
            metas.size(),
            [&](Leaf& leaf, size_t i) {
              leaf.Prepare(*metas[i], *h.doc, h.Layout(), *h.pos, h.pay);
            },
            widths, std::span<const score_t>{}, intervals,
            std::forward<decltype(args)>(args)...);
        });
    });
  });
}

template<bool All>
offsets::Root::ptr MakeNGramOffsets(const NGramSimilarityQuery& query) {
  const auto& state = query.State();
  const auto& h = state.handles;
  if (!h.HasOffsets()) {
    return {};
  }
  const std::span metas{state.terms.data(), state.terms.size()};
  return ResolveBounds(h.bounds, [&]<bool Bounds> -> offsets::Root::ptr {
    return ResolveInput(*h.doc, [&]<typename Input> -> offsets::Root::ptr {
      using Leaf = search::PostingPos<Input, Bounds, true>;
      if constexpr (All) {
        using Slots = search::NGramAllSlots<Leaf, 0, false, true>;
        using Impl = offsets::Impl<offsets::NGram<Slots>>;
        return memory::make_managed<Impl>(metas, *h.doc, h.Layout(), *h.pos,
                                          h.pay, state.total_terms);
      } else {
        using Slots = search::NGramSlots<Leaf, false, true>;
        using Impl = offsets::Impl<offsets::NGram<Slots>>;
        return memory::make_managed<Impl>(
          metas.size(),
          [&](Leaf& leaf, size_t i) {
            leaf.Prepare(metas[i], *h.doc, h.Layout(), *h.pos, h.pay);
          },
          static_cast<uint32_t>(query.MinMatchCount()), state.total_terms);
      }
    });
  });
}

inline offsets::Root::ptr MakePostingOffsets(const PostingMeta& meta,
                                             const PhraseHandles& h) {
  if (!h.HasOffsets() || meta.docs_count == 0) {
    return {};
  }
  return ResolveBounds(h.bounds, [&]<bool Bounds> -> offsets::Root::ptr {
    return ResolveInput(*h.doc, [&]<typename Input> -> offsets::Root::ptr {
      using Leaf = search::PostingPos<Input, Bounds, true>;
      using Impl = offsets::Impl<offsets::Posting<Leaf>>;
      return memory::make_managed<Impl>(meta, *h.doc, h.Layout(), *h.pos,
                                        h.pay);
    });
  });
}

}  // namespace irs::search
