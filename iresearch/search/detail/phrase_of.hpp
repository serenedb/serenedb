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

#include <cstdint>
#include <optional>
#include <span>
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/node_of.hpp"
#include "iresearch/search/detail/phrase_match.hpp"
#include "iresearch/search/detail/phrase_slop_matcher.hpp"
#include "iresearch/search/detail/phrase_slots.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/posting_pos.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/lead/two_phase_docs.hpp"
#include "iresearch/search/probe/two_phase_docs.hpp"
#include "iresearch/search/queries/phrase_query.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"

namespace irs::detail {

template<PhraseMatch M, bool Bounds, typename Input, bool HasFreq = false,
         bool Offs = false, bool HasBoost = false, typename Query, typename F>
auto ResolveSlotMatcherOf(const Query& query, F&& f) {
  using Leaf = detail::PostingPos<Input, Bounds, Offs>;
  using Slot =
    std::pair<detail::PhraseVariadicPositions<Leaf, HasBoost>*, TermInterval>;
  if constexpr (M == PhraseMatch::Slop) {
    return f.template
    operator()<PhraseSlopMatcher<Slot, Offs, HasFreq, 0, HasBoost>, Leaf>(
      query.slop);
  } else {
    static constexpr bool kIntervals = M == PhraseMatch::Intervals;
    return f.template operator()<
      PhraseMatcher<Slot, Offs, HasFreq, kIntervals, HasBoost>, Leaf>();
  }
}

template<PhraseMatch M, size_t N, bool HasFreq = false, bool Offs = false,
         typename Query, typename F>
auto ResolveMatcherOf(const Query& query, F&& f) {
  if constexpr (M == PhraseMatch::Slop) {
    return f.template operator()<FixedPhraseSlopMatcher<Offs, HasFreq, N>>(
      query.slop);
  } else {
    static constexpr bool kIntervals = M == PhraseMatch::Intervals;
    return f
      .template operator()<FixedPhraseMatcher<Offs, HasFreq, kIntervals, N>>();
  }
}

template<PhraseMatch M, bool HasFreq = false, bool Offs = false,
         bool Boosts = HasFreq, typename F>
auto BuildPhrase(const FixedPhraseQuery& query, F&& f) {
  const auto& state = query.state;
  const auto& h = state.handles;
  const std::span<const PostingMeta> metas{state.metas.data(),
                                           state.metas.size()};
  const std::span<const TermInterval> intervals{query.positions};
  return ResolveBounds(h.bounds, [&]<bool Bounds> {
    return ResolveInput(*h.doc, [&]<typename Input> {
      using Leaf = PostingPos<Input, Bounds, Offs>;
      const auto build = [&]<size_t N> {
        return ResolveMatcherOf<M, N, HasFreq, Offs>(
          query, [&]<typename Matcher>(auto&&... args) {
            return f.template operator()<PhraseSlots<Matcher, Leaf, N>>(
              metas, intervals, *h.doc, h.Layout(), *h.pos, h.pay,
              std::forward<decltype(args)>(args)...);
          });
      };
      if constexpr (Offs) {
        return build.template operator()<0>();
      } else {
        return ResolveArity<kSlotArity, kSlotFloor>(metas.size(), build);
      }
    });
  });
}

template<PhraseMatch M, bool HasFreq = false, bool Offs = false,
         bool Boosts = HasFreq, typename F>
auto BuildPhrase(const VariadicPhraseQuery& query, F&& f) {
  const auto& state = query.state;
  const auto& h = state.handles;
  const std::span<const PostingMeta> metas{state.metas.data(),
                                           state.metas.size()};
  const std::span offsets{state.offsets.data(), state.offsets.size()};
  const std::span<const TermInterval> intervals{query.positions};
  SDB_ASSERT(!state.Fixed());
  return ResolveBounds(h.bounds, [&]<bool Bounds> {
    return ResolveInput(*h.doc, [&]<typename Input> {
      const auto build = [&]<bool HasBoost> {
        return ResolveSlotMatcherOf<M, Bounds, Input, HasFreq, Offs, HasBoost>(
          query, [&]<typename Matcher, typename Leaf>(auto&&... args) {
            const std::span boosts =
              HasBoost ? std::span{state.boosts.data(), state.boosts.size()}
                       : std::span<const score_t>{};
            return f.template operator()<
              PhraseSlots<Matcher, Leaf, 0, PhraseVariadicPos<Leaf, HasBoost>>>(
              metas, offsets, boosts, intervals, *h.doc, h.Layout(), *h.pos,
              h.pay, std::forward<decltype(args)>(args)...);
          });
      };
      if constexpr (Boosts) {
        return ResolveBool(!state.boosts.empty(), build);
      } else {
        return build.template operator()<false>();
      }
    });
  });
}

template<PhraseMatch M, template<typename> typename Impl, typename Result,
         bool Scored = false, template<typename> typename Wrap = DeducedNode,
         typename Query, typename... Prefix>
Result MakePhraseOf(const Query& query, Prefix&&... prefix) {
  return BuildPhrase<M, Scored>(
    query, [&]<typename Slots>(auto&&... args) -> Result {
      return memory::make_managed<Impl<NodeOf<Wrap, Result, Slots>>>(
        std::forward<Prefix>(prefix)..., std::forward<decltype(args)>(args)...);
    });
}

template<PhraseMatch M>
std::optional<score_t> PhraseConstantOf(const FixedPhraseQuery& query,
                                        const ScoreArgs& args) {
  if (M == PhraseMatch::Slop) {
    return std::nullopt;
  }
  return ConstantOf(query.Segment(), *query.state.reader, args);
}

template<PhraseMatch M>
std::optional<score_t> PhraseConstantOf(const VariadicPhraseQuery& query,
                                        const ScoreArgs& args) {
  if (M == PhraseMatch::Slop || !query.state.boosts.empty()) {
    return std::nullopt;
  }
  return ConstantOf(query.Segment(), *query.state.reader, args);
}

}  // namespace irs::detail
