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

#include <span>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/phrase_of.hpp"
#include "iresearch/search/detail/phrase_slots.hpp"
#include "iresearch/search/detail/phrase_variadic_pos.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/queries/phrase_query.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/pruned_phrase.hpp"

namespace irs::top {
namespace {

template<irs::detail::PhraseMatch M>
Root::ptr MakeOf(const VariadicPhraseQuery& query, const Context& ctx,
                 const irs::detail::ScoreArgs& args) {
  const auto& state = query.state;
  const auto& h = state.handles;
  const std::span<const PostingMeta> metas{state.metas.data(),
                                           state.metas.size()};
  const std::span offsets{state.offsets.data(), state.offsets.size()};
  const std::span intervals{query.positions};
  return irs::detail::ResolveBounds(h.bounds, [&]<bool Bounds> -> Root::ptr {
    return irs::detail::ResolveInput(*h.doc, [&]<typename Input> -> Root::ptr {
      return irs::detail::ResolveSlotMatcherOf<M, Bounds, Input, true>(
        query, [&]<typename Matcher, typename Leaf>() -> Root::ptr {
          using Slots =
            irs::detail::PhraseSlots<Matcher, Leaf, 0,
                                     irs::detail::PhraseVariadicPos<Leaf>>;
          return MakeShape<PrunedPhrase, Slots>(
            ctx, query.Segment(), *state.reader, args, metas, offsets,
            std::span<const score_t>{}, intervals, *h.doc, h.Layout(), *h.pos,
            h.pay);
        });
    });
  });
}

}  // namespace

Root::ptr MakeVariadicPhrasePruned(const VariadicPhraseQuery& query,
                                   const Context& ctx) {
  SDB_ASSERT(query.slop == 0);
  const auto& state = query.state;
  if (!state.boosts.empty() || !state.reader) {
    return {};
  }
  const auto record = query.Stats(ScoredOf(ctx));
  if (!record.stats) {
    return {};
  }
  const irs::detail::ScoreArgs args{.scorer = record.scorer,
                                    .stats = record.stats,
                                    .fetcher = &ctx.fetcher,
                                    .boost = query.Boost()};
  if (query.has_intervals) {
    return MakeOf<irs::detail::PhraseMatch::Intervals>(query, ctx, args);
  }
  return MakeOf<irs::detail::PhraseMatch::Plain>(query, ctx, args);
}

}  // namespace irs::top
