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
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/phrase_fixed_slots.hpp"
#include "iresearch/search/detail/phrase_of.hpp"
#include "iresearch/search/detail/posting_pos.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/detail/phrase_iterator.hpp"
#include "iresearch/search/phrase_query.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/pruned_phrase.hpp"

namespace irs::top {
namespace {

template<typename Leaf, size_t Slots, typename... Prefix>
Root::ptr MakeSlots(const FixedPhraseQuery& query,
                    std::span<const PostingMeta* const> metas,
                    const irs::detail::PhraseHandles& h, const Context& ctx,
                    Prefix&&... prefix) {
  using Matcher = FixedPhraseFrequency<false, true, true, Slots>;
  using SlotsType = irs::detail::PhraseFixedSlots<Matcher, Leaf, Slots>;
  return MakeShape<PrunedPhrase, SlotsType>(
    ctx, std::forward<Prefix>(prefix)..., metas, query.positions, *h.doc,
    h.Layout(), *h.pos, h.pay);
}

}  // namespace

Root::ptr MakeFixedPhraseIntervalsPruned(const FixedPhraseQuery& query,
                                         const Context& ctx) {
  SDB_ASSERT(query.slop == 0);
  SDB_ASSERT(query.has_intervals);
  const auto record = query.Stats(ScoredOf(ctx));
  const auto* const stats = record.stats;
  if (stats == nullptr) {
    return {};
  }
  const auto& state = query.state;
  const auto& h = state.handles;
  const std::span metas{state.metas.data(), state.metas.size()};
  const irs::detail::ScoreArgs args{.scorer = record.scorer,
                               .stats = stats,
                               .fetcher = &ctx.fetcher,
                               .boost = query.Boost()};

  return irs::detail::ResolveBounds(h.bounds, [&]<bool Bounds> -> Root::ptr {
    return irs::detail::ResolveInput(*h.doc, [&]<typename Input> -> Root::ptr {
      using Leaf = irs::detail::PostingPos<Input, Bounds>;
      return irs::detail::ResolveArity<irs::detail::kSlotArity, irs::detail::kSlotFloor>(
        metas.size(), [&]<size_t N> -> Root::ptr {
          static constexpr size_t kSlots = N == 1 ? 0 : N;
          return MakeSlots<Leaf, kSlots>(query, metas, h, ctx, query.Segment(),
                                         *state.reader, args);
        });
    });
  });
}

}  // namespace irs::top
