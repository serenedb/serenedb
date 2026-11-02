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

#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/phrase_of.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/queries/phrase_query.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/pruned_phrase.hpp"

namespace irs::top {

template<irs::detail::PhraseMatch M, typename Query>
Root::ptr MakePhrasePruned(const Query& query, const Context& ctx) {
  static_assert(M != irs::detail::PhraseMatch::Slop);
  if (!query.state.boosts.empty() || !query.state.reader) {
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
  return irs::detail::BuildPhrase<M, true, false, false>(
    query, [&]<typename Slots>(auto&&... rest) -> Root::ptr {
      return MakeShape<PrunedPhrase, Slots>(
        ctx, query.Segment(), *query.state.reader, args,
        std::forward<decltype(rest)>(rest)...);
    });
}

}  // namespace irs::top

#define IRS_TOP_PHRASE_PRUNED(Query, Match)                             \
  template Root::ptr MakePhrasePruned<irs::detail::PhraseMatch::Match>( \
    const Query&, const Context&)
