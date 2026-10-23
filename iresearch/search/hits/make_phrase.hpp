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
#include "iresearch/search/hits/make.hpp"
#include "iresearch/search/hits/walk.hpp"
#include "iresearch/search/lead/two_phase_docs.hpp"
#include "iresearch/search/lead/two_phase_scored.hpp"
#include "iresearch/search/queries/phrase_query.hpp"

namespace irs::hits {

template<irs::detail::PhraseMatch M, typename Query>
Root::ptr MakePhrase(const Query& query, const Context& ctx) {
  const auto record = query.Stats(ScoredOf(ctx));
  if (!record.stats || !query.state.reader) {
    return {};
  }
  const irs::detail::ScoreArgs args{.scorer = record.scorer,
                                    .stats = record.stats,
                                    .fetcher = &ctx.fetcher,
                                    .boost = query.Boost()};
  if (const auto value = irs::detail::PhraseConstantOf<M>(query, args)) {
    return irs::detail::BuildPhrase<M>(
      query, [&]<typename Slots>(auto&&... rest) -> Root::ptr {
        return MakeShape<ConstantWalk, lead::TwoPhaseDocs<Slots>>(
          ctx, *value, std::forward<decltype(rest)>(rest)...);
      });
  }
  return irs::detail::BuildPhrase<M, true>(
    query, [&]<typename Slots>(auto&&... rest) -> Root::ptr {
      return MakeShape<Walk, lead::TwoPhaseScored<Slots>>(
        ctx, ctx.fetcher, query.Segment(), *query.state.reader, args,
        std::forward<decltype(rest)>(rest)...);
    });
}

}  // namespace irs::hits

#define IRS_HITS_PHRASE(Query, Match)                            \
  template Root::ptr MakePhrase<irs::detail::PhraseMatch::Match>( \
    const Query&, const Context&)
