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

#include "iresearch/search/detail/phrase_of.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/fill/plan.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/lead/two_phase_docs.hpp"
#include "iresearch/search/lead/two_phase_scored.hpp"

namespace irs::fill {

template<detail::PhraseMatch M, typename Query>
Node::ptr MakePhraseDocs(const Query& query) {
  return detail::BuildPhrase<M>(
    query, [&]<typename Slots>(auto&&... args) -> Node::ptr {
      return memory::make_managed<ByWalkDocs<lead::TwoPhaseDocs<Slots>>>(
        std::forward<decltype(args)>(args)...);
    });
}

template<detail::PhraseMatch M, typename Query>
Node::ptr MakePhraseScored(const Query& query, const detail::ScoredCtx& ctx,
                           ScoreMergeType merge) {
  const auto record = query.Stats(ctx);
  if (!record.stats || !query.state.reader) {
    return {};
  }
  const detail::ScoreArgs args{.scorer = record.scorer,
                               .stats = record.stats,
                               .fetcher = ctx.fetcher,
                               .boost = query.Boost()};
  return detail::BuildPhrase<M, true>(
    query, [&]<typename Slots>(auto&&... rest) -> Node::ptr {
      return memory::make_managed<ByWalkScored<lead::TwoPhaseScored<Slots>>>(
        merge, *ctx.fetcher, query.Segment(), *query.state.reader, args,
        std::forward<decltype(rest)>(rest)...);
    });
}

}  // namespace irs::fill

#define IRS_FILL_PHRASE_DOCS(Query, Match) \
  template Node::ptr MakePhraseDocs<detail::PhraseMatch::Match>(const Query&)

#define IRS_FILL_PHRASE_SCORED(Query, Match)                         \
  template Node::ptr MakePhraseScored<detail::PhraseMatch::Match>(   \
    const Query&, const detail::ScoredCtx&, ScoreMergeType)
