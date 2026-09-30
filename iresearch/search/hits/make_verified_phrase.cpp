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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/detail/verified_phrase_of.hpp"
#include "iresearch/search/hits/make.hpp"
#include "iresearch/search/hits/walk.hpp"
#include "iresearch/search/lead/two_phase_scored.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"

namespace irs::hits {

Root::ptr MakeVerifiedPhrase(const VerifiedPhraseQuery& query,
                             const Context& ctx) {
  if (query.Stats().stats == nullptr) {
    return irs::detail::MakeVerifiedPhrase<ConstantWalk, Root::ptr>(query, 0,
                                                                    score_t{0});
  }
  const auto record = query.Stats(ScoredOf(ctx));
  const irs::detail::ScoreArgs args{.scorer = record.scorer,
                                    .stats = record.stats,
                                    .fetcher = &ctx.fetcher,
                                    .boost = query.Boost()};
  if (const auto value =
        irs::detail::ConstantOf(query.Segment(), query.Reader(), args)) {
    return irs::detail::MakeVerifiedPhrase<ConstantWalk, Root::ptr>(query, 0,
                                                                    *value);
  }
  return irs::detail::MakeVerifiedPhrase<Walk, Root::ptr, true,
                                         lead::TwoPhaseScored>(
    query, 0, ctx.fetcher, query.Segment(), query.Reader(), args);
}

}  // namespace irs::hits
