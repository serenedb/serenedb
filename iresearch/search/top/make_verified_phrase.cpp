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
#include "iresearch/search/lead/two_phase_scored.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/walk.hpp"

namespace irs::top {

Root::ptr MakeVerifiedPhrase(const VerifiedPhraseQuery& query,
                             const Context& ctx) {
  if (query.Stats().stats == nullptr) {
    if (ctx.table != nullptr) {
      return irs::detail::MakeVerifiedPhrase<FilteredConstantWalk, Root::ptr>(
        query, 0, ctx.table, score_t{0});
    }
    return irs::detail::MakeVerifiedPhrase<PlainConstantWalk, Root::ptr>(
      query, 0, utils::Empty{}, score_t{0});
  }
  const auto record = query.Stats(ScoredOf(ctx));
  const irs::detail::ScoreArgs args{.scorer = record.scorer,
                                    .stats = record.stats,
                                    .fetcher = &ctx.fetcher,
                                    .boost = query.Boost()};
  if (const auto value =
        irs::detail::ConstantOf(query.Segment(), query.Reader(), args)) {
    if (ctx.table != nullptr) {
      return irs::detail::MakeVerifiedPhrase<FilteredConstantWalk, Root::ptr>(
        query, 0, ctx.table, *value);
    }
    return irs::detail::MakeVerifiedPhrase<PlainConstantWalk, Root::ptr>(
      query, 0, utils::Empty{}, *value);
  }
  if (ctx.table != nullptr) {
    return irs::detail::MakeVerifiedPhrase<FilteredWalk, Root::ptr, true,
                                           lead::TwoPhaseScored>(
      query, 0, ctx.table, ctx.fetcher, query.Segment(), query.Reader(), args);
  }
  return irs::detail::MakeVerifiedPhrase<PlainWalk, Root::ptr, true,
                                         lead::TwoPhaseScored>(
    query, 0, utils::Empty{}, ctx.fetcher, query.Segment(), query.Reader(),
    args);
}

}  // namespace irs::top
