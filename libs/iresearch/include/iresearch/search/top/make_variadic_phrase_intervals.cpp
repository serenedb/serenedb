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
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/phrase_of.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/lead/two_phase_scored.hpp"
#include "iresearch/search/phrase_query.hpp"
#include "iresearch/search/top/detail/walk.hpp"
#include "iresearch/search/top/make.hpp"

namespace irs::top {

Root::ptr MakeVariadicPhraseIntervals(const VariadicPhraseQuery& query,
                                      const Context& ctx) {
  const auto record = query.Stats(ScoredOf(ctx));
  const auto* const stats = record.stats;
  if (stats == nullptr || query.state.reader == nullptr) {
    return {};
  }
  const search::ScoreArgs args{.scorer = record.scorer,
                               .stats = stats,
                               .fetcher = &ctx.fetcher,
                               .boost = query.Boost()};
  if (!query.state.volatile_boost) {
    if (const auto value =
          search::ConstantOf(query.Segment(), *query.state.reader, args)) {
      if (ctx.table != nullptr) {
        return search::MakeVariadicPhraseOf<search::PhraseMatch::Intervals,
                                            FilteredConstantWalk, Root::ptr>(
          query, ctx.table, *value);
      }
      return search::MakeVariadicPhraseOf<search::PhraseMatch::Intervals,
                                          PlainConstantWalk, Root::ptr>(
        query, utils::Empty{}, *value);
    }
  }
  if (ctx.table != nullptr) {
    return search::MakeVariadicPhraseOf<search::PhraseMatch::Intervals,
                                        FilteredWalk, Root::ptr, true,
                                        lead::TwoPhaseScored>(
      query, ctx.table, ctx.fetcher, query.Segment(), *query.state.reader,
      args);
  }
  return search::MakeVariadicPhraseOf<search::PhraseMatch::Intervals, PlainWalk,
                                      Root::ptr, true, lead::TwoPhaseScored>(
    query, utils::Empty{}, ctx.fetcher, query.Segment(), *query.state.reader,
    args);
}

}  // namespace irs::top
