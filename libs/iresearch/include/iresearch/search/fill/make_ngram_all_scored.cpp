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

#include <utility>
#include <vector>

#include "iresearch/search/common/ngram_of.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/fill/plan.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/lead/two_phase_scored.hpp"
#include "iresearch/search/ngram_similarity_query.hpp"

namespace irs::fill {

Node::ptr MakeNGramAllScored(const NGramSimilarityQuery& query,
                             const ScoredCtx& ctx, ScoreMergeType merge) {
  const auto record = query.Stats(ctx);
  const auto* const stats = record.stats;
  if (stats == nullptr) {
    return {};
  }
  const ScoreArgs args{.scorer = record.scorer,
                       .stats = stats,
                       .fetcher = ctx.fetcher,
                       .boost = query.Boost()};
  return search::BuildAll<true>(
    query, [&]<typename Slots>(auto&&... rest) -> Node::ptr {
      using Node = lead::TwoPhaseScored<Slots>;
      return memory::make_managed<ByWalkScored<Node>>(
        merge, query.Segment(), *query.State().reader, args,
        std::forward<decltype(rest)>(rest)...);
    });
}

}  // namespace irs::fill
