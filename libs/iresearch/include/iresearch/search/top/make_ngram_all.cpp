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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/ngram_of.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/lead/two_phase_scored.hpp"
#include "iresearch/search/ngram_similarity_query.hpp"
#include "iresearch/search/top/detail/walk.hpp"
#include "iresearch/search/top/make.hpp"

namespace irs::top {

Root::ptr MakeNGramAll(const NGramSimilarityQuery& query, const Context& ctx) {
  const auto record = query.Stats(ScoredOf(ctx));
  const auto* const stats = record.stats;
  if (stats == nullptr) {
    return {};
  }
  const search::ScoreArgs args{.scorer = record.scorer,
                               .stats = stats,
                               .fetcher = &ctx.fetcher,
                               .boost = query.Boost()};
  if (const auto value =
        search::ConstantOf(query.Segment(), *query.State().reader, args)) {
    return search::BuildAll(
      query, [&]<typename Slots>(auto&&... rest) -> Root::ptr {
        using Node = lead::TwoPhaseDocs<Slots>;
        return MakeShape<detail::ConstantWalk, Node>(
          ctx, *value, std::forward<decltype(rest)>(rest)...);
      });
  }
  return search::BuildAll<true>(
    query, [&]<typename Slots>(auto&&... rest) -> Root::ptr {
      using Node = lead::TwoPhaseScored<Slots>;
      return MakeShape<detail::Walk, Node>(
        ctx, ctx.fetcher, query.Segment(), *query.State().reader, args,
        std::forward<decltype(rest)>(rest)...);
    });
}

}  // namespace irs::top
