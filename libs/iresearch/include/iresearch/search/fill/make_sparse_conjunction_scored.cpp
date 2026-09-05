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
#include <type_traits>
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/conjunction_scored.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/fill/plan.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/lead/sparse_conjunction_scored.hpp"
#include "iresearch/search/probe/make.hpp"

namespace irs::fill {

Node::ptr MakeSparseConjunctionScored(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  const ScoredCtx& ctx, ScoreMergeType merge, score_t absorbed) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  return search::BuildScoredConjunction<Node::ptr>(
    terms, filters, nullptr, nullptr, kNoBoost, segment, recipe,
    [&](const search::PostingClause& posting, const QueryBuilder* child,
        uint64_t interrogations) -> probe::Node::ptr {
      if (child == nullptr) {
        return probe::MakePostingScored(posting, segment, recipe);
      }
      return child->PlanProbe(ctx, interrogations);
    },
    [&](const QueryBuilder& child) -> lead::Node::ptr {
      return child.PlanLead(ctx);
    },
    [&]<typename Head, typename Tail>(auto&& head, auto&& tail) -> Node::ptr {
      using Node = lead::SparseConjunctionScored<Head, Tail>;
      return memory::make_managed<ByWalkScored<Node>>(
        merge, std::piecewise_construct, ScoreMergeType::Sum,
        std::forward<decltype(head)>(head), std::forward<decltype(tail)>(tail),
        absorbed);
    });
}

}  // namespace irs::fill
