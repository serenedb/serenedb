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

#include <cstdint>
#include <span>
#include <type_traits>
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/conjunction_scored.hpp"
#include "iresearch/search/common/optional_scored.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/lead/sparse_conjunction_scored.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/plan.hpp"

namespace irs::lead {

Node::ptr MakeSparseConjunctionScored(
  std::span<const PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  const ScoredCtx& ctx, ScoreMergeType merge, score_t absorbed) {
  if (terms.empty() && filters.empty()) {
    return {};
  }
  const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  if (absorbed == 0 && terms.size() + filters.size() == 1) {
    if (!terms.empty()) {
      return MakePostingScored(terms.front(), segment, recipe);
    }
    return filters.front()->PlanLead(ctx);
  }
  return BuildScoredConjunction<Node::ptr>(
    terms, filters, nullptr, nullptr, kNoBoost, segment, recipe,
    [&](const PostingClause& posting, const QueryBuilder* child,
        uint64_t interrogations) -> probe::Node::ptr {
      if (child == nullptr) {
        return probe::MakePostingScored(posting, segment, recipe);
      }
      return child->PlanProbe(ctx, interrogations);
    },
    [&](const QueryBuilder& child) -> Node::ptr { return child.PlanLead(ctx); },
    [&]<typename Head, typename Tail>(auto&& head, auto&& tail) -> Node::ptr {
      using Node = SparseConjunctionScored<Head, Tail>;
      return memory::make_managed<Impl<Node>>(
        std::piecewise_construct, merge, std::forward<decltype(head)>(head),
        std::forward<decltype(tail)>(tail), absorbed);
    });
}

Node::ptr MakeSparseConjunctionWithScored(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters,
  search::Terms should_uniformity, uint32_t min_should_match,
  const SubReader& segment, const ScoredCtx& ctx, ScoreMergeType merge,
  score_t absorbed) {
  SDB_ASSERT(min_should_match != 0);
  SDB_ASSERT(!must.empty() || !must_filters.empty());
  const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  auto head = MakeSparseConjunctionScored(must, must_filters, segment, ctx,
                                          merge, absorbed);
  if (!head) {
    return {};
  }
  auto tail = probe::BuildOptionalProbeScored(
    should, should_filters, should_uniformity, min_should_match, segment,
    recipe, merge, IncludeCandidates(must, must_filters, segment), ctx);
  if (!tail) {
    return {};
  }
  using Node = SparseConjunctionScored<Erased, probe::Erased>;
  return memory::make_managed<Impl<Node>>(
    std::piecewise_construct, merge, std::forward_as_tuple(std::move(head)),
    std::forward_as_tuple(std::move(tail)));
}

}  // namespace irs::lead
