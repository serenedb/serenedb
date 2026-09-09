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
#include <tuple>
#include <utility>

#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/collect_scored.hpp"
#include "iresearch/search/common/conjunction_scored.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/common/probe_leaves.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/common/score_policy.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/lead/boolean_sparse.hpp"
#include "iresearch/search/lead/make_boolean.hpp"
#include "iresearch/search/lead/posting_scored.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/leaves.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/plan.hpp"

namespace irs::lead {
namespace {

template<typename Lead, typename Probes, typename Optional, typename Excludes,
         typename Score, typename LeadArgs, typename ProbesArgs,
         typename OptionalArgs, typename ExcludesArgs>
Node::ptr MakeSparseScored(LeadArgs&& lead, ProbesArgs&& probes,
                           OptionalArgs&& optional, ExcludesArgs&& excludes,
                           Score score) {
  using Node = BooleanSparse<Lead, Probes, Optional, Excludes, Score>;
  return memory::make_managed<Impl<Node>>(
    std::piecewise_construct, std::forward<LeadArgs>(lead),
    std::forward<ProbesArgs>(probes), std::forward<OptionalArgs>(optional),
    std::forward<ExcludesArgs>(excludes), score);
}

}  // namespace

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
      return MakeSparseScored<Head, Tail, utils::Empty, utils::Empty>(
        std::forward<decltype(head)>(head), std::forward<decltype(tail)>(tail),
        std::forward_as_tuple(), std::forward_as_tuple(),
        search::Scored{merge, absorbed});
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
  return MakeSparseScored<Erased, probe::Erased, utils::Empty, utils::Empty>(
    std::forward_as_tuple(std::move(head)),
    std::forward_as_tuple(std::move(tail)), std::forward_as_tuple(),
    std::forward_as_tuple(), search::Scored{merge, 0});
}

Node::ptr MakeSparseExclusionScored(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters,
  search::Terms should_uniformity, uint32_t min_should_match,
  std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const ScoredCtx& ctx, ScoreMergeType merge, score_t absorbed) {
  SDB_ASSERT(!excludes.empty() || !exclude_filters.empty());
  const auto candidates = IncludeCandidates(must, must_filters, segment);
  if (absorbed == 0 && should.empty() && should_filters.empty() &&
      must.size() == 1 && must_filters.empty() &&
      search::ScoresPerDocTerm(must.front())) {
    const auto& posting = must.front();
    const auto& meta = posting.state.cookie;
    const auto& own = *posting.state.reader;
    const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
    const auto& doc = *search::DocOf(own);
    return ResolveInput(doc, [&]<typename Input> -> Node::ptr {
      using Include = PostingLeadScored<Input>;
      return search::BuildExcludeSideOf<Node::ptr, Input>(
        excludes, exclude_filters, nullptr, segment, candidates,
        [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
          return MakeSparseScored<Include, utils::Empty, utils::Empty, Exclude>(
            std::forward_as_tuple(meta, doc, segment, own,
                                  recipe.Args(posting.stats, posting.boost)),
            std::forward_as_tuple(), std::forward_as_tuple(),
            std::forward<decltype(exclude)>(exclude), search::Inherited{});
        });
    });
  }
  auto include = MakeRequiredScored(must, must_filters, should, should_filters,
                                    should_uniformity, min_should_match,
                                    segment, ctx, merge, absorbed);
  if (!include) {
    return {};
  }
  return search::BuildExcludeSide<Node::ptr>(
    excludes, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& exclude) -> Node::ptr {
      return MakeSparseScored<Erased, utils::Empty, utils::Empty, Exclude>(
        std::forward_as_tuple(std::move(include)), std::forward_as_tuple(),
        std::forward_as_tuple(), std::forward<decltype(exclude)>(exclude),
        search::Inherited{});
    });
}

Node::ptr MakeSparseBoostScored(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters,
  search::Terms should_uniformity, const SubReader& segment,
  const ScoredCtx& ctx, ScoreMergeType merge, score_t absorbed) {
  SDB_ASSERT(!should.empty() || !should_filters.empty());
  const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  const auto clause = probe::ScoredClauseOf(segment, ctx, recipe);
  const auto candidates = IncludeCandidates(must, must_filters, segment);
  const auto build = [&]<typename Head>(auto&& head) -> Node::ptr {
    return search::BuildOptionalLeaves<Node::ptr>(
      should, should_filters, should_uniformity, nullptr, nullptr, kNoBoost,
      segment, recipe, candidates, clause,
      [&]<typename Optional>(size_t size, auto&& init) -> Node::ptr {
        return search::ResolveArity<search::kTailArity, search::kTailFloor>(
          size, [&]<size_t N> -> Node::ptr {
            return MakeSparseScored<Head, utils::Empty,
                                    probe::BoostLeaves<Optional, N>,
                                    utils::Empty>(
              std::forward<decltype(head)>(head), std::forward_as_tuple(),
              std::forward_as_tuple(size, std::forward<decltype(init)>(init)),
              std::forward_as_tuple(), search::Scored{merge, 0});
          });
      });
  };
  if (absorbed == 0 && must.size() == 1 && must_filters.empty() &&
      search::ScoresPerDocTerm(must.front())) {
    const auto& posting = must.front();
    const auto& meta = posting.state.cookie;
    const auto& own = *posting.state.reader;
    const auto& doc = *search::DocOf(own);
    return ResolveInput(doc, [&]<typename Input> -> Node::ptr {
      using Head = PostingLeadScored<Input>;
      return build.template operator()<Head>(std::forward_as_tuple(
        meta, doc, segment, own, recipe.Args(posting.stats, posting.boost)));
    });
  }
  Node::ptr head = (must.empty() && must_filters.empty())
                     ? MakeAllScored(segment, absorbed)
                     : MakeSparseConjunctionScored(must, must_filters, segment,
                                                   ctx, merge, absorbed);
  if (!head) {
    return {};
  }
  return build.template operator()<Erased>(
    std::forward_as_tuple(std::move(head)));
}

}  // namespace irs::lead
