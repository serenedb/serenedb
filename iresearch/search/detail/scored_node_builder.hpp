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

#include <cstdint>
#include <span>
#include <tuple>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/boolean_builder.hpp"
#include "iresearch/search/detail/boolean_groups.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/conjunction.hpp"
#include "iresearch/search/detail/exclusion_of.hpp"
#include "iresearch/search/detail/fill_posting_scored.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/probe_leaves.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/detail/window_set.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/posting_scored.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/leaves.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::detail::builder {

template<typename Api>
Result<Api> MakeNodeConjunction(std::span<const PostingClause> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                const SubReader& segment,
                                const Context<Api>& ctx, ScoreMergeType merge,
                                score_t absorbed) {
  const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  if constexpr (Api::kSingleClause) {
    if (terms.empty() && filters.empty()) {
      return {};
    }
    if (absorbed == 0 && terms.size() + filters.size() == 1) {
      return terms.empty()
               ? filters.front()->PlanLead(ctx)
               : lead::MakePostingScored(terms.front(), segment, recipe);
    }
  } else {
    SDB_ASSERT(!terms.empty() || !filters.empty());
  }
  return MakeSparseConjunction<Api>(terms, filters, segment, ctx,
                                    ScoringOf<Api>(segment, ctx, merge),
                                    Scored{Api::Inner(merge), absorbed});
}

template<typename Api>
Result<Api> MakeNodeConjunctionWith(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters, Terms uniformity,
  uint32_t min_match, const SubReader& segment, const Context<Api>& ctx,
  ScoreMergeType merge, score_t absorbed) {
  SDB_ASSERT(min_match != 0);
  SDB_ASSERT(!must.empty() || !must_filters.empty());
  auto head = MakeNodeConjunction<typename Api::SparseApi>(
    must, must_filters, segment, ctx, merge, absorbed);
  if (!head) {
    return {};
  }
  const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  auto tail = probe::BuildOptionalProbeScored(
    should, should_filters, uniformity, min_match, segment, recipe, merge,
    IncludeCandidates(must, must_filters, segment), ctx);
  if (!tail) {
    return {};
  }
  return Api::template MakeSparse<lead::Erased, probe::Erased, utils::Empty,
                                  utils::Empty>(
    ctx, merge, Scored{merge, 0}, std::forward_as_tuple(std::move(head)),
    std::forward_as_tuple(std::move(tail)), std::forward_as_tuple(),
    std::forward_as_tuple());
}

template<typename Api>
Result<Api> MakeNodeExclusion(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters, Terms uniformity,
  uint32_t min_match, std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const Context<Api>& ctx, ScoreMergeType merge, ScoreMergeType own,
  score_t absorbed) {
  SDB_ASSERT(!excludes.empty() || !exclude_filters.empty());
  const auto candidates = IncludeCandidates(must, must_filters, segment);
  if (absorbed == 0 && should.empty() && should_filters.empty() &&
      must.size() == 1 && must_filters.empty() &&
      ScoresPerDocTerm(must.front())) {
    const auto& posting = must.front();
    const auto& meta = posting.state.cookie;
    const auto& reader = *posting.state.reader;
    const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
    const auto& doc = *DocOf(reader);
    return ResolveInput(doc, [&]<typename Input> -> Result<Api> {
      using Include = PostingLeadScored<Input>;
      return BuildExcludeSideOf<Result<Api>, Input>(
        excludes, exclude_filters, nullptr, segment, candidates,
        [&]<typename Exclude>(auto&& exclude) -> Result<Api> {
          return Api::template MakeSparse<Include, utils::Empty, utils::Empty,
                                          Exclude>(
            ctx, merge, Inherited{},
            std::forward_as_tuple(meta, doc, segment, reader,
                                  recipe.Args(posting.stats, posting.boost)),
            std::forward_as_tuple(), std::forward_as_tuple(),
            std::forward<decltype(exclude)>(exclude));
        });
    });
  }
  auto include = lead::MakeRequiredScored(must, must_filters, should,
                                          should_filters, uniformity, min_match,
                                          segment, ctx, own, absorbed);
  if (!include) {
    return {};
  }
  return MakeSparseExclusionOf<Api>(std::move(include), excludes,
                                    exclude_filters, segment, candidates, ctx,
                                    merge, Inherited{});
}

template<typename Api>
Result<Api> MakeNodeBoost(std::span<const PostingClause> must,
                          std::span<const QueryBuilder::ptr> must_filters,
                          std::span<const PostingClause> should,
                          std::span<const QueryBuilder::ptr> should_filters,
                          Terms uniformity, const SubReader& segment,
                          const Context<Api>& ctx, ScoreMergeType merge,
                          score_t absorbed) {
  SDB_ASSERT(!should.empty() || !should_filters.empty());
  const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  const auto clause = probe::ScoredClauseOf(segment, ctx, recipe);
  const auto candidates = IncludeCandidates(must, must_filters, segment);
  const auto inner = Api::Inner(merge);
  const auto build = [&]<typename Head>(auto&& head) -> Result<Api> {
    return BuildProbeLeaves<Result<Api>>(
      should, should_filters, nullptr, segment, candidates, ProbeOrder::Densest,
      TermScoring{.scorer = nullptr,
                  .boost = kNoBoost,
                  .uniformity = uniformity,
                  .recipe = recipe},
      clause, [&]<typename Optional>(size_t size, auto&& init) -> Result<Api> {
        return ResolveArity<kTailArity, kTailFloor>(
          size, [&]<size_t N> -> Result<Api> {
            return Api::template MakeSparse<Head, utils::Empty,
                                            probe::BoostLeaves<Optional, N>,
                                            utils::Empty>(
              ctx, merge, Scored{inner, 0}, std::forward<decltype(head)>(head),
              std::forward_as_tuple(),
              std::forward_as_tuple(size, std::forward<decltype(init)>(init)),
              std::forward_as_tuple());
          });
      });
  };
  if (absorbed == 0 && must.size() == 1 && must_filters.empty() &&
      ScoresPerDocTerm(must.front())) {
    const auto& posting = must.front();
    const auto& meta = posting.state.cookie;
    const auto& reader = *posting.state.reader;
    const auto& doc = *DocOf(reader);
    return ResolveInput(doc, [&]<typename Input> -> Result<Api> {
      using Head = PostingLeadScored<Input>;
      return build.template operator()<Head>(std::forward_as_tuple(
        meta, doc, segment, reader, recipe.Args(posting.stats, posting.boost)));
    });
  }
  lead::Node::ptr head =
    must.empty() && must_filters.empty()
      ? lead::MakeAllScored(segment, absorbed)
      : lead::MakeSparseConjunctionScored(must, must_filters, segment, ctx,
                                          inner, absorbed);
  if (!head) {
    return {};
  }
  return build.template operator()<lead::Erased>(
    std::forward_as_tuple(std::move(head)));
}

template<typename Api>
Result<Api> MakeNodeRequired(std::span<const PostingClause> must,
                             std::span<const QueryBuilder::ptr> must_filters,
                             std::span<const PostingClause> should,
                             std::span<const QueryBuilder::ptr> should_filters,
                             Terms uniformity, uint32_t min_match,
                             const SubReader& segment, const Context<Api>& ctx,
                             ScoreMergeType merge, score_t absorbed) {
  const bool optional = !should.empty() || !should_filters.empty();
  if (optional && min_match == 0) {
    return MakeNodeBoost<Api>(must, must_filters, should, should_filters,
                              uniformity, segment, ctx, merge, absorbed);
  }
  if (must.empty() && must_filters.empty()) {
    if (!optional) {
      return Api::MakeAll(segment, ctx, merge, absorbed);
    }
    const auto scoring = ScoringOf<Api>(segment, ctx, merge, uniformity);
    return min_match == 1
             ? MakeDisjunction<Api>(should, should_filters, nullptr, segment,
                                    scoring, ctx, absorbed)
             : MakeThreshold<Api>(should, should_filters, segment, min_match,
                                  scoring, ctx, absorbed);
  }
  if (min_match != 0) {
    return Api::MakeRequiredWith(must, must_filters, should, should_filters,
                                 uniformity, min_match, segment, ctx, merge,
                                 absorbed);
  }
  return MakeNodeConjunction<Api>(must, must_filters, segment, ctx, merge,
                                  absorbed);
}

template<typename Api>
Result<Api> MakeNode(const BooleanQuery& query, const Context<Api>& ctx,
                     ScoreMergeType merge) {
  const auto& segment = query.Segment();
  const auto must = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto should = query.Terms(Occur::Should);
  const auto should_filters = query.Queries(Occur::Should);
  const auto excludes = query.Terms(Occur::MustNot);
  const auto exclude_filters = query.Queries(Occur::MustNot);
  const auto uniformity = query.Uniformity(Occur::Should);
  const auto min_match = query.MinShouldMatch();
  const auto absorbed = query.Absorbed();
  const auto own = query.MergeType();
  if (!excludes.empty() || !exclude_filters.empty()) {
    return MakeNodeExclusion<Api>(
      must, must_filters, should, should_filters, uniformity, min_match,
      excludes, exclude_filters, segment, ctx, merge, own, absorbed);
  }
  if constexpr (Api::kWrapsMerge) {
    if (own != merge) {
      auto child = MakeNode<Api>(query, ctx, own);
      if (!child) {
        return {};
      }
      return Api::WrapMerge(merge, std::move(child));
    }
  }
  return MakeNodeRequired<Api>(must, must_filters, should, should_filters,
                               uniformity, min_match, segment, ctx, merge,
                               absorbed);
}

}  // namespace irs::detail::builder
