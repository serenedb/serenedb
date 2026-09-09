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
#include <tuple>
#include <utility>

#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/collect_scored.hpp"
#include "iresearch/search/common/conjunction_scored.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/common/optional_scored.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/common/score_policy.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/posting_scored.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/leaves.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/scored/boolean_sparse.hpp"
#include "iresearch/search/scored/make_boolean.hpp"

namespace irs::scored {
namespace {

template<typename Lead, typename Probes, typename Optional, typename Excludes,
         typename LeadArgs, typename ProbesArgs, typename OptionalArgs,
         typename ExcludesArgs>
Root::ptr MakeSparse(const Context& ctx, search::Scored score, LeadArgs&& lead,
                     ProbesArgs&& probes, OptionalArgs&& optional,
                     ExcludesArgs&& excludes) {
  return MakeShape<BooleanSparse, Lead, Probes, Optional, Excludes>(
    ctx, std::piecewise_construct, ctx.fetcher, score,
    std::forward<LeadArgs>(lead), std::forward<ProbesArgs>(probes),
    std::forward<OptionalArgs>(optional), std::forward<ExcludesArgs>(excludes));
}

}  // namespace

Root::ptr MakeSparseConjunction(const BooleanQuery& query,
                                const SubReader& segment, const Context& ctx,
                                ScoreMergeType merge, score_t absorbed) {
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  const auto should_uniformity = query.Uniformity(Occur::Should);
  const auto min_should_match = query.MinShouldMatch();
  const bool no_must = must.empty() && must_filters.empty();
  const bool optional = !should.empty() || !should_filters.empty();
  SDB_ASSERT(!no_must || optional);
  const search::ScoreRecipe recipe{.segment = &segment,
                                   .fetcher = &ctx.fetcher};
  const auto child_ctx = ScoredOf(ctx);
  const auto clause = probe::ScoredClauseOf(segment, child_ctx, recipe);
  const auto candidates =
    search::IncludeCandidates(must, must_filters, segment);
  const search::Scored score{merge, absorbed};
  const auto conjunction = [&]<typename Make>(Make&& make) -> Root::ptr {
    return search::BuildScoredConjunction<Root::ptr>(
      must, must_filters, nullptr, nullptr, kNoBoost, segment, recipe, clause,
      [&](const QueryBuilder& child) -> lead::Node::ptr {
        return child.PlanLead(child_ctx);
      },
      std::forward<Make>(make));
  };
  if (optional && min_should_match == 0) {
    if (auto boosted =
          MakeBoostedPosting(query, segment, ctx, merge, absorbed)) {
      return boosted;
    }
    return search::BuildOptionalLeaves<Root::ptr>(
      should, should_filters, should_uniformity, nullptr, nullptr, kNoBoost,
      segment, recipe, candidates, clause,
      [&]<typename Leaf>(size_t size, auto&& init) -> Root::ptr {
        using Boost = probe::BoostLeaves<Leaf>;
        const auto boost =
          std::forward_as_tuple(size, std::forward<decltype(init)>(init));
        if (no_must) {
          auto all = lead::MakeAllScored(
            segment,
            search::AllDocsScore(segment, ScoreArgs{.scorer = &ctx.scorer,
                                                    .fetcher = &ctx.fetcher,
                                                    .boost = kNoBoost}));
          if (!all) {
            return {};
          }
          return MakeSparse<lead::Erased, utils::Empty, Boost, utils::Empty>(
            ctx, score, std::forward_as_tuple(std::move(all)),
            std::forward_as_tuple(), boost, std::forward_as_tuple());
        }
        return conjunction([&]<typename Head, typename Tail>(
                             auto&& head, auto&& tail) -> Root::ptr {
          return MakeSparse<Head, Tail, Boost, utils::Empty>(
            ctx, score, std::forward<decltype(head)>(head),
            std::forward<decltype(tail)>(tail), boost, std::forward_as_tuple());
        });
      });
  }
  SDB_ASSERT(!no_must);
  probe::Node::ptr held;
  if (optional) {
    held = probe::MakeRequiredScored(
      {}, {}, search::Terms::Mixed, should, should_filters, should_uniformity,
      min_should_match, segment, recipe, merge, candidates, child_ctx);
    if (!held) {
      return {};
    }
  }
  if (must.size() + must_filters.size() == 1 && !held && absorbed == 0) {
    Root::ptr only;
    query.VisitHead(
      Occur::Must,
      [&](const PostingClause& posting) {
        only = posting.state.cookie.docs_count == 1
                 ? MakeSinglePosting(posting, segment, ctx)
                 : MakePosting(posting, segment, ctx);
        return true;
      },
      [&](const QueryBuilder& child) {
        only = child.PlanScored(ctx);
        return true;
      });
    return only;
  }
  return conjunction(
    [&]<typename Head, typename Tail>(auto&& head, auto&& tail) -> Root::ptr {
      if (!held) {
        return MakeSparse<Head, Tail, utils::Empty, utils::Empty>(
          ctx, score, std::forward<decltype(head)>(head),
          std::forward<decltype(tail)>(tail), std::forward_as_tuple(),
          std::forward_as_tuple());
      }
      return MakeSparse<Head, Tail, probe::Erased, utils::Empty>(
        ctx, score, std::forward<decltype(head)>(head),
        std::forward<decltype(tail)>(tail),
        std::forward_as_tuple(std::move(held)), std::forward_as_tuple());
    });
}

Root::ptr MakeSparseExclusion(const BooleanQuery& query,
                              const SubReader& segment, const Context& ctx,
                              ScoreMergeType merge, score_t absorbed) {
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  const std::span excludes = query.Terms(Occur::MustNot);
  const std::span exclude_filters = query.Queries(Occur::MustNot);
  SDB_ASSERT(!excludes.empty() || !exclude_filters.empty());
  const auto candidates =
    search::IncludeCandidates(must, must_filters, segment);
  const search::Scored score{merge, 0};
  if (absorbed == 0 && should.empty() && should_filters.empty() &&
      must.size() == 1 && must_filters.empty() &&
      search::ScoresPerDocTerm(must.front())) {
    const auto& posting = must.front();
    const auto& own = *posting.state.reader;
    const auto& meta = posting.state.cookie;
    const auto& doc = *search::DocOf(own);
    const search::ScoreRecipe recipe{.segment = &segment,
                                     .fetcher = &ctx.fetcher};
    return search::ResolveInput(doc, [&]<typename Input> -> Root::ptr {
      using Include = search::PostingLeadScored<Input>;
      return search::BuildExcludeSideOf<Root::ptr, Input>(
        excludes, exclude_filters, nullptr, segment, candidates,
        [&]<typename Exclude>(auto&& negated) -> Root::ptr {
          return MakeSparse<Include, utils::Empty, utils::Empty, Exclude>(
            ctx, score,
            std::forward_as_tuple(meta, doc, segment, own,
                                  recipe.Args(posting.stats, posting.boost)),
            std::forward_as_tuple(), std::forward_as_tuple(),
            std::forward<decltype(negated)>(negated));
        });
    });
  }
  auto include = lead::MakeRequiredScored(
    must, must_filters, should, should_filters, query.Uniformity(Occur::Should),
    query.MinShouldMatch(), segment, ScoredOf(ctx), merge, absorbed);
  if (!include) {
    return {};
  }
  return search::BuildExcludeSide<Root::ptr>(
    excludes, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& negated) -> Root::ptr {
      return MakeSparse<lead::Erased, utils::Empty, utils::Empty, Exclude>(
        ctx, score, std::forward_as_tuple(std::move(include)),
        std::forward_as_tuple(), std::forward_as_tuple(),
        std::forward<decltype(negated)>(negated));
    });
}

}  // namespace irs::scored
