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
#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/make_term.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/probe_leaves.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/detail/window_set.hpp"
#include "iresearch/search/fill/all_docs.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/posting_scored.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/leaves.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::detail::builder {

template<typename Family, typename F>
Result<Family> ResolveBoostArity(size_t size, F&& f) {
  if constexpr (Family::kBoostArity) {
    return ResolveArity<kTailArity, kTailFloor>(size, std::forward<F>(f));
  } else {
    return f.template operator()<0>();
  }
}

template<typename Family, typename Ctx>
auto ScoringFor(const SubReader& segment, const Ctx& ctx, ScoreMergeType merge,
                Terms uniformity) {
  if constexpr (kScoredPlan<Family, Ctx>) {
    return ScoringOf<Family>(segment, ctx, merge, uniformity);
  } else {
    return NoScoring{};
  }
}

template<typename Family, typename Ctx>
auto ScoreFor(ScoreMergeType merge, score_t absorbed) {
  if constexpr (kScoredPlan<Family, Ctx>) {
    return Scored{merge, absorbed};
  } else {
    return utils::Empty{};
  }
}

template<typename Family, typename Ctx>
Result<Family> MakeConjunction(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters, Terms uniformity,
  uint32_t min_match, const SubReader& segment, const Ctx& ctx,
  ScoreMergeType merge, score_t absorbed) {
  const bool no_must = must.empty() && must_filters.empty();
  const bool optional = (!should.empty() || !should_filters.empty()) &&
                        (kScoredPlan<Family, Ctx> || min_match != 0);
  SDB_ASSERT(!no_must || optional);
  const auto score = ScoreFor<Family, Ctx>(merge, absorbed);
  const auto conjunction = [&]<typename Make>(Make&& make) -> Result<Family> {
    return BuildSparseConjunction<Family>(
      must, must_filters, segment, ctx,
      ScoringFor<Family>(segment, ctx, merge, Terms::Mixed),
      std::forward<Make>(make));
  };
  if constexpr (kScoredPlan<Family, Ctx>) {
    if (optional && min_match == 0) {
      if constexpr (requires { &Family::MakeBoosted; }) {
        if (auto boosted =
              Family::MakeBoosted(must, must_filters, should, should_filters,
                                  segment, ctx, merge, absorbed)) {
          return boosted;
        }
      }
      const auto recipe = RecipeOf<Family>(segment, ctx);
      return BuildProbeLeaves<Result<Family>>(
        should, should_filters, nullptr, segment,
        IncludeCandidates(must, must_filters, segment), ProbeOrder::Densest,
        TermScoring{.scorer = nullptr,
                    .boost = kNoBoost,
                    .uniformity = uniformity,
                    .recipe = recipe},
        probe::ScoredClauseOf(segment, Family::Scoring(ctx), recipe),
        [&]<typename Leaf>(size_t size, auto&& init) -> Result<Family> {
          return ResolveBoostArity<Family>(
            size, [&]<size_t N> -> Result<Family> {
              using Boost = probe::BoostLeaves<Leaf, N>;
              const auto boost =
                std::forward_as_tuple(size, std::forward<decltype(init)>(init));
              if (no_must) {
                auto all = lead::MakeAllScored(segment, 0);
                if (!all) {
                  return {};
                }
                return Family::template MakeSparse<lead::Erased, utils::Empty,
                                                   Boost, utils::Empty>(
                  ctx, merge, score, std::forward_as_tuple(std::move(all)),
                  std::forward_as_tuple(), boost, std::forward_as_tuple());
              }
              return conjunction([&]<typename Head, typename Tail>(
                                   auto&& head, auto&& tail) -> Result<Family> {
                return Family::template MakeSparse<Head, Tail, Boost,
                                                   utils::Empty>(
                  ctx, merge, score, std::forward<decltype(head)>(head),
                  std::forward<decltype(tail)>(tail), boost,
                  std::forward_as_tuple());
              });
            });
        });
    }
  }
  SDB_ASSERT(!no_must);
  if (!optional) {
    if (must.size() + must_filters.size() == 1 && absorbed == 0) {
      return must.empty()
               ? Family::PlanChild(*must_filters.front(), ctx, merge)
               : Family::MakePosting(must.front(), segment, ctx, merge);
    }
    if constexpr (!kScoredPlan<Family, Ctx>) {
      if (auto windowed =
            MakeWindowConjunction<Family>(must, must_filters, segment, ctx)) {
        return windowed;
      }
      if (!must_filters.empty()) {
        if (auto folded = MakeBitset<Family>(
              {.must = must, .must_filters = must_filters}, segment, ctx)) {
          return folded;
        }
      }
    }
    return conjunction([&]<typename Head, typename Tail>(
                         auto&& head, auto&& tail) -> Result<Family> {
      return Family::template MakeSparse<Head, Tail, utils::Empty,
                                         utils::Empty>(
        ctx, merge, score, std::forward<decltype(head)>(head),
        std::forward<decltype(tail)>(tail), std::forward_as_tuple(),
        std::forward_as_tuple());
    });
  }
  const auto candidates = IncludeCandidates(must, must_filters, segment);
  ScoredCtx child;
  if constexpr (kScoredPlan<Family, Ctx>) {
    child = Family::Scoring(ctx);
  }
  auto held = probe::MakeOptional(
    should, should_filters, min_match, segment, candidates,
    ScoringFor<Family>(segment, ctx, merge, uniformity), child, score_t{0});
  if (!held) {
    return {};
  }
  if constexpr (!kScoredPlan<Family, Ctx>) {
    if (must.size() + must_filters.size() > 1) {
      if (auto folded = MakeConjunctionBitset<LeadNode::ptr>(
            must, must_filters, nullptr, segment, nullptr)) {
        return Family::template MakeSparse<lead::Erased, utils::Empty,
                                           probe::Erased, utils::Empty>(
          ctx, merge, score, std::forward_as_tuple(std::move(folded)),
          std::forward_as_tuple(), std::forward_as_tuple(std::move(held)),
          std::forward_as_tuple());
      }
    }
  }
  return conjunction([&]<typename Head, typename Tail>(
                       auto&& head, auto&& tail) -> Result<Family> {
    return Family::template MakeSparse<Head, Tail, probe::Erased, utils::Empty>(
      ctx, merge, score, std::forward<decltype(head)>(head),
      std::forward<decltype(tail)>(tail),
      std::forward_as_tuple(std::move(held)), std::forward_as_tuple());
  });
}

template<typename Family, typename Ctx>
Result<Family> MakeRequired(std::span<const PostingClause> must,
                            std::span<const QueryBuilder::ptr> must_filters,
                            std::span<const PostingClause> should,
                            std::span<const QueryBuilder::ptr> should_filters,
                            Terms uniformity, uint32_t min_match,
                            const SubReader& segment, const Ctx& ctx,
                            ScoreMergeType merge, score_t absorbed) {
  const bool optional = (!should.empty() || !should_filters.empty()) &&
                        (kScoredPlan<Family, Ctx> || min_match != 0);
  if (must.empty() && must_filters.empty() && (!optional || min_match != 0)) {
    if (!optional) {
      return Family::MakeAll(segment, ctx, merge, absorbed);
    }
    const auto scoring = ScoringFor<Family>(segment, ctx, merge, uniformity);
    if (min_match == 1) {
      return MakeDisjunction<Family>(should, should_filters, nullptr, segment,
                                     scoring, ctx, absorbed);
    }
    return MakeThreshold<Family>(should, should_filters, segment, min_match,
                                 scoring, ctx, absorbed);
  }
  return MakeConjunction<Family>(must, must_filters, should, should_filters,
                                 uniformity, min_match, segment, ctx, merge,
                                 absorbed);
}

template<typename Family, typename Ctx>
Result<Family> MakeExclusion(const BooleanQuery& query, const Ctx& ctx,
                             ScoreMergeType merge, score_t absorbed);

template<typename Family, typename Ctx>
Result<Family> Make(const BooleanQuery& query, const Ctx& ctx,
                    ScoreMergeType merge) {
  score_t absorbed = 0;
  if constexpr (kScoredPlan<Family, Ctx>) {
    absorbed = query.Absorbed();
  }
  if (!query.Terms(Occur::MustNot).empty() ||
      !query.Queries(Occur::MustNot).empty()) {
    return MakeExclusion<Family>(query, ctx, merge, absorbed);
  }
  if constexpr (kScoredPlan<Family, Ctx>) {
    if constexpr (Family::kWrapsMerge) {
      if (const auto own = query.MergeType(); own != merge) {
        auto child = Make<Family>(query, ctx, own);
        if (!child) {
          return {};
        }
        return Family::WrapMerge(merge, std::move(child));
      }
    }
  }
  return MakeRequired<Family>(
    query.Terms(Occur::Must), query.Queries(Occur::Must),
    query.Terms(Occur::Should), query.Queries(Occur::Should),
    query.Uniformity(Occur::Should), query.MinShouldMatch(), query.Segment(),
    ctx, merge, absorbed);
}

}  // namespace irs::detail::builder
namespace irs::detail {

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const BooleanQuery& query, const Ctx& ctx) {
  if constexpr (kProbePlan<Family>) {
    if constexpr (kScoredPlan<Family, Ctx>) {
      const auto& segment = query.Segment();
      return probe::MakeBoolean(
        query, Family::Interrogations(ctx),
        TermScoring{
          .recipe = {.segment = &segment, .fetcher = ctx.scored.fetcher},
          .merge = query.MergeType(),
        },
        ctx.scored);
    } else {
      return probe::MakeBoolean(query, Family::Interrogations(ctx), NoScoring{},
                                ScoredCtx{});
    }
  } else {
    return builder::Make<Family>(query, ctx, MergeOf(query, ctx));
  }
}

}  // namespace irs::detail
