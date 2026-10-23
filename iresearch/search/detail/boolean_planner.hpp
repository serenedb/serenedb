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

template<typename Api>
Result<Api> MakeScoredNegation(
  std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  ScoreMergeType merge, score_t absorbed, const Context<Api>& ctx) {
  return BuildNegationWindow<Result<Api>>(
    excludes, exclude_filters, segment,
    [&]<typename Lead, typename Excludes>(auto&& lead,
                                          auto&& excluded) -> Result<Api> {
      return Api::template MakeWindow<Lead, utils::Empty, utils::Empty,
                                      Excludes>(
        ctx, merge, Scored{merge, absorbed}, std::forward<decltype(lead)>(lead),
        std::forward_as_tuple(), std::forward_as_tuple(),
        std::forward<decltype(excluded)>(excluded));
    });
}

template<typename Api>
Result<Api> MakeScoredExclusionWindow(const BooleanQuery& query,
                                      const SubReader& segment,
                                      const Context<Api>& ctx,
                                      ScoreMergeType merge, score_t absorbed) {
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  const std::span excludes = query.Terms(Occur::MustNot);
  const std::span exclude_filters = query.Queries(Occur::MustNot);
  SDB_ASSERT(!excludes.empty() || !exclude_filters.empty());
  std::span<const PostingClause> terms;
  std::span<const QueryBuilder::ptr> filters;
  if (must.empty() && must_filters.empty()) {
    if (should.empty() && should_filters.empty()) {
      return MakeScoredNegation<Api>(excludes, exclude_filters, segment, merge,
                                     absorbed, ctx);
    }
    if (query.MinShouldMatch() != 1) {
      return {};
    }
    terms = should;
    filters = should_filters;
  } else if (must.empty() && must_filters.size() == 1 && should.empty() &&
             should_filters.empty()) {
    filters = must_filters;
  } else {
    return {};
  }
  const auto child = Api::ChildContext(ctx);
  const IndexInput* doc = nullptr;
  std::vector<FillNode::ptr> rest;
  if (!CollectDense(terms, filters, nullptr, doc, rest,
                    [&](const QueryBuilder& filter) {
                      return filter.PlanFill(child, merge);
                    })) {
    return {};
  }
  auto candidates = IncludeCandidates(must, must_filters, segment);
  if (must.empty() && must_filters.empty()) {
    candidates = std::min(candidates,
                          LeadCandidates(terms, filters, segment.docs_count()));
  }
  const auto recipe = Api::Recipe(segment, ctx);
  const auto uniformity = query.Uniformity(Occur::Should);
  return BuildWindowExcludes<Result<Api>>(
    excludes, exclude_filters, nullptr, segment, candidates,
    [&]<typename Excludes>(auto&& negated) -> Result<Api> {
      const auto make = [&]<typename Set>(auto&&... args) -> Result<Api> {
        return Api::template MakeWindow<utils::Empty, utils::Empty,
                                        OrGroup<Set>, Excludes>(
          ctx, merge, Scored{merge, absorbed}, std::forward_as_tuple(),
          std::forward_as_tuple(),
          std::forward_as_tuple(std::forward<decltype(args)>(args)...),
          std::forward<decltype(negated)>(negated));
      };
      return BuildWindowSet<Result<Api>>(terms, nullptr, doc, rest,
                                         TermScoring{.scorer = nullptr,
                                                     .boost = kNoBoost,
                                                     .uniformity = uniformity,
                                                     .recipe = recipe,
                                                     .merge = merge},
                                         make);
    });
}

template<typename Api, typename F>
Result<Api> ResolveBoostArity(size_t size, F&& f) {
  if constexpr (Api::kBoostArity) {
    return ResolveArity<kTailArity, kTailFloor>(size, std::forward<F>(f));
  } else {
    return f.template operator()<0>();
  }
}

template<typename Api>
auto ScoringFor(const SubReader& segment, const Context<Api>& ctx,
                ScoreMergeType merge, Terms uniformity) {
  if constexpr (Api::kScored) {
    return ScoringOf<Api>(segment, ctx, merge, uniformity);
  } else {
    return NoScoring{};
  }
}

template<typename Api>
auto ScoreFor(ScoreMergeType merge, score_t absorbed) {
  if constexpr (Api::kScored) {
    return Scored{merge, absorbed};
  } else {
    return utils::Empty{};
  }
}

template<typename Api>
Result<Api> MakeConjunction(std::span<const PostingClause> must,
                            std::span<const QueryBuilder::ptr> must_filters,
                            std::span<const PostingClause> should,
                            std::span<const QueryBuilder::ptr> should_filters,
                            Terms uniformity, uint32_t min_match,
                            const SubReader& segment, const Context<Api>& ctx,
                            ScoreMergeType merge, score_t absorbed) {
  const bool no_must = must.empty() && must_filters.empty();
  const bool optional = (!should.empty() || !should_filters.empty()) &&
                        (Api::kScored || min_match != 0);
  SDB_ASSERT(!no_must || optional);
  const auto score = ScoreFor<Api>(merge, absorbed);
  const auto conjunction = [&]<typename Make>(Make&& make) -> Result<Api> {
    return BuildSparseConjunction<Api>(
      must, must_filters, segment, ctx,
      ScoringFor<Api>(segment, ctx, merge, Terms::Mixed),
      std::forward<Make>(make));
  };
  if constexpr (Api::kScored) {
    if (optional && min_match == 0) {
      if constexpr (requires { &Api::MakeBoosted; }) {
        if (auto boosted =
              Api::MakeBoosted(must, must_filters, should, should_filters,
                               segment, ctx, merge, absorbed)) {
          return boosted;
        }
      }
      const auto recipe = Api::Recipe(segment, ctx);
      return BuildProbeLeaves<Result<Api>>(
        should, should_filters, nullptr, segment,
        IncludeCandidates(must, must_filters, segment), ProbeOrder::Densest,
        TermScoring{.scorer = nullptr,
                    .boost = kNoBoost,
                    .uniformity = uniformity,
                    .recipe = recipe},
        probe::ScoredClauseOf(segment, Api::ChildContext(ctx), recipe),
        [&]<typename Leaf>(size_t size, auto&& init) -> Result<Api> {
          return ResolveBoostArity<Api>(size, [&]<size_t N> -> Result<Api> {
            using Boost = probe::BoostLeaves<Leaf, N>;
            const auto boost =
              std::forward_as_tuple(size, std::forward<decltype(init)>(init));
            if (no_must) {
              auto all = lead::MakeAllScored(segment, 0);
              if (!all) {
                return {};
              }
              return Api::template MakeSparse<lead::Erased, utils::Empty, Boost,
                                              utils::Empty>(
                ctx, merge, score, std::forward_as_tuple(std::move(all)),
                std::forward_as_tuple(), boost, std::forward_as_tuple());
            }
            return conjunction([&]<typename Head, typename Tail>(
                                 auto&& head, auto&& tail) -> Result<Api> {
              return Api::template MakeSparse<Head, Tail, Boost, utils::Empty>(
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
      return must.empty() ? Api::PlanChild(*must_filters.front(), ctx, merge)
                          : Api::MakePosting(must.front(), segment, ctx, merge);
    }
    if constexpr (!Api::kScored) {
      if (auto windowed =
            MakeWindowConjunction<Api>(must, must_filters, segment, ctx)) {
        return windowed;
      }
      if (!must_filters.empty()) {
        if (auto folded = MakeBitset<Api>(
              {.must = must, .must_filters = must_filters}, segment, ctx)) {
          return folded;
        }
      }
    }
    return conjunction([&]<typename Head, typename Tail>(
                         auto&& head, auto&& tail) -> Result<Api> {
      return Api::template MakeSparse<Head, Tail, utils::Empty, utils::Empty>(
        ctx, merge, score, std::forward<decltype(head)>(head),
        std::forward<decltype(tail)>(tail), std::forward_as_tuple(),
        std::forward_as_tuple());
    });
  }
  const auto candidates = IncludeCandidates(must, must_filters, segment);
  probe::Node::ptr held;
  if constexpr (Api::kScored) {
    held = probe::MakeRequiredScored({}, {}, Terms::Mixed, should,
                                     should_filters, uniformity, min_match,
                                     segment, Api::Recipe(segment, ctx), merge,
                                     candidates, Api::ChildContext(ctx));
  } else {
    held = probe::BuildOptionalProbe(should, should_filters, min_match, segment,
                                     candidates);
  }
  if (!held) {
    return {};
  }
  if constexpr (!Api::kScored) {
    if (must.size() + must_filters.size() > 1) {
      if (auto folded = MakeConjunctionBitset<LeadNode::ptr>(
            must, must_filters, nullptr, segment, nullptr)) {
        return Api::template MakeSparse<lead::Erased, utils::Empty,
                                        probe::Erased, utils::Empty>(
          ctx, merge, score, std::forward_as_tuple(std::move(folded)),
          std::forward_as_tuple(), std::forward_as_tuple(std::move(held)),
          std::forward_as_tuple());
      }
    }
  }
  return conjunction(
    [&]<typename Head, typename Tail>(auto&& head, auto&& tail) -> Result<Api> {
      return Api::template MakeSparse<Head, Tail, probe::Erased, utils::Empty>(
        ctx, merge, score, std::forward<decltype(head)>(head),
        std::forward<decltype(tail)>(tail),
        std::forward_as_tuple(std::move(held)), std::forward_as_tuple());
    });
}

template<typename Api>
Result<Api> MakeRequired(std::span<const PostingClause> must,
                         std::span<const QueryBuilder::ptr> must_filters,
                         std::span<const PostingClause> should,
                         std::span<const QueryBuilder::ptr> should_filters,
                         Terms uniformity, uint32_t min_match,
                         const SubReader& segment, const Context<Api>& ctx,
                         ScoreMergeType merge, score_t absorbed) {
  const bool optional = (!should.empty() || !should_filters.empty()) &&
                        (Api::kScored || min_match != 0);
  if (must.empty() && must_filters.empty() && (!optional || min_match != 0)) {
    if (!optional) {
      return Api::MakeAll(segment, ctx, merge, absorbed);
    }
    const auto scoring = ScoringFor<Api>(segment, ctx, merge, uniformity);
    if (min_match == 1) {
      return MakeDisjunction<Api>(should, should_filters, nullptr, segment,
                                  scoring, ctx, absorbed);
    }
    return MakeThreshold<Api>(should, should_filters, segment, min_match,
                              scoring, ctx, absorbed);
  }
  return MakeConjunction<Api>(must, must_filters, should, should_filters,
                              uniformity, min_match, segment, ctx, merge,
                              absorbed);
}

template<typename Api>
Result<Api> MakeExclusion(const BooleanQuery& query, const Context<Api>& ctx,
                          ScoreMergeType merge, score_t absorbed) {
  const auto& segment = query.Segment();
  const auto own = query.MergeType();
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  const std::span excludes = query.Terms(Occur::MustNot);
  const std::span exclude_filters = query.Queries(Occur::MustNot);
  const auto min_match = query.MinShouldMatch();
  SDB_ASSERT(!excludes.empty() || !exclude_filters.empty());
  const bool no_must = must.empty() && must_filters.empty();
  auto candidates = IncludeCandidates(must, must_filters, segment);
  if (no_must && min_match != 0) {
    candidates = std::min(
      candidates, LeadCandidates(should, should_filters, segment.docs_count()));
  }
  if constexpr (Api::kScored) {
    if (absorbed == 0 && should.empty() && should_filters.empty() &&
        must.size() == 1 && must_filters.empty() &&
        ScoresPerDocTerm(must.front())) {
      const auto& posting = must.front();
      const auto& own_field = *posting.state.reader;
      const auto& doc = *DocOf(own_field);
      const auto recipe = Api::Recipe(segment, ctx);
      return ResolveInput(doc, [&]<typename Input> -> Result<Api> {
        return BuildExcludesOf<Result<Api>, Input>(
          Api::kRoot ? ExcludeUse::PerBlock : ExcludeUse::PerDoc, excludes,
          exclude_filters, nullptr, segment, candidates, candidates,
          [&]<typename Exclude>(auto&& negated) -> Result<Api> {
            if constexpr (Api::kRoot) {
              return Api::template MakeExcludedPosting<Input, Exclude>(
                ctx, std::forward<decltype(negated)>(negated), posting, doc,
                segment, own_field, recipe);
            } else {
              return Api::template MakeSparse<
                PostingLeadScored<Input>, utils::Empty, utils::Empty, Exclude>(
                ctx, merge, Inherited{},
                std::forward_as_tuple(
                  posting.state.cookie, doc, segment, own_field,
                  recipe.Args(posting.stats, posting.boost)),
                std::forward_as_tuple(), std::forward_as_tuple(),
                std::forward<decltype(negated)>(negated));
            }
          });
      });
    }
    auto include =
      lead::MakeRequiredScored(must, must_filters, should, should_filters,
                               query.Uniformity(Occur::Should), min_match,
                               segment, Api::ChildContext(ctx), own, absorbed);
    if (!include) {
      return {};
    }
    if constexpr (Api::kRoot) {
      return MakeSparseExclusionOf<Api>(std::move(include), excludes,
                                        exclude_filters, segment, candidates,
                                        ctx, merge, Scored{merge, 0});
    } else {
      return MakeSparseExclusionOf<Api>(std::move(include), excludes,
                                        exclude_filters, segment, candidates,
                                        ctx, merge, Inherited{});
    }
  } else {
    if (min_match == 0) {
      if (no_must) {
        return Api::MakeNegation(excludes, exclude_filters, segment, candidates,
                                 ctx);
      }
      if (auto folded = MakeBitset<Api>({.must = must,
                                         .must_filters = must_filters,
                                         .must_not = excludes,
                                         .must_not_filters = exclude_filters},
                                        segment, ctx)) {
        return folded;
      }
      if (auto windowed = Api::MakeWindowExclusion(must, must_filters, excludes,
                                                   exclude_filters, segment,
                                                   candidates, ctx)) {
        return windowed;
      }
      return MakeSparseExclusion<Api>(must, must_filters, excludes,
                                      exclude_filters, segment, candidates,
                                      ctx);
    }
    auto include = lead::MakeRequiredDocs(must, must_filters, should,
                                          should_filters, min_match, segment);
    if (!include) {
      return {};
    }
    return MakeSparseExclusionOf<Api>(std::move(include), excludes,
                                      exclude_filters, segment, candidates, ctx,
                                      merge, utils::Empty{});
  }
}

template<typename Api>
Result<Api> MakePruned(const BooleanQuery& query, const Context<Api>& ctx,
                       ScoreMergeType merge) {
  const auto& segment = query.Segment();
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  const std::span excludes = query.Terms(Occur::MustNot);
  const std::span exclude_filters = query.Queries(Occur::MustNot);
  const auto min_match = query.MinShouldMatch();
  const bool no_must = must.empty() && must_filters.empty();
  const bool optional = !should.empty() || !should_filters.empty();
  if (no_must) {
    if (!optional || min_match == 0) {
      return {};
    }
    if ((!excludes.empty() || !exclude_filters.empty()) &&
        should.size() + should_filters.size() == 1) {
      return {};
    }
    return Api::MakePrunedDisjunction(
      should, should_filters, query.Uniformity(Occur::Should), excludes,
      exclude_filters, segment, ctx, merge, min_match);
  }
  if (!optional) {
    if (must.size() == 1 && must_filters.empty() &&
        (!excludes.empty() || !exclude_filters.empty())) {
      return Api::MakePrunedPosting(must.front(), excludes, exclude_filters,
                                    segment, ctx);
    }
    return Api::MakePrunedConjunction(must, must_filters,
                                      query.Uniformity(Occur::Must), excludes,
                                      exclude_filters, segment, ctx, merge);
  }
  if (excludes.empty() && exclude_filters.empty() && min_match == 0 &&
      must.size() == 1 && must_filters.empty() && should_filters.empty() &&
      query.Uniformity(Occur::Must) == Terms::Bounded &&
      query.Uniformity(Occur::Should) == Terms::Bounded) {
    return Api::MakePrunedReqOpt(must.front(), should, segment, ctx, merge);
  }
  return {};
}

template<typename Api>
Result<Api> Make(const BooleanQuery& query, const Context<Api>& ctx,
                 ScoreMergeType merge) {
  score_t absorbed = 0;
  if constexpr (Api::kScored) {
    absorbed = query.Absorbed();
  }
  if constexpr (Api::kPrunes) {
    if (Api::Prunes(ctx, merge, absorbed)) {
      if (auto pruned = MakePruned<Api>(query, ctx, merge)) {
        return pruned;
      }
    }
  }
  if (!query.Terms(Occur::MustNot).empty() ||
      !query.Queries(Occur::MustNot).empty()) {
    if constexpr (Api::kScored) {
      if constexpr (Api::kRoot) {
        if (auto windowed = Api::MakeExclusionWindow(query, ctx, absorbed)) {
          return windowed;
        }
      }
    }
    return Api::MakeExclusion(query, ctx, merge, absorbed);
  }
  if constexpr (Api::kWrapsMerge) {
    if (const auto own = query.MergeType(); own != merge) {
      auto child = Make<Api>(query, ctx, own);
      if (!child) {
        return {};
      }
      return Api::WrapMerge(merge, std::move(child));
    }
  }
  return MakeRequired<Api>(
    query.Terms(Occur::Must), query.Queries(Occur::Must),
    query.Terms(Occur::Should), query.Queries(Occur::Should),
    query.Uniformity(Occur::Should), query.MinShouldMatch(), query.Segment(),
    ctx, merge, absorbed);
}

}  // namespace irs::detail::builder
