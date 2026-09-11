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

#include "iresearch/utils/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/boolean_builder.hpp"
#include "iresearch/search/detail/boolean_groups.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/collect_scored.hpp"
#include "iresearch/search/detail/conjunction_scored.hpp"
#include "iresearch/search/detail/exclusion_of.hpp"
#include "iresearch/search/detail/fill_posting_scored.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/probe_leaves.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/fill/all_docs.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/leaves.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"
#include "iresearch/search/scorers/score_policy.hpp"

namespace irs::detail::builder {

template<typename Api>
Result<Api> MakeScoredNegation(
  std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  ScoreMergeType merge, score_t absorbed, const Context<Api>& ctx) {
  SDB_ASSERT(!excludes.empty() || !exclude_filters.empty());
  std::vector<FillNode::ptr> nodes;
  if (!CollectFills(excludes, exclude_filters, nullptr, segment, nodes)) {
    return {};
  }
  using Excludes = fill::FilledAndNot<fill::SetLeaves<fill::Erased>>;
  return Api::template MakeWindow<fill::AllDocs, utils::Empty, Excludes>(
    ctx, merge, absorbed, std::forward_as_tuple(segment),
    std::forward_as_tuple(),
    std::forward_as_tuple(
      std::piecewise_construct,
      std::forward_as_tuple(nodes.size(), [&](fill::Erased& leaf, size_t i) {
        leaf = fill::Erased{std::move(nodes[i])};
      })));
}

template<typename Api, template<typename> class Group, typename Set,
         typename... Args>
Result<Api> MakeScoredThresholdWindow(const Context<Api>& ctx,
                                      ScoreMergeType merge, uint32_t min_match,
                                      score_t absorbed, Args&&... args) {
  return Api::template MakeWindow<utils::Empty, Group<Set>, utils::Empty>(
    ctx, merge, absorbed, std::forward_as_tuple(),
    std::forward_as_tuple(std::piecewise_construct,
                          std::forward_as_tuple(std::forward<Args>(args)...),
                          min_match, Api::Base(absorbed)),
    std::forward_as_tuple());
}

template<typename Api>
Result<Api> MakeScoredThreshold(std::span<const PostingClause> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                Terms uniformity, const SubReader& segment,
                                const Context<Api>& ctx, ScoreMergeType merge,
                                uint32_t min_match, score_t absorbed) {
  SDB_ASSERT(min_match > 1);
  SDB_ASSERT(terms.size() + filters.size() >= min_match);
  SDB_ASSERT(min_match != terms.size() + filters.size());
  const auto child = Api::ChildContext(ctx);
  const IndexInput* doc = nullptr;
  std::vector<FillNode::ptr> rest;
  if (!CollectDenseScored(terms, filters, nullptr, doc, rest,
                          [&](const QueryBuilder& filter) {
                            return filter.PlanFill(child, merge);
                          })) {
    return {};
  }
  const auto recipe = Api::Recipe(segment, ctx);
  if (min_match > kBitplaneMaxMatch && rest.empty() &&
      uniformity != Terms::Mixed) {
    auto counted = ResolveCountScored<Result<Api>>(
      *doc, uniformity >= Terms::Scored, merge,
      [&]<typename Leaf, typename Plain> -> Result<Api> {
        return BuildScoredTerms<Result<Api>, Leaf, Plain>(
          terms, nullptr, nullptr, kNoBoost, doc, recipe,
          [&]<typename Set>(auto&&... args) -> Result<Api> {
            return MakeScoredThresholdWindow<Api, TallyGroup, Set>(
              ctx, merge, min_match, absorbed,
              std::forward<decltype(args)>(args)...);
          });
      });
    if (counted) {
      return counted;
    }
  }
  return BuildScoredWindow<Result<Api>>(
    terms, nullptr, nullptr, kNoBoost, doc, rest, uniformity, recipe, merge,
    [&]<typename Set>(auto&&... args) -> Result<Api> {
      return MakeScoredThresholdWindow<Api, ThresholdGroup, Set>(
        ctx, merge, min_match, absorbed, std::forward<decltype(args)>(args)...);
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
  if (!CollectDenseScored(terms, filters, nullptr, doc, rest,
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
        return Api::template MakeWindow<utils::Empty, OrGroup<Set>, Excludes>(
          ctx, merge, absorbed, std::forward_as_tuple(),
          std::forward_as_tuple(std::forward<decltype(args)>(args)...),
          std::forward<decltype(negated)>(negated));
      };
      return BuildScoredWindow<Result<Api>>(terms, nullptr, nullptr, kNoBoost,
                                            doc, rest, uniformity, recipe,
                                            merge, make);
    });
}

template<typename Api, typename Term>
Result<Api> MakeScoredDisjunction(std::span<const Term> terms,
                                  std::span<const QueryBuilder::ptr> filters,
                                  Terms uniformity, const TermReader* field,
                                  const Scorer* scorer, score_t boost,
                                  const SubReader& segment,
                                  const Context<Api>& ctx, ScoreMergeType merge,
                                  score_t absorbed) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  const auto child = Api::ChildContext(ctx);
  const IndexInput* doc = nullptr;
  std::vector<FillNode::ptr> rest;
  if (!CollectDenseScored(terms, filters, field, doc, rest,
                          [&](const QueryBuilder& filter) {
                            return filter.PlanFill(child, merge);
                          })) {
    return {};
  }
  const auto make = [&]<typename Set>(auto&&... args) -> Result<Api> {
    return Api::template MakeWindow<utils::Empty, OrGroup<Set>, utils::Empty>(
      ctx, merge, absorbed, std::forward_as_tuple(),
      std::forward_as_tuple(std::forward<decltype(args)>(args)...),
      std::forward_as_tuple());
  };
  const auto recipe = Api::Recipe(segment, ctx);
  return BuildScoredWindow<Result<Api>>(terms, field, scorer, boost, doc, rest,
                                        uniformity, recipe, merge, make);
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
Result<Api> MakeScoredConjunction(const BooleanQuery& query,
                                  const SubReader& segment,
                                  const Context<Api>& ctx, ScoreMergeType merge,
                                  score_t absorbed) {
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  const auto should_uniformity = query.Uniformity(Occur::Should);
  const auto min_should_match = query.MinShouldMatch();
  const bool no_must = must.empty() && must_filters.empty();
  const bool optional = !should.empty() || !should_filters.empty();
  SDB_ASSERT(!no_must || optional);
  const auto recipe = Api::Recipe(segment, ctx);
  const auto child = Api::ChildContext(ctx);
  const auto clause = probe::ScoredClauseOf(segment, child, recipe);
  const auto candidates = IncludeCandidates(must, must_filters, segment);
  const Scored score{merge, absorbed};
  const auto conjunction = [&]<typename Make>(Make&& make) -> Result<Api> {
    return BuildScoredConjunction<Result<Api>>(
      must, must_filters, nullptr, nullptr, kNoBoost, segment, recipe, clause,
      [&](const QueryBuilder& filter) -> lead::Node::ptr {
        return filter.PlanLead(child);
      },
      std::forward<Make>(make));
  };
  if (optional && min_should_match == 0) {
    if (auto boosted = Api::MakeBoosted(query, segment, ctx, merge, absorbed)) {
      return boosted;
    }
    return BuildOptionalLeaves<Result<Api>>(
      should, should_filters, should_uniformity, nullptr, nullptr, kNoBoost,
      segment, recipe, candidates, clause,
      [&]<typename Leaf>(size_t size, auto&& init) -> Result<Api> {
        return ResolveBoostArity<Api>(size, [&]<size_t N> -> Result<Api> {
          using Boost = probe::BoostLeaves<Leaf, N>;
          const auto boost =
            std::forward_as_tuple(size, std::forward<decltype(init)>(init));
          if (no_must) {
            auto all = lead::MakeAllScored(
              segment, AllDocsScore(segment, ScoreArgs{.scorer = child.scorer,
                                                       .fetcher = child.fetcher,
                                                       .boost = kNoBoost}));
            if (!all) {
              return {};
            }
            return Api::template MakeSparse<lead::Erased, utils::Empty, Boost,
                                            utils::Empty>(
              ctx, score, std::forward_as_tuple(std::move(all)),
              std::forward_as_tuple(), boost, std::forward_as_tuple());
          }
          return conjunction([&]<typename Head, typename Tail>(
                               auto&& head, auto&& tail) -> Result<Api> {
            return Api::template MakeSparse<Head, Tail, Boost, utils::Empty>(
              ctx, score, std::forward<decltype(head)>(head),
              std::forward<decltype(tail)>(tail), boost,
              std::forward_as_tuple());
          });
        });
      });
  }
  SDB_ASSERT(!no_must);
  probe::Node::ptr held;
  if (optional) {
    held = probe::MakeRequiredScored(
      {}, {}, Terms::Mixed, should, should_filters, should_uniformity,
      min_should_match, segment, recipe, merge, candidates, child);
    if (!held) {
      return {};
    }
  }
  if (must.size() + must_filters.size() == 1 && !held && absorbed == 0) {
    Result<Api> only;
    query.VisitHead(
      Occur::Must,
      [&](const PostingClause& posting) {
        only = posting.state.cookie.docs_count == 1
                 ? Api::MakeSinglePosting(posting, segment, ctx)
                 : Api::MakePosting(posting, segment, ctx);
        return true;
      },
      [&](const QueryBuilder& filter) {
        only = Api::PlanChild(filter, ctx);
        return true;
      });
    return only;
  }
  return conjunction(
    [&]<typename Head, typename Tail>(auto&& head, auto&& tail) -> Result<Api> {
      if (!held) {
        return Api::template MakeSparse<Head, Tail, utils::Empty, utils::Empty>(
          ctx, score, std::forward<decltype(head)>(head),
          std::forward<decltype(tail)>(tail), std::forward_as_tuple(),
          std::forward_as_tuple());
      }
      return Api::template MakeSparse<Head, Tail, probe::Erased, utils::Empty>(
        ctx, score, std::forward<decltype(head)>(head),
        std::forward<decltype(tail)>(tail),
        std::forward_as_tuple(std::move(held)), std::forward_as_tuple());
    });
}

template<typename Api>
Result<Api> MakeScoredExclusion(const BooleanQuery& query,
                                const SubReader& segment,
                                const Context<Api>& ctx, ScoreMergeType merge,
                                score_t absorbed) {
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  const std::span excludes = query.Terms(Occur::MustNot);
  const std::span exclude_filters = query.Queries(Occur::MustNot);
  SDB_ASSERT(!excludes.empty() || !exclude_filters.empty());
  auto candidates = IncludeCandidates(must, must_filters, segment);
  if (must.empty() && must_filters.empty()) {
    candidates = std::min(
      candidates, LeadCandidates(should, should_filters, segment.docs_count()));
  }
  if (absorbed == 0 && should.empty() && should_filters.empty() &&
      must.size() == 1 && must_filters.empty() &&
      ScoresPerDocTerm(must.front())) {
    const auto& posting = must.front();
    const auto& own = *posting.state.reader;
    const auto& doc = *DocOf(own);
    const auto recipe = Api::Recipe(segment, ctx);
    return ResolveInput(doc, [&]<typename Input> -> Result<Api> {
      return BuildBlockExcludesOf<Result<Api>, Input>(
        excludes, exclude_filters, nullptr, segment, candidates, candidates,
        [&]<typename Exclude>(auto&& negated) -> Result<Api> {
          return Api::template MakeExcludedPosting<Input, Exclude>(
            ctx, std::forward<decltype(negated)>(negated), posting, doc,
            segment, own, recipe);
        });
    });
  }
  auto include = lead::MakeRequiredScored(
    must, must_filters, should, should_filters, query.Uniformity(Occur::Should),
    query.MinShouldMatch(), segment, Api::ChildContext(ctx), merge, absorbed);
  if (!include) {
    return {};
  }
  return BuildExcludeSide<Result<Api>>(
    excludes, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& negated) -> Result<Api> {
      return Api::template MakeSparse<lead::Erased, utils::Empty, utils::Empty,
                                      Exclude>(
        ctx, Scored{merge, 0}, std::forward_as_tuple(std::move(include)),
        std::forward_as_tuple(), std::forward_as_tuple(),
        std::forward<decltype(negated)>(negated));
    });
}

template<typename Api>
Result<Api> MakeScored(const BooleanQuery& query, const Context<Api>& ctx) {
  const auto& segment = query.Segment();
  const auto merge = query.MergeType();
  const auto absorbed = query.Absorbed();
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  const std::span excludes = query.Terms(Occur::MustNot);
  const std::span exclude_filters = query.Queries(Occur::MustNot);
  const auto min_match = query.MinShouldMatch();
  const bool no_must = must.empty() && must_filters.empty();
  const bool optional = !should.empty() || !should_filters.empty();
  const bool only_scores = optional && min_match == 0;
  if (!excludes.empty() || !exclude_filters.empty()) {
    if constexpr (Api::kPrunes) {
      if (Api::Prunes(ctx, merge, absorbed)) {
        if (no_must) {
          if (optional && min_match != 0 &&
              should.size() + should_filters.size() > 1) {
            if (auto pruned = Api::MakePrunedDisjunction(
                  should, should_filters, query.Uniformity(Occur::Should),
                  excludes, exclude_filters, segment, ctx, merge, min_match)) {
              return pruned;
            }
          }
        } else if (!optional) {
          if (must.size() == 1 && must_filters.empty()) {
            if (auto pruned = Api::MakePrunedPosting(
                  must.front(), excludes, exclude_filters, segment, ctx)) {
              return pruned;
            }
          } else if (auto pruned = Api::MakePrunedConjunction(
                       must, must_filters, query.Uniformity(Occur::Must),
                       excludes, exclude_filters, segment, ctx, merge)) {
            return pruned;
          }
        }
      }
    }
    if (auto windowed = MakeScoredExclusionWindow<Api>(query, segment, ctx,
                                                       merge, absorbed)) {
      return windowed;
    }
    return MakeScoredExclusion<Api>(query, segment, ctx, merge, absorbed);
  }
  if (no_must && !only_scores) {
    if (!optional) {
      return Api::MakeAll(segment, ctx, absorbed);
    }
    const auto uniformity = query.Uniformity(Occur::Should);
    if constexpr (Api::kPrunes) {
      if (Api::Prunes(ctx, merge, absorbed)) {
        if (auto pruned =
              Api::MakePrunedDisjunction(should, should_filters, uniformity, {},
                                         {}, segment, ctx, merge, min_match)) {
          return pruned;
        }
      }
    }
    if (min_match == 1) {
      return MakeScoredDisjunction<Api>(should, should_filters, uniformity,
                                        nullptr, nullptr, kNoBoost, segment,
                                        ctx, merge, absorbed);
    }
    return MakeScoredThreshold<Api>(should, should_filters, uniformity, segment,
                                    ctx, merge, min_match, absorbed);
  }
  if constexpr (Api::kPrunes) {
    if (!optional && Api::Prunes(ctx, merge, absorbed)) {
      if (auto pruned = Api::MakePrunedConjunction(
            must, must_filters, query.Uniformity(Occur::Must), {}, {}, segment,
            ctx, merge)) {
        return pruned;
      }
    }
  }
  return MakeScoredConjunction<Api>(query, segment, ctx, merge, absorbed);
}

}  // namespace irs::detail::builder
