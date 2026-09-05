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
#include <vector>

#include "basics/debugging.h"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/search/common/collect_scored.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/top/detail/walk.hpp"
#include "iresearch/search/top/max_score_disjunction.hpp"
#include "iresearch/search/top/posting_pruned_disj.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/search/top/window_disjunction.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::top {

template<template<typename...> class Shape, typename... Parts, typename... Args>
Root::ptr MakeShape(const Context& ctx, Args&&... args) {
  if (ctx.table != nullptr) {
    return memory::make_managed<Shape<Parts..., search::TableFilter*>>(
      ctx.table, std::forward<Args>(args)...);
  }
  return memory::make_managed<Shape<Parts..., utils::Empty>>(
    utils::Empty{}, std::forward<Args>(args)...);
}

template<typename Make>
Root::ptr MakePrepared(const Context& ctx, Make&& make) {
  if (ctx.table != nullptr) {
    return make(ctx.table);
  }
  return make(utils::Empty{});
}

template<typename Node>
using PlainWalk = detail::Walk<Node, utils::Empty>;
template<typename Node>
using FilteredWalk = detail::Walk<Node, search::TableFilter*>;
template<typename Node>
using PlainConstantWalk = detail::ConstantWalk<Node, utils::Empty>;
template<typename Node>
using FilteredConstantWalk = detail::ConstantWalk<Node, search::TableFilter*>;

using search::PostingClause;
using search::ScoreArgs;
using search::ScoreRecipe;

Root::ptr MakeRoot(const QueryBuilder& query, const Context& ctx);

Root::ptr MakeEmpty();

Root::ptr Make(const TermQuery& query, const Context& ctx);
Root::ptr Make(const MultiTermQuery& query, const Context& ctx);
Root::ptr Make(const FixedPhraseQuery& query, const Context& ctx);
Root::ptr Make(const VariadicPhraseQuery& query, const Context& ctx);
Root::ptr Make(const NGramSimilarityQuery& query, const Context& ctx);
Root::ptr Make(const AllQuery& query, const Context& ctx);
Root::ptr Make(const WildcardNgramQuery& query, const Context& ctx);
Root::ptr Make(const ByNestedQuery& query, const Context& ctx);
Root::ptr Make(const KnnVectorQuery& query, const Context& ctx);
Root::ptr Make(const RangeVectorQuery& query, const Context& ctx);
Root::ptr Make(const BooleanQuery& query, const Context& ctx);
template<typename Parser, typename Acceptor>
Root::ptr Make(const GeoQuery<Parser, Acceptor>& query, const Context& ctx);

inline Root::ptr Make(const EmptyQueryBuilder&, const Context&) {
  return MakeEmpty();
}

Root::ptr MakePosting(const PostingClause& posting, const SubReader& segment,
                      const Context& ctx);
Root::ptr MakeSinglePosting(const PostingClause& posting,
                            const SubReader& segment, const Context& ctx);
Root::ptr MakeAll(const SubReader& segment, const Context& ctx,
                  const search::StatsRecord& record, score_t boost);
Root::ptr MakeAll(const SubReader& segment, const Context& ctx, score_t score);

Root::ptr MakeSparseConjunction(const BooleanQuery& query,
                                const SubReader& segment, const Context& ctx,
                                ScoreMergeType merge, score_t absorbed);

Root::ptr MakeSparseExclusion(const BooleanQuery& query,
                              const SubReader& segment, const Context& ctx,
                              ScoreMergeType merge, score_t absorbed);

Root::ptr MakeWindowExclusion(const BooleanQuery& query,
                              const SubReader& segment, const Context& ctx,
                              ScoreMergeType merge, score_t absorbed);

Root::ptr MakeBitsThreshold(std::span<const PostingClause> terms,
                            std::span<const QueryBuilder::ptr> filters,
                            search::Terms uniformity, const SubReader& segment,
                            const Context& ctx, ScoreMergeType merge,
                            uint32_t min_match, score_t absorbed);

Root::ptr MakeCountThreshold(std::span<const PostingClause> terms,
                             std::span<const QueryBuilder::ptr> filters,
                             search::Terms uniformity, const SubReader& segment,
                             const Context& ctx, ScoreMergeType merge,
                             uint32_t min_match, score_t absorbed);

Root::ptr MakeFixedPhrase(const FixedPhraseQuery& query, const Context& ctx);
Root::ptr MakeFixedPhraseIntervals(const FixedPhraseQuery& query,
                                   const Context& ctx);
Root::ptr MakeFixedPhraseSlop(const FixedPhraseQuery& query,
                              const Context& ctx);

Root::ptr MakeVariadicPhrase(const VariadicPhraseQuery& query,
                             const Context& ctx);
Root::ptr MakeVariadicPhraseIntervals(const VariadicPhraseQuery& query,
                                      const Context& ctx);
Root::ptr MakeVariadicPhraseSlop(const VariadicPhraseQuery& query,
                                 const Context& ctx);

Root::ptr MakeNGram(const NGramSimilarityQuery& query, const Context& ctx);
Root::ptr MakeNGramAll(const NGramSimilarityQuery& query, const Context& ctx);

Root::ptr MakeWildcardNgram(const WildcardNgramQuery& query,
                            const Context& ctx);

Root::ptr MakeMasked(const QueryBuilder& query, const Context& ctx,
                     const DocumentMask& mask);

Root::ptr MakePrunedPosting(const PostingClause& posting,
                            const SubReader& segment, const Context& ctx);

Root::ptr MakeFixedPhrasePruned(const FixedPhraseQuery& query,
                                const Context& ctx);
Root::ptr MakeFixedPhraseIntervalsPruned(const FixedPhraseQuery& query,
                                         const Context& ctx);

Root::ptr MakeWandConjunction(std::span<const PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              search::Terms uniformity,
                              const SubReader& segment, const Context& ctx,
                              ScoreMergeType merge);

template<typename Term>
Root::ptr MakeWindowDisjunction(std::span<const Term> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                search::Terms uniformity,
                                const TermReader* field, const Scorer* scorer,
                                score_t boost, const SubReader& segment,
                                const Context& ctx, ScoreMergeType merge,
                                score_t absorbed) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  const IndexInput* doc = nullptr;
  std::vector<search::FillNode::ptr> rest;
  if (!search::CollectDenseScored(terms, filters, field, doc, rest,
                                  [&](const QueryBuilder& child) {
                                    return child.PlanFill(ScoredOf(ctx), merge);
                                  })) {
    return {};
  }
  const auto make = [&]<typename Set>(auto&&... args) -> Root::ptr {
    const auto leaves =
      std::forward_as_tuple(std::forward<decltype(args)>(args)...);
    return MakeShape<WindowDisjunction, Set, utils::Empty>(
      ctx, std::piecewise_construct, leaves, std::forward_as_tuple(), merge,
      absorbed);
  };
  const search::ScoreRecipe recipe{.segment = &segment,
                                   .fetcher = &ctx.fetcher};
  return search::BuildScoredWindow<Root::ptr>(
    terms, field, scorer, boost, doc, rest, uniformity, recipe, merge, make);
}

template<typename Term>
Root::ptr MakeMaxScoreDisjunction(std::span<const Term> terms,
                                  std::span<const QueryBuilder::ptr> filters,
                                  search::Terms uniformity,
                                  const TermReader* field, const Scorer* scorer,
                                  score_t boost, const SubReader& segment,
                                  const Context& ctx, ScoreMergeType merge) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  if (merge != ScoreMergeType::Sum || !filters.empty() ||
      uniformity != search::Terms::Bounded) {
    return {};
  }
  for (size_t i = 0; i != terms.size(); ++i) {
    if (!search::ScoresOf(terms[i], scorer)) {
      return {};
    }
  }
  SDB_IF_FAILURE("irs::PruningIterator") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  const auto* const doc = search::DocOf(search::FieldOf(terms.front(), field));
  return search::ResolveInput(*doc, [&]<typename Input> -> Root::ptr {
    using Leaf = search::PostingPrunedDisj<Input>;
    return MakeShape<MaxScoreDisjunction, Leaf>(
      ctx, terms.size(), [&](Leaf& leaf, size_t i) {
        const auto posting = search::ClauseOf(terms[i], field, scorer, boost);
        const auto& own = *posting.state.reader;
        SDB_ASSERT(search::DocOf(own) == doc);
        leaf.Prepare(posting.state.cookie, *doc, search::LayoutOf(own), segment,
                     own,
                     ScoreArgs{.scorer = posting.stats.scorer,
                               .stats = posting.stats.stats,
                               .fetcher = &ctx.fetcher,
                               .boost = posting.boost});
        return posting.state.cookie.docs_count;
      });
  });
}

}  // namespace irs::top
