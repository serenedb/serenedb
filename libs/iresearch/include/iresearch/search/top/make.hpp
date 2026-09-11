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

#include <algorithm>
#include <cstdint>
#include <span>
#include <vector>

#include "basics/debugging.h"
#include "basics/empty.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/common/collect_scored.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/top/detail/walk.hpp"
#include "iresearch/search/top/posting_pruned_disj.hpp"
#include "iresearch/search/top/pruned_disjunction.hpp"
#include "iresearch/search/top/root.hpp"
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

inline constexpr uint64_t kPrunedVisitsPerHit = 400;
inline constexpr uint64_t kPrunedVisitedShare = 100;

inline uint64_t PrunedCandidates(uint64_t docs, const Context& ctx) noexcept {
  if (ctx.k == 0) {
    return docs;
  }
  return std::min(
    docs, uint64_t{ctx.k} * kPrunedVisitsPerHit + docs / kPrunedVisitedShare);
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

Root::ptr MakeRoot(const QueryBuilder& query, const Context& ctx);

Root::ptr MakeEmpty();

Root::ptr Make(const TermQuery& query, const Context& ctx);
Root::ptr Make(const MultiTermQuery& query, const Context& ctx);
Root::ptr Make(const FixedPhraseQuery& query, const Context& ctx);
Root::ptr Make(const VariadicPhraseQuery& query, const Context& ctx);
Root::ptr Make(const NGramSimilarityQuery& query, const Context& ctx);
Root::ptr Make(const AllQuery& query, const Context& ctx);
Root::ptr Make(const WildcardNGramQuery& query, const Context& ctx);
Root::ptr Make(const ByNestedQuery& query, const Context& ctx);
Root::ptr Make(const HnswQuery& query, const Context& ctx);
Root::ptr Make(const KnnVectorQuery& query, const Context& ctx);
Root::ptr Make(const RangeVectorQuery& query, const Context& ctx);
Root::ptr Make(const BooleanQuery& query, const Context& ctx);
template<typename Parser, typename Acceptor>
Root::ptr Make(const GeoQuery<Parser, Acceptor>& query, const Context& ctx);

inline Root::ptr Make(const EmptyQueryBuilder&, const Context&) {
  return MakeEmpty();
}

Root::ptr MakePosting(const search::PostingClause& posting, const SubReader& segment,
                      const Context& ctx);
Root::ptr MakeSinglePosting(const search::PostingClause& posting,
                            const SubReader& segment, const Context& ctx);
Root::ptr MakeAll(const SubReader& segment, const Context& ctx,
                  const search::StatsRecord& record, score_t boost);
Root::ptr MakeAll(const SubReader& segment, const Context& ctx, score_t score);

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

Root::ptr MakeWildcardNGram(const WildcardNGramQuery& query,
                            const Context& ctx);

Root::ptr MakeMasked(const QueryBuilder& query, const Context& ctx,
                     const DocumentMask& mask);

Root::ptr MakePrunedPosting(const search::PostingClause& posting,
                            const SubReader& segment, const Context& ctx);
Root::ptr MakePrunedPosting(const search::PostingClause& posting,
                            std::span<const search::PostingClause> excludes,
                            std::span<const QueryBuilder::ptr> exclude_filters,
                            const SubReader& segment, const Context& ctx);

Root::ptr MakeFixedPhrasePruned(const FixedPhraseQuery& query,
                                const Context& ctx);
Root::ptr MakeFixedPhraseIntervalsPruned(const FixedPhraseQuery& query,
                                         const Context& ctx);

Root::ptr MakePrunedConjunction(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, search::Terms uniformity,
  std::span<const search::PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const Context& ctx, ScoreMergeType merge);

Root::ptr MakeNestedPrunedConjunction(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const Context& ctx, ScoreMergeType merge);

template<typename Term>
Root::ptr MakePrunedDisjunction(
  std::span<const Term> terms, std::span<const QueryBuilder::ptr> filters,
  search::Terms uniformity, const TermReader* field, const Scorer* scorer,
  score_t boost, std::span<const search::PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const Context& ctx, ScoreMergeType merge, uint32_t min_match = 1) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  SDB_ASSERT(min_match != 0);
  if (merge != ScoreMergeType::Sum || !filters.empty() ||
      uniformity != search::Terms::Bounded || min_match != 1) {
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
    const auto init = [&](Leaf& leaf, size_t i) {
      const auto posting = search::ClauseOf(terms[i], field, scorer, boost);
      const auto& own = *posting.state.reader;
      SDB_ASSERT(search::DocOf(own) == doc);
      leaf.Prepare(posting.state.cookie, *doc, search::LayoutOf(own), segment,
                   own,
                   search::ScoreArgs{.scorer = posting.stats.scorer,
                             .stats = posting.stats.stats,
                             .fetcher = &ctx.fetcher,
                             .boost = posting.boost});
      return posting.state.cookie.docs_count;
    };
    const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
    const auto make = [&]() -> Root::ptr {
      if (excludes.empty() && exclude_filters.empty()) {
        return MakeShape<PrunedDisjunction, Leaf, utils::Empty>(
          ctx, terms.size(), docs_count, init, std::forward_as_tuple());
      }
      const auto candidates =
        std::min<uint64_t>(search::SumDocs(terms), segment.docs_count());
      return search::BuildBlockExcludes<Root::ptr>(
        excludes, exclude_filters, nullptr, segment, candidates, candidates,
        [&]<typename Exclude>(auto&& negated) -> Root::ptr {
          return MakeShape<PrunedDisjunction, Leaf,
                           fill::ProbedAndNot<Exclude>>(
            ctx, terms.size(), docs_count, init,
            std::forward_as_tuple(std::piecewise_construct,
                                  std::forward<decltype(negated)>(negated)));
        });
    };
    return make();
  });
}

}  // namespace irs::top
