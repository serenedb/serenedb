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

#include "iresearch/index/index_meta.hpp"
#include "iresearch/search/common/collect_scored.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/scored/detail/walk.hpp"
#include "iresearch/search/scored/root.hpp"

namespace irs {
namespace scored {

template<template<typename...> class Shape, typename... Parts, typename... Args>
Root::ptr MakeShape(const Context& ctx, Args&&... args) {
  if (ctx.table != nullptr) {
    return memory::make_managed<Shape<Parts..., search::DeadRuns*>>(
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
using FilteredWalk = detail::Walk<Node, search::DeadRuns*>;
template<typename Node>
using PlainConstantWalk = detail::ConstantWalk<Node, utils::Empty>;
template<typename Node>
using FilteredConstantWalk = detail::ConstantWalk<Node, search::DeadRuns*>;

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
Root::ptr Make(const WildcardNGramQuery& query, const Context& ctx);
Root::ptr Make(const ByNestedQuery& query, const Context& ctx);
Root::ptr Make(const RangeVectorQuery& query, const Context& ctx);
Root::ptr Make(const BooleanQuery& query, const Context& ctx);
template<typename Parser, typename Acceptor>
Root::ptr Make(const GeoQuery<Parser, Acceptor>& query, const Context& ctx);

inline Root::ptr Make(const EmptyQueryBuilder&, const Context&) {
  return MakeEmpty();
}

Root::ptr Make(const HnswQuery& query, const Context& ctx);
Root::ptr Make(const KnnVectorQuery& query, const Context& ctx);

Root::ptr MakePosting(const PostingClause& posting, const SubReader& segment,
                      const Context& ctx);

Root::ptr MakeSinglePosting(const PostingClause& posting,
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

}  // namespace scored
}  // namespace irs
