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
#include <duckdb/common/fault_points.hpp>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/search/detail/bitset_of.hpp"
#include "iresearch/search/detail/exclusion_of.hpp"
#include "iresearch/search/detail/phrase_match.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/window_set.hpp"
#include "iresearch/search/detail/with_mask.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/filters/filter.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/top/posting_pruned_disj.hpp"
#include "iresearch/search/top/pruned_clause.hpp"
#include "iresearch/search/top/pruned_disjunction.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/search/top/union_member.hpp"
#include "iresearch/search/top/walk.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::top {

template<template<typename...> typename Shape, typename... Parts,
         typename... Args>
Root::ptr MakeShape(const Context& ctx, Args&&... args) {
  return irs::detail::WithTable(ctx.table, [&](auto table) -> Root::ptr {
    return memory::make_managed<Shape<Parts..., decltype(table)>>(
      table, std::forward<Args>(args)...);
  });
}

inline uint64_t PrunedCandidates(uint64_t docs, const Context& ctx) noexcept {
  if (docs <= ctx.k || ctx.k == 0) {
    return docs;
  }
  const auto k = static_cast<double>(ctx.k);
  return std::min(docs, static_cast<uint64_t>(
                          k * (1 + std::log(static_cast<double>(docs) / k))));
}

Root::ptr MakeRoot(const QueryBuilder& query, const Context& ctx);

Root::ptr MakeEmpty();

template<irs::detail::PhraseMatch M, typename Query>
struct PhraseMaker {
  static Root::ptr Pruned(const Query& query, const Context& ctx);
  static UnionMember::ptr Member(const Query& query, const Context& ctx);
  static PrunedClause::ptr Clause(const Query& query, const Context& ctx);
};

UnionMember::ptr MakeUnionMember(const QueryBuilder& query, const Context& ctx);

PrunedClause::ptr MakeConjunctionClause(const QueryBuilder& query,
                                        const Context& ctx);

Root::ptr MakeTokenPhrasePruned(const TokenPhraseQuery& query,
                                const Context& ctx);

Root::ptr MakePrunedPosting(const irs::detail::PostingClause& posting,
                            const SubReader& segment, const Context& ctx);
Root::ptr MakePrunedPosting(
  const irs::detail::PostingClause& posting,
  std::span<const irs::detail::PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const Context& ctx);

Root::ptr MakePrunedReqOpt(const irs::detail::PostingClause& required,
                           std::span<const irs::detail::PostingClause> optional,
                           const SubReader& segment, const Context& ctx,
                           ScoreMergeType merge);

template<typename P>
struct PrunedConjunctionMaker {
  static Root::ptr Make(std::span<const irs::detail::PostingClause> terms,
                        std::span<const QueryBuilder::ptr> filters,
                        irs::detail::Terms uniformity,
                        std::span<const irs::detail::PostingClause> excludes,
                        std::span<const QueryBuilder::ptr> exclude_filters,
                        const SubReader& segment, const Context& ctx,
                        ScoreMergeType merge);
};

template<typename P>
struct NestedPrunedConjunctionMaker {
  static Root::ptr Make(std::span<const irs::detail::PostingClause> terms,
                        std::span<const QueryBuilder::ptr> filters,
                        std::span<const irs::detail::PostingClause> excludes,
                        std::span<const QueryBuilder::ptr> exclude_filters,
                        const SubReader& segment, const Context& ctx,
                        ScoreMergeType merge);
};

template<bool kExcludes, typename Term, typename P = NoPins>
struct PrunedDisjunctionMaker {
  static Root::ptr Make(std::span<const Term> terms,
                        std::span<const QueryBuilder::ptr> filters,
                        irs::detail::Terms uniformity, const TermReader* field,
                        const Scorer* scorer, score_t boost,
                        std::span<const irs::detail::PostingClause> excludes,
                        std::span<const QueryBuilder::ptr> exclude_filters,
                        const SubReader& segment, const Context& ctx,
                        ScoreMergeType merge, uint32_t min_match = 1);
};

}  // namespace irs::top
