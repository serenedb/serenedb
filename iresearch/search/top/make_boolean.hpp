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

#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/scored_builder.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/search/top/boolean_sparse.hpp"
#include "iresearch/search/top/boolean_window.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/posting.hpp"
#include "iresearch/utils/debugging.h"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/pg/sql_exception_macro.h"

namespace irs::top {

struct Api {
  using Result = Root::ptr;
  using Context = top::Context;

  static constexpr bool kPrunes = true;
  static constexpr bool kBoostArity = true;

  template<typename Lead, typename Optional, typename Excludes,
           typename LeadArgs, typename OptionalArgs, typename ExcludesArgs>
  static Result MakeWindow(const Context& ctx, ScoreMergeType merge,
                           score_t absorbed, LeadArgs&& lead,
                           OptionalArgs&& optional, ExcludesArgs&& excludes) {
    return MakeShape<BooleanWindow, Lead, Optional, Excludes>(
      ctx, std::piecewise_construct, std::forward<LeadArgs>(lead),
      std::forward<OptionalArgs>(optional),
      std::forward<ExcludesArgs>(excludes), merge, absorbed);
  }

  template<typename Lead, typename Probes, typename Optional, typename Excludes,
           typename... Args>
  static Result MakeSparse(const Context& ctx, irs::detail::Scored score,
                           Args&&... args) {
    return MakeShape<BooleanSparse, Lead, Probes, Optional, Excludes>(
      ctx, std::piecewise_construct, ctx.fetcher, score,
      std::forward<Args>(args)...);
  }

  template<typename Input, typename Exclude, typename ExcludeArgs>
  static Result MakeExcludedPosting(const Context& ctx, ExcludeArgs&& negated,
                                    const irs::detail::PostingClause& posting,
                                    const IndexInput& doc,
                                    const SubReader& segment,
                                    const TermReader& own,
                                    const irs::detail::ScoreRecipe& recipe) {
    SDB_IF_FAILURE("irs::PruningIterator") {
      if (irs::detail::BoundsOf(own)) {
        THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
      }
    }
    return MakePrepared(ctx, [&](auto table) -> Result {
      auto root =
        memory::make_managed<Posting<Input, Exclude, decltype(table)>>(
          table, std::piecewise_construct, std::forward<ExcludeArgs>(negated));
      root->Prepare(posting.state.cookie, doc, segment, own,
                    recipe.Args(posting.stats, posting.boost),
                    irs::detail::LayoutOf(own), irs::detail::BoundsOf(own));
      return root;
    });
  }

  static score_t Base(score_t) noexcept { return 0; }

  static irs::detail::ScoredCtx ChildContext(const Context& ctx) noexcept {
    return ScoredOf(ctx);
  }

  static irs::detail::ScoreRecipe Recipe(const SubReader& segment,
                                         const Context& ctx) noexcept {
    return {.segment = &segment, .fetcher = &ctx.fetcher};
  }

  static Result PlanChild(const QueryBuilder& child, const Context& ctx) {
    return child.PlanTop({.scorer = ctx.scorer,
                          .fetcher = ctx.fetcher,
                          .table = ctx.table,
                          .prune = ctx.prune,
                          .k = ctx.k});
  }

  static Result MakePosting(const irs::detail::PostingClause& posting,
                            const SubReader& segment, const Context& ctx) {
    return top::MakePosting(posting, segment, ctx);
  }

  static Result MakeSinglePosting(const irs::detail::PostingClause& posting,
                                  const SubReader& segment,
                                  const Context& ctx) {
    return top::MakeSinglePosting(posting, segment, ctx);
  }

  static Result MakeAll(const SubReader& segment, const Context& ctx,
                        score_t absorbed) {
    return top::MakeAll(segment, ctx, absorbed);
  }

  static Result MakeBoosted(const BooleanQuery&, const SubReader&,
                            const Context&, ScoreMergeType, score_t) {
    return {};
  }

  static bool Prunes(const Context& ctx, ScoreMergeType merge,
                     score_t absorbed) noexcept {
    return ctx.prune && merge == ScoreMergeType::Sum && absorbed == 0;
  }

  static Result MakePrunedDisjunction(
    std::span<const irs::detail::PostingClause> should,
    std::span<const QueryBuilder::ptr> should_filters,
    irs::detail::Terms uniformity,
    std::span<const irs::detail::PostingClause> excludes,
    std::span<const QueryBuilder::ptr> exclude_filters,
    const SubReader& segment, const Context& ctx, ScoreMergeType merge,
    uint32_t min_match) {
    return top::MakePrunedDisjunction(
      should, should_filters, uniformity, nullptr, nullptr, kNoBoost, excludes,
      exclude_filters, segment, ctx, merge, min_match);
  }

  static Result MakePrunedConjunction(
    std::span<const irs::detail::PostingClause> must,
    std::span<const QueryBuilder::ptr> must_filters,
    irs::detail::Terms uniformity,
    std::span<const irs::detail::PostingClause> excludes,
    std::span<const QueryBuilder::ptr> exclude_filters,
    const SubReader& segment, const Context& ctx, ScoreMergeType merge) {
    return top::MakePrunedConjunction(must, must_filters, uniformity, excludes,
                                      exclude_filters, segment, ctx, merge);
  }

  static Result MakePrunedPosting(
    const irs::detail::PostingClause& posting,
    std::span<const irs::detail::PostingClause> excludes,
    std::span<const QueryBuilder::ptr> exclude_filters,
    const SubReader& segment, const Context& ctx) {
    return top::MakePrunedPosting(posting, excludes, exclude_filters, segment,
                                  ctx);
  }
};

template<typename Term>
Root::ptr MakeWindowDisjunction(std::span<const Term> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                irs::detail::Terms uniformity,
                                const TermReader* field, const Scorer* scorer,
                                score_t boost, const SubReader& segment,
                                const Context& ctx, ScoreMergeType merge,
                                score_t absorbed) {
  return irs::detail::builder::MakeScoredDisjunction<Api, Term>(
    terms, filters, uniformity, field, scorer, boost, segment, ctx, merge,
    absorbed);
}

}  // namespace irs::top
