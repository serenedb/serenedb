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
#include "iresearch/search/detail/sparse_cursor.hpp"
#include "iresearch/search/hits/boolean_window.hpp"
#include "iresearch/search/hits/make.hpp"
#include "iresearch/search/hits/posting.hpp"
#include "iresearch/search/hits/walk.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::hits {

Root::ptr MakeBoostedPosting(std::span<const irs::detail::PostingClause> must,
                             std::span<const QueryBuilder::ptr> must_filters,
                             std::span<const irs::detail::PostingClause> should,
                             std::span<const QueryBuilder::ptr> should_filters,
                             const SubReader& segment, const Context& ctx,
                             ScoreMergeType merge, score_t absorbed);

struct Api {
  using Result = Root::ptr;
  using Context = hits::Context;

  static constexpr bool kRoot = true;
  static constexpr bool kWrapsMerge = false;
  static constexpr bool kPrunes = false;
  static constexpr bool kLazyGroups = false;
  static constexpr bool kBoostArity = false;

  template<typename Lead, typename Others, typename Optional, typename Excludes,
           typename LeadArgs, typename OthersArgs, typename OptionalArgs,
           typename ExcludesArgs>
  static Result MakeWindow(const Context& ctx, ScoreMergeType,
                           irs::detail::Scored score, LeadArgs&& lead,
                           OthersArgs&&, OptionalArgs&& optional,
                           ExcludesArgs&& excludes) {
    static_assert(std::is_same_v<Others, utils::Empty>);
    return MakeShape<BooleanWindow, Lead, Optional, Excludes>(
      ctx, std::piecewise_construct, std::forward<LeadArgs>(lead),
      std::forward<OptionalArgs>(optional),
      std::forward<ExcludesArgs>(excludes), score.absorbed);
  }

  template<typename Lead, typename Probes, typename Optional, typename Excludes,
           typename... Args>
  static Result MakeSparse(const Context& ctx, ScoreMergeType,
                           irs::detail::Scored score, Args&&... args) {
    return MakeShape<
      Walk, irs::detail::SparseCursor<Lead, Probes, Optional, Excludes>>(
      ctx, ctx.fetcher, score, std::forward<Args>(args)...);
  }

  template<typename Input, typename Exclude, typename ExcludeArgs>
  static Result MakeExcludedPosting(const Context& ctx, ExcludeArgs&& negated,
                                    const irs::detail::PostingClause& posting,
                                    const IndexInput& doc,
                                    const SubReader& segment,
                                    const TermReader& own,
                                    const irs::detail::ScoreRecipe& recipe) {
    auto root = memory::make_managed<Posting<Input, utils::Empty, Exclude>>(
      std::piecewise_construct, std::forward_as_tuple(),
      std::forward<ExcludeArgs>(negated));
    root->Prepare(posting.state.cookie, doc, segment, own,
                  recipe.Args(posting.stats, posting.boost),
                  irs::detail::LayoutOf(own), irs::detail::BoundsOf(own));
    return root;
  }

  static score_t Base(score_t absorbed) noexcept { return absorbed; }

  static irs::detail::ScoredCtx ChildContext(const Context& ctx) noexcept {
    return ScoredOf(ctx);
  }

  static irs::detail::ScoreRecipe Recipe(const SubReader& segment,
                                         const Context& ctx) noexcept {
    return {.segment = &segment, .fetcher = &ctx.fetcher};
  }

  static Result PlanChild(const QueryBuilder& child, const Context& ctx,
                          ScoreMergeType) {
    return child.PlanScored(ctx);
  }

  static Result MakePosting(const irs::detail::PostingClause& posting,
                            const SubReader& segment, const Context& ctx,
                            ScoreMergeType) {
    return hits::MakePosting(posting, segment, ctx);
  }

  static Result MakeSinglePosting(const irs::detail::PostingClause& posting,
                                  const SubReader& segment, const Context& ctx,
                                  ScoreMergeType) {
    return hits::MakeSinglePosting(posting, segment, ctx);
  }

  static Result MakeAll(const SubReader& segment, const Context& ctx,
                        ScoreMergeType, score_t absorbed) {
    return hits::MakeAll(segment, ctx, absorbed);
  }

  static Result MakeBoosted(std::span<const irs::detail::PostingClause> must,
                            std::span<const QueryBuilder::ptr> must_filters,
                            std::span<const irs::detail::PostingClause> should,
                            std::span<const QueryBuilder::ptr> should_filters,
                            const SubReader& segment, const Context& ctx,
                            ScoreMergeType merge, score_t absorbed) {
    return MakeBoostedPosting(must, must_filters, should, should_filters,
                              segment, ctx, merge, absorbed);
  }

  static Result MakeExclusionWindow(const BooleanQuery& query,
                                    const Context& ctx, score_t absorbed);

  static Result MakeExclusion(const BooleanQuery& query, const Context& ctx,
                              ScoreMergeType merge, score_t absorbed);
};

template<typename Term>
Root::ptr MakeWindowDisjunction(std::span<const Term> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                irs::detail::Terms uniformity,
                                const TermReader* field, const Scorer* scorer,
                                score_t boost, const SubReader& segment,
                                const Context& ctx, ScoreMergeType merge,
                                score_t absorbed) {
  return irs::detail::builder::MakeDisjunction<Api>(
    terms, filters, field, segment,
    irs::detail::TermScoring{.scorer = scorer,
                             .boost = boost,
                             .uniformity = uniformity,
                             .recipe = Api::Recipe(segment, ctx),
                             .merge = merge},
    ctx, absorbed);
}

}  // namespace irs::hits
