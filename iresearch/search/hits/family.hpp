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
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/detail/sparse_cursor.hpp"
#include "iresearch/search/filters/filter.hpp"
#include "iresearch/search/hits/boolean_window.hpp"
#include "iresearch/search/hits/make.hpp"
#include "iresearch/search/hits/posting.hpp"
#include "iresearch/search/hits/walk.hpp"
#include "iresearch/search/lead/two_phase.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::hits {

struct Family {
  using Result = Root::ptr;
  using ScoredContext = Context;

  template<typename Slots>
  using TwoPhaseDocs = lead::TwoPhaseDocs<Slots>;
  template<typename Slots>
  using TwoPhaseScored = lead::TwoPhaseScored<Slots>;

  static uint64_t Interrogations(const Context&) noexcept { return 0; }

  static irs::detail::ScoredCtx Scoring(const Context& ctx) noexcept {
    return ScoredOf(ctx);
  }

  template<typename Node, typename... Args>
  static Result Constant(const Context& ctx, score_t value, Args&&... args) {
    return MakeShape<ConstantWalk, Node>(ctx, value,
                                         std::forward<Args>(args)...);
  }

  template<typename Node, typename... Args>
  static Result Scored(const Context& ctx, Args&&... args) {
    return MakeShape<Walk, Node>(ctx, ctx.fetcher, std::forward<Args>(args)...);
  }

  static constexpr bool kRoot = true;
  static constexpr bool kWrapsMerge = false;
  static constexpr bool kLazyGroups = false;
  static constexpr bool kBoostArity = false;

  static score_t Base(score_t absorbed) noexcept { return absorbed; }

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
  static Result MakeExcludedPosting(const Context&, ExcludeArgs&& negated,
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

  static Result PlanChild(const QueryBuilder& child, const Context& ctx,
                          ScoreMergeType) {
    return child.PlanScored(ctx);
  }

  static Result MakePosting(const irs::detail::PostingClause& posting,
                            const SubReader& segment, const Context& ctx,
                            ScoreMergeType);

  static Result MakeAll(const SubReader& segment, const Context& ctx,
                        ScoreMergeType, score_t score);

  static Result MakeBoosted(std::span<const irs::detail::PostingClause> must,
                            std::span<const QueryBuilder::ptr> must_filters,
                            std::span<const irs::detail::PostingClause> should,
                            std::span<const QueryBuilder::ptr> should_filters,
                            const SubReader& segment, const Context& ctx,
                            ScoreMergeType merge, score_t absorbed);

  static Result Make(const HnswQuery& query, const Context& ctx);

  static Result Make(const KnnVectorQuery& query, const Context& ctx);

  static Result Make(const ExactVectorQuery& query, const Context& ctx);

  static Result Make(const EmptyQueryBuilder&, const Context&) {
    return MakeEmpty();
  }
};

}  // namespace irs::hits
