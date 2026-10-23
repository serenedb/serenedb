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

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/detail/bitset_of.hpp"
#include "iresearch/search/detail/boolean_builder.hpp"
#include "iresearch/search/detail/boolean_groups.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/fill_posting_scored.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/detail/window_set.hpp"
#include "iresearch/search/fill/boolean_window.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/make.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/fill/window_scored.hpp"
#include "iresearch/search/lead/boolean_sparse.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/make_boolean.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::fill {

struct Api {
  using Result = Node::ptr;
  using Context = utils::Empty;

  static constexpr bool kWindowNodes = false;
  static constexpr bool kLazyGroups = false;
  static constexpr bool kWindowLeadDrains = false;
  static constexpr double kSparseLeadCost = 6.0;
  static constexpr bool kWindowLeadRefills = false;

  template<typename Lead, typename Others, typename Optional, typename Excludes,
           typename Ctx, typename Score, typename... Args>
  static Result MakeWindow(const Ctx&, ScoreMergeType, Score score,
                           Args&&... args) {
    using Window = BooleanWindow<Lead, Others, Optional, Excludes, Score>;
    return memory::make_managed<Impl<Window>>(
      std::piecewise_construct, std::forward<Args>(args)..., score);
  }

  template<typename Lead, typename Probes, typename Optional, typename Excludes,
           typename Ctx, typename Score, typename... Args>
  static Result MakeSparse(const Ctx& ctx, ScoreMergeType merge, Score score,
                           Args&&... args) {
    using Sparse = lead::BooleanSparse<Lead, Probes, Optional, Excludes, Score>;
    if constexpr (std::is_same_v<Score, utils::Empty>) {
      return memory::make_managed<ByWalkDocs<Sparse>>(
        std::piecewise_construct, std::forward<Args>(args)..., score);
    } else {
      return memory::make_managed<ByWalkScored<Sparse>>(
        merge, *ctx.fetcher, std::piecewise_construct,
        std::forward<Args>(args)..., score);
    }
  }

  static Result PlanChild(const QueryBuilder& child, const Context&) {
    return child.PlanFill({}, ScoreMergeType::Noop);
  }

  static Result MakeTerm(const detail::PostingClause& term,
                         const SubReader& segment, const Context&) {
    return FillOf(term, nullptr, segment);
  }

  static Result MakeAll(const SubReader& segment, const Context&) {
    return MakeAllDocs(segment);
  }

  static detail::TableFilter* BitsetTable(const Context&) noexcept {
    return nullptr;
  }

  static doc_id_t BitsetSpan(const Context&, doc_id_t docs_count) noexcept {
    return docs_count;
  }

  static Result MakeExclusion(const BooleanQuery& query, const Context& ctx);

  static Result MakeWindowExclusion(
    std::span<const detail::PostingClause> terms,
    std::span<const QueryBuilder::ptr> filters,
    std::span<const detail::PostingClause> exclude_terms,
    std::span<const QueryBuilder::ptr> exclude_filters,
    const SubReader& segment, uint64_t candidates, const Context& ctx);

  static Result MakeNegation(
    std::span<const detail::PostingClause> exclude_terms,
    std::span<const QueryBuilder::ptr> exclude_filters,
    const SubReader& segment, uint64_t candidates, const Context& ctx);
};

struct ScoredApi {
  using Result = Node::ptr;
  using Context = detail::ScoredCtx;

  static constexpr bool kRoot = false;
  static constexpr bool kPrunes = false;
  static constexpr bool kWrapsMerge = true;
  static constexpr bool kLazyGroups = false;
  static constexpr bool kBoostArity = true;

  static const detail::ScoredCtx& ChildContext(const Context& ctx) noexcept {
    return ctx;
  }

  static detail::ScoreRecipe Recipe(const SubReader& segment,
                                    const Context& ctx) noexcept {
    return {.segment = &segment, .fetcher = ctx.fetcher};
  }

  static score_t Base(score_t absorbed) noexcept { return absorbed; }

  template<typename Lead, typename Others, typename Optional, typename Excludes,
           typename Score, typename... Args>
  static Result MakeWindow(const Context& ctx, ScoreMergeType merge,
                           Score score, Args&&... args) {
    return Api::MakeWindow<Lead, Others, Optional, Excludes>(
      ctx, merge, score, std::forward<Args>(args)...);
  }

  template<typename Lead, typename Probes, typename Optional, typename Excludes,
           typename Score, typename... Args>
  static Result MakeSparse(const Context& ctx, ScoreMergeType merge,
                           Score score, Args&&... args) {
    return Api::MakeSparse<Lead, Probes, Optional, Excludes>(
      ctx, merge, score, std::forward<Args>(args)...);
  }

  static Result PlanChild(const QueryBuilder& child, const Context& ctx,
                          ScoreMergeType merge) {
    return child.PlanFill(ctx, merge);
  }

  static Result MakePosting(const detail::PostingClause& posting,
                            const SubReader& segment, const Context& ctx,
                            ScoreMergeType merge) {
    return MakePostingScored(posting, segment, ctx, merge);
  }

  static Result MakeSinglePosting(const detail::PostingClause& posting,
                                  const SubReader& segment, const Context& ctx,
                                  ScoreMergeType merge) {
    return MakeSinglePostingScored(posting, segment, ctx, merge);
  }

  static Result MakeAll(const SubReader& segment, const Context&,
                        ScoreMergeType merge, score_t absorbed) {
    return MakeAllScored(segment, merge, absorbed);
  }

  static Result MakeExclusion(const BooleanQuery& query, const Context& ctx,
                              ScoreMergeType merge, score_t absorbed);

  static Result WrapMerge(ScoreMergeType merge, Result child) {
    return irs::ResolveMergeType(merge, [&]<ScoreMergeType Merge> -> Result {
      return memory::make_managed<ByWindowScored<Erased, Merge>>(
        Erased{std::move(child)});
    });
  }
};

template<typename Term>
Node::ptr MakeWindowDisjunctionScored(
  std::span<const Term> terms, const TermReader* field, const Scorer* scorer,
  score_t boost, const IndexInput* doc, std::vector<Node::ptr>& rest,
  detail::Terms uniformity, const detail::ScoreRecipe& recipe,
  const detail::ScoredCtx& ctx, ScoreMergeType merge, score_t absorbed = 0) {
  return detail::builder::MakeWindowDisjunction<ScoredApi>(
    terms, field, doc, rest,
    detail::TermScoring{.scorer = scorer,
                        .boost = boost,
                        .uniformity = uniformity,
                        .recipe = recipe,
                        .merge = merge},
    ctx, absorbed);
}

}  // namespace irs::fill
