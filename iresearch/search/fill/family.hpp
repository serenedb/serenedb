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
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/fill/boolean_window.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/make.hpp"
#include "iresearch/search/fill/node.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/fill/window_scored.hpp"
#include "iresearch/search/filters/filter.hpp"
#include "iresearch/search/lead/boolean_sparse.hpp"
#include "iresearch/search/lead/two_phase.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/memory.hpp"

namespace irs::fill {

struct Family {
  using Result = Node::ptr;
  template<typename Slots>
  using TwoPhaseDocs = lead::TwoPhaseDocs<Slots>;
  template<typename Slots>
  using TwoPhaseScored = lead::TwoPhaseScored<Slots>;
  using DocsContext = utils::Empty;

  struct ScoredContext {
    detail::ScoredCtx scored;
    ScoreMergeType merge;
  };

  static uint64_t Interrogations(const DocsContext&) noexcept { return 0; }

  static uint64_t Interrogations(const ScoredContext&) noexcept { return 0; }

  static DocsContext DocsOf(const ScoredContext&) noexcept { return {}; }

  static const detail::ScoredCtx& Scoring(const ScoredContext& ctx) noexcept {
    return ctx.scored;
  }

  template<typename Node, typename... Args>
  static Result Docs(const DocsContext&, Args&&... args) {
    return memory::make_managed<ByWalkDocs<Node>>(std::forward<Args>(args)...);
  }

  template<typename Node, typename... Args>
  static Result Constant(const ScoredContext& ctx, score_t value,
                         Args&&... args) {
    return memory::make_managed<WalkConstantScored<Node>>(
      ctx.merge, *ctx.scored.fetcher, value, std::forward<Args>(args)...);
  }

  template<typename Node, typename... Args>
  static Result Scored(const ScoredContext& ctx, Args&&... args) {
    return memory::make_managed<ByWalkScored<Node>>(
      ctx.merge, *ctx.scored.fetcher, std::forward<Args>(args)...);
  }

  static constexpr bool kRoot = false;
  static constexpr bool kWrapsMerge = true;
  static constexpr bool kLazyGroups = false;
  static constexpr bool kBoostArity = true;
  static constexpr bool kSparseNegation = false;
  static constexpr bool kWindowNodes = false;
  static constexpr bool kWindowLeadDrains = false;
  static constexpr double kSparseLeadCost = 6.0;
  static constexpr bool kWindowLeadRefills = false;

  static score_t Base(score_t absorbed) noexcept { return absorbed; }

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
        merge, *Scoring(ctx).fetcher, std::piecewise_construct,
        std::forward<Args>(args)..., score);
    }
  }

  static Result PlanChild(const QueryBuilder& child, const DocsContext&,
                          ScoreMergeType) {
    return child.PlanFill({}, ScoreMergeType::Noop);
  }

  static Result PlanChild(const QueryBuilder& child, const ScoredContext& ctx,
                          ScoreMergeType merge) {
    return child.PlanFill(ctx.scored, merge);
  }

  static Result MakePosting(const detail::PostingClause& posting,
                            const SubReader& segment, const DocsContext&,
                            ScoreMergeType) {
    return posting.state.cookie.docs_count == 1
             ? MakeSinglePostingDocs(posting)
             : MakePostingDocs(posting, segment);
  }

  static Result MakePosting(const detail::PostingClause& posting,
                            const SubReader& segment, const ScoredContext& ctx,
                            ScoreMergeType merge) {
    return posting.state.cookie.docs_count == 1
             ? MakeSinglePostingScored(posting, segment, ctx.scored, merge)
             : MakePostingScored(posting, segment, ctx.scored, merge);
  }

  static Result MakeAll(const SubReader& segment, const DocsContext&,
                        ScoreMergeType, score_t) {
    return MakeAllDocs(segment);
  }

  static Result MakeAll(const SubReader& segment, const ScoredContext&,
                        ScoreMergeType merge, score_t absorbed) {
    return MakeAllScored(segment, merge, absorbed);
  }

  static Result MakeLiveDocs(const SubReader& segment);

  static detail::TableFilter* BitsetTable(const DocsContext&) noexcept {
    return nullptr;
  }

  static doc_id_t BitsetSpan(const DocsContext&, doc_id_t docs_count) noexcept {
    return docs_count;
  }

  static Result WrapMerge(ScoreMergeType merge, Result child) {
    return irs::ResolveMergeType(merge, [&]<ScoreMergeType Merge> -> Result {
      return memory::make_managed<ByWindowScored<Erased, Merge>>(
        Erased{std::move(child)});
    });
  }
};

}  // namespace irs::fill
