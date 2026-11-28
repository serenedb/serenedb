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
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/probe/constant_scored.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/node.hpp"
#include "iresearch/search/probe/two_phase.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/utils/memory.hpp"

namespace irs::probe {

struct Family {
  using Result = Node::ptr;
  template<typename Slots>
  using TwoPhaseDocs = probe::TwoPhaseDocs<Slots>;
  template<typename Slots>
  using TwoPhaseScored = probe::TwoPhaseScored<Slots>;

  struct DocsContext {
    PlanMemory& memory;
    uint64_t interrogations = 0;
  };

  struct ScoredContext {
    detail::ScoredCtx scored;
    uint64_t interrogations = 0;
  };

  static uint64_t Interrogations(const DocsContext& ctx) noexcept {
    return ctx.interrogations;
  }

  static uint64_t Interrogations(const ScoredContext& ctx) noexcept {
    return ctx.interrogations;
  }

  static DocsContext DocsOf(const ScoredContext& ctx) noexcept {
    return {ctx.scored.memory, ctx.interrogations};
  }

  static const detail::ScoredCtx& Scoring(const ScoredContext& ctx) noexcept {
    return ctx.scored;
  }

  static PlanMemory& MemoryOf(const DocsContext& ctx) noexcept {
    return ctx.memory;
  }

  static PlanMemory& MemoryOf(const ScoredContext& ctx) noexcept {
    return ctx.scored.memory;
  }

  static Result PlanChild(const QueryBuilder& child, const DocsContext& ctx,
                          ScoreMergeType) {
    return child.PlanProbe({.memory = ctx.memory}, ctx.interrogations);
  }

  template<typename Node, typename... Args>
  static Result Docs(const DocsContext& ctx, Args&&... args) {
    return ctx.memory.Make<Impl<Node>>(std::forward<Args>(args)...);
  }

  template<typename Node, typename... Args>
  static Result Constant(const ScoredContext& ctx, score_t value,
                         Args&&... args) {
    auto& memory = MemoryOf(ctx);
    return memory.Make<ConstantScoredImpl<Node>>(memory, value,
                                                 std::forward<Args>(args)...);
  }

  template<typename Node, typename... Args>
  static Result Scored(const ScoredContext& ctx, Args&&... args) {
    return MemoryOf(ctx).Make<Impl<Node>>(std::forward<Args>(args)...);
  }

  static Result MakeAll(const SubReader& segment, const DocsContext& ctx,
                        ScoreMergeType, score_t) {
    return MakeAllDocs(ctx.memory, segment);
  }

  static Result MakeAll(const SubReader& segment, const ScoredContext& ctx,
                        ScoreMergeType, score_t score) {
    return MakeAllScored(MemoryOf(ctx), segment, score);
  }

  static Result MakePosting(const detail::PostingClause& posting,
                            const SubReader& segment, const DocsContext& ctx,
                            ScoreMergeType) {
    return MakePostingDocs(ctx.memory, posting, segment);
  }

  static Result MakePosting(const detail::PostingClause& posting,
                            const SubReader& segment, const ScoredContext& ctx,
                            ScoreMergeType) {
    return MakePostingScored(MemoryOf(ctx), posting, segment,
                             {.memory = &MemoryOf(ctx),
                              .segment = &segment,
                              .fetcher = ctx.scored.fetcher});
  }
};

}  // namespace irs::probe
