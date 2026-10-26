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
#include "iresearch/search/filters/filter.hpp"
#include "iresearch/search/lead/boolean_sparse.hpp"
#include "iresearch/search/lead/boolean_window.hpp"
#include "iresearch/search/lead/constant_scored.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/node.hpp"
#include "iresearch/search/lead/two_phase.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/memory.hpp"

namespace irs::lead {

struct Family {
  using Result = Node::ptr;
  template<typename Slots>
  using TwoPhaseDocs = lead::TwoPhaseDocs<Slots>;
  template<typename Slots>
  using TwoPhaseScored = lead::TwoPhaseScored<Slots>;
  using DocsContext = utils::Empty;
  using ScoredContext = detail::ScoredCtx;

  static uint64_t Interrogations(const DocsContext&) noexcept { return 0; }

  static uint64_t Interrogations(const ScoredContext&) noexcept { return 0; }

  static DocsContext DocsOf(const ScoredContext&) noexcept { return {}; }

  static const detail::ScoredCtx& Scoring(const ScoredContext& ctx) noexcept {
    return ctx;
  }

  template<typename Node, typename... Args>
  static Result Docs(const DocsContext&, Args&&... args) {
    return memory::make_managed<Impl<Node>>(std::forward<Args>(args)...);
  }

  template<typename Node, typename... Args>
  static Result Constant(const ScoredContext&, score_t value, Args&&... args) {
    return memory::make_managed<ConstantScoredImpl<Node>>(
      value, std::forward<Args>(args)...);
  }

  template<typename Node, typename... Args>
  static Result Scored(const ScoredContext&, Args&&... args) {
    return memory::make_managed<Impl<Node>>(std::forward<Args>(args)...);
  }

  static constexpr bool kRoot = false;
  static constexpr bool kWrapsMerge = false;
  static constexpr bool kLazyGroups = true;
  static constexpr bool kBoostArity = true;
  static constexpr bool kSparseNegation = true;
  static constexpr bool kWindowNodes = false;
  static constexpr bool kWindowLeadDrains = true;
  static constexpr double kSparseLeadCost = 1.0;
  static constexpr bool kWindowLeadRefills = true;

  static score_t Base(score_t) noexcept { return 0; }

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
  static Result MakeSparse(const Ctx&, ScoreMergeType, Score score,
                           Args&&... args) {
    using Sparse = BooleanSparse<Lead, Probes, Optional, Excludes, Score>;
    return memory::make_managed<Impl<Sparse>>(
      std::piecewise_construct, std::forward<Args>(args)..., score);
  }

  static Result PlanChild(const QueryBuilder& child, const DocsContext&,
                          ScoreMergeType) {
    return child.PlanLead({});
  }

  static Result PlanChild(const QueryBuilder& child, const ScoredContext& ctx,
                          ScoreMergeType) {
    return child.PlanLead(ctx);
  }

  static Result MakePosting(const detail::PostingClause& posting,
                            const SubReader& segment, const DocsContext&,
                            ScoreMergeType) {
    return detail::LeadOf(posting, nullptr, segment);
  }

  static Result MakePosting(const detail::PostingClause& posting,
                            const SubReader& segment, const ScoredContext& ctx,
                            ScoreMergeType) {
    return MakePostingScored(posting, segment,
                             {.segment = &segment, .fetcher = ctx.fetcher});
  }

  static Result MakeAll(const SubReader& segment, const DocsContext&,
                        ScoreMergeType, score_t) {
    return MakeAllDocs(segment);
  }

  static Result MakeAll(const SubReader& segment, const ScoredContext&,
                        ScoreMergeType, score_t absorbed) {
    return MakeAllScored(segment, absorbed);
  }

  static Result MakeLiveDocs(const SubReader& segment);

  static detail::TableFilter* BitsetTable(const DocsContext&) noexcept {
    return nullptr;
  }

  static doc_id_t BitsetSpan(const DocsContext&, doc_id_t docs_count) noexcept {
    return docs_count;
  }

  static Result Make(const HnswQuery& query, const ScoredContext& ctx);

  static Result Make(const KnnVectorQuery& query, const ScoredContext& ctx);
};

}  // namespace irs::lead
