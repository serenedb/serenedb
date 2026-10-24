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
#include <utility>

#include "iresearch/search/count/boolean_sparse.hpp"
#include "iresearch/search/count/boolean_window.hpp"
#include "iresearch/search/count/make.hpp"
#include "iresearch/search/count/plan.hpp"
#include "iresearch/search/count/walk.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/filters/filter.hpp"
#include "iresearch/search/lead/two_phase.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::count {

struct Family {
  using Result = Root::ptr;
  using DocsContext = Context;

  template<typename Slots>
  using TwoPhaseDocs = lead::TwoPhaseDocs<Slots>;

  static uint64_t Interrogations(const Context&) noexcept { return 0; }

  template<typename Node, typename... Args>
  static Result Docs(const Context& ctx, Args&&... args) {
    return MakeShape<Walk, Node>(ctx, std::forward<Args>(args)...);
  }

  static constexpr bool kWindowNodes = true;
  static constexpr bool kWindowLeadDrains = false;
  static constexpr double kSparseLeadCost = 1.0;
  static constexpr bool kWindowLeadRefills = true;

  template<typename Lead, typename Others, typename Optional, typename Excludes,
           typename... Args>
  static Result MakeWindow(const Context& ctx, ScoreMergeType, utils::Empty,
                           Args&&... args) {
    return MakeShape<BooleanWindow, Lead, Others, Optional, Excludes>(
      ctx, std::piecewise_construct, std::forward<Args>(args)...);
  }

  template<typename Lead, typename Probes, typename Optional, typename Excludes,
           typename... Args>
  static Result MakeSparse(const Context& ctx, ScoreMergeType, utils::Empty,
                           Args&&... args) {
    return MakeShape<BooleanSparse, Lead, Probes, Optional, Excludes>(
      ctx, std::piecewise_construct, std::forward<Args>(args)...);
  }

  static Result PlanChild(const QueryBuilder& child, const Context& ctx,
                          ScoreMergeType) {
    return child.PlanCount(ctx);
  }

  static Result MakePosting(const detail::PostingClause& posting,
                            const SubReader& segment, const Context& ctx,
                            ScoreMergeType);

  static Result MakeAll(const SubReader& segment, const Context& ctx,
                        ScoreMergeType, score_t score);

  static detail::TableFilter* BitsetTable(const Context& ctx) noexcept {
    return ctx.table;
  }

  static doc_id_t BitsetSpan(const Context& ctx, doc_id_t docs_count) noexcept {
    return ctx.span != 0 ? std::min(ctx.span, docs_count) : docs_count;
  }

  static Result Make(const MultiTermQuery& query, const Context& ctx);

  static Result Make(const BooleanQuery& query, const Context& ctx);

  static Result Make(const HnswQuery& query, const Context& ctx);

  static Result Make(const ExactVectorQuery& query, const Context& ctx);

  static Result Make(const EmptyQueryBuilder&, const Context&) {
    return MakeConstant(0);
  }

  static Result MakeNegation(
    std::span<const detail::PostingClause> exclude_terms,
    std::span<const QueryBuilder::ptr> exclude_filters,
    const SubReader& segment, uint64_t candidates, const Context& ctx);
};

}  // namespace irs::count
