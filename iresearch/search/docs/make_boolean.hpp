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
#include <tuple>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/bitset_of.hpp"
#include "iresearch/search/detail/boolean_groups.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/docs/boolean_sparse.hpp"
#include "iresearch/search/docs/boolean_window.hpp"
#include "iresearch/search/docs/plan.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::docs {

inline doc_id_t FoldSpan(const Context& ctx, doc_id_t docs_count) noexcept {
  return ctx.span != 0 ? std::min(ctx.span, docs_count) : docs_count;
}

struct Api {
  using Result = Root::ptr;
  using Context = docs::Context;

  static constexpr bool kWindowNodes = true;
  static constexpr bool kLazyGroups = false;
  static constexpr bool kWindowLeadDrains = true;
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
           typename LeadArgs, typename ProbesArgs, typename OptionalArgs,
           typename ExcludesArgs>
  static Result MakeSparse(const Context& ctx, ScoreMergeType, utils::Empty,
                           LeadArgs&& lead, ProbesArgs&& probes, OptionalArgs&&,
                           ExcludesArgs&& excludes) {
    static_assert(std::is_same_v<Optional, utils::Empty>);
    return MakeShape<BooleanSparse, Lead, Probes, Excludes>(
      ctx, std::piecewise_construct, std::forward<LeadArgs>(lead),
      std::forward<ProbesArgs>(probes), std::forward<ExcludesArgs>(excludes));
  }

  static Result PlanChild(const QueryBuilder& child, const Context& ctx) {
    return child.PlanDocs(ctx);
  }

  static Result MakeTerm(const detail::PostingClause& term,
                         const SubReader& segment, const Context& ctx) {
    return MakePosting(term, segment, ctx);
  }

  static Result MakeAll(const SubReader& segment, const Context& ctx) {
    return docs::MakeAll(static_cast<doc_id_t>(segment.docs_count()), ctx);
  }

  static detail::TableFilter* BitsetTable(const Context&) noexcept {
    return nullptr;
  }

  static doc_id_t BitsetSpan(const Context& ctx, doc_id_t docs_count) noexcept {
    return FoldSpan(ctx, docs_count);
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

}  // namespace irs::docs
