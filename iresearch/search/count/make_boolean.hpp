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

#include "iresearch/search/count/boolean_sparse.hpp"
#include "iresearch/search/count/boolean_window.hpp"
#include "iresearch/search/count/plan.hpp"
#include "iresearch/search/detail/bitset_of.hpp"
#include "iresearch/search/detail/boolean_groups.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::count {

struct Api {
  using Result = Root::ptr;
  using Context = count::Context;

  static constexpr bool kWindowNodes = true;
  static constexpr bool kWindowLeadDrains = false;
  static constexpr double kSparseLeadCost = 1.0;
  static constexpr bool kWindowLeadRefills = true;

  template<typename Lead, typename Others, typename Optional, typename Excludes,
           typename... Args>
  static Result MakeWindow(const Context& ctx, Args&&... args) {
    return MakeShape<BooleanWindow, Lead, Others, Optional, Excludes>(
      ctx, std::piecewise_construct, std::forward<Args>(args)...);
  }

  template<typename Lead, typename Probes, typename Excludes, typename... Args>
  static Result MakeSparse(const Context& ctx, Args&&... args) {
    return MakeShape<BooleanSparse, Lead, Probes, Excludes>(
      ctx, std::piecewise_construct, std::forward<Args>(args)...);
  }

  static Result PlanChild(const QueryBuilder& child, const Context& ctx) {
    return child.PlanCount(ctx);
  }

  static Result MakeTerm(const detail::PostingClause& term,
                         const SubReader& segment, const Context& ctx) {
    return count::MakeTerm(term, segment, ctx);
  }

  static Result MakeAll(const SubReader& segment, const Context& ctx) {
    return count::MakeAll(segment, ctx);
  }

  static detail::TableFilter* BitsetTable(const Context& ctx) noexcept {
    return ctx.table;
  }

  static Result MakeNegation(
    std::span<const detail::PostingClause> exclude_terms,
    std::span<const QueryBuilder::ptr> exclude_filters,
    const SubReader& segment, uint64_t candidates, const Context& ctx);
};

Root::ptr MakeSubtractConjunction(std::span<const detail::PostingClause> terms,
                                  std::span<const QueryBuilder::ptr> filters,
                                  const SubReader& segment, const Context& ctx);
Root::ptr MakeSubtractDisjunction(const detail::PostingClause& first,
                                  const detail::PostingClause& second,
                                  const SubReader& segment, const Context& ctx);

template<typename Term>
doc_id_t RarestOf(std::span<const Term> terms) noexcept {
  SDB_ASSERT(terms.size() == 2);
  return std::min(detail::CookieOf(terms.front()).docs_count,
                  detail::CookieOf(terms.back()).docs_count);
}

template<typename Term>
bool SubtractsPair(std::span<const Term> terms) noexcept {
  if (terms.size() != 2) {
    return false;
  }
  const auto densest = std::max(detail::CookieOf(terms.front()).docs_count,
                                detail::CookieOf(terms.back()).docs_count);
  return detail::SubtractsDisjunction(RarestOf(terms), densest);
}

template<typename Term>
Root::ptr MakeBitsetDisjunctionOfTerms(std::span<const Term> terms,
                                       const TermReader* field,
                                       const IndexInput& doc,
                                       doc_id_t docs_count,
                                       const Context& ctx) {
  return detail::MakeBitsetOf<Root::ptr>(terms, field, doc, docs_count,
                                         ctx.table);
}

template<typename Term>
Root::ptr MakeWindowDisjunctionOfTerms(std::span<const Term> terms,
                                       const TermReader* field,
                                       const IndexInput& doc,
                                       const Context& ctx) {
  SDB_ASSERT(terms.size() > 1);
  return detail::ResolveInput(doc, [&]<typename Input> -> Root::ptr {
    using Leaf = detail::PostingFill<Input>;
    using Optional = detail::OrGroup<fill::SetLeaves<Leaf>>;
    const auto init = [&](Leaf& leaf, size_t i) {
      const auto& own = detail::FieldOf(terms[i], field);
      const auto& meta = detail::CookieOf(terms[i]);
      SDB_ASSERT(meta.docs_count != 0);
      leaf.Prepare(meta, doc, meta.docs_count != 1 && detail::BoundsOf(own),
                   meta.docs_count != 1 && detail::FreqOf(own));
    };
    return MakeShape<BooleanWindow, utils::Empty, utils::Empty, Optional,
                     utils::Empty>(
      ctx, std::piecewise_construct, std::forward_as_tuple(),
      std::forward_as_tuple(), std::forward_as_tuple(terms.size(), init),
      std::forward_as_tuple());
  });
}

template<typename Term>
Root::ptr MakeDisjunctionOfTerms(std::span<const Term> terms,
                                 const TermReader* field, const IndexInput& doc,
                                 doc_id_t docs_count, const Context& ctx) {
  SDB_ASSERT(terms.size() > 1);
  if (auto folded =
        MakeBitsetDisjunctionOfTerms(terms, field, doc, docs_count, ctx)) {
    return folded;
  }
  return MakeWindowDisjunctionOfTerms(terms, field, doc, ctx);
}

}  // namespace irs::count
