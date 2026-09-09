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

#include "basics/empty.hpp"
#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/common/boolean_groups.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/docs/boolean_window.hpp"
#include "iresearch/search/docs/plan.hpp"
#include "iresearch/search/fill/set_leaves.hpp"

namespace irs::docs {

Root::ptr MakeConjunction(std::span<const search::PostingClause> terms,
                          std::span<const QueryBuilder::ptr> filters,
                          const SubReader& segment, const Context& ctx);
Root::ptr MakeDisjunction(std::span<const search::PostingClause> terms,
                          std::span<const QueryBuilder::ptr> filters,
                          const SubReader& segment, const Context& ctx);
Root::ptr MakeThreshold(std::span<const search::PostingClause> terms,
                        std::span<const QueryBuilder::ptr> filters,
                        const SubReader& segment, uint32_t min_match,
                        const Context& ctx);
Root::ptr MakeRequired(const BooleanQuery& query, const Context& ctx);
Root::ptr MakeExclusion(const BooleanQuery& query, const Context& ctx);

Root::ptr MakeBitset(const search::BooleanGroups& groups,
                     const SubReader& segment, const Context& ctx);

Root::ptr MakeWindowDisjunction(std::span<const search::PostingClause> terms,
                                const IndexInput* doc,
                                std::vector<FillNode::ptr>& rest,
                                const Context& ctx);
Root::ptr MakeWindowConjunction(std::span<const search::PostingClause> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                const SubReader& segment, const Context& ctx);
Root::ptr MakeWindowExclusion(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Context& ctx);

Root::ptr MakeSparseConjunction(std::span<const search::PostingClause> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                const SubReader& segment, const Context& ctx);
Root::ptr MakeSparseConjunctionWith(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  ProbeNode::ptr other, const Context& ctx);
Root::ptr MakeSparseExclusion(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Context& ctx);
Root::ptr MakeSparseExclusionOf(
  LeadNode::ptr include, std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Context& ctx);

Root::ptr MakeWindowThreshold(std::span<const search::PostingClause> terms,
                              const IndexInput* doc,
                              std::vector<FillNode::ptr>& rest,
                              uint32_t min_match, const Context& ctx);

template<typename Term>
Root::ptr MakeBitsetDisjunctionOfTerms(std::span<const Term> terms,
                                       const TermReader* field,
                                       const IndexInput& doc,
                                       doc_id_t docs_count, const Context&) {
  return search::MakeBitsetOf<Root::ptr>(terms, field, doc, docs_count,
                                         nullptr);
}

template<typename Term>
Root::ptr MakeWindowDisjunctionOfTerms(std::span<const Term> terms,
                                       const TermReader* field,
                                       const IndexInput& doc,
                                       const Context& ctx) {
  SDB_ASSERT(terms.size() > 1);
  return ResolveInput(doc, [&]<typename Input> -> Root::ptr {
    using Leaf = PostingFill<Input>;
    using Optional = search::OrGroup<fill::SetLeaves<Leaf>>;
    const auto init = [&](Leaf& leaf, size_t i) {
      const auto& own = search::FieldOf(terms[i], field);
      const auto& meta = search::CookieOf(terms[i]);
      SDB_ASSERT(meta.docs_count != 0);
      leaf.Prepare(meta, doc, meta.docs_count != 1 && search::BoundsOf(own),
                   meta.docs_count != 1 && search::FreqOf(own));
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

}  // namespace irs::docs
