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
#include <vector>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/phrase_of.hpp"
#include "iresearch/search/count/bitset.hpp"
#include "iresearch/search/count/make.hpp"
#include "iresearch/search/count/walk.hpp"
#include "iresearch/search/count/window_disjunction.hpp"
#include "iresearch/search/fill/set_leaves.hpp"

namespace irs::count {

using search::kWindowBits;
using search::kWindowDocs;
using search::kWindowWords;

template<template<typename...> class Shape, typename... Parts, typename... Args>
Root::ptr MakeShape(const Context& ctx, Args&&... args) {
  if (ctx.table != nullptr) {
    return memory::make_managed<Shape<Parts..., search::TableFilter*>>(
      ctx.table, std::forward<Args>(args)...);
  }
  return memory::make_managed<Shape<Parts..., utils::Empty>>(
    utils::Empty{}, std::forward<Args>(args)...);
}

template<typename Node>
using PlainWalk = Walk<Node, utils::Empty>;
template<typename Node>
using FilteredWalk = Walk<Node, search::TableFilter*>;

template<search::PhraseMatch M>
Root::ptr MakeFixedPhraseWalk(const FixedPhraseQuery& query,
                              const Context& ctx) {
  if (ctx.table != nullptr) {
    return search::MakeFixedPhraseOf<M, FilteredWalk, Root::ptr>(query,
                                                                 ctx.table);
  }
  return search::MakeFixedPhraseOf<M, PlainWalk, Root::ptr>(query,
                                                            utils::Empty{});
}

template<search::PhraseMatch M>
Root::ptr MakeVariadicPhraseWalk(const VariadicPhraseQuery& query,
                                 const Context& ctx) {
  if (ctx.table != nullptr) {
    return search::MakeVariadicPhraseOf<M, FilteredWalk, Root::ptr>(query,
                                                                    ctx.table);
  }
  return search::MakeVariadicPhraseOf<M, PlainWalk, Root::ptr>(query,
                                                               utils::Empty{});
}

using search::FillNode;
using search::LeadNode;
using search::ProbeNode;

using search::BuildConjunction;
using search::BuildDense;
using search::CollectDense;
using search::FillOf;
using search::LeadOf;
using search::ProbeOf;

using search::PostingFill;
using search::PostingLead;
using search::PostingProbe;
using search::ResolveArity;
using search::ResolveBounds;
using search::ResolveInput;
using search::SegmentDoc;

Root::ptr MakeBitsetDisjunction(std::span<const search::PostingClause> terms,
                                const IndexInput* doc,
                                std::vector<FillNode::ptr>& rest,
                                doc_id_t docs_count, const Context& ctx);
Root::ptr MakeWindowDisjunction(std::span<const search::PostingClause> terms,
                                const IndexInput* doc,
                                std::vector<FillNode::ptr>& rest,
                                const Context& ctx);
Root::ptr MakeBitsetConjunction(std::span<const search::PostingClause> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                const SubReader& segment, const Context& ctx);

Root::ptr MakeWindowConjunction(std::span<const search::PostingClause> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                const SubReader& segment, const Context& ctx);

Root::ptr MakeSparseConjunction(std::span<const search::PostingClause> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                const SubReader& segment, const Context& ctx);
Root::ptr MakeSparseConjunctionWith(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  ProbeNode::ptr other, const Context& ctx);

Root::ptr MakeSubtractConjunction(std::span<const search::PostingClause> terms,
                                  std::span<const QueryBuilder::ptr> filters,
                                  const SubReader& segment, const Context& ctx);

Root::ptr MakeSubtractDisjunction(const search::PostingClause& first,
                                  const search::PostingClause& second,
                                  const SubReader& segment, const Context& ctx);

Root::ptr MakeBitsetExclusion(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Context& ctx);

Root::ptr MakeWindowExclusion(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Context& ctx);

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

Root::ptr MakeBitsThreshold(std::span<const search::PostingClause> terms,
                            const IndexInput* doc,
                            std::vector<FillNode::ptr>& rest,
                            uint32_t min_match, const Context& ctx);
Root::ptr MakeCountThreshold(std::span<const search::PostingClause> terms,
                             const IndexInput* doc,
                             const std::vector<FillNode::ptr>& rest,
                             uint32_t min_match, const Context& ctx);

Root::ptr MakeFixedPhrase(const FixedPhraseQuery& query, const Context& ctx);
Root::ptr MakeFixedPhraseIntervals(const FixedPhraseQuery& query,
                                   const Context& ctx);
Root::ptr MakeFixedPhraseSlop(const FixedPhraseQuery& query,
                              const Context& ctx);
Root::ptr MakeVariadicPhrase(const VariadicPhraseQuery& query,
                             const Context& ctx);
Root::ptr MakeVariadicPhraseIntervals(const VariadicPhraseQuery& query,
                                      const Context& ctx);
Root::ptr MakeVariadicPhraseSlop(const VariadicPhraseQuery& query,
                                 const Context& ctx);

Root::ptr MakeNGram(const NGramSimilarityQuery& query, const Context& ctx);
Root::ptr MakeNGramAll(const NGramSimilarityQuery& query, const Context& ctx);

template<typename Term>
Root::ptr MakeBitsetDisjunctionOfTerms(std::span<const Term> terms,
                                       const TermReader* field,
                                       const IndexInput& doc,
                                       doc_id_t docs_count,
                                       const Context& ctx) {
  return search::MakeBitsetOf<Root::ptr>(terms, field, doc, docs_count,
                                         ctx.table);
}

template<typename Term>
Root::ptr MakeWindowDisjunctionOfTerms(std::span<const Term> terms,
                                       const TermReader* field,
                                       const IndexInput& doc,
                                       const Context& ctx) {
  SDB_ASSERT(terms.size() > 1);
  return ResolveInput(doc, [&]<typename Input> -> Root::ptr {
    using Leaf = PostingFill<Input>;
    const auto init = [&](Leaf& leaf, size_t i) {
      const auto& own = search::FieldOf(terms[i], field);
      const auto& meta = search::CookieOf(terms[i]);
      SDB_ASSERT(meta.docs_count != 0);
      leaf.Prepare(meta, doc, meta.docs_count != 1 && search::BoundsOf(own),
                   meta.docs_count != 1 && search::FreqOf(own));
    };
    return MakeShape<WindowDisjunction, fill::SetLeaves<Leaf>>(
      ctx, std::piecewise_construct, std::forward_as_tuple(terms.size(), init));
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
