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
#include "iresearch/search/common/collect_scored.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/score_policy.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/fill/boolean_window.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/plan.hpp"
#include "iresearch/search/fill/set_leaves.hpp"

namespace irs::fill {

Node::ptr MakeBitsetDocs(const search::BooleanGroups& groups,
                         const SubReader& segment);

Node::ptr MakeSparseConjunctionScored(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  const ScoredCtx& ctx, ScoreMergeType merge, score_t absorbed);
Node::ptr MakeSparseExclusionScored(
  std::span<const search::PostingClause> must_terms,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const search::PostingClause> should_terms,
  std::span<const QueryBuilder::ptr> should_filters,
  search::Terms should_uniformity, uint32_t min_should_match,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const ScoredCtx& ctx, ScoreMergeType merge, ScoreMergeType own,
  score_t absorbed);
Node::ptr MakeSparseBoostScored(
  std::span<const search::PostingClause> must_terms,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const search::PostingClause> should_terms,
  std::span<const QueryBuilder::ptr> should_filters, search::Terms uniformity,
  const SubReader& segment, const ScoredCtx& ctx, ScoreMergeType merge,
  score_t absorbed);

Node::ptr MakeWindowDisjunctionDocs(
  std::span<const search::PostingClause> terms, const IndexInput* doc,
  std::vector<Node::ptr>& rest);
Node::ptr MakeWindowConjunctionDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment);
Node::ptr MakeWindowExclusionDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates);

Node::ptr MakeSparseConjunctionDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment);
Node::ptr MakeSparseConjunctionWithDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  ProbeNode::ptr other);
Node::ptr MakeSparseExclusionDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates);
Node::ptr MakeSparseExclusionOfDocs(
  LeadNode::ptr include, std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates);

template<typename Term>
Node::ptr MakeWindowDisjunctionOfTermsDocs(std::span<const Term> terms,
                                           const TermReader* field,
                                           const IndexInput& doc) {
  SDB_ASSERT(terms.size() > 1);
  return search::ResolveInput(doc, [&]<typename Input> -> Node::ptr {
    using Leaf = search::PostingFill<Input>;
    using Optional = search::OrGroup<SetLeaves<Leaf>>;
    using Node =
      BooleanWindow<utils::Empty, utils::Empty, Optional, utils::Empty>;
    return memory::make_managed<Impl<Node>>(
      std::piecewise_construct, std::forward_as_tuple(),
      std::forward_as_tuple(),
      std::forward_as_tuple(
        terms.size(),
        [&](Leaf& leaf, size_t i) {
          const auto& own = search::FieldOf(terms[i], field);
          const auto& meta = search::CookieOf(terms[i]);
          SDB_ASSERT(meta.docs_count != 0);
          leaf.Prepare(meta, doc, meta.docs_count != 1 && search::BoundsOf(own),
                       meta.docs_count != 1 && search::FreqOf(own));
        }),
      std::forward_as_tuple());
  });
}

template<typename Term>
Node::ptr MakeDisjunctionOfTermsDocs(std::span<const Term> terms,
                                     const TermReader* field,
                                     const IndexInput& doc,
                                     doc_id_t docs_count) {
  SDB_ASSERT(terms.size() > 1);
  if (search::TakeBitset<Node::ptr>(terms, doc, docs_count)) {
    return search::MakeBitsetOf<Node::ptr>(terms, field, doc, docs_count,
                                           nullptr);
  }
  return MakeWindowDisjunctionOfTermsDocs(terms, field, doc);
}

template<typename Term>
Node::ptr MakeWindowDisjunctionScored(
  std::span<const Term> terms, const TermReader* field, const Scorer* scorer,
  score_t boost, const IndexInput* doc, std::vector<Node::ptr>& rest,
  search::Terms uniformity, const ScoreRecipe& recipe, ScoreMergeType merge,
  score_t absorbed = 0) {
  SDB_ASSERT(!terms.empty() || !rest.empty());
  const auto make = [&]<typename Set>(auto&&... args) -> Node::ptr {
    using Node = BooleanWindow<utils::Empty, utils::Empty, search::OrGroup<Set>,
                               utils::Empty, search::Scored>;
    return memory::make_managed<Impl<Node>>(
      std::piecewise_construct, std::forward_as_tuple(),
      std::forward_as_tuple(),
      std::forward_as_tuple(std::forward<decltype(args)>(args)...),
      std::forward_as_tuple(), search::Scored{merge, absorbed});
  };
  return search::BuildScoredWindow<Node::ptr>(
    terms, field, scorer, boost, doc, rest, uniformity, recipe, merge, make);
}

}  // namespace irs::fill
