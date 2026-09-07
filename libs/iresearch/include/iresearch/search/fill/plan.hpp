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
#include "iresearch/search/common/collect_scored.hpp"
#include "iresearch/search/common/fill_posting_scored.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/make.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/fill/window_disjunction.hpp"

namespace irs::fill {

using search::kWindowBits;
using search::kWindowDocs;
using search::kWindowWords;

using search::FillNode;
using search::LeadNode;
using search::ProbeNode;

using search::BuildConjunction;
using search::BuildDense;
using search::BuildScoredSet;
using search::CollectDense;
using search::CollectDenseScored;
using search::FillOf;
using search::LeadOf;
using search::ProbeOf;
using search::ScoreArgs;
using search::ScoreRecipe;

using search::PostingFill;
using search::PostingLead;
using search::PostingProbe;
using search::ResolveArity;
using search::ResolveBounds;
using search::ResolveFillScored;
using search::ResolveInput;
using search::SegmentDoc;

Node::ptr MakeBitsetDisjunctionDocs(
  std::span<const search::PostingClause> terms, const IndexInput* doc,
  const std::vector<Node::ptr>& rest, doc_id_t docs_count);

Node::ptr MakeWindowDisjunctionDocs(
  std::span<const search::PostingClause> terms, const IndexInput* doc,
  std::vector<Node::ptr>& rest);

Node::ptr MakeBitsetConjunctionDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment);
Node::ptr MakeWindowConjunctionDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment);
Node::ptr MakeSparseConjunctionDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment);
Node::ptr MakeSparseConjunctionWithDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  ProbeNode::ptr other);
Node::ptr MakeSparseConjunctionScored(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  const ScoredCtx& ctx, ScoreMergeType merge, score_t absorbed);

Node::ptr MakeBitsetExclusionDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates);
Node::ptr MakeWindowExclusionDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates);
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

Node::ptr MakeBitsThresholdDocs(std::span<const search::PostingClause> terms,
                                const IndexInput* doc,
                                std::vector<Node::ptr>& rest,
                                uint32_t min_match);
Node::ptr MakeCountThresholdDocs(std::span<const search::PostingClause> terms,
                                 const IndexInput* doc,
                                 const std::vector<Node::ptr>& rest,
                                 uint32_t min_match);
Node::ptr MakeBitsThresholdScored(std::span<const search::PostingClause> terms,
                                  const IndexInput* doc,
                                  std::vector<Node::ptr>& rest,
                                  search::Terms uniformity,
                                  const ScoreRecipe& recipe,
                                  ScoreMergeType merge, uint32_t min_match,
                                  score_t absorbed);
Node::ptr MakeCountThresholdScored(std::span<const search::PostingClause> terms,
                                   const IndexInput* doc,
                                   const std::vector<Node::ptr>& rest,
                                   search::Terms uniformity,
                                   const ScoreRecipe& recipe,
                                   ScoreMergeType merge, uint32_t min_match,
                                   score_t absorbed);

Node::ptr MakeSparseBoostScored(
  std::span<const search::PostingClause> must_terms,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const search::PostingClause> should_terms,
  std::span<const QueryBuilder::ptr> should_filters, search::Terms uniformity,
  const SubReader& segment, const ScoredCtx& ctx, ScoreMergeType merge,
  score_t absorbed);

Node::ptr MakeFixedPhraseDocs(const FixedPhraseQuery& query);
Node::ptr MakeFixedPhraseIntervalsDocs(const FixedPhraseQuery& query);
Node::ptr MakeFixedPhraseSlopDocs(const FixedPhraseQuery& query);
Node::ptr MakeVariadicPhraseDocs(const VariadicPhraseQuery& query);
Node::ptr MakeVariadicPhraseIntervalsDocs(const VariadicPhraseQuery& query);
Node::ptr MakeVariadicPhraseSlopDocs(const VariadicPhraseQuery& query);

Node::ptr MakeFixedPhraseScored(const FixedPhraseQuery& query,
                                const ScoredCtx& ctx, ScoreMergeType merge);
Node::ptr MakeFixedPhraseIntervalsScored(const FixedPhraseQuery& query,
                                         const ScoredCtx& ctx,
                                         ScoreMergeType merge);
Node::ptr MakeFixedPhraseSlopScored(const FixedPhraseQuery& query,
                                    const ScoredCtx& ctx, ScoreMergeType merge);
Node::ptr MakeVariadicPhraseScored(const VariadicPhraseQuery& query,
                                   const ScoredCtx& ctx, ScoreMergeType merge);
Node::ptr MakeVariadicPhraseIntervalsScored(const VariadicPhraseQuery& query,
                                            const ScoredCtx& ctx,
                                            ScoreMergeType merge);
Node::ptr MakeVariadicPhraseSlopScored(const VariadicPhraseQuery& query,
                                       const ScoredCtx& ctx,
                                       ScoreMergeType merge);

Node::ptr MakeNGramDocs(const NGramSimilarityQuery& query);
Node::ptr MakeNGramAllDocs(const NGramSimilarityQuery& query);
Node::ptr MakeNGramScored(const NGramSimilarityQuery& query,
                          const ScoredCtx& ctx, ScoreMergeType merge);
Node::ptr MakeNGramAllScored(const NGramSimilarityQuery& query,
                             const ScoredCtx& ctx, ScoreMergeType merge);

template<typename Term>
Node::ptr MakeWindowDisjunctionOfTermsDocs(std::span<const Term> terms,
                                           const TermReader* field,
                                           const IndexInput& doc) {
  SDB_ASSERT(terms.size() > 1);
  return search::ResolveInput(doc, [&]<typename Input> -> Node::ptr {
    using Leaf = search::PostingFill<Input>;
    return memory::make_managed<Impl<WindowDisjunctionDocs<SetLeaves<Leaf>>>>(
      std::piecewise_construct,
      std::forward_as_tuple(terms.size(), [&](Leaf& leaf, size_t i) {
        const auto& own = search::FieldOf(terms[i], field);
        const auto& meta = search::CookieOf(terms[i]);
        SDB_ASSERT(meta.docs_count != 0);
        leaf.Prepare(meta, doc, meta.docs_count != 1 && search::BoundsOf(own),
                     meta.docs_count != 1 && search::FreqOf(own));
      }));
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
    const auto leaves =
      std::forward_as_tuple(std::forward<decltype(args)>(args)...);
    if (absorbed == 0) {
      using Node = WindowDisjunctionScored<Set, false>;
      return memory::make_managed<Impl<Node>>(std::piecewise_construct, leaves,
                                              merge);
    }
    using Node = WindowDisjunctionScored<Set, true>;
    return memory::make_managed<Impl<Node>>(std::piecewise_construct, leaves,
                                            merge, absorbed);
  };
  return search::BuildScoredWindow<Node::ptr>(
    terms, field, scorer, boost, doc, rest, uniformity, recipe, merge, make);
}

}  // namespace irs::fill
