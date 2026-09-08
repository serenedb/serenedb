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

#include <span>
#include <tuple>
#include <utility>
#include <vector>

#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/collect_scored.hpp"
#include "iresearch/search/common/conjunction_scored.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/single_posting_docs.hpp"
#include "iresearch/search/lead/window_disjunction_docs.hpp"
#include "iresearch/search/lead/window_disjunction_scored.hpp"

namespace irs::lead {

using search::kWindowBits;
using search::kWindowDocs;
using search::kWindowWords;

using search::FillNode;
using search::LeadNode;

using search::BuildConjunction;
using search::BuildDense;
using search::BuildScoredConjunction;
using search::BuildScoredSet;
using search::BuildScoredTerms;
using search::ClauseOf;
using search::CollectDense;
using search::CollectDenseScored;
using search::CookieOf;
using search::FieldOf;
using search::FillOf;
using search::IncludeCandidates;
using search::LeadOf;
using search::ProbeOf;

using search::PostingFill;
using search::PostingLead;
using search::PostingLeadScored;
using search::PostingProbe;
using search::ResolveArity;
using search::ResolveBounds;
using search::ResolveInput;
using search::SegmentDoc;

template<typename Result, typename Make>
Result ResolvePostingDocs(const PostingClause& posting, Make&& make) {
  const auto& meta = posting.state.cookie;
  SDB_ASSERT(meta.docs_count != 0);
  if (meta.docs_count == 1) {
    return make.template operator()<SinglePostingDocs>(doc_limits::min() +
                                                       meta.doc_delta);
  }
  SDB_ASSERT(posting.state.reader != nullptr);
  const auto& own = *posting.state.reader;
  const auto& doc = *search::DocOf(own);
  return ResolveInput(doc, [&]<typename Input> -> Result {
    return make.template operator()<PostingLead<Input>>(
      meta, doc, search::LayoutOf(own), search::BoundsOf(own));
  });
}

template<typename Term>
Node::ptr MakeWindowDisjunctionOfTermsDocs(std::span<const Term> terms,
                                           const TermReader* field,
                                           const IndexInput& doc) {
  SDB_ASSERT(terms.size() > 1);
  return search::ResolveInput(doc, [&]<typename Input> -> Node::ptr {
    using Leaf = search::PostingFill<Input>;
    using Set = fill::SetLeaves<Leaf>;
    return memory::make_managed<Impl<WindowDisjunctionDocs<Set>>>(
      std::piecewise_construct,
      std::forward_as_tuple(terms.size(), [&](Leaf& leaf, size_t i) {
        const auto& own = FieldOf(terms[i], field);
        const auto& meta = CookieOf(terms[i]);
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
  return MakeWindowDisjunctionOfTermsDocs<Term>(terms, field, doc);
}

template<typename Term>
Node::ptr MakeWindowDisjunctionOfTermsScored(
  std::span<const Term> terms, const TermReader* field, const Scorer* scorer,
  score_t boost, const IndexInput& doc, search::Terms uniformity,
  const SubReader& segment, const ScoredCtx& ctx, ScoreMergeType merge,
  score_t absorbed) {
  const auto make = [&]<typename Set>(auto&&... args) -> Node::ptr {
    const auto leaves =
      std::forward_as_tuple(std::forward<decltype(args)>(args)...);
    using Node = WindowDisjunctionScored<Set>;
    return memory::make_managed<Impl<Node>>(std::piecewise_construct, leaves,
                                            merge, absorbed);
  };
  const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  std::vector<fill::Node::ptr> rest;
  return search::BuildScoredWindow<Node::ptr, Term>(
    terms, field, scorer, boost, &doc, rest, uniformity, recipe, merge, make);
}

}  // namespace irs::lead
