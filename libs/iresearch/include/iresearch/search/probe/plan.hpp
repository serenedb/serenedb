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

#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/common/optional_scored.hpp"
#include "iresearch/search/common/posting_probe.hpp"
#include "iresearch/search/common/probe_leaves.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/single_posting.hpp"
#include "iresearch/search/probe/sparse_disjunction_docs.hpp"
#include "iresearch/search/probe/sparse_disjunction_scored.hpp"

namespace irs::probe {

template<typename Result, typename Make>
Result ResolvePostingDocs(const search::PostingClause& posting, Make&& make) {
  const auto& meta = posting.state.cookie;
  SDB_ASSERT(meta.docs_count != 0);
  if (meta.docs_count == 1) {
    return make.template operator()<SinglePostingDocs>(meta);
  }
  const auto& own = *posting.state.reader;
  return search::ResolveInput(
    *search::DocOf(own), [&]<typename Input> -> Result {
      return make.template operator()<search::PostingProbe<Input>>(
        meta, *search::DocOf(own), search::LayoutOf(own),
        search::BoundsOf(own));
    });
}

template<typename Term>
Node::ptr MakeSparseDisjunctionDocs(std::span<const Term> terms,
                                    std::span<const QueryBuilder::ptr> filters,
                                    const TermReader* field,
                                    const SubReader& segment,
                                    uint64_t interrogations) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  return search::BuildProbeLeaves<Node::ptr>(
    terms, filters, field, segment, interrogations, search::ProbeOrder::Densest,
    [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
      return search::ResolveArity<search::kRunArity, search::kRunFloor>(
        size, [&]<size_t N> -> Node::ptr {
          using Node = SparseDisjunctionDocs<Leaf, N>;
          return memory::make_managed<Impl<Node>>(
            size, std::forward<decltype(init)>(init));
        });
    });
}

template<typename Term>
Node::ptr MakeDisjunctionDocs(std::span<const Term> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              const TermReader* field, const SubReader& segment,
                              uint64_t interrogations) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  if (filters.empty() && !terms.empty()) {
    const auto* const doc =
      search::DocOf(search::FieldOf(terms.front(), field));
    const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
    if (doc != nullptr &&
        search::TakeProbeBitset(terms, *doc, docs_count, interrogations)) {
      return search::MakeBitsetNode<Node::ptr>(
        search::DisjunctionBuckets(terms, field), *doc, docs_count, nullptr);
    }
  }
  return MakeSparseDisjunctionDocs(terms, filters, field, segment,
                                   interrogations);
}

template<typename Term, typename ClauseFn>
Node::ptr MakeSparseDisjunctionScored(
  std::span<const Term> terms, std::span<const QueryBuilder::ptr> filters,
  search::Terms uniformity, const TermReader* field, const Scorer* scorer,
  score_t boost, const SubReader& segment, const ScoreRecipe& recipe,
  ScoreMergeType merge, uint64_t interrogations, ClauseFn clause,
  score_t absorbed = 0) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  return search::BuildOptionalLeaves<Node::ptr>(
    terms, filters, uniformity, field, scorer, boost, segment, recipe,
    interrogations, clause,
    [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
      return search::ResolveArity<search::kRunArity, search::kRunFloor>(
        size, [&]<size_t N> -> Node::ptr {
          using Node = SparseDisjunctionScored<Leaf, N>;
          return memory::make_managed<Impl<Node>>(
            size, std::forward<decltype(init)>(init), merge, absorbed);
        });
    },
    search::ProbeOrder::Densest);
}

inline Node::ptr BuildOptionalProbe(
  std::span<const search::PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters, uint32_t min_should_match,
  const SubReader& segment, uint64_t interrogations) {
  SDB_ASSERT(min_should_match != 0);
  return min_should_match == 1
           ? MakeDisjunctionDocs(should, should_filters, nullptr, segment,
                                 interrogations)
           : MakeSparseThresholdDocs(should, should_filters, segment,
                                     min_should_match, interrogations);
}

inline Node::ptr BuildOptionalProbeScored(
  std::span<const search::PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters, search::Terms uniformity,
  uint32_t min_should_match, const SubReader& segment,
  const ScoreRecipe& recipe, ScoreMergeType merge, uint64_t interrogations,
  const ScoredCtx& ctx) {
  SDB_ASSERT(min_should_match != 0);
  SDB_ASSERT(should.size() + should_filters.size() >= min_should_match);
  return min_should_match == 1
           ? MakeSparseDisjunctionScored(should, should_filters, uniformity,
                                         nullptr, nullptr, kNoBoost, segment,
                                         recipe, merge, interrogations,
                                         ScoredClauseOf(segment, ctx, recipe))
           : MakeSparseThresholdScored(should, should_filters, uniformity,
                                       segment, recipe, merge, min_should_match,
                                       interrogations, ctx);
}

}  // namespace irs::probe
