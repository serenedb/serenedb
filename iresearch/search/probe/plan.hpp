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

#include "iresearch/search/detail/bitset_of.hpp"
#include "iresearch/search/detail/boolean_groups.hpp"
#include "iresearch/search/detail/collect_scored.hpp"
#include "iresearch/search/detail/posting_probe.hpp"
#include "iresearch/search/detail/probe_leaves.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/probe/boolean_sparse.hpp"
#include "iresearch/search/probe/boolean_window.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/leaves.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/single_posting.hpp"
#include "iresearch/search/scorers/score_policy.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::probe {

template<typename Result, typename Make>
Result ResolvePostingDocs(const detail::PostingClause& posting, Make&& make) {
  const auto& meta = posting.state.cookie;
  SDB_ASSERT(meta.docs_count != 0);
  if (meta.docs_count == 1) {
    return make.template operator()<SinglePostingDocs>(meta);
  }
  const auto& own = *posting.state.reader;
  return detail::ResolveInput(
    *detail::DocOf(own), [&]<typename Input> -> Result {
      return make.template operator()<detail::PostingProbe<Input>>(
        meta, *detail::DocOf(own), detail::LayoutOf(own),
        detail::BoundsOf(own));
    });
}

template<typename Term>
Node::ptr MakeSparseDisjunctionDocs(std::span<const Term> terms,
                                    std::span<const QueryBuilder::ptr> filters,
                                    const TermReader* field,
                                    const SubReader& segment,
                                    uint64_t interrogations) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  return detail::BuildProbeLeaves<Node::ptr>(
    terms, filters, field, segment, interrogations, detail::ProbeOrder::Densest,
    [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
      return detail::ResolveArity<detail::kRunArity, detail::kRunFloor>(
        size, [&]<size_t N> -> Node::ptr {
          using Node =
            BooleanSparse<utils::Empty, OrLeaves<Leaf, N>, utils::Empty>;
          return memory::make_managed<Impl<Node>>(
            std::piecewise_construct, std::forward_as_tuple(),
            std::forward_as_tuple(size, std::forward<decltype(init)>(init)),
            std::forward_as_tuple());
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
      detail::DocOf(detail::FieldOf(terms.front(), field));
    const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
    if (doc != nullptr &&
        detail::TakeProbeBitset(terms, *doc, docs_count, interrogations)) {
      return detail::MakeBitsetNode<Node::ptr>(
        detail::DisjunctionBuckets(terms, field), *doc, docs_count, nullptr);
    }
  }
  return MakeSparseDisjunctionDocs(terms, filters, field, segment,
                                   interrogations);
}

template<typename Term, typename ClauseFn>
Node::ptr MakeSparseDisjunctionScored(
  std::span<const Term> terms, std::span<const QueryBuilder::ptr> filters,
  detail::Terms uniformity, const TermReader* field, const Scorer* scorer,
  score_t boost, const SubReader& segment, const detail::ScoreRecipe& recipe,
  ScoreMergeType merge, uint64_t interrogations, ClauseFn clause,
  score_t absorbed = 0) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  return detail::BuildOptionalLeaves<Node::ptr>(
    terms, filters, uniformity, field, scorer, boost, segment, recipe,
    interrogations, clause,
    [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
      return detail::ResolveArity<detail::kRunArity, detail::kRunFloor>(
        size, [&]<size_t N> -> Node::ptr {
          using Node = BooleanSparse<utils::Empty, OrLeaves<Leaf, N, true>,
                                     utils::Empty, detail::Scored>;
          return memory::make_managed<Impl<Node>>(
            std::piecewise_construct, std::forward_as_tuple(),
            std::forward_as_tuple(size, std::forward<decltype(init)>(init)),
            std::forward_as_tuple(), detail::Scored{merge, absorbed});
        });
    },
    detail::ProbeOrder::Densest);
}

template<typename Term>
Node::ptr MakeWindowDisjunctionScored(
  std::span<const Term> terms, std::span<const QueryBuilder::ptr> filters,
  detail::Terms uniformity, const TermReader* field, const Scorer* scorer,
  score_t boost, const SubReader& segment, const detail::ScoreRecipe& recipe,
  ScoreMergeType merge, const detail::ScoredCtx& ctx, score_t absorbed = 0) {
  const IndexInput* doc = nullptr;
  std::vector<detail::FillNode::ptr> rest;
  if (!detail::CollectDenseScored(terms, filters, field, doc, rest,
                                  [&](const QueryBuilder& child) {
                                    return child.PlanFill(ctx, merge);
                                  })) {
    return {};
  }
  return detail::BuildScoredWindow<Node::ptr>(
    terms, field, scorer, boost, doc, rest, uniformity, recipe, merge,
    [&]<typename Set>(auto&&... args) -> Node::ptr {
      using Node = BooleanWindow<detail::OrGroup<Set>, detail::Scored>;
      return memory::make_managed<Impl<Node>>(
        std::piecewise_construct,
        std::forward_as_tuple(std::forward<decltype(args)>(args)...),
        detail::Scored{merge, absorbed});
    });
}

template<typename Term, typename ClauseFn>
Node::ptr MakeDisjunctionScored(
  std::span<const Term> terms, std::span<const QueryBuilder::ptr> filters,
  detail::Terms uniformity, const TermReader* field, const Scorer* scorer,
  score_t boost, const SubReader& segment, const detail::ScoreRecipe& recipe,
  ScoreMergeType merge, uint64_t interrogations, ClauseFn clause,
  const detail::ScoredCtx& ctx, score_t absorbed = 0) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  if (filters.empty() && !terms.empty()) {
    const auto* const doc =
      detail::DocOf(detail::FieldOf(terms.front(), field));
    const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
    if (doc != nullptr &&
        detail::TakeProbeBitset(terms, *doc, docs_count, interrogations)) {
      if (auto windowed = MakeWindowDisjunctionScored(
            terms, filters, uniformity, field, scorer, boost, segment, recipe,
            merge, ctx, absorbed)) {
        return windowed;
      }
    }
  }
  return MakeSparseDisjunctionScored(terms, filters, uniformity, field, scorer,
                                     boost, segment, recipe, merge,
                                     interrogations, clause, absorbed);
}

inline Node::ptr BuildOptionalProbe(
  std::span<const detail::PostingClause> should,
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
  std::span<const detail::PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters, detail::Terms uniformity,
  uint32_t min_should_match, const SubReader& segment,
  const detail::ScoreRecipe& recipe, ScoreMergeType merge,
  uint64_t interrogations, const detail::ScoredCtx& ctx) {
  SDB_ASSERT(min_should_match != 0);
  SDB_ASSERT(should.size() + should_filters.size() >= min_should_match);
  return min_should_match == 1
           ? MakeDisjunctionScored(should, should_filters, uniformity, nullptr,
                                   nullptr, kNoBoost, segment, recipe, merge,
                                   interrogations,
                                   ScoredClauseOf(segment, ctx, recipe), ctx)
           : MakeSparseThresholdScored(should, should_filters, uniformity,
                                       segment, recipe, merge, min_should_match,
                                       interrogations, ctx);
}

}  // namespace irs::probe
