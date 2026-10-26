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
#include "iresearch/search/detail/posting_probe.hpp"
#include "iresearch/search/detail/probe_leaves.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/window_set.hpp"
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

template<typename Scoring>
auto ClauseFor(const SubReader& segment, const detail::ScoredCtx& ctx,
               const Scoring& scoring) {
  if constexpr (detail::kScores<Scoring>) {
    return ScoredClauseOf(segment, ctx, scoring.recipe);
  } else {
    return detail::ProbeClauseOf(segment);
  }
}

template<typename Musts, typename Optional, typename Excludes, typename Score,
         typename... Args>
Node::ptr MakeSparse(Score score, Args&&... args) {
  using Node = BooleanSparse<Musts, Optional, Excludes, Score>;
  return memory::make_managed<Impl<Node>>(std::piecewise_construct,
                                          std::forward<Args>(args)..., score);
}

template<typename Term, typename Scoring>
Node::ptr MakeSparseDisjunction(
  std::span<const Term> terms, std::span<const QueryBuilder::ptr> filters,
  const TermReader* field, const SubReader& segment, uint64_t interrogations,
  const Scoring& scoring, const detail::ScoredCtx& ctx, score_t absorbed) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  return detail::BuildProbeLeaves<Node::ptr>(
    terms, filters, field, segment, interrogations, detail::ProbeOrder::Densest,
    scoring, ClauseFor(segment, ctx, scoring),
    [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
      return detail::ResolveArity<detail::kRunArity, detail::kRunFloor>(
        size, [&]<size_t N> -> Node::ptr {
          return MakeSparse<utils::Empty,
                            OrLeaves<Leaf, N, detail::kScores<Scoring>>,
                            utils::Empty>(
            detail::ScoreOf(scoring, absorbed), std::forward_as_tuple(),
            std::forward_as_tuple(size, std::forward<decltype(init)>(init)),
            std::forward_as_tuple());
        });
    });
}

template<typename Term>
Node::ptr MakeWindowDisjunction(std::span<const Term> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                const TermReader* field,
                                const detail::TermScoring& scoring,
                                const detail::ScoredCtx& ctx,
                                score_t absorbed) {
  const IndexInput* doc = nullptr;
  std::vector<detail::FillNode::ptr> rest;
  if (!detail::CollectDense(terms, filters, field, doc, rest,
                            [&](const QueryBuilder& child) {
                              return child.PlanFill(ctx, scoring.merge);
                            })) {
    return {};
  }
  return detail::BuildWindowSet<Node::ptr>(
    terms, field, doc, rest, scoring,
    [&]<typename Set>(auto&&... args) -> Node::ptr {
      using Node = BooleanWindow<detail::OrGroup<Set>, detail::Scored>;
      return memory::make_managed<Impl<Node>>(
        std::piecewise_construct,
        std::forward_as_tuple(std::forward<decltype(args)>(args)...),
        detail::Scored{scoring.merge, absorbed});
    });
}

template<typename Term, typename Scoring>
Node::ptr MakeDisjunction(std::span<const Term> terms,
                          std::span<const QueryBuilder::ptr> filters,
                          const TermReader* field, const SubReader& segment,
                          uint64_t interrogations, const Scoring& scoring,
                          const detail::ScoredCtx& ctx, score_t absorbed = 0) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  if (filters.empty() && !terms.empty()) {
    const auto* const doc =
      detail::DocOf(detail::FieldOf(terms.front(), field));
    const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
    if (doc != nullptr &&
        detail::TakeProbeBitset(terms, *doc, docs_count, interrogations)) {
      if constexpr (detail::kScores<Scoring>) {
        if (auto windowed = MakeWindowDisjunction(terms, filters, field,
                                                  scoring, ctx, absorbed)) {
          return windowed;
        }
      } else {
        return detail::MakeBitsetNode<Node::ptr>(
          detail::DisjunctionBuckets(terms, field), *doc, docs_count, nullptr);
      }
    }
  }
  return MakeSparseDisjunction(terms, filters, field, segment, interrogations,
                               scoring, ctx, absorbed);
}

template<typename Scoring>
Node::ptr MakeSparseThreshold(std::span<const detail::PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              const SubReader& segment, uint32_t min_match,
                              uint64_t interrogations, const Scoring& scoring,
                              const detail::ScoredCtx& ctx, score_t absorbed) {
  SDB_ASSERT(min_match > 1);
  if (terms.size() + filters.size() < min_match) {
    return {};
  }
  return detail::BuildProbeLeaves<Node::ptr>(
    terms, filters, nullptr, segment, interrogations,
    detail::ProbeOrder::Densest, scoring, ClauseFor(segment, ctx, scoring),
    [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
      return detail::ResolveArity<detail::kRunArity, detail::kRunFloor>(
        size, [&]<size_t N> -> Node::ptr {
          return MakeSparse<utils::Empty,
                            ThresholdLeaves<Leaf, N, detail::kScores<Scoring>>,
                            utils::Empty>(
            detail::ScoreOf(scoring, absorbed), std::forward_as_tuple(),
            std::forward_as_tuple(size, std::forward<decltype(init)>(init),
                                  min_match),
            std::forward_as_tuple());
        });
    });
}

}  // namespace irs::probe
