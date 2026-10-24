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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/probe_leaves.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/detail/window_set.hpp"
#include "iresearch/search/probe/boolean_sparse.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/leaves.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/probe/posting_scored.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::probe {

template<typename Scoring>
Scoring WithUniformity(Scoring scoring, detail::Terms uniformity) noexcept {
  if constexpr (detail::kScores<Scoring>) {
    scoring.uniformity = uniformity;
  }
  return scoring;
}

template<typename Scoring>
Node::ptr MakeOptional(std::span<const detail::PostingClause> should,
                       std::span<const QueryBuilder::ptr> should_filters,
                       uint32_t min_match, const SubReader& segment,
                       uint64_t interrogations, const Scoring& scoring,
                       const detail::ScoredCtx& ctx, score_t absorbed) {
  SDB_ASSERT(min_match != 0);
  return min_match == 1
           ? MakeDisjunction(should, should_filters, nullptr, segment,
                             interrogations, scoring, ctx, absorbed)
           : MakeSparseThreshold(should, should_filters, segment, min_match,
                                 interrogations, scoring, ctx, absorbed);
}

extern template Node::ptr MakeOptional(std::span<const detail::PostingClause>,
                                       std::span<const QueryBuilder::ptr>,
                                       uint32_t, const SubReader&, uint64_t,
                                       const detail::NoScoring&,
                                       const detail::ScoredCtx&, score_t);

extern template Node::ptr MakeOptional(std::span<const detail::PostingClause>,
                                       std::span<const QueryBuilder::ptr>,
                                       uint32_t, const SubReader&, uint64_t,
                                       const detail::TermScoring&,
                                       const detail::ScoredCtx&, score_t);

template<typename Scoring, typename Make>
Node::ptr BuildMusts(std::span<const detail::PostingClause> terms,
                     std::span<const QueryBuilder::ptr> filters,
                     const SubReader& segment, uint64_t interrogations,
                     const Scoring& scoring, const detail::ScoredCtx& ctx,
                     Make&& make) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  const auto clause = ClauseFor(segment, ctx, scoring);
  if (terms.size() + filters.size() == 1) {
    if (filters.empty()) {
      const auto& posting = terms.front();
      if constexpr (detail::kScores<Scoring>) {
        if (detail::ScoresPerDocTerm(posting)) {
          const auto& own = *posting.state.reader;
          const auto& doc = *detail::DocOf(own);
          return detail::ResolveInput(doc, [&]<typename Input> -> Node::ptr {
            return make.template operator()<detail::PostingProbeScored<Input>>(
              std::forward_as_tuple(
                posting.state.cookie, doc, segment, own,
                scoring.recipe.Args(posting.stats, posting.boost)));
          });
        }
      } else {
        return ResolvePostingDocs<Node::ptr>(
          posting, [&]<typename Leaf>(auto&&... args) -> Node::ptr {
            return make.template operator()<Leaf>(
              std::forward_as_tuple(std::forward<decltype(args)>(args)...));
          });
      }
    }
    auto node = filters.empty()
                  ? clause(terms.front(), nullptr, interrogations)
                  : clause(detail::PostingClause{TermState{nullptr, {}}},
                           filters.front().get(), interrogations);
    if (!node) {
      return {};
    }
    return make.template operator()<Erased>(
      std::forward_as_tuple(std::move(node)));
  }
  return detail::BuildProbeLeaves<Node::ptr>(
    terms, filters, nullptr, segment, interrogations,
    detail::ProbeOrder::Narrowest, scoring, clause,
    [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
      return detail::ResolveArity<detail::kRunArity, detail::kRunFloor>(
        size, [&]<size_t N> -> Node::ptr {
          return make.template operator()<AndLeaves<Leaf, N>>(
            std::forward_as_tuple(size, std::forward<decltype(init)>(init)));
        });
    });
}

template<typename Scoring>
Node::ptr MakeConjunction(std::span<const detail::PostingClause> terms,
                          std::span<const QueryBuilder::ptr> filters,
                          const SubReader& segment, uint64_t interrogations,
                          const Scoring& scoring, const detail::ScoredCtx& ctx,
                          score_t absorbed) {
  const auto size = terms.size() + filters.size();
  if (size == 0) {
    if constexpr (detail::kScores<Scoring>) {
      return MakeAllScored(segment, absorbed);
    } else {
      return MakeAllDocs(segment);
    }
  }
  if (size == 1 && absorbed == 0) {
    const auto clause = ClauseFor(segment, ctx, scoring);
    return filters.empty()
             ? clause(terms.front(), nullptr, interrogations)
             : clause(detail::PostingClause{TermState{nullptr, {}}},
                      filters.front().get(), interrogations);
  }
  return BuildMusts(terms, filters, segment, interrogations, scoring, ctx,
                    [&]<typename Musts>(auto&& musts) -> Node::ptr {
                      return MakeSparse<Musts, utils::Empty, utils::Empty>(
                        detail::ScoreOf(scoring, absorbed),
                        std::forward<decltype(musts)>(musts),
                        std::forward_as_tuple(), std::forward_as_tuple());
                    });
}

template<typename Scoring>
Node::ptr MakeRequired(const BooleanQuery& query, uint64_t interrogations,
                       const Scoring& scoring, const detail::ScoredCtx& ctx,
                       score_t absorbed) {
  const auto& segment = query.Segment();
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  const auto min_match = query.MinShouldMatch();
  const auto must_scoring =
    WithUniformity(scoring, query.Uniformity(Occur::Must));
  if (min_match == 0) {
    return MakeConjunction(must, must_filters, segment, interrogations,
                           must_scoring, ctx, absorbed);
  }
  const auto should_scoring =
    WithUniformity(scoring, query.Uniformity(Occur::Should));
  if (must.empty() && must_filters.empty()) {
    return MakeOptional(should, should_filters, min_match, segment,
                        interrogations, should_scoring, ctx, absorbed);
  }
  auto held = MakeOptional(
    should, should_filters, min_match, segment,
    std::min(interrogations,
             detail::IncludeCandidates(must, must_filters, segment)),
    should_scoring, ctx, score_t{0});
  if (!held) {
    return {};
  }
  return BuildMusts(must, must_filters, segment, interrogations, must_scoring,
                    ctx, [&]<typename Musts>(auto&& musts) -> Node::ptr {
                      return MakeSparse<Musts, Erased, utils::Empty>(
                        detail::ScoreOf(scoring, absorbed),
                        std::forward<decltype(musts)>(musts),
                        std::forward_as_tuple(std::move(held)),
                        std::forward_as_tuple());
                    });
}

inline Node::ptr MakeBoost(const BooleanQuery& query, uint64_t interrogations,
                           const detail::TermScoring& scoring,
                           const detail::ScoredCtx& ctx, score_t absorbed) {
  const auto& segment = query.Segment();
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  SDB_ASSERT(!should.empty() || !should_filters.empty());
  const bool no_must = must.empty() && must_filters.empty();
  const auto should_scoring =
    WithUniformity(scoring, query.Uniformity(Occur::Should));
  const auto must_scoring =
    WithUniformity(scoring, query.Uniformity(Occur::Must));
  const auto build = [&]<typename Head>(auto&& head) -> Node::ptr {
    return detail::BuildProbeLeaves<Node::ptr>(
      should, should_filters, nullptr, segment,
      no_must ? interrogations
              : std::min(interrogations, detail::IncludeCandidates(
                                           must, must_filters, segment)),
      detail::ProbeOrder::Densest, should_scoring,
      ClauseFor(segment, ctx, should_scoring),
      [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
        return detail::ResolveArity<detail::kTailArity, detail::kTailFloor>(
          size, [&]<size_t N> -> Node::ptr {
            return MakeSparse<Head, BoostLeaves<Leaf, N>, utils::Empty>(
              detail::Scored{scoring.merge, absorbed},
              std::forward<decltype(head)>(head),
              std::forward_as_tuple(size, std::forward<decltype(init)>(init)),
              std::forward_as_tuple());
          });
      });
  };
  if (no_must) {
    return build.template operator()<utils::Empty>(std::forward_as_tuple());
  }
  if (must.size() + must_filters.size() == 1) {
    return BuildMusts(must, must_filters, segment, interrogations, must_scoring,
                      ctx, [&]<typename Head>(auto&& head) -> Node::ptr {
                        return build.template operator()<Head>(
                          std::forward<decltype(head)>(head));
                      });
  }
  auto head = MakeConjunction(must, must_filters, segment, interrogations,
                              must_scoring, ctx, score_t{0});
  if (!head) {
    return {};
  }
  return build.template operator()<Erased>(
    std::forward_as_tuple(std::move(head)));
}

template<typename Scoring>
Node::ptr MakeIncluded(const BooleanQuery& query, uint64_t interrogations,
                       const Scoring& scoring, const detail::ScoredCtx& ctx,
                       score_t absorbed) {
  if constexpr (detail::kScores<Scoring>) {
    if (query.MinShouldMatch() == 0 &&
        (!query.Terms(Occur::Should).empty() ||
         !query.Queries(Occur::Should).empty())) {
      return MakeBoost(query, interrogations, scoring, ctx, absorbed);
    }
  }
  return MakeRequired(query, interrogations, scoring, ctx, absorbed);
}

template<typename Scoring>
Node::ptr MakeBoolean(const BooleanQuery& query, uint64_t interrogations,
                      const Scoring& scoring, const detail::ScoredCtx& ctx) {
  score_t absorbed = 0;
  if constexpr (detail::kScores<Scoring>) {
    SDB_ASSERT(ctx.fetcher);
    absorbed = query.Absorbed(*ctx.fetcher);
  }
  if (!query.Terms(Occur::MustNot).empty() ||
      !query.Queries(Occur::MustNot).empty()) {
    return MakeExclusion(query, interrogations, scoring, ctx, absorbed);
  }
  return MakeIncluded(query, interrogations, scoring, ctx, absorbed);
}

}  // namespace irs::probe
