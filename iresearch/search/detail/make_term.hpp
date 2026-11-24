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
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/bitset_of.hpp"
#include "iresearch/search/detail/boolean_builder.hpp"
#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/make_query.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/search/queries/term_query.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"
#include "iresearch/search/scorers/constant_score.hpp"

namespace irs::detail {

template<typename Family>
inline constexpr bool kProbePlan =
  std::is_same_v<typename Family::Result, ProbeNode::ptr>;

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const TermQuery& query, const Ctx& ctx) {
  if constexpr (kScoredPlan<Family, Ctx>) {
    const PostingClause posting{.state = query.State(),
                                .boost = query.Boost(),
                                .stats = query.Stats(Family::Scoring(ctx))};
    return Family::MakePosting(posting, query.Segment(), ctx,
                               MergeOf(query, ctx));
  } else {
    return Family::MakePosting(PostingClause{query.State()}, query.Segment(),
                               ctx, ScoreMergeType::Noop);
  }
}

template<typename Family>
inline constexpr bool kLeadPlan =
  std::is_same_v<typename Family::Result, LeadNode::ptr>;

template<typename Family>
inline constexpr bool kRootPlan = requires { requires Family::kRoot; };

template<typename Family, typename Ctx>
typename Family::Result MakeConstantRows(const MultiTermQuery& query,
                                         const TermReader& field,
                                         const Ctx& ctx, ScoreMergeType merge) {
  std::vector<FillNode::ptr> rows;
  rows.emplace_back(query.PlanFill({}, ScoreMergeType::Noop));
  if (!rows.front()) {
    return {};
  }
  const TermScoring absorb{
    .recipe = builder::RecipeOf<Family>(query.Segment(), ctx),
    .merge = merge,
  };
  return builder::MakeWindowDisjunction<Family>(
    std::span<const MultiTermState::Entry>{}, &field, DocOf(field), rows,
    absorb, ctx, AllDocsScoreOf(query, Family::Scoring(ctx)));
}

template<typename Family, typename Ctx>
typename Family::Result MakeConstantWindow(const MultiTermQuery& query,
                                           const Ctx& ctx,
                                           ScoreMergeType merge) {
  auto docs = query.PlanFill({}, ScoreMergeType::Noop);
  if (!docs) {
    return {};
  }
  return Family::template MakeWindow<fill::Erased, utils::Empty, utils::Empty,
                                     utils::Empty>(
    ctx, merge, Scored{merge, AllDocsScoreOf(query, Family::Scoring(ctx))},
    std::forward_as_tuple(std::move(docs)), std::forward_as_tuple(),
    std::forward_as_tuple(), std::forward_as_tuple());
}

template<typename Family, typename Ctx>
typename Family::Result MakeConstantBitset(
  const MultiTermQuery& query, std::span<const MultiTermState::Entry> terms,
  const TermReader& field, const Ctx& ctx) {
  using Result = typename Family::Result;
  const auto* const doc = DocOf(field);
  const auto docs_count = static_cast<doc_id_t>(query.Segment().docs_count());
  if (!doc) {
    return {};
  }
  if constexpr (kProbePlan<Family>) {
    if (!TakeProbeBitset(terms, *doc, docs_count,
                         Family::Interrogations(ctx))) {
      return {};
    }
  } else if (!TakeBitset<Result>(terms, *doc, docs_count)) {
    return {};
  }
  return MakeConstantBitsetNode<Result>(
    DisjunctionBuckets(terms, &field), *doc, docs_count,
    AllDocsScoreOf(query, Family::Scoring(ctx)));
}

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const MultiTermQuery& query, const Ctx& ctx) {
  const auto& state = query.State();
  const auto& segment = query.Segment();
  const auto* const field = state.Reader();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  if constexpr (kScoredPlan<Family, Ctx>) {
    const auto* const scorer = query.Stats(Family::Scoring(ctx)).scorer;
    const auto boost = query.Boost();
    const auto merge = MergeOf(query, ctx);
    if (terms.size() == 1) {
      return Family::MakePosting(ClauseOf(terms.front(), field, scorer, boost),
                                 segment, ctx, merge);
    }
    if (IsConstantScore(scorer) && query.MergeType() == ScoreMergeType::Max) {
      if constexpr (kProbePlan<Family> || kLeadPlan<Family>) {
        if (auto folded =
              MakeConstantBitset<Family>(query, terms, *field, ctx)) {
          return folded;
        }
      } else if constexpr (kRootPlan<Family>) {
        return MakeConstantWindow<Family>(query, ctx, merge);
      } else if (merge != ScoreMergeType::Max) {
        return MakeConstantRows<Family>(query, *field, ctx, merge);
      }
    }
    if constexpr (requires { ctx.merge; }) {
      if (const auto own = query.MergeType(); own != merge) {
        auto own_ctx = ctx;
        own_ctx.merge = own;
        auto child = MakeQuery<Family>(query, own_ctx);
        if (!child) {
          return {};
        }
        return Family::WrapMerge(merge, std::move(child));
      }
    }
    const TermScoring scoring{
      .scorer = scorer,
      .boost = boost,
      .uniformity = UniformityOf(*field, scorer),
      .recipe = builder::RecipeOf<Family>(segment, ctx),
      .merge = merge,
    };
    if constexpr (kProbePlan<Family>) {
      return probe::MakeDisjunction(terms, {}, field, segment,
                                    Family::Interrogations(ctx), scoring,
                                    Family::Scoring(ctx));
    } else {
      return builder::MakeDisjunction<Family>(terms, {}, field, segment,
                                              scoring, ctx, 0);
    }
  } else {
    if (terms.size() == 1) {
      return Family::MakePosting(ClauseOf(terms.front(), field), segment, ctx,
                                 ScoreMergeType::Noop);
    }
    if constexpr (kProbePlan<Family>) {
      return probe::MakeDisjunction(terms, {}, field, segment,
                                    Family::Interrogations(ctx), NoScoring{},
                                    ScoredCtx{});
    } else {
      return builder::MakeDisjunctionOfTerms<Family>(
        terms, field, *DocOf(*field),
        static_cast<doc_id_t>(segment.docs_count()), ctx);
    }
  }
}

}  // namespace irs::detail
