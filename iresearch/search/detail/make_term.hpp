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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/boolean_builder.hpp"
#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/make_query.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/search/queries/term_query.hpp"

namespace irs::detail {

template<typename Family>
inline constexpr bool kProbePlan =
  std::is_same_v<typename Family::Result, ProbeNode::ptr>;

template<typename Family, typename Ctx>
typename Family::Result MakeQuery(const TermQuery& query, const Ctx& ctx) {
  if constexpr (kScoredPlan<Family, Ctx>) {
    const PostingClause posting{query.State(), query.Boost(),
                                query.Stats(Family::Scoring(ctx))};
    return Family::MakePosting(posting, query.Segment(), ctx,
                               MergeOf(query, ctx));
  } else {
    return Family::MakePosting(PostingClause{query.State()}, query.Segment(),
                               ctx, ScoreMergeType::Noop);
  }
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
