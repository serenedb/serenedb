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
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/probe/node.hpp"
#include "iresearch/search/queries/term_state.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::probe {

Node::ptr MakePostingDocs(const detail::PostingClause& posting,
                          const SubReader& segment);

Node::ptr MakeAllDocs(const SubReader& segment);
Node::ptr MakeLiveDocs(const SubReader& segment);

Node::ptr MakePostingScored(const detail::PostingClause& posting,
                            const SubReader& segment,
                            const detail::ScoreRecipe& recipe);

Node::ptr MakeAllScored(const SubReader& segment, score_t score);

inline auto ScoredClauseOf(const SubReader& segment,
                           const detail::ScoredCtx& ctx,
                           const detail::ScoreRecipe& recipe) {
  return [&](const detail::PostingClause& posting, const QueryBuilder* child,
             uint64_t interrogations) -> Node::ptr {
    if (child == nullptr) {
      return MakePostingScored(posting, segment, recipe);
    }
    return child->PlanProbe(ctx, interrogations);
  };
}

template<typename Scoring>
Node::ptr MakeOptional(std::span<const detail::PostingClause> should,
                       std::span<const QueryBuilder::ptr> should_filters,
                       uint32_t min_match, const SubReader& segment,
                       uint64_t interrogations, const Scoring& scoring,
                       const detail::ScoredCtx& ctx, score_t absorbed);

template<typename Scoring>
Node::ptr MakeExclusion(const BooleanQuery& query, uint64_t interrogations,
                        const Scoring& scoring, const detail::ScoredCtx& ctx,
                        score_t absorbed);

template<typename Scoring>
Node::ptr MakeBoolean(const BooleanQuery& query, uint64_t interrogations,
                      const Scoring& scoring, const detail::ScoredCtx& ctx);

}  // namespace irs::probe
