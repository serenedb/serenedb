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

Node::ptr Make(const TermQuery& query, uint64_t interrogations);
Node::ptr Make(const MultiTermQuery& query, uint64_t interrogations);
Node::ptr Make(const FixedPhraseQuery& query, uint64_t interrogations);
Node::ptr Make(const VariadicPhraseQuery& query, uint64_t interrogations);
Node::ptr Make(const NGramSimilarityQuery& query, uint64_t interrogations);
Node::ptr Make(const AllQuery& query, uint64_t interrogations);
Node::ptr Make(const WildcardNGramQuery& query, uint64_t interrogations);
Node::ptr Make(const ApproxQuery& query, uint64_t interrogations);
Node::ptr Make(const TokenPhraseQuery& query, uint64_t interrogations);
Node::ptr Make(const ByNestedQuery& query, uint64_t interrogations);
inline Node::ptr Make(const HnswQuery&, uint64_t) { return {}; }
inline Node::ptr Make(const KnnVectorQuery&, uint64_t) { return {}; }
inline Node::ptr Make(const ExactVectorQuery&, uint64_t) { return {}; }
Node::ptr Make(const RangeVectorQuery& query, uint64_t interrogations);
inline Node::ptr Make(const EmptyQueryBuilder&, uint64_t) { return {}; }
Node::ptr Make(const BooleanQuery& query, uint64_t interrogations);
template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query,
               uint64_t interrogations);

Node::ptr Make(const TermQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations);
Node::ptr Make(const MultiTermQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations);
Node::ptr Make(const FixedPhraseQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations);
Node::ptr Make(const VariadicPhraseQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations);
Node::ptr Make(const NGramSimilarityQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations);
Node::ptr Make(const AllQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations);
Node::ptr Make(const WildcardNGramQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations);
Node::ptr Make(const ApproxQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations);
Node::ptr Make(const TokenPhraseQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations);
Node::ptr Make(const ByNestedQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations);

inline Node::ptr Make(const HnswQuery&, const detail::ScoredCtx&, uint64_t) {
  return {};
}
inline Node::ptr Make(const KnnVectorQuery&, const detail::ScoredCtx&,
                      uint64_t) {
  return {};
}
inline Node::ptr Make(const ExactVectorQuery&, const detail::ScoredCtx&,
                      uint64_t) {
  return {};
}
Node::ptr Make(const RangeVectorQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations);
inline Node::ptr Make(const EmptyQueryBuilder&, const detail::ScoredCtx&,
                      uint64_t) {
  return {};
}
Node::ptr Make(const BooleanQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations);
template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query,
               const detail::ScoredCtx& ctx, uint64_t interrogations);

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
