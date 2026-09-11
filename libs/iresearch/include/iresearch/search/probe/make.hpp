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
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/probe/node.hpp"
#include "iresearch/search/states/term_state.hpp"

namespace irs::probe {

Node::ptr Make(const TermQuery& query, uint64_t interrogations);
Node::ptr Make(const MultiTermQuery& query, uint64_t interrogations);
Node::ptr Make(const FixedPhraseQuery& query, uint64_t interrogations);
Node::ptr Make(const VariadicPhraseQuery& query, uint64_t interrogations);
Node::ptr Make(const NGramSimilarityQuery& query, uint64_t interrogations);
Node::ptr Make(const AllQuery& query, uint64_t interrogations);
Node::ptr Make(const WildcardNGramQuery& query, uint64_t interrogations);
Node::ptr Make(const ByNestedQuery& query, uint64_t interrogations);
inline Node::ptr Make(const HnswQuery&, uint64_t) { return {}; }
inline Node::ptr Make(const KnnVectorQuery&, uint64_t) { return {}; }
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
Node::ptr Make(const ByNestedQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations);

inline Node::ptr Make(const HnswQuery&, const detail::ScoredCtx&, uint64_t) {
  return {};
}
inline Node::ptr Make(const KnnVectorQuery&, const detail::ScoredCtx&, uint64_t) {
  return {};
}
Node::ptr Make(const RangeVectorQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations);
inline Node::ptr Make(const EmptyQueryBuilder&, const detail::ScoredCtx&, uint64_t) {
  return {};
}
Node::ptr Make(const BooleanQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations);
template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations);

Node::ptr MakePostingDocs(const detail::PostingClause& posting,
                          const SubReader& segment);

Node::ptr MakeAllDocs(const SubReader& segment);

Node::ptr MakeSparseConjunctionDocs(
  std::span<const detail::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  uint64_t interrogations);

Node::ptr MakeSparseConjunctionWithDocs(
  std::span<const detail::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  uint64_t interrogations, Node::ptr other);

Node::ptr MakeSparseThresholdDocs(std::span<const detail::PostingClause> terms,
                                  std::span<const QueryBuilder::ptr> filters,
                                  const SubReader& segment, uint32_t min_match,
                                  uint64_t interrogations);

Node::ptr MakeSparseExclusionDocs(
  std::span<const detail::PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const detail::PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters, uint32_t min_should_match,
  std::span<const detail::PostingClause> exclude,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t interrogations);

Node::ptr MakeFixedPhraseDocs(const FixedPhraseQuery& query);
Node::ptr MakeFixedPhraseIntervalsDocs(const FixedPhraseQuery& query);
Node::ptr MakeFixedPhraseSlopDocs(const FixedPhraseQuery& query);

Node::ptr MakeVariadicPhraseDocs(const VariadicPhraseQuery& query);
Node::ptr MakeVariadicPhraseIntervalsDocs(const VariadicPhraseQuery& query);
Node::ptr MakeVariadicPhraseSlopDocs(const VariadicPhraseQuery& query);

Node::ptr MakeNGramDocs(const NGramSimilarityQuery& query);
Node::ptr MakeNGramAllDocs(const NGramSimilarityQuery& query);

Node::ptr MakeWildcardNGramDocs(const WildcardNGramQuery& query,
                                uint64_t interrogations);

Node::ptr MakeRequiredDocs(std::span<const detail::PostingClause> must,
                           std::span<const QueryBuilder::ptr> must_filters,
                           std::span<const detail::PostingClause> should,
                           std::span<const QueryBuilder::ptr> should_filters,
                           uint32_t min_should_match, const SubReader& segment,
                           uint64_t interrogations);

Node::ptr MakePostingScored(const detail::PostingClause& posting,
                            const SubReader& segment,
                            const detail::ScoreRecipe& recipe);

Node::ptr MakeSinglePostingScored(const detail::PostingClause& posting,
                                  const SubReader& segment,
                                  const detail::ScoreRecipe& recipe);

Node::ptr MakeAllScored(const SubReader& segment, score_t score);

inline auto ScoredClauseOf(const SubReader& segment, const detail::ScoredCtx& ctx,
                           const detail::ScoreRecipe& recipe) {
  return [&](const detail::PostingClause& posting, const QueryBuilder* child,
             uint64_t interrogations) -> Node::ptr {
    if (child == nullptr) {
      return MakePostingScored(posting, segment, recipe);
    }
    return child->PlanProbe(ctx, interrogations);
  };
}

Node::ptr MakeSparseConjunctionScored(
  std::span<const detail::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, detail::Terms uniformity,
  const SubReader& segment, const detail::ScoreRecipe& recipe, ScoreMergeType merge,
  uint64_t interrogations, const detail::ScoredCtx& ctx, score_t absorbed = 0);

Node::ptr MakeRequiredScored(
  std::span<const detail::PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  detail::Terms must_uniformity, std::span<const detail::PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters,
  detail::Terms should_uniformity, uint32_t min_should_match,
  const SubReader& segment, const detail::ScoreRecipe& recipe, ScoreMergeType merge,
  uint64_t interrogations, const detail::ScoredCtx& ctx, score_t absorbed = 0);

Node::ptr MakeSparseThresholdScored(
  std::span<const detail::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, detail::Terms uniformity,
  const SubReader& segment, const detail::ScoreRecipe& recipe, ScoreMergeType merge,
  uint32_t min_match, uint64_t interrogations, const detail::ScoredCtx& ctx,
  score_t absorbed = 0);

Node::ptr MakeSparseExclusionScored(
  std::span<const detail::PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  detail::Terms must_uniformity, std::span<const detail::PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters,
  detail::Terms should_uniformity, uint32_t min_should_match,
  std::span<const detail::PostingClause> exclude,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const detail::ScoreRecipe& recipe, ScoreMergeType merge, uint64_t interrogations,
  const detail::ScoredCtx& ctx, score_t absorbed = 0);

Node::ptr MakeSparseBoostScored(
  std::span<const detail::PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  detail::Terms must_uniformity, std::span<const detail::PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters,
  detail::Terms should_uniformity, const SubReader& segment,
  const detail::ScoreRecipe& recipe, ScoreMergeType merge, uint64_t interrogations,
  const detail::ScoredCtx& ctx, score_t absorbed = 0);

Node::ptr MakeFixedPhraseScored(const FixedPhraseQuery& query,
                                const detail::ScoreArgs& args);
Node::ptr MakeFixedPhraseIntervalsScored(const FixedPhraseQuery& query,
                                         const detail::ScoreArgs& args);
Node::ptr MakeFixedPhraseSlopScored(const FixedPhraseQuery& query,
                                    const detail::ScoreArgs& args);

Node::ptr MakeVariadicPhraseScored(const VariadicPhraseQuery& query,
                                   const detail::ScoreArgs& args);
Node::ptr MakeVariadicPhraseIntervalsScored(const VariadicPhraseQuery& query,
                                            const detail::ScoreArgs& args);
Node::ptr MakeVariadicPhraseSlopScored(const VariadicPhraseQuery& query,
                                       const detail::ScoreArgs& args);

Node::ptr MakeNGramScored(const NGramSimilarityQuery& query,
                          const detail::ScoreArgs& args);
Node::ptr MakeNGramAllScored(const NGramSimilarityQuery& query,
                             const detail::ScoreArgs& args);

Node::ptr MakeWildcardNGramScored(const WildcardNGramQuery& query,
                                  score_t score, uint64_t interrogations);

}  // namespace irs::probe
