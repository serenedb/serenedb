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
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/lead/node.hpp"
#include "iresearch/search/states/term_state.hpp"

namespace irs::lead {

using search::PostingClause;
using search::ProbeNode;
using search::ScoreArgs;
using search::ScoredCtx;
using search::ScoreRecipe;

Node::ptr Make(const TermQuery& query);
Node::ptr Make(const MultiTermQuery& query);
Node::ptr Make(const FixedPhraseQuery& query);
Node::ptr Make(const VariadicPhraseQuery& query);
Node::ptr Make(const NGramSimilarityQuery& query);
Node::ptr Make(const AllQuery& query);
Node::ptr Make(const WildcardNGramQuery& query);
Node::ptr Make(const ByNestedQuery& query);
Node::ptr Make(const RangeVectorQuery& query);
inline Node::ptr Make(const KnnVectorQuery&) { return {}; }
inline Node::ptr Make(const EmptyQueryBuilder&) { return {}; }
Node::ptr Make(const BooleanQuery& query);
template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query);

Node::ptr Make(const TermQuery& query, const ScoredCtx& ctx);
Node::ptr Make(const MultiTermQuery& query, const ScoredCtx& ctx);
Node::ptr Make(const FixedPhraseQuery& query, const ScoredCtx& ctx);
Node::ptr Make(const VariadicPhraseQuery& query, const ScoredCtx& ctx);
Node::ptr Make(const NGramSimilarityQuery& query, const ScoredCtx& ctx);
Node::ptr Make(const AllQuery& query, const ScoredCtx& ctx);
Node::ptr Make(const WildcardNGramQuery& query, const ScoredCtx& ctx);
Node::ptr Make(const ByNestedQuery& query, const ScoredCtx& ctx);
Node::ptr Make(const KnnVectorQuery& query, const ScoredCtx& ctx);
Node::ptr Make(const RangeVectorQuery& query, const ScoredCtx& ctx);
inline Node::ptr Make(const EmptyQueryBuilder&, const ScoredCtx&) { return {}; }
Node::ptr Make(const BooleanQuery& query, const ScoredCtx& ctx);
template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query, const ScoredCtx& ctx);

Node::ptr MakePostingDocs(const PostingClause& posting,
                          const SubReader& segment);
Node::ptr MakePostingScored(const PostingClause& posting,
                            const SubReader& segment,
                            const ScoreRecipe& recipe);

Node::ptr MakeAllDocs(const SubReader& segment);
Node::ptr MakeAllScored(const SubReader& segment, score_t score);
Node::ptr MakeAllScored(const SubReader& segment, const ScoreArgs& args);

Node::ptr MakeBitsetDisjunctionDocs(std::span<const PostingClause> terms,
                                    std::span<const QueryBuilder::ptr> filters,
                                    const SubReader& segment);

Node::ptr MakeWindowDisjunctionDocs(std::span<const PostingClause> terms,
                                    std::span<const QueryBuilder::ptr> filters,
                                    const SubReader& segment);
Node::ptr MakeWindowDisjunctionScored(
  std::span<const PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, search::Terms uniformity,
  const SubReader& segment, const ScoredCtx& ctx, ScoreMergeType merge,
  score_t absorbed);

Node::ptr MakeBitsetConjunctionDocs(std::span<const PostingClause> terms,
                                    std::span<const QueryBuilder::ptr> filters,
                                    const SubReader& segment);
Node::ptr MakeWindowConjunctionDocs(std::span<const PostingClause> terms,
                                    std::span<const QueryBuilder::ptr> filters,
                                    const SubReader& segment);
Node::ptr MakeSparseConjunctionDocs(std::span<const PostingClause> terms,
                                    std::span<const QueryBuilder::ptr> filters,
                                    const SubReader& segment);

Node::ptr MakeSparseConjunctionWithDocs(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters, const SubReader& segment,
  ProbeNode::ptr other);

Node::ptr MakeSparseConjunctionScored(
  std::span<const PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  const ScoredCtx& ctx, ScoreMergeType merge, score_t absorbed);
Node::ptr MakeSparseConjunctionWithScored(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters,
  search::Terms should_uniformity, uint32_t min_should_match,
  const SubReader& segment, const ScoredCtx& ctx, ScoreMergeType merge,
  score_t absorbed);

Node::ptr MakeBitsetExclusionDocs(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters, uint32_t min_should_match,
  std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment);
Node::ptr MakeWindowExclusionDocs(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters, uint32_t min_should_match,
  std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment);
Node::ptr MakeSparseExclusionDocs(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters, uint32_t min_should_match,
  std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment);
Node::ptr MakeSparseExclusionScored(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters,
  search::Terms should_uniformity, uint32_t min_should_match,
  std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const ScoredCtx& ctx, ScoreMergeType merge, score_t absorbed);

Node::ptr MakeBitsThresholdDocs(std::span<const PostingClause> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                const SubReader& segment, uint32_t min_match);
Node::ptr MakeCountThresholdDocs(std::span<const PostingClause> terms,
                                 std::span<const QueryBuilder::ptr> filters,
                                 const SubReader& segment, uint32_t min_match);
Node::ptr MakeBitsThresholdScored(std::span<const PostingClause> terms,
                                  std::span<const QueryBuilder::ptr> filters,
                                  search::Terms uniformity,
                                  const SubReader& segment,
                                  const ScoredCtx& ctx, ScoreMergeType merge,
                                  uint32_t min_match, score_t absorbed);
Node::ptr MakeCountThresholdScored(std::span<const PostingClause> terms,
                                   std::span<const QueryBuilder::ptr> filters,
                                   search::Terms uniformity,
                                   const SubReader& segment,
                                   const ScoredCtx& ctx, ScoreMergeType merge,
                                   uint32_t min_match, score_t absorbed);

Node::ptr MakeSparseBoostScored(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters,
  search::Terms should_uniformity, const SubReader& segment,
  const ScoredCtx& ctx, ScoreMergeType merge, score_t absorbed);

Node::ptr MakeFixedPhraseDocs(const FixedPhraseQuery& query);
Node::ptr MakeFixedPhraseIntervalsDocs(const FixedPhraseQuery& query);
Node::ptr MakeFixedPhraseSlopDocs(const FixedPhraseQuery& query);
Node::ptr MakeVariadicPhraseDocs(const VariadicPhraseQuery& query);
Node::ptr MakeVariadicPhraseIntervalsDocs(const VariadicPhraseQuery& query);
Node::ptr MakeVariadicPhraseSlopDocs(const VariadicPhraseQuery& query);

Node::ptr MakeFixedPhraseScored(const FixedPhraseQuery& query,
                                const ScoreArgs& args);
Node::ptr MakeFixedPhraseIntervalsScored(const FixedPhraseQuery& query,
                                         const ScoreArgs& args);
Node::ptr MakeFixedPhraseSlopScored(const FixedPhraseQuery& query,
                                    const ScoreArgs& args);
Node::ptr MakeVariadicPhraseScored(const VariadicPhraseQuery& query,
                                   const ScoreArgs& args);
Node::ptr MakeVariadicPhraseIntervalsScored(const VariadicPhraseQuery& query,
                                            const ScoreArgs& args);
Node::ptr MakeVariadicPhraseSlopScored(const VariadicPhraseQuery& query,
                                       const ScoreArgs& args);

Node::ptr MakeNGramDocs(const NGramSimilarityQuery& query);
Node::ptr MakeNGramAllDocs(const NGramSimilarityQuery& query);
Node::ptr MakeNGramScored(const NGramSimilarityQuery& query,
                          const ScoreArgs& args);
Node::ptr MakeNGramAllScored(const NGramSimilarityQuery& query,
                             const ScoreArgs& args);

Node::ptr MakeWildcardNGramDocs(const WildcardNGramQuery& query);
Node::ptr MakeWildcardNGramScored(const WildcardNGramQuery& query,
                                  score_t score);

Node::ptr MakeConjunctionDocs(std::span<const PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              const SubReader& segment);
Node::ptr MakeDisjunctionDocs(std::span<const PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              const SubReader& segment);
Node::ptr MakeThresholdDocs(std::span<const PostingClause> terms,
                            std::span<const QueryBuilder::ptr> filters,
                            const SubReader& segment, uint32_t min_match);
Node::ptr MakeRequiredDocs(std::span<const PostingClause> must,
                           std::span<const QueryBuilder::ptr> must_filters,
                           std::span<const PostingClause> should,
                           std::span<const QueryBuilder::ptr> should_filters,
                           uint32_t min_should_match, const SubReader& segment);
Node::ptr MakeRequiredScored(std::span<const PostingClause> must,
                             std::span<const QueryBuilder::ptr> must_filters,
                             std::span<const PostingClause> should,
                             std::span<const QueryBuilder::ptr> should_filters,
                             search::Terms should_uniformity,
                             uint32_t min_should_match,
                             const SubReader& segment, const ScoredCtx& ctx,
                             ScoreMergeType merge, score_t absorbed);
Node::ptr MakeExclusionDocs(std::span<const PostingClause> must,
                            std::span<const QueryBuilder::ptr> must_filters,
                            std::span<const PostingClause> should,
                            std::span<const QueryBuilder::ptr> should_filters,
                            uint32_t min_should_match,
                            std::span<const PostingClause> excludes,
                            std::span<const QueryBuilder::ptr> exclude_filters,
                            const SubReader& segment);

}  // namespace irs::lead
