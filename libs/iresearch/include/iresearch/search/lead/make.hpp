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

Node::ptr Make(const TermQuery& query);
Node::ptr Make(const MultiTermQuery& query);
Node::ptr Make(const FixedPhraseQuery& query);
Node::ptr Make(const VariadicPhraseQuery& query);
Node::ptr Make(const NGramSimilarityQuery& query);
Node::ptr Make(const AllQuery& query);
Node::ptr Make(const WildcardNGramQuery& query);
Node::ptr Make(const ByNestedQuery& query);
Node::ptr Make(const RangeVectorQuery& query);
inline Node::ptr Make(const HnswQuery&) { return {}; }
inline Node::ptr Make(const KnnVectorQuery&) { return {}; }
inline Node::ptr Make(const EmptyQueryBuilder&) { return {}; }
Node::ptr Make(const BooleanQuery& query);
template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query);

Node::ptr Make(const TermQuery& query, const search::ScoredCtx& ctx);
Node::ptr Make(const MultiTermQuery& query, const search::ScoredCtx& ctx);
Node::ptr Make(const FixedPhraseQuery& query, const search::ScoredCtx& ctx);
Node::ptr Make(const VariadicPhraseQuery& query, const search::ScoredCtx& ctx);
Node::ptr Make(const NGramSimilarityQuery& query, const search::ScoredCtx& ctx);
Node::ptr Make(const AllQuery& query, const search::ScoredCtx& ctx);
Node::ptr Make(const WildcardNGramQuery& query, const search::ScoredCtx& ctx);
Node::ptr Make(const ByNestedQuery& query, const search::ScoredCtx& ctx);
Node::ptr Make(const HnswQuery& query, const search::ScoredCtx& ctx);
Node::ptr Make(const KnnVectorQuery& query, const search::ScoredCtx& ctx);
Node::ptr Make(const RangeVectorQuery& query, const search::ScoredCtx& ctx);
inline Node::ptr Make(const EmptyQueryBuilder&, const search::ScoredCtx&) { return {}; }
Node::ptr Make(const BooleanQuery& query, const search::ScoredCtx& ctx);
template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query, const search::ScoredCtx& ctx);

Node::ptr MakePostingDocs(const search::PostingClause& posting,
                          const SubReader& segment);
Node::ptr MakePostingScored(const search::PostingClause& posting,
                            const SubReader& segment,
                            const search::ScoreRecipe& recipe);

Node::ptr MakeAllDocs(const SubReader& segment);
Node::ptr MakeAllScored(const SubReader& segment, score_t score);
Node::ptr MakeAllScored(const SubReader& segment, const search::ScoreArgs& args);

Node::ptr MakeSparseConjunctionScored(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  const search::ScoredCtx& ctx, ScoreMergeType merge, score_t absorbed);

Node::ptr MakeFixedPhraseDocs(const FixedPhraseQuery& query);
Node::ptr MakeFixedPhraseIntervalsDocs(const FixedPhraseQuery& query);
Node::ptr MakeFixedPhraseSlopDocs(const FixedPhraseQuery& query);
Node::ptr MakeVariadicPhraseDocs(const VariadicPhraseQuery& query);
Node::ptr MakeVariadicPhraseIntervalsDocs(const VariadicPhraseQuery& query);
Node::ptr MakeVariadicPhraseSlopDocs(const VariadicPhraseQuery& query);

Node::ptr MakeFixedPhraseScored(const FixedPhraseQuery& query,
                                const search::ScoreArgs& args);
Node::ptr MakeFixedPhraseIntervalsScored(const FixedPhraseQuery& query,
                                         const search::ScoreArgs& args);
Node::ptr MakeFixedPhraseSlopScored(const FixedPhraseQuery& query,
                                    const search::ScoreArgs& args);
Node::ptr MakeVariadicPhraseScored(const VariadicPhraseQuery& query,
                                   const search::ScoreArgs& args);
Node::ptr MakeVariadicPhraseIntervalsScored(const VariadicPhraseQuery& query,
                                            const search::ScoreArgs& args);
Node::ptr MakeVariadicPhraseSlopScored(const VariadicPhraseQuery& query,
                                       const search::ScoreArgs& args);

Node::ptr MakeNGramDocs(const NGramSimilarityQuery& query);
Node::ptr MakeNGramAllDocs(const NGramSimilarityQuery& query);
Node::ptr MakeNGramScored(const NGramSimilarityQuery& query,
                          const search::ScoreArgs& args);
Node::ptr MakeNGramAllScored(const NGramSimilarityQuery& query,
                             const search::ScoreArgs& args);

Node::ptr MakeWildcardNGramDocs(const WildcardNGramQuery& query);
Node::ptr MakeWildcardNGramScored(const WildcardNGramQuery& query,
                                  score_t score);

Node::ptr MakeRequiredDocs(std::span<const search::PostingClause> must,
                           std::span<const QueryBuilder::ptr> must_filters,
                           std::span<const search::PostingClause> should,
                           std::span<const QueryBuilder::ptr> should_filters,
                           uint32_t min_should_match, const SubReader& segment);
Node::ptr MakeRequiredScored(std::span<const search::PostingClause> must,
                             std::span<const QueryBuilder::ptr> must_filters,
                             std::span<const search::PostingClause> should,
                             std::span<const QueryBuilder::ptr> should_filters,
                             search::Terms should_uniformity,
                             uint32_t min_should_match,
                             const SubReader& segment, const search::ScoredCtx& ctx,
                             ScoreMergeType merge, score_t absorbed);

}  // namespace irs::lead
