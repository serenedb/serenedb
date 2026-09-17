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
#include "iresearch/search/lead/node.hpp"
#include "iresearch/search/queries/term_state.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"
#include "iresearch/search/scorers/score_args.hpp"

namespace irs::lead {

Node::ptr Make(const TermQuery& query, DocRange range);
Node::ptr Make(const MultiTermQuery& query, DocRange range);
Node::ptr Make(const FixedPhraseQuery& query, DocRange range);
Node::ptr Make(const VariadicPhraseQuery& query, DocRange range);
Node::ptr Make(const NGramSimilarityQuery& query, DocRange range);
Node::ptr Make(const AllQuery& query, DocRange range);
Node::ptr Make(const WildcardNGramQuery& query, DocRange range);
Node::ptr Make(const ByNestedQuery& query, DocRange range);
Node::ptr Make(const RangeVectorQuery& query, DocRange range);
inline Node::ptr Make(const HnswQuery&, DocRange) { return {}; }
inline Node::ptr Make(const KnnVectorQuery&, DocRange) { return {}; }
inline Node::ptr Make(const EmptyQueryBuilder&, DocRange) { return {}; }
Node::ptr Make(const BooleanQuery& query, DocRange range);
template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query, DocRange range);

Node::ptr Make(const TermQuery& query, const detail::ScoredCtx& ctx);
Node::ptr Make(const MultiTermQuery& query, const detail::ScoredCtx& ctx);
Node::ptr Make(const FixedPhraseQuery& query, const detail::ScoredCtx& ctx);
Node::ptr Make(const VariadicPhraseQuery& query, const detail::ScoredCtx& ctx);
Node::ptr Make(const NGramSimilarityQuery& query, const detail::ScoredCtx& ctx);
Node::ptr Make(const AllQuery& query, const detail::ScoredCtx& ctx);
Node::ptr Make(const WildcardNGramQuery& query, const detail::ScoredCtx& ctx);
Node::ptr Make(const ByNestedQuery& query, const detail::ScoredCtx& ctx);
Node::ptr Make(const HnswQuery& query, const detail::ScoredCtx& ctx);
Node::ptr Make(const KnnVectorQuery& query, const detail::ScoredCtx& ctx);
Node::ptr Make(const RangeVectorQuery& query, const detail::ScoredCtx& ctx);
inline Node::ptr Make(const EmptyQueryBuilder&, const detail::ScoredCtx&) {
  return {};
}
Node::ptr Make(const BooleanQuery& query, const detail::ScoredCtx& ctx);
template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query,
               const detail::ScoredCtx& ctx);

Node::ptr MakePostingDocs(const detail::PostingClause& posting,
                          const SubReader& segment, DocRange range);
Node::ptr MakePostingScored(const detail::PostingClause& posting,
                            const SubReader& segment,
                            const detail::ScoreRecipe& recipe, DocRange range);

Node::ptr MakeAllDocs(const SubReader& segment, DocRange range);
Node::ptr MakeAllScored(const SubReader& segment, score_t score,
                        DocRange range);
Node::ptr MakeAllScored(const SubReader& segment, const detail::ScoreArgs& args,
                        DocRange range);

Node::ptr MakeSparseConjunctionScored(
  std::span<const detail::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  const detail::ScoredCtx& ctx, ScoreMergeType merge, score_t absorbed);

Node::ptr MakeFixedPhraseDocs(const FixedPhraseQuery& query, DocRange range);
Node::ptr MakeFixedPhraseIntervalsDocs(const FixedPhraseQuery& query,
                                       DocRange range);
Node::ptr MakeFixedPhraseSlopDocs(const FixedPhraseQuery& query,
                                  DocRange range);
Node::ptr MakeVariadicPhraseDocs(const VariadicPhraseQuery& query,
                                 DocRange range);
Node::ptr MakeVariadicPhraseIntervalsDocs(const VariadicPhraseQuery& query,
                                          DocRange range);
Node::ptr MakeVariadicPhraseSlopDocs(const VariadicPhraseQuery& query,
                                     DocRange range);

Node::ptr MakeFixedPhraseScored(const FixedPhraseQuery& query,
                                const detail::ScoreArgs& args, DocRange range);
Node::ptr MakeFixedPhraseIntervalsScored(const FixedPhraseQuery& query,
                                         const detail::ScoreArgs& args,
                                         DocRange range);
Node::ptr MakeFixedPhraseSlopScored(const FixedPhraseQuery& query,
                                    const detail::ScoreArgs& args,
                                    DocRange range);
Node::ptr MakeVariadicPhraseScored(const VariadicPhraseQuery& query,
                                   const detail::ScoreArgs& args,
                                   DocRange range);
Node::ptr MakeVariadicPhraseIntervalsScored(const VariadicPhraseQuery& query,
                                            const detail::ScoreArgs& args,
                                            DocRange range);
Node::ptr MakeVariadicPhraseSlopScored(const VariadicPhraseQuery& query,
                                       const detail::ScoreArgs& args,
                                       DocRange range);

Node::ptr MakeNGramDocs(const NGramSimilarityQuery& query, DocRange range);
Node::ptr MakeNGramAllDocs(const NGramSimilarityQuery& query, DocRange range);
Node::ptr MakeNGramScored(const NGramSimilarityQuery& query,
                          const detail::ScoreArgs& args, DocRange range);
Node::ptr MakeNGramAllScored(const NGramSimilarityQuery& query,
                             const detail::ScoreArgs& args, DocRange range);

Node::ptr MakeWildcardNGramDocs(const WildcardNGramQuery& query,
                                DocRange range);
Node::ptr MakeWildcardNGramScored(const WildcardNGramQuery& query,
                                  score_t score, DocRange range);

Node::ptr MakeRequiredDocs(std::span<const detail::PostingClause> must,
                           std::span<const QueryBuilder::ptr> must_filters,
                           std::span<const detail::PostingClause> should,
                           std::span<const QueryBuilder::ptr> should_filters,
                           uint32_t min_should_match, const SubReader& segment,
                           DocRange range);
Node::ptr MakeRequiredScored(std::span<const detail::PostingClause> must,
                             std::span<const QueryBuilder::ptr> must_filters,
                             std::span<const detail::PostingClause> should,
                             std::span<const QueryBuilder::ptr> should_filters,
                             detail::Terms should_uniformity,
                             uint32_t min_should_match,
                             const SubReader& segment,
                             const detail::ScoredCtx& ctx, ScoreMergeType merge,
                             score_t absorbed);

}  // namespace irs::lead
