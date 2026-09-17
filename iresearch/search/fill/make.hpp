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

#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/fill/node.hpp"
#include "iresearch/search/queries/term_state.hpp"

namespace irs::fill {

Node::ptr Make(const TermQuery& query, DocRange range);
Node::ptr Make(const MultiTermQuery& query, DocRange range);
Node::ptr Make(const FixedPhraseQuery& query, DocRange range);
Node::ptr Make(const VariadicPhraseQuery& query, DocRange range);
Node::ptr Make(const NGramSimilarityQuery& query, DocRange range);
Node::ptr Make(const AllQuery& query, DocRange range);
Node::ptr Make(const WildcardNGramQuery& query, DocRange range);
Node::ptr Make(const ByNestedQuery& query, DocRange range);
inline Node::ptr Make(const HnswQuery&, DocRange) { return {}; }
inline Node::ptr Make(const KnnVectorQuery&, DocRange) { return {}; }
Node::ptr Make(const RangeVectorQuery& query, DocRange range);
inline Node::ptr Make(const EmptyQueryBuilder&, DocRange) { return {}; }
Node::ptr Make(const BooleanQuery& query, DocRange range);
template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query, DocRange range);

Node::ptr Make(const TermQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge);
Node::ptr Make(const MultiTermQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge);
Node::ptr Make(const FixedPhraseQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge);
Node::ptr Make(const VariadicPhraseQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge);
Node::ptr Make(const NGramSimilarityQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge);
Node::ptr Make(const AllQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge);
Node::ptr Make(const WildcardNGramQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge);
Node::ptr Make(const ByNestedQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge);
inline Node::ptr Make(const HnswQuery&, const detail::ScoredCtx&,
                      ScoreMergeType) {
  return {};
}
inline Node::ptr Make(const KnnVectorQuery&, const detail::ScoredCtx&,
                      ScoreMergeType) {
  return {};
}
Node::ptr Make(const RangeVectorQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge);
inline Node::ptr Make(const EmptyQueryBuilder&, const detail::ScoredCtx&,
                      ScoreMergeType) {
  return {};
}
Node::ptr Make(const BooleanQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge);
template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query,
               const detail::ScoredCtx& ctx, ScoreMergeType merge);

Node::ptr MakePostingDocs(const detail::PostingClause& posting,
                          const SubReader& segment, DocRange range);
Node::ptr MakePostingScored(const detail::PostingClause& posting,
                            const SubReader& segment,
                            const detail::ScoredCtx& ctx, ScoreMergeType merge);

Node::ptr MakeSinglePostingDocs(const detail::PostingClause& posting,
                                DocRange range);
Node::ptr MakeSinglePostingScored(const detail::PostingClause& posting,
                                  const SubReader& segment,
                                  const detail::ScoredCtx& ctx,
                                  ScoreMergeType merge);

Node::ptr MakeAllDocs(const SubReader& segment, DocRange range);
Node::ptr MakeAllScored(const SubReader& segment, const detail::ScoredCtx& ctx,
                        const detail::StatsRecord& record, ScoreMergeType merge,
                        score_t boost);
Node::ptr MakeAllScored(const SubReader& segment, ScoreMergeType merge,
                        score_t score, DocRange range);

Node::ptr MakeWildcardNGramDocs(const WildcardNGramQuery& query,
                                DocRange range);
Node::ptr MakeWildcardNGramScored(const WildcardNGramQuery& query,
                                  const detail::ScoredCtx& ctx,
                                  ScoreMergeType merge);

}  // namespace irs::fill
