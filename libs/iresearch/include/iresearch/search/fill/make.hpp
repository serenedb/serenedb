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

#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/fill/node.hpp"
#include "iresearch/search/states/term_state.hpp"

namespace irs::fill {

using search::ScoredCtx;

Node::ptr Make(const TermQuery& query);
Node::ptr Make(const MultiTermQuery& query);
Node::ptr Make(const FixedPhraseQuery& query);
Node::ptr Make(const VariadicPhraseQuery& query);
Node::ptr Make(const NGramSimilarityQuery& query);
Node::ptr Make(const AllQuery& query);
Node::ptr Make(const WildcardNgramQuery& query);
Node::ptr Make(const ByNestedQuery& query);
inline Node::ptr Make(const KnnVectorQuery&) { return {}; }
Node::ptr Make(const RangeVectorQuery& query);
inline Node::ptr Make(const EmptyQueryBuilder&) { return {}; }
Node::ptr Make(const BooleanQuery& query);
template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query);

Node::ptr Make(const TermQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge);
Node::ptr Make(const MultiTermQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge);
Node::ptr Make(const FixedPhraseQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge);
Node::ptr Make(const VariadicPhraseQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge);
Node::ptr Make(const NGramSimilarityQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge);
Node::ptr Make(const AllQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge);
Node::ptr Make(const WildcardNgramQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge);
Node::ptr Make(const ByNestedQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge);
inline Node::ptr Make(const KnnVectorQuery&, const ScoredCtx&, ScoreMergeType) {
  return {};
}
Node::ptr Make(const RangeVectorQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge);
inline Node::ptr Make(const EmptyQueryBuilder&, const ScoredCtx&,
                      ScoreMergeType) {
  return {};
}
Node::ptr Make(const BooleanQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge);
template<typename Parser, typename Acceptor>
Node::ptr Make(const GeoQuery<Parser, Acceptor>& query, const ScoredCtx& ctx,
               ScoreMergeType merge);

Node::ptr MakeConjunctionDocs(std::span<const search::PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              const SubReader& segment);
Node::ptr MakeDisjunctionDocs(std::span<const search::PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              const SubReader& segment);
Node::ptr MakeThresholdDocs(std::span<const search::PostingClause> terms,
                            std::span<const QueryBuilder::ptr> filters,
                            const SubReader& segment, uint32_t min_match);
Node::ptr MakeRequiredDocs(std::span<const search::PostingClause> must_terms,
                           std::span<const QueryBuilder::ptr> must_filters,
                           std::span<const search::PostingClause> should_terms,
                           std::span<const QueryBuilder::ptr> should_filters,
                           uint32_t min_should_match, const SubReader& segment);
Node::ptr MakeExclusionDocs(
  std::span<const search::PostingClause> must_terms,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const search::PostingClause> should_terms,
  std::span<const QueryBuilder::ptr> should_filters, uint32_t min_should_match,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment);

Node::ptr MakeDisjunctionScored(std::span<const search::PostingClause> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                search::Terms uniformity,
                                const SubReader& segment, const ScoredCtx& ctx,
                                ScoreMergeType merge, score_t absorbed);
Node::ptr MakeThresholdScored(std::span<const search::PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              search::Terms uniformity,
                              const SubReader& segment, const ScoredCtx& ctx,
                              ScoreMergeType merge, uint32_t min_match,
                              score_t absorbed);

Node::ptr MakePostingDocs(const search::PostingClause& posting,
                          const SubReader& segment);
Node::ptr MakePostingScored(const search::PostingClause& posting,
                            const SubReader& segment, const ScoredCtx& ctx,
                            ScoreMergeType merge);

Node::ptr MakeSinglePostingDocs(const search::PostingClause& posting);
Node::ptr MakeSinglePostingScored(const search::PostingClause& posting,
                                  const SubReader& segment,
                                  const ScoredCtx& ctx, ScoreMergeType merge);

Node::ptr MakeAllDocs(const SubReader& segment);
Node::ptr MakeAllScored(const SubReader& segment, const ScoredCtx& ctx,
                        const search::StatsRecord& record, ScoreMergeType merge,
                        score_t boost);
Node::ptr MakeAllScored(const SubReader& segment, ScoreMergeType merge,
                        score_t score);

Node::ptr MakeWildcardNgramDocs(const WildcardNgramQuery& query);
Node::ptr MakeWildcardNgramScored(const WildcardNgramQuery& query,
                                  const ScoredCtx& ctx, ScoreMergeType merge);

}  // namespace irs::fill
