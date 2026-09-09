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

#include <span>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/plan.hpp"

namespace irs::lead {

Node::ptr MakeRequiredScored(std::span<const PostingClause> must,
                             std::span<const QueryBuilder::ptr> must_filters,
                             std::span<const PostingClause> should,
                             std::span<const QueryBuilder::ptr> should_filters,
                             search::Terms should_uniformity,
                             uint32_t min_should_match,
                             const SubReader& segment, const ScoredCtx& ctx,
                             ScoreMergeType merge, score_t absorbed) {
  if (!(should.empty() && should_filters.empty()) && min_should_match == 0) {
    return MakeSparseBoostScored(must, must_filters, should, should_filters,
                                 should_uniformity, segment, ctx, merge,
                                 absorbed);
  }
  if (must.empty() && must_filters.empty()) {
    if (min_should_match == 0) {
      return MakeAllScored(segment, absorbed);
    }
    if (min_should_match == 1) {
      return MakeWindowDisjunctionScored(should, should_filters,
                                         should_uniformity, segment, ctx, merge,
                                         absorbed);
    }
    return MakeWindowThresholdScored(should, should_filters, should_uniformity,
                                     segment, ctx, merge, min_should_match,
                                     absorbed);
  }
  if (min_should_match != 0) {
    return MakeSparseConjunctionWithScored(
      must, must_filters, should, should_filters, should_uniformity,
      min_should_match, segment, ctx, merge, absorbed);
  }
  return MakeSparseConjunctionScored(must, must_filters, segment, ctx, merge,
                                     absorbed);
}

Node::ptr Make(const BooleanQuery& query, const ScoredCtx& ctx) {
  const auto& segment = query.Segment();
  const auto merge = query.MergeType();
  const auto absorbed = query.Absorbed();
  const auto excludes = query.Terms(Occur::MustNot);
  const auto exclude_filters = query.Queries(Occur::MustNot);
  const auto must = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto should = query.Terms(Occur::Should);
  const auto should_filters = query.Queries(Occur::Should);
  const auto should_uniformity = query.Uniformity(Occur::Should);
  const auto min_should_match = query.MinShouldMatch();
  if (!excludes.empty() || !exclude_filters.empty()) {
    return MakeSparseExclusionScored(must, must_filters, should, should_filters,
                                     should_uniformity, min_should_match,
                                     excludes, exclude_filters, segment, ctx,
                                     merge, absorbed);
  }
  return MakeRequiredScored(must, must_filters, should, should_filters,
                            should_uniformity, min_should_match, segment, ctx,
                            merge, absorbed);
}

}  // namespace irs::lead
