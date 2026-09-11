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
#include "iresearch/search/common/scored_node_builder.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/make_boolean.hpp"

namespace irs::lead {

Node::ptr MakeSparseConjunctionScored(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, const SubReader& segment,
  const search::ScoredCtx& ctx, ScoreMergeType merge, score_t absorbed) {
  return search::builder::MakeNodeConjunction<ScoredApi>(
    terms, filters, segment, ctx, merge, absorbed);
}

Node::ptr MakeRequiredScored(std::span<const search::PostingClause> must,
                             std::span<const QueryBuilder::ptr> must_filters,
                             std::span<const search::PostingClause> should,
                             std::span<const QueryBuilder::ptr> should_filters,
                             search::Terms should_uniformity,
                             uint32_t min_should_match,
                             const SubReader& segment, const search::ScoredCtx& ctx,
                             ScoreMergeType merge, score_t absorbed) {
  return search::builder::MakeNodeRequired<ScoredApi>(
    must, must_filters, should, should_filters, should_uniformity,
    min_should_match, segment, ctx, merge, absorbed);
}

Node::ptr Make(const BooleanQuery& query, const search::ScoredCtx& ctx) {
  return search::builder::MakeNode<ScoredApi>(query, ctx, query.MergeType());
}

}  // namespace irs::lead
