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

#include <algorithm>
#include <span>
#include <tuple>
#include <utility>

#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/score_policy.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/probe/boolean_sparse.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/plan.hpp"

namespace irs::probe {

Node::ptr MakeRequiredScored(
  std::span<const detail::PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  detail::Terms must_uniformity, std::span<const detail::PostingClause> should,
  std::span<const QueryBuilder::ptr> should_filters,
  detail::Terms should_uniformity, uint32_t min_should_match,
  const SubReader& segment, const detail::ScoreRecipe& recipe, ScoreMergeType merge,
  uint64_t interrogations, const detail::ScoredCtx& ctx, score_t absorbed) {
  if (min_should_match == 0) {
    return MakeSparseConjunctionScored(must, must_filters, must_uniformity,
                                       segment, recipe, merge, interrogations,
                                       ctx, absorbed);
  }
  const auto no_must = must.empty() && must_filters.empty();
  const auto reach =
    no_must ? interrogations
            : std::min(interrogations,
                       detail::IncludeCandidates(must, must_filters, segment));
  const auto optional_absorbed = no_must ? absorbed : score_t{0};
  auto optional =
    min_should_match == 1
      ? MakeDisjunctionScored(
          should, should_filters, should_uniformity, nullptr, nullptr, kNoBoost,
          segment, recipe, merge, reach, ScoredClauseOf(segment, ctx, recipe),
          ctx, optional_absorbed)
      : MakeSparseThresholdScored(should, should_filters, should_uniformity,
                                  segment, recipe, merge, min_should_match,
                                  reach, ctx, optional_absorbed);
  if (!optional) {
    return {};
  }
  if (no_must) {
    return optional;
  }
  auto required =
    MakeSparseConjunctionScored(must, must_filters, must_uniformity, segment,
                                recipe, merge, interrogations, ctx, absorbed);
  if (!required) {
    return {};
  }
  using Node = BooleanSparse<Erased, Erased, utils::Empty, detail::Scored>;
  return memory::make_managed<Impl<Node>>(
    std::piecewise_construct, std::forward_as_tuple(std::move(required)),
    std::forward_as_tuple(std::move(optional)), std::forward_as_tuple(),
    detail::Scored{merge, 0});
}

Node::ptr Make(const BooleanQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations) {
  const auto& segment = query.Segment();
  const auto merge = query.MergeType();
  const detail::ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  const auto absorbed = query.Absorbed();
  const auto must = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto must_uniformity = query.Uniformity(Occur::Must);
  const auto should = query.Terms(Occur::Should);
  const auto should_filters = query.Queries(Occur::Should);
  const auto should_uniformity = query.Uniformity(Occur::Should);
  const auto min_should_match = query.MinShouldMatch();
  const auto exclude = query.Terms(Occur::MustNot);
  const auto exclude_filters = query.Queries(Occur::MustNot);
  if (!exclude.empty() || !exclude_filters.empty()) {
    return MakeSparseExclusionScored(
      must, must_filters, must_uniformity, should, should_filters,
      should_uniformity, min_should_match, exclude, exclude_filters, segment,
      recipe, merge, interrogations, ctx, absorbed);
  }
  if ((!should.empty() || !should_filters.empty()) && min_should_match == 0) {
    return MakeSparseBoostScored(must, must_filters, must_uniformity, should,
                                 should_filters, should_uniformity, segment,
                                 recipe, merge, interrogations, ctx, absorbed);
  }
  return MakeRequiredScored(must, must_filters, must_uniformity, should,
                            should_filters, should_uniformity, min_should_match,
                            segment, recipe, merge, interrogations, ctx,
                            absorbed);
}

}  // namespace irs::probe
