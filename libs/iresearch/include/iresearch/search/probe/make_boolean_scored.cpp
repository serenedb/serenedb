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
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/probe/boolean_sparse.hpp"
#include "iresearch/search/probe/classify.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/search/scorers/score_policy.hpp"

namespace irs::probe {

Node::ptr MakeRequiredScored(std::span<const detail::PostingClause> must,
                             std::span<const QueryBuilder::ptr> must_filters,
                             detail::Terms must_uniformity,
                             std::span<const detail::PostingClause> should,
                             std::span<const QueryBuilder::ptr> should_filters,
                             detail::Terms should_uniformity,
                             uint32_t min_should_match,
                             const SubReader& segment,
                             const detail::ScoreRecipe& recipe,
                             ScoreMergeType merge, uint64_t interrogations,
                             const detail::ScoredCtx& ctx, score_t absorbed) {
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

struct ScoredApi {
  using Result = Node::ptr;
  using Context = detail::ScoredCtx;

  static constexpr bool kScored = true;

  static Result MakeRequired(const BooleanQuery& query, const Context& ctx,
                             uint64_t interrogations,
                             const BooleanGroups& groups) {
    const auto& segment = query.Segment();
    const detail::ScoreRecipe recipe{.segment = &segment,
                                     .fetcher = ctx.fetcher};
    return MakeRequiredScored(
      groups.must, groups.must_filters, query.Uniformity(Occur::Must),
      groups.should, groups.should_filters, query.Uniformity(Occur::Should),
      groups.min_should_match, segment, recipe, query.MergeType(),
      interrogations, ctx, query.Absorbed());
  }

  static Result MakeBoost(const BooleanQuery& query, const Context& ctx,
                          uint64_t interrogations,
                          const BooleanGroups& groups) {
    const auto& segment = query.Segment();
    const detail::ScoreRecipe recipe{.segment = &segment,
                                     .fetcher = ctx.fetcher};
    return MakeSparseBoostScored(
      groups.must, groups.must_filters, query.Uniformity(Occur::Must),
      groups.should, groups.should_filters, query.Uniformity(Occur::Should),
      segment, recipe, query.MergeType(), interrogations, ctx,
      query.Absorbed());
  }

  static Result MakeExclusion(const BooleanQuery& query, const Context& ctx,
                              uint64_t interrogations,
                              const BooleanGroups& groups) {
    const auto& segment = query.Segment();
    const detail::ScoreRecipe recipe{.segment = &segment,
                                     .fetcher = ctx.fetcher};
    return MakeSparseExclusionScored(
      groups.must, groups.must_filters, query.Uniformity(Occur::Must),
      groups.should, groups.should_filters, query.Uniformity(Occur::Should),
      groups.min_should_match, groups.exclude, groups.exclude_filters, segment,
      recipe, query.MergeType(), interrogations, ctx, query.Absorbed());
  }
};

Node::ptr Make(const BooleanQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations) {
  return MakeBoolean<ScoredApi>(query, ctx, interrogations);
}

}  // namespace irs::probe
