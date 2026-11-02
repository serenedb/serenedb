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
#include <tuple>
#include <utility>
#include <vector>

#include "iresearch/search/detail/boolean_planner.hpp"

namespace irs::detail::builder {

template<typename Family, typename Ctx>
Result<Family> MakeScoredNegation(
  std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  ScoreMergeType merge, score_t absorbed, const Ctx& ctx) {
  return BuildNegationWindow<Result<Family>>(
    excludes, exclude_filters, segment,
    [&]<typename Lead, typename Excludes>(auto&& lead,
                                          auto&& excluded) -> Result<Family> {
      return Family::template MakeWindow<Lead, utils::Empty, utils::Empty,
                                         Excludes>(
        ctx, merge, Scored{merge, absorbed}, std::forward<decltype(lead)>(lead),
        std::forward_as_tuple(), std::forward_as_tuple(),
        std::forward<decltype(excluded)>(excluded));
    });
}

template<typename Family, typename Ctx>
Result<Family> MakeExclusionWindow(const BooleanQuery& query, const Ctx& ctx,
                                   ScoreMergeType merge, score_t absorbed,
                                   uint64_t candidates) {
  const auto& segment = query.Segment();
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span excludes = query.Terms(Occur::MustNot);
  const std::span exclude_filters = query.Queries(Occur::MustNot);
  SDB_ASSERT(!excludes.empty() || !exclude_filters.empty());
  if constexpr (kScoredPlan<Family, Ctx>) {
    const std::span should = query.Terms(Occur::Should);
    const std::span should_filters = query.Queries(Occur::Should);
    std::span<const PostingClause> terms;
    std::span<const QueryBuilder::ptr> filters;
    if (must.empty() && must_filters.empty()) {
      if (should.empty() && should_filters.empty()) {
        return MakeScoredNegation<Family>(excludes, exclude_filters, segment,
                                          merge, absorbed, ctx);
      }
      if (query.MinShouldMatch() != 1) {
        return {};
      }
      terms = should;
      filters = should_filters;
    } else if (must.empty() && must_filters.size() == 1 && should.empty() &&
               should_filters.empty()) {
      filters = must_filters;
    } else {
      return {};
    }
    const IndexInput* doc = nullptr;
    std::vector<FillNode::ptr> rest;
    if (!CollectDense(terms, filters, nullptr, doc, rest,
                      [&](const QueryBuilder& filter) {
                        return filter.PlanFill(Family::Scoring(ctx), merge);
                      })) {
      return {};
    }
    const auto scoring =
      ScoringOf<Family>(segment, ctx, merge, query.Uniformity(Occur::Should));
    return BuildWindowExcludes<Result<Family>>(
      excludes, exclude_filters, nullptr, segment, candidates,
      [&]<typename Excludes>(auto&& negated) -> Result<Family> {
        return BuildWindowSet<Result<Family>>(
          terms, nullptr, doc, rest, scoring,
          [&]<typename Set>(auto&&... args) -> Result<Family> {
            return Family::template MakeWindow<utils::Empty, utils::Empty,
                                               OrGroup<Set>, Excludes>(
              ctx, merge, Scored{merge, absorbed}, std::forward_as_tuple(),
              std::forward_as_tuple(),
              std::forward_as_tuple(std::forward<decltype(args)>(args)...),
              std::forward<decltype(negated)>(negated));
          });
      });
  } else {
    return MakeWindowExclusion<Family>(
      must, must_filters, excludes, exclude_filters, segment, candidates, ctx);
  }
}

}  // namespace irs::detail::builder
