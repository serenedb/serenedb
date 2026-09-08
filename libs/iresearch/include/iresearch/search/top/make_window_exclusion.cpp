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
#include <tuple>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/collect_scored.hpp"
#include "iresearch/search/common/exclusion_of.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/fill/leaves.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/window_disjunction.hpp"

namespace irs::top {

Root::ptr MakeWindowExclusion(const BooleanQuery& query,
                              const SubReader& segment, const Context& ctx,
                              ScoreMergeType merge, score_t absorbed) {
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  const std::span excludes = query.Terms(Occur::MustNot);
  const std::span exclude_filters = query.Queries(Occur::MustNot);
  SDB_ASSERT(!excludes.empty() || !exclude_filters.empty());

  std::span<const PostingClause> terms;
  std::span<const QueryBuilder::ptr> filters;
  if (must.empty() && must_filters.empty()) {
    if (query.MinShouldMatch() != 1 ||
        (should.empty() && should_filters.empty())) {
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
  std::vector<search::FillNode::ptr> rest;
  if (!search::CollectDenseScored(terms, filters, nullptr, doc, rest,
                                  [&](const QueryBuilder& child) {
                                    return child.PlanFill(ScoredOf(ctx), merge);
                                  })) {
    return {};
  }
  const auto candidates =
    search::IncludeCandidates(must, must_filters, segment);
  const search::ScoreRecipe recipe{.segment = &segment,
                                   .fetcher = &ctx.fetcher};
  const auto uniformity = query.Uniformity(Occur::Should);
  return search::BuildExcludeSide<Root::ptr>(
    excludes, exclude_filters, nullptr, segment, candidates,
    [&]<typename Exclude>(auto&& negated) -> Root::ptr {
      using Excludes = fill::ProbedAndNot<Exclude>;
      const auto make = [&]<typename Set>(auto&&... args) -> Root::ptr {
        const auto leaves =
          std::forward_as_tuple(std::forward<decltype(args)>(args)...);
        return MakeShape<WindowDisjunction, Set, Excludes>(
          ctx, std::piecewise_construct, leaves,
          std::forward_as_tuple(std::piecewise_construct,
                                std::forward<decltype(negated)>(negated)),
          merge, absorbed);
      };
      return search::BuildScoredWindow<Root::ptr>(
        terms, nullptr, nullptr, kNoBoost, doc, rest, uniformity, recipe, merge,
        make);
    });
}

}  // namespace irs::top
