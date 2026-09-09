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

#include "basics/empty.hpp"
#include "iresearch/search/common/boolean_groups.hpp"
#include "iresearch/search/common/collect_scored.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/scored/boolean_window.hpp"
#include "iresearch/search/scored/make.hpp"

namespace irs::scored {

Root::ptr MakeSparseConjunction(const BooleanQuery& query,
                                const SubReader& segment, const Context& ctx,
                                ScoreMergeType merge, score_t absorbed);
Root::ptr MakeSparseExclusion(const BooleanQuery& query,
                              const SubReader& segment, const Context& ctx,
                              ScoreMergeType merge, score_t absorbed);
Root::ptr MakeWindowExclusion(const BooleanQuery& query,
                              const SubReader& segment, const Context& ctx,
                              ScoreMergeType merge, score_t absorbed);
Root::ptr MakeBoostedPosting(const BooleanQuery& query,
                             const SubReader& segment, const Context& ctx,
                             ScoreMergeType merge, score_t absorbed);

template<typename Term>
Root::ptr MakeWindowDisjunction(std::span<const Term> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                search::Terms uniformity,
                                const TermReader* field, const Scorer* scorer,
                                score_t boost, const SubReader& segment,
                                const Context& ctx, ScoreMergeType merge,
                                score_t absorbed) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  const IndexInput* doc = nullptr;
  std::vector<search::FillNode::ptr> rest;
  if (!search::CollectDenseScored(terms, filters, field, doc, rest,
                                  [&](const QueryBuilder& child) {
                                    return child.PlanFill(ScoredOf(ctx), merge);
                                  })) {
    return {};
  }
  const auto make = [&]<typename Set>(auto&&... args) -> Root::ptr {
    return MakeShape<BooleanWindow, utils::Empty, search::OrGroup<Set>,
                     utils::Empty>(
      ctx, std::piecewise_construct, std::forward_as_tuple(),
      std::forward_as_tuple(std::forward<decltype(args)>(args)...),
      std::forward_as_tuple(), absorbed);
  };
  const search::ScoreRecipe recipe{.segment = &segment,
                                   .fetcher = &ctx.fetcher};
  return search::BuildScoredWindow<Root::ptr>(
    terms, field, scorer, boost, doc, rest, uniformity, recipe, merge, make);
}

}  // namespace irs::scored
