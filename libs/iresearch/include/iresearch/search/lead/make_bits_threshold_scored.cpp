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
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/fill_posting_scored.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/lead/bits_threshold_scored.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/plan.hpp"

namespace irs::lead {

Node::ptr MakeBitsThresholdScored(std::span<const PostingClause> terms,
                                  std::span<const QueryBuilder::ptr> filters,
                                  search::Terms uniformity,
                                  const SubReader& segment,
                                  const ScoredCtx& ctx, ScoreMergeType merge,
                                  uint32_t min_match, score_t absorbed) {
  SDB_ASSERT(min_match > 1);
  const IndexInput* doc = nullptr;
  std::vector<FillNode::ptr> rest;
  const auto plan = [&](const QueryBuilder& child) {
    return child.PlanFill(ctx, merge);
  };
  if (!CollectDenseScored(terms, filters, nullptr, doc, rest, plan) ||
      terms.size() + rest.size() < min_match) {
    return {};
  }
  const auto make = [&]<typename Set>(auto&&... args) -> Node::ptr {
    const auto leaves =
      std::forward_as_tuple(std::forward<decltype(args)>(args)...);
    using Node = BitsThresholdScored<Set>;
    return memory::make_managed<Impl<Node>>(std::piecewise_construct, leaves,
                                            min_match, merge, absorbed);
  };
  const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  return search::BuildScoredWindow<Node::ptr>(terms, nullptr, nullptr, kNoBoost,
                                              doc, rest, uniformity, recipe,
                                              merge, make);
}

}  // namespace irs::lead
