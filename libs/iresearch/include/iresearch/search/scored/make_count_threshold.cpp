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
#include "iresearch/search/common/collect_scored.hpp"
#include "iresearch/search/common/fill_posting_scored.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/scored/count_threshold.hpp"
#include "iresearch/search/scored/make.hpp"

namespace irs::scored {

Root::ptr MakeCountThreshold(std::span<const PostingClause> terms,
                             std::span<const QueryBuilder::ptr> filters,
                             search::Terms uniformity, const SubReader& segment,
                             const Context& ctx, ScoreMergeType merge,
                             uint32_t min_match, score_t absorbed) {
  SDB_ASSERT(min_match > 1);
  SDB_ASSERT(terms.size() + filters.size() >= min_match);
  SDB_ASSERT(min_match != terms.size() + filters.size());
  if (!filters.empty()) {
    return {};
  }
  const IndexInput* doc = nullptr;
  std::vector<search::FillNode::ptr> rest;
  if (!search::CollectDenseScored(
        terms, filters, nullptr, doc, rest,
        [](const QueryBuilder&) { return fill::Node::ptr{}; })) {
    return {};
  }
  SDB_ASSERT(rest.empty());
  SDB_ASSERT(terms.size() >= min_match);
  const search::ScoreRecipe recipe{.segment = &segment,
                                   .fetcher = &ctx.fetcher};

  const auto make = [&]<typename Set>(auto&&... args) -> Root::ptr {
    const auto leaves =
      std::forward_as_tuple(std::forward<decltype(args)>(args)...);
    return MakeShape<CountThreshold, Set>(ctx, std::piecewise_construct, leaves,
                                          min_match, absorbed);
  };
  if (uniformity == search::Terms::Mixed) {
    return {};
  }
  return search::ResolveCountScored<Root::ptr>(
    *doc, uniformity >= search::Terms::Scored, merge,
    [&]<typename Leaf, typename Plain> -> Root::ptr {
      return search::BuildScoredTerms<Root::ptr, Leaf, Plain>(
        terms, nullptr, nullptr, kNoBoost, doc, recipe, make);
    });
}

}  // namespace irs::scored
