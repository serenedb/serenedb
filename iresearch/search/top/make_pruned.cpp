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
#include "iresearch/search/detail/make_query.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/search/top/family.hpp"
#include "iresearch/search/top/make.hpp"

namespace irs::top {
namespace {

Root::ptr MakePruned(const BooleanQuery& query, const Context& ctx) {
  const auto merge = query.MergeType();
  if (!ctx.prune || merge != ScoreMergeType::Sum || query.Absorbed() != 0) {
    return {};
  }
  const auto& segment = query.Segment();
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  const std::span excludes = query.Terms(Occur::MustNot);
  const std::span exclude_filters = query.Queries(Occur::MustNot);
  const auto min_match = query.MinShouldMatch();
  const bool optional = !should.empty() || !should_filters.empty();
  if (must.empty() && must_filters.empty()) {
    if (!optional || min_match == 0) {
      return {};
    }
    if ((!excludes.empty() || !exclude_filters.empty()) &&
        should.size() + should_filters.size() == 1) {
      return {};
    }
    return MakePrunedDisjunction(
      should, should_filters, query.Uniformity(Occur::Should), excludes,
      exclude_filters, segment, ctx, merge, min_match);
  }
  if (!optional) {
    if (must.size() == 1 && must_filters.empty() &&
        (!excludes.empty() || !exclude_filters.empty())) {
      return MakePrunedPosting(must.front(), excludes, exclude_filters, segment,
                               ctx);
    }
    return MakePrunedConjunction(must, must_filters,
                                 query.Uniformity(Occur::Must), excludes,
                                 exclude_filters, segment, ctx, merge);
  }
  if (excludes.empty() && exclude_filters.empty() && min_match == 0 &&
      must.size() == 1 && must_filters.empty() && should_filters.empty() &&
      query.Uniformity(Occur::Must) == irs::detail::Terms::Bounded &&
      query.Uniformity(Occur::Should) == irs::detail::Terms::Bounded) {
    return MakePrunedReqOpt(must.front(), should, segment, ctx, merge);
  }
  return {};
}

}  // namespace

Root::ptr Family::Make(const BooleanQuery& query, const Context& ctx) {
  if (auto pruned = MakePruned(query, ctx)) {
    return pruned;
  }
  return irs::detail::MakeQuery<Family>(query, ctx);
}

}  // namespace irs::top
