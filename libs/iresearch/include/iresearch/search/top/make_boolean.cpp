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

#include "iresearch/search/top/make_boolean.hpp"

#include <span>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"

namespace irs::top {

Root::ptr Make(const BooleanQuery& query, const Context& ctx) {
  const auto& segment = query.Segment();
  const auto merge = query.MergeType();
  const auto absorbed = query.Absorbed();
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  const std::span excludes = query.Terms(Occur::MustNot);
  const std::span exclude_filters = query.Queries(Occur::MustNot);
  const auto min_match = query.MinShouldMatch();
  const bool optional = !should.empty() || !should_filters.empty();
  const bool only_scores = optional && min_match == 0;
  const bool prune = ctx.prune && merge == ScoreMergeType::Sum && absorbed == 0;
  if (!excludes.empty() || !exclude_filters.empty()) {
    if (prune) {
      if (must.empty() && must_filters.empty()) {
        if (optional && min_match == 1 &&
            should.size() + should_filters.size() > 1) {
          if (auto pruned = MakeMaxScoreDisjunction(
                should, should_filters, query.Uniformity(Occur::Should),
                nullptr, nullptr, kNoBoost, excludes, exclude_filters, segment,
                ctx, merge)) {
            return pruned;
          }
        }
      } else if (!optional) {
        if (must.size() == 1 && must_filters.empty()) {
          if (auto pruned = MakePrunedPosting(must.front(), excludes,
                                              exclude_filters, segment, ctx)) {
            return pruned;
          }
        } else if (auto pruned = MakeWandConjunction(
                     must, must_filters, query.Uniformity(Occur::Must),
                     excludes, exclude_filters, segment, ctx, merge)) {
          return pruned;
        }
      }
    }
    if (auto windowed =
          MakeWindowExclusion(query, segment, ctx, merge, absorbed)) {
      return windowed;
    }
    return MakeSparseExclusion(query, segment, ctx, merge, absorbed);
  }
  if (must.empty() && must_filters.empty() && !only_scores) {
    if (!optional) {
      return MakeAll(segment, ctx, absorbed);
    }
    const auto uniformity = query.Uniformity(Occur::Should);
    if (min_match == 1) {
      if (prune) {
        if (auto pruned = MakeMaxScoreDisjunction(
              should, should_filters, uniformity, nullptr, nullptr, kNoBoost,
              {}, {}, segment, ctx, merge)) {
          return pruned;
        }
      }
      return MakeWindowDisjunction(should, should_filters, uniformity, nullptr,
                                   nullptr, kNoBoost, segment, ctx, merge,
                                   absorbed);
    }
    return MakeWindowThreshold(should, should_filters, uniformity, segment, ctx,
                               merge, min_match, absorbed);
  }
  if (prune && !optional) {
    if (auto pruned =
          MakeWandConjunction(must, must_filters, query.Uniformity(Occur::Must),
                              {}, {}, segment, ctx, merge)) {
      return pruned;
    }
  }
  return MakeSparseConjunction(query, segment, ctx, merge, absorbed);
}

}  // namespace irs::top
