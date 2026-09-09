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

#include "iresearch/search/scored/make_boolean.hpp"

#include <span>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"

namespace irs::scored {

Root::ptr Make(const BooleanQuery& query, const Context& ctx) {
  const auto& segment = query.Segment();
  const auto merge = query.MergeType();
  const auto absorbed = query.Absorbed();
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  const auto min_match = query.MinShouldMatch();
  const bool optional = !should.empty() || !should_filters.empty();
  const bool only_scores = optional && min_match == 0;
  if (!query.Terms(Occur::MustNot).empty() ||
      !query.Queries(Occur::MustNot).empty()) {
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
      return MakeWindowDisjunction(should, should_filters, uniformity, nullptr,
                                   nullptr, kNoBoost, segment, ctx, merge,
                                   absorbed);
    }
    if (min_match > search::kBitplaneMaxMatch) {
      if (auto counted =
            MakeCountThreshold(should, should_filters, uniformity, segment, ctx,
                               merge, min_match, absorbed)) {
        return counted;
      }
    }
    return MakeBitsThreshold(should, should_filters, uniformity, segment, ctx,
                             merge, min_match, absorbed);
  }
  return MakeSparseConjunction(query, segment, ctx, merge, absorbed);
}

}  // namespace irs::scored
