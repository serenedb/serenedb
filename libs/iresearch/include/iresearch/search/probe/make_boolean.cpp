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
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/plan.hpp"

namespace irs::probe {

Node::ptr MakeRequiredDocs(std::span<const detail::PostingClause> must,
                           std::span<const QueryBuilder::ptr> must_filters,
                           std::span<const detail::PostingClause> should,
                           std::span<const QueryBuilder::ptr> should_filters,
                           uint32_t min_should_match, const SubReader& segment,
                           uint64_t interrogations) {
  if (min_should_match == 0) {
    return MakeSparseConjunctionDocs(must, must_filters, segment,
                                     interrogations);
  }
  if (must.empty() && must_filters.empty()) {
    return BuildOptionalProbe(should, should_filters, min_should_match, segment,
                              interrogations);
  }
  auto other = BuildOptionalProbe(
    should, should_filters, min_should_match, segment,
    std::min(interrogations,
             detail::IncludeCandidates(must, must_filters, segment)));
  if (!other) {
    return {};
  }
  return MakeSparseConjunctionWithDocs(must, must_filters, segment,
                                       interrogations, std::move(other));
}

Node::ptr Make(const BooleanQuery& query, uint64_t interrogations) {
  const auto& segment = query.Segment();
  const auto exclude = query.Terms(Occur::MustNot);
  const auto exclude_filters = query.Queries(Occur::MustNot);
  const auto must = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto should = query.Terms(Occur::Should);
  const auto should_filters = query.Queries(Occur::Should);
  const auto min_should_match = query.MinShouldMatch();
  if (exclude.empty() && exclude_filters.empty()) {
    return MakeRequiredDocs(must, must_filters, should, should_filters,
                            min_should_match, segment, interrogations);
  }
  return MakeSparseExclusionDocs(must, must_filters, should, should_filters,
                                 min_should_match, exclude, exclude_filters,
                                 segment, interrogations);
}

}  // namespace irs::probe
