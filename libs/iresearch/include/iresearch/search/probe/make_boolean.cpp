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

#include "basics/empty.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/probe/classify.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/queries/boolean_query.hpp"

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

struct DocsApi {
  using Result = Node::ptr;
  using Context = utils::Empty;

  static constexpr bool kScored = false;

  static Result MakeRequired(const BooleanQuery& query, Context,
                             uint64_t interrogations,
                             const BooleanGroups& groups) {
    return MakeRequiredDocs(groups.must, groups.must_filters, groups.should,
                            groups.should_filters, groups.min_should_match,
                            query.Segment(), interrogations);
  }

  static Result MakeExclusion(const BooleanQuery& query, Context,
                              uint64_t interrogations,
                              const BooleanGroups& groups) {
    return MakeSparseExclusionDocs(
      groups.must, groups.must_filters, groups.should, groups.should_filters,
      groups.min_should_match, groups.exclude, groups.exclude_filters,
      query.Segment(), interrogations);
  }
};

Node::ptr Make(const BooleanQuery& query, uint64_t interrogations) {
  return MakeBoolean<DocsApi>(query, utils::Empty{}, interrogations);
}

}  // namespace irs::probe
