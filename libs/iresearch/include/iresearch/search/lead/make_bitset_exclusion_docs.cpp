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

#include <cstdint>
#include <span>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/exclusion_bitset.hpp"
#include "iresearch/search/lead/plan.hpp"

namespace irs::lead {

Node::ptr MakeBitsetExclusionDocs(
  std::span<const PostingClause> must,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const PostingClause>, std::span<const QueryBuilder::ptr>,
  uint32_t min_should_match, std::span<const PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters,
  const SubReader& segment) {
  SDB_ASSERT(!excludes.empty() || !exclude_filters.empty());
  if (min_should_match != 0) {
    return {};
  }
  if (must.empty() && must_filters.empty()) {
    return {};
  }
  return search::MakeExclusionBitset<Node::ptr>(
    must, must_filters, nullptr, excludes, exclude_filters, nullptr, segment,
    IncludeCandidates(must, must_filters, segment), nullptr);
}

}  // namespace irs::lead
