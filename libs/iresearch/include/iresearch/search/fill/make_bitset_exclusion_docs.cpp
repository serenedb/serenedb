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
#include "iresearch/search/common/exclusion_bitset.hpp"
#include "iresearch/search/fill/plan.hpp"

namespace irs::fill {

Node::ptr MakeBitsetExclusionDocs(
  std::span<const search::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates) {
  return search::MakeExclusionBitset<Node::ptr>(
    terms, filters, nullptr, exclude_terms, exclude_filters, nullptr, segment,
    candidates, nullptr);
}

}  // namespace irs::fill
