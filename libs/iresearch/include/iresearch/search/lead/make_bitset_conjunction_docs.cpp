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
#include "iresearch/search/common/conjunction_bitset.hpp"
#include "iresearch/search/lead/plan.hpp"

namespace irs::lead {

Node::ptr MakeBitsetConjunctionDocs(std::span<const PostingClause> terms,
                                    std::span<const QueryBuilder::ptr> filters,
                                    const SubReader& segment) {
  if (terms.size() + filters.size() < 2) {
    return {};
  }
  return search::MakeConjunctionBitset<Node::ptr>(terms, filters, nullptr,
                                                  segment, nullptr);
}

}  // namespace irs::lead
