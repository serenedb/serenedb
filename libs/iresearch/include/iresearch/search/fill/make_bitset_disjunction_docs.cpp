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
#include <utility>
#include <vector>

#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/fill/plan.hpp"

namespace irs::fill {

Node::ptr MakeBitsetDisjunctionDocs(
  std::span<const search::PostingClause> terms, const IndexInput* doc,
  const std::vector<Node::ptr>& rest, doc_id_t docs_count) {
  if (terms.empty() || !rest.empty()) {
    return {};
  }
  return search::MakeBitsetOf<Node::ptr>(terms, nullptr, *doc, docs_count,
                                         nullptr);
}

}  // namespace irs::fill
