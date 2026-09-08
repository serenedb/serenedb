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

#include <utility>

#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/docs/bitset.hpp"
#include "iresearch/search/docs/plan.hpp"

namespace irs::docs {

Root::ptr MakeBitsetDisjunction(std::span<const search::PostingClause> terms,
                                const IndexInput* doc,
                                std::vector<FillNode::ptr>& rest,
                                doc_id_t docs_count, const Context& ctx) {
  if (terms.empty() ||
      !search::TakeBitset<Root::ptr>(terms, *doc, docs_count)) {
    return {};
  }
  return search::MakeBitsetWith<Root::ptr>(terms, nullptr, *doc, docs_count,
                                           std::move(rest), nullptr);
}

}  // namespace irs::docs
