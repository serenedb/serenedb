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

#include "iresearch/search/common/posting_count.hpp"
#include "iresearch/search/fill/count_threshold_docs.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/plan.hpp"
#include "iresearch/search/fill/set_leaves.hpp"

namespace irs::fill {

Node::ptr MakeCountThresholdDocs(std::span<const search::PostingClause> terms,
                                 const IndexInput* input,
                                 const std::vector<Node::ptr>& rest,
                                 uint32_t min_match) {
  if (!rest.empty()) {
    return {};
  }
  SDB_ASSERT(terms.size() >= min_match);
  const auto& doc = *input;
  return ResolveInput(doc, [&]<typename Input> -> Node::ptr {
    using Leaf = search::PostingCount<Input>;
    const auto init = [&](Leaf& leaf, size_t i) {
      const auto& own = *terms[i].state.reader;
      const auto& meta = terms[i].state.cookie;
      leaf.Prepare(meta, doc, meta.docs_count != 1 && search::BoundsOf(own),
                   meta.docs_count != 1 && search::FreqOf(own));
    };
    return memory::make_managed<Impl<CountThresholdDocs<SetLeaves<Leaf>>>>(
      std::piecewise_construct, std::forward_as_tuple(terms.size(), init),
      min_match);
  });
}

}  // namespace irs::fill
