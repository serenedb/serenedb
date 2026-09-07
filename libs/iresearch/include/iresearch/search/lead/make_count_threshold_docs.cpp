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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/posting_count.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/lead/count_threshold_docs.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/plan.hpp"

namespace irs::lead {

Node::ptr MakeCountThresholdDocs(std::span<const PostingClause> terms,
                                 std::span<const QueryBuilder::ptr> filters,
                                 const SubReader&, uint32_t min_match) {
  SDB_ASSERT(min_match > 1);
  if (!filters.empty() || terms.size() < min_match) {
    return {};
  }
  const IndexInput* input = nullptr;
  std::vector<FillNode::ptr> rest;
  if (!CollectDense(terms, filters, nullptr, input, rest)) {
    return {};
  }
  const auto& doc = *input;
  return ResolveInput(doc, [&]<typename Input> -> Node::ptr {
    using Leaf = search::PostingCount<Input>;
    const auto init = [&](Leaf& leaf, size_t i) {
      const auto& own = FieldOf(terms[i], nullptr);
      const auto& meta = CookieOf(terms[i]);
      leaf.Prepare(meta, doc, meta.docs_count != 1 && search::BoundsOf(own),
                   meta.docs_count != 1 && search::FreqOf(own));
    };
    using Set = fill::SetLeaves<Leaf>;
    return memory::make_managed<Impl<CountThresholdDocs<Set>>>(
      std::piecewise_construct, std::forward_as_tuple(terms.size(), init),
      min_match);
  });
}

}  // namespace irs::lead
