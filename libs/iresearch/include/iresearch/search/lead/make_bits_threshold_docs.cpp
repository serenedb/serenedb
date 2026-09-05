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
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/lead/bits_threshold_docs.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/plan.hpp"

namespace irs::lead {

Node::ptr MakeBitsThresholdDocs(std::span<const PostingClause> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                const SubReader&, uint32_t min_match) {
  SDB_ASSERT(min_match > 1);
  SDB_ASSERT(terms.size() + filters.size() >= min_match);
  const IndexInput* doc = nullptr;
  std::vector<FillNode::ptr> rest;
  if (!CollectDense(terms, filters, nullptr, doc, rest) ||
      terms.size() + rest.size() < min_match) {
    return {};
  }
  return BuildDense<Node::ptr>(
    terms, nullptr, doc, rest, [&]<typename Set>(auto&&... args) -> Node::ptr {
      const auto leaves =
        std::forward_as_tuple(std::forward<decltype(args)>(args)...);
      using Node = BitsThresholdDocs<Set>;
      return memory::make_managed<Impl<Node>>(std::piecewise_construct, leaves,
                                              min_match);
    });
}

}  // namespace irs::lead
