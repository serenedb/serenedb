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

#include "iresearch/search/fill/bits_threshold_docs.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/plan.hpp"

namespace irs::fill {

Node::ptr MakeBitsThresholdDocs(std::span<const search::PostingClause> terms,
                                const IndexInput* doc,
                                std::vector<Node::ptr>& rest,
                                uint32_t min_match) {
  SDB_ASSERT(terms.size() + rest.size() >= min_match);
  return BuildDense<Node::ptr>(
    terms, nullptr, doc, rest, [&]<typename Set>(auto&&... args) -> Node::ptr {
      const auto leaves =
        std::forward_as_tuple(std::forward<decltype(args)>(args)...);
      return memory::make_managed<Impl<BitsThresholdDocs<Set>>>(
        std::piecewise_construct, leaves, min_match);
    });
}

}  // namespace irs::fill
