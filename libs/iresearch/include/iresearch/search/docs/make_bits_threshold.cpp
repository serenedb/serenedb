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

#include <type_traits>
#include <utility>

#include "iresearch/search/docs/bits_threshold.hpp"
#include "iresearch/search/docs/plan.hpp"

namespace irs::docs {

Root::ptr MakeBitsThreshold(std::span<const search::PostingClause> terms,
                            const IndexInput* doc,
                            std::vector<FillNode::ptr>& rest,
                            uint32_t min_match, const Context& ctx) {
  SDB_ASSERT(terms.size() + rest.size() >= min_match);
  return BuildDense<Root::ptr>(
    terms, nullptr, doc, rest, [&]<typename Set>(auto&&... args) -> Root::ptr {
      const auto leaves =
        std::forward_as_tuple(std::forward<decltype(args)>(args)...);
      return MakeShape<BitsThreshold, Set>(ctx, std::piecewise_construct,
                                           leaves, min_match);
    });
}

}  // namespace irs::docs
