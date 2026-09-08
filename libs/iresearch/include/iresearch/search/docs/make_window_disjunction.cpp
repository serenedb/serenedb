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
#include <vector>

#include "iresearch/search/docs/plan.hpp"
#include "iresearch/search/docs/window.hpp"
#include "iresearch/search/fill/set_leaves.hpp"

namespace irs::docs {

Root::ptr MakeWindowDisjunction(std::span<const search::PostingClause> terms,
                                const IndexInput* doc,
                                std::vector<FillNode::ptr>& rest,
                                const Context& ctx) {
  return BuildDense<Root::ptr>(
    terms, nullptr, doc, rest, [&]<typename Set>(auto&&... args) -> Root::ptr {
      return MakeWindowOfSet<Set>(ctx, std::forward<decltype(args)>(args)...);
    });
}

}  // namespace irs::docs
