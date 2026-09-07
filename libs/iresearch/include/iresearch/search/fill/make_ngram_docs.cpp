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

#include "iresearch/search/common/ngram_of.hpp"
#include "iresearch/search/fill/plan.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/lead/two_phase_docs.hpp"

namespace irs::fill {

Node::ptr MakeNGramDocs(const NGramSimilarityQuery& query) {
  return search::Build(query, [&]<typename Slots>(auto&&... args) -> Node::ptr {
    using Node = lead::TwoPhaseDocs<Slots>;
    return memory::make_managed<ByWalkDocs<Node>>(
      std::forward<decltype(args)>(args)...);
  });
}

}  // namespace irs::fill
