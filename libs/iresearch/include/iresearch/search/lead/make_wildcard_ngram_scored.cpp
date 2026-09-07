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

#include "iresearch/search/lead/constant_scored.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/wildcard_ngram_filter.hpp"

namespace irs::lead {

Node::ptr MakeWildcardNGramScored(const WildcardNGramQuery& query,
                                  score_t score) {
  auto node = MakeWildcardNGramDocs(query);
  if (!node) {
    return {};
  }
  using Node = ConstantScored<Erased>;
  return memory::make_managed<Impl<Node>>(score, std::move(node));
}

}  // namespace irs::lead
