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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/ngram_of.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/lead/two_phase_scored.hpp"
#include "iresearch/search/ngram_similarity_query.hpp"

namespace irs::lead {

Node::ptr MakeNGramScored(const NGramSimilarityQuery& query,
                          const ScoreArgs& args) {
  if (args.stats == nullptr) {
    return {};
  }
  return search::Build<true>(query,
                             [&]<typename Slots>(auto&&... rest) -> Node::ptr {
                               using Node = TwoPhaseScored<Slots>;
                               return memory::make_managed<Impl<Node>>(
                                 query.Segment(), *query.State().reader, args,
                                 std::forward<decltype(rest)>(rest)...);
                             });
}

}  // namespace irs::lead
