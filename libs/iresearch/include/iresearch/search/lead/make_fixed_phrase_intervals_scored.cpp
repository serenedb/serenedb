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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/phrase_of.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/lead/two_phase_scored.hpp"
#include "iresearch/search/phrase_query.hpp"

namespace irs::lead {

Node::ptr MakeFixedPhraseIntervalsScored(const FixedPhraseQuery& query,
                                         const ScoreArgs& args) {
  if (args.stats == nullptr || query.state.reader == nullptr) {
    return {};
  }
  return search::MakeFixedPhraseOf<search::PhraseMatch::Intervals, Impl,
                                   Node::ptr, true, TwoPhaseScored>(
    query, query.Segment(), *query.state.reader, args);
}

}  // namespace irs::lead
