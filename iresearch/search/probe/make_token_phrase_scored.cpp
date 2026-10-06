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

#include "iresearch/search/detail/token_phrase_of.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/two_phase_scored.hpp"

namespace irs::probe {

Node::ptr MakeTokenPhraseScored(const TokenPhraseQuery& query,
                                const detail::ScoreArgs& args,
                                uint64_t interrogations) {
  if (!args.stats) {
    return {};
  }
  return detail::MakeTokenPhrase<Impl, Node::ptr, true, probe::TwoPhaseScored>(
    query, interrogations, query.Segment(), query.Reader(), args);
}

}  // namespace irs::probe
