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

#pragma once

#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/phrase_of.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/lead/two_phase_docs.hpp"
#include "iresearch/search/lead/two_phase_scored.hpp"
#include "iresearch/search/queries/phrase_query.hpp"

namespace irs::lead {

template<detail::PhraseMatch M, typename Query>
Node::ptr MakePhraseDocs(const Query& query) {
  return detail::BuildPhrase<M>(
    query, [&]<typename Slots>(auto&&... args) -> Node::ptr {
      return memory::make_managed<Impl<TwoPhaseDocs<Slots>>>(
        std::forward<decltype(args)>(args)...);
    });
}

template<detail::PhraseMatch M, typename Query>
Node::ptr MakePhraseScored(const Query& query, const detail::ScoreArgs& args) {
  if (!args.stats || !query.state.reader) {
    return {};
  }
  return detail::BuildPhrase<M, true>(
    query, [&]<typename Slots>(auto&&... rest) -> Node::ptr {
      return memory::make_managed<Impl<TwoPhaseScored<Slots>>>(
        query.Segment(), *query.state.reader, args,
        std::forward<decltype(rest)>(rest)...);
    });
}

}  // namespace irs::lead

#define IRS_LEAD_PHRASE_DOCS(Query, Match) \
  template Node::ptr MakePhraseDocs<detail::PhraseMatch::Match>(const Query&)

#define IRS_LEAD_PHRASE_SCORED(Query, Match)                       \
  template Node::ptr MakePhraseScored<detail::PhraseMatch::Match>( \
    const Query&, const detail::ScoreArgs&)
