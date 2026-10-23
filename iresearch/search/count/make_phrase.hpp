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

#include "iresearch/search/count/plan.hpp"
#include "iresearch/search/count/walk.hpp"
#include "iresearch/search/detail/phrase_of.hpp"
#include "iresearch/search/lead/two_phase_docs.hpp"

namespace irs::count {

template<detail::PhraseMatch M, typename Query>
Root::ptr MakePhrase(const Query& query, const Context& ctx) {
  return detail::BuildPhrase<M>(
    query, [&]<typename Slots>(auto&&... args) -> Root::ptr {
      return MakeShape<Walk, lead::TwoPhaseDocs<Slots>>(
        ctx, std::forward<decltype(args)>(args)...);
    });
}

}  // namespace irs::count

#define IRS_COUNT_PHRASE(Query, Match)                              \
  template Root::ptr MakePhrase<detail::PhraseMatch::Match>(const Query&, \
                                                            const Context&)
