////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/formats/seek_cookie.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {

struct TermReader;

struct MultiTermState {
  struct ScoredTermState {
    ScoredTermState(SeekCookie::ptr&& cookie, uint32_t stat_offset,
                    score_t boost = kNoBoost) noexcept
      : cookie{std::move(cookie)}, stat_offset{stat_offset}, boost{boost} {}

    SeekCookie::ptr cookie;
    uint32_t stat_offset{};
    float_t boost{kNoBoost};
  };

  explicit MultiTermState(IResourceManager& memory) noexcept
    : scored_states{{memory}}, unscored_terms{{memory}} {}

  using UnscoredTermState = SeekCookie::ptr;

  // Return true if state is empty
  bool empty() const noexcept {
    return scored_states.empty() && unscored_terms.empty();
  }

  // Return total cost of execution
  CostAttr::Type estimation() const noexcept {
    return scored_states_estimation + unscored_states_estimation;
  }

  // Reader using for iterate over the terms
  const TermReader* reader{};

  // Scored term states
  ManagedVector<ScoredTermState> scored_states;

  // Matching terms that may have been skipped
  // while collecting statistics and should not be
  // scored by the disjunction.
  ManagedVector<UnscoredTermState> unscored_terms;

  // Estimated cost of scored states
  CostAttr::Type scored_states_estimation{};

  // Estimated cost of unscored states
  CostAttr::Type unscored_states_estimation{};
};

}  // namespace irs
