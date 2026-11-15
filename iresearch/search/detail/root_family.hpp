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

#include <cstdint>

#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/lead/two_phase.hpp"

namespace irs::detail {

template<typename Context>
struct RootFamily {
  template<typename Slots>
  using TwoPhaseDocs = lead::TwoPhaseDocs<Slots>;

  static uint64_t Interrogations(const Context&) noexcept { return 0; }
};

template<typename Context>
struct ScoredRootFamily : RootFamily<Context> {
  template<typename Slots>
  using TwoPhaseScored = lead::TwoPhaseScored<Slots>;

  static constexpr bool kRoot = true;
  static constexpr bool kWrapsMerge = false;
  static constexpr bool kLazyGroups = false;

  static ScoredCtx Scoring(const Context& ctx) noexcept {
    return ScoredOf(ctx);
  }
};

}  // namespace irs::detail
