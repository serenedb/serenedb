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
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/hits/make.hpp"
#include "iresearch/search/hits/walk.hpp"
#include "iresearch/search/lead/two_phase.hpp"
#include "iresearch/search/scorers/score_args.hpp"

namespace irs::hits {

struct Family {
  using Result = Root::ptr;
  using ScoredContext = Context;

  template<typename Slots>
  using TwoPhaseDocs = lead::TwoPhaseDocs<Slots>;
  template<typename Slots>
  using TwoPhaseScored = lead::TwoPhaseScored<Slots>;

  static uint64_t Interrogations(const Context&) noexcept { return 0; }

  static irs::detail::ScoredCtx Scoring(const Context& ctx) noexcept {
    return ScoredOf(ctx);
  }

  template<typename Node, typename... Args>
  static Result Constant(const Context& ctx, score_t value, Args&&... args) {
    return MakeShape<ConstantWalk, Node>(ctx, value,
                                         std::forward<Args>(args)...);
  }

  template<typename Node, typename... Args>
  static Result Scored(const Context& ctx, Args&&... args) {
    return MakeShape<Walk, Node>(ctx, ctx.fetcher, std::forward<Args>(args)...);
  }
};

}  // namespace irs::hits
