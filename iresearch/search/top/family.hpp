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
#include "iresearch/search/lead/two_phase_docs.hpp"
#include "iresearch/search/lead/two_phase_scored.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/walk.hpp"

namespace irs::top {

struct Family {
  using Result = Root::ptr;
  using ScoredContext = Context;

  static uint64_t Interrogations(const Context&) noexcept { return 0; }

  static irs::detail::ScoredCtx Scoring(const Context& ctx) noexcept {
    return ScoredOf(ctx);
  }

  template<typename Slots, typename... Args>
  static Result Constant(const Context& ctx, score_t value, Args&&... args) {
    return MakeShape<ConstantWalk, lead::TwoPhaseDocs<Slots>>(
      ctx, value, std::forward<Args>(args)...);
  }

  template<typename Slots, typename... Args>
  static Result Scored(const Context& ctx, const SubReader& segment,
                       const TermReader& field,
                       const irs::detail::ScoreArgs& args, Args&&... rest) {
    return MakeShape<Walk, lead::TwoPhaseScored<Slots>>(
      ctx, ctx.fetcher, segment, field, args, std::forward<Args>(rest)...);
  }
};

}  // namespace irs::top
