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
#include "iresearch/search/lead/constant_scored.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/node.hpp"
#include "iresearch/search/lead/two_phase_docs.hpp"
#include "iresearch/search/lead/two_phase_scored.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/memory.hpp"

namespace irs::lead {

struct Family {
  using Result = Node::ptr;
  using DocsContext = utils::Empty;
  using ScoredContext = detail::ScoredCtx;

  static uint64_t Interrogations(const DocsContext&) noexcept { return 0; }

  static uint64_t Interrogations(const ScoredContext&) noexcept { return 0; }

  static DocsContext DocsOf(const ScoredContext&) noexcept { return {}; }

  static const detail::ScoredCtx& Scoring(const ScoredContext& ctx) noexcept {
    return ctx;
  }

  template<typename Slots, typename... Args>
  static Result Docs(const DocsContext&, Args&&... args) {
    return memory::make_managed<Impl<TwoPhaseDocs<Slots>>>(
      std::forward<Args>(args)...);
  }

  template<typename Slots, typename... Args>
  static Result Constant(const ScoredContext&, score_t value, Args&&... args) {
    return memory::make_managed<ConstantScoredImpl<TwoPhaseDocs<Slots>>>(
      value, std::forward<Args>(args)...);
  }

  template<typename Slots, typename... Args>
  static Result Scored(const ScoredContext&, const SubReader& segment,
                       const TermReader& field, const detail::ScoreArgs& args,
                       Args&&... rest) {
    return memory::make_managed<Impl<TwoPhaseScored<Slots>>>(
      segment, field, args, std::forward<Args>(rest)...);
  }
};

}  // namespace irs::lead
