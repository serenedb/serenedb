////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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

#include "boost_scorer.hpp"

#include <absl/container/inlined_vector.h>

#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/score.hpp"
#include "iresearch/search/score_function.hpp"

namespace irs {
namespace {

Scorer::ptr MakeJson(std::string_view /*args*/) {
  return std::make_unique<BoostScore>();
}

struct VolatileBoostScoreCtx final : ScoreCtx {
  VolatileBoostScoreCtx(const score_t* volatile_boost, score_t boost) noexcept
    : const_boost{boost}, volatile_boost{volatile_boost} {
    SDB_ASSERT(volatile_boost);
  }

  score_t const_boost;
  absl::InlinedVector<score_t, kScoreWindow> boost;
  const score_t* volatile_boost;
};

}  // namespace

ScoreFunction BoostScore::PrepareScorer(const ScoreContext& ctx) const {
  const auto* volatile_boost = irs::get<irs::FilterBoost>(ctx.doc_attrs);

  if (volatile_boost == nullptr) {
    return ScoreFunction::Constant(ctx.boost);
  }

  return ScoreFunction::Make<VolatileBoostScoreCtx>(
    [](ScoreCtx* ctx, score_t* res) noexcept {
      auto& state = *static_cast<VolatileBoostScoreCtx*>(ctx);
      std::memcpy(res, state.boost.data(),
                  sizeof(score_t) * state.boost.size());
    },
    [](ScoreCtx* ctx) noexcept {
      auto& state = *static_cast<VolatileBoostScoreCtx*>(ctx);
      state.boost.emplace_back(*state.volatile_boost);
    },
    ScoreFunction::NoopMin, &volatile_boost->value, ctx.boost);
}

void BoostScore::init() { REGISTER_SCORER_JSON(BoostScore, MakeJson); }

}  // namespace irs
