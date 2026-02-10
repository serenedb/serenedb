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

#include "iresearch/search/score_function.hpp"

#include <absl/algorithm/container.h>

#include <bit>

#include "iresearch/search/score.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {
namespace {

struct ConstantCtx {
  score_t value;
  uint32_t padding = 0;
};
static_assert(sizeof(ConstantCtx) == sizeof(ScoreCtx*));

void ConstantScore(ScoreCtx* ctx, score_t* res, size_t n) noexcept {
  SDB_ASSERT(res);
  auto state = std::bit_cast<ConstantCtx>(ctx);
  std::fill_n(res, n, state.value);
}

}  // namespace

void ScoreFunction::DefaultScore(ScoreCtx* ctx, score_t* res,
                                 size_t n) noexcept {
  SDB_ASSERT(res != nullptr);
  std::memset(res, 0, sizeof(score_t) * n);
}

ScoreFunction ScoreFunction::Constant(score_t value) noexcept {
  ConstantCtx ctx{value};
  return ScoreFunction{std::bit_cast<ScoreCtx*>(ctx), ConstantScore, NoopMin};
}

ScoreFunction ScoreFunction::Default() {
  return ScoreFunction{nullptr, DefaultScore, NoopMin};
}

score_t ScoreFunction::Max() const noexcept {
  if (_score == DefaultScore) {
    return 0.f;
  } else if (_score == ConstantScore) {
    score_t score;
    Score(&score, 1);
    return score;
  }
  return std::numeric_limits<score_t>::max();
}

}  // namespace irs
