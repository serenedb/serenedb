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

namespace irs {
namespace {

class ConstanScore : public ScoreOperator {
 public:
  explicit ConstanScore(score_t value) noexcept : _value{value} {}

  void Score(score_t* res, size_t n) noexcept final {
    std::fill_n(res, n, _value);
  }

  void ScoreBlock(score_t* res) noexcept final {
    std::fill_n(res, kScoreBlock, _value);
  }

  void ScorePostingBlock(score_t* res) noexcept final {
    std::fill_n(res, kPostingBlock, _value);
  }

 private:
  score_t _value;
};

}  // namespace

DefaultScore DefaultScore::gInstance;

void DefaultScore::Score(score_t* res, size_t n) noexcept {
  std::memset(res, 0, sizeof(score_t) * n);
}

void DefaultScore::ScoreBlock(score_t* res) noexcept {
  std::memset(res, 0, sizeof(score_t) * kScoreBlock);
}

void DefaultScore::ScorePostingBlock(score_t* res) noexcept {
  std::memset(res, 0, sizeof(score_t) * kPostingBlock);
}

ScoreFunction ScoreFunction::Constant(score_t value) noexcept {
  return ScoreFunction::Make<ConstanScore>(value);
}

}  // namespace irs
