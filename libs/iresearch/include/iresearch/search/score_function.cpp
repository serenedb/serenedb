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

#include "iresearch/search/score.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {
namespace {

class ConstanScore : public ScoreFunctionImpl {
 public:
  explicit ConstanScore(score_t value) noexcept : _value{value} {}

  void Score(score_t* res, size_t n) noexcept final {
    SDB_ASSERT(res);
    std::fill_n(res, n, _value);
  }

 private:
  score_t _value;
};

}  // namespace

DefaultScore DefaultScore::gInstance;

void DefaultScore::Score(score_t* res, size_t n) noexcept {
  SDB_ASSERT(res);
  std::memset(res, 0, sizeof(score_t) * n);
}

ScoreFunction ScoreFunction::Constant(score_t value) noexcept {
  return ScoreFunction::Make<ConstanScore>(value);
}

}  // namespace irs
