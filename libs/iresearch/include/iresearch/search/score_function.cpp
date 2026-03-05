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

#include "iresearch/search/scorer.hpp"

namespace irs {
namespace {

template<ScoreMergeType MergeType>
IRS_FORCE_INLINE void ConstantScoreImpl(score_t* res, scores_size_t n,
                                        score_t value) {
  if constexpr (MergeType == ScoreMergeType::Noop) {
    std::fill_n(res, n, value);
  } else {
    for (scores_size_t i = 0; i != n; ++i) {
      Merge<MergeType>(res[i], value);
    }
  }
}

class ConstanScore : public ScoreOperator {
 public:
  explicit ConstanScore(score_t value) noexcept : _value{value} {}

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* res,
                                  scores_size_t n) const noexcept {
    ConstantScoreImpl<MergeType>(res, n, _value);
  }

  score_t Score() const noexcept final { return _value; }

  void Score(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl(res, n);
  }
  void ScoreSum(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, n);
  }
  void ScoreMax(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, n);
  }

  void ScoreBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kScoreBlock);
  }
  void ScoreSumBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, kScoreBlock);
  }
  void ScoreMaxBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, kScoreBlock);
  }

  void ScorePostingBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kPostingBlock);
  }

 private:
  score_t _value;
};

template<ScoreMergeType MergeType = ScoreMergeType::Noop>
IRS_FORCE_INLINE void DefaultScoreImpl(score_t* res, scores_size_t n) {
  if constexpr (MergeType == ScoreMergeType::Noop) {
    std::memset(res, 0, sizeof(score_t) * n);
  } else {
    SDB_ASSERT(std::all_of(res, res + n, [](score_t s) { return s >= 0; }));
  }
}

}  // namespace

score_t DefaultScore::Score() const noexcept { return 0; }

void DefaultScore::Score(score_t* res, scores_size_t n) const noexcept {
  DefaultScoreImpl(res, n);
}
void DefaultScore::ScoreSum(score_t* res, scores_size_t n) const noexcept {
  DefaultScoreImpl<ScoreMergeType::Sum>(res, n);
}
void DefaultScore::ScoreMax(score_t* res, scores_size_t n) const noexcept {
  DefaultScoreImpl<ScoreMergeType::Max>(res, n);
}

void DefaultScore::ScoreBlock(score_t* res) const noexcept {
  DefaultScoreImpl(res, kScoreBlock);
}
void DefaultScore::ScoreSumBlock(score_t* res) const noexcept {
  DefaultScoreImpl<ScoreMergeType::Sum>(res, kScoreBlock);
}
void DefaultScore::ScoreMaxBlock(score_t* res) const noexcept {
  DefaultScoreImpl<ScoreMergeType::Max>(res, kScoreBlock);
}

void DefaultScore::ScorePostingBlock(score_t* res) const noexcept {
  DefaultScoreImpl(res, kPostingBlock);
}

ScoreFunction ScoreFunction::Constant(score_t value) noexcept {
  return ScoreFunction::Make<ConstanScore>(value);
}

}  // namespace irs
