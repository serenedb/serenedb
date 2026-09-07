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

#include "iresearch/search/common/score/make_window.hpp"

#include "basics/shared.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs::search {
namespace {

class WindowScore : public ScoreOperator {
 public:
  explicit WindowScore(const score_t* gathered) noexcept : _gathered{gathered} {
    SDB_ASSERT(_gathered);
  }

  score_t Score() const noexcept final { return _gathered[0]; }

  void Score(score_t* res, scores_size_t n) const noexcept final {
    irs::Merge<ScoreMergeType::Noop>(res, _gathered, n);
  }
  void ScoreSum(score_t* res, scores_size_t n) const noexcept final {
    irs::Merge<ScoreMergeType::Sum>(res, _gathered, n);
  }
  void ScoreMax(score_t* res, scores_size_t n) const noexcept final {
    irs::Merge<ScoreMergeType::Max>(res, _gathered, n);
  }

  void ScoreBlock(score_t* res) const noexcept final {
    irs::Merge<ScoreMergeType::Noop>(res, _gathered, kScoreBlock);
  }
  void ScoreSumBlock(score_t* res) const noexcept final {
    irs::Merge<ScoreMergeType::Sum>(res, _gathered, kScoreBlock);
  }
  void ScoreMaxBlock(score_t* res) const noexcept final {
    irs::Merge<ScoreMergeType::Max>(res, _gathered, kScoreBlock);
  }

 private:
  const score_t* _gathered;
};

template<ScoreMergeType Inner>
class WindowConstScore : public ScoreOperator {
  static_assert(Inner != ScoreMergeType::Noop);

 public:
  WindowConstScore(const score_t* gathered, score_t constant) noexcept
    : _gathered{gathered}, _constant{constant} {
    SDB_ASSERT(_gathered);
  }

  score_t Score() const noexcept final {
    auto res = _gathered[0];
    irs::Merge<Inner>(res, _constant);
    return res;
  }

  void Score(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Noop>(res, n);
  }
  void ScoreSum(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, n);
  }
  void ScoreMax(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, n);
  }

  void ScoreBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Noop>(res, kScoreBlock);
  }
  void ScoreSumBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, kScoreBlock);
  }
  void ScoreMaxBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, kScoreBlock);
  }

 private:
  template<ScoreMergeType Outer>
  IRS_FORCE_INLINE void ScoreImpl(score_t* IRS_RESTRICT res,
                                  scores_size_t n) const noexcept {
    for (scores_size_t i = 0; i != n; ++i) {
      auto value = _gathered[i];
      irs::Merge<Inner>(value, _constant);
      irs::Merge<Outer>(res[i], value);
    }
  }

  const score_t* _gathered;
  score_t _constant;
};

}  // namespace

ScoreFunction MakeWindowScore(ScoreMergeType inner, const score_t* gathered,
                              score_t constant) {
  if (constant == 0) {
    return ScoreFunction::Make<WindowScore>(gathered);
  }
  if (inner == ScoreMergeType::Max) {
    return ScoreFunction::Make<WindowConstScore<ScoreMergeType::Max>>(gathered,
                                                                      constant);
  }
  SDB_ASSERT(inner == ScoreMergeType::Sum);
  return ScoreFunction::Make<WindowConstScore<ScoreMergeType::Sum>>(gathered,
                                                                    constant);
}

}  // namespace irs::search
