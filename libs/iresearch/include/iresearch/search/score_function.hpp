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

#pragma once

#include <limits>

#include "basics/memory.hpp"
#include "basics/noncopyable.hpp"
#include "iresearch/types.hpp"

namespace irs {

using scores_size_t = uint8_t;  // NOLINT
inline constexpr scores_size_t kScoreBlock = 32;
static_assert(kScoreBlock < std::numeric_limits<scores_size_t>::max());
inline constexpr scores_size_t kPostingBlock = 4 * kScoreBlock;
static_assert(kPostingBlock < std::numeric_limits<scores_size_t>::max());
static_assert(kPostingBlock % kScoreBlock == 0);

// Possible variants of merging multiple scores
enum class ScoreMergeType {
  // Do nothing
  Noop = 0,

  // Sum multiple scores
  Sum,

  // Find max amongst multiple scores
  Max,
};

struct ScoreOperator : memory::Managed {
  // TODO(mbkkt): Maybe add ScoreSum and ScoreMax for single score?
  virtual score_t Score() const noexcept {
    score_t result;
    Score(&result, 1);
    return result;
  }

  virtual void Score(score_t* res, scores_size_t n) const noexcept = 0;
  virtual void ScoreSum(score_t* res, scores_size_t n) const noexcept = 0;
  virtual void ScoreMax(score_t* res, scores_size_t n) const noexcept = 0;

  virtual void ScoreBlock(score_t* res) const noexcept {
    Score(res, kScoreBlock);
  }
  virtual void ScoreSumBlock(score_t* res) const noexcept {
    ScoreSum(res, kScoreBlock);
  }
  virtual void ScoreMaxBlock(score_t* res) const noexcept {
    ScoreMax(res, kScoreBlock);
  }

  virtual void ScorePostingBlock(score_t* res) const noexcept {
    SDB_ASSERT(false);
  }
};

struct DefaultScore final : public ScoreOperator {
  score_t Score() const noexcept final;

  void Score(score_t* res, scores_size_t n) const noexcept final;
  void ScoreSum(score_t* res, scores_size_t n) const noexcept final;
  void ScoreMax(score_t* res, scores_size_t n) const noexcept final;

  void ScoreBlock(score_t* res) const noexcept final;
  void ScoreSumBlock(score_t* res) const noexcept final;
  void ScoreMaxBlock(score_t* res) const noexcept final;

  void ScorePostingBlock(score_t* res) const noexcept final;
};

// For disjunction/conjunction it's just sum of sub-iterators max score
// For iterator without score it depends on count of documents in iterator
// For wanderator it's max score for whole skip-list
// TODO(mbkkt) tail better here and not affect correctness
//  but to support it we need to know max value in the tail blocks.
//  Open question: how do it without read next blocks?
// TODO(mbkkt) At least when iterator exhausted, we could set it to zero.
struct UpperBounds {
  score_t tail = std::numeric_limits<score_t>::max();
  score_t leaf = std::numeric_limits<score_t>::max();
#ifdef SDB_GTEST
  std::span<const score_t> levels;  // levels.back() == leaf
#endif
};

class ScoreFunction {
  inline static constinit DefaultScore gDefaultScore;

 public:
  template<typename T, typename... Args>
  static ScoreFunction Make(Args&&... args) {
    return ScoreFunction{memory::make_managed<T>(std::forward<Args>(args)...)};
  }
  static ScoreFunction Wrap(ScoreOperator& impl) noexcept {
    return ScoreFunction{memory::to_managed<ScoreOperator>(impl)};
  }
  static ScoreFunction Default() noexcept { return ScoreFunction{}; }
  static ScoreFunction Constant(score_t value) noexcept;

  ScoreFunction() noexcept
    : ScoreFunction{memory::to_managed<ScoreOperator>(gDefaultScore)} {}
  ScoreFunction(ScoreFunction&& other) noexcept : ScoreFunction{} {
    std::swap(_impl, other._impl);
  }
  ScoreFunction& operator=(ScoreFunction&& other) noexcept {
    if (this != &other) {
      std::swap(_impl, other._impl);
    }
    return *this;
  }

  bool IsDefault() const noexcept { return _impl.get() == &gDefaultScore; }

  IRS_FORCE_INLINE score_t Score() const noexcept { return _impl->Score(); }

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void Score(score_t* res, scores_size_t n) const noexcept {
    if constexpr (MergeType == ScoreMergeType::Sum) {
      _impl->ScoreSum(res, n);
    } else if constexpr (MergeType == ScoreMergeType::Max) {
      _impl->ScoreMax(res, n);
    } else {
      _impl->Score(res, n);
    }
  }

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreBlock(score_t* res) const noexcept {
    if constexpr (MergeType == ScoreMergeType::Sum) {
      _impl->ScoreSumBlock(res);
    } else if constexpr (MergeType == ScoreMergeType::Max) {
      _impl->ScoreMaxBlock(res);
    } else {
      _impl->ScoreBlock(res);
    }
  }

  IRS_FORCE_INLINE void ScorePostingBlock(score_t* res) const noexcept {
    _impl->ScorePostingBlock(res);
  }

  bool operator==(const ScoreFunction& rhs) const noexcept = default;

 private:
  explicit ScoreFunction(memory::managed_ptr<ScoreOperator> impl) noexcept
    : _impl{std::move(impl)} {
    SDB_ASSERT(_impl);
  }

  memory::managed_ptr<ScoreOperator> _impl;
};

}  // namespace irs
