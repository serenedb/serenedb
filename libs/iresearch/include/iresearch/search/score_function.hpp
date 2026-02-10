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

inline constexpr size_t kScoreBlock = 32;
static_assert(kScoreBlock < std::numeric_limits<uint16_t>::max());
inline constexpr size_t kMaxScoreBlock = 4 * kScoreBlock;

struct ScoreFunctionImpl : memory::Managed {
  virtual void Score(score_t* res, size_t n) noexcept = 0;
  virtual void Score(score_t* res) noexcept { Score(res, 1); }
};

struct DefaultScore final : public ScoreFunctionImpl {
  static DefaultScore gInstance;

  void Score(score_t* res, size_t n) noexcept final;
};

class ScoreFunction {
 public:
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
  } max;

  template<typename T, typename... Args>
  static ScoreFunction Make(Args&&... args) {
    return ScoreFunction{memory::make_managed<T>(std::forward<Args>(args)...)};
  }
  static ScoreFunction Wrap(ScoreFunctionImpl& impl) noexcept {
    return ScoreFunction{memory::to_managed<ScoreFunctionImpl>(impl)};
  }
  static ScoreFunction Default() {
    return ScoreFunction::Wrap(DefaultScore::gInstance);
  }
  static ScoreFunction Constant(score_t value) noexcept;

  ScoreFunction() : ScoreFunction{Default()} {}
  ScoreFunction(ScoreFunction&& other) noexcept
    : max{std::move(other.max)},
      _impl{std::exchange(other._impl,
                          memory::to_managed<ScoreFunctionImpl>(
                            DefaultScore::gInstance))} {}
  ScoreFunction& operator=(ScoreFunction&& other) noexcept {
    if (this != &other) {
      max = std::move(other.max);
      _impl = std::exchange(other._impl,
                            memory::to_managed<ScoreFunctionImpl>(
                              DefaultScore::gInstance));
    }
    return *this;
  }
  ~ScoreFunction() noexcept = default;

  bool IsDefault() const noexcept {
    return _impl.get() == &DefaultScore::gInstance;
  }

  IRS_FORCE_INLINE void Score(score_t* res, size_t n) const noexcept {
    SDB_ASSERT(_impl);
    _impl->Score(res, n);
  }

  IRS_FORCE_INLINE void ScoreBlock(score_t* res) const noexcept {
    SDB_ASSERT(_impl);
    _impl->Score(res, kScoreBlock);
  }

  IRS_FORCE_INLINE void ScoreMaxBlock(score_t* res) const noexcept {
    SDB_ASSERT(_impl);
    _impl->Score(res, kMaxScoreBlock);
  }

  IRS_FORCE_INLINE void Score(score_t* res) const noexcept {
    SDB_ASSERT(_impl);
    _impl->Score(res);
  }
  // TODO(mbkkt) Remove it, use Score
  IRS_FORCE_INLINE void operator()(score_t* res) const noexcept { Score(res); }

  bool operator==(const ScoreFunction& rhs) const noexcept {
    return _impl == rhs._impl;
  }

 private:
  explicit ScoreFunction(memory::managed_ptr<ScoreFunctionImpl> impl) noexcept
    : _impl{std::move(impl)} {}

  memory::managed_ptr<ScoreFunctionImpl> _impl;
};

}  // namespace irs
