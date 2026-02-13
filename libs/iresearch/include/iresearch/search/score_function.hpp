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
inline constexpr size_t kPostingBlock = 4 * kScoreBlock;
static_assert(kPostingBlock < std::numeric_limits<uint16_t>::max());
static_assert(kPostingBlock % kScoreBlock == 0);

struct ScoreOperator : memory::Managed {
  virtual void Score(score_t* res, size_t n) noexcept = 0;
  virtual void ScoreBlock(score_t* res) noexcept { Score(res, kScoreBlock); }
  virtual void ScorePostingBlock(score_t* res) noexcept { SDB_ASSERT(false); }
  virtual score_t Score() noexcept {
    score_t result;
    Score(&result, 1);
    return result;
  }
};

struct DefaultScore final : public ScoreOperator {
  static DefaultScore gInstance;
  static memory::managed_ptr<ScoreOperator> Make() {
    return memory::to_managed<ScoreOperator>(gInstance);
  }

  void Score(score_t* res, size_t n) noexcept final;
  void ScoreBlock(score_t* res) noexcept final;
  void ScorePostingBlock(score_t* res) noexcept final;
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
 public:
  template<typename T, typename... Args>
  static ScoreFunction Make(Args&&... args) {
    return ScoreFunction{memory::make_managed<T>(std::forward<Args>(args)...)};
  }
  static ScoreFunction Wrap(ScoreOperator& impl) noexcept {
    return ScoreFunction{memory::to_managed<ScoreOperator>(impl)};
  }
  static ScoreFunction Default() noexcept {
    return ScoreFunction::Wrap(DefaultScore::gInstance);
  }
  static ScoreFunction Constant(score_t value) noexcept;

  ScoreFunction() noexcept : ScoreFunction{Default()} {}
  ScoreFunction(ScoreFunction&& other) noexcept
    : _impl{std::exchange(other._impl, DefaultScore::Make())} {
    SDB_ASSERT(_impl);
  }
  ScoreFunction& operator=(ScoreFunction&& other) noexcept {
    if (this != &other) {
      _impl = std::exchange(other._impl, DefaultScore::Make());
      SDB_ASSERT(_impl);
    }
    return *this;
  }

  bool IsDefault() const noexcept {
    return _impl.get() == &DefaultScore::gInstance;
  }

  IRS_FORCE_INLINE void Score(score_t* res, size_t n) const noexcept {
    _impl->Score(res, n);
  }

  IRS_FORCE_INLINE void ScoreBlock(score_t* res) const noexcept {
    _impl->ScoreBlock(res);
  }

  IRS_FORCE_INLINE void ScorePostingBlock(score_t* res) const noexcept {
    _impl->ScorePostingBlock(res);
  }

  IRS_FORCE_INLINE score_t Score() const noexcept { return _impl->Score(); }

  bool operator==(const ScoreFunction& rhs) const noexcept {
    return _impl == rhs._impl;
  }

 private:
  explicit ScoreFunction(memory::managed_ptr<ScoreOperator> impl) noexcept
    : _impl{std::move(impl)} {
    SDB_ASSERT(_impl);
  }

  memory::managed_ptr<ScoreOperator> _impl;
};

}  // namespace irs
