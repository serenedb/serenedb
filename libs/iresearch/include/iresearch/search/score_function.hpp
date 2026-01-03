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

#include <iresearch/types.hpp>

#include "basics/memory.hpp"
#include "basics/noncopyable.hpp"

namespace irs {

// Stateful object used for computing the document score
// based on the stored state.
struct ScoreCtx {
  ScoreCtx() = default;
  ScoreCtx(ScoreCtx&&) = default;
  ScoreCtx& operator=(ScoreCtx&&) = default;

 protected:
  ~ScoreCtx() = default;
};

// Convenient wrapper around score_ctx, score_f and min_f.
class ScoreFunction : util::Noncopyable {
  using score_f = void (*)(ScoreCtx* ctx, score_t* res) noexcept;
  using min_f = void (*)(ScoreCtx* ctx, score_t min) noexcept;
  using collect_f = void (*)(ScoreCtx* ctx) noexcept;
  using deleter_f = void (*)(ScoreCtx* ctx) noexcept;
  static void NoopDelete(ScoreCtx* /*ctx*/) noexcept {}

 public:
  static void DefaultScore(ScoreCtx* ctx, score_t* res) noexcept;
  static void NoopScore(ScoreCtx* /*ctx*/, score_t* /*res*/) noexcept {}
  static void NoopMin(ScoreCtx* /*ctx*/, score_t /*min*/) noexcept {}
  static void NoopCollect(ScoreCtx* /*ctx*/) noexcept {}

  static ScoreFunction Noop() noexcept {
    return {nullptr, NoopScore, NoopCollect, NoopMin, NoopDelete};
  }
  static ScoreFunction Default();
  static ScoreFunction Constant(score_t value) noexcept;

  template<typename T, typename... Args>
  static auto Make(score_f score, collect_f collect, min_f min,
                   Args&&... args) {
    return ScoreFunction{
      new T{std::forward<Args>(args)...}, score, collect, min,
      [](ScoreCtx* ctx) noexcept { delete static_cast<T*>(ctx); }};
  }

  ScoreFunction() noexcept = default;
  ScoreFunction(ScoreCtx& ctx, score_f score, collect_f collect,
                min_f min = NoopMin) noexcept
    : ScoreFunction{&ctx, score, collect, min, NoopDelete} {}
  ScoreFunction(ScoreFunction&& rhs) noexcept
    : ScoreFunction{std::exchange(rhs._ctx, nullptr),
                    std::exchange(rhs._score, NoopScore),
                    std::exchange(rhs._collect, NoopCollect),
                    std::exchange(rhs._min, NoopMin),
                    std::exchange(rhs._deleter, NoopDelete)} {}
  ScoreFunction& operator=(ScoreFunction&& rhs) noexcept {
    if (this != &rhs) [[likely]] {
      std::swap(_ctx, rhs._ctx);
      std::swap(_score, rhs._score);
      std::swap(_collect, rhs._collect);
      std::swap(_min, rhs._min);
      std::swap(_deleter, rhs._deleter);
    }
    return *this;
  }
  ~ScoreFunction() noexcept { _deleter(_ctx); }

  void Reset(ScoreCtx& ctx, score_f score, collect_f collect,
             min_f min = NoopMin) noexcept {
    SDB_ASSERT(&ctx != _ctx || _deleter == NoopDelete);
    _deleter(_ctx);
    _ctx = &ctx;
    _score = score;
    _collect = collect;
    _min = min;
    _deleter = NoopDelete;
  }

  bool IsDefault() const noexcept {
    // TOOD(gnusi): use only one
    return _score == NoopScore || _score == DefaultScore;
  }

  IRS_FORCE_INLINE void Score(score_t* res) const noexcept {
    SDB_ASSERT(_score != nullptr);
    _score(_ctx, res);
  }

  IRS_FORCE_INLINE void Collect() const noexcept {
    SDB_ASSERT(_collect != nullptr);
    _collect(_ctx);
  }

  IRS_FORCE_INLINE void Min(score_t arg) const noexcept {
    SDB_ASSERT(_min != nullptr);
    _min(_ctx, arg);
  }

  score_t Max() const noexcept;

  // TODO(mbkkt) Remove it, use Score
  IRS_FORCE_INLINE void operator()(score_t* res) const noexcept { Score(res); }

  bool operator==(const ScoreFunction& rhs) const noexcept {
    return _ctx == rhs._ctx && _score == rhs._score && _min == rhs._min;
  }

#ifdef SDB_GTEST
  [[nodiscard]] ScoreCtx* Ctx() const noexcept { return _ctx; }
  [[nodiscard]] score_f Func() const noexcept { return _score; }
#endif

 private:
  ScoreFunction(ScoreCtx* ctx, score_f score, collect_f collect, min_f min,
                deleter_f deleter) noexcept
    : _ctx{ctx},
      _score{score},
      _collect{collect},
      _min{min},
      _deleter{deleter} {}

  ScoreCtx* _ctx = nullptr;
  score_f _score = NoopScore;
  collect_f _collect = NoopCollect;
  min_f _min = NoopMin;
  deleter_f _deleter = NoopDelete;
};

}  // namespace irs
