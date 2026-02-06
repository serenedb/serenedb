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
  using ScoreF = void (*)(ScoreCtx* ctx, score_t* res, size_t n) noexcept;
  using Score2F = void (*)(ScoreCtx* ctx, std::pair<doc_id_t, score_t>* res,
                           size_t n) noexcept;
  using MinF = void (*)(ScoreCtx* ctx, score_t min) noexcept;
  using DeleterF = void (*)(ScoreCtx* ctx) noexcept;
  static void NoopDelete(ScoreCtx* /*ctx*/) noexcept {}

 public:
  static void DefaultScore(ScoreCtx* ctx, score_t* res, size_t n) noexcept;
  static void DefaultScore2(ScoreCtx* ctx, std::pair<doc_id_t, score_t>* res,
                            size_t n) noexcept;
  static void NoopMin(ScoreCtx* /*ctx*/, score_t /*min*/) noexcept {}

  static ScoreFunction Noop() noexcept {
    return {nullptr, DefaultScore, DefaultScore2, NoopMin, NoopDelete};
  }
  static ScoreFunction Default();
  static ScoreFunction Constant(score_t value) noexcept;

  template<typename T, typename... Args>
  static auto Make(ScoreF score, Score2F score2, MinF min, Args&&... args) {
    return ScoreFunction{
      new T{std::forward<Args>(args)...}, score, score2, min,
      [](ScoreCtx* ctx) noexcept { delete static_cast<T*>(ctx); }};
  }

  ScoreFunction() noexcept = default;
  ScoreFunction(ScoreCtx* ctx, ScoreF score, Score2F score2,
                MinF min = NoopMin) noexcept
    : ScoreFunction{ctx, score, score2, min, NoopDelete} {}
  ScoreFunction(ScoreFunction&& rhs) noexcept
    : ScoreFunction{std::exchange(rhs._ctx, nullptr),
                    std::exchange(rhs._score, DefaultScore),
                    std::exchange(rhs._score2, DefaultScore2),
                    std::exchange(rhs._min, NoopMin),
                    std::exchange(rhs._deleter, NoopDelete)} {}
  ScoreFunction& operator=(ScoreFunction&& rhs) noexcept {
    if (this != &rhs) [[likely]] {
      std::swap(_ctx, rhs._ctx);
      std::swap(_score, rhs._score);
      std::swap(_score2, rhs._score2);
      std::swap(_min, rhs._min);
      std::swap(_deleter, rhs._deleter);
    }
    return *this;
  }
  ~ScoreFunction() noexcept { _deleter(_ctx); }

  void Reset(ScoreCtx& ctx, ScoreF score, Score2F score2,
             MinF min = NoopMin) noexcept {
    SDB_ASSERT(&ctx != _ctx || _deleter == NoopDelete);
    _deleter(_ctx);
    _ctx = &ctx;
    _score = score;
    _score2 = score2;
    _min = min;
    _deleter = NoopDelete;
  }

  bool IsDefault() const noexcept { return _score == DefaultScore; }

  IRS_FORCE_INLINE void Score(score_t* res, size_t n) const noexcept {
    SDB_ASSERT(_score != nullptr);
    _score(_ctx, res, n);
  }

  IRS_FORCE_INLINE void Score2(std::pair<doc_id_t, score_t>* res,
                               size_t n) const noexcept {
    SDB_ASSERT(_score != nullptr);
    _score2(_ctx, res, n);
  }

  IRS_FORCE_INLINE void Min(score_t arg) const noexcept {
    SDB_ASSERT(_min != nullptr);
    _min(_ctx, arg);
  }

  score_t Max() const noexcept;

  // TODO(mbkkt) Remove it, use Score
  IRS_FORCE_INLINE void operator()(score_t* res) const noexcept {
    Score(res, 1);
  }

  bool operator==(const ScoreFunction& rhs) const noexcept {
    return _ctx == rhs._ctx && _score == rhs._score && _score2 == rhs._score2 &&
           _min == rhs._min;
  }

  [[nodiscard]] auto* Ctx() const noexcept { return _ctx; }
  [[nodiscard]] auto Func() const noexcept { return _score; }

 private:
  ScoreFunction(ScoreCtx* ctx, ScoreF score, Score2F score2, MinF min,
                DeleterF deleter) noexcept
    : _ctx{ctx}, _score{score}, _score2{score2}, _min{min}, _deleter{deleter} {}

  ScoreCtx* _ctx = nullptr;
  ScoreF _score = DefaultScore;
  Score2F _score2 = DefaultScore2;
  MinF _min = NoopMin;
  DeleterF _deleter = NoopDelete;
};

}  // namespace irs
