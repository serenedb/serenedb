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

#include "iresearch/search/scorers/make_conjunction.hpp"

#include <absl/base/optimization.h>

#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/containers/fixed.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::detail {
namespace {

template<ScoreMergeType Inner>
class ConjunctionScore
  : public ScoreOperatorOf<ConjunctionScore<Inner>, false> {
  static_assert(Inner != ScoreMergeType::Noop);

 public:
  explicit ConjunctionScore(std::vector<ScoreFunction>&& children) noexcept
    : _children{children.size(), [&](ScoreFunction& slot, size_t i) noexcept {
                  slot = std::move(children[i]);
                }} {
    SDB_ASSERT(_children.size() > 1);
  }

  score_t ScoreOne() const noexcept {
    auto child = _children.begin();
    const auto end = _children.end();

    auto res = child->Score();
    for (++child; child != end; ++child) {
      irs::Merge<Inner>(res, child->Score());
    }
    return res;
  }

  template<ScoreMergeType Outer = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* res,
                                  scores_size_t n) const noexcept {
    auto child = _children.begin();
    const auto end = _children.end();

    child->Score<Outer>(res, n);
    for (++child; child != end; ++child) {
      child->Score<Inner>(res, n);
    }
  }

  template<ScoreMergeType Outer>
  IRS_FORCE_INLINE void ScoreBlockImpl(score_t* res) const noexcept {
    auto child = _children.begin();
    const auto end = _children.end();

    child->ScoreBlock<Outer>(res);
    for (++child; child != end; ++child) {
      child->ScoreBlock<Inner>(res);
    }
  }

 private:
  containers::Fixed<ScoreFunction> _children;
};

template<ScoreMergeType Inner>
class ConjunctionConstScore
  : public ScoreOperatorOf<ConjunctionConstScore<Inner>, false> {
  static_assert(Inner != ScoreMergeType::Noop);

 public:
  ConjunctionConstScore(std::vector<ScoreFunction>&& children,
                        score_t constant) noexcept
    : _children{children.size(),
                [&](ScoreFunction& slot, size_t i) noexcept {
                  slot = std::move(children[i]);
                }},
      _constant{constant} {
    SDB_ASSERT(!_children.empty());
  }

  score_t ScoreOne() const noexcept {
    auto child = _children.begin();
    const auto end = _children.end();

    auto res = child->Score();
    for (++child; child != end; ++child) {
      irs::Merge<Inner>(res, child->Score());
    }
    irs::Merge<Inner>(res, _constant);
    return res;
  }

  template<ScoreMergeType Outer = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* res,
                                  scores_size_t n) const noexcept {
    if constexpr (Outer == ScoreMergeType::Noop) {
      Gather(res, n);
      for (scores_size_t i = 0; i != n; ++i) {
        irs::Merge<Inner>(res[i], _constant);
      }
    } else {
      Settle<Outer>(res, n);
    }
  }

 private:
  IRS_FORCE_INLINE void Gather(score_t* dst, scores_size_t n) const noexcept {
    auto child = _children.begin();
    const auto end = _children.end();

    child->Score(dst, n);
    for (++child; child != end; ++child) {
      child->Score<Inner>(dst, n);
    }
  }

  template<ScoreMergeType Outer>
  IRS_FORCE_INLINE void Settle(score_t* IRS_RESTRICT res,
                               scores_size_t n) const noexcept {
    Gather(_scratch, n);
    for (scores_size_t i = 0; i != n; ++i) {
      auto value = _scratch[i];
      irs::Merge<Inner>(value, _constant);
      irs::Merge<Outer>(res[i], value);
    }
  }

  containers::Fixed<ScoreFunction> _children;
  score_t _constant;
  ABSL_CACHELINE_ALIGNED mutable score_t _scratch[kScoreBlock];
};

template<template<ScoreMergeType> typename Score, typename... Args>
ScoreFunction Resolve(ScoreMergeType inner, Args&&... args) {
  if (inner == ScoreMergeType::Max) {
    return ScoreFunction::Make<Score<ScoreMergeType::Max>>(
      std::forward<Args>(args)...);
  }
  SDB_ASSERT(inner == ScoreMergeType::Sum);
  return ScoreFunction::Make<Score<ScoreMergeType::Sum>>(
    std::forward<Args>(args)...);
}

void DropDefaults(std::vector<ScoreFunction>& children) {
  std::erase_if(children, [](const ScoreFunction& s) { return s.IsDefault(); });
}

}  // namespace

ScoreFunction MakeConjunctionScore(ScoreMergeType inner,
                                   std::vector<ScoreFunction>&& children,
                                   score_t constant) {
  DropDefaults(children);
  if (constant != 0) {
    if (children.empty()) {
      return ScoreFunction::Constant(constant);
    }
    return Resolve<ConjunctionConstScore>(inner, std::move(children), constant);
  }
  if (children.empty()) {
    return ScoreFunction::Default();
  }
  if (children.size() == 1) {
    return std::move(children.front());
  }
  return Resolve<ConjunctionScore>(inner, std::move(children));
}

}  // namespace irs::detail
