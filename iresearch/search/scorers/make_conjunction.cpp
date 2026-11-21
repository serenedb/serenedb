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

#include <algorithm>

#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/containers/fixed.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::detail {
namespace {

using Children = containers::Fixed<ScoreFunction>;

template<ScoreMergeType Inner>
score_t ScoreChildren(const Children& children) noexcept {
  score_t res = 0;
  for (const auto& child : children) {
    irs::Merge<Inner>(res, child.Score());
  }
  return res;
}

template<ScoreMergeType Inner>
IRS_FORCE_INLINE void GatherChildren(const Children& children,
                                     score_t* IRS_RESTRICT dst,
                                     scores_size_t n) noexcept {
  auto child = children.begin();
  if constexpr (Inner == ScoreMergeType::Max) {
    std::fill_n(dst, n, score_t{0});
  } else {
    child->Score(dst, n);
    ++child;
  }
  for (const auto end = children.end(); child != end; ++child) {
    child->Score<Inner>(dst, n);
  }
}

template<ScoreMergeType Inner>
IRS_FORCE_INLINE void GatherChildrenBlock(const Children& children,
                                          score_t* IRS_RESTRICT dst) noexcept {
  auto child = children.begin();
  if constexpr (Inner == ScoreMergeType::Max) {
    std::fill_n(dst, kScoreBlock, score_t{0});
  } else {
    child->ScoreBlock(dst);
    ++child;
  }
  for (const auto end = children.end(); child != end; ++child) {
    child->ScoreBlock<Inner>(dst);
  }
}

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

  score_t ScoreOne() const noexcept { return ScoreChildren<Inner>(_children); }

  template<ScoreMergeType Outer = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* res,
                                  scores_size_t n) const noexcept {
    if constexpr (Outer == Inner) {
      for (const auto& child : _children) {
        child.Score<Inner>(res, n);
      }
    } else if constexpr (Outer == ScoreMergeType::Noop) {
      GatherChildren<Inner>(_children, res, n);
    } else {
      GatherChildren<Inner>(_children, _scratch, n);
      irs::Merge<Outer>(res, _scratch, n);
    }
  }

  template<ScoreMergeType Outer>
  IRS_FORCE_INLINE void ScoreBlockImpl(score_t* res) const noexcept {
    if constexpr (Outer == Inner) {
      for (const auto& child : _children) {
        child.ScoreBlock<Inner>(res);
      }
    } else if constexpr (Outer == ScoreMergeType::Noop) {
      GatherChildrenBlock<Inner>(_children, res);
    } else {
      GatherChildrenBlock<Inner>(_children, _scratch);
      irs::Merge<Outer>(res, _scratch, kScoreBlock);
    }
  }

 private:
  Children _children;
  ABSL_CACHELINE_ALIGNED mutable score_t _scratch[kScoreBlock];
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
    auto res = ScoreChildren<Inner>(_children);
    irs::Merge<Inner>(res, _constant);
    return res;
  }

  template<ScoreMergeType Outer = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* res,
                                  scores_size_t n) const noexcept {
    if constexpr (Outer == ScoreMergeType::Noop) {
      GatherChildren<Inner>(_children, res, n);
      for (scores_size_t i = 0; i != n; ++i) {
        irs::Merge<Inner>(res[i], _constant);
      }
    } else {
      Settle<Outer>(res, n);
    }
  }

 private:
  template<ScoreMergeType Outer>
  IRS_FORCE_INLINE void Settle(score_t* IRS_RESTRICT res,
                               scores_size_t n) const noexcept {
    GatherChildren<Inner>(_children, _scratch, n);
    for (scores_size_t i = 0; i != n; ++i) {
      auto value = _scratch[i];
      irs::Merge<Inner>(value, _constant);
      irs::Merge<Outer>(res[i], value);
    }
  }

  Children _children;
  score_t _constant;
  ABSL_CACHELINE_ALIGNED mutable score_t _scratch[kScoreBlock];
};

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
    return Resolve(inner, [&]<ScoreMergeType M> {
      return ScoreFunction::Make<ConjunctionConstScore<M>>(std::move(children),
                                                           constant);
    });
  }
  if (children.empty()) {
    return ScoreFunction::Default();
  }
  if (children.size() == 1) {
    return std::move(children.front());
  }
  return Resolve(inner, [&]<ScoreMergeType M> {
    return ScoreFunction::Make<ConjunctionScore<M>>(std::move(children));
  });
}

}  // namespace irs::detail
