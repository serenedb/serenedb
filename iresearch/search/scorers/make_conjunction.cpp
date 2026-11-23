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

#include <algorithm>

#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/containers/fixed.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::detail {
namespace {

template<ScoreMergeType Inner, bool HasConst>
class ConjunctionScore
  : public ScoreOperatorOf<ConjunctionScore<Inner, HasConst>, false> {
  static_assert(Inner != ScoreMergeType::Noop);
  static constexpr bool kStarts = HasConst || Inner == ScoreMergeType::Max;

 public:
  ConjunctionScore(std::vector<ScoreFunction>&& children,
                   score_t constant) noexcept
    : _children{children.size(),
                [&](ScoreFunction& slot, size_t i) noexcept {
                  slot = std::move(children[i]);
                }},
      _constant{constant} {
    SDB_ASSERT(kStarts ? !_children.empty() : _children.size() > 1);
  }

  score_t ScoreOne() const noexcept {
    auto child = _children.begin();
    const auto end = _children.end();

    auto res = kStarts ? Start() : (child++)->Score();
    for (; child != end; ++child) {
      irs::Merge<Inner>(res, child->Score());
    }
    return res;
  }

  template<ScoreMergeType Outer = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* res,
                                  scores_size_t n) const noexcept {
    Settle<Outer, false>(res, n);
  }

  template<ScoreMergeType Outer>
  IRS_FORCE_INLINE void ScoreBlockImpl(score_t* res) const noexcept {
    Settle<Outer, true>(res, kScoreBlock);
  }

 private:
  template<ScoreMergeType Outer, bool Block>
  IRS_FORCE_INLINE void Settle(score_t* res, scores_size_t n) const noexcept {
    if constexpr (Outer == ScoreMergeType::Noop || Outer == Inner) {
      Gather<Outer, Block>(res, n);
    } else {
      SDB_ASSERT(n <= kScoreBlock);
      score_t own[kScoreBlock];
      Gather<ScoreMergeType::Noop, Block>(own, n);
      irs::Merge<Outer>(res, own, n);
    }
  }

  template<ScoreMergeType Into, bool Block>
  IRS_FORCE_INLINE void Gather(score_t* dst, scores_size_t n) const noexcept {
    auto child = _children.begin();
    const auto end = _children.end();

    if constexpr (Into == Inner) {
      if constexpr (HasConst) {
        for (scores_size_t i = 0; i != n; ++i) {
          irs::Merge<Inner>(dst[i], _constant);
        }
      }
    } else if constexpr (kStarts) {
      std::fill_n(dst, n, Start());
    } else {
      ScoreChild<ScoreMergeType::Noop, Block>(*child++, dst, n);
    }
    for (; child != end; ++child) {
      ScoreChild<Inner, Block>(*child, dst, n);
    }
  }

  score_t Start() const noexcept {
    if constexpr (HasConst) {
      return _constant;
    } else {
      return 0;
    }
  }

  template<ScoreMergeType M, bool Block>
  IRS_FORCE_INLINE static void ScoreChild(const ScoreFunction& child,
                                          score_t* dst,
                                          scores_size_t n) noexcept {
    if constexpr (Block) {
      child.ScoreBlock<M>(dst);
    } else {
      child.Score<M>(dst, n);
    }
  }

  containers::Fixed<ScoreFunction> _children;
  [[no_unique_address]] utils::Need<HasConst, score_t> _constant;
};

void DropDefaults(std::vector<ScoreFunction>& children) {
  std::erase_if(children, [](const ScoreFunction& s) { return s.IsDefault(); });
}

}  // namespace

ScoreFunction MakeConjunctionScore(ScoreMergeType inner,
                                   std::vector<ScoreFunction>&& children,
                                   score_t constant) {
  DropDefaults(children);
  if (children.empty()) {
    return constant != 0 ? ScoreFunction::Constant(constant)
                         : ScoreFunction::Default();
  }
  if (constant == 0 && children.size() == 1 && inner == ScoreMergeType::Sum) {
    return std::move(children.front());
  }
  return Resolve(constant != 0, [&]<bool HasConst> {
    return Resolve(inner, [&]<ScoreMergeType M> {
      return ScoreFunction::Make<ConjunctionScore<M, HasConst>>(
        std::move(children), constant);
    });
  });
}

}  // namespace irs::detail
