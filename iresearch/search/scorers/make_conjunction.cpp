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

#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::detail {
namespace {

template<ScoreMergeType Inner, bool HasConst>
class ConjunctionScore
  : public ScoreOperatorOf<ConjunctionScore<Inner, HasConst>, false> {
  static_assert(Inner != ScoreMergeType::Noop);

 public:
  explicit ConjunctionScore(score_t constant,
                            std::vector<ScoreFunction>&& children) noexcept
    : _constant{constant}, _children{std::move(children)} {
    SDB_ASSERT(_children.size() > (HasConst ? 0 : 1));
  }

  score_t ScoreOne() const noexcept {
    if constexpr (HasConst) {
      auto res = _constant;
      for (const auto& child : _children) {
        irs::Merge<Inner>(res, child.Score());
      }
      return res;
    } else {
      auto child = _children.begin();
      const auto end = _children.end();
      auto res = child->Score();
      for (++child; child != end; ++child) {
        irs::Merge<Inner>(res, child->Score());
      }
      return res;
    }
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
      Gather<Outer, Inner, Block>(res, n);
    } else {
      SDB_ASSERT(n <= kScoreBlock);
      score_t own[kScoreBlock];
      Gather<ScoreMergeType::Noop, Inner, Block>(own, n);
      irs::Merge<Outer>(res, own, n);
    }
  }

  template<ScoreMergeType Head, ScoreMergeType Tail, bool Block>
  IRS_FORCE_INLINE void Gather(score_t* dst, scores_size_t n) const noexcept {
    if constexpr (HasConst) {
      for (scores_size_t i = 0; i != n; ++i) {
        irs::Merge<Head>(dst[i], _constant);
      }
      for (const auto& child : _children) {
        ScoreChild<Tail, Block>(child, dst, n);
      }
    } else {
      auto child = _children.begin();
      const auto end = _children.end();
      ScoreChild<Head, Block>(*child, dst, n);
      for (++child; child != end; ++child) {
        ScoreChild<Tail, Block>(*child, dst, n);
      }
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

  [[no_unique_address]] utils::Need<HasConst, score_t> _constant;
  std::vector<ScoreFunction> _children;
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
      return ScoreFunction::Make<ConjunctionScore<M, true>>(
        constant, std::move(children));
    });
  }
  if (children.empty()) {
    return ScoreFunction::Default();
  }
  if (children.size() == 1) {
    return std::move(children.front());
  }
  return Resolve(inner, [&]<ScoreMergeType M> {
    return ScoreFunction::Make<ConjunctionScore<M, false>>(constant,
                                                           std::move(children));
  });
}

}  // namespace irs::detail
