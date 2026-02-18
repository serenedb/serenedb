////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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

#include "boost_scorer.hpp"

#include <absl/container/inlined_vector.h>

#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/score_function.hpp"

namespace irs {
namespace {

Scorer::ptr MakeJson(std::string_view /*args*/) {
  return std::make_unique<BoostScore>();
}

template<ScoreMergeType MergeType>
IRS_FORCE_INLINE void Impl(score_t* IRS_RESTRICT res, scores_size_t n,
                           const score_t* IRS_RESTRICT volatile_boost,
                           score_t boost) noexcept {
  for (scores_size_t i = 0; i != n; ++i) {
    const auto r = volatile_boost[i] * boost;
    Merge<MergeType>(res[i], r);
  }
}

class VolatileBoostScore : public ScoreOperator {
 public:
  VolatileBoostScore(const score_t* volatile_boost, score_t boost) noexcept
    : _const_boost{boost}, _volatile_boost{volatile_boost} {
    SDB_ASSERT(volatile_boost);
  }

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* res, size_t n) const noexcept {
    Impl<MergeType>(res, n, _volatile_boost, _const_boost);
  }

  score_t Score() const noexcept final {
    score_t res{};
    ScoreImpl(&res, 1);
    return res;
  }

  void Score(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl(res, n);
  }
  void ScoreSum(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, n);
  }
  void ScoreMax(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, n);
  }

  void ScoreBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kScoreBlock);
  }
  void ScoreSumBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, kScoreBlock);
  }
  void ScoreMaxBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, kScoreBlock);
  }

  void ScorePostingBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kPostingBlock);
  }

 private:
  score_t _const_boost;
  const score_t* _volatile_boost;
};

}  // namespace

ScoreFunction BoostScore::PrepareScorer(const ScoreContext& ctx) const {
  const auto* volatile_boost = irs::get<BoostBlockAttr>(ctx.doc_attrs);

  if (!volatile_boost) {
    return ScoreFunction::Constant(ctx.boost);
  }

  return ScoreFunction::Make<VolatileBoostScore>(volatile_boost->value,
                                                 ctx.boost);
}

void BoostScore::init() { REGISTER_SCORER_JSON(BoostScore, MakeJson); }

}  // namespace irs
