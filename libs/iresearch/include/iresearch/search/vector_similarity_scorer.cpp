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

#include "iresearch/search/vector_similarity_scorer.hpp"

#include "basics/assert.h"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/search/score_function.hpp"

namespace irs {
namespace {

class VectorScore : public ScoreOperator {
 public:
  VectorScore(const score_t* block, score_t boost) noexcept
    : _block{block}, _boost{boost} {
    SDB_ASSERT(block);
  }

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* res, size_t n) const noexcept {
    for (scores_size_t i = 0; i != n; ++i) {
      Merge<MergeType>(res[i], _block[i] * _boost);
    }
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

 private:
  const score_t* _block;
  score_t _boost;
};

}  // namespace

ScoreFunction VectorSimilarityScorer::PrepareScorer(
  const ScoreContext& ctx) const {
  const auto* block = irs::get<BoostBlockAttr>(ctx.doc_attrs);
  if (!block || !block->value) {
    return ScoreFunction::Constant(ctx.boost);
  }
  return ScoreFunction::Make<VectorScore>(block->value, ctx.boost);
}

}  // namespace irs
