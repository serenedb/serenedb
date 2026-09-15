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

#include "raw_tf.hpp"

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/score_bound_writer.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/search/scorers/scorer_options.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/misc.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {
namespace {

template<typename T>
constexpr const T* TryGetValue(const T* value) noexcept {
  return value;
}

constexpr std::nullptr_t TryGetValue(utils::Empty) noexcept { return nullptr; }

template<ScoreMergeType MergeType, bool HasScale>
IRS_FORCE_INLINE void RawTfImpl(
  score_t* IRS_RESTRICT res, scores_size_t n, const uint32_t* IRS_RESTRICT freq,
  [[maybe_unused]] const score_t* IRS_RESTRICT scale, score_t num) noexcept {
  for (scores_size_t i = 0; i != n; ++i) {
    Merge<MergeType>(res[i], num * ScaledFreq<HasScale>(freq, scale, i));
  }
}

template<bool HasScale>
struct RawTfScore : public ScoreOperator {
  RawTfScore(score_t boost, const FreqBlockAttr* freq,
             const score_t* scale) noexcept
    : freq{freq}, scale{scale}, boost{boost} {
    SDB_ASSERT(this->freq);
  }

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* IRS_RESTRICT res,
                                  scores_size_t n) const noexcept {
    RawTfImpl<MergeType, HasScale>(res, n, freq->value, TryGetValue(scale),
                                   boost);
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

  const FreqBlockAttr* freq;
  [[no_unique_address]] utils::Need<HasScale, const score_t*> scale;
  score_t boost;
};

}  // namespace

ScoreFunction RawTF::PrepareScorer(const ScoreContext& ctx) const {
  auto* freq = irs::get<FreqBlockAttr>(ctx.doc_attrs);
  if (!freq) {
    if (0.f == ctx.boost) {
      return ScoreFunction::Default();
    }
    return ScoreFunction::Default();
  }

  auto* scale = [&] {
    auto* attr = irs::get<ScaleBlockAttr>(ctx.doc_attrs);
    return attr ? attr->value : nullptr;
  }();

  return ResolveBool(scale != nullptr, [&]<bool HasScale>() {
    return ScoreFunction::Make<RawTfScore<HasScale>>(ctx.boost, freq, scale);
  });
}

ScoreBoundWriter::ptr RawTF::PrepareScoreBoundWriter(size_t max_levels) const {
  return std::make_unique<FreqNormWriter<kScoreBoundMaxFreq>>(max_levels);
}

ScoreBoundSource::ptr RawTF::PrepareScoreBoundSource() const {
  return std::make_unique<FreqNormSource<kScoreBoundFreq>>();
}

bool RawTF::Compatible(const ScorerOptions& persisted) const noexcept {
  return irs::BoundTypeOf(persisted) == BoundTypeOf(Options{});
}

}  // namespace irs
