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

#include "lm_dirichlet.hpp"

#include <absl/strings/str_cat.h>

#include <cmath>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/score_bound_writer.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/score_inputs.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/search/scorers/scorer_options.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/misc.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {
namespace {

template<ScoreMergeType MergeType, bool HasScale>
IRS_FORCE_INLINE void LmDirImpl(
  score_t* IRS_RESTRICT res, scores_size_t n, const uint32_t* IRS_RESTRICT freq,
  const uint32_t* IRS_RESTRICT norm,
  [[maybe_unused]] const score_t* IRS_RESTRICT scale, score_t mu_p_inv,
  score_t mu_inv, score_t const_boost) noexcept {
  for (scores_size_t i = 0; i != n; ++i) {
    const score_t tf = ScaledFreq<HasScale>(freq, scale, i);
    const score_t dl = TermCountToScore(norm[i]);
    const score_t weight = std::log1p(tf * mu_p_inv);
    const score_t doc_norm = std::log1p(dl * mu_inv);
    score_t r = weight - doc_norm;
    if (r < 0.f) {
      r = 0.f;
    }
    r *= const_boost;
    Merge<MergeType>(res[i], r);
  }
}

template<bool HasScale>
struct LmDirScore : public ScoreOperatorOf<LmDirScore<HasScale>> {
  LmDirScore(score_t boost, score_t mu, const LMStats& stats,
             const FreqBlockAttr* freq, const uint32_t* norm,
             const score_t* scale) noexcept
    : freq{freq},
      norm{norm},
      scale{scale},
      boost{boost},
      mu_inv{1.f / mu},
      mu_p_inv{1.f / (mu * stats.collection_prob)} {}

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* res,
                                  scores_size_t n) const noexcept {
    LmDirImpl<MergeType, HasScale>(res, n, freq->value, norm,
                                   TryGetValue(scale), mu_p_inv, mu_inv, boost);
  }

  const FreqBlockAttr* freq;
  const uint32_t* norm;
  [[no_unique_address]] utils::Need<HasScale, const score_t*> scale;
  score_t boost;
  score_t mu_inv;
  score_t mu_p_inv;
};

}  // namespace

void LMDirichlet::collect(byte_type* stats_buf, const FieldCollector* field,
                          const TermCollector* term) const {
  CollectLMStats(*stats_cast(stats_buf), field, term);
}

ScoreFunction LMDirichlet::PrepareScorer(const ScoreContext& ctx) const {
  if (_mu <= 0.f) {
    return ScoreFunction::Default();
  }
  return PrepareLMScorer<LmDirScore>(ctx, *stats_cast(ctx.stats), _mu);
}

std::string LMDirichlet::ToString() const {
  return absl::StrCat("lm_dirichlet(mu=", _mu, ")");
}

bool LMDirichlet::equals(const Scorer& other) const noexcept {
  if (!Scorer::equals(other)) {
    return false;
  }
  const auto& p = irs::utils::downCast<LMDirichlet>(other);
  return p._mu == _mu;
}

ScoreBoundWriter::ptr LMDirichlet::PrepareScoreBoundWriter() const {
  return std::make_unique<FreqNormWriter<kScoreBoundMinNorm>>();
}

ScoreBoundSource::ptr LMDirichlet::PrepareScoreBoundSource() const {
  return std::make_unique<FreqNormSource<kScoreBoundFreq | kScoreBoundNorm>>();
}

bool LMDirichlet::Compatible(const ScorerOptions& persisted) const noexcept {
  return irs::BoundTypeOf(persisted) == BoundTypeOf(Options{}) &&
         !std::get_if<ScorerOptions::Bm25>(&persisted.params);
}

}  // namespace irs
