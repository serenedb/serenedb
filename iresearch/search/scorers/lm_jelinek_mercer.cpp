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

#include "lm_jelinek_mercer.hpp"

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

struct LmJmFormula {
  IRS_FORCE_INLINE score_t operator()(score_t tf, score_t dl) const noexcept {
    SDB_ASSERT(dl != 0.f);
    return std::log1p((num * tf / dl) * denom_inv) * boost;
  }

  score_t boost;
  score_t num;
  score_t denom_inv;
};

template<bool HasScale>
struct LmJmScore : public ScoreOperatorOf<LmJmScore<HasScale>> {
  LmJmScore(score_t boost, score_t lambda, const LMStats& stats,
            const FreqBlockAttr* freq, const uint32_t* norm,
            const score_t* scale) noexcept
    : freq{freq},
      norm{norm},
      scale{scale},
      formula{boost, 1.f - lambda, 1.f / (lambda * stats.collection_prob)} {}

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* res,
                                  scores_size_t n) const noexcept {
    ScoreFreqs<MergeType, HasScale>(res, n, freq->value, norm,
                                    TryGetValue(scale), formula);
  }

  const FreqBlockAttr* freq;
  const uint32_t* norm;
  [[no_unique_address]] utils::Need<HasScale, const score_t*> scale;
  LmJmFormula formula;
};

}  // namespace

void LMJelinekMercer::collect(byte_type* stats_buf, const FieldCollector* field,
                              const TermCollector* term) const {
  CollectLMStats(*stats_cast(stats_buf), field, term);
}

ScoreFunction LMJelinekMercer::PrepareScorer(const ScoreContext& ctx) const {
  return PrepareLMScorer<LmJmScore>(ctx, *stats_cast(ctx.stats), _lambda);
}

std::string LMJelinekMercer::ToString() const {
  return absl::StrCat("lm_jm(lambda=", _lambda, ")");
}

bool LMJelinekMercer::equals(const Scorer& other) const noexcept {
  if (!Scorer::equals(other)) {
    return false;
  }
  const auto& p = irs::utils::downCast<LMJelinekMercer>(other);
  return p._lambda == _lambda;
}

ScoreBoundWriter::ptr LMJelinekMercer::PrepareScoreBoundWriter() const {
  return std::make_unique<FreqNormWriter<kScoreBoundDivNorm>>();
}

ScoreBoundSource::ptr LMJelinekMercer::PrepareScoreBoundSource() const {
  return std::make_unique<FreqNormSource<kScoreBoundFreq | kScoreBoundNorm>>();
}

bool LMJelinekMercer::Compatible(
  const ScorerOptions& persisted) const noexcept {
  return irs::BoundTypeOf(persisted) == BoundTypeOf(Options{});
}

}  // namespace irs
