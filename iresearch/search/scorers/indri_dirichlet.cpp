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

#include "indri_dirichlet.hpp"

#include <absl/strings/str_cat.h>

#include <cmath>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/score_inputs.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/misc.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {
namespace {

struct IndriFormula {
  IRS_FORCE_INLINE score_t operator()(score_t tf, score_t dl) const noexcept {
    return std::log((tf + mu_p) / (dl + mu)) * boost;
  }

  score_t boost;
  score_t mu;
  score_t mu_p;
};

template<bool HasScale>
struct IndriScore : public ScoreOperatorOf<IndriScore<HasScale>> {
  IndriScore(score_t boost, score_t mu, const LMStats& stats,
             const FreqBlockAttr* freq, const uint32_t* norm,
             const score_t* scale) noexcept
    : freq{freq},
      norm{norm},
      scale{scale},
      formula{boost, mu, mu * stats.collection_prob} {}

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* res,
                                  scores_size_t n) const noexcept {
    ScoreFreqs<MergeType, HasScale>(res, n, freq->value, norm,
                                    TryGetValue(scale), formula);
  }

  const FreqBlockAttr* freq;
  const uint32_t* norm;
  [[no_unique_address]] utils::Need<HasScale, const score_t*> scale;
  IndriFormula formula;
};

}  // namespace

void IndriDirichlet::collect(byte_type* stats_buf, const FieldCollector* field,
                             const TermCollector* term) const {
  CollectLMStats(*stats_cast(stats_buf), field, term);
}

ScoreFunction IndriDirichlet::PrepareScorer(const ScoreContext& ctx) const {
  if (_mu <= 0.f) {
    return ScoreFunction::Default();
  }
  return PrepareLMScorer<IndriScore>(ctx, *stats_cast(ctx.stats), _mu);
}

std::string IndriDirichlet::ToString() const {
  return absl::StrCat("indri_dirichlet(mu=", _mu, ")");
}

bool IndriDirichlet::equals(const Scorer& other) const noexcept {
  if (!Scorer::equals(other)) {
    return false;
  }
  const auto& p = irs::utils::downCast<IndriDirichlet>(other);
  return p._mu == _mu;
}

}  // namespace irs
