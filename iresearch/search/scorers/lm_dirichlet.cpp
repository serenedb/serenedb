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

#include <algorithm>
#include <cmath>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/score_bound_writer.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/explain/factors.hpp"
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

struct LmDirFormula {
  IRS_FORCE_INLINE score_t operator()(score_t tf, score_t dl) const noexcept {
    const score_t weight = std::log1p(tf * mu_p_inv);
    const score_t doc_norm = std::log1p(dl * mu_inv);
    return std::max(weight - doc_norm, 0.f) * boost;
  }

  score_t boost;
  score_t mu_inv;
  score_t mu_p_inv;
};

LmDirFormula LmDirOf(score_t boost, score_t mu, score_t share) noexcept {
  return {boost, 1.f / mu, 1.f / (mu * share)};
}

}  // namespace

void LMDirichlet::collect(byte_type* stats_buf, const FieldCollector* field,
                          const TermCollector* term) const {
  CollectLMStats(*stats_cast(stats_buf), field, term);
}

void LMDirichlet::ReserveTerms(byte_type* stats_buf, size_t terms,
                               StatsArena& arena) const {
  ReserveLMTerms(*stats_cast(stats_buf), terms, arena);
}

ScoreFunction LMDirichlet::PrepareScorer(const ScoreContext& ctx) const {
  if (_mu <= 0.f) {
    return ScoreFunction::Default();
  }
  return PrepareLMScorer(ctx, *stats_cast(ctx.stats), [&](score_t share) {
    return LmDirOf(ctx.boost, _mu, share);
  });
}

void LMDirichlet::Explain(const ScoreContext& ctx,
                          const explain::SlotStats* stats,
                          explain::Node& out) const {
  if (_mu <= 0.f) {
    out.note = "mu is not positive, so the clause scores 0";
    return;
  }
  explain::ExplainLanguageModel(
    ctx, SharesOf(*stats_cast(ctx.stats)), stats, {.key = "mu", .value = _mu},
    "boost * max(0, log(1 + freq / (mu * p)) - log(1 + dl / mu))",
    [&](score_t tf, score_t dl, score_t share) {
      return LmDirOf(ctx.boost, _mu, share)(tf, dl);
    },
    out);
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
