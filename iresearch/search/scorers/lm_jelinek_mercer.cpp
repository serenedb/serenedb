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

#include <absl/container/inlined_vector.h>
#include <absl/strings/str_cat.h>

#include <cmath>
#include <span>

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

LmJmFormula LmJmOf(score_t boost, score_t lambda, score_t share) noexcept {
  return {boost, 1.f - lambda, 1.f / (lambda * share)};
}

struct LmJmTermsFormula {
  IRS_FORCE_INLINE score_t operator()(score_t tf, score_t dl) const noexcept {
    SDB_ASSERT(dl != 0.f);
    const double x = num * tf / dl;
    const double* const denom_inv = denom_invs.data();
    return static_cast<score_t>(
             LogOfProduct(denom_invs.size(),
                          [&](size_t i) { return 1 + x * denom_inv[i]; })) *
           boost;
  }

  score_t boost;
  score_t num;
  absl::InlinedVector<double, 4> denom_invs;
};

LmJmTermsFormula LmJmTermsOf(score_t boost, score_t lambda,
                             std::span<const score_t> shares) {
  LmJmTermsFormula formula{.boost = boost, .num = 1.f - lambda};
  formula.denom_invs.reserve(shares.size());
  for (const auto share : shares) {
    formula.denom_invs.push_back(1 / (static_cast<double>(lambda) * share));
  }
  return formula;
}

}  // namespace

void LMJelinekMercer::collect(byte_type* stats_buf, const FieldCollector* field,
                              const TermCollector* term,
                              StatsArena& arena) const {
  CollectShare(*stats_cast(stats_buf), field, term, arena);
}

ScoreFunction LMJelinekMercer::PrepareScorer(const ScoreContext& ctx) const {
  return PrepareShareScorer(
    ctx, *stats_cast(ctx.stats),
    [&](score_t share) { return LmJmOf(ctx.boost, _lambda, share); },
    [&](std::span<const score_t> shares) {
      return LmJmTermsOf(ctx.boost, _lambda, shares);
    });
}

void LMJelinekMercer::Explain(const ScoreContext& ctx,
                              const explain::SlotStats* stats,
                              explain::Node& out) const {
  explain::ExplainLanguageModel(
    ctx, SharesOf(*stats_cast(ctx.stats)), stats,
    {.key = "lambda", .value = _lambda},
    "boost * log(1 + ((1 - lambda) * freq / dl) / (lambda * p))",
    [&](score_t tf, score_t dl, score_t share) {
      return LmJmOf(ctx.boost, _lambda, share)(tf, dl);
    },
    out);
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
