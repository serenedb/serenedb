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

#include <absl/container/inlined_vector.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <span>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/explain/factors.hpp"
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

IndriFormula IndriOf(score_t boost, score_t mu, score_t share) noexcept {
  return {boost, mu, mu * share};
}

struct IndriTermsFormula {
  template<ScoreMergeType MergeType, bool HasScale>
  IRS_FORCE_INLINE void ScoreBlock(
    score_t* IRS_RESTRICT res, scores_size_t n,
    const uint32_t* IRS_RESTRICT freq, const uint32_t* IRS_RESTRICT norm,
    [[maybe_unused]] const score_t* IRS_RESTRICT scale) const noexcept {
    score_t tf[kScoreBlock];
    score_t doc_inv[kScoreBlock];
    score_t product[kScoreBlock];
    for (scores_size_t begin = 0; begin < n; begin += kScoreBlock) {
      const auto size = std::min<scores_size_t>(kScoreBlock, n - begin);
      for (scores_size_t i = 0; i != size; ++i) {
        tf[i] = ScaledFreq<HasScale>(freq, scale, begin + i);
        doc_inv[i] = 1.f / (TermCountToScore(norm[begin + i]) + mu);
        product[i] = 1.f;
      }
      for (const auto mu_p : mu_ps) {
        for (scores_size_t i = 0; i != size; ++i) {
          product[i] *= (tf[i] + mu_p) * doc_inv[i];
        }
      }
      for (scores_size_t i = 0; i != size; ++i) {
        Merge<MergeType>(res[begin + i],
                         LogOf(product[i], tf[i], doc_inv[i]) * boost);
      }
    }
  }

  IRS_FORCE_INLINE score_t LogOf(score_t product, score_t tf,
                                 score_t doc_inv) const noexcept {
    if (product >= std::numeric_limits<score_t>::min() &&
        product <= std::numeric_limits<score_t>::max()) [[likely]] {
      return std::log(product);
    }
    return static_cast<score_t>(LogOfProductInChunks(
      mu_ps.size(), [&](size_t i) { return (tf + mu_ps[i]) * doc_inv; }));
  }

  score_t boost;
  score_t mu;
  absl::InlinedVector<score_t, 4> mu_ps;
};

IndriTermsFormula IndriTermsOf(score_t boost, score_t mu,
                               std::span<const score_t> shares) {
  IndriTermsFormula formula{.boost = boost, .mu = mu};
  formula.mu_ps.reserve(shares.size());
  for (const auto share : shares) {
    formula.mu_ps.push_back(mu * share);
  }
  return formula;
}

}  // namespace

void IndriDirichlet::collect(byte_type* stats_buf, const FieldCollector* field,
                             const TermCollector* term,
                             StatsArena& arena) const {
  CollectShare(*stats_cast(stats_buf), field, term, arena);
}

ScoreFunction IndriDirichlet::PrepareScorer(const ScoreContext& ctx) const {
  if (_mu <= 0.f) {
    return ScoreFunction::Default();
  }
  return PrepareLMScorer(
    ctx, *stats_cast(ctx.stats),
    [&](score_t share) { return IndriOf(ctx.boost, _mu, share); },
    [&](std::span<const score_t> shares) {
      return IndriTermsOf(ctx.boost, _mu, shares);
    });
}

void IndriDirichlet::Explain(const ScoreContext& ctx,
                             const explain::SlotStats* stats,
                             explain::Node& out) const {
  if (_mu <= 0.f) {
    out.note = "mu is not positive, so the clause scores 0";
    return;
  }
  explain::ExplainLanguageModel(
    ctx, SharesOf(*stats_cast(ctx.stats)), stats, {.key = "mu", .value = _mu},
    "boost * log((freq + mu * p) / (dl + mu))",
    [&](score_t tf, score_t dl, score_t share) {
      return IndriOf(ctx.boost, _mu, share)(tf, dl);
    },
    out);
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
