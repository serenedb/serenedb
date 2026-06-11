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

#include <cmath>

#include "basics/down_cast.h"
#include "basics/empty.hpp"
#include "basics/misc.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {
namespace {

template<typename T>
constexpr const T* TryGetValue(const T* value) noexcept {
  return value;
}

constexpr std::nullptr_t TryGetValue(utils::Empty /*value*/) noexcept {
  return nullptr;
}

template<ScoreMergeType MergeType, bool HasBoost>
IRS_FORCE_INLINE void LmDirImpl(
  score_t* IRS_RESTRICT res, scores_size_t n, const uint32_t* IRS_RESTRICT freq,
  const uint32_t* IRS_RESTRICT norm,
  [[maybe_unused]] const score_t* IRS_RESTRICT boost, score_t mu_p_inv,
  score_t mu, score_t const_boost) noexcept {
  // mu_p_inv = 1 / (mu * collection_prob)
  // score = log(1 + tf * mu_p_inv) + log(mu / (dl + mu))
  //       = log(1 + tf * mu_p_inv) - log((dl + mu) / mu)
  //       = log(1 + tf * mu_p_inv) - log1p(dl / mu)
  // clamp to 0
  for (scores_size_t i = 0; i != n; ++i) {
    const score_t tf = static_cast<score_t>(freq[i]);
    const score_t dl = static_cast<score_t>(norm[i]);
    const score_t weight = std::log1p(tf * mu_p_inv);
    const score_t doc_norm = std::log1p(dl / mu);  // = -log(mu/(dl+mu))
    score_t r = weight - doc_norm;
    if (r < 0.f) {
      r = 0.f;
    }
    if constexpr (HasBoost) {
      r *= const_boost * boost[i];
    } else {
      r *= const_boost;
    }
    Merge<MergeType>(res[i], r);
  }
}

template<bool HasFilterBoost>
struct LmDirScore : public ScoreOperator {
  LmDirScore(score_t boost, score_t mu, const LMStats& stats,
             const FreqBlockAttr* freq, const uint32_t* norm,
             const score_t* fb) noexcept
    : freq{freq},
      norm{norm},
      filter_boost{fb},
      boost{boost},
      mu{mu},
      mu_p_inv{1.f / (mu * stats.collection_prob)} {}

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* res,
                                  scores_size_t n) const noexcept {
    LmDirImpl<MergeType, HasFilterBoost>(res, n, freq->value, norm,
                                         TryGetValue(filter_boost), mu_p_inv,
                                         mu, boost);
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
  const uint32_t* norm;
  [[no_unique_address]] utils::Need<HasFilterBoost, const score_t*>
    filter_boost;
  score_t boost;
  score_t mu;
  score_t mu_p_inv;  // 1 / (mu * collection_prob)
};

}  // namespace

void LMDirichlet::collect(byte_type* stats_buf, const FieldCollector* field,
                          const TermCollector* term) const {
  auto* stats = stats_cast(stats_buf);

  const auto ttf_field = field ? field->total_term_freq : 0;
  const auto ttf_term = term ? term->total_term_freq : 0;

  const double num = static_cast<double>(ttf_term) + 1.0;
  const double den = static_cast<double>(ttf_field) + 1.0;
  stats->collection_prob = static_cast<score_t>(num / den);
}

ScoreFunction LMDirichlet::PrepareScorer(const ScoreContext& ctx) const {
  auto* freq = irs::get<FreqBlockAttr>(ctx.doc_attrs);
  if (!freq) {
    return ScoreFunction::Default();
  }

  auto* stats = stats_cast(ctx.stats);
  if (stats->collection_prob <= 0.f || _mu <= 0.f) {
    return ScoreFunction::Default();
  }

  const uint32_t* norm = [&] {
    auto* attr = irs::get<Norm>(ctx.doc_attrs);
    return attr ? &attr->value : nullptr;
  }();

  if (!norm && ctx.fetcher) {
    auto norm_reader = ctx.segment.norms(ctx.field.norm);
    norm = ctx.fetcher->AddNorms(ctx.field.norm, std::move(norm_reader));
  }

  if (!norm) {
    return ScoreFunction::Default();
  }

  auto* filter_boost = [&] {
    auto* attr = irs::get<BoostBlockAttr>(ctx.doc_attrs);
    return attr ? attr->value : nullptr;
  }();

  return ResolveBool(filter_boost != nullptr, [&]<bool HasBoost>() {
    return ScoreFunction::Make<LmDirScore<HasBoost>>(ctx.boost, _mu, *stats,
                                                     freq, norm, filter_boost);
  });
}

bool LMDirichlet::equals(const Scorer& other) const noexcept {
  if (!Scorer::equals(other)) {
    return false;
  }
  const auto& p = sdb::basics::downCast<LMDirichlet>(other);
  return p._mu == _mu;
}

}  // namespace irs
