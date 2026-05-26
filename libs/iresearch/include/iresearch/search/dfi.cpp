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

#include "dfi.hpp"

#include <cmath>
#include <string_view>

#include "basics/down_cast.h"
#include "basics/empty.hpp"
#include "basics/misc.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/scorers.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {
namespace {

template<typename T>
constexpr const T* TryGetValue(const T* value) noexcept {
  return value;
}

constexpr std::nullptr_t TryGetValue(utils::Empty /*value*/) noexcept {
  return nullptr;
}

bool ParseMeasure(std::string_view s, DFIMeasure& out) {
  if (s == "standardized") {
    out = DFIMeasure::Standardized;
    return true;
  }
  if (s == "saturated") {
    out = DFIMeasure::Saturated;
    return true;
  }
  if (s == "chi_squared" || s == "chisquared") {
    out = DFIMeasure::ChiSquared;
    return true;
  }
  return false;
}

// Independence measure kernels; inlined by the templated scorer.
template<DFIMeasure M>
IRS_FORCE_INLINE score_t MeasureKernel(score_t diff,
                                       score_t expected) noexcept {
  if constexpr (M == DFIMeasure::Standardized) {
    return diff / std::sqrt(expected);
  } else if constexpr (M == DFIMeasure::Saturated) {
    return diff / expected;
  } else if constexpr (M == DFIMeasure::ChiSquared) {
    return diff * diff / expected;
  } else {
    static_assert(false);
  }
}

// score = boost * log2(measure(tf, expected) + 1) for tf > expected, else 0.
//   expected = ratio * dl,  where ratio = (ttf_t + 1) / (ttf_field + 1)
template<ScoreMergeType MergeType, DFIMeasure M, bool HasBoost>
IRS_FORCE_INLINE void DFIImpl(
  score_t* IRS_RESTRICT res, scores_size_t n, const uint32_t* IRS_RESTRICT freq,
  const uint32_t* IRS_RESTRICT norm,
  [[maybe_unused]] const score_t* IRS_RESTRICT boost, score_t ratio,
  score_t const_boost) noexcept {
  // 1 / ln(2) -- log2(x) = ln(x) * kInvLn2
  constexpr score_t kInvLn2 = 1.4426950408889634f;
  for (scores_size_t i = 0; i != n; ++i) {
    const score_t tf = static_cast<score_t>(freq[i]);
    const score_t dl = static_cast<score_t>(norm[i]);
    const score_t expected = ratio * dl;
    score_t r = 0.f;
    if (tf > expected && expected > 0.f) {
      const score_t measure = MeasureKernel<M>(tf - expected, expected);
      r = std::log1p(measure) * kInvLn2;
    }
    if constexpr (HasBoost) {
      r *= const_boost * boost[i];
    } else {
      r *= const_boost;
    }
    Merge<MergeType>(res[i], r);
  }
}

template<DFIMeasure M, bool HasFilterBoost>
struct DFIScore : public ScoreOperator {
  DFIScore(score_t boost, const DFIStats& stats, const FreqBlockAttr* freq,
           const uint32_t* norm, const score_t* fb) noexcept
    : freq{freq},
      norm{norm},
      filter_boost{fb},
      boost{boost},
      ratio{stats.ratio} {}

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* res,
                                  scores_size_t n) const noexcept {
    DFIImpl<MergeType, M, HasFilterBoost>(
      res, n, freq->value, norm, TryGetValue(filter_boost), ratio, boost);
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
  score_t ratio;  // (ttf_t + 1) / (ttf_field + 1)
};

template<DFIMeasure M>
ScoreFunction MakeScoreMeasure(const ScoreContext& ctx, const DFIStats& stats,
                               const FreqBlockAttr* freq, const uint32_t* norm,
                               const score_t* filter_boost) {
  return ResolveBool(filter_boost != nullptr, [&]<bool HasBoost>() {
    return ScoreFunction::Make<DFIScore<M, HasBoost>>(ctx.boost, stats, freq,
                                                      norm, filter_boost);
  });
}

}  // namespace

void DFI::collect(byte_type* stats_buf, const FieldCollector* field,
                  const TermCollector* term) const {
  auto* stats = stats_cast(stats_buf);

  const auto ttf_field = field ? field->total_term_freq : 0;
  const auto ttf_term = term ? term->total_term_freq : 0;

  const double num = static_cast<double>(ttf_term) + 1.0;
  const double den = static_cast<double>(ttf_field) + 1.0;
  stats->ratio = static_cast<score_t>(num / den);
}

ScoreFunction DFI::PrepareScorer(const ScoreContext& ctx) const {
  auto* freq = irs::get<FreqBlockAttr>(ctx.doc_attrs);
  if (!freq) {
    return ScoreFunction::Default();
  }

  auto* stats = stats_cast(ctx.stats);
  if (stats->ratio <= 0.f) {
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

  switch (_measure) {
    case DFIMeasure::Standardized:
      return MakeScoreMeasure<DFIMeasure::Standardized>(ctx, *stats, freq, norm,
                                                        filter_boost);
    case DFIMeasure::Saturated:
      return MakeScoreMeasure<DFIMeasure::Saturated>(ctx, *stats, freq, norm,
                                                     filter_boost);
    case DFIMeasure::ChiSquared:
      return MakeScoreMeasure<DFIMeasure::ChiSquared>(ctx, *stats, freq, norm,
                                                      filter_boost);
  }
  return ScoreFunction::Default();  // unreachable
}

bool DFI::equals(const Scorer& other) const noexcept {
  if (!Scorer::equals(other)) {
    return false;
  }
  const auto& p = sdb::basics::downCast<DFI>(other);
  return p._measure == _measure;
}

}  // namespace irs
