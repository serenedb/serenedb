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

#include <absl/strings/str_cat.h>

#include <cmath>
#include <string_view>

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
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/misc.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/system_compiler.hpp"

namespace irs {
namespace {

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

template<DFIMeasure M>
struct DFIFormula {
  IRS_FORCE_INLINE score_t operator()(score_t tf, score_t dl) const noexcept {
    constexpr score_t kInvLn2 = 1.4426950408889634f;
    const score_t expected = ratio * dl;
    score_t r = 0.f;
    if (tf > expected && expected > 0.f) {
      r = std::log1p(MeasureKernel<M>(tf - expected, expected)) * kInvLn2;
    }
    return r * boost;
  }

  score_t boost;
  score_t ratio;
};

template<DFIMeasure M>
ScoreFunction MakeScoreMeasure(const ScoreContext& ctx,
                               std::span<const score_t> ratios,
                               const FreqBlockAttr* freq, const uint32_t* norm,
                               const score_t* scale) {
  return MakeShareScore(
    ratios, [&](score_t ratio) { return DFIFormula<M>{ctx.boost, ratio}; },
    freq, norm, scale);
}

score_t ScoreOf(DFIMeasure measure, score_t boost, score_t ratio, score_t tf,
                score_t dl) noexcept {
  switch (measure) {
    case DFIMeasure::Standardized:
      return DFIFormula<DFIMeasure::Standardized>{boost, ratio}(tf, dl);
    case DFIMeasure::Saturated:
      return DFIFormula<DFIMeasure::Saturated>{boost, ratio}(tf, dl);
    case DFIMeasure::ChiSquared:
      return DFIFormula<DFIMeasure::ChiSquared>{boost, ratio}(tf, dl);
  }
  SDB_UNREACHABLE();
}

std::string_view KernelOf(DFIMeasure measure) noexcept {
  switch (measure) {
    case DFIMeasure::Standardized:
      return "(freq - expected) / sqrt(expected)";
    case DFIMeasure::Saturated:
      return "(freq - expected) / expected";
    case DFIMeasure::ChiSquared:
      return "(freq - expected)^2 / expected";
  }
  SDB_UNREACHABLE();
}

std::span<const score_t> RatiosOf(const DFIStats& stats) noexcept {
  if (stats.ratios) {
    return {stats.ratios, stats.terms};
  }
  if (stats.ratio > 0.f) {
    return {&stats.ratio, 1};
  }
  return {};
}

}  // namespace

void DFI::collect(byte_type* stats_buf, const FieldCollector* field,
                  const TermCollector* term) const {
  auto& stats = *stats_cast(stats_buf);
  const auto ratio = CollectionShareOf(field, term);
  if (stats.ratios) {
    stats.ratios[stats.terms++] = ratio;
  } else {
    stats.ratio = ratio;
  }
}

void DFI::ReserveTerms(byte_type* stats_buf, size_t terms,
                       StatsArena& arena) const {
  if (terms > 1) {
    stats_cast(stats_buf)->ratios = AllocateShares(arena, terms);
  }
}

ScoreFunction DFI::PrepareScorer(const ScoreContext& ctx) const {
  auto* freq = irs::get<FreqBlockAttr>(ctx.doc_attrs);
  const auto ratios = RatiosOf(*stats_cast(ctx.stats));
  if (!freq || ratios.empty()) {
    return ScoreFunction::Default();
  }

  const uint32_t* norm = NormsOf(ctx);

  const auto* scale = ScaleOf(ctx);

  switch (_measure) {
    case DFIMeasure::Standardized:
      return MakeScoreMeasure<DFIMeasure::Standardized>(ctx, ratios, freq, norm,
                                                        scale);
    case DFIMeasure::Saturated:
      return MakeScoreMeasure<DFIMeasure::Saturated>(ctx, ratios, freq, norm,
                                                     scale);
    case DFIMeasure::ChiSquared:
      return MakeScoreMeasure<DFIMeasure::ChiSquared>(ctx, ratios, freq, norm,
                                                      scale);
  }
  return ScoreFunction::Default();
}

void DFI::Explain(const ScoreContext& ctx, const explain::SlotStats* stats,
                  explain::Node& out) const {
  const auto ratios = RatiosOf(*stats_cast(ctx.stats));
  if (!explain::FreqOf(ctx) || ratios.empty()) {
    explain::ExplainNoFreq(ctx, false, out);
    return;
  }
  const auto kernel = KernelOf(_measure);
  const auto describe = [&](explain::Node& node, score_t ratio) {
    explain::AddBoost(node, ctx.boost);
    const auto freq = explain::AddFreq(node, ctx);
    auto& expected = explain::AddDetail(node, "expected", score_t{0});
    expected.expression = "p * dl";
    const auto length = TermCountToScore(explain::AddLength(expected, ctx));
    expected.value = ratio * length;
    if (freq > ratio * length) {
      node.expression = absl::StrCat("boost * log2(1 + ", kernel, ")");
    } else {
      node.note = "the term occurs no more often than expected, so it scores 0";
    }
    return ScoreOf(_measure, ctx.boost, ratio, freq, length);
  };
  if (ratios.size() == 1) {
    explain::AddCollectionShare(out, ratios.front(), stats);
    describe(out, ratios.front());
    return;
  }
  explain::ExplainTerms(
    stats, out,
    [&](explain::Node& part, const explain::TermStats& term, score_t ratio) {
      explain::AddShare(part, ratio, term.total_term_freq,
                        stats->total_term_freq);
      part.value = describe(part, ratio);
    });
}

std::string DFI::ToString() const {
  const auto* measure = [&]() -> const char* {
    switch (_measure) {
      case DFIMeasure::Standardized:
        return "standardized";
      case DFIMeasure::Saturated:
        return "saturated";
      case DFIMeasure::ChiSquared:
        return "chi_squared";
    }
    return "?";
  }();
  return absl::StrCat("dfi(measure=", measure, ")");
}

bool DFI::equals(const Scorer& other) const noexcept {
  if (!Scorer::equals(other)) {
    return false;
  }
  const auto& p = irs::utils::downCast<DFI>(other);
  return p._measure == _measure;
}

ScoreBoundWriter::ptr DFI::PrepareScoreBoundWriter() const {
  return std::make_unique<FreqNormWriter<kScoreBoundMinNorm>>();
}

ScoreBoundSource::ptr DFI::PrepareScoreBoundSource() const {
  return std::make_unique<FreqNormSource<kScoreBoundFreq | kScoreBoundNorm>>();
}

bool DFI::Compatible(const ScorerOptions& persisted) const noexcept {
  return irs::BoundTypeOf(persisted) == BoundTypeOf(Options{}) &&
         !std::get_if<ScorerOptions::Bm25>(&persisted.params);
}

}  // namespace irs
