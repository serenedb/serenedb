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

#include <absl/container/inlined_vector.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cmath>
#include <span>
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
#include "iresearch/utils/dispatch.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/system_compiler.hpp"

namespace irs {
namespace {

template<DFIMeasure M>
struct Measure;

template<>
struct Measure<DFIMeasure::Standardized> {
  static constexpr std::string_view kName = "standardized";
  static constexpr std::string_view kKernel =
    "(freq - expected) / sqrt(expected)";

  IRS_FORCE_INLINE static score_t Kernel(score_t diff,
                                         score_t expected) noexcept {
    return diff / std::sqrt(expected);
  }

  static double Inverse(double value) noexcept { return 1 / std::sqrt(value); }

  IRS_FORCE_INLINE static double Growth(double diff, double inverse) noexcept {
    return 1 + diff * inverse;
  }
};

template<>
struct Measure<DFIMeasure::Saturated> {
  static constexpr std::string_view kName = "saturated";
  static constexpr std::string_view kKernel = "(freq - expected) / expected";

  IRS_FORCE_INLINE static score_t Kernel(score_t diff,
                                         score_t expected) noexcept {
    return diff / expected;
  }

  static double Inverse(double value) noexcept { return 1 / value; }

  IRS_FORCE_INLINE static double Growth(double diff, double inverse) noexcept {
    return 1 + diff * inverse;
  }
};

template<>
struct Measure<DFIMeasure::ChiSquared> {
  static constexpr std::string_view kName = "chi_squared";
  static constexpr std::string_view kKernel = "(freq - expected)^2 / expected";

  IRS_FORCE_INLINE static score_t Kernel(score_t diff,
                                         score_t expected) noexcept {
    return diff * diff / expected;
  }

  static double Inverse(double value) noexcept { return 1 / value; }

  IRS_FORCE_INLINE static double Growth(double diff, double inverse) noexcept {
    return 1 + diff * diff * inverse;
  }
};

template<DFIMeasure M>
struct DFIFormula {
  IRS_FORCE_INLINE score_t operator()(score_t tf, score_t dl) const noexcept {
    constexpr score_t kInvLn2 = 1.4426950408889634f;
    const score_t expected = ratio * dl;
    score_t r = 0.f;
    if (tf > expected && expected > 0.f) {
      r = std::log1p(Measure<M>::Kernel(tf - expected, expected)) * kInvLn2;
    }
    return r * boost;
  }

  score_t boost;
  score_t ratio;
};

template<DFIMeasure M>
struct DFITermsFormula {
  IRS_FORCE_INLINE score_t operator()(score_t tf, score_t dl) const noexcept {
    constexpr double kInvLn2 = 1.4426950408889634;
    if (tf <= min_ratio * dl) {
      return 0.f;
    }
    const double doc_inverse = Measure<M>::Inverse(dl);
    const auto* const term = terms.data();
    const double log = LogOfProduct(terms.size(), [&](size_t i) {
      const score_t expected = term[i].ratio * dl;
      if (tf > expected && expected > 0.f) {
        return Measure<M>::Growth(tf - expected, term[i].inverse * doc_inverse);
      }
      return 1.0;
    });
    return static_cast<score_t>(log * kInvLn2) * boost;
  }

  struct Term {
    score_t ratio;
    double inverse;
  };

  score_t boost;
  score_t min_ratio;
  absl::InlinedVector<Term, 4> terms;
};

template<DFIMeasure M>
DFITermsFormula<M> DFITermsOf(score_t boost, std::span<const score_t> ratios) {
  DFITermsFormula<M> formula{.boost = boost,
                             .min_ratio = std::ranges::min(ratios)};
  formula.terms.reserve(ratios.size());
  for (const auto ratio : ratios) {
    formula.terms.push_back({ratio, Measure<M>::Inverse(ratio)});
  }
  return formula;
}

}  // namespace

void DFI::collect(byte_type* stats_buf, const FieldCollector* field,
                  const TermCollector* term, StatsArena& arena) const {
  CollectShare(*stats_cast(stats_buf), field, term, arena);
}

ScoreFunction DFI::PrepareScorer(const ScoreContext& ctx) const {
  return Resolve(_measure, [&]<DFIMeasure M> {
    return PrepareShareScorer(
      ctx, *stats_cast(ctx.stats),
      [&](score_t ratio) { return DFIFormula<M>{ctx.boost, ratio}; },
      [&](std::span<const score_t> ratios) {
        return DFITermsOf<M>(ctx.boost, ratios);
      });
  });
}

void DFI::Explain(const ScoreContext& ctx, const explain::SlotStats* stats,
                  explain::Node& out) const {
  const auto ratios = SharesOf(*stats_cast(ctx.stats));
  if (!explain::FreqOf(ctx) || ratios.empty()) {
    explain::ExplainNoFreq(ctx, false, out);
    return;
  }
  const auto kernel =
    Resolve(_measure, []<DFIMeasure M> { return Measure<M>::kKernel; });
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
    return Resolve(_measure, [&]<DFIMeasure M> {
      return DFIFormula<M>{ctx.boost, ratio}(freq, length);
    });
  };
  if (ratios.size() == 1) {
    explain::AddCollectionShare(out, ratios.front(), stats);
    describe(out, ratios.front());
    return;
  }
  explain::ExplainParts(
    stats, out,
    [&](explain::Node& part, const explain::TermStats& term, score_t ratio) {
      explain::AddShare(part, ratio, term.total_term_freq,
                        stats->total_term_freq);
      part.value = describe(part, ratio);
    });
}

std::string DFI::ToString() const {
  return absl::StrCat(
    "dfi(measure=",
    Resolve(_measure, []<DFIMeasure M> { return Measure<M>::kName; }), ")");
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
