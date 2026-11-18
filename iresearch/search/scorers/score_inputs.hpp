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

#pragma once

#include <absl/container/inlined_vector.h>

#include <cstdint>
#include <span>
#include <type_traits>
#include <utility>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/stats_arena.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/misc.hpp"

namespace irs {

inline score_t CollectionShareOf(uint64_t term_ttf,
                                 uint64_t field_ttf) noexcept {
  const double num = static_cast<double>(term_ttf) + 1.0;
  const double den = static_cast<double>(field_ttf) + 1.0;
  return static_cast<score_t>(num / den);
}

inline score_t CollectionShareOf(const FieldCollector* field,
                                 const TermCollector* term) noexcept {
  return CollectionShareOf(term ? term->total_term_freq : 0,
                           field ? field->total_term_freq : 0);
}

inline std::span<const score_t> CollectShares(
  const FieldCollector* field, std::span<const TermCollector> terms,
  StatsArena& arena) {
  auto* shares =
    reinterpret_cast<score_t*>(arena.Allocate(terms.size() * sizeof(score_t)));
  for (size_t i = 0; i != terms.size(); ++i) {
    shares[i] = CollectionShareOf(field, &terms[i]);
  }
  return {shares, terms.size()};
}

template<bool HasScale, typename Formula>
struct FreqScore : public ScoreOperatorOf<FreqScore<HasScale, Formula>> {
  FreqScore(Formula formula, const FreqBlockAttr* freq, const uint32_t* norm,
            const score_t* scale) noexcept
    : freq{freq}, norm{norm}, scale{scale}, formula{formula} {}

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* res,
                                  scores_size_t n) const noexcept {
    ScoreFreqs<MergeType, HasScale>(res, n, freq->value, norm,
                                    TryGetValue(scale), formula);
  }

  const FreqBlockAttr* freq;
  const uint32_t* norm;
  [[no_unique_address]] utils::Need<HasScale, const score_t*> scale;
  Formula formula;
};

template<typename Formula>
struct SumOf {
  IRS_FORCE_INLINE score_t operator()(score_t tf, score_t dl) const noexcept {
    score_t sum = 0.f;
    for (const auto& formula : formulas) {
      sum += formula(tf, dl);
    }
    return sum;
  }

  std::span<const Formula> formulas;
};

template<bool HasScale, typename Formula>
struct SumScore : public ScoreOperatorOf<SumScore<HasScale, Formula>> {
  SumScore(absl::InlinedVector<Formula, 4> formulas, const FreqBlockAttr* freq,
           const uint32_t* norm, const score_t* scale) noexcept
    : freq{freq}, norm{norm}, scale{scale}, formulas{std::move(formulas)} {}

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* res,
                                  scores_size_t n) const noexcept {
    ScoreFreqs<MergeType, HasScale>(
      res, n, freq->value, norm, TryGetValue(scale), SumOf<Formula>{formulas});
  }

  const FreqBlockAttr* freq;
  const uint32_t* norm;
  [[no_unique_address]] utils::Need<HasScale, const score_t*> scale;
  absl::InlinedVector<Formula, 4> formulas;
};

template<typename MakeFormula>
ScoreFunction MakeShareScore(std::span<const score_t> shares,
                             MakeFormula make_formula,
                             const FreqBlockAttr* freq, const uint32_t* norm,
                             const score_t* scale) {
  using Formula = std::invoke_result_t<MakeFormula&, score_t>;
  if (shares.size() == 1) {
    return ResolveBool(scale != nullptr, [&]<bool HasScale>() {
      return ScoreFunction::Make<FreqScore<HasScale, Formula>>(
        make_formula(shares.front()), freq, norm, scale);
    });
  }
  absl::InlinedVector<Formula, 4> formulas;
  formulas.reserve(shares.size());
  for (const auto share : shares) {
    formulas.push_back(make_formula(share));
  }
  return ResolveBool(scale != nullptr, [&]<bool HasScale>() {
    return ScoreFunction::Make<SumScore<HasScale, Formula>>(std::move(formulas),
                                                            freq, norm, scale);
  });
}

inline const score_t* ScaleOf(const ScoreContext& ctx) noexcept {
  const auto* attr = irs::get<ScaleBlockAttr>(ctx.doc_attrs);
  return attr ? attr->value : nullptr;
}

inline const uint32_t* FieldNormsOf(const ScoreContext& ctx) {
  if (const auto* attr = irs::get<Norm>(ctx.doc_attrs)) {
    return &attr->value;
  }
  return ctx.fetcher.AddNorms(ctx.field.norm,
                              ctx.segment.norms(ctx.field.norm));
}

inline const uint32_t* NormsOf(const ScoreContext& ctx) {
  const auto* norm = FieldNormsOf(ctx);
  return norm ? norm : kNorms.data();
}

}  // namespace irs
