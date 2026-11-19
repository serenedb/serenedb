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

#include <absl/functional/function_ref.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
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

struct ShareStats {
  score_t* shares;
  score_t share;
  uint32_t terms;
  uint32_t capacity;
};

inline std::span<const score_t> SharesOf(const ShareStats& stats) noexcept {
  return {stats.shares ? stats.shares : &stats.share, stats.terms};
}

inline void CollectShare(ShareStats& stats, const FieldCollector* field,
                         const TermCollector* term, StatsArena& arena) {
  const auto share = CollectionShareOf(field, term);
  if (stats.terms == 0) {
    stats.share = share;
    stats.terms = 1;
    return;
  }
  const uint32_t capacity = stats.shares ? stats.capacity : 1;
  if (stats.terms == capacity) {
    const auto grown = std::max<uint32_t>(4, 2 * capacity);
    auto* shares =
      reinterpret_cast<score_t*>(arena.Allocate(grown * sizeof(score_t)));
    std::memcpy(shares, stats.shares ? stats.shares : &stats.share,
                stats.terms * sizeof(score_t));
    stats.shares = shares;
    stats.capacity = grown;
  }
  stats.shares[stats.terms++] = share;
}

double LogOfProductInChunks(size_t n,
                            absl::FunctionRef<double(size_t)> factor) noexcept;

template<typename T, typename Factor>
IRS_FORCE_INLINE T LogOfProduct(size_t n, Factor factor) noexcept {
  T product = 1;
  for (size_t i = 0; i != n; ++i) {
    product *= factor(i);
  }
  if (product >= std::numeric_limits<T>::min() &&
      product <= std::numeric_limits<T>::max()) [[likely]] {
    return std::log(product);
  }
  return static_cast<T>(LogOfProductInChunks(n, factor));
}

template<bool HasScale, typename Formula>
struct FreqScore : public ScoreOperatorOf<FreqScore<HasScale, Formula>> {
  FreqScore(Formula formula, const FreqBlockAttr* freq, const uint32_t* norm,
            const score_t* scale) noexcept
    : freq{freq}, norm{norm}, scale{scale}, formula{std::move(formula)} {}

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
ScoreFunction MakeFreqScore(Formula formula, const FreqBlockAttr* freq,
                            const uint32_t* norm, const score_t* scale) {
  return ResolveBool(scale != nullptr, [&]<bool HasScale>() {
    return ScoreFunction::Make<FreqScore<HasScale, Formula>>(std::move(formula),
                                                             freq, norm, scale);
  });
}

template<typename MakeTerm, typename MakeTerms>
ScoreFunction MakeShareScore(std::span<const score_t> shares,
                             MakeTerm make_term, MakeTerms make_terms,
                             const FreqBlockAttr* freq, const uint32_t* norm,
                             const score_t* scale) {
  if (shares.size() == 1) {
    return MakeFreqScore(make_term(shares.front()), freq, norm, scale);
  }
  return MakeFreqScore(make_terms(shares), freq, norm, scale);
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
