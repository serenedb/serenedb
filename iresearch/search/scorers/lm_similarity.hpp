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

#include <cstdint>
#include <span>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/score_inputs.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/misc.hpp"

namespace irs {

struct LMStats {
  score_t collection_prob;
  const score_t* shares;
  uint32_t terms;
};

inline std::span<const score_t> SharesOf(const LMStats& stats) noexcept {
  if (stats.terms > 1) {
    return {stats.shares, stats.terms};
  }
  if (stats.collection_prob > 0.f) {
    return {&stats.collection_prob, 1};
  }
  return {};
}

inline void CollectLMStats(LMStats& stats, const FieldCollector* field,
                           const TermCollector* term) noexcept {
  stats.collection_prob = CollectionShareOf(field, term);
}

inline void CollectLMTerms(LMStats& stats, const FieldCollector* field,
                           std::span<const TermCollector> terms,
                           StatsArena& arena) {
  if (terms.size() < 2) {
    for (const auto& term : terms) {
      CollectLMStats(stats, field, &term);
    }
    return;
  }
  const auto shares = CollectShares(field, terms, arena);
  stats.shares = shares.data();
  stats.terms = static_cast<uint32_t>(shares.size());
}

template<typename MakeFormula>
ScoreFunction PrepareLMScorer(const ScoreContext& ctx, const LMStats& stats,
                              MakeFormula make_formula) {
  auto* freq = irs::get<FreqBlockAttr>(ctx.doc_attrs);
  const auto shares = SharesOf(stats);
  if (!freq || shares.empty()) {
    return ScoreFunction::Default();
  }
  return MakeShareScore(shares, make_formula, freq, NormsOf(ctx), ScaleOf(ctx));
}

}  // namespace irs
