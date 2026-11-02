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
};

inline void CollectLMStats(LMStats& stats, const FieldCollector* field,
                           const TermCollector* term) noexcept {
  const auto ttf_field = field ? field->total_term_freq : 0;
  const auto ttf_term = term ? term->total_term_freq : 0;

  const double num = static_cast<double>(ttf_term) + 1.0;
  const double den = static_cast<double>(ttf_field) + 1.0;
  stats.collection_prob = static_cast<score_t>(num / den);
}

template<template<bool> typename Score>
ScoreFunction PrepareLMScorer(const ScoreContext& ctx, const LMStats& stats,
                              score_t param) {
  auto* freq = irs::get<FreqBlockAttr>(ctx.doc_attrs);
  if (!freq || stats.collection_prob <= 0.f) {
    return ScoreFunction::Default();
  }

  const uint32_t* norm = NormsOf(ctx);

  const auto* scale = ScaleOf(ctx);

  return ResolveBool(scale != nullptr, [&]<bool HasScale>() {
    return ScoreFunction::Make<Score<HasScale>>(ctx.boost, param, stats, freq,
                                                norm, scale);
  });
}

}  // namespace irs
