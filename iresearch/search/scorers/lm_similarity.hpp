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

using LMStats = ShareStats;

template<typename MakeTerm, typename MakeTerms>
ScoreFunction PrepareLMScorer(const ScoreContext& ctx, const LMStats& stats,
                              MakeTerm make_term, MakeTerms make_terms) {
  auto* freq = irs::get<FreqBlockAttr>(ctx.doc_attrs);
  const auto shares = SharesOf(stats);
  if (!freq || shares.empty()) {
    return ScoreFunction::Default();
  }
  return MakeShareScore(shares, make_term, make_terms, freq, NormsOf(ctx),
                        ScaleOf(ctx));
}

}  // namespace irs
