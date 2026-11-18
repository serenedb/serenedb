////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "raw_boost.hpp"

#include <absl/container/inlined_vector.h>

#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/detail/scale_score.hpp"
#include "iresearch/search/explain/factors.hpp"
#include "iresearch/search/scorers/score_inputs.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {

ScoreFunction RawBoost::PrepareScorer(const ScoreContext& ctx) const {
  return MakeScaleScore(ctx, ctx.boost);
}

void RawBoost::Explain(const ScoreContext& ctx, const explain::SlotStats*,
                       explain::Node& out) const {
  explain::AddDetail(out, "boost", ctx.boost,
                     "boost, the query's weight for this clause");
  explain::AddScale(out, ctx);
  out.description = ScaleOf(ctx) ? "computed as scale * boost from:"
                                 : "computed as boost from:";
}

}  // namespace irs
