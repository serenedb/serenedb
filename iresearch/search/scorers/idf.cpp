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

#include "idf.hpp"

#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/detail/scale_score.hpp"
#include "iresearch/search/explain/factors.hpp"
#include "iresearch/search/scorers/bm25.hpp"
#include "iresearch/search/scorers/score_inputs.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {

void IDF::collect(byte_type* stats_buf, const FieldCollector* field,
                  const TermCollector* term) const {
  const auto docs_with_field = field ? field->docs_with_field : 0;
  const auto docs_with_term = term ? term->docs_with_term : 0;

  auto* stats = stats_cast(stats_buf);
  stats->value += Bm25Idf(docs_with_field, docs_with_term);
  SDB_ASSERT(stats->value >= 0.f);
}

ScoreFunction IDF::PrepareScorer(const ScoreContext& ctx) const {
  return MakeScaleScore(ctx, ctx.boost * stats_cast(ctx.stats)->value);
}

void IDF::Explain(const ScoreContext& ctx, const explain::SlotStats* stats,
                  explain::Node& out) const {
  explain::AddBoost(out, ctx.boost);
  explain::AddIdf(out, stats_cast(ctx.stats)->value, stats,
                  "log(1 + (N - n + 0.5) / (n + 0.5))", &Bm25Idf);
  explain::AddScale(out, ctx);
  out.formula = ScaleOf(ctx) ? "scale * boost * idf" : "boost * idf";
}

}  // namespace irs
