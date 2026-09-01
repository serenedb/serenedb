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

#include <cmath>

#include "basics/assert.h"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/volatile_boost_score.hpp"

namespace irs {

void IDF::collect(byte_type* stats_buf, const FieldCollector* field,
                  const TermCollector* term) const {
  const auto docs_with_field = field ? field->docs_with_field : 0;
  const auto docs_with_term = term ? term->docs_with_term : 0;

  auto* stats = stats_cast(stats_buf);
  stats->value += static_cast<score_t>(
    std::log1p((static_cast<double>(docs_with_field - docs_with_term) + 0.5) /
               (static_cast<double>(docs_with_term) + 0.5)));
  SDB_ASSERT(stats->value >= 0.f);
}

ScoreFunction IDF::PrepareScorer(const ScoreContext& ctx) const {
  return MakeVolatileBoostScore(ctx, ctx.boost * stats_cast(ctx.stats)->value);
}

}  // namespace irs
