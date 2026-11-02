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

#include "raw_dl.hpp"

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/score_inputs.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/misc.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {
namespace {

template<ScoreMergeType MergeType, bool HasScale>
IRS_FORCE_INLINE void DocLenImpl(
  score_t* IRS_RESTRICT res, scores_size_t n, const uint32_t* IRS_RESTRICT norm,
  [[maybe_unused]] const score_t* IRS_RESTRICT scale,
  score_t base_boost) noexcept {
  for (scores_size_t i = 0; i != n; ++i) {
    const auto r = [&] IRS_FORCE_INLINE {
      if constexpr (HasScale) {
        return scale[i] * base_boost * TermCountToScore(norm[i]);
      } else {
        return base_boost * TermCountToScore(norm[i]);
      }
    }();
    Merge<MergeType>(res[i], r);
  }
}

template<bool HasScale>
struct RawDLScore : public ScoreOperatorOf<RawDLScore<HasScale>> {
  RawDLScore(score_t boost, const uint32_t* norm, const score_t* scale) noexcept
    : norm{norm}, scale{scale}, boost{boost} {
    SDB_ASSERT(this->norm);
  }

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* IRS_RESTRICT res,
                                  scores_size_t n) const noexcept {
    DocLenImpl<MergeType, HasScale>(res, n, norm, TryGetValue(scale), boost);
  }

  const uint32_t* norm;
  [[no_unique_address]] utils::Need<HasScale, const score_t*> scale;
  score_t boost;
};

}  // namespace

ScoreFunction RawDL::PrepareScorer(const ScoreContext& ctx) const {
  if (!irs::get<FreqBlockAttr>(ctx.doc_attrs)) {
    return ScoreFunction::Default();
  }

  const uint32_t* norm = NormsOf(ctx);

  const auto* scale = ScaleOf(ctx);

  return ResolveBool(scale != nullptr, [&]<bool HasScale>() {
    return ScoreFunction::Make<RawDLScore<HasScale>>(ctx.boost, norm, scale);
  });
}

}  // namespace irs
