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

#include <algorithm>

#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/filters/filter.hpp"
#include "iresearch/search/filters/filter_optimizer.hpp"
#include "iresearch/search/scorers/constant_score.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/search/scorers/unscored.hpp"

namespace irs::optimizer {

inline bool IsAllDocs(const Filter& filter) noexcept {
  return filter.type() == irs::Type<All>::id();
}

inline score_t MergedBoost(ScoreMergeType merge_type, score_t lo,
                           score_t hi) noexcept {
  switch (merge_type) {
    case ScoreMergeType::Max:
      return std::max(lo, hi);
    case ScoreMergeType::Noop:
      return kNoBoost;
    case ScoreMergeType::Sum:
      break;
  }
  return lo + hi;
}

inline bool ScoreDependsOnTerms(const Filter& node,
                                const OptimizeContext& ctx) noexcept {
  if (!ctx.scored) {
    return false;
  }
  const auto* const scorer = node.GetScorer();
  if (scorer == nullptr) {
    return true;
  }
  return !IsUnscored(*scorer) &&
         scorer->type() != irs::Type<ConstantScore>::id();
}

inline bool ScoreIsIgnored(const Filter& node,
                           const OptimizeContext& ctx) noexcept {
  if (!ctx.scored) {
    return true;
  }
  const auto* const scorer = node.GetScorer();
  return scorer != nullptr && IsUnscored(*scorer);
}

inline bool ScoreIsConstant(const Filter& node,
                            const OptimizeContext& ctx) noexcept {
  if (!ctx.scored) {
    return false;
  }
  const auto* const scorer = node.GetScorer();
  return scorer != nullptr && scorer->type() == irs::Type<ConstantScore>::id();
}

inline void FoldBoost(Filter& survivor, score_t boost, bool scored) {
  if (boost == kNoBoost || !scored) {
    return;
  }
  survivor.SetBoost(survivor.GetBoost() * boost);
}

}  // namespace irs::optimizer
