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

#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"

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

inline void FoldBoost(Filter& survivor, score_t boost, bool scored) {
  if (boost == kNoBoost || !scored) {
    return;
  }
  survivor.SetBoost(survivor.GetBoost() * boost);
}

}  // namespace irs::optimizer
