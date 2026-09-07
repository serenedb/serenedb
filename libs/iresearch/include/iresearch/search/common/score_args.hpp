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
#include <cstdint>
#include <vector>

#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class ColumnArgsFetcher;
struct SubReader;

namespace search {

struct ScoreArgs {
  const Scorer* scorer = nullptr;
  const byte_type* stats = nullptr;
  ColumnArgsFetcher* fetcher = nullptr;
  score_t boost = kNoBoost;
};

enum class Terms : uint8_t {
  Mixed,
  Constant,
  Scored,
  Bounded,
};

struct StatsRecord {
  const byte_type* stats = nullptr;
  const Scorer* scorer = nullptr;
};

struct ScoreRecipe {
  const SubReader* segment = nullptr;
  ColumnArgsFetcher* fetcher = nullptr;

  ScoreArgs Args(const StatsRecord& record, score_t boost) const noexcept {
    return {
      .scorer = record.scorer,
      .stats = record.stats,
      .fetcher = fetcher,
      .boost = boost,
    };
  }
};

inline void AppendScorer(std::vector<ScoreFunction>& out,
                         ScoreFunction&& score) {
  if (!score.IsDefault()) {
    out.emplace_back(std::move(score));
  }
}

}  // namespace search
}  // namespace irs
