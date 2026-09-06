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
#include <utility>
#include <vector>

#include "basics/shared.hpp"
#include "iresearch/search/score_function.hpp"

namespace irs::search {

ScoreFunction MakeProbeScore(ScoreMergeType inner, ScoreFunction&& required,
                             std::vector<ScoreFunction>&& probed,
                             uint32_t* held, score_t constant = 0);

template<typename Leaves, typename Held>
ScoreFunction MakeProbeOf(ScoreMergeType inner, Leaves& leaves, Held& held,
                          score_t constant = 0) {
  const auto count = leaves.size();
  SDB_ASSERT(held.size() == count);
  std::vector<ScoreFunction> probed;
  probed.reserve(count);
  for (size_t i = 0; i != count; ++i) {
    probed.emplace_back(leaves[i].PrepareScore());
  }
  return MakeProbeScore(inner, ScoreFunction::Default(), std::move(probed),
                        held.data(), constant);
}

}  // namespace irs::search
