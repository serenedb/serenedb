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

#include <absl/algorithm/container.h>

#include <cstdint>
#include <iresearch/search/detail/phrase_slop_matcher.hpp>
#include <vector>

namespace irs::detail::slop {

template<size_t N = 0>
MatchResult Run(const std::vector<std::vector<PosAttr::value_t>>& slot_pos,
                PosAttr::value_t slop,
                const std::vector<PosAttr::value_t>& expected_steps,
                MatchScratch& scratch, bool early_exit,
                const std::vector<uint32_t>& groups = {},
                std::vector<MatchSpan>* out = nullptr) {
  if (out) {
    out->clear();
  }
  const size_t n = slot_pos.size();
  if (n < 2) {
    return {};
  }
  SDB_ASSERT(expected_steps.size() == n - 1);
  SDB_ASSERT(groups.empty() || groups.size() == n);

  std::vector<int64_t> offsets(n);
  for (size_t i = 1; i != n; ++i) {
    offsets[i] = offsets[i - 1] + expected_steps[i - 1];
  }
  SpanCursors cur{slot_pos, scratch};
  std::vector<GroupPair> pairs;
  BuildGroupPairs(
    n, [&](size_t i) { return groups.empty() ? 0 : groups[i]; }, pairs);
  auto res = Sweep<N>(cur, offsets, slop, pairs, scratch, early_exit, [&] {
    if (out) {
      out->push_back(SpanOf(cur.Snapshot()));
    }
  });

  if (early_exit) {
    res.freq = res.any ? 1 : 0;
  }
  if (out) {
    absl::c_sort(*out);
  }
  return res;
}

}  // namespace irs::detail::slop
