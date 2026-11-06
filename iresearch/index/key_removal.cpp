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

#include "iresearch/index/key_removal.hpp"

namespace irs {

void SortUnique(std::vector<int64_t>& keys) {
  if (!absl::c_is_sorted(keys)) {
    absl::c_sort(keys);
  }
  keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
}

std::span<const int64_t> KeysWithin(const duckdb::BaseStatistics& stats,
                                    std::span<const int64_t> keys) {
  if (!duckdb::NumericStats::HasMinMax(stats)) {
    return keys;
  }
  const auto first =
    absl::c_lower_bound(keys, duckdb::NumericStats::GetMin<int64_t>(stats));
  return {first,
          std::upper_bound(first, keys.end(),
                           duckdb::NumericStats::GetMax<int64_t>(stats))};
}

}  // namespace irs
