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
#include <limits>

namespace sdb::search {

struct RowIdRange {
  int64_t first = std::numeric_limits<int64_t>::max();
  int64_t last = std::numeric_limits<int64_t>::min();

  constexpr bool Empty() const noexcept { return first > last; }
};

constexpr RowIdRange Union(RowIdRange lhs, RowIdRange rhs) noexcept {
  return {.first = std::min(lhs.first, rhs.first),
          .last = std::max(lhs.last, rhs.last)};
}

}  // namespace sdb::search
