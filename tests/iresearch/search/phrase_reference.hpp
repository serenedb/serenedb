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
#include <absl/container/btree_set.h>

#include <cstdint>
#include <iresearch/utils/type_limits.hpp>
#include <utility>
#include <vector>

namespace tests::phrase_ref {

struct Gap {
  uint32_t lo;
  uint32_t hi;
};

struct Span {
  uint32_t start;
  uint32_t end;

  auto operator<=>(const Span&) const = default;
};

struct Result {
  bool matched = false;
  uint64_t tuples = 0;
  std::vector<Span> spans;
  uint64_t freq = 0;
  double weight = 0;
};

inline Result Enumerate(const std::vector<std::vector<uint32_t>>& slots,
                        const std::vector<Gap>& gaps) {
  Result res;
  const size_t n = slots.size();
  absl::btree_set<Span> all;
  std::vector<uint32_t> placement(n);

  auto walk = [&](auto& self, size_t i) -> void {
    if (i == n) {
      ++res.tuples;
      all.insert({placement.front(), placement.back()});
      return;
    }
    for (const uint32_t pos : slots[i]) {
      if (i > 0) {
        const int64_t d = int64_t{pos} - int64_t{placement[i - 1]};
        if (d < int64_t{gaps[i].lo}) {
          continue;
        }
        if (gaps[i].hi != irs::pos_limits::kOpenGap &&
            d > int64_t{gaps[i].hi}) {
          continue;
        }
      }
      placement[i] = pos;
      self(self, i + 1);
    }
  };
  if (n > 0) {
    walk(walk, 0);
  }

  for (const Span s : all) {
    const bool minimal = absl::c_none_of(all, [&](Span t) {
      return t != s && t.start >= s.start && t.end <= s.end;
    });
    if (minimal) {
      res.spans.push_back(s);
    }
  }

  int64_t min_extent = 1;
  for (size_t i = 1; i < n; ++i) {
    min_extent += gaps[i].lo;
  }
  for (const Span s : res.spans) {
    const int64_t width = int64_t{s.end} - int64_t{s.start} + 1;
    res.weight +=
      1.0 / static_cast<double>(std::max<int64_t>(width - min_extent + 1, 1));
  }
  res.freq = res.spans.size();
  res.matched = res.tuples != 0;
  return res;
}

}  // namespace tests::phrase_ref
