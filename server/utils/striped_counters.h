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

#include <absl/base/optimization.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace sdb {

class StripedCounters {
 public:
  static constexpr size_t kCounters =
    ABSL_CACHELINE_SIZE / sizeof(std::atomic_int64_t);

  StripedCounters();

  void Add(std::span<const int64_t> deltas) noexcept;
  int64_t Read(size_t counter) const noexcept;
  void Reset() noexcept;

 private:
  struct ABSL_CACHELINE_ALIGNED Line {
    std::array<std::atomic_int64_t, kCounters> values{};
  };

  std::vector<Line> _lines;
};

}  // namespace sdb
