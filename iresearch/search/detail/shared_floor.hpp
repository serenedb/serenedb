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
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <limits>
#include <span>
#include <vector>

#include "iresearch/types.hpp"

namespace irs::detail {

class SharedFloor {
 public:
  explicit SharedFloor(uint32_t k) : _k{k} { _lowers.reserve(2 * size_t{k}); }

  score_t Load() const noexcept {
    return _floor.load(std::memory_order_acquire);
  }

  void Merge(std::span<const score_t> lowers) {
    if (_k == 0 || lowers.empty()) {
      return;
    }
    absl::MutexLock lock{&_mutex};
    const auto floor = _floor.load(std::memory_order_relaxed);
    for (const auto lower : lowers) {
      if (lower > floor) {
        _lowers.push_back(lower);
      }
    }
    const size_t limit =
      floor == std::numeric_limits<score_t>::lowest() ? _k : 2 * size_t{_k};
    if (_lowers.size() >= limit) {
      const auto kth = _lowers.begin() + (_k - 1);
      absl::c_nth_element(_lowers, kth, std::greater{});
      _floor.store(*kth, std::memory_order_release);
      _lowers.resize(_k);
    }
  }

 private:
  uint32_t _k;
  absl::Mutex _mutex;
  std::vector<score_t> _lowers ABSL_GUARDED_BY(_mutex);
  std::atomic<score_t> _floor{std::numeric_limits<score_t>::lowest()};
};

}  // namespace irs::detail
