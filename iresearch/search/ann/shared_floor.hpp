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

namespace irs::ann {

class SharedFloor {
 public:
  explicit SharedFloor(uint32_t k,
                       score_t seed = std::numeric_limits<score_t>::lowest())
    : _k{k}, _floor{seed} {
    _lowers.reserve(2 * size_t{k});
  }

  uint32_t K() const noexcept { return _k; }

  score_t Load() const noexcept {
    return _floor.load(std::memory_order_acquire);
  }

  void Reset(score_t seed) noexcept {
    absl::MutexLock lock{&_mutex};
    _lowers.clear();
    _ranked = false;
    _floor.store(seed, std::memory_order_release);
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
    if (_lowers.size() >= (_ranked ? 2 * size_t{_k} : size_t{_k})) {
      const auto kth = _lowers.begin() + (_k - 1);
      absl::c_nth_element(_lowers, kth, std::greater{});
      _floor.store(*kth, std::memory_order_release);
      _lowers.resize(_k);
      _ranked = true;
    }
  }

 private:
  uint32_t _k;
  absl::Mutex _mutex;
  std::vector<score_t> _lowers ABSL_GUARDED_BY(_mutex);
  bool _ranked ABSL_GUARDED_BY(_mutex) = false;
  std::atomic<score_t> _floor{std::numeric_limits<score_t>::lowest()};
};

}  // namespace irs::ann
