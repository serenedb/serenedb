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

#include <atomic>
#include <cstdint>

#include "catalog/types.h"

namespace sdb::search {

class TickDomain {
 public:
  static TickDomain& Instance() noexcept {
    static TickDomain domain;
    return domain;
  }

  Tick Advance(uint64_t count) noexcept {
    return _tick.fetch_add(count, std::memory_order_relaxed) + count;
  }

  Tick Current() const noexcept {
    return _tick.load(std::memory_order_relaxed);
  }

  void SeedAtLeast(Tick tick) noexcept {
    auto current = _tick.load(std::memory_order_relaxed);
    while (current < tick && !_tick.compare_exchange_weak(
                               current, tick, std::memory_order_relaxed)) {
    }
  }

 private:
  std::atomic<Tick> _tick{1};
};

}  // namespace sdb::search
