////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "ticks.h"

#include <stddef.h>

#include <atomic>

#include "basics/hybrid_logical_clock.h"
#include "catalog/identifiers/object_id.h"
#include "general_server/state.h"

namespace sdb {
namespace {

std::atomic<uint64_t> gCurrentTick = id::kMaxSystem.id();
basics::HybridLogicalClock gHybridLogicalClock;

}  // namespace

Tick NewTickHybridLogicalClock() { return gHybridLogicalClock.getTimeStamp(); }

Tick NewTickHybridLogicalClock(Tick received) {
  return gHybridLogicalClock.getTimeStamp(received);
}

Tick NewTickServer() { return ++gCurrentTick; }

void UpdateTickServer(Tick tick) {
  Tick t = tick;

  auto expected = gCurrentTick.load(std::memory_order_relaxed);

  // only update global tick if less than the specified value...
  while (expected < t &&
         !gCurrentTick.compare_exchange_weak(
           expected, t, std::memory_order_release, std::memory_order_relaxed)) {
    expected = gCurrentTick.load(std::memory_order_relaxed);
  }
}

Tick GetCurrentTickServer() { return gCurrentTick; }

Tick NewServerSpecificTick() {
  static constexpr uint64_t kLowerMask{0x000000FFFFFFFFFF};
  static constexpr uint64_t kUpperMask{0xFFFFFF0000000000};
  static constexpr size_t kUpperShift{40};

  uint64_t lower = NewTickServer() & kLowerMask;
  uint64_t upper = (static_cast<uint64_t>(ServerState::instance()->GetShortId())
                    << kUpperShift) &
                   kUpperMask;
  uint64_t tick = (upper | lower);
  return static_cast<Tick>(tick);
}

Tick NewServerSpecificTickMod4() {
  static constexpr uint64_t kLowerMask{0x000000FFFFFFFFFC};
  static constexpr uint64_t kUpperMask{0xFFFFFF0000000000};
  static constexpr size_t kLowerShift{2};
  static constexpr size_t kUpperShift{40};

  const uint64_t lower = (NewTickServer() << kLowerShift) & kLowerMask;
  const uint64_t upper =
    (static_cast<uint64_t>(ServerState::instance()->GetShortId())
     << kUpperShift) &
    kUpperMask;
  return static_cast<Tick>(upper | lower);
}

uint32_t ExtractServerIdFromTick(Tick tick) {
  static constexpr uint64_t kMask{0x0000000000FFFFFF};
  static constexpr size_t kShift{40};

  uint32_t short_id = static_cast<uint32_t>((tick >> kShift) & kMask);
  return short_id;
}

}  // namespace sdb
