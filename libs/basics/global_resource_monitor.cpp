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

#include "basics/global_resource_monitor.h"

#include "basics/debugging.h"
#include "basics/logger/logger.h"
#include "basics/system-compiler.h"

namespace sdb {
namespace {

// a global shared instance for tracking all resources
GlobalResourceMonitor gInstance;

}  // namespace

int64_t GlobalResourceMonitor::current() const noexcept {
  return _current.load(std::memory_order_relaxed);
}

GlobalResourceMonitor::Stats GlobalResourceMonitor::stats() const noexcept {
  return {
    .global_limit_reached =
      _global_limit_reached_counter.load(std::memory_order_relaxed),
    .local_limit_reached =
      _local_limit_reached_counter.load(std::memory_order_relaxed),
  };
}

void GlobalResourceMonitor::trackGlobalViolation() noexcept {
  _global_limit_reached_counter.fetch_add(1, std::memory_order_relaxed);
}

void GlobalResourceMonitor::trackLocalViolation() noexcept {
  _local_limit_reached_counter.fetch_add(1, std::memory_order_relaxed);
}

/// increase global memory usage by <value> bytes. if increasing exceeds
/// the memory limit, does not perform the increase and returns false. if
/// increasing succeeds, the global value is modified and true is returned
bool GlobalResourceMonitor::increaseMemoryUsage(int64_t value) noexcept {
  SDB_ASSERT(value >= 0);
  if (_limit == 0) {
    // since we have no limit, we can simply use fetch-add for the increment
    _current.fetch_add(value, std::memory_order_relaxed);
  } else {
    // we only want to perform the update if we don't exceed the limit!
    int64_t cur = _current.load(std::memory_order_relaxed);
    int64_t next;
    do {
      next = cur + value;
      if (next > _limit) [[unlikely]] {
        return false;
      }
    } while (
      !_current.compare_exchange_weak(cur, next, std::memory_order_relaxed));
  }

  return true;
}

void GlobalResourceMonitor::decreaseMemoryUsage(int64_t value) noexcept {
  SDB_ASSERT(value >= 0);
  _current.fetch_sub(value, std::memory_order_relaxed);
}

void GlobalResourceMonitor::forceUpdateMemoryUsage(int64_t value) noexcept {
  _current.fetch_add(value, std::memory_order_relaxed);
}

GlobalResourceMonitor& GlobalResourceMonitor::instance() noexcept {
  return gInstance;
}

}  // namespace sdb
