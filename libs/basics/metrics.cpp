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

#include "basics/metrics.h"

#include <array>
#include <atomic>

namespace sdb::metrics {
namespace {

std::array<std::atomic<int64_t>, kGaugeCount> gValues{};

constexpr std::array<const char*, kGaugeCount> kNames = {
  "pg_connections",  "http_connections",  "refresh_active",
  "refresh_pending", "compaction_active", "compaction_pending",
  "cleanup_active",  "cleanup_pending",
};

constexpr std::array<const char*, kGaugeCount> kDescriptions = {
  "Open pg-wire client connections",
  "Open HTTP client connections",
  "Search refresh tasks currently running",
  "Search refresh tasks waiting to run",
  "Search compaction tasks currently running",
  "Search compaction tasks waiting to run",
  "Search cleanup tasks currently running",
  "Search cleanup tasks waiting to run",
};

std::atomic<int64_t>& Slot(Gauge g) noexcept {
  return gValues[static_cast<size_t>(g)];
}

}  // namespace

void Add(Gauge g, int64_t delta) noexcept {
  Slot(g).fetch_add(delta, std::memory_order_relaxed);
}

void Sub(Gauge g, int64_t delta) noexcept {
  Slot(g).fetch_sub(delta, std::memory_order_relaxed);
}

void Set(Gauge g, int64_t value) noexcept {
  Slot(g).store(value, std::memory_order_relaxed);
}

int64_t Get(Gauge g) noexcept {
  return Slot(g).load(std::memory_order_relaxed);
}

const char* Name(Gauge g) noexcept { return kNames[static_cast<size_t>(g)]; }

const char* Description(Gauge g) noexcept {
  return kDescriptions[static_cast<size_t>(g)];
}

}  // namespace sdb::metrics
