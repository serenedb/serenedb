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

#include <cstddef>
#include <cstdint>

namespace sdb::metrics {

// A small fixed registry of process-wide gauges, modeled on ClickHouse's
// CurrentMetrics. Backed by relaxed atomics and bumped ONLY at task/connection
// boundaries (never per row or per byte), so both maintaining and reading them
// is overhead-free -- no locks, no contention on the hot paths.
//
// These are DYNAMIC runtime measurements only. Static configuration such as a
// thread-pool size is a server parameter, not a metric, and is surfaced from
// the flag registry instead.
enum class Gauge : uint8_t {
  PgConnections,      // live pg-wire connections
  HttpConnections,    // live http connections
  RefreshActive,      // refresh tasks running now
  RefreshPending,     // refresh tasks queued
  CompactionActive,   // compaction tasks running now
  CompactionPending,  // compaction tasks queued
  CleanupActive,      // cleanup tasks running now
  CleanupPending,     // cleanup tasks queued
  Count,
};

inline constexpr size_t kGaugeCount = static_cast<size_t>(Gauge::Count);

void Add(Gauge g, int64_t delta = 1) noexcept;
void Sub(Gauge g, int64_t delta = 1) noexcept;
void Set(Gauge g, int64_t value) noexcept;
int64_t Get(Gauge g) noexcept;
const char* Name(Gauge g) noexcept;
const char* Description(Gauge g) noexcept;

// RAII: +1 on construction, -1 on destruction -- for a live connection / a
// running task. One relaxed fetch_add/sub per scope, nothing per row.
class Scoped {
 public:
  explicit Scoped(Gauge gauge) noexcept : _gauge{gauge} { Add(_gauge); }
  ~Scoped() { Sub(_gauge); }
  Scoped(const Scoped&) = delete;
  Scoped& operator=(const Scoped&) = delete;

 private:
  Gauge _gauge;
};

}  // namespace sdb::metrics
