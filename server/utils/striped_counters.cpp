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

#include "server/utils/striped_counters.h"

#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/assert.hpp>

#include "server/utils/number_of_cores.h"

namespace sdb {

StripedCounters::StripedCounters()
  : _lines(static_cast<size_t>(CountLogicalCores())) {}

void StripedCounters::Add(std::span<const int64_t> deltas) noexcept {
  SDB_ASSERT(deltas.size() <= kCounters);
  auto& line =
    _lines[duckdb::TaskScheduler::GetEstimatedCPUId() % _lines.size()];
  for (size_t i = 0; i != deltas.size(); ++i) {
    line.values[i].fetch_add(deltas[i], std::memory_order_relaxed);
  }
}

int64_t StripedCounters::Read(size_t counter) const noexcept {
  int64_t sum = 0;
  for (const auto& line : _lines) {
    sum += line.values[counter].load(std::memory_order_relaxed);
  }
  return sum;
}

void StripedCounters::Reset() noexcept {
  for (auto& line : _lines) {
    for (auto& value : line.values) {
      value.store(0, std::memory_order_relaxed);
    }
  }
}

}  // namespace sdb
