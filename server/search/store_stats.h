////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <absl/status/status.h>

#include <atomic>
#include <cstdint>

#include "search/maintenance.h"

namespace irs {

class DirectoryReader;

}  // namespace irs
namespace sdb::search {

struct StoreStats {
  // NOLINTBEGIN
  uint64_t numDocs = 0;
  uint64_t numLiveDocs = 0;
  uint64_t numBufferedDocs = 0;
  uint64_t numSegments = 0;
  uint64_t numFiles = 0;
  uint64_t indexSize = 0;
  uint64_t numFailedCommits = 0;
  uint64_t numFailedCleanups = 0;
  uint64_t numFailedConsolidations = 0;
  uint64_t avgCommitTimeMs = 0;
  uint64_t avgCleanupTimeMs = 0;
  uint64_t avgConsolidationTimeMs = 0;
  // NOLINTEND

  static StoreStats FromReader(const irs::DirectoryReader& reader);
};

class MovingAverageMs {
 public:
  void Record(uint64_t time_ms) noexcept {
    const uint64_t old =
      _time_num.fetch_add((time_ms << 32U) + 1, std::memory_order_relaxed);
    const uint64_t old_time = old >> 32U;
    const uint64_t old_num = static_cast<uint32_t>(old);
    if (old_num >= kWindow) {
      _time_num.fetch_sub(((old_time / old_num) << 32U) + 1,
                          std::memory_order_relaxed);
    }
  }
  uint64_t Average() const noexcept {
    const uint64_t v = _time_num.load(std::memory_order_relaxed);
    const uint64_t time = v >> 32U;
    const uint64_t num = static_cast<uint32_t>(v);
    return num == 0 ? 0 : time / num;
  }

 private:
  static constexpr uint64_t kWindow = 10;
  std::atomic<uint64_t> _time_num{0};
};

class MaintenanceCounters {
 public:
  void RecordCommit(const absl::Status& result, RefreshResult code,
                    uint64_t time_ms) noexcept {
    if (!result.ok()) {
      _failed_commits.fetch_add(1, std::memory_order_relaxed);
    } else if (code == RefreshResult::Done) {
      _commit_time_ms.Record(time_ms);
    }
  }
  void RecordCompaction(const absl::Status& result, bool empty_compaction,
                        uint64_t time_ms) noexcept {
    if (!result.ok()) {
      _failed_consolidations.fetch_add(1, std::memory_order_relaxed);
    } else if (!empty_compaction) {
      _consolidation_time_ms.Record(time_ms);
    }
  }
  void RecordCleanup(const absl::Status& result, uint64_t time_ms) noexcept {
    if (!result.ok()) {
      _failed_cleanups.fetch_add(1, std::memory_order_relaxed);
    } else {
      _cleanup_time_ms.Record(time_ms);
    }
  }
  void Fill(StoreStats& stats) const noexcept {
    stats.numFailedCommits = _failed_commits.load(std::memory_order_relaxed);
    stats.numFailedCleanups = _failed_cleanups.load(std::memory_order_relaxed);
    stats.numFailedConsolidations =
      _failed_consolidations.load(std::memory_order_relaxed);
    stats.avgCommitTimeMs = _commit_time_ms.Average();
    stats.avgCleanupTimeMs = _cleanup_time_ms.Average();
    stats.avgConsolidationTimeMs = _consolidation_time_ms.Average();
  }

 private:
  std::atomic<uint64_t> _failed_commits{0};
  std::atomic<uint64_t> _failed_cleanups{0};
  std::atomic<uint64_t> _failed_consolidations{0};
  MovingAverageMs _commit_time_ms;
  MovingAverageMs _cleanup_time_ms;
  MovingAverageMs _consolidation_time_ms;
};

class MaintainedStore {
 public:
  auto& GetTasksSettings() noexcept { return _tasks_settings; }

  // Wake the compaction coordinator: a refresh that produced new segments bumps
  // this generation so the coordinator re-evaluates without waiting for its
  // timer. The coordinator polls CompactionGeneration() during its backoff
  // wait.
  void NudgeCompaction() noexcept {
    _compaction_gen.fetch_add(1, std::memory_order_release);
  }
  uint64_t CompactionGeneration() const noexcept {
    return _compaction_gen.load(std::memory_order_acquire);
  }

  // Demand-driven cleanup: a non-empty compaction leaves unreferenced files, so
  // it raises stale pressure. The refresh loop runs cleanup once the pressure
  // crosses a small threshold (or on its periodic step), clearing it.
  void BumpStalePressure() noexcept {
    _stale_pressure.fetch_add(1, std::memory_order_relaxed);
  }
  uint32_t StalePressure() const noexcept {
    return _stale_pressure.load(std::memory_order_relaxed);
  }
  void ClearStalePressure() noexcept {
    _stale_pressure.store(0, std::memory_order_relaxed);
  }

 protected:
  template<typename Options>
  void SetTasksSettings(const Options& options) {
    _tasks_settings.refresh_interval_msec = options.refresh_interval_ms;
    _tasks_settings.compaction_interval_msec = options.compaction_interval_ms;
    _tasks_settings.cleanup_interval_step = options.cleanup_interval_step;
    _tasks_settings.compaction_max_segments = options.compaction_max_segments;
    _tasks_settings.compaction_max_segments_bytes =
      options.compaction_max_segments_bytes;
    _tasks_settings.compaction_floor_segment_bytes =
      options.compaction_floor_segment_bytes;
  }

  TasksSettings _tasks_settings;
  MaintenanceCounters _maintenance;

 private:
  std::atomic<uint64_t> _compaction_gen{0};
  std::atomic<uint32_t> _stale_pressure{0};
};

}  // namespace sdb::search
