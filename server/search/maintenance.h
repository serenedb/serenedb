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

#include <cstddef>
#include <cstdint>

// Background-maintenance vocabulary shared by the maintained iresearch stores
// (InvertedIndexStorage, SearchTable) and the loops in search/task.h that drive
// them.
namespace sdb::search {

// Maintenance cadence, read by the background loops. The writebuffer_*/version
// fields are only meaningful for inverted indexes; search tables leave them
// defaulted.
struct TasksSettings {
  size_t cleanup_interval_step{};
  size_t refresh_interval_msec{};
  size_t compaction_interval_msec{};
  size_t compaction_max_segments{};
  size_t compaction_max_segments_bytes{};
  size_t compaction_floor_segment_bytes{};
  uint32_t version{};
  size_t writebuffer_active{};
  size_t writebuffer_idle{};
  size_t writebuffer_size_max{};
};

enum class RefreshResult {
  Undefined = 0,
  NoChanges,
  InProgress,
  Done,
};

// Outcome of a best-effort background tick (refresh / compaction /
// cleanup): the *Impl worker throws, the *Unsafe wrapper converts it to a
// Status and the caller logs + counts it. A failure is never propagated
// past the caller.
struct ResultWithTime {
  absl::Status res;
  uint64_t time_ms;
};

}  // namespace sdb::search
