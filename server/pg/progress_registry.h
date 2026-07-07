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

#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "basics/containers/flat_hash_map.h"

namespace duckdb {

class ClientContext;
}

namespace sdb::pg {

enum class ProgressCommand : int64_t {
  None = 0,
  CopyFrom,
  CopyTo,
  CreateIndex,
  CreateTableAs,
  Analyze,
  Vacuum,
};

enum class ProgressIoType : int64_t {
  None = 0,
  File,
  Program,
  Pipe,
  Callback,
};

namespace progress_phase {

enum class CreateIndex : int64_t {
  Initializing = 1,
  BuildingIndex = 2,
  Committing = 3,
  Finalizing = 4,
};

enum class CreateTableAs : int64_t {
  Ingesting = 1,
  Committing = 2,
  Finalizing = 3,
};

enum class Analyze : int64_t {
  Initializing = 0,
  ComputingStatistics = 3,
};

enum class Vacuum : int64_t {
  Initializing = 0,
  VacuumingIndexes = 2,
};

}  // namespace progress_phase

std::string_view ProgressCommandName(ProgressCommand command);
std::string_view ProgressIoTypeName(ProgressIoType type);
std::string_view ProgressPhaseName(ProgressCommand command, int64_t phase);

// Command-specific counters for the currently executing statement, written
// directly by the execution paths that know the real numbers (wire COPY
// collector, insert/CTAS/CREATE INDEX sinks, VACUUM/ANALYZE) and read by
// sdb_progress snapshots.
struct ProgressMetrics {
  std::atomic_int64_t command{0};
  std::atomic_int64_t io_type{0};
  std::atomic_int64_t relid{0};
  std::atomic_int64_t current_relid{0};
  std::atomic_int64_t phase{0};
  std::atomic_int64_t bytes_processed{0};
  std::atomic_int64_t bytes_total{0};
  std::atomic_int64_t tuples_processed{0};
  std::atomic_int64_t tuples_total{0};
  std::atomic_int64_t stage{0};
  std::atomic_int64_t stages_total{0};
  std::atomic_int64_t step{0};
  std::atomic_int64_t steps_total{0};
  std::atomic_int64_t items_processed{0};
  std::atomic_int64_t items_total{0};

  void SetCommand(ProgressCommand c) {
    command.store(std::to_underlying(c), std::memory_order_relaxed);
  }
  void SetIoType(ProgressIoType t) {
    io_type.store(std::to_underlying(t), std::memory_order_relaxed);
  }
  template<typename Phase>
  void SetPhase(Phase p) {
    phase.store(std::to_underlying(p), std::memory_order_relaxed);
  }
  static void Set(std::atomic<int64_t>& slot, int64_t value) {
    slot.store(value, std::memory_order_relaxed);
  }
  static void Add(std::atomic<int64_t>& slot, int64_t delta) {
    slot.fetch_add(delta, std::memory_order_relaxed);
  }

  void Reset();
};

// One row of sdb_progress: a live connection. Immutable identity fields are
// set at registration; query + ctx are guarded by mu (the same detach pattern
// as CancelToken: a snapshotting thread must never touch the ClientContext
// after the owning session started tearing down).
struct ProgressSource {
  int32_t pid{0};
  int64_t datid{0};
  std::string user;
  std::string database;
  int64_t backend_start_us{0};

  absl::Mutex mu;
  std::string query;
  duckdb::ClientContext* ctx{nullptr};

  std::atomic<int64_t> query_start_us{0};
  ProgressMetrics metrics;

  void BeginQuery(std::string query_text);
  void EndQuery();
  void Detach();
};

struct ProgressSnapshot {
  int32_t pid{0};
  int64_t datid{0};
  std::string user;
  std::string database;
  std::string query;
  int64_t backend_start_us{0};
  int64_t query_start_us{0};
  double percent{-1};
  int64_t rows_processed{0};
  int64_t rows_total{0};
  int64_t command{0};
  int64_t io_type{0};
  int64_t relid{0};
  int64_t current_relid{0};
  int64_t phase{0};
  int64_t bytes_processed{0};
  int64_t bytes_total{0};
  int64_t tuples_processed{0};
  int64_t tuples_total{0};
  int64_t stage{0};
  int64_t stages_total{0};
  int64_t step{0};
  int64_t steps_total{0};
  int64_t items_processed{0};
  int64_t items_total{0};
};

class ProgressRegistry {
 public:
  static ProgressRegistry& Instance() {
    static ProgressRegistry gInstance;
    return gInstance;
  }

  void Register(std::shared_ptr<ProgressSource> source);
  void Unregister(const ProgressSource* source);

  std::vector<ProgressSnapshot> GetSnapshots() const;

 private:
  mutable absl::Mutex _mu;
  containers::FlatHashMap<const ProgressSource*,
                          std::shared_ptr<ProgressSource>>
    _sources;
};

}  // namespace sdb::pg
