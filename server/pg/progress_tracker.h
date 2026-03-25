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

#include <array>
#include <atomic>
#include <ranges>
#include <vector>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "catalog/identifiers/object_id.h"

namespace sdb::pg {

enum class ProgressCommand : int64_t {
  Copy,
  CreateIndex,
};

inline constexpr size_t kProgressMaxParams = 20;

struct ProgressEntry {
  int64_t pid{0};
  ObjectId datid;
  ObjectId relid;
  ProgressCommand command_type;

  ProgressEntry& Base() { return *this; }
  const ProgressEntry& Base() const { return *this; }
};

struct ProgressSnapshot : ProgressEntry {
  std::array<int64_t, kProgressMaxParams> params{};
};

class ProgressTracker {
 public:
  static ProgressTracker& Instance() {
    static ProgressTracker gInstance;
    return gInstance;
  }

  uint64_t StartCommand(ProgressEntry entry) {
    absl::MutexLock lock{&_mutex};
    auto e = std::make_unique<InternalEntry>();
    e->Base() = std::move(entry);
    auto id = _next_id++;
    _entries.emplace(id, std::move(e));
    return id;
  }

  void UpdateParam(uint64_t id, size_t param_idx, int64_t value) {
    absl::ReaderMutexLock lock(&_mutex);
    if (auto it = _entries.find(id); it != _entries.end()) {
      it->second->params[param_idx].store(value, std::memory_order_relaxed);
    }
  }

  using Params = std::array<std::atomic<int64_t>, kProgressMaxParams>;

  Params& GetParams(uint64_t id) {
    absl::ReaderMutexLock lock(&_mutex);
    auto it = _entries.find(id);
    SDB_ASSERT(it != _entries.end());
    return it->second->params;
  }

  void EndCommand(uint64_t id) {
    absl::MutexLock lock(&_mutex);
    _entries.erase(id);
  }

  std::vector<ProgressSnapshot> GetSnapshots() const {
    absl::ReaderMutexLock lock{&_mutex};
    auto view =
      _entries | std::views::values |
      std::views::transform([](const auto& e) { return e->Snapshot(); });
    return {view.begin(), view.end()};
  }

 private:
  struct InternalEntry : ProgressEntry {
    std::array<std::atomic<int64_t>, kProgressMaxParams> params{};

    ProgressSnapshot Snapshot() const {
      ProgressSnapshot s{Base()};
      for (size_t i = 0; i < kProgressMaxParams; ++i) {
        s.params[i] = params[i].load(std::memory_order_relaxed);
      }
      return s;
    }
  };

  mutable absl::Mutex _mutex;
  containers::FlatHashMap<uint64_t, std::unique_ptr<InternalEntry>> _entries;
  uint64_t _next_id = 1;
};

namespace copy_progress {

inline constexpr size_t kBytesProcessed = 0;
inline constexpr size_t kBytesTotal = 1;
inline constexpr size_t kTuplesProcessed = 2;
inline constexpr size_t kTuplesExcluded = 3;
inline constexpr size_t kCommand = 4;
inline constexpr size_t kType = 5;
inline constexpr size_t kTuplesSkipped = 6;

enum class Command : int64_t { CopyFrom = 1, CopyTo = 2 };
enum class Type : int64_t { File = 1, Program = 2, Pipe = 3, Callback = 4 };

}  // namespace copy_progress
namespace create_index_progress {

inline constexpr size_t kCommand = 0;
inline constexpr size_t kReservedInPG1 = 1;
inline constexpr size_t kReservedInPG2 = 2;
inline constexpr size_t kLockersTotal = 3;
inline constexpr size_t kLockersDone = 4;
inline constexpr size_t kCurrentLockerPid = 5;
inline constexpr size_t kIndexRelid = 6;
inline constexpr size_t kAccessMethodOid = 7;
inline constexpr size_t kPhase = 9;
inline constexpr size_t kSubphase = 10;
inline constexpr size_t kTuplesTotal = 11;
inline constexpr size_t kTuplesDone = 12;
inline constexpr size_t kPartitionsTotal = 13;
inline constexpr size_t kPartitionsDone = 14;

enum class Command : int64_t {
  CreateIndex = 1,
  CreateIndexConcurrently = 2,
  Reindex = 3,
  ReindexConcurrently = 4,
};

// SereneDB-specific phases (differs from PostgreSQL)
enum class Phase : int64_t {
  Initializing = 1,
  BuildingIndex = 2,
  Committing = 3,
  Finalizing = 4,
};

}  // namespace create_index_progress

class ProgressReporterBase {
 public:
  ProgressReporterBase(const ProgressReporterBase&) = delete;
  ProgressReporterBase& operator=(const ProgressReporterBase&) = delete;
  ~ProgressReporterBase();

 protected:
  ProgressReporterBase(ObjectId relid, ProgressCommand command_type,
                       ObjectId datid);

  ProgressTracker& _tracker;
  uint64_t _id;
  ProgressTracker::Params& _params;
};

class CopyProgressReporter : public ProgressReporterBase {
 public:
  CopyProgressReporter(ObjectId datid, ObjectId relid,
                       copy_progress::Command command,
                       copy_progress::Type type);

  void ReportBatch(uint64_t delta_rows, uint64_t delta_bytes,
                   uint64_t delta_excluded);
  void SetBytesTotal(int64_t bytes);
};

class IndexProgressReporter : public ProgressReporterBase {
 public:
  IndexProgressReporter(ObjectId datid, ObjectId relid,
                        create_index_progress::Command command,
                        create_index_progress::Phase phase,
                        ObjectId index_relid);

  void SetPhase(create_index_progress::Phase phase);
  void ReportBatch(uint64_t delta_rows);
  void SetTuplesTotal(uint64_t rows);
};

}  // namespace sdb::pg
