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
#include <utility>
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

enum class Param : size_t {
  BytesProcessed = 0,
  BytesTotal = 1,
  TuplesProcessed = 2,
  TuplesExcluded = 3,
  Command = 4,
  Type = 5,
  TuplesSkipped = 6,
};

enum class Command : int64_t { CopyFrom = 1, CopyTo = 2 };
enum class Type : int64_t { File = 1, Program = 2, Pipe = 3, Callback = 4 };

}  // namespace copy_progress
namespace create_index_progress {

enum class Param : size_t {
  Command = 0,
  ReservedInPG1 = 1,
  ReservedInPG2 = 2,
  LockersTotal = 3,
  LockersDone = 4,
  CurrentLockerPid = 5,
  IndexRelid = 6,
  AccessMethodOid = 7,
  Phase = 9,
  Subphase = 10,
  TuplesTotal = 11,
  TuplesDone = 12,
  PartitionsTotal = 13,
  PartitionsDone = 14,
};

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

class ProgressReporter {
 public:
  ProgressReporter(ObjectId datid, ObjectId relid, ProgressCommand command);
  ProgressReporter(const ProgressReporter&) = delete;
  ProgressReporter& operator=(const ProgressReporter&) = delete;
  ~ProgressReporter();

  template<typename Param>
  void Set(Param param, int64_t value) {
    const auto idx = std::to_underlying(param);
    SDB_ASSERT(idx < kProgressMaxParams);
    _params[idx].store(value, std::memory_order_relaxed);
  }

  template<typename Param>
  void Add(Param param, int64_t delta) {
    const auto idx = std::to_underlying(param);
    SDB_ASSERT(idx < kProgressMaxParams);
    _params[idx].fetch_add(delta, std::memory_order_relaxed);
  }

  void SetCommand(copy_progress::Command c) {
    Set(copy_progress::Param::Command, std::to_underlying(c));
  }
  void SetType(copy_progress::Type t) {
    Set(copy_progress::Param::Type, std::to_underlying(t));
  }
  void SetCommand(create_index_progress::Command c) {
    Set(create_index_progress::Param::Command, std::to_underlying(c));
  }
  void SetPhase(create_index_progress::Phase p) {
    Set(create_index_progress::Param::Phase, std::to_underlying(p));
  }

 private:
  ProgressTracker& _tracker;
  uint64_t _id;
  ProgressTracker::Params& _params;
};

}  // namespace sdb::pg
