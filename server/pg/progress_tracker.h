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
#include <string>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "catalog/identifiers/object_id.h"

namespace sdb::pg {

// Generic progress tracking for long-running commands, modeled after
// PostgreSQL's pgstat_progress infrastructure. All pg_stat_progress_*
// views read from this single tracker — each command type uses
// its own param indices (all int64, just like PG).
//
// The views (GetTableData) do int→string mapping, not the tracker.
//
// Usage:
//   auto& tracker = ProgressTracker::Instance();
//   auto id = tracker.StartCommand({.pid = ..., .command_type = ...});
//   tracker.UpdateParam(id, CopyProgress::kTuplesProcessed, rows);
//   tracker.EndCommand(id);  // or let Guard RAII handle it.

enum class ProgressCommand : int64_t {
  Copy,
  CreateIndex,
};

// Up to 10 int64 param slots per entry, like PG's PROGRESS_NPARAMS.
inline constexpr size_t kProgressMaxParams = 20;

struct ProgressEntry {
  int64_t pid{0};
  ObjectId datid;
  std::string datname;
  ObjectId relid;
  ProgressCommand command_type;
};

class ProgressTracker {
 public:
  static ProgressTracker& Instance() {
    static ProgressTracker instance;
    return instance;
  }

  struct Snapshot {
    int64_t pid;
    ObjectId datid;
    std::string datname;
    ObjectId relid;
    ProgressCommand command_type;
    std::array<int64_t, kProgressMaxParams> params;
  };

  uint64_t StartCommand(ProgressEntry entry) {
    absl::MutexLock lock(&_mutex);
    auto id = _next_id++;
    auto e = std::make_unique<InternalEntry>();
    e->pid = entry.pid;
    e->datid = entry.datid;
    e->datname = std::move(entry.datname);
    e->relid = entry.relid;
    e->command_type = entry.command_type;
    _entries.emplace(id, std::move(e));
    return id;
  }

  void UpdateParam(uint64_t id, size_t param_idx, int64_t value) {
    absl::ReaderMutexLock lock(&_mutex);
    if (auto it = _entries.find(id); it != _entries.end()) {
      it->second->params[param_idx].store(value, std::memory_order_relaxed);
    }
  }

  // Returns a direct pointer to the atomic params array for lock-free access.
  // The pointer is stable for the lifetime of the entry (until EndCommand).
  using ParamsPtr = std::array<std::atomic<int64_t>, kProgressMaxParams>*;

  ParamsPtr GetParams(uint64_t id) {
    absl::ReaderMutexLock lock(&_mutex);
    if (auto it = _entries.find(id); it != _entries.end()) {
      return &it->second->params;
    }
    return nullptr;
  }

  void EndCommand(uint64_t id) {
    absl::MutexLock lock(&_mutex);
    _entries.erase(id);
  }

  std::vector<Snapshot> GetSnapshots() const {
    absl::ReaderMutexLock lock(&_mutex);
    std::vector<Snapshot> result;
    for (const auto& [_, e] : _entries) {
      Snapshot s;
      s.pid = e->pid;
      s.datid = e->datid;
      s.datname = e->datname;
      s.relid = e->relid;
      s.command_type = e->command_type;
      for (size_t i = 0; i < kProgressMaxParams; ++i) {
        s.params[i] = e->params[i].load(std::memory_order_relaxed);
      }
      result.push_back(std::move(s));
    }
    return result;
  }

  std::vector<Snapshot> GetSnapshots(ProgressCommand filter) const {
    absl::ReaderMutexLock lock(&_mutex);
    std::vector<Snapshot> result;
    for (const auto& [_, e] : _entries) {
      if (e->command_type != filter) {
        continue;
      }
      Snapshot s;
      s.pid = e->pid;
      s.datid = e->datid;
      s.datname = e->datname;
      s.relid = e->relid;
      s.command_type = e->command_type;
      for (size_t i = 0; i < kProgressMaxParams; ++i) {
        s.params[i] = e->params[i].load(std::memory_order_relaxed);
      }
      result.push_back(std::move(s));
    }
    return result;
  }

  // RAII guard that calls EndCommand on destruction.
  class Guard {
   public:
    Guard() : _tracker{nullptr}, _id{0} {}
    Guard(ProgressTracker& tracker, uint64_t id)
      : _tracker{&tracker}, _id{id} {}
    ~Guard() {
      if (_tracker) {
        _tracker->EndCommand(_id);
      }
    }

    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;
    Guard(Guard&& other) noexcept
      : _tracker{other._tracker}, _id{other._id} {
      other._tracker = nullptr;
    }
    Guard& operator=(Guard&& other) noexcept {
      if (this != &other) {
        if (_tracker) {
          _tracker->EndCommand(_id);
        }
        _tracker = other._tracker;
        _id = other._id;
        other._tracker = nullptr;
      }
      return *this;
    }

    uint64_t Id() const { return _id; }

   private:
    ProgressTracker* _tracker;
    uint64_t _id;
  };

  Guard MakeGuard(uint64_t id) { return Guard{*this, id}; }

 private:
  ProgressTracker() = default;

  struct InternalEntry {
    int64_t pid{0};
    ObjectId datid;
    std::string datname;
    ObjectId relid;
    ProgressCommand command_type;
    std::array<std::atomic<int64_t>, kProgressMaxParams> params{};
  };

  mutable absl::Mutex _mutex;
  containers::FlatHashMap<uint64_t, std::unique_ptr<InternalEntry>> _entries;
  uint64_t _next_id = 1;
};

// ---- Per-command param index constants ----
// All values are int64, views do int→string mapping.

namespace copy_progress {
// Matches PG's PROGRESS_COPY_* layout
inline constexpr size_t kBytesProcessed = 0;
inline constexpr size_t kBytesTotal = 1;
inline constexpr size_t kTuplesProcessed = 2;
inline constexpr size_t kTuplesExcluded = 3;
inline constexpr size_t kCommand = 4;  // CopyCommand enum
inline constexpr size_t kType = 5;     // CopyType enum

enum class Command : int64_t { CopyFrom = 1, CopyTo = 2 };
enum class Type : int64_t { File = 1, Program = 2, Pipe = 3, Callback = 4 };
}  // namespace copy_progress

namespace create_index_progress {
// Matches PG's PROGRESS_CREATEIDX_* layout
inline constexpr size_t kTuplesTotal = 0;
inline constexpr size_t kTuplesDone = 1;
inline constexpr size_t kIndexRelid = 2;
inline constexpr size_t kCommand = 3;  // CreateIndexCommand enum
inline constexpr size_t kPhase = 4;    // CreateIndexPhase enum

enum class Command : int64_t {
  CreateIndex = 1,
  CreateIndexConcurrently = 2,
  Reindex = 3,
  ReindexConcurrently = 4,
};

enum class Phase : int64_t {
  Initializing = 1,
  BuildingIndex = 2,
  WaitingForWriters = 3,
  Validating = 4,
};
}  // namespace create_index_progress

}  // namespace sdb::pg
