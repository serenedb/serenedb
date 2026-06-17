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

#pragma once

#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>

#include <atomic>
#include <filesystem>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/scorer.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>

#include "catalog/inverted_index.h"
#include "catalog/types.h"
#include "storage_engine/search_engine.h"

namespace sdb::query {

class Transaction;

}
namespace sdb::search {

class InvertedIndexStorage;

struct ThreadPoolState {
  std::atomic_size_t pending_refreshes{0};
  std::atomic_size_t non_empty_refreshes{0};
  std::atomic_size_t pending_compactions{0};
  std::atomic_size_t noop_compaction_count{0};
  std::atomic_size_t noop_refresh_count{0};
};

struct TasksSettings {
  size_t cleanup_interval_step{};
  size_t refresh_interval_msec{};
  size_t compaction_interval_msec{};
  irs::CompactionPolicy compaction_policy;
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

struct InvertedIndexSnapshot {
  explicit InvertedIndexSnapshot(irs::DirectoryReader&& index)
    : reader{std::move(index)} {}

  irs::DirectoryReader reader;
};
using InvertedIndexSnapshotPtr = std::shared_ptr<InvertedIndexSnapshot>;

struct WalCursor {
  uint64_t generation = 0;
  uint64_t offset = 0;
};

class InvertedIndexStorage final
  : public std::enable_shared_from_this<InvertedIndexStorage> {
 public:
  struct Stats {
    // NOLINTBEGIN
    uint64_t numDocs = 0;
    uint64_t numLiveDocs = 0;
    uint64_t numSegments = 0;
    uint64_t numFiles = 0;
    uint64_t indexSize = 0;
    // NOLINTEND
  };

  struct ResultWithTime {
    Result res;
    uint64_t time_ms;
  };

  InvertedIndexStorage(ObjectId id, const catalog::InvertedIndex& index,
                       bool is_new);

  static std::filesystem::path GetPath(ObjectId db_id, ObjectId schema_id,
                                       ObjectId table_id, ObjectId index_id,
                                       ObjectId storage_id);

  static std::shared_ptr<InvertedIndexStorage> Create(
    ObjectId id, const catalog::InvertedIndex& index, bool is_new);

  struct TruncateGuard {
    struct UnlockDeleter {
      void operator()(absl::Mutex* m) const ABSL_NO_THREAD_SAFETY_ANALYSIS {
        m->Unlock();
      }
    };
    using Ptr = std::unique_ptr<absl::Mutex, UnlockDeleter>;
    Ptr mutex;
  };
  TruncateGuard TruncateBegin() ABSL_NO_THREAD_SAFETY_ANALYSIS {
    _refresh_mutex.Lock();
    return {TruncateGuard::Ptr{&_refresh_mutex}};
  }
  void TruncateCommit(TruncateGuard&& guard, Tick tick,
                      query::Transaction* user_txn);

  auto GetTransaction() {
    SDB_ASSERT(_writer);
    return _writer->GetBatch();
  }

  ResultWithTime CompactUnsafe(const irs::CompactionPolicy& policy,
                               const irs::MergeWriter::FlushProgress& progress,
                               bool& empty_compaction);

  ResultWithTime RefreshUnsafe(bool wait,
                               const irs::ProgressReportCallback& progress,
                               RefreshResult& code,
                               bool for_checkpoint = false);

  ResultWithTime CleanupUnsafe();
  Stats UpdateStatsUnsafe(InvertedIndexSnapshotPtr data) const;

  void ScheduleCompaction(absl::Duration delay);
  void ScheduleRefresh(absl::Duration delay);

  void Refresh();
  void CheckpointRefresh();

  ObjectId GetId() const noexcept { return _index_id; }
  auto GetState() const noexcept { return _state; }

  Stats GetStats() const;

  auto& GetMutex() { return _mutex; }

  InvertedIndexSnapshotPtr GetInvertedIndexSnapshot() const {
    std::shared_lock guard{_snapshot_mutex};
    return _snapshot;
  }

  void StoreInvertedIndexSnapshot(
    InvertedIndexSnapshotPtr inverted_index_snapshot) {
    std::unique_lock guard{_snapshot_mutex};
    _snapshot = std::move(inverted_index_snapshot);
  }

  auto& GetTasksSettings() { return _tasks_settings; }

  void StartTasks() {
    ScheduleRefresh({});
    ScheduleCompaction({});
  }

  void FinishCreation();

  Tick GetRecoveryTick() const noexcept { return _recovery_tick; }

  WalCursor GetRecoveryWalCursor() const noexcept {
    return _recovery_wal_cursor;
  }

  void RecordFlushCursor(Tick tick, WalCursor cursor) noexcept;
  WalCursor CursorAtOrBelow(Tick tick) noexcept;

  void MarkOutOfSync() noexcept {
    _out_of_sync.store(true, std::memory_order_relaxed);
  }
  bool IsOutOfSync() const noexcept {
    return _out_of_sync.load(std::memory_order_relaxed);
  }

  enum class Phase : uint8_t {
    Creating,
    Recovering,
    Active,
  };

  void StartRecovery() noexcept {
    std::lock_guard lock{_refresh_mutex};
    SDB_ASSERT(_phase == Phase::Creating);
    _phase = Phase::Recovering;
  }

  void SetIcebergSnapshotId(int64_t id) noexcept { _iceberg_snapshot_id = id; }
  int64_t GetIcebergSnapshotId() const noexcept { return _iceberg_snapshot_id; }

 private:
  Result CompactUnsafeImpl(const irs::CompactionPolicy& policy,
                           const irs::MergeWriter::FlushProgress& progress,
                           bool& empty_compaction);
  Result RefreshUnsafeImpl(bool wait,
                           const irs::ProgressReportCallback& progress,
                           RefreshResult& code, bool for_checkpoint);
  Result CleanupUnsafeImpl();

  ObjectId _index_id;
  SearchEngine& _search;
  std::shared_ptr<ThreadPoolState> _state;
  mutable std::shared_mutex _snapshot_mutex;
  InvertedIndexSnapshotPtr _snapshot;
  std::unique_ptr<irs::Directory> _dir;
  std::unique_ptr<irs::Scorer> _topk_scorer;
  std::shared_ptr<irs::IndexWriter> _writer;
  TasksSettings _tasks_settings;
  absl::Mutex _mutex;
  absl::Mutex _refresh_mutex;

  Tick _recovery_tick{0};
  Tick _last_durable_tick{0};
  WalCursor _pending_wal_cursor;
  WalCursor _recovery_wal_cursor;
  bool _stamp_cursor_from_flush{false};
  duckdb::mutex _flush_cursors_mutex;
  std::map<Tick, WalCursor> _flush_cursors;
  std::atomic<bool> _out_of_sync{false};
  int64_t _iceberg_snapshot_id{0};
  Phase _phase{Phase::Creating};

  irs::IResourceManager* _writers_memory{&irs::IResourceManager::gNoop};
  irs::IResourceManager* _readers_memory{&irs::IResourceManager::gNoop};
  irs::IResourceManager* _compactions_memory{&irs::IResourceManager::gNoop};
  irs::IResourceManager* _file_descriptors_count{&irs::IResourceManager::gNoop};
};

}  // namespace sdb::search
