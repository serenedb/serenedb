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

#include "catalog/inverted_index.h"
#include "catalog/types.h"
#include "storage_engine/search_engine.h"

namespace sdb::query {

class Transaction;

}  // namespace sdb::query
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

// Durable WAL cursor: generation = checkpoint iteration, offset = byte offset
// within it; bounds recovery replay to the index's un-durable tail.
struct WalCursor {
  uint64_t generation = 0;
  uint64_t offset = 0;
};

// Physical representation of a search index (catalog::InvertedIndex). Owns the
// iresearch writer/reader and all mutable index state; lives in the
// SearchEngine registry keyed by index_id, not in the catalog snapshot.
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
  // `user_txn` (nullable) is the connection's transaction whose pending
  // per-conn iresearch staging we need to drop before Clear -- the new-arch
  // analog of the old SearchTrxState cookie cleanup. Pass nullptr from
  // contexts that don't have a user transaction (WAL recovery).
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
  // Refresh driven by the checkpoint barrier: the store WAL is about to be
  // truncated and its iteration bumped, so the stamped durable cursor must
  // carry the NEXT generation (offset 0), not the live one (see
  // RefreshUnsafeImpl). Synchronous; the flag is consumed by this call.
  void CheckpointRefresh();

  ObjectId GetId() const noexcept { return _index_id; }
  auto GetState() const noexcept { return _state; }

  Stats GetStats() const;

  auto& GetMutex() { return _mutex; }

  InvertedIndexSnapshotPtr GetInvertedIndexSnapshot() const {
    return std::atomic_load(&_snapshot);
  }

  void StoreInvertedIndexSnapshot(
    InvertedIndexSnapshotPtr inverted_index_snapshot) {
    std::atomic_store(&_snapshot, std::move(inverted_index_snapshot));
  }

  auto& GetTasksSettings() { return _tasks_settings; }

  void StartTasks() {
    ScheduleRefresh({});
    ScheduleCompaction({});
  }

  void FinishCreation();

  Tick GetRecoveryTick() const noexcept { return _recovery_tick; }

  // Durable WAL cursor (store-table WAL generation + byte offset) read back
  // from the segment meta at open. Recovery replays only operations at or past
  // it (operations strictly below are already durable in the segments). The
  // refresh stamps the exact WAL end offset of the highest batch it flushed
  // (see RefreshUnsafeImpl).
  WalCursor GetRecoveryWalCursor() const noexcept {
    return _recovery_wal_cursor;
  }

  // Per-index map from a search commit tick to the store-WAL cursor that the
  // commit's WAL bytes end at. CommitSearch records one entry per settled batch
  // BEFORE the batch becomes flushable (before IndexWriter::Transaction::Commit
  // emplaces it), and after the store WAL is durable, so the recorded offset is
  // that commit's exact WAL end offset; commits serialize, so ticks and WAL
  // offsets arrive in the same order.
  void RecordFlushCursor(Tick tick, WalCursor cursor) noexcept;
  // Cursor of the highest recorded tick <= `tick`, or {0, 0} if none. Prunes
  // entries strictly below the returned one for THIS index (they can never be
  // selected again here), which is safe because the table is per-index.
  WalCursor CursorAtOrBelow(Tick tick) noexcept;

  // The index lost a committed transaction's rows (an iresearch tick commit
  // failed after the store transaction was already durable). The storage keeps
  // serving, but the clean-shutdown checkpoint is suppressed so the next
  // boot rebuilds it from the store table.
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

  // Persisted in the segment meta payload to survive iceberg compactions. 0 =
  // not pinned.
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
  // Accessed via std::atomic_load/std::atomic_store (libc++ lacks
  // std::atomic<std::shared_ptr>).
  InvertedIndexSnapshotPtr _snapshot;
  std::unique_ptr<irs::Directory> _dir;
  std::unique_ptr<irs::Scorer> _topk_scorer;
  std::shared_ptr<irs::IndexWriter> _writer;
  TasksSettings _tasks_settings;
  absl::Mutex _mutex;
  absl::Mutex _refresh_mutex;

  Tick _recovery_tick{0};
  Tick _last_durable_tick{0};
  // Durable store-WAL cursor (generation + byte offset). Captured from the
  // store WAL at refresh -> _pending_wal_cursor -> stamped into the segment
  // meta. _recovery_wal_cursor is read back from the meta at open (the recovery
  // skip bound).
  WalCursor _pending_wal_cursor;
  WalCursor _recovery_wal_cursor;
  // When true, the meta payload provider stamps _pending_wal_cursor from
  // CursorAtOrBelow(_last_durable_tick) -- the durable tick it is persisting in
  // that same call. When false (checkpoint refresh), _pending_wal_cursor was
  // already set by RefreshUnsafeImpl (next generation, offset 0) and is left
  // as-is.
  bool _stamp_cursor_from_flush{false};
  // Per-index commit-tick -> store-WAL cursor table. Recorded by
  // CommitSearch/FinishReplay before a batch becomes flushable; consumed by
  // the meta payload provider via CursorAtOrBelow(_last_durable_tick).
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
