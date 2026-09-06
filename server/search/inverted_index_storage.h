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

#include <absl/status/status.h>
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>

#include <atomic>
#include <filesystem>
#include <functional>
#include <iresearch/formats/ann_build_env.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/scorer.hpp>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "catalog/inverted_index.h"
#include "connector/file_manifest.h"
#include "search/maintenance.h"
#include "search/tick_domain.h"
#include "storage_engine/search_engine.h"

namespace sdb::query {

class Transaction;

}  // namespace sdb::query
namespace sdb::search {

class InvertedIndexStorage;

struct InvertedIndexSnapshot {
  InvertedIndexSnapshot(irs::DirectoryReader&& index,
                        std::shared_ptr<const FileManifest> manifest)
    : reader{std::move(index)}, file_manifest{std::move(manifest)} {}

  irs::DirectoryReader reader;
  const std::shared_ptr<const FileManifest> file_manifest;
};
using InvertedIndexSnapshotPtr = std::shared_ptr<InvertedIndexSnapshot>;

// Durable WAL cursor: generation = checkpoint iteration, offset = byte offset
// within it; bounds recovery replay to the index's un-durable tail.
struct WalCursor {
  uint64_t generation = 0;
  uint64_t offset = 0;
};

// Removes a dropped storage's directory tree. Only the leaf: an emptied
// ancestor is shared with concurrent creations, and boot's orphan sweep
// reclaims whatever is left, because a dropped object's ids are never
// reissued. A failed removal is only logged for the same reason.
void RemoveDroppedStorageDir(const std::filesystem::path& path);

// Physical representation of a search index (InvertedIndex). Owns the
// iresearch writer/reader and all mutable index state; lives in the
// SearchEngine registry keyed by index_id, not in the catalog snapshot.
class InvertedIndexStorage final
  : public std::enable_shared_from_this<InvertedIndexStorage> {
 public:
  struct Stats {
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
  };

  InvertedIndexStorage(ObjectId db_id, const catalog::InvertedIndex& index,
                       bool is_new);
  ~InvertedIndexStorage();

  // A drop commits while readers may still hold this storage; the destructor
  // removes the directory once the last of them lets go. Never set on
  // shutdown or detach, where the directory must survive.
  void MarkDropped() noexcept {
    _dropped.store(true, std::memory_order_release);
  }

  static std::filesystem::path GetPath(ObjectId db_id, ObjectId schema_id,
                                       ObjectId table_id, ObjectId index_id);

  // `db_id` is passed in rather than derived from the catalog: an index
  // created inside a transaction lives in that transaction's overlay, and so
  // may the schema its database has to be walked through.
  static std::shared_ptr<InvertedIndexStorage> Create(
    ObjectId db_id, const catalog::InvertedIndex& index, bool is_new);

  auto GetTransaction() {
    SDB_ASSERT(_writer);
    return _writer->GetBatch();
  }

  // Delete-log for online CREATE INDEX: open while the build runs, drained
  // once at publish by TakeDeleteLog, then latched forever. It holds removes
  // for rowids in [begin, end) -- rows whose backfill copy may still be
  // uncommitted, where a native remove could get lost. Outside the window
  // removes go native: below begin the copy is already committed, at/above
  // end the row came through the live writer during the build.
  //   begin: rises as backfill segments commit (stale read = over-log, safe).
  //   end:   the rowid allocator at build start, fixed.
  // AppendDeleteLog returns false once latched; the caller then removes
  // natively (a delete racing publish is an ordinary post-publish remove).
  bool IsDeleteLogOpen() const noexcept {
    return _delete_log_open.load(std::memory_order_relaxed);
  }
  bool AppendDeleteLog(std::vector<int64_t>&& rows);
  std::vector<int64_t> TakeDeleteLog();
  void SetDeleteLogRowidEnd(int64_t end) noexcept {
    _delete_log_rowid_end.store(end, std::memory_order_relaxed);
  }
  int64_t DeleteLogRowidEnd() const noexcept {
    return _delete_log_rowid_end.load(std::memory_order_relaxed);
  }
  void SetDeleteLogRowidBegin(int64_t begin) noexcept {
    _delete_log_rowid_begin.store(begin, std::memory_order_release);
  }
  int64_t DeleteLogRowidBegin() const noexcept {
    return _delete_log_rowid_begin.load(std::memory_order_acquire);
  }

  // `field_options` (nullable) is the per-merge per-column encoding config: the
  // compaction task hands the info from its own DDL view so the
  // merge encodes against that view, never the live catalog. It pins for the
  // whole synchronous merge, so non-owning.
  ResultWithTime CompactUnsafe(const irs::CompactionPolicy& policy,
                               const irs::MergeWriter::FlushProgress& progress,
                               bool& empty_compaction,
                               const irs::IndexFieldOptions* field_options);

  // CompactUnsafe driven by the caller's executor. A null `env` never suspends,
  // so the returned Future is ready on return.
  auto CompactUnsafeAsync(const irs::CompactionPolicy& policy,
                          const irs::MergeWriter::FlushProgress& progress,
                          bool& empty_compaction,
                          const irs::IndexFieldOptions* field_options,
                          const irs::AnnBuildEnv* env)
    -> yaclib::Future<ResultWithTime>;

  ResultWithTime RefreshUnsafe(bool wait,
                               const irs::ProgressReportCallback& progress,
                               RefreshResult& code,
                               bool for_checkpoint = false);

  ResultWithTime CleanupUnsafe();
  Stats UpdateStatsUnsafe(InvertedIndexSnapshotPtr data) const;

  void Refresh(const irs::ProgressReportCallback& progress = nullptr);
  // Refresh driven by the checkpoint barrier: the store WAL is about to be
  // truncated and its iteration bumped, so the stamped durable cursor must
  // carry the NEXT generation (offset 0), not the live one (see
  // RefreshUnsafeImpl). Synchronous; the flag is consumed by this call.
  void CheckpointRefresh();

  ObjectId GetId() const noexcept { return _index_id; }
  // The database whose attachment holds this index's catalog entry.
  ObjectId GetDatabaseId() const noexcept { return _db_id; }

  Stats GetStats() const;

  InvertedIndexSnapshotPtr GetInvertedIndexSnapshot() const {
    return std::atomic_load(&_snapshot);
  }

  // One REINDEX at a time per index, across all connections: claim the
  // storage for the whole refresh (observe -> delta/rebuild -> publish).
  // Fail-fast, never waits -- a losing claimant reports "already in
  // progress".
  struct ReindexClaim {
    explicit ReindexClaim(InvertedIndexStorage& storage) noexcept
      : _storage{&storage},
        _claimed{!storage._reindex_in_flight.exchange(
          true, std::memory_order_acq_rel)} {}
    ~ReindexClaim() {
      if (_claimed) {
        _storage->_reindex_in_flight.store(false, std::memory_order_release);
      }
    }
    ReindexClaim(const ReindexClaim&) = delete;
    ReindexClaim& operator=(const ReindexClaim&) = delete;
    bool Claimed() const noexcept { return _claimed; }

   private:
    InvertedIndexStorage* _storage;
    bool _claimed;
  };

  void StoreInvertedIndexSnapshot(
    InvertedIndexSnapshotPtr inverted_index_snapshot) {
    std::atomic_store(&_snapshot, std::move(inverted_index_snapshot));
  }

  std::shared_ptr<const FileManifest> GetFileManifest() const {
    return std::atomic_load(&_file_manifest);
  }
  void SetFileManifest(std::shared_ptr<const FileManifest> manifest) {
    std::atomic_store(&_file_manifest, std::move(manifest));
  }

  auto& GetTasksSettings() { return _tasks_settings; }

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

  void StartTasks();

  void FinishCreation();

  void ApplyOptions(const catalog::InvertedIndexOptions& options);

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

  // Highest tick the recovery replay has both retired and covered with a
  // cursor point; a Recovering-phase refresh commits at most this tick.
  void SetRecoveryFrontierTick(Tick tick) noexcept {
    _recovery_frontier_tick.store(tick, std::memory_order_release);
  }

 private:
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

  auto CompactUnsafeImpl(const irs::CompactionPolicy& policy,
                         const irs::MergeWriter::FlushProgress& progress,
                         bool& empty_compaction,
                         const irs::IndexFieldOptions* field_options,
                         const irs::AnnBuildEnv* env)
    -> yaclib::Future<absl::Status>;
  absl::Status RefreshUnsafeImpl(bool wait,
                                 const irs::ProgressReportCallback& progress,
                                 RefreshResult& code, bool for_checkpoint);
  absl::Status CleanupUnsafeImpl();

  ObjectId _index_id;
  // The database whose duckdb file backs the indexed table: the refresh reads
  // its checkpoint iteration to stamp the recovery cursor.
  ObjectId _db_id;
  std::filesystem::path _path;
  std::atomic<bool> _dropped{false};
  SearchEngine& _search;
  // Accessed via std::atomic_load/std::atomic_store (libc++ lacks
  // std::atomic<std::shared_ptr>).
  InvertedIndexSnapshotPtr _snapshot;
  std::shared_ptr<const FileManifest> _file_manifest;
  std::unique_ptr<irs::Directory> _dir;
  std::unique_ptr<irs::Scorer> _topk_scorer;
  std::shared_ptr<irs::IndexWriter> _writer;
  std::atomic<bool> _reindex_in_flight{false};
  TasksSettings _tasks_settings;
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
  duckdb::mutex _delete_log_mutex;
  std::atomic<bool> _delete_log_open{false};
  std::atomic<int64_t> _delete_log_rowid_end{
    std::numeric_limits<int64_t>::max()};
  std::atomic<int64_t> _delete_log_rowid_begin{0};
  std::vector<std::vector<int64_t>> _delete_log;
  std::atomic<uint64_t> _compaction_gen{0};
  std::atomic<uint32_t> _stale_pressure{0};
  std::atomic<uint64_t> _num_failed_commits{0};
  std::atomic<uint64_t> _num_failed_cleanups{0};
  std::atomic<uint64_t> _num_failed_consolidations{0};
  MovingAverageMs _avg_commit_time_ms;
  MovingAverageMs _avg_cleanup_time_ms;
  MovingAverageMs _avg_consolidation_time_ms;
  Phase _phase{Phase::Creating};
  std::atomic<Tick> _recovery_frontier_tick{0};

  irs::IResourceManager* _writers_memory{&irs::IResourceManager::gNoop};
  irs::IResourceManager* _readers_memory{&irs::IResourceManager::gNoop};
  irs::IResourceManager* _compactions_memory{&irs::IResourceManager::gNoop};
  irs::IResourceManager* _file_descriptors_count{&irs::IResourceManager::gNoop};
};

}  // namespace sdb::search
