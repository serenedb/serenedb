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
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/directory.hpp>
#include <memory>
#include <mutex>
#include <shared_mutex>

#include "basics/assert.h"
#include "catalog1/entry/search_table.h"
#include "search/maintenance.h"
#include "search/search_db_wal.h"

namespace sdb::search {

class SearchTable : public std::enable_shared_from_this<SearchTable> {
 public:
  // `is_new` opens a fresh index; otherwise the durable one is reopened.
  // `options` carries the maintenance intervals resolved and persisted by the
  // catalog (mirrors InvertedIndexStorage).
  SearchTable(duckdb::idx_t db_id, duckdb::idx_t schema_id,
              duckdb::idx_t table_id, bool is_new,
              const catalog::SearchTableOptions& options);
  ~SearchTable();

  SearchTable(const SearchTable&) = delete;
  SearchTable& operator=(const SearchTable&) = delete;
  static std::shared_ptr<SearchTable> Create(
    duckdb::idx_t db_id, duckdb::idx_t schema_id, duckdb::idx_t table_id,
    bool is_new, const catalog::SearchTableOptions& options);

  duckdb::idx_t GetTableId() const noexcept { return _table_id; }
  auto& GetTableLock() noexcept { return _table_lock; }

  static std::filesystem::path GetPath(duckdb::idx_t db_id,
                                       duckdb::idx_t schema_id,
                                       duckdb::idx_t table_id);
  static std::filesystem::path GetWalPath(duckdb::idx_t db_id);
  static std::filesystem::path GetChunkDir(duckdb::idx_t db_id,
                                           duckdb::idx_t table_id);

  // A drop commits while readers may still hold this table; the destructor
  // removes the index dir and the WAL shard once the last of them lets go.
  // Never set on shutdown or detach, where both must survive.
  void MarkDropped() noexcept {
    _dropped.store(true, std::memory_order_release);
  }

  irs::IndexWriter::Transaction GetTransaction() noexcept {
    SDB_ASSERT(_writer);
    return _writer->GetBatch();
  }

  irs::DirectoryReader GetDirectoryReader() noexcept {
    SDB_ASSERT(_writer);
    return _writer->GetSnapshot();
  }

  void Commit() {
    SDB_ASSERT(_writer && _wal);
    _writer->RefreshCommit();
    _wal->OnShardCommit(GetTableId(), _last_committed_tick);
  }

  void Clear(uint64_t tick) {
    SDB_ASSERT(_writer);
    _writer->Clear(tick);
    if (tick > _last_committed_tick) {
      _last_committed_tick = tick;
    }
  }

  SearchDbWal& Wal() noexcept {
    SDB_ASSERT(_wal);
    return *_wal;
  }

  SearchDbWal::ChunkWriter NewChunkWriter() {
    SDB_ASSERT(_wal);
    return _wal->NewChunkWriter(GetTableId());
  }

  uint64_t CommittedTick() const noexcept { return _last_committed_tick; }

  // --- Background maintenance ---
  // Mirrors the interface InvertedIndexStorage exposes, so the shared refresh /
  // compaction loops (search/task.h) drive a search table too.
  duckdb::idx_t GetId() const noexcept { return _table_id; }
  auto& GetTasksSettings() { return _maint_settings; }

  // Wake the compaction loop after a refresh produced new segments.
  void NudgeCompaction() noexcept {
    _compaction_gen.fetch_add(1, std::memory_order_release);
  }
  uint64_t CompactionGeneration() const noexcept {
    return _compaction_gen.load(std::memory_order_acquire);
  }
  // Compaction leaves unreferenced files behind; raising stale pressure signals
  // the refresh loop to run cleanup.
  void BumpStalePressure() noexcept {
    _stale_pressure.fetch_add(1, std::memory_order_relaxed);
  }
  uint32_t StalePressure() const noexcept {
    return _stale_pressure.load(std::memory_order_relaxed);
  }
  void ClearStalePressure() noexcept {
    _stale_pressure.store(0, std::memory_order_relaxed);
  }

  // Launch this table's refresh + compaction loops (via SearchEngine). Call
  // once, after the table is open and recovery (if any) finalized.
  void StartTasks();

  // The maintenance ops the loops invoke: RefreshUnsafe publishes pending
  // inserts, CompactUnsafe merges segments, CleanupUnsafe reclaims unreferenced
  // files.
  ResultWithTime RefreshUnsafe(bool wait,
                               const irs::ProgressReportCallback& progress,
                               RefreshResult& code);
  ResultWithTime CompactUnsafe(const irs::CompactionPolicy& policy,
                               const irs::MergeWriter::FlushProgress& progress,
                               bool& empty_compaction,
                               const irs::IndexFieldOptions* field_options);
  ResultWithTime CleanupUnsafe();

  // Synchronous maintenance for explicit VACUUM (REFRESH_* / COMPACT_*).
  void VacuumRefresh();
  void VacuumCompact();

 private:
  void OpenWriter();

  duckdb::idx_t _table_id;
  duckdb::idx_t _db_id;
  duckdb::idx_t _schema_id;
  bool _is_new;
  std::atomic<bool> _dropped{false};
  std::shared_mutex _table_lock;
  std::unique_ptr<irs::Directory> _dir;
  std::shared_ptr<irs::IndexWriter> _writer;
  // Borrowed from the search engine (set in OpenWriter). Outlives this object.
  SearchDbWal* _wal = nullptr;
  uint64_t _last_committed_tick = 0;

  // Background maintenance state (mirrors InvertedIndexStorage). A zero
  // refresh/compaction interval disables the loops.
  TasksSettings _maint_settings;
  absl::Mutex _refresh_mutex;
  std::atomic<uint64_t> _compaction_gen{0};
  std::atomic<uint32_t> _stale_pressure{0};
#ifdef SDB_DEV
  // Dev-only tripwire: asserts StartTasks runs at most once, so a bug can't
  // spawn competing maintenance loops.
  std::atomic<bool> _tasks_started{false};
#endif
};

}  // namespace sdb::search
