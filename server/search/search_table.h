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
#include <vector>

#include "basics/assert.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/inverted_index.h"
#include "catalog/search_table_options.h"
#include "catalog/table_options.h"
#include "search/maintenance.h"
#include "search/search_db_wal.h"

namespace sdb::catalog {

struct Snapshot;

}  // namespace sdb::catalog
namespace sdb::search {

// Per-table iresearch columnstore store for a TableEngine::Search table -- the
// Search-engine sibling of InvertedIndexStorage. Attached to the
// catalog::Table via GetData()/SetData().
class SearchTable : public std::enable_shared_from_this<SearchTable> {
 public:
  // `is_new` opens a fresh index; otherwise the durable one is reopened.
  // `options` carries the maintenance intervals resolved and persisted by the
  // catalog (mirrors InvertedIndexStorage).
  SearchTable(ObjectId db_id, ObjectId schema_id, ObjectId table_id,
              bool is_new, const catalog::SearchTableOptions& options,
              std::vector<catalog::Column::Id> pk_columns);
  ~SearchTable();

  SearchTable(const SearchTable&) = delete;
  SearchTable& operator=(const SearchTable&) = delete;

  // Opens this table's on-disk store and binds the database WAL; the handle is
  // attached to the catalog Table via SetData. Mirror of
  // InvertedIndexStorage::Create.
  static std::shared_ptr<SearchTable> Create(
    ObjectId db_id, ObjectId schema_id, ObjectId table_id, bool is_new,
    const catalog::SearchTableOptions& options,
    std::vector<catalog::Column::Id> pk_columns);

  ObjectId GetTableId() const noexcept { return _table_id; }

  // The merged per-field index config: PRIMARY KEY columns (term-indexed +
  // still stored, so PK predicates push down) unioned with every declared
  // inverted index's entries (analyzers/features carried verbatim). The
  // Search-engine analogue of InvertedIndex::Entries, consumed by the write
  // sink + the read-side pushdown. Returned by shared_ptr so a caller can hold
  // one immutable snapshot across a whole read/write op while
  // RebuildIndexConfig swaps in a new one under _table_lock (entry pointers
  // stay valid).
  std::shared_ptr<const catalog::InvertedIndex::Entries> GetIndexConfig()
    const noexcept;

  // Resolve the analyzer/features for `field_id` from the current config; PK
  // and keyword columns fall back to the default string tokenizer. Mirrors
  // InvertedIndex::GetTokenizer via the shared catalog::TokenizerForEntry.
  catalog::ColumnTokenizer GetTokenizer(
    const std::shared_ptr<const catalog::Snapshot>& snapshot,
    irs::field_id field_id) const;

  // Fold one inverted index's entries into the merged config. Incremental (no
  // snapshot needed), so it drives the paths where the new index is in hand:
  // the per-index catalog bootstrap read and CREATE INDEX commit.
  void MergeIndexConfig(const catalog::InvertedIndex& index);

  // Rebuild the merged config from scratch: PK columns + every inverted index
  // the relation still has in `snapshot`. Used by DROP INDEX, where an index's
  // columns must drop out but may still be covered by the PK or another index.
  void RebuildIndexConfig(const catalog::Snapshot& snapshot);

  auto& GetTableLock() noexcept { return _table_lock; }

  void UpdateNumRows(int64_t delta) noexcept {
    _num_rows.fetch_add(delta, std::memory_order_relaxed);
  }
  int64_t NumRows() const noexcept {
    return _num_rows.load(std::memory_order_relaxed);
  }

  static std::filesystem::path GetPath(ObjectId db_id, ObjectId schema_id,
                                       ObjectId table_id);
  static std::filesystem::path GetWalPath(ObjectId db_id);
  static std::filesystem::path GetChunkDir(ObjectId db_id, ObjectId table_id);

  // Drop on-disk artifacts. The index dir nests under the schema, so a
  // schema/database drop already wipes it; the WAL shard lives under the
  // per-database WAL and always needs its own per-table drop.
  static absl::Status DropIndexDir(ObjectId db_id, ObjectId schema_id,
                                   ObjectId table_id);
  static absl::Status DropWalShard(ObjectId db_id, ObjectId table_id);

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

  void SyncNumRowsFromIndex() {
    SDB_ASSERT(_writer);
    auto reader = _writer->GetSnapshot();
    auto live = static_cast<int64_t>(reader.live_docs_count());
    UpdateNumRows(live - NumRows());
  }

  // --- Background maintenance ---
  // Mirrors the interface InvertedIndexStorage exposes, so the shared refresh /
  // compaction loops (search/task.h) drive a search table too.
  ObjectId GetId() const noexcept { return _table_id; }
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

  ObjectId _table_id;
  ObjectId _db_id;
  ObjectId _schema_id;
  bool _is_new;
  std::vector<catalog::Column::Id> _pk_columns;
  std::atomic<int64_t> _num_rows{0};
  mutable std::shared_mutex _table_lock;
  // Merged per-field index config (PK + declared inverted indexes), RCU-swapped
  // by RebuildIndexConfig under _table_lock so readers holding an old snapshot
  // keep valid entry pointers. Never null after construction.
  std::shared_ptr<const catalog::InvertedIndex::Entries> _entries;
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
