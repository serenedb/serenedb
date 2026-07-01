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
#include "basics/result.h"
#include "catalog/identifiers/object_id.h"
#include "search/maintenance.h"
#include "search/search_db_wal.h"

namespace sdb::search {

// Per-table iresearch columnstore store for a TableEngine::Search table -- the
// Search-engine sibling of InvertedIndexStorage. Attached to the
// catalog::Table via GetData()/SetData(); the directory layout is derived from
// (db_id, table_id), see GetPath.
class SearchTable : public std::enable_shared_from_this<SearchTable> {
 public:
  // `is_new` opens a fresh index (and publishes an empty commit); otherwise the
  // durable index is reopened and its committed tick restored.
  SearchTable(ObjectId db_id, ObjectId schema_id, ObjectId table_id,
              bool is_new);
  ~SearchTable();

  SearchTable(const SearchTable&) = delete;
  SearchTable& operator=(const SearchTable&) = delete;

  // Opens (or reopens, when !is_new) this table's on-disk iresearch store and
  // binds the database WAL; the returned handle is attached to the catalog
  // Table via Table::SetData. Mirror of InvertedIndexStorage::Create.
  static std::shared_ptr<SearchTable> Create(ObjectId db_id, ObjectId schema_id,
                                             ObjectId table_id, bool is_new);

  ObjectId GetTableId() const noexcept { return _table_id; }
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

  // Drop on-disk artifacts. The iresearch index dir nests under the schema
  // (engine_search/<db>/<schema>/<table>), so a schema/database drop wipes it
  // wholesale -- only a standalone table drop calls DropIndexDir. The WAL chunk
  // dir + shard registration live under the per-database WAL, which no ancestor
  // drop reaches, so DropWalShard is always per-table.
  static Result DropIndexDir(ObjectId db_id, ObjectId schema_id,
                             ObjectId table_id);
  static Result DropWalShard(ObjectId db_id, ObjectId table_id);

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
  // Same interface InvertedIndexStorage exposes, so the templated RefreshLoop /
  // CompactionCoordinator (search/task.h) drive a search table too: the store
  // provides the "how" (these ops + the cadence/nudge/stale-pressure state),
  // the loops provide the "when".
  ObjectId GetId() const noexcept { return _table_id; }
  auto& GetTasksSettings() { return _maint_settings; }

  // Wake the compaction coordinator after a refresh produced new segments; the
  // coordinator polls this generation during its backoff wait.
  void NudgeCompaction() noexcept {
    _compaction_gen.fetch_add(1, std::memory_order_release);
  }
  uint64_t CompactionGeneration() const noexcept {
    return _compaction_gen.load(std::memory_order_acquire);
  }
  // Demand-driven cleanup: a non-empty compaction leaves unreferenced files and
  // raises stale pressure; the refresh loop runs cleanup once it crosses a
  // threshold (or on its periodic step), then clears it.
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

  // The maintenance ops the loops invoke. RefreshUnsafe takes _refresh_mutex
  // (try-lock; wait=true blocks) and publishes pending inserts; Compact/Cleanup
  // are lock-free (iresearch serializes them internally) so a long merge never
  // blocks the refresh chain. `field_options` is the merge's per-field encoding
  // config -- generic (nullptr) for now, since a search table is a plain
  // columnstore + PK term; the catalog Table becomes the source once
  // indexed-column support lands. Each returns timing for the loop's logging.
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
  std::atomic<int64_t> _num_rows{0};
  std::shared_mutex _table_lock;
  std::unique_ptr<irs::Directory> _dir;
  std::shared_ptr<irs::IndexWriter> _writer;
  // Borrowed from the search engine (set in OpenWriter). Outlives this object.
  SearchDbWal* _wal = nullptr;
  uint64_t _last_committed_tick = 0;

  // Background maintenance (new model -- mirrors InvertedIndexStorage).
  // _refresh_mutex serializes refresh-vs-refresh (Compact/Cleanup are
  // lock-free; iresearch serializes them internally). A zero refresh/compaction
  // interval disables that chain. The cadence lives in _maint_settings; the
  // loops poll _compaction_gen (refresh nudges) and _stale_pressure
  // (compaction-driven cleanup demand).
  TasksSettings _maint_settings;
  absl::Mutex _refresh_mutex;
  std::atomic<uint64_t> _compaction_gen{0};
  std::atomic<uint32_t> _stale_pressure{0};
#ifdef SDB_DEV
  // Dev-only double-start tripwire: StartTasks pushes one refresh + one
  // compaction loop into the engine, so a second call would create competing
  // loops. Not a release concern -- each instance is started exactly once
  // (recovery's StartSearchTableMaintenance pass, or CREATE/CTAS finalize).
  std::atomic<bool> _tasks_started{false};
#endif
};

}  // namespace sdb::search
