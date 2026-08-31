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
#include "basics/containers/flat_hash_map.h"
#include "catalog/column_id.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/inverted_index.h"
#include "catalog/persistence/search_table_options.h"
#include "search/maintenance.h"
#include "search/search_db_wal.h"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::search {

class SearchTable;

// The text dictionaries every inverted index declared on `shard` names,
// unioned: each index allocates its own field ids, so the union is
// collision-free. Read out of the shard's own database catalog rather than the
// session's, so WAL replay -- which has no SereneDB session state -- can pass a
// null context.
catalog::TokenizerMap ResolveShardTokenizers(const SearchTable& shard,
                                             duckdb::ClientContext* context);

// Per-table iresearch columnstore store for a TableEngine::Search table -- the
// Search-engine sibling of InvertedIndexStorage. Held by the table's entry,
// which shares it with every version of that table.
class SearchTable : public std::enable_shared_from_this<SearchTable> {
 public:
  // `is_new` opens a fresh index; otherwise the durable one is reopened.
  // `options` carries the maintenance intervals resolved and persisted by the
  // catalog (mirrors InvertedIndexStorage).
  SearchTable(ObjectId db_id, ObjectId schema_id, ObjectId table_id,
              bool is_new,
              const catalog::persistence::SearchTableOptions& options,
              std::vector<catalog::ColumnId> pk_columns);
  ~SearchTable();

  SearchTable(const SearchTable&) = delete;
  SearchTable& operator=(const SearchTable&) = delete;

  // Opens this table's on-disk store and binds the database WAL; the handle is
  // attached to the catalog Table via SetData. Mirror of
  // InvertedIndexStorage::Create.
  static std::shared_ptr<SearchTable> Create(
    ObjectId db_id, ObjectId schema_id, ObjectId table_id, bool is_new,
    const catalog::persistence::SearchTableOptions& options,
    std::vector<catalog::ColumnId> pk_columns);

  ObjectId GetTableId() const noexcept { return _table_id; }
  ObjectId GetSchemaId() const noexcept { return _schema_id; }
  ObjectId GetDbId() const noexcept { return _db_id; }

  // The merged per-field index config: PRIMARY KEY columns (term-indexed +
  // still stored, so PK predicates push down) unioned with every declared
  // inverted index's entries. Returned by shared_ptr so a caller can hold one
  // immutable snapshot across a whole op while the config is RCU-swapped.
  std::shared_ptr<const catalog::InvertedIndex::Entries> GetIndexConfig()
    const noexcept;

  // Per-column list of term field_ids the write path emits under. A search
  // table stores each column value once (keyed by column id) but term-indexes
  // it once per declared index (each index's own field_id) plus the PK's term
  // at the column id, so several indexes on one column keep independent
  // analyzers.
  using TermsByColumn =
    containers::FlatHashMap<catalog::ColumnId, std::vector<irs::field_id>>;
  std::shared_ptr<const TermsByColumn> GetTermsByColumn() const noexcept;

  // The per-field iresearch encoding config (norms/compression/row-group) the
  // writer asks for at flush + merge, resolved against the merged config;
  // without it a norm-featured field trips a writer assert. Must stay
  // pointer-stable within a config generation -- the segment-reuse gate is
  // pointer identity.
  std::shared_ptr<const irs::IndexFieldOptions> GetFieldOptions()
    const noexcept;

  // Resolve the analyzer/features for `field_id` from the current config; PK
  // and keyword columns fall back to the default string tokenizer. Reads the
  // dictionaries through `context`, so this is a plan-path call, not a flush
  // one (see catalog::ResolveTokenizers).
  catalog::ColumnTokenizer GetTokenizer(duckdb::ClientContext& context,
                                        irs::field_id field_id) const;

  // Fold one inverted index's entries into the merged config, incrementally
  // (no snapshot needed).
  void MergeIndexConfig(const catalog::InvertedIndex& index);

  // Rebuild the merged config from scratch: PK columns + every inverted index
  // the relation still has. Needed for DROP INDEX -- a dropped index's columns
  // may still be covered by the PK or another index. Pass a null context to
  // read committed state, which is what a post-commit drop action wants.
  void RebuildIndexConfig(duckdb::ClientContext* context);

  auto& GetTableLock() noexcept { return _table_lock; }

  static std::filesystem::path GetPath(ObjectId db_id, ObjectId schema_id,
                                       ObjectId table_id);
  static std::filesystem::path GetWalPath(ObjectId db_id);

  // A drop commits while readers may still hold this table; the destructor
  // removes the index dir and the WAL shard once the last of them lets go.
  // Never set on shutdown or detach, where both must survive.
  void MarkDropped() noexcept {
    _dropped.store(true, std::memory_order_release);
  }

  // `exclusive_segment` is required of a writer that will record its flushed
  // segments in the WAL -- see irs::IndexWriter::GetBatch.
  irs::IndexWriter::Transaction GetTransaction(
    bool exclusive_segment = false) noexcept {
    SDB_ASSERT(_writer);
    return _writer->GetBatch(exclusive_segment);
  }

  // Re-attach a segment this shard already flushed + fsynced, named by its meta
  // file. `tick` must be in the adopting transaction's space -- it orders the
  // segment against that transaction's removals. False == cannot be reopened.
  bool AdoptSegment(std::string_view meta_file, std::string_view codec_name,
                    uint64_t tick) {
    SDB_ASSERT(_writer);
    return _writer->AdoptSegment(meta_file, irs::formats::Get(codec_name),
                                 tick);
  }

  // Called once this shard's WAL has been replayed, to reclaim what the replay
  // did not adopt (the writer was opened with cleanup suppressed). Promptness
  // only: the refresh loop's periodic cleanup would get there a tick later.
  void FinishRecovery() { CleanupUnsafe(); }

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

  uint64_t CommittedTick() const noexcept { return _last_committed_tick; }

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
  std::atomic<bool> _dropped{false};
  std::vector<catalog::ColumnId> _pk_columns;
  uint64_t _segment_memory_max;
  std::atomic<int64_t> _num_rows{0};
  mutable std::shared_mutex _table_lock;
  // Merged per-field index config (PK + declared inverted indexes), RCU-swapped
  // under _table_lock so readers holding an old snapshot keep valid entry
  // pointers. Never null after construction.
  std::shared_ptr<const catalog::InvertedIndex::Entries> _entries;
  // Column -> its term field_ids, RCU-swapped together with _entries.
  std::shared_ptr<const TermsByColumn> _terms_by_column;
  // Writer encoding config over the merged _entries, RCU-swapped with them.
  std::shared_ptr<const irs::IndexFieldOptions> _field_options;
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
