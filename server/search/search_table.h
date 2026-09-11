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

#include <absl/functional/function_ref.h>
#include <absl/status/status.h>
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/store/directory.hpp>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <vector>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "catalog/column_id.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/inverted_index.h"
#include "catalog/persistence/search_table_options.h"
#include "search/maintenance.h"
#include "search/search_db_wal.h"
#include "search/store_stats.h"
#include "search/writer_generations.h"

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

  // --- Writer accounting ---
  // A write transaction registers against this shard before it writes and
  // deregisters when it commits or aborts. Nothing is ever *excluded* -- no
  // writer waits on this, and no reader either.
  //
  // It exists for the one thing a rebuild cannot otherwise observe. A segment
  // self-reports its fields, and iresearch never lets a single segment mix two
  // configs (UpdateSegment cuts a fresh segment when the options differ), so
  // "which live segments lack this field" is already an exact question. But a
  // transaction that straddles the config swap holds its pre-swap docs in an
  // uncommitted iresearch transaction, invisible to both the reader and those
  // field lists -- so without this a rebuild could see a clean index and have
  // such a writer commit stale segments behind it.
  //
  // Two counters and a generation, rather than a set of live writers: only one
  // build runs per shard (BuildClaim), so at most one generation flip can
  // happen while a writer is registered, and a two-slot count is enough to
  // separate "registered before the swap" from "after". Registration is per
  // (transaction, shard) and cold -- the per-chunk write path never touches it.
  //
  // A writer must register BEFORE its sink reads the config, or it could land
  // in the new generation while still holding the old config.
  [[nodiscard]] unsigned RegisterWriter();
  void DeregisterWriter(unsigned slot) noexcept;

  // Opens a new writer generation and waits for every writer of the previous
  // one to finish. Call *after* publishing the config: a writer that registers
  // past the flip must be one that reads the new config, and ordering it the
  // other way round would leave a window where it reads the old one and goes
  // unwaited. Requires a held BuildClaim.
  //
  // Waits indefinitely. A deadline would only turn "someone left a transaction
  // open" into a failed CREATE INDEX that fails again on retry -- no more
  // correct, and less useful; postgres waits out old transactions the same way.
  // `cancelled` is polled between waits instead, so a cancelled statement stops
  // waiting. That, not a constant, is the escape hatch.
  void DrainPriorWriters(absl::FunctionRef<bool()> cancelled);

  // One index build per shard, across all connections: what makes the
  // two-generation accounting safe to reuse, and what keeps two builds from
  // each rewriting the table at once. Fail-fast; a losing claimant reports
  // that a build is already running. Mirrors
  // InvertedIndexStorage::ReindexClaim.
  class [[nodiscard]] BuildClaim {
   public:
    explicit BuildClaim(SearchTable& table) noexcept
      : _table{&table},
        _claimed{
          !table._build_in_flight.exchange(true, std::memory_order_acq_rel)} {}
    ~BuildClaim() {
      if (_claimed) {
        _table->_build_in_flight.store(false, std::memory_order_release);
      }
    }
    BuildClaim(const BuildClaim&) = delete;
    BuildClaim& operator=(const BuildClaim&) = delete;

    bool Claimed() const noexcept { return _claimed; }

   private:
    SearchTable* _table;
    bool _claimed;
  };

  // Whether a build holds the claim. Only DrainPriorWriters' assert reads this:
  // compaction is deliberately NOT gated on it. A build arms an
  // IndexWriter::CompactionFloorGuard over the segments it is going to rewrite
  // and leaves everything above it alone, so merges of the segments concurrent
  // inserts produce go ahead as usual -- and a flag could not have done that
  // job anyway, being check-then-act.
  bool BuildInFlight() const noexcept {
    return _build_in_flight.load(std::memory_order_acquire);
  }

  // --- Delete log ---
  // Open for the length of an index build. Records the synthetic rowids
  // deleted since the build published its config: the segments it rebuilds
  // come from a snapshot that predates those deletes, so each group swap
  // reissues them after adoption to keep a deleted row from coming back.
  //
  // Unlike the transactional build's log ([inverted_index_storage.h]) this one
  // does not *divert* deletes -- the live removal still happens, because the
  // base segments are still serving. It only records, and drains per swap
  // rather than latching closed after a single publish.
  //
  // Reissuing is idempotent: under Pillar A a rowid is never reused, so a
  // removal for a row already gone matches nothing. That is what lets every
  // swap replay the whole log so far without tracking which group saw what.
  bool IsDeleteLogOpen() const noexcept {
    return _delete_log_open.load(std::memory_order_acquire);
  }
  void OpenDeleteLog();
  void AppendDeleteLog(std::span<const int64_t> rows);
  // Drains what has accumulated; leaves the log open for the next group.
  std::vector<int64_t> TakeDeleteLog();
  void CloseDeleteLog();

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

  // --- Index build primitives (see search_table_backfill.md) ---
  // The floor a build holds over the segments it rewrites; compaction leaves
  // everything at or below it alone and keeps merging what is above.
  irs::IndexWriter::CompactionFloorGuard ArmCompactionFloor() {
    SDB_ASSERT(_writer);
    return _writer->ArmCompactionFloor();
  }
  const irs::Format::ptr& Codec() const noexcept {
    SDB_ASSERT(_writer);
    return _writer->Codec();
  }
  // One index-meta generation that retires `replaced` and adopts the
  // already-flushed segments named by `adopted_metas`.
  bool ReplaceSegments(std::span<const std::string_view> replaced,
                       std::span<const std::string_view> adopted_metas,
                       const irs::Format::ptr& codec, uint64_t tick) {
    SDB_ASSERT(_writer);
    return _writer->ReplaceSegments(replaced, adopted_metas, codec, tick);
  }

  irs::DirectoryReader GetDirectoryReader() noexcept {
    SDB_ASSERT(_writer);
    return _writer->GetSnapshot();
  }

  StoreStats GetStats() const;

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

  auto CompactUnsafeAsync(const irs::CompactionPolicy& policy,
                          const irs::MergeWriter::FlushProgress& progress,
                          bool& empty_compaction,
                          const irs::IndexFieldOptions* field_options,
                          const irs::AnnBuildEnv* env)
    -> yaclib::Future<ResultWithTime>;
  ResultWithTime CleanupUnsafe();

  // Synchronous maintenance for explicit VACUUM (REFRESH_* / COMPACT_*).
  void VacuumRefresh();
  void VacuumCompact();

 private:
  void OpenWriter();

  void CloseWriterGate();
  void OpenWriterGate() noexcept;

  // Gate state, in one word: bit 0 says a rebuild holds it closed, the rest is
  // the registered writer count.
  static constexpr uint64_t kGateClosed = 1;
  static constexpr uint64_t kWriterUnit = 2;
  static uint64_t WriterCount(uint64_t state) noexcept {
    return state / kWriterUnit;
  }
  // How long either side waits before giving up: a writer for the gate to
  // reopen, a rebuild for the registered writers to finish. Both are bounded
  // by how long a user transaction stays open, so neither can be a hard wait.

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
  std::unique_ptr<irs::Scorer> _topk_scorer;
  std::unique_ptr<irs::Directory> _dir;
  std::shared_ptr<irs::IndexWriter> _writer;
  // Borrowed from the search engine (set in OpenWriter). Outlives this object.
  SearchDbWal* _wal = nullptr;
  uint64_t _last_committed_tick = 0;

  // Background maintenance state (mirrors InvertedIndexStorage). A zero
  // refresh/compaction interval disables the loops.
  TasksSettings _maint_settings;
  absl::Mutex _refresh_mutex;

  WriterGenerations _writers;
  std::atomic<bool> _build_in_flight{false};

  // Delete log. The open flag is atomic so the commit path can skip the mutex
  // entirely when no build is running, which is the normal case.
  std::atomic<bool> _delete_log_open{false};
  absl::Mutex _delete_log_mutex;
  std::vector<int64_t> _delete_log ABSL_GUARDED_BY(_delete_log_mutex);
  // How often a waiting rebuild surfaces to check for cancellation. The
  // CondVar does the blocking; this only bounds how long a cancelled statement
  // keeps waiting.
  static constexpr absl::Duration kWriterWaitPoll = absl::Milliseconds(100);

  std::atomic<uint64_t> _compaction_gen{0};
  std::atomic<uint32_t> _stale_pressure{0};
  MaintenanceCounters _maintenance;
#ifdef SDB_DEV
  // Dev-only tripwire: asserts StartTasks runs at most once, so a bug can't
  // spawn competing maintenance loops.
  std::atomic<bool> _tasks_started{false};
#endif
};

}  // namespace sdb::search
