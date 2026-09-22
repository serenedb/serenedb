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
#include <iresearch/formats/ann_build_env.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/scorers/scorer.hpp>
#include <iresearch/search/scorers/scorer_options.hpp>
#include <iresearch/store/directory.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/async.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "search/maintenance.h"
#include "search/search_db_wal.h"
#include "search/store_stats.h"
#include "search/writer_generations.h"

namespace sdb::search {

class SearchTable final : public std::enable_shared_from_this<SearchTable> {
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
    bool is_new, const catalog::SearchTableOptions& options) {
    return std::make_shared<SearchTable>(db_id, schema_id, table_id, is_new,
                                         options);
  }

  std::shared_ptr<const catalog::InvertedIndexConfig> Config() const;
  void MergeIndexConfig(
    duckdb::idx_t index_oid,
    std::shared_ptr<const catalog::InvertedIndexConfig> config);
  void RemoveIndexConfig(duckdb::idx_t index_oid);

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

  irs::IndexWriter::Transaction GetTransaction(
    bool exclusive_segment = false) noexcept {
    return _writer->GetBatch(exclusive_segment);
  }

  irs::DirectoryReader GetDirectoryReader() noexcept {
    return _writer->GetSnapshot();
  }

  void Commit() {
    _writer->RefreshCommit();
    _wal->OnShardCommit(GetTableId(), _last_committed_tick);
  }

  void Clear(uint64_t tick) {
    _writer->Clear(tick);
    if (tick > _last_committed_tick) {
      _last_committed_tick = tick;
    }
  }

  SearchDbWal& Wal() noexcept { return *_wal; }

  SearchDbWal::ChunkWriter NewChunkWriter() {
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
                               const irs::IndexFieldOptions* field_options) {
    return irs::GetReady(CompactUnsafeAsync(policy, progress, empty_compaction,
                                            field_options, nullptr));
  }
  auto CompactUnsafeAsync(const irs::CompactionPolicy& policy,
                          const irs::MergeWriter::FlushProgress& progress,
                          bool& empty_compaction,
                          const irs::IndexFieldOptions* field_options,
                          const irs::AnnBuildEnv* env)
    -> yaclib::Future<ResultWithTime>;
  ResultWithTime CleanupUnsafe();

  const std::optional<irs::ScorerOptions>& TopKScorer() const noexcept {
    return _topk_options;
  }

  StoreStats GetStats() const;

  // Synchronous maintenance for explicit VACUUM (REFRESH_* / COMPACT_*).
  void VacuumRefresh();
  void VacuumCompact();

  [[nodiscard]] unsigned RegisterWriter() { return _writers.Register(); }
  void DeregisterWriter(unsigned slot) noexcept { _writers.Deregister(slot); }
  void DrainPriorWriters(absl::FunctionRef<bool()> cancelled);

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

  bool BuildInFlight() const noexcept {
    return _build_in_flight.load(std::memory_order_acquire);
  }

  bool IsDeleteLogOpen() const noexcept {
    return _delete_log_open.load(std::memory_order_acquire);
  }
  void OpenDeleteLog();
  void AppendDeleteLog(std::span<const int64_t> rows);

  template<typename Fn>
  bool SwapWithDrainedDeletes(Fn&& swap) {
    absl::MutexLock lock{&_delete_log_mutex};
    return swap(std::exchange(_delete_log, {}));
  }
  std::vector<int64_t> TakeDeleteLog();
  void CloseDeleteLog();

  irs::IndexWriter::CompactionFloorGuard ArmCompactionFloor() {
    SDB_ASSERT(_writer);
    return _writer->ArmCompactionFloor();
  }
  const irs::Format::ptr& Codec() const noexcept {
    SDB_ASSERT(_writer);
    return _writer->Codec();
  }
  bool ReplaceSegments(std::span<const std::string_view> replaced,
                       std::span<const std::string_view> adopted_metas,
                       const irs::Format::ptr& codec,
                       irs::IndexWriter::Transaction* removals = nullptr,
                       uint64_t removals_tick = irs::writer_limits::kMinTick) {
    SDB_ASSERT(_writer);
    return _writer->ReplaceSegments(replaced, adopted_metas, codec, removals,
                                    removals_tick);
  }

 private:
  struct IndexConfig {
    duckdb::idx_t oid;
    std::shared_ptr<const catalog::InvertedIndexConfig> config;
  };

  void OpenWriter();
  void RebuildConfig();

  duckdb::idx_t _table_id;
  duckdb::idx_t _db_id;
  duckdb::idx_t _schema_id;
  bool _is_new;
  uint64_t _segment_memory_max;
  uint32_t _row_group_size;
  std::atomic<bool> _dropped{false};
  mutable std::shared_mutex _table_lock;
  std::vector<IndexConfig> _configs;
  std::shared_ptr<const catalog::InvertedIndexConfig> _config;
  std::unique_ptr<irs::Directory> _dir;
  std::shared_ptr<irs::IndexWriter> _writer;
  std::optional<irs::ScorerOptions> _topk_options;
  std::unique_ptr<irs::Scorer> _topk_scorer;
  // Borrowed from the search engine (set in OpenWriter). Outlives this object.
  SearchDbWal* _wal = nullptr;
  uint64_t _last_committed_tick = 0;

  // Background maintenance state (mirrors InvertedIndexStorage). A zero
  // refresh/compaction interval disables the loops.
  TasksSettings _maint_settings;
  MaintenanceCounters _maintenance;
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
#ifdef SDB_DEV
  // Dev-only tripwire: asserts StartTasks runs at most once, so a bug can't
  // spawn competing maintenance loops.
  std::atomic<bool> _tasks_started{false};
#endif
};

}  // namespace sdb::search
