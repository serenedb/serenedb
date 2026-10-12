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

#include <absl/base/thread_annotations.h>
#include <absl/functional/function_ref.h>
#include <absl/status/status.h>
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>

#include <atomic>
#include <filesystem>
#include <iresearch/formats/ann_build_env.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/scorers/scorer.hpp>
#include <iresearch/store/directory.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/resource_manager.hpp>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "catalog/database_directory.h"
#include "catalog/persistence/inverted_index.h"
#include "connector/file_manifest.h"
#include "search/maintenance.h"
#include "search/store_stats.h"
#include "storage_engine/search_engine.h"

namespace sdb::query {

class Transaction;

}  // namespace sdb::query
namespace sdb::catalog {

using persistence::InvertedIndexSettings;

}  // namespace sdb::catalog
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

struct StorageDirectory {
  std::unique_ptr<irs::Directory> directory;
  bool on_disk = false;
  bool absent = false;
};

StorageDirectory OpenStorageDirectory(
  const catalog::DatabaseDirectory& database, duckdb::idx_t oid, bool is_new,
  bool in_memory, const irs::ResourceManagementOptions& resources);

// Physical representation of a search index (InvertedIndex). Owns the
// iresearch writer/reader and all mutable index state; lives in the
// SearchEngine registry keyed by index_id, not in the catalog snapshot.
class InvertedIndexStorage final
  : public std::enable_shared_from_this<InvertedIndexStorage> {
 public:
  InvertedIndexStorage(std::shared_ptr<catalog::DatabaseDirectory> directory,
                       bool in_memory, duckdb::idx_t db_id,
                       duckdb::idx_t index_id,
                       const catalog::InvertedIndexSettings& options,
                       const std::optional<irs::ScorerOptions>& top_k_scorer,
                       bool is_new);
  ~InvertedIndexStorage();

  // A drop commits while readers may still hold this storage; the destructor
  // removes the directory once the last of them lets go. Never set on
  // shutdown or detach, where the directory must survive.
  void MarkDropped() noexcept {
    _dropped.store(true, std::memory_order_release);
  }
  std::filesystem::path Path() const {
    return _directory->StoragePath(_index_id);
  }
  bool Absent() const noexcept { return _absent; }

  // `db_id` is passed in rather than derived from the catalog: an index
  // created inside a transaction lives in that transaction's overlay, and so
  // may the schema its database has to be walked through.
  static std::shared_ptr<InvertedIndexStorage> Create(
    std::shared_ptr<catalog::DatabaseDirectory> directory, bool in_memory,
    duckdb::idx_t db_id, duckdb::idx_t index_id,
    const catalog::InvertedIndexSettings& options,
    const std::optional<irs::ScorerOptions>& top_k_scorer, bool is_new) {
    return std::make_shared<InvertedIndexStorage>(
      std::move(directory), in_memory, db_id, index_id, options, top_k_scorer,
      is_new);
  }

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

  ResultWithTime RefreshUnsafe(bool wait,
                               const irs::ProgressReportCallback& progress,
                               RefreshResult& code);

  ResultWithTime CleanupUnsafe();
  StoreStats UpdateStatsUnsafe(InvertedIndexSnapshotPtr data) const;

  void Refresh(const irs::ProgressReportCallback& progress = nullptr);

  void PrepareCheckpoint(uint64_t iteration);
  void FinishCheckpoint();

  uint64_t NextTick(uint64_t queries) noexcept {
    return _writer->NextTick(queries);
  }

  duckdb::idx_t GetId() const noexcept { return _index_id; }
  // The database whose attachment holds this index's catalog entry.
  duckdb::idx_t GetDatabaseId() const noexcept { return _db_id; }

  StoreStats GetStats() const {
    return UpdateStatsUnsafe(GetInvertedIndexSnapshot());
  }

  InvertedIndexSnapshotPtr GetInvertedIndexSnapshot() const {
    return std::atomic_load(&_snapshot);
  }

  // One REINDEX at a time per index, across all connections: claim the
  // storage for the whole refresh (observe -> delta/rebuild -> publish).
  class [[nodiscard]] ReindexClaim {
   public:
    static ReindexClaim TryAcquire(InvertedIndexStorage& storage);
    static ReindexClaim Acquire(InvertedIndexStorage& storage,
                                absl::FunctionRef<bool()> cancelled,
                                absl::Duration poll);
    ReindexClaim(ReindexClaim&& other) noexcept
      : _storage{std::exchange(other._storage, nullptr)} {}
    ReindexClaim& operator=(ReindexClaim&&) = delete;
    ~ReindexClaim();
    bool Claimed() const noexcept { return _storage != nullptr; }

   private:
    explicit ReindexClaim(InvertedIndexStorage* storage) noexcept
      : _storage{storage} {}

    InvertedIndexStorage* _storage;
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

  void StartTasks() { _search.StartTasks(shared_from_this()); }

  void ApplyOptions(const catalog::InvertedIndexSettings& options);

  const irs::SourcePosition& PersistedPosition() const noexcept {
    return _position;
  }

  void MarkOutOfSync() noexcept {
    _out_of_sync.store(true, std::memory_order_relaxed);
  }
  bool IsOutOfSync() const noexcept {
    return _out_of_sync.load(std::memory_order_relaxed);
  }

 private:
  auto CompactUnsafeImpl(const irs::CompactionPolicy& policy,
                         const irs::MergeWriter::FlushProgress& progress,
                         bool& empty_compaction,
                         const irs::IndexFieldOptions* field_options,
                         const irs::AnnBuildEnv* env)
    -> yaclib::Future<absl::Status>;
  absl::Status RefreshUnsafeImpl(bool wait,
                                 const irs::ProgressReportCallback& progress,
                                 RefreshResult& code);
  bool CanPersist() const;
  uint64_t RunningCheckpoint(uint64_t generation) const;
  void WaitForCheckpoint() const;
  void SyncDirectory();
  void PublishSnapshot();
  absl::Status CleanupUnsafeImpl();

  duckdb::idx_t _index_id;
  duckdb::idx_t _db_id;
  std::shared_ptr<catalog::DatabaseDirectory> _directory;
  bool _absent = false;
  bool _on_disk = false;
  std::atomic<bool> _directory_dirty{true};
  std::atomic<bool> _dropped{false};
  SearchEngine& _search;
  // Accessed via std::atomic_load/std::atomic_store (libc++ lacks
  // std::atomic<std::shared_ptr>).
  InvertedIndexSnapshotPtr _snapshot;
  std::shared_ptr<const FileManifest> _file_manifest;
  std::unique_ptr<irs::Directory> _dir;
  std::unique_ptr<irs::Scorer> _topk_scorer;
  std::shared_ptr<irs::IndexWriter> _writer;
  absl::Mutex _reindex_mutex;
  absl::CondVar _reindex_cv;
  bool _reindex_in_flight ABSL_GUARDED_BY(_reindex_mutex) = false;
  uint32_t _reindex_waiters ABSL_GUARDED_BY(_reindex_mutex) = 0;
  TasksSettings _tasks_settings;
  absl::Mutex _refresh_mutex;

  irs::SourcePosition _position;
  uint64_t _checkpoint = 0;
  uint64_t _pending_checkpoint = 0;
  std::atomic<bool> _out_of_sync{false};
  duckdb::mutex _delete_log_mutex;
  std::atomic<bool> _delete_log_open{false};
  std::atomic<int64_t> _delete_log_rowid_end{
    std::numeric_limits<int64_t>::max()};
  std::atomic<int64_t> _delete_log_rowid_begin{0};
  std::vector<std::vector<int64_t>> _delete_log;
  std::atomic<uint64_t> _compaction_gen{0};
  std::atomic<uint32_t> _stale_pressure{0};
  MaintenanceCounters _maintenance;

  irs::IResourceManager* _writers_memory{&irs::IResourceManager::gNoop};
  irs::IResourceManager* _readers_memory{&irs::IResourceManager::gNoop};
  irs::IResourceManager* _compactions_memory{&irs::IResourceManager::gNoop};
  irs::IResourceManager* _file_descriptors_count{&irs::IResourceManager::gNoop};
};

}  // namespace sdb::search
