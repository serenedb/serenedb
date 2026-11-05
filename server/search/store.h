////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <absl/base/thread_annotations.h>
#include <absl/status/status.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <duckdb/common/constants.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/column_list.hpp>
#include <filesystem>
#include <iresearch/formats/ann_build_env.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/scorers/scorer.hpp>
#include <iresearch/search/scorers/scorer_options.hpp>
#include <iresearch/store/directory.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/resource_manager.hpp>
#include <iresearch/utils/tracked.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "catalog/database_directory.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "catalog/persistence/inverted_index.h"
#include "connector/column_id.h"
#include "connector/file_manifest.h"
#include "search/index_durability.h"
#include "search/maintenance.h"
#include "search/store_stats.h"
#include "search/writer_generations.h"
#include "utils/drain_gate.h"

namespace duckdb {

class Catalog;
class ClientContext;
class DataChunk;

}  // namespace duckdb
namespace sdb::catalog {

using persistence::InvertedIndexSettings;

}  // namespace sdb::catalog
namespace sdb::search {

struct StoreSnapshot {
  StoreSnapshot(irs::DirectoryReader&& index,
                std::shared_ptr<const FileManifest> manifest)
    : reader{std::move(index)}, file_manifest{std::move(manifest)} {}

  irs::DirectoryReader reader;
  const std::shared_ptr<const FileManifest> file_manifest;
};
using StoreSnapshotPtr = std::shared_ptr<StoreSnapshot>;

struct StorageDirectory {
  std::unique_ptr<irs::Directory> directory;
  bool on_disk = false;
  bool absent = false;
};

StorageDirectory OpenStorageDirectory(
  const catalog::DatabaseDirectory& database, duckdb::idx_t oid, bool is_new,
  bool in_memory, const irs::ResourceManagementOptions& resources);

// The iresearch index the server owns for a search table, for an inverted
// index of a DuckDB table and for an inverted index of a view. Lives in the
// SearchEngine registry keyed by its oid, not in the catalog snapshot.
class Store final : public std::enable_shared_from_this<Store>,
                    public MaintainedStore {
 public:
  static constexpr duckdb::transaction_t kNoLayoutOwner =
    std::numeric_limits<duckdb::transaction_t>::max();

  Store(std::shared_ptr<catalog::DatabaseDirectory> directory, bool in_memory,
        bool read_only, duckdb::idx_t db_id, duckdb::idx_t id,
        const catalog::InvertedIndexSettings& options,
        const std::optional<irs::ScorerOptions>& top_k_scorer, bool is_new);
  Store(std::shared_ptr<catalog::DatabaseDirectory> directory, bool in_memory,
        bool read_only, duckdb::idx_t db_id, duckdb::idx_t id, bool is_new,
        const catalog::SearchTableOptions& options,
        const duckdb::ColumnList& columns);
  ~Store();

  Store(const Store&) = delete;
  Store& operator=(const Store&) = delete;

  duckdb::idx_t GetId() const noexcept { return _id; }
  // The database whose attachment holds this store's catalog entry.
  duckdb::idx_t GetDatabaseId() const noexcept { return _db_id; }
  std::filesystem::path Path() const { return _directory->StoragePath(_id); }
  bool Absent() const noexcept { return _absent; }
  bool ReadOnly() const noexcept { return !_writer; }

  void Open();
  void OpenReader();

  // A drop commits while readers may still hold this store; the destructor
  // removes the directory once the last of them lets go. Never set on
  // shutdown or detach, where the directory must survive.
  void MarkDropped() noexcept {
    if (_writer) {
      _dropped.store(true, std::memory_order_release);
    }
  }

  StoreSnapshotPtr Snapshot() const { return std::atomic_load(&_snapshot); }
  irs::DirectoryReader GetDirectoryReader() const { return Snapshot()->reader; }
  std::pair<irs::DirectoryReader, irs::SourcePosition> SnapshotWithPosition() {
    return _durability.Snapshot(*_writer);
  }

  std::shared_ptr<const FileManifest> GetFileManifest() const {
    return std::atomic_load(&_file_manifest);
  }
  void SetFileManifest(std::shared_ptr<const FileManifest> manifest) {
    std::atomic_store(&_file_manifest, std::move(manifest));
  }

  // `exclusive_segment` is required of a writer that will record its flushed
  // segments in the WAL -- see irs::IndexWriter::GetBatch.
  irs::IndexWriter::Transaction GetTransaction(bool exclusive_segment = false) {
    SDB_ASSERT(_writer);
    return _writer->GetBatch(exclusive_segment);
  }

  void Clear(const irs::SourcePosition& position);
  void CommitRemoval(std::shared_ptr<const irs::DocRemoval> removal);
  void CommitRemoval(std::vector<irs::KeyRemoval> removals);
  void CommitRemoveAll();

  template<typename Visit>
  void VisitDocMaps(std::span<const irs::DocRemoval* const> removals,
                    const Visit& visit) const {
    _writer->VisitDocMaps(removals, visit);
  }

  void MarkStaged(const irs::SourcePosition& position) {
    _durability.MarkStaged(position);
  }
  std::optional<uint64_t> NeededWAL() const { return _durability.NeededWAL(); }
  const irs::SourcePosition& PersistedPosition() const noexcept {
    return _durability.Position();
  }
  void MarkOutOfSync() noexcept { _durability.MarkOutOfSync(); }
  bool IsOutOfSync() const noexcept { return _durability.IsOutOfSync(); }

  void PrepareCheckpoint(uint64_t iteration);
  void FinishCheckpoint();
  void FinishCheckpointIfIdle();
  bool HasPendingCheckpoint() { return _durability.HasPendingCheckpoint(); }

  // Launch this store's refresh + compaction loops (via SearchEngine). Call
  // once, after the store is open and recovery (if any) finalized.
  void StartTasks();
  void ApplyOptions(const catalog::InvertedIndexSettings& options);
  void ApplyOptions(const catalog::SearchTableOptions& options) {
    SetTasksSettings(options);
  }

  // The maintenance ops the loops invoke: RefreshUnsafe publishes pending
  // inserts, CompactUnsafe merges segments, CleanupUnsafe reclaims unreferenced
  // files.
  ResultWithTime RefreshUnsafe(bool wait,
                               const irs::ProgressReportCallback& progress,
                               RefreshResult& code);
  // `field_options` (nullable) is the per-merge per-column encoding config,
  // pinned by the caller for the whole merge.
  auto CompactUnsafeAsync(utils::DrainGate::Pass maintenance,
                          const irs::CompactionPolicy& policy,
                          const irs::MergeWriter::FlushProgress& progress,
                          bool& empty_compaction,
                          const irs::IndexFieldOptions* field_options,
                          const irs::AnnBuildEnv* env)
    -> yaclib::Future<ResultWithTime>;
  ResultWithTime CleanupUnsafe();

  // The field options a background merge encodes against, pinned for it; null
  // once the store's definition is gone.
  std::shared_ptr<const irs::IndexFieldOptions> CompactionOptions();

  [[nodiscard]] absl::Status Refresh(
    const irs::ProgressReportCallback& progress = nullptr);
  // Synchronous maintenance for explicit VACUUM (REFRESH_* / COMPACT_*).
  [[nodiscard]] absl::Status VacuumRefresh();
  [[nodiscard]] absl::Status VacuumCompact(duckdb::ClientContext& context,
                                           uint32_t target_segments);
  void Publish();

  StoreStats GetStats() const;

  const std::optional<irs::ScorerOptions>& TopKScorer() const noexcept {
    return _topk_options;
  }

  // A search table's merged index layout: the configs of its indexes, with a
  // build in flight hidden from scans until it finishes.
  std::shared_ptr<const catalog::InvertedIndexConfig> Config() const;
  std::shared_ptr<const catalog::InvertedIndexConfig> ScanConfig() const;
  void MergeIndexConfig(
    duckdb::idx_t index_oid,
    std::shared_ptr<const catalog::InvertedIndexConfig> config);
  void RemoveIndexConfig(duckdb::idx_t index_oid);
  void ReleaseRetiredConfig(duckdb::idx_t index_oid) noexcept;

  void ClaimLayout(duckdb::ClientContext& context, std::string_view name);
  [[nodiscard]] bool AcquireLayout(duckdb::transaction_t owner,
                                   std::string_view name);
  void ReleaseLayout(duckdb::transaction_t owner) noexcept;

  class [[nodiscard]] BuildClaim {
   public:
    BuildClaim() noexcept = default;
    BuildClaim(BuildClaim&& other) noexcept
      : _store{std::exchange(other._store, nullptr)},
        _index_oid{other._index_oid} {}
    BuildClaim& operator=(BuildClaim&& other) noexcept {
      BuildClaim{std::move(other)}.Swap(*this);
      return *this;
    }
    ~BuildClaim() { Release(/*finished=*/false); }

    void Finish() noexcept;

   private:
    friend class Store;

    BuildClaim(Store& store, duckdb::idx_t index_oid) noexcept
      : _store{&store}, _index_oid{index_oid} {}

    void Swap(BuildClaim& other) noexcept {
      std::swap(_store, other._store);
      std::swap(_index_oid, other._index_oid);
    }
    void Release(bool finished) noexcept;

    Store* _store = nullptr;
    duckdb::idx_t _index_oid = 0;
  };

  BuildClaim BeginIndexBuild(
    duckdb::idx_t index_oid,
    std::shared_ptr<const catalog::InvertedIndexConfig> config);
  bool BuildInFlight() const;

  [[nodiscard]] WriterGenerations::Writer RegisterWriter() noexcept {
    return _writers.Register();
  }
  [[nodiscard]] WriterGenerations::Truncate ClaimTruncate() noexcept {
    return _writers.ClaimTruncate();
  }
  void DrainPriorWriters(duckdb::ClientContext& context);

  void WatchTruncates();
  void RecordTruncateForBuild(const irs::SourcePosition& position);
  bool TruncatedAfter(const irs::SourcePosition& position) {
    absl::MutexLock lock{&_truncate_mutex};
    return _build_truncate > position;
  }

  irs::IndexWriter::CompactionFloorGuard ArmCompactionFloor() {
    return _writer->ArmCompactionFloor();
  }
  void ReplaceSegments(std::span<const irs::SubReader* const> replaced,
                       std::vector<irs::IndexWriter::Replacement> replacements);

  void ReplayInsert(duckdb::ClientContext& context, duckdb::Catalog& catalog,
                    std::span<const connector::ColumnId> column_ids,
                    duckdb::DataChunk& chunk);
  void ReplayDeleteRowPositions(
    duckdb::AttachedDatabase& db, std::shared_ptr<const duckdb::data_t> entry,
    std::span<const duckdb::WALFrozenRowPositionGroup> groups);
  void ReplayRowPositionMaps(
    duckdb::AttachedDatabase& db, std::shared_ptr<const duckdb::data_t> entry,
    uint64_t source, std::span<const duckdb::WALFrozenRowPositionMap> maps);
  void ReplayTruncate(duckdb::AttachedDatabase& db);
  void ReplayAdoptSegments(duckdb::AttachedDatabase& db,
                           std::vector<std::string> segments);
  bool FinishReplay();
  bool Replays(duckdb::AttachedDatabase& db) const;

  // Delete-log for online CREATE INDEX: open while the build runs, drained
  // once by TakeDeleteLog after the backfill is committed, then latched
  // forever. AppendDeleteLog hands back the rows it does not log, all of them
  // once latched; the caller removes those natively (a delete racing publish
  // is an ordinary post-publish remove).
  irs::TrackedVector<int64_t> AppendDeleteLog(irs::TrackedVector<int64_t> rows);
  std::vector<irs::KeyRemoval> TakeDeleteLog();
  void SetDeleteLogRowidEnd(int64_t end) noexcept {
    _delete_log_rowid_end.store(end, std::memory_order_relaxed);
  }

  // One REINDEX at a time per index, across all connections: claim the
  // store for the whole refresh (observe -> delta/rebuild -> publish).
  struct ReindexReleaser {
    void operator()(Store* store) const noexcept;
  };
  using ReindexClaim = std::unique_ptr<Store, ReindexReleaser>;

  [[nodiscard]] ReindexClaim TryClaimReindex() noexcept;
  [[nodiscard]] ReindexClaim ClaimReindex(duckdb::ClientContext& context);

 private:
  static constexpr uint32_t kReindexing = 1;
  static constexpr uint32_t kReindexWaiter = 2;

  struct IndexConfig {
    duckdb::idx_t oid;
    std::shared_ptr<const catalog::InvertedIndexConfig> config;
  };
  struct Layout {
    duckdb::transaction_t owner = kNoLayoutOwner;
    std::vector<IndexConfig> configs;
    std::vector<IndexConfig> retired;
    std::optional<duckdb::idx_t> building_index;
    bool compaction_stale = false;
    std::shared_ptr<const catalog::InvertedIndexConfig> config;
    std::shared_ptr<const catalog::InvertedIndexConfig> scan_config;
    std::shared_ptr<const catalog::InvertedIndexConfig> compaction_config;
  };
  struct ReplaySession;

  bool IsSearchTable() const noexcept { return _table_columns.has_value(); }
  irs::IndexReaderOptions ReaderOptions() const;
  void OpenWriter();
  void PublishSnapshot();
  template<typename Remove>
  void CommitRemovalWith(const Remove& remove);
  auto CompactUnsafeImpl(const irs::CompactionPolicy& policy,
                         const irs::MergeWriter::FlushProgress& progress,
                         bool& empty_compaction,
                         const irs::IndexFieldOptions* field_options,
                         const irs::AnnBuildEnv* env)
    -> yaclib::Future<absl::Status>;
  std::shared_ptr<const catalog::InvertedIndexConfig> MergeConfigs(
    std::span<const IndexConfig> configs, std::span<const IndexConfig> retired,
    std::optional<duckdb::idx_t> skip) const;
  template<typename Configs>
  void RebuildConfig(Configs&& configs,
                     std::optional<duckdb::idx_t> building_index)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_layout_mutex);
  bool IsCurrentConfig(const irs::IndexFieldOptions* options) const;
  void FinishIndexBuild(duckdb::idx_t index_oid) noexcept;
  void AbortIndexBuild(duckdb::idx_t index_oid) noexcept;
  ReplaySession* BeginReplay(duckdb::AttachedDatabase& db);

  duckdb::idx_t _id;
  duckdb::idx_t _db_id;
  std::shared_ptr<catalog::DatabaseDirectory> _directory;
  bool _in_memory;
  bool _read_only;
  bool _is_new;
  bool _absent = false;
  std::atomic<bool> _dropped{false};
  uint64_t _segment_memory_max;
  uint32_t _segment_docs_max = 0;
  std::optional<irs::ScorerOptions> _topk_options;
  std::unique_ptr<irs::Scorer> _topk_scorer;
  IndexDurability _durability;
  std::unique_ptr<irs::Directory> _dir;
  std::shared_ptr<irs::IndexWriter> _writer;
  // Accessed via std::atomic_load/std::atomic_store (libc++ lacks
  // std::atomic<std::shared_ptr>).
  StoreSnapshotPtr _snapshot;
  std::shared_ptr<const FileManifest> _file_manifest;

  // A search table's columns and layout; empty for an inverted index.
  std::optional<irs::containers::FlatHashSet<irs::field_id>> _table_columns;
  uint32_t _row_group_size = 0;
  catalog::CompressionByColumn _compression;
  mutable absl::Mutex _layout_mutex;
  Layout _layout ABSL_GUARDED_BY(_layout_mutex);
  WriterGenerations _writers;
  absl::Mutex _truncate_mutex;
  irs::SourcePosition _build_truncate ABSL_GUARDED_BY(_truncate_mutex);
  std::unique_ptr<ReplaySession> _replay;

  std::atomic<uint32_t> _reindex_state{0};

  absl::Mutex _delete_log_mutex;
  std::atomic<bool> _delete_log_open{false};
  std::atomic<int64_t> _delete_log_rowid_end{
    std::numeric_limits<int64_t>::max()};
  std::vector<irs::KeyRemoval> _delete_log ABSL_GUARDED_BY(_delete_log_mutex);

#ifdef SDB_DEV
  // Dev-only tripwire: asserts StartTasks runs at most once, so a bug can't
  // spawn competing maintenance loops.
  std::atomic<bool> _tasks_started{false};
#endif
};

void ThrowIfRefreshFailed(const absl::Status& status,
                          std::string_view index_name);

}  // namespace sdb::search
