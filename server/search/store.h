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
#include <absl/container/btree_map.h>
#include <absl/functional/function_ref.h>
#include <absl/status/status.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/types/data_chunk.hpp>
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
#include "search/table.h"
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

enum class TableChange : uint8_t {
  Layout,
  Schema,
};

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
  bool Opened() const noexcept { return _dir != nullptr; }
  bool ReadOnly() const noexcept { return !_writer; }

  void Open();

  // A drop commits while readers may still hold this store; the destructor
  // removes the directory once the last of them lets go. Never set on
  // shutdown or detach, where the directory must survive.
  void MarkDropped() noexcept {
    if (_writer) {
      _dropped.store(true, std::memory_order_release);
    }
  }
  bool Dropped() const noexcept {
    return _dropped.load(std::memory_order_acquire);
  }

  StoreSnapshotPtr Snapshot() const;
  irs::DirectoryReader GetDirectoryReader() const { return Snapshot()->reader; }
  irs::DirectoryReader BuildSnapshot();

  irs::DirectoryReader View(Tick snapshot, const OwnWrites& own) const {
    return _table->View(snapshot, own);
  }
  Table& GetTable() noexcept { return *_table; }
  const Table& GetTable() const noexcept { return *_table; }
  irs::IndexWriter& Writer() noexcept { return *_writer; }
  std::vector<OwnSegment> OpenFlushed(
    std::span<const irs::IndexWriter::FlushedSegment> flushed) const;
  void ReleaseHot(const std::shared_ptr<HotContext>& hot);

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

  void CommitRemoval(std::shared_ptr<const irs::DocRemoval> removal);
  void CommitRemoval(std::vector<irs::KeyRemoval> removals);
  void CommitRemoveAll();

  void StartTasks();
  void ApplyOptions(const catalog::InvertedIndexSettings& options);
  void ApplyOptions(const catalog::SearchTableOptions& options) {
    SetTasksSettings(options);
  }

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

  void Commit(const irs::ProgressReportCallback& progress = nullptr);
  void CoolAll();
  [[nodiscard]] absl::Status VacuumCompact(duckdb::ClientContext& context,
                                           uint32_t target_segments);

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

  enum class Exclusive : uint8_t {
    None,
    Drop,
  };

  void ClaimLayout(duckdb::ClientContext& context, std::string_view name,
                   TableChange change);
  void ClaimExclusive(duckdb::ClientContext& context, std::string_view name,
                      Exclusive kind);
  [[nodiscard]] bool AcquireLayout(duckdb::transaction_t owner,
                                   std::string_view name, TableChange change);
  void ReleaseLayout(duckdb::transaction_t owner, bool committed) noexcept;
  Exclusive ExclusiveClaim() const;

  uint64_t SchemaEpoch() const noexcept { return _schema_epoch.load(); }
  void AddTableColumns(const duckdb::ColumnList& columns);
  void SetTableColumns(const duckdb::ColumnList& columns);
  void SetConstraintKeys(std::vector<catalog::ConstraintKey> keys);

  // The foreign keys referencing this table's unique keys, while a catalog
  // version declaring one is alive.
  void AddReferencing(std::weak_ptr<const Referencing> referencing);
  std::vector<std::shared_ptr<const Referencing>> ReferencingKeys() const;

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
  bool HasIndexes() const;

  [[nodiscard]] WriterGenerations::Writer RegisterWriter() noexcept {
    return _writers.Register();
  }
  [[nodiscard]] WriterGenerations::Truncate ClaimTruncate() noexcept {
    return _writers.ClaimTruncate();
  }
  bool SoleWriter() const noexcept { return _writers.SoleWriter(); }
  void DrainPriorWriters(duckdb::ClientContext& context);

  irs::IndexWriter::CompactionFloorGuard ArmCompactionFloor() {
    return _writer->ArmCompactionFloor();
  }
  void AwaitMerges(duckdb::ClientContext& context);
  Tick ReplaceSegments(std::span<const irs::SubReader* const> replaced,
                       std::vector<irs::IndexWriter::Replacement> replacements);

  void ReplayHotRows(duckdb::Catalog& catalog,
                     std::vector<connector::ColumnId> column_ids,
                     uint64_t segment, irs::doc_id_t first,
                     duckdb::unique_ptr<duckdb::DataChunk> chunk, Lsn lsn);
  void ReplayAddSegment(const ReplacedSegment& segment, Lsn lsn);
  void ReplayRemoval(const irs::DocRemoval& removal, Lsn lsn);
  void ReplayCooled(const ReplacedSegment& segment, Lsn lsn);
  void ReplayTruncate(Lsn lsn);
  void ReplayReplace(std::span<const uint64_t> replaced,
                     std::span<const ReplacedSegment> segments,
                     std::span<const irs::DocMap> maps, Lsn lsn);
  void ReplayState(std::span<const StateUnit> units, Lsn lsn);
  void InvertReplayed(duckdb::ClientContext& context);
  void FinishReplay();

  void Cool(bool all);
  void Cover(Lsn target);

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
  struct SchemaUndo {
    uint64_t epoch = 0;
    irs::containers::FlatHashSet<irs::field_id> table_columns;
    catalog::CompressionByColumn compression;
    std::vector<catalog::ConstraintKey> constraint_keys;
  };
  struct Layout {
    duckdb::transaction_t owner = kNoLayoutOwner;
    Exclusive exclusive = Exclusive::None;
    std::optional<SchemaUndo> schema_undo;
    std::vector<IndexConfig> configs;
    std::vector<IndexConfig> retired;
    std::optional<duckdb::idx_t> building_index;
    bool compaction_stale = false;
    std::shared_ptr<const catalog::InvertedIndexConfig> config;
    std::shared_ptr<const catalog::InvertedIndexConfig> scan_config;
    std::shared_ptr<const catalog::InvertedIndexConfig> compaction_config;
  };
  bool IsSearchTable() const noexcept { return _table_columns.has_value(); }
  irs::IndexReaderOptions ReaderOptions() const;
  void OpenWriter();
  void PublishSnapshot();
  void EnsureOpen();
  void ClaimColumnOids(const irs::DirectoryReader& reader) const;
  void SyncSegments() const;
  void CoolOne(const std::shared_ptr<HotContext>& hot);
  void CommitStorage(std::string entry,
                     absl::FunctionRef<void(Tick, Tick, Lsn)> apply);
  void ScheduleCooling();
  void CoolRetired(Lsn before);
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
    std::optional<duckdb::idx_t> skip) const
    ABSL_SHARED_LOCKS_REQUIRED(_layout_mutex);
  template<typename Configs>
  void RebuildConfig(Configs&& configs,
                     std::optional<duckdb::idx_t> building_index)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_layout_mutex);
  bool IsCurrentConfig(const irs::IndexFieldOptions* options) const;
  void FinishIndexBuild(duckdb::idx_t index_oid) noexcept;
  void AbortIndexBuild(duckdb::idx_t index_oid) noexcept;

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
  std::vector<catalog::ConstraintKey> _constraint_keys
    ABSL_GUARDED_BY(_layout_mutex);
  mutable absl::Mutex _referencing_mutex;
  mutable std::vector<std::weak_ptr<const Referencing>> _referencing
    ABSL_GUARDED_BY(_referencing_mutex);
  std::atomic<uint64_t> _schema_epoch{0};
  WriterGenerations _writers;
  std::unique_ptr<Table> _table;
  bool _on_disk = false;
  std::atomic<bool> _cooling_scheduled{false};
  std::atomic<uint32_t> _merges{0};
  struct ReplayedRows {
    irs::doc_id_t first = 0;
    duckdb::unique_ptr<duckdb::DataChunk> chunk;
    std::vector<connector::ColumnId> column_ids;
    duckdb::Catalog* catalog = nullptr;
  };
  absl::btree_map<uint64_t, std::vector<ReplayedRows>> _replayed_rows;

  std::atomic<uint32_t> _reindex_state{0};

#ifdef SDB_DEV
  // Dev-only tripwire: asserts StartTasks runs at most once, so a bug can't
  // spawn competing maintenance loops.
  std::atomic<bool> _tasks_started{false};
#endif
};

}  // namespace sdb::search
