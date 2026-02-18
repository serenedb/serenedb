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
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>
#include <rocksdb/types.h>

#include <atomic>
#include <filesystem>
#include <iresearch/index/index_writer.hpp>
#include <memory>

#include "catalog/inverted_index.h"
#include "rest_server/flush_feature.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/index_shard.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {

class InvertedIndexShard;

struct InvertedIndexShardOptions {
  ObjectId id;
  size_t commit_interval_ms = 1000;
  size_t consolidation_interval_ms = 1000;
};

struct ThreadPoolState {
  std::atomic_size_t pending_commits{0};
  std::atomic_size_t non_empty_commits{0};
  std::atomic_size_t pending_consolidations{0};
  std::atomic_size_t noop_consolidation_count{0};
  std::atomic_size_t noop_commit_count{0};
};

struct TasksSettings {
  size_t cleanup_interval_step{};
  size_t commit_interval_msec{};
  size_t consolidation_interval_msec{};
  irs::ConsolidationPolicy consolidation_policy;
  uint32_t version{};
  size_t writebuffer_active{};
  size_t writebuffer_idle{};
  size_t writebuffer_size_max{};
};

enum class CommitResult {
  Undefined = 0,
  NoChanges,
  InProgress,
  Done,
};

struct InvertedIndexSnapshot {
  InvertedIndexSnapshot(irs::DirectoryReader&& index,
                        std::shared_ptr<StorageSnapshot> rocksdb_snapshot)
    : reader{std::move(index)}, snapshot{std::move(rocksdb_snapshot)} {}

  [[nodiscard]] auto GetSequenceNumber() const noexcept {
    return snapshot->GetSnapshot()->GetSequenceNumber();
  }

  irs::DirectoryReader reader;
  std::shared_ptr<StorageSnapshot> snapshot;
};
using InvertedIndexSnapshotPtr = std::shared_ptr<InvertedIndexSnapshot>;

class Snapshot {
 public:
  Snapshot(std::shared_ptr<const InvertedIndexShard> inverted_index_shard,
           InvertedIndexSnapshotPtr inverted_index_snapshot)
    : _inverted_index_shard{std::move(inverted_index_shard)},
      _snapshot{std::move(inverted_index_snapshot)} {}
  Snapshot(Snapshot&&) = default;

  Snapshot& operator=(Snapshot&& other) {
    if (this != &other) {
      _inverted_index_shard = std::move(other._inverted_index_shard);
      _snapshot = std::move(other._snapshot);
    }
    return *this;
  }

  ~Snapshot() = default;

  Snapshot(const Snapshot&) = delete;
  Snapshot& operator=(const Snapshot&) = delete;

  [[nodiscard]] const auto& GetDirectoryReader() const noexcept {
    return _snapshot->reader;
  }

  [[nodiscard]] auto GetSequenceNumber() {
    return _snapshot->GetSequenceNumber();
  }

 private:
  std::shared_ptr<const InvertedIndexShard> _inverted_index_shard;
  InvertedIndexSnapshotPtr _snapshot;
};

// Physical representation of a search index(catalog::Index)
// Used for creating writers/readers and managing index lifecycle
class InvertedIndexShard
  : public std::enable_shared_from_this<InvertedIndexShard>,
    public IndexShard {
 public:
  struct Stats {
    // NOLINTBEGIN
    uint64_t numDocs = 0;
    uint64_t numLiveDocs = 0;
    uint64_t numPrimaryDocs = 0;
    uint64_t numSegments = 0;
    uint64_t numFiles = 0;
    uint64_t indexSize = 0;
    // NOLINTEND
  };

  struct ResultWithTime {
    Result res;
    uint64_t time_ms;
  };

  InvertedIndexShard(const catalog::InvertedIndex& index,
                     InvertedIndexShardOptions options, bool is_new);

  static std::filesystem::path GetPath(ObjectId db, ObjectId schema,
                                       ObjectId id);

  void WriteInternal(vpack::Builder& builder) const final;

  auto GetTransaction() { return _writer->GetBatch(); }

  ResultWithTime ConsolidateUnsafe(
    const irs::ConsolidationPolicy& policy,
    const irs::MergeWriter::FlushProgress& progress, bool& empty_consolidation);

  ResultWithTime CommitUnsafe(bool wait,
                              const irs::ProgressReportCallback& progress,
                              CommitResult& code);

  ResultWithTime CleanupUnsafe();
  Stats UpdateStatsUnsafe(InvertedIndexSnapshotPtr data) const;

  void ScheduleConsolidation(absl::Duration delay);
  void ScheduleCommit(absl::Duration delay);

  yaclib::Future<> CommitWait();

  ObjectId GetId() const noexcept { return _id; }
  ObjectId GetRelationId() const noexcept { return _relation_id; }
  auto GetState() const noexcept { return _state; }

  void StatsToVPack(vpack::Builder& builder);
  Stats GetStats() const;
  bool SetOutOfSync() noexcept;
  void MarkOutOfSyncUnsafe();
  bool IsOutOfSync() const noexcept;
  bool FailQueriesOnOutOfSync() const noexcept;

  auto& GetMutex() { return _mutex; }
  Snapshot GetSnapshot() const;

  InvertedIndexSnapshotPtr GetInvertedIndexSnapshot() const {
    return std::atomic_load_explicit(&_snapshot, std::memory_order_acquire);
  }

  void StoreInvertedIndexSnapshot(
    InvertedIndexSnapshotPtr inverted_index_snapshot) {
    std::atomic_store_explicit(&_snapshot, std::move(inverted_index_snapshot),
                               std::memory_order_release);
  }

  void ResetInvertedIndexSnapshot() { _snapshot.reset(); }

  auto& GetTasksSettings() { return _tasks_settings; }

  void StartTasks() {
    ScheduleCommit({});
    ScheduleConsolidation({});
  }

  void FinishCreation();

  void RecoveryCommit(Tick tick);

  Tick GetRecoveryTick() const noexcept { return _recovery_tick; }

  void MarkDeleted() { _is_deleted.store(true, std::memory_order_release); }

  bool IsDeleted() const noexcept {
    return _is_deleted.load(std::memory_order_acquire);
  }

 private:
  Result ConsolidateUnsafeImpl(const irs::ConsolidationPolicy& policy,
                               const irs::MergeWriter::FlushProgress& progress,
                               bool& empty_consolidation);
  Result CommitUnsafeImpl(bool wait,
                          const irs::ProgressReportCallback& progress,
                          CommitResult& code);
  Result CleanupUnsafeImpl();

  RocksDBEngineCatalog& _engine;
  SearchEngine& _search;
  std::shared_ptr<ThreadPoolState> _state;
  InvertedIndexSnapshotPtr _snapshot;
  std::unique_ptr<irs::Directory> _dir;
  InvertedIndexShardOptions _options;
  std::shared_ptr<irs::IndexWriter> _writer;
  TasksSettings _tasks_settings;
  absl::Mutex _mutex;
  absl::Mutex _commit_mutex;

  std::shared_ptr<FlushSubscription> _flush_subscription;

  Tick _recovery_tick{0};
  Tick _last_committed_tick{0};
  bool _is_creation{true};

  std::atomic_bool _is_deleted{false};

  irs::IResourceManager* _writers_memory{&irs::IResourceManager::gNoop};
  irs::IResourceManager* _readers_memory{&irs::IResourceManager::gNoop};
  irs::IResourceManager* _consolidations_memory{&irs::IResourceManager::gNoop};
  irs::IResourceManager* _file_descriptors_count{&irs::IResourceManager::gNoop};

  // Stats
  metrics::Gauge<uint64_t>* _mapped_memory{nullptr};
  metrics::Gauge<uint64_t>* _num_failed_commits{nullptr};
  metrics::Gauge<uint64_t>* _num_failed_cleanups{nullptr};
  metrics::Gauge<uint64_t>* _num_failed_consolidations{nullptr};

  std::atomic_uint64_t _commit_time_num{0};
  metrics::Gauge<uint64_t>* _avg_commit_time_ms{nullptr};

  std::atomic_uint64_t _cleanup_time_num{0};
  metrics::Gauge<uint64_t>* _avg_cleanup_time_ms{nullptr};

  std::atomic_uint64_t _consolidation_time_num{0};
  metrics::Gauge<uint64_t>* _avg_consolidation_time_ms{nullptr};
  metrics::Guard<Stats>* _metric_stats{nullptr};

  enum class Error : uint8_t {
    // inverted index shard has no issues
    NoError = 0,
    // inverted index shard is out of sync
    OutOfSync = 1,
    // inverted index shard is failed (currently not used)
    Failed = 2,
  };
  std::atomic<Error> _error{Error::NoError};
};
}  // namespace sdb::search
