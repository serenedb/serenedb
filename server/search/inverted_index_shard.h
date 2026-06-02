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
#include <iresearch/search/scorer.hpp>
#include <memory>
#include <shared_mutex>

#include "catalog/inverted_index.h"
#include "rest_server/flush_feature.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/index_shard.h"
#include "storage_engine/search_engine.h"

namespace sdb::query {

class Transaction;

}  // namespace sdb::query
namespace sdb::search {

class InvertedIndexShard;

struct ThreadPoolState {
  std::atomic_size_t pending_commits{0};
  std::atomic_size_t non_empty_commits{0};
  std::atomic_size_t pending_compactions{0};
  std::atomic_size_t noop_compaction_count{0};
  std::atomic_size_t noop_commit_count{0};
};

struct TasksSettings {
  size_t cleanup_interval_step{};
  size_t refresh_interval_msec{};
  size_t compaction_interval_msec{};
  irs::CompactionPolicy compaction_policy;
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
class InvertedIndexShard final
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

  InvertedIndexShard(ObjectId id, const catalog::InvertedIndex& index,
                     bool is_new);

  static std::filesystem::path GetPath(ObjectId db_id, ObjectId schema_id,
                                       ObjectId table_id, ObjectId index_id,
                                       ObjectId shard_id);

  static std::shared_ptr<InvertedIndexShard> Create(
    ObjectId id, const catalog::InvertedIndex& index, bool is_new);

  void WriteInternal(vpack::Builder& builder) const final;

  struct TruncateGuard {
    struct UnlockDeleter {
      void operator()(absl::Mutex* m) const ABSL_NO_THREAD_SAFETY_ANALYSIS {
        m->Unlock();
      }
    };
    using Ptr = std::unique_ptr<absl::Mutex, UnlockDeleter>;
    Ptr mutex;
  };
  TruncateGuard TruncateBegin() ABSL_NO_THREAD_SAFETY_ANALYSIS {
    _commit_mutex.Lock();
    return {TruncateGuard::Ptr{&_commit_mutex}};
  }
  // `user_txn` (nullable) is the connection's transaction whose pending
  // per-conn iresearch staging we need to drop before Clear -- the new-arch
  // analog of the old SearchTrxState cookie cleanup. Pass nullptr from
  // contexts that don't have a user transaction (WAL recovery).
  void TruncateCommit(TruncateGuard&& guard, Tick tick,
                      query::Transaction* user_txn);

  auto GetTransaction() {
    SDB_ASSERT(_writer);
    return _writer->GetBatch();
  }

  ResultWithTime CompactUnsafe(const irs::CompactionPolicy& policy,
                               const irs::MergeWriter::FlushProgress& progress,
                               bool& empty_compaction);

  ResultWithTime CommitUnsafe(bool wait,
                              const irs::ProgressReportCallback& progress,
                              CommitResult& code);

  ResultWithTime CleanupUnsafe();
  Stats UpdateStatsUnsafe(InvertedIndexSnapshotPtr data) const;

  void ScheduleCompaction(absl::Duration delay);
  void ScheduleCommit(absl::Duration delay);

  yaclib::Future<> CommitWait();

  ObjectId GetId() const noexcept { return _id; }
  auto GetState() const noexcept { return _state; }

  bool HasActiveSegments() const noexcept {
    return _writer && _writer->HasActiveSegments();
  }

  void StatsToVPack(vpack::Builder& builder) const;
  Stats GetStats() const;

  auto& GetMutex() { return _mutex; }

  InvertedIndexSnapshotPtr GetInvertedIndexSnapshot() const {
    std::shared_lock guard{_snapshot_mutex};
    return _snapshot;
  }

  void StoreInvertedIndexSnapshot(
    InvertedIndexSnapshotPtr inverted_index_snapshot) {
    std::unique_lock guard{_snapshot_mutex};
    _snapshot = std::move(inverted_index_snapshot);
  }

  void ResetInvertedIndexSnapshot() {
    std::unique_lock guard{_snapshot_mutex};
    _snapshot.reset();
  }

  auto& GetTasksSettings() { return _tasks_settings; }

  void StartTasks() {
    ScheduleCommit({});
    ScheduleCompaction({});
  }

  void FinishCreation();

  void RecoveryCommit(Tick tick);

  Tick GetRecoveryTick() const noexcept { return _recovery_tick; }

  enum class Phase : uint8_t {
    Creating,
    Recovering,
    Active,
  };

  void StartRecovery() noexcept {
    std::lock_guard lock{_commit_mutex};
    SDB_ASSERT(_phase == Phase::Creating);
    _phase = Phase::Recovering;
  }

  // Persisted in the commit payload to survive iceberg compactions. 0 = not
  // pinned.
  void SetIcebergSnapshotId(int64_t id) noexcept { _iceberg_snapshot_id = id; }
  int64_t GetIcebergSnapshotId() const noexcept { return _iceberg_snapshot_id; }

 private:
  Result CompactUnsafeImpl(const irs::CompactionPolicy& policy,
                           const irs::MergeWriter::FlushProgress& progress,
                           bool& empty_compaction);
  Result CommitUnsafeImpl(bool wait,
                          const irs::ProgressReportCallback& progress,
                          CommitResult& code);
  Result CleanupUnsafeImpl();

  RocksDBEngineCatalog& _engine;
  SearchEngine& _search;
  std::shared_ptr<ThreadPoolState> _state;
  mutable std::shared_mutex _snapshot_mutex;
  InvertedIndexSnapshotPtr _snapshot;
  std::unique_ptr<irs::Directory> _dir;
  std::unique_ptr<irs::Scorer> _topk_scorer;
  std::shared_ptr<irs::IndexWriter> _writer;
  TasksSettings _tasks_settings;
  absl::Mutex _mutex;
  absl::Mutex _commit_mutex;

  std::shared_ptr<FlushSubscription> _flush_subscription;

  Tick _recovery_tick{0};
  Tick _last_committed_tick{0};
  int64_t _iceberg_snapshot_id{0};
  Phase _phase{Phase::Creating};

  irs::IResourceManager* _writers_memory{&irs::IResourceManager::gNoop};
  irs::IResourceManager* _readers_memory{&irs::IResourceManager::gNoop};
  irs::IResourceManager* _compactions_memory{&irs::IResourceManager::gNoop};
  irs::IResourceManager* _file_descriptors_count{&irs::IResourceManager::gNoop};

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
