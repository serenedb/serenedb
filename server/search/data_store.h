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

#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/data_store_meta.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {

class DataStore;

struct DataStoreOptions {
  irs::IndexWriterOptions writer_options;
  irs::IndexReaderOptions reader_options;
};

struct ThreadPoolState {
  std::atomic_size_t pending_commits{0};
  std::atomic_size_t non_empty_commits{0};
  std::atomic_size_t pending_consolidations{0};
  std::atomic_size_t noop_consolidation_count{0};
  std::atomic_size_t noop_commit_count{0};
};

enum class CommitResult {
  Undefined = 0,
  NoChanges,
  InProgress,
  Done,
};

struct DataSnapshot {
  DataSnapshot(irs::DirectoryReader&& index,
               std::shared_ptr<StorageSnapshot> rocksdb_snapshot)
    : reader{std::move(index)}, snapshot{std::move(rocksdb_snapshot)} {}

  [[nodiscard]] auto GetSequenceNumber() const noexcept {
    return snapshot->GetSnapshot()->GetSequenceNumber();
  }

  irs::DirectoryReader reader;
  std::shared_ptr<StorageSnapshot> snapshot;
};
using DataSnapshotPtr = std::shared_ptr<DataSnapshot>;

class Snapshot {
 public:
  Snapshot(std::shared_ptr<const DataStore> data_store,
           DataSnapshotPtr data_snapshot)
    : _data_store{std::move(data_store)}, _snapshot{std::move(data_snapshot)} {}
  Snapshot(Snapshot&&) = default;

  Snapshot& operator=(Snapshot&& other) {
    if (this != &other) {
      _data_store = std::move(other._data_store);
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
  std::shared_ptr<const DataStore> _data_store;
  DataSnapshotPtr _snapshot;
};

// Physical representation of a search index(catalog::Index)
// Used for creating writers/readers and managing index lifecycle
class DataStore : public std::enable_shared_from_this<DataStore> {
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

  class Transaction {
   public:
    Transaction(std::shared_ptr<DataStore> data_store)
      : _data_store(data_store),
        _transaction(data_store->_writer->GetBatch()) {}
    auto& GetIndexWriterTransaction() { return _transaction; }
    const auto& GetDataStore() const { return _data_store; }
    ResultWithTime Commit() &&;
    ResultWithTime Abort() &&;

   private:
    std::shared_ptr<DataStore> _data_store;
    irs::IndexWriter::Transaction _transaction;
  };

  DataStore(const catalog::Index& table, const DataStoreOptions& options);

  auto GetTransaction() { return Transaction{shared_from_this()}; }

  ResultOr<CommitResult> Commit(bool wait = false);
  ResultWithTime CommitUnsafe(bool wait,
                              const irs::ProgressReportCallback& progress,
                              CommitResult& res);

  ResultWithTime ConsolidateUnsafe(
    const DataStoreMeta::ConsolidationPolicy& policy,
    const irs::MergeWriter::FlushProgress& progress, bool& empty_consolidation);

  ResultWithTime CleanupUnsafe();
  Stats UpdateStatsUnsafe(DataSnapshotPtr data) const;

  void ScheduleCommit(absl::Duration delay);
  void ScheduleConsolidation(absl::Duration delay);

  ObjectId GetId() const noexcept { return _id; }

  void StatsToVPack(vpack::Builder& builder);
  Stats GetStats() const;
  Result Properties(const DataStoreMeta& meta);
  bool SetOutOfSync() noexcept;
  void MarkOutOfSyncUnsafe();
  bool IsOutOfSync() const noexcept;
  bool FailQueriesOnOutOfSync() const noexcept;

  auto& GetMutex() { return _mutex; }
  Snapshot GetSnapshot() const;
  DataSnapshotPtr GetDataSnapshot() const {
    return std::atomic_load_explicit(&_snapshot, std::memory_order_acquire);
  }
  void StoreDataSnapshot(DataSnapshotPtr data_snapshot) {
    std::atomic_store_explicit(&_snapshot, std::move(data_snapshot),
                               std::memory_order_release);
  }
  auto& GetMeta() { return _meta; }

 private:
  Result CommitUnsafeImpl(bool wait,
                          const irs::ProgressReportCallback& progress,
                          CommitResult& res);
  Result ConsolidateUnsafeImpl(const DataStoreMeta::ConsolidationPolicy& policy,
                               const irs::MergeWriter::FlushProgress& progress,
                               bool& empty_consolidation);
  Result CleanupUnsafeImpl();

  RocksDBEngineCatalog& _engine;
  SearchEngine& _search;
  ObjectId _id;
  std::shared_ptr<ThreadPoolState> _state;
  DataSnapshotPtr _snapshot;
  std::shared_ptr<irs::IndexWriter> _writer;
  DataStoreOptions _options;
  std::unique_ptr<irs::Directory> _dir;
  DataStoreMeta _meta;
  absl::Mutex _mutex;
  absl::Mutex _commit_mutex;

  uint64_t _last_committed_tick{0};
  bool _is_creation{true};

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
    // data store has no issues
    NoError = 0,
    // data store is out of sync
    OutOfSync = 1,
    // data store is failed (currently not used)
    Failed = 2,
  };
  std::atomic<Error> _error{Error::NoError};
};
}  // namespace sdb::search
