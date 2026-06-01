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

#include <rocksdb/snapshot.h>
#include <rocksdb/utilities/transaction.h>

#include <iresearch/index/index_writer.hpp>
#include <yaclib/async/future.hpp>

#include "basics/containers/flat_hash_map.h"
#include "basics/down_cast.h"
#include "basics/result.h"
#include "catalog/catalog.h"
#include "query/config.h"
#include "query/local_table_changes.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/inverted_index_shard.h"

namespace sdb::query {

class Transaction : public Config {
 public:
  using Config::Config;

#ifdef SDB_GTEST
  // Test-only: wrap a pre-built rocksdb::Transaction so unit tests can use
  // query::Transaction without spinning up the storage engine.
  Transaction(duckdb::ClientContext& client_ctx,
              std::unique_ptr<rocksdb::Transaction> rocksdb_txn) noexcept
    : Config{client_ctx} {
    _rocksdb_transaction = std::move(rocksdb_txn);
  }
#endif

#ifdef SDB_DEV
  virtual ~Transaction() {
    // Search transactions have implicit commit in destructor (historical
    // reasons) So if we get here explicit Commit/Rollback should be already
    // called. Otherwise we might have some unexpected data
    SDB_ASSERT(_search_transactions.empty());
    SDB_ASSERT(_parallel_search_transactions.empty());
    // RocksDB transactions aborts itself in destructor but just for consistency
    // we should do Commit/Rollback explicitly
    SDB_ASSERT(!_rocksdb_transaction);
  }
#endif

  void OnNewStatement();

  // Pre-commit work that needs an active transaction (revert SET LOCAL for
  // custom-impl settings). Runs before the rocksdb commit.
  void PreCommit() noexcept;
  // Pre-rollback counterpart -- restores all SET values.
  void PreRollback() noexcept;

  Result Commit();

  Result Rollback();

  void UpdateNumRows(ObjectId table_id, int64_t delta) noexcept {
    _table_rows_deltas[table_id] += delta;
  }

  bool HasRocksDBSnapshot() const noexcept {
    return _rocksdb_snapshot != nullptr;
  }
  bool HasRocksDBTransaction() const noexcept {
    return _rocksdb_transaction != nullptr;
  }

  rocksdb::Transaction& GetRocksDBTransaction() const noexcept {
    SDB_ASSERT(_rocksdb_transaction);
    return *_rocksdb_transaction;
  }

  // Counts PutLogData blobs (marker-only writes) so the commit gate sees
  // them as work -- rocksdb's own counters ignore PutLogData.
  void RegisterLogDataMarker() noexcept { ++_num_log_data_markers; }
  uint64_t GetNumLogDataMarkers() const noexcept {
    return _num_log_data_markers;
  }

  const rocksdb::Snapshot& GetRocksDBSnapshot() const noexcept {
    SDB_ASSERT(_rocksdb_snapshot);
    return *_rocksdb_snapshot;
  }

  void EnsureRocksDBTransaction();
  void EnsureRocksDBSnapshot();

  search::InvertedIndexSnapshotPtr EnsureSearchSnapshot(ObjectId index_id);

  void EraseSearchTransaction(ObjectId shard_id) noexcept {
    _search_transactions.erase(shard_id);
  }

  // Single helper for ensuring an iresearch trx exists for any shard type
  // (InvertedIndexShard or SearchTableShard). ObjectIds are catalog-wide
  // unique via NextId(), so one trx-per-shard_id map serves both without
  // collision. `make_trx` is invoked only on first call for a given
  // shard_id; for either shard type pass [&]{ return shard.GetTransaction(); }.
  // Lifetime of the returned trx is owned by the sdb txn (Commit/Rollback
  // drains the map).
  template<typename Factory>
  irs::IndexWriter::Transaction& EnsureSearchTransaction(ObjectId shard_id,
                                                         Factory&& make_trx) {
    auto [it, inserted] = _search_transactions.try_emplace(shard_id, nullptr);
    if (inserted) {
      it->second = std::make_unique<irs::IndexWriter::Transaction>(make_trx());
    }
    return *it->second;
  }

  // Parallel search-table INSERT (SereneDBSearchInsert): each sink thread
  // creates its own iresearch IndexWriter::Transaction (one segment) and
  // hands it off here at Combine. Unlike _search_transactions (one trx per
  // shard, used by the inverted-index path), a single search-table INSERT
  // contributes N transactions on the same shard. They are committed
  // together with the shared post_commit_seq tick in Commit() -- valid
  // because these writes are insert-only (no removes), so all N segments
  // share first_tick and one RefreshCommit publishes them.
  //
  // The operator serialises calls with its own sink-state mutex; this is the
  // only concurrent mutator of the container during the parallel Sink phase.
  void AddParallelSearchTransaction(
    std::unique_ptr<irs::IndexWriter::Transaction> trx) {
    _parallel_search_transactions.push_back(std::move(trx));
  }

  // Pins a SearchTableShard's DirectoryReader for the lifetime of this
  // sdb txn so every scan in the same transaction sees the same view
  // (committed state at the moment of the first scan). Mirrors
  // EnsureSearchSnapshot for inverted-index reads but on a separate map
  // -- the reader type is different (no rocksdb snapshot pair, no
  // segment-mask coordination) and recycling the existing map would
  // bloat its value type. `make_reader` is invoked only on first call
  // for a given shard_id; subsequent calls return the same reader.
  template<typename Factory>
  std::shared_ptr<irs::DirectoryReader> EnsureSearchTableReader(
    ObjectId shard_id, Factory&& make_reader) {
    auto it = _search_table_readers.find(shard_id);
    if (it == _search_table_readers.end()) {
      it = _search_table_readers
             .emplace(shard_id,
                      std::make_shared<irs::DirectoryReader>(make_reader()))
             .first;
    }
    return it->second;
  }

  // Per-search-table in-flight INSERT/UPDATE buffer (see
  // local_table_changes.h). Lazily populated by SereneDBSearchInsert; read
  // back by the scan overlay and the commit-time sink/marker drain.
  // PR 3.1: struct + accessor only -- nothing populates it yet.
  template<typename Self>
  auto& GetLocalTableChanges(this Self&& self) noexcept {
    return self._local_table_changes;
  }

  void Destroy() noexcept;

  catalog::TableStats GetTableStats(ObjectId table_id) const;

  // Must be called BEFORE the SST ingest so the IResearch background commit
  // thread knows to wait for us before advancing _committed_tick.
  void RegisterSearchFlushes() noexcept;

  // Commit all IResearch transactions after an SST ingest.
  // Uses Commit(post_ingest_seq + queries) so first_tick = post_ingest_seq,
  // which is guaranteed > _committed_tick when RegisterSearchFlushes() was
  // called before the ingest.
  void CommitSearchTransactions(uint64_t post_ingest_seq) noexcept;

  template<typename Visit, typename Filter = std::nullptr_t>
  void EnsureIndexesTransactions(ObjectId table_id, Visit&& visit,
                                 Filter&& filter = nullptr) {
    auto snapshot = EnsureCatalogSnapshot();
    SDB_ASSERT(snapshot->GetObject(table_id)->GetType() ==
               catalog::ObjectType::Table);

    for (auto index_shard : snapshot->GetIndexShardsByRelation(table_id)) {
      auto index =
        snapshot->GetObject<catalog::Index>(index_shard->GetIndexId());
      SDB_ASSERT(index);

      if constexpr (!std::is_same_v<std::decay_t<Filter>, std::nullptr_t>) {
        auto referenced = index->GetReferencedColumnIds();
        if (!filter(referenced)) {
          continue;
        }
      }

      if (index_shard->GetType() == catalog::ObjectType::InvertedIndexShard) {
        auto& inverted_index_shard =
          basics::downCast<search::InvertedIndexShard>(*index_shard);
        _search_transactions.try_emplace(inverted_index_shard.GetId(), nullptr);
        auto& transaction = _search_transactions[inverted_index_shard.GetId()];
        if (!transaction) {
          transaction = std::make_unique<irs::IndexWriter::Transaction>(
            inverted_index_shard.GetTransaction());
        }
        visit(*transaction, *index);
      } else {
        visit(GetRocksDBTransaction(), *index);
      }
    }
  }

 private:
  void ApplyTableStatsDiffs() noexcept;

  std::shared_ptr<StorageSnapshot> _storage_snapshot;
  std::unique_ptr<rocksdb::Transaction> _rocksdb_transaction;
  const rocksdb::Snapshot* _rocksdb_snapshot = nullptr;
  containers::FlatHashMap<ObjectId,
                          std::unique_ptr<irs::IndexWriter::Transaction>>
    _search_transactions;
  // Per-thread iresearch transactions from a parallel search-table INSERT.
  // See AddParallelSearchTransaction. Flush-registered/committed/aborted
  // alongside _search_transactions in Commit/Rollback; cleared in Destroy.
  std::vector<std::unique_ptr<irs::IndexWriter::Transaction>>
    _parallel_search_transactions;
  containers::FlatHashMap<ObjectId, search::InvertedIndexSnapshotPtr>
    _search_snapshots;
  // Per-(sdb txn, SearchTableShard) DirectoryReader cache. shared_ptr so
  // multiple scans inside one txn alias the same reader without a fresh
  // _writer->GetSnapshot() per scan.
  containers::FlatHashMap<ObjectId, std::shared_ptr<irs::DirectoryReader>>
    _search_table_readers;
  containers::FlatHashMap<ObjectId, int64_t> _table_rows_deltas;
  LocalTableChanges _local_table_changes;
  uint64_t _num_log_data_markers = 0;
};

}  // namespace sdb::query
