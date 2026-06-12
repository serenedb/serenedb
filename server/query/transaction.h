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
        if (!_rocksdb_transaction) {
          // Store-table commits have no rocksdb transaction; their
          // secondary indexes are native ART on the store table, nothing
          // to feed here.
          continue;
        }
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
  containers::FlatHashMap<ObjectId, search::InvertedIndexSnapshotPtr>
    _search_snapshots;
  containers::FlatHashMap<ObjectId, int64_t> _table_rows_deltas;
};

}  // namespace sdb::query
