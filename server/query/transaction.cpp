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

#include "query/transaction.h"

#include <absl/cleanup/cleanup.h>

#include <duckdb/main/client_context.hpp>

#include "basics/assert.h"
#include "catalog/catalog.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/table_shard.h"

namespace sdb::query {

void Transaction::OnNewStatement() {
  auto level = GetIsolationLevel();
  if (level == IsolationLevel::READ_COMMITTED) {
    DropCatalogSnapshot();
    _rocksdb_snapshot = nullptr;
    _search_snapshots.clear();
  }
}

void Transaction::PreCommit() noexcept {
  // Revert SET LOCAL overlays (and clear the txn map) while the DuckDB
  // transaction is still active so custom-impl settings (search_path,
  // transaction_isolation) can use their normal set_local path (which may
  // do catalog lookups).
  CommitVariables();
}

void Transaction::PreRollback() noexcept { RollbackVariables(); }

Result Transaction::Commit() {
  uint64_t num_ops = _rocksdb_transaction
                       ? _rocksdb_transaction->GetNumPuts() +
                           _rocksdb_transaction->GetNumDeletes()
                       : 0;
  SDB_ASSERT(!_rocksdb_transaction || _rocksdb_transaction->GetNumMerges() == 0,
             "We do not expect merges for now");
  // Marker-only txns have zero rocksdb ops and would skip the commit block;
  // force a no-op Delete to consume a seq number so markers reach the WAL
  // and the inverted index has a tick to commit on.
  if (num_ops == 0 && _num_log_data_markers > 0 && _rocksdb_transaction) {
    _rocksdb_transaction->Delete(rocksdb::Slice{});
    ++num_ops;
  }
  if (num_ops > 0) [[likely]] {
    for (auto& search_transaction : _search_transactions) {
      // tie iresearch transaction's active segment to current flush context in
      // writer and let IndexWriter know that he need to wait for this
      // transaction to settle before proceeding with commit. That is important
      // as we are committing "on tick" and must ensure that if we mark this
      // transaction with tick (see below commit calls) writer will not commit
      // without it. This could happend if background index commit will start
      // AFTER RocksDB commit but before transaction commit. But as we told
      // index writer to wait, we are safe.
      search_transaction.second->RegisterFlush();
    }
    absl::Cleanup rollback = [&] {
      for (auto& search_transaction : _search_transactions) {
        search_transaction.second->Abort();
      }
      // PreCommit already ran CommitVariables, which cleared the txn map
      // after restoring SET LOCAL overlays. Nothing left to roll back here
      // on rocksdb commit failure -- plain-SET values have already been
      // accepted as committed.
      Destroy();
    };

    // When updating non-PK columns with a search index, the search engine
    // Remove consumes a tick from the same range as rocksdb seq numbers.
    // With num_ops == _queries, first_tick == committed_tick which violates
    // the strict ordering invariant. Add an extra Delete to bump seq by 1.
    for (const auto& [id, trx] : _search_transactions) {
      SDB_ASSERT(trx->GetQueries() <= num_ops);
      if (trx->GetQueries() == num_ops) {
        SDB_ASSERT(trx->GetQueries() != 0);
        // TODO: I'm not sure in what column family we should write
        _rocksdb_transaction->Delete(rocksdb::Slice{});
        ++num_ops;
        break;
      }
    }
    SDB_ASSERT(absl::c_all_of(_search_transactions, [&](const auto& p) {
      return p.second->GetQueries() < num_ops;
    }));

    SDB_IF_FAILURE("crash_before_rocksdb_commit") { SDB_IMMEDIATE_ABORT(); }
    auto status = _rocksdb_transaction->Commit();
    SDB_IF_FAILURE("crash_after_rocksdb_commit") { SDB_IMMEDIATE_ABORT(); }

    if (!status.ok()) {
      return {ERROR_INTERNAL,
              "Failed to commit RocksDB transaction: ", status.ToString()};
    }

    // id is first write operation seqno in the WAL
    auto post_commit_seq = _rocksdb_transaction->GetId();
    // add number of operations to get last operation seqno
    post_commit_seq += num_ops - 1;

    std::move(rollback).Cancel();

    for (auto& search_transaction : _search_transactions) {
      search_transaction.second->Commit(post_commit_seq);
    }
  }
  ApplyTableStatsDiffs();
  Destroy();

  return {};
}

Result Transaction::Rollback() {
  absl::Cleanup rollback = [&] {
    for (auto& search_transaction : _search_transactions) {
      search_transaction.second->Abort();
    }
    RollbackVariables();
    Destroy();
  };

  if (_rocksdb_transaction) {
    auto status = _rocksdb_transaction->Rollback();
    if (!status.ok()) {
      return {ERROR_INTERNAL,
              "Failed to rollback RocksDB transaction: ", status.ToString()};
    }
  }
  return {};
}

search::InvertedIndexSnapshotPtr Transaction::EnsureSearchSnapshot(
  ObjectId index_id) {
  auto it = _search_snapshots.find(index_id);
  if (it == _search_snapshots.end()) {
    auto index_shard = EnsureCatalogSnapshot()->GetIndexShard(index_id);
    SDB_ASSERT(index_shard);
    SDB_ASSERT(index_shard->GetType() ==
               catalog::ObjectType::InvertedIndexShard);
    auto& inverted_index_shard =
      basics::downCast<search::InvertedIndexShard>(*index_shard.get());
    it = _search_snapshots
           .emplace(index_id, inverted_index_shard.GetInvertedIndexSnapshot())
           .first;
  }
  return it->second;
}

void Transaction::EnsureRocksDBSnapshot() {
  if (_rocksdb_snapshot) {
    return;
  }
  SDB_ASSERT(!_storage_snapshot);
  if (_rocksdb_transaction) {
    _rocksdb_transaction->SetSnapshot();
    _rocksdb_snapshot = _rocksdb_transaction->GetSnapshot();
  } else {
    _storage_snapshot = GetServerEngine().currentSnapshot();
    SDB_ASSERT(_storage_snapshot);
    _rocksdb_snapshot = _storage_snapshot->GetSnapshot();
  }
  SDB_ASSERT(_rocksdb_snapshot);
}

void Transaction::EnsureRocksDBTransaction() {
  SDB_ASSERT(!_storage_snapshot);
  if (_rocksdb_transaction) {
    return;
  }
  SDB_ASSERT(!_storage_snapshot);
  SDB_ASSERT(!_rocksdb_snapshot);
  auto* db = GetServerEngine().db();
  SDB_ASSERT(db);
  rocksdb::WriteOptions write_options;
  rocksdb::TransactionOptions txn_options;
  txn_options.skip_concurrency_control = true;
  _rocksdb_transaction.reset(db->BeginTransaction(write_options, txn_options));
  SDB_ASSERT(_rocksdb_transaction);
}

void Transaction::RegisterSearchFlushes() noexcept {
  for (auto& [id, trx] : _search_transactions) {
    trx->RegisterFlush();
  }
}

void Transaction::CommitSearchTransactions(uint64_t post_ingest_seq) noexcept {
  for (auto& [id, trx] : _search_transactions) {
    const auto queries = trx->GetQueries();
    trx->Commit(post_ingest_seq + queries);
  }
}

void Transaction::Destroy() noexcept {
  DropCatalogSnapshot();
  _storage_snapshot.reset();
  _rocksdb_transaction.reset();
  _rocksdb_snapshot = nullptr;
  _search_transactions.clear();
  _table_rows_deltas.clear();
  _search_snapshots.clear();
  _num_log_data_markers = 0;
}

catalog::TableStats Transaction::GetTableStats(ObjectId table_id) const {
  // TODO(codeworse): manage catalog snapshot in transaction
  auto table_shard = EnsureCatalogSnapshot()->GetTableShard(table_id);
  if (!table_shard) {
    SDB_THROW(ERROR_BAD_PARAMETER,
              "Table shard not found for table id: ", table_id);
  }
  return table_shard->GetTableStats();
}

void Transaction::ApplyTableStatsDiffs() noexcept {
  if (_table_rows_deltas.empty()) {
    return;
  }
  auto snapshot = EnsureCatalogSnapshot();
  for (const auto& [table_id, delta] : _table_rows_deltas) {
    // It's possible that while transaction was active, table got dropped.
    if (!snapshot->GetObject(table_id)) {
      continue;
    }
    auto table_shard = snapshot->GetTableShard(table_id);
    SDB_ASSERT(table_shard);
    if (table_shard) {
      table_shard->UpdateNumRows(delta);
    }
  }
  _table_rows_deltas.clear();
}

}  // namespace sdb::query
