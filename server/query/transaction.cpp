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

#include "basics/assert.h"
#include "catalog/catalog.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/table_shard.h"

namespace sdb::query {

Result Transaction::Begin() {
  SDB_ASSERT(!HasTransactionBegin());
  CreateRocksDBTransaction();
  _state |= State::HasTransactionBegin;
  return {};
}

Result Transaction::Commit() {
  SDB_ASSERT(_rocksdb_transaction);

  const uint64_t num_ops =
    _rocksdb_transaction->GetNumPuts() + _rocksdb_transaction->GetNumDeletes();
  SDB_ASSERT(_rocksdb_transaction->GetNumMerges() == 0,
             "We do not expect merges for now");

  if (num_ops > 0) [[likely]] {
    auto abort_guard = absl::Cleanup([&] {
      for (auto& search_transaction : _search_transactions) {
        search_transaction.second->Abort();
      }
      RollbackVariables();
      Destroy();
    });

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

    for (auto& search_transaction : _search_transactions) {
      search_transaction.second->Commit(post_commit_seq);
    }
    std::move(abort_guard).Cancel();
    ApplyTableStatsDiffs();
  }
  CommitVariables();
  Destroy();
  return {};
}

Result Transaction::Rollback() {
  SDB_ASSERT(_rocksdb_transaction);

  auto cleanup = absl::Cleanup([&] {
    for (auto& search_transaction : _search_transactions) {
      search_transaction.second->Abort();
    }
    RollbackVariables();
    Destroy();
  });

  auto status = _rocksdb_transaction->Rollback();
  if (!status.ok()) {
    return {ERROR_INTERNAL,
            "Failed to rollback RocksDB transaction: ", status.ToString()};
  }
  return {};
}

void Transaction::AddRocksDBRead() noexcept { _state |= State::HasRocksDBRead; }

bool Transaction::HasRocksDBRead() const noexcept {
  return (_state & State::HasRocksDBRead) != State::None;
}

void Transaction::AddRocksDBWrite() noexcept {
  _state |= State::HasRocksDBWrite;
}

bool Transaction::HasRocksDBWrite() const noexcept {
  return (_state & State::HasRocksDBWrite) != State::None;
}

bool Transaction::HasTransactionBegin() const noexcept {
  return (_state & State::HasTransactionBegin) != State::None;
}

rocksdb::Transaction* Transaction::GetRocksDBTransaction() const noexcept {
  return _rocksdb_transaction.get();
}

const rocksdb::Snapshot& Transaction::EnsureRocksDBSnapshot() {
  SDB_ASSERT((_state & State::HasRocksDBRead) != State::None);
  if (!_rocksdb_snapshot) {
    if (HasTransactionBegin()) {
      // What if we will create transaction lazily?
      SetTransactionSnapshot();
    } else if (HasRocksDBWrite()) {
      CreateRocksDBTransaction();
      SetTransactionSnapshot();
    } else {
      CreateStorageSnapshot();
    }
  }

  SDB_ASSERT(_rocksdb_snapshot);
  return *_rocksdb_snapshot;
}

rocksdb::Transaction& Transaction::EnsureRocksDBTransaction() {
  SDB_ASSERT((_state & State::HasRocksDBWrite) != State::None);
  if (!_rocksdb_transaction) {
    CreateRocksDBTransaction();
  }
  if (!_rocksdb_snapshot &&
      GetIsolationLevel() == IsolationLevel::RepeatableRead) {
    SetTransactionSnapshot();
  }
  return *_rocksdb_transaction;
}

void Transaction::CreateStorageSnapshot() {
  SDB_ASSERT(!_rocksdb_snapshot);
  SDB_ASSERT(!_storage_snapshot);
  _storage_snapshot = GetServerEngine().currentSnapshot();
  SDB_ASSERT(_storage_snapshot != nullptr);
  _rocksdb_snapshot = _storage_snapshot->GetSnapshot();
  SDB_ASSERT(_rocksdb_snapshot != nullptr);
}

void Transaction::CreateRocksDBTransaction() {
  SDB_ASSERT(!_rocksdb_snapshot);
  SDB_ASSERT(!_rocksdb_transaction);
  auto* db = GetServerEngine().db();
  SDB_ASSERT(db != nullptr);
  rocksdb::WriteOptions write_options;
  rocksdb::TransactionOptions txn_options;
  txn_options.skip_concurrency_control = true;
  _rocksdb_transaction.reset(db->BeginTransaction(write_options, txn_options));
  SDB_ASSERT(_rocksdb_transaction != nullptr);
}

void Transaction::Destroy() noexcept {
  _state = State::None;
  _storage_snapshot.reset();
  _rocksdb_transaction.reset();
  _rocksdb_snapshot = nullptr;
  _search_transactions.clear();
  _table_rows_deltas.clear();
}

catalog::TableStats Transaction::GetTableStats(ObjectId table_id) const {
  // TODO(codeworse): manage catalog snapshot in transaction
  auto table_shard = GetCatalogSnapshot()->GetTableShard(table_id);
  if (!table_shard) {
    SDB_THROW(ERROR_BAD_PARAMETER,
              "Table shard not found for table id: ", table_id);
  }
  return table_shard->GetTableStats();
}

void Transaction::ApplyTableStatsDiffs() {
  for (const auto& [table_id, delta] : _table_rows_deltas) {
    auto table_shard = catalog::GetTableShard(table_id);
    SDB_ASSERT(table_shard);
    if (table_shard) {
      table_shard->UpdateNumRows(delta);
    }
  }
  _table_rows_deltas.clear();
}

void Transaction::SetTransactionSnapshot() const {
  _rocksdb_transaction->SetSnapshot();
  _rocksdb_snapshot = _rocksdb_transaction->GetSnapshot();
  SDB_ASSERT(_rocksdb_snapshot);
}

}  // namespace sdb::query
