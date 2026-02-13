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

#include "basics/assert.h"
#include "catalog/catalog.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/table_shard.h"

namespace sdb::query {

Result Transaction::Begin(IsolationLevel isolation_level) {
  SDB_ASSERT(!HasTransactionBegin());
  _isolation_level = isolation_level;
  CreateRocksDBTransaction();
  _state |= State::HasTransactionBegin;
  return {};
}

Result Transaction::Commit() {
  SDB_ASSERT(_rocksdb_transaction);
  auto status = _rocksdb_transaction->Commit();
  if (!status.ok()) {
    return {ERROR_INTERNAL,
            "Failed to commit RocksDB transaction: ", status.ToString()};
  }
  for (auto& search_transaction : _search_transactions) {
    search_transaction.second->Commit();
  }
  ApplyTableStatsDiffs();
  CommitVariables();
  Destroy();
  return {};
}

Result Transaction::Rollback() {
  SDB_ASSERT(_rocksdb_transaction);
  auto status = _rocksdb_transaction->Rollback();
  if (!status.ok()) {
    return {ERROR_INTERNAL,
            "Failed to rollback RocksDB transaction: ", status.ToString()};
  }
  for (auto& search_transaction : _search_transactions) {
    search_transaction.second->Abort();
  }
  RollbackVariables();
  Destroy();
  return {};
}

void Transaction::AddRocksDBRead() noexcept { _state |= State::HasRocksDBRead; }

void Transaction::AddRocksDBWrite() noexcept {
  _state |= State::HasRocksDBWrite;
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
    if ((_state & State::HasRocksDBWrite) != State::None) {
      CreateRocksDBTransaction();
    } else {
      CreateStorageSnapshot();
    }
  }
  return *_rocksdb_snapshot;
}

rocksdb::Transaction& Transaction::EnsureRocksDBTransaction() {
  SDB_ASSERT((_state & State::HasRocksDBWrite) != State::None);
  if (!_rocksdb_transaction) {
    CreateRocksDBTransaction();
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
  _rocksdb_transaction->SetSnapshot();
  _rocksdb_snapshot = _rocksdb_transaction->GetSnapshot();
  SDB_ASSERT(_rocksdb_snapshot != nullptr);
}

void Transaction::Destroy() noexcept {
  _state = State::None;
  _isolation_level = IsolationLevel::RepeatableRead;
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

}  // namespace sdb::query
