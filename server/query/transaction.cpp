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

#include "storage_engine/engine_feature.h"

namespace sdb::query {

Result Transaction::Begin() {
  SDB_ASSERT(!HasTransactionBegin());
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
  SDB_ASSERT((_state & State::HasRocksDBWrite) != State::None);
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
  _storage_snapshot.reset();
  _rocksdb_transaction.reset();
  _rocksdb_snapshot = nullptr;
}

}  // namespace sdb::query
