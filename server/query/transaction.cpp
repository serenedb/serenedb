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

#include <yaclib/async/make.hpp>

#include "query/config.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb {

std::shared_ptr<rocksdb::Transaction> CreateTransaction(
  rocksdb::TransactionDB& db) {
  rocksdb::WriteOptions write_options;
  rocksdb::TransactionOptions txn_options;
  txn_options.skip_concurrency_control = true;
  return std::shared_ptr<rocksdb::Transaction>{
    db.BeginTransaction(write_options, txn_options)};
}

Result TxnState::Begin() {
  SDB_ASSERT(_state != State::SNAPSHOT);
  if (!InsideTransaction()) {
    auto txn = CreateTransaction(*GetServerEngine().db());
    if (!txn) {
      return {ERROR_INTERNAL, "Failed to create RocksDB transaction"};
    }
    txn->SetSnapshot();
    _data.emplace<Transaction>(std::move(txn));
    _state = State::TRANSACTION;
  }
  SDB_ASSERT(InsideTransaction());
  return {};
}

Result TxnState::Commit() {
  SDB_ASSERT(_state != State::SNAPSHOT);
  if (!InsideTransaction()) {
    return {};
  }
  Transaction& txn = GetTransaction();
  auto status = txn->Commit();
  if (!status.ok()) {
    return {ERROR_INTERNAL,
            "Failed to commit transaction: ", status.ToString()};
  }
  _data = {};
  _state = State::NONE;
  Config::CommitVariables();
  return {};
}

Result TxnState::Rollback() {
  SDB_ASSERT(_state != State::SNAPSHOT);
  if (!InsideTransaction()) {
    return {};
  }
  Config::RollbackVariables();
  auto& txn = GetTransaction();
  auto status = txn->Rollback();
  if (!status.ok()) {
    return {ERROR_INTERNAL,
            "Failed to rollback RocksDB transaction: ", status.ToString()};
  }
  _state = State::NONE;
  _data = {};
  return {};
}

void TxnState::CreateLocalTransaction() const {
  auto txn = CreateTransaction(*GetServerEngine().db());
  if (!txn) {
    SDB_THROW(ERROR_INTERNAL, "Failed to create RocksDB transaction");
  }
  txn->SetSnapshot();
  _data.emplace<Transaction>(std::move(txn));
}

void TxnState::CreateLocalSnapshot() const {
  auto external_snapshot = GetServerEngine().currentSnapshot();
  _data.emplace<Snapshot>(std::move(external_snapshot));
}

void TxnState::EnsureTransaction() {
  switch (_state) {
    case State::LOCAL:
      if (!std::holds_alternative<Transaction>(_data) ||
          !std::get<Transaction>(_data)) {
        CreateLocalTransaction();
      }
      [[fallthrough]];
    case State::TRANSACTION:
      SDB_ASSERT(std::get<Transaction>(_data));
      return;
    default:
      SDB_UNREACHABLE();
  }
}

TxnState::Transaction& TxnState::GetTransaction() {
  EnsureTransaction();
  auto& txn = std::get<Transaction>(_data);
  SDB_ASSERT(txn);
  return txn;
}

void TxnState::EnsureSnapshot() {
  switch (_state) {
    case State::SNAPSHOT:
      if (!std::holds_alternative<Snapshot>(_data) ||
          !std::get<Snapshot>(_data)) {
        CreateLocalSnapshot();
      }
      return;
    case State::LOCAL:
    case State::TRANSACTION:
      return;
    default:
      SDB_UNREACHABLE();
  }
}

const rocksdb::Snapshot* TxnState::GetSnapshot() {
  EnsureSnapshot();
  switch (_state) {
    case State::SNAPSHOT: {
      auto& snapshot = std::get<Snapshot>(_data);
      SDB_ASSERT(snapshot);
      SDB_ASSERT(std::dynamic_pointer_cast<RocksDBSnapshot>(snapshot));
      return std::dynamic_pointer_cast<RocksDBSnapshot>(snapshot)
        ->getSnapshot();
    }
    case State::LOCAL:
    case State::TRANSACTION: {
      auto& txn = std::get<Transaction>(_data);
      SDB_ASSERT(txn);
      return txn->GetSnapshot();
    }
    default:
      SDB_UNREACHABLE();
  }
}

void TxnState::ResetState() noexcept {
  _state = State::NONE;
  _data = {};
}

}  // namespace sdb
