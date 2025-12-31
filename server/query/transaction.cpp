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

TxnState::TxnState()
  : _lazy_snapshot{[] { return GetServerEngine().currentSnapshot(); }} {}

yaclib::Future<Result> TxnState::Begin() {
  if (!InsideTransaction()) {
    _txn = CreateTransaction(*GetServerEngine().db());
    if (!_txn) {
      SDB_THROW(ERROR_INTERNAL, "Failed to create RocksDB transaction");
    }
    _txn->SetSnapshot();
  }
  return {};
}

yaclib::Future<Result> TxnState::Commit() {
  if (!InsideTransaction()) {
    return {};
  }
  auto status = _txn->Commit();
  if (!status.ok()) {
    return yaclib::MakeFuture(Result{
      ERROR_INTERNAL, "Failed to commit transaction: ", status.ToString()});
  }
  _txn.reset();
  Config::CommitVariables();
  return {};
}

yaclib::Future<Result> TxnState::Rollback() {
  if (!InsideTransaction()) {
    return {};
  }
  Config::RollbackVariables();
  auto status = _txn->Rollback();
  if (!status.ok()) {
    return yaclib::MakeFuture(
      Result{ERROR_INTERNAL,
             "Failed to rollback RocksDB transaction: ", status.ToString()});
  }
  _txn.reset();
  return {};
}

}  // namespace sdb
