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

yaclib::Future<Result> TxnState::Begin() {
  Result r;
  if (InsideTransaction()) {
    return yaclib::MakeFuture(std::move(r));
  }
  SDB_ASSERT(_variables.empty());
  auto* db = GetServerEngine().db();
  SDB_ASSERT(db);
  _txn = CreateTransaction(*db);
  if (!_txn) {
    r = {ERROR_INTERNAL, "Failed to create RocksDB transaction"};
  }
  _txn->SetSnapshot();
  return yaclib::MakeFuture(std::move(r));
}

yaclib::Future<Result> TxnState::Commit() {
  Result r;
  for (auto&& [key, var] : _variables) {
    if (var.action == Action::Apply) {
      _config.Set(Config::VariableContext::Session, key, std::move(var.value));
    }
  }
  _variables.clear();
  auto status = _txn->Commit();
  if (!status.ok()) {
    r = {ERROR_INTERNAL, "Failed to commit RocksDB transaction: ",
                                      status.ToString()};
  } else {
    _txn.reset();
  }
  return yaclib::MakeFuture(std::move(r));
}

yaclib::Future<Result> TxnState::Abort() {
  Result r;
  if (!InsideTransaction()) {
    return yaclib::MakeFuture(std::move(r));
  }
  _variables.clear();
  auto status = _txn->Rollback();
  if (!status.ok()) {
    r = {ERROR_INTERNAL,
         absl::StrCat("Failed to rollback RocksDB transaction: ",
                      status.ToString())};
  } else {
    _txn.reset();
  }
  return yaclib::MakeFuture(std::move(r));
}

}  // namespace sdb
