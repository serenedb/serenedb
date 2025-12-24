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

#include "query/config.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_selector_feature.h"

namespace sdb {

std::shared_ptr<rocksdb::Transaction> CreateTransaction(
  rocksdb::TransactionDB& db) {
  rocksdb::WriteOptions write_options;
  rocksdb::TransactionOptions txn_options;
  txn_options.skip_concurrency_control = true;
  return std::shared_ptr<rocksdb::Transaction>{
    db.BeginTransaction(write_options, txn_options)};
}

void TxnState::Begin() {
  if (InsideTransaction()) {
    return;
  }
  SDB_ASSERT(_variables.empty());
  auto* db = GetServerEngineAs<RocksDBEngineCatalog>().db();
  SDB_ASSERT(db);
  _txn = CreateTransaction(*db);
  SDB_ASSERT(_txn);
  _txn->SetSnapshot();
}

void TxnState::Commit() {
  for (auto&& [key, var] : _variables) {
    if (var.action == TxnAction::Apply) {
      _config._session.insert_or_assign(key, std::move(var.value));
    }
  }
  _variables.clear();
  _txn->Commit();
  _txn.reset();
}

void TxnState::Abort() {
  if (!InsideTransaction()) {
    return;
  }
  _variables.clear();
  _txn->Rollback();
  _txn.reset();
}

}  // namespace sdb
