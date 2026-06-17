////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "connector/duckdb_transaction.h"

#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/transaction/transaction_manager.hpp>

#include "catalog/store/store.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"

namespace sdb::connector {

SereneDBTransaction::SereneDBTransaction(duckdb::TransactionManager& manager,
                                         duckdb::ClientContext& context)
  : duckdb::Transaction(manager, context) {}

SereneDBTransactionManager::SereneDBTransactionManager(
  duckdb::AttachedDatabase& db)
  : duckdb::TransactionManager(db) {}

duckdb::Transaction& SereneDBTransactionManager::StartTransaction(
  duckdb::ClientContext& context) {
  duckdb::lock_guard<duckdb::mutex> lock(_lock);
  auto txn = duckdb::make_uniq<SereneDBTransaction>(*this, context);
  auto& ref = *txn;
  _transactions.push_back(std::move(txn));
  return ref;
}

duckdb::ErrorData SereneDBTransactionManager::CommitTransaction(
  duckdb::ClientContext& context, duckdb::Transaction& transaction) {
  return {};
}

void SereneDBTransactionManager::RollbackTransaction(
  duckdb::Transaction& transaction) {}

void SereneDBTransactionManager::Checkpoint(duckdb::ClientContext& context,
                                            bool force) {
  auto store = duckdb::DatabaseManager::Get(context).GetDatabase(
    context, std::string{catalog::kStoreDatabaseName});
  if (store) {
    store->GetTransactionManager().Checkpoint(context, force);
  }
}

}  // namespace sdb::connector
