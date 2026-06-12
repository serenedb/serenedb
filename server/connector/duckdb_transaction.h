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

#pragma once

#include <duckdb.hpp>
#include <duckdb/transaction/transaction.hpp>
#include <duckdb/transaction/transaction_manager.hpp>

namespace sdb {

class ConnectionContext;
}

namespace sdb::connector {

class SereneDBTransaction final : public duckdb::Transaction {
 public:
  SereneDBTransaction(duckdb::TransactionManager& manager,
                      duckdb::ClientContext& context);

  void SetConnectionContext(ConnectionContext* ctx) { _connection_ctx = ctx; }
  ConnectionContext* GetConnectionContext() const { return _connection_ctx; }

 private:
  ConnectionContext* _connection_ctx = nullptr;
};

class SereneDBTransactionManager final : public duckdb::TransactionManager {
 public:
  bool IsTransactionShell() const final { return true; }
  explicit SereneDBTransactionManager(duckdb::AttachedDatabase& db);

  duckdb::Transaction& StartTransaction(duckdb::ClientContext& context) final;
  duckdb::ErrorData CommitTransaction(duckdb::ClientContext& context,
                                      duckdb::Transaction& transaction) final;
  void RollbackTransaction(duckdb::Transaction& transaction) final;
  void Checkpoint(duckdb::ClientContext& context, bool force) final;

 private:
  duckdb::mutex _lock;
  std::vector<duckdb::unique_ptr<SereneDBTransaction>> _transactions;
};

}  // namespace sdb::connector
