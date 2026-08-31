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
#include <duckdb/transaction/duck_transaction_manager.hpp>

namespace sdb::connector {

// DDL on a serenedb database runs on a real DuckTransaction so it gets the undo
// buffer, commit-id stamping and rollback that CatalogSet mutations require --
// they call DuckTransactionManager::Get(), which throws for a foreign manager.
//
// The rows are in this attachment, so a statement writing a serenedb table
// writes exactly one database and occupies the single-writable-db slot like any
// duckdb table.
class SereneDBTransactionManager final : public duckdb::DuckTransactionManager {
 public:
  explicit SereneDBTransactionManager(duckdb::AttachedDatabase& db);

  void Checkpoint(duckdb::ClientContext& context, bool force) final;

  duckdb::ErrorData CommitTransaction(duckdb::ClientContext& context,
                                      duckdb::Transaction& transaction) final;

  void RollbackTransaction(duckdb::Transaction& transaction) final;
};

}  // namespace sdb::connector
