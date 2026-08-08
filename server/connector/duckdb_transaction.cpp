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
#include <duckdb/transaction/transaction.hpp>

#include "catalog/store/store.h"

namespace sdb::connector {

SereneDBTransactionManager::SereneDBTransactionManager(
  duckdb::AttachedDatabase& db)
  : duckdb::DuckTransactionManager(db) {}

void SereneDBTransactionManager::Checkpoint(duckdb::ClientContext& context,
                                            bool force) {
  // The rows are in this attachment, so the user's CHECKPOINT reaches them
  // directly. The statement issuing it already has a transaction here -- it is
  // the database it runs in -- and duckdb refuses a FORCE with one open. That
  // refusal guards against waiting on oneself, which a read-only transaction
  // cannot cause, so drop the force rather than the checkpoint.
  const bool self_force =
    force && !duckdb::Transaction::TryGet(context, db).get();
  duckdb::DuckTransactionManager::Checkpoint(context, self_force);
}

}  // namespace sdb::connector
