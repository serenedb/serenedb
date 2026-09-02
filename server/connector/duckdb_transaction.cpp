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

#include <absl/cleanup/cleanup.h>

#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/transaction/transaction.hpp>

#include "catalog/log/duckdb_global_catalog.h"
#include "catalog/log/store.h"

namespace sdb::connector {
namespace {

// One statement allocates a handful of ids; a block this size covers a long run
// of them, so the horizon is written about as often as it was before and never
// from under a catalog lock.
constexpr uint64_t kOidHeadroom = 4096;

}  // namespace

SereneDBTransactionManager::SereneDBTransactionManager(
  duckdb::AttachedDatabase& db)
  : duckdb::DuckTransactionManager(db) {}

// Object ids are handed out wherever a catalog entry is built, under the
// catalog locks, and raising their durable horizon writes to the cluster log --
// whose lock has to stay outside those. Raise it here instead, where this
// transaction holds no catalog lock, so the allocations it goes on to make are
// already covered.
duckdb::Transaction& SereneDBTransactionManager::StartTransaction(
  duckdb::ClientContext& context) {
  const catalog::OidHorizonWaitScope may_wait;
  duckdb::DatabaseManager::Get(db).EnsureOidHeadroom(kOidHeadroom);
  return duckdb::DuckTransactionManager::StartTransaction(context);
}

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

duckdb::ErrorData SereneDBTransactionManager::CommitTransaction(
  duckdb::ClientContext& context, duckdb::Transaction& transaction) {
  const bool wrote = !transaction.IsReadOnly();
  // A run that wrote a record holds the cluster WAL until it ends, so it has to
  // end however the commit leaves: an exception escaping duckdb's commit would
  // otherwise leave the WAL taken by a thread that is already gone.
  bool committed = false;
  const absl::Cleanup end_run = [&] {
    catalog::EndCommittingCatalogRun(committed);
    if (wrote) {
      catalog::EndCommittingWrites(context, committed);
    }
  };
  auto error =
    duckdb::DuckTransactionManager::CommitTransaction(context, transaction);
  committed = !error.HasError();
  return error;
}

void SereneDBTransactionManager::RollbackTransaction(
  duckdb::Transaction& transaction) {
  const auto context = transaction.context.lock();
  duckdb::DuckTransactionManager::RollbackTransaction(transaction);
  if (context) {
    catalog::EndCommittingWrites(*context, /*committed=*/false);
  }
}

}  // namespace sdb::connector
