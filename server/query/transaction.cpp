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

#include <absl/cleanup/cleanup.h>

#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/meta_transaction.hpp>

#include "basics/assert.h"
#include "basics/duckdb_engine.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/log.h"
#include "catalog/catalog.h"
#include "catalog/store/store.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"
#include "search/tick_domain.h"

namespace sdb::query {

// Snapshot lifecycle across statements and transactions.
//
// A query reads through three independently-versioned views:
//   * the catalog snapshot -- serenedb metadata (tables, indexes, schemas);
//   * the native read view -- DuckDB MVCC over table storage;
//   * search snapshots     -- iresearch MVCC readers over index data.
//
// Only READ COMMITTED and REPEATABLE READ exist. The native read view and the
// search readers (the actual MVCC data) follow isolation (IsStableSnapshot): an
// explicit REPEATABLE READ transaction, OR any transaction that has performed
// uncommitted DML, holds one frozen data snapshot for the rest of its life, so
// another session's committed DML stays invisible; everywhere else (autocommit,
// or a DML-less READ COMMITTED transaction) the data refreshes per statement.
// Commit/rollback releases everything (Commit/Rollback -> Destroy).
//
// Freezing the data once a transaction has written is a safety requirement, not
// just a REPEATABLE READ nicety: the uncommitted rows are tied to this catalog
// + read view, and serenedb's atomic, lock-free DDL (unlike PG's row/table
// locks) would otherwise let another session's DDL drift them.
//
// The catalog is the exception: it is NOT under MVCC isolation, because
// serenedb DDL is atomic and non-transactional (it commits immediately, the
// analog of postgres' CommandCounterIncrement). So the catalog refreshes per
// statement under READ COMMITTED, AND after our own DDL even under REPEATABLE
// READ -- it only stays pinned while uncommitted DML is held (reacquiring it
// then would be unsafe: our pending rows are tied to it). DuckDB's
// ModifiedDatabase() cannot drive this -- it is set for DDL too
// (CheckIfPreparedStatementIsExecutable records every statement's
// modified_databases, and ALTER/CREATE/DROP declare the catalog database) -- so
// DML and DDL are classified from the statement itself (MarkStatementDml /
// MarkStatementDdl). The motivating case is ALTER TABLE ADD COLUMN ... DEFAULT
// <volatile>: it expands to [ADD COLUMN; UPDATE backfill; SET DEFAULT] and runs
// as an implicit block under the default REPEATABLE READ, so the backfill
// UPDATE must bind the catalog the ADD COLUMN just produced. (Once DML has
// frozen the views, same-session DDL is no longer observed mid-transaction --
// an accepted edge case.)
//
// Views release eagerly -- per-statement views at statement end, the
// transaction-held ones at commit/rollback -- so they never pin MVCC versions
// or index segments against background cleanup, and re-acquire lazily on first
// use (CatalogSnapshot / EnsureSearchSnapshot). The one per-statement
// step that must run at statement *start* is advancing the native read view:
// RefreshStartTime() captures "now", so to see everything committed before the
// statement it has to run when the statement begins, not when the prior ended.

bool Transaction::IsStableSnapshot() const {
  // An explicit REPEATABLE READ transaction holds one snapshot for its life.
  if (!GetClientContext().transaction.IsAutoCommit() &&
      GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    return true;
  }
  // A transaction that has performed uncommitted DML freezes too: its rows are
  // tied to this catalog + read view, so re-acquiring either would let them
  // drift (e.g. another session's atomic DDL on a table we hold pending writes
  // for). PG prevents this with row/table locks; serenedb's lock-free atomic
  // DDL cannot, so we pin instead.
  return _had_dml;
}

void Transaction::OnStatementBegin() {
  // Statement-scoped views are acquired here, on the connection thread,
  // before binding starts and before any executor task can read them.
  AcquireCatalogSnapshot();
  if (IsStableSnapshot()) {
    return;
  }
  // READ COMMITTED / autocommit with no writes yet (IsStableSnapshot already
  // excluded REPEATABLE READ and any modified transaction): advance the native
  // read view so this statement sees the latest committed data. Autocommit
  // already begins a fresh transaction per statement, so only an explicit
  // transaction needs the advance.
  auto& txn = GetClientContext().transaction;
  if (txn.HasActiveTransaction() && !txn.IsAutoCommit()) {
    txn.ActiveTransaction().RefreshStartTime();
  }
}

void Transaction::OnStatementEnd() {
  const bool was_ddl = _statement_is_ddl;
  if (_statement_is_dml) {
    _had_dml = true;
  }
  _statement_is_ddl = false;
  _statement_is_dml = false;

  if (_had_dml) {
    // Uncommitted DML pins all three views for the rest of the transaction:
    // read-your-writes, plus the safety freeze -- our pending rows are tied to
    // this catalog + read view, and another session's lock-free atomic DDL
    // would otherwise drift them. Commit/Rollback -> Destroy releases all.
    return;
  }
  if (!IsStableSnapshot()) {
    // READ COMMITTED / autocommit, no DML: refresh everything per statement so
    // the next statement sees the latest committed catalog and data. Drop
    // search readers so background compaction is not pinned. All re-acquire
    // lazily; the native read view advances at the next statement's
    // OnStatementBegin.
    DropCatalogSnapshot();
    _search_snapshots.clear();
    // Search-table reads go through SearchTxn()'s reader cache, not the
    // _search_snapshots above; reset on the same (non-pinned) boundary.
    if (_search_txn) {
      _search_txn->ResetReaders();
    }
    return;
  }
  // Explicit REPEATABLE READ, no DML: the native read view and search readers
  // stay frozen for the transaction's life. The catalog is NOT under MVCC
  // isolation -- serenedb DDL is atomic and non-transactional -- so our own DDL
  // must still drop it, or a later statement could not observe the column/table
  // it changed: ALTER ADD COLUMN ... DEFAULT <volatile> expands to [ADD COLUMN;
  // UPDATE backfill; SET DEFAULT] and, under the default REPEATABLE READ, runs
  // as an implicit block whose backfill must bind the new column.
  if (was_ddl) {
    DropCatalogSnapshot();
  }
}

void Transaction::PreCommit() noexcept {
  // Revert SET LOCAL overlays (and clear the txn map) while the DuckDB
  // transaction is still active so custom-impl settings (search_path,
  // transaction_isolation) can use their normal set_local path (which may
  // do catalog lookups).
  CommitVariables();
}

void Transaction::PreRollback() noexcept { RollbackVariables(); }

void Transaction::CommitSearch(
  std::optional<search::WalCursor> cursor) noexcept {
  if (_search_transactions.empty()) {
    return;
  }
  for (auto& search_transaction : _search_transactions) {
    // Tie the iresearch transaction's active segment to the current flush
    // context so a background index commit that starts now waits for this
    // transaction to settle before committing "on tick".
    search_transaction.second.transaction->RegisterFlush();
  }
  absl::Cleanup rollback = [&] {
    for (auto& search_transaction : _search_transactions) {
      search_transaction.second.transaction->Abort();
    }
    _search_transactions.clear();
  };

  // Reserve a tick range covering every writer's query (Remove/Replace)
  // count, so that per writer first_tick = last_tick - queries stays
  // strictly above its previously committed tick.
  uint64_t max_queries = 0;
  for (const auto& [id, entry] : _search_transactions) {
    max_queries =
      std::max<uint64_t>(max_queries, entry.transaction->GetQueries());
  }
  const auto last_tick =
    search::TickDomain::Instance().Advance(max_queries + 1);

  std::move(rollback).Cancel();

  for (auto& [index_id, entry] : _search_transactions) {
    // Record this commit's WAL cursor into the index's own table BEFORE its
    // segment becomes flushable (Commit below emplaces it into the flush
    // context). A concurrent refresh can only flush this batch after Commit, by
    // which point the offset is already in the table, so the refresh's
    // CursorAtOrBelow(flushed_tick) can never under-claim and re-stream an
    // already-durable insert after a crash. The cursor is this commit's exact
    // WAL position, captured under the WAL lock by the engine: commits overlap,
    // so reading the WAL size here would include later transactions' bytes and
    // over-claim (skipping their re-stream after a crash).
    if (entry.storage && cursor) {
      entry.storage->RecordFlushCursor(last_tick, *cursor);
    }
    if (entry.transaction->Commit(last_tick)) {
      continue;
    }
    // The store transaction is already durable; losing the index leg
    // silently would diverge the index forever. Mark the storage so the
    // clean-shutdown checkpoint is suppressed and the next boot rebuilds
    // it from the store table.
    SDB_ERROR(SEARCH, "search index commit failed for index '", index_id.id(),
              "' at tick ", last_tick,
              "; the index will be rebuilt from the store on next boot");
    if (entry.storage) {
      entry.storage->MarkOutOfSync();
    }
  }

  _search_transactions.clear();
}

void Transaction::Commit() {
  // Search-table segments commit on the database WAL tick; register their flush
  // up-front -- before any commit point -- so a concurrent background
  // RefreshCommit waits for them. They commit in the WAL block below.
  if (_search_txn) {
    _search_txn->RegisterFlush();
  }

  // Inverted-index trxs: normally already settled inside the engine commit
  // (TransactionPreCheckpoint); this is the fallback for transactions that did
  // not commit the store database, so there is no store-WAL cursor to record.
  CommitSearch(std::nullopt);

  // Search-table (TableEngine::Search) commit point (WAL_DESIGN.md §9): the §9
  // crash boundaries + the single multi-shard WAL fsync that is the atomic
  // commit point live in SearchTableTransaction::Commit.
  if (_search_txn && !_search_txn->Empty()) {
    try {
      _search_txn->Commit();
    } catch (const std::exception& e) {
      _search_txn->Abort();
      Destroy();
      SDB_THROW(ERROR_INTERNAL,
                "Failed to commit search-table WAL: ", e.what());
    }
  }

  Destroy();
}

void Transaction::Rollback() {
  for (auto& search_transaction : _search_transactions) {
    search_transaction.second.transaction->Abort();
  }
  if (_search_txn) {
    _search_txn->Abort();
  }
  RollbackVariables();
  Destroy();
}

search::InvertedIndexSnapshotPtr Transaction::EnsureSearchSnapshot(
  ObjectId index_id) {
  auto it = _search_snapshots.find(index_id);
  if (it == _search_snapshots.end()) {
    auto index = CatalogSnapshot()->GetObject<catalog::InvertedIndex>(index_id);
    SDB_ASSERT(index);
    auto storage = index->GetData();
    SDB_ASSERT(storage);
    it =
      _search_snapshots.emplace(index_id, storage->GetInvertedIndexSnapshot())
        .first;
  }
  return it->second;
}

void Transaction::Destroy() noexcept {
  DropCatalogSnapshot();
  _search_transactions.clear();
  _search_snapshots.clear();
  _search_txn.reset();
  _num_log_data_markers = 0;
  _had_query_in_transaction = false;
  _had_dml = false;
  _statement_is_dml = false;
  _statement_is_ddl = false;
}

}  // namespace sdb::query
