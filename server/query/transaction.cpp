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

#include <chrono>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <random>
#include <thread>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/duckdb_engine.h"
#include "basics/log.h"
#include "catalog1/catalog.h"
#include "catalog1/entry/inverted_index.h"
#include "pg/sql_exception_macro.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"
#include "search/tick_domain.h"

namespace sdb::query {

// Snapshot lifecycle across statements and transactions.
//
// A query reads through two views:
//   * duckdb's own read view -- MVCC over table storage AND over the catalog,
//     which is the same transaction's view of both: an entry is a version in a
//     CatalogSet, committed with the rows it describes;
//   * search snapshots -- iresearch MVCC readers over index data.
//
// Only READ COMMITTED and REPEATABLE READ exist. Both views follow isolation
// (IsStableSnapshot): an explicit REPEATABLE READ transaction, OR any
// transaction that has performed uncommitted DML, holds one frozen snapshot for
// the rest of its life, so another session's committed DML stays invisible;
// everywhere else (autocommit, or a DML-less READ COMMITTED transaction) the
// data refreshes per statement. Commit/rollback releases everything
// (Commit/Rollback -> Destroy).
//
// Freezing once a transaction has written is a safety requirement, not just a
// REPEATABLE READ nicety: the uncommitted rows are tied to the read view they
// were written through, so another session's committed DML would otherwise
// drift them.
//
// The catalog needs nothing of its own here -- no snapshot to acquire, none to
// release. What a statement re-plans against is duckdb's version of the catalog
// it read, sampled at the statement boundaries below because duckdb asserts
// that two reads of it inside one statement agree. DuckDB's ModifiedDatabase()
// cannot tell DML from DDL -- it is set for both -- so DML is classified from
// the statement itself (MarkStatementDml) and DDL never pins anything.
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
  // tied to this read view, so re-acquiring it would let them drift.
  return _had_dml;
}

void Transaction::OnStatementBegin() {
  // Fixed for the statement: duckdb asserts that every read of a catalog's
  // identity inside one statement agrees, and a commit moves it.
  RefreshCatalogEpoch();
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
  // Resampled here so the next Bind -- which runs between statements -- sees
  // the DDL this one performed and re-plans against it.
  RefreshCatalogEpoch();
  if (_statement_is_dml) {
    _had_dml = true;
  }
  _statement_is_dml = false;

  if (_had_dml) {
    // Uncommitted DML pins the read view and the search readers for the rest
    // of the transaction: read-your-writes, plus the safety freeze -- our
    // pending rows are tied to this read view. Commit/Rollback -> Destroy
    // releases both.
    return;
  }
  if (!IsStableSnapshot()) {
    // READ COMMITTED / autocommit, no DML: refresh everything per statement so
    // the next statement sees the latest committed catalog and data. Drop
    // search readers so background compaction is not pinned. All re-acquire
    // lazily; the native read view advances at the next statement's
    // OnStatementBegin.
    _search_snapshots.clear();
    // Search-table reads go through SearchTxn()'s reader cache, not the
    // _search_snapshots above; reset on the same (non-pinned) boundary.
    if (_search_txn) {
      _search_txn->ResetReaders();
    }
    return;
  }
  // Explicit REPEATABLE READ, no DML: the native read view and search readers
  // stay frozen for the transaction's life, and the catalog entries follow the
  // same view, so there is nothing to drop here.
}

void Transaction::PreCommit() noexcept {
  // Revert SET LOCAL overlays (and clear the txn map) while the DuckDB
  // transaction is still active so custom-impl settings (search_path,
  // transaction_isolation) can use their normal set_local path (which may
  // do catalog lookups).
  CommitVariables();
}

void Transaction::PreRollback() noexcept { RollbackVariables(); }

irs::IndexWriter::Transaction& Transaction::EnsureIndexTransaction(
  duckdb::idx_t index_id, std::shared_ptr<search::InvertedIndexStorage> storage,
  std::shared_ptr<const catalog::InvertedIndexConfig> config) {
  SDB_ASSERT(storage);
  auto& entry = _search_transactions.try_emplace(index_id).first->second;
  if (!entry.transaction) {
    entry.transaction = std::make_unique<irs::IndexWriter::Transaction>(
      storage->GetTransaction());
    entry.transaction->SetFieldOptions(std::move(config));
    entry.storage = std::move(storage);
  }
  return *entry.transaction;
}

void Transaction::CommitSearch(
  std::optional<search::WalCursor> cursor) noexcept {
  if (_search_transactions.empty()) {
    return;
  }
  absl::Cleanup rollback = [&] { _search_transactions.clear(); };

  // Pin every staged segment onto the flush context before the tick exists.
  // Pinning must precede Advance -- otherwise a refresh whose tick snapshot
  // lands in between could advance its committed tick past an unpinned segment
  // (lost insert / FlushPending assert). The widest query count sizes the
  // reserved band so every writer's first_tick stays strictly above the tick
  // it last committed at.
  uint64_t max_queries = 0;
  for (auto& [index_id, entry] : _search_transactions) {
    entry.transaction->RegisterFlush();
    max_queries =
      std::max<uint64_t>(max_queries, entry.transaction->GetQueries());
  }
  SDB_IF_FAILURE("long_waited_advance") {
    static std::atomic<uint32_t> gSeedCounter{0};
    static thread_local std::mt19937 gRng{
      gSeedCounter.fetch_add(1, std::memory_order_relaxed)};
    std::this_thread::sleep_for(std::chrono::microseconds(
      std::uniform_int_distribution<int>(0, 20000)(gRng)));
  }

  const auto last_tick =
    search::TickDomain::Instance().Advance(max_queries + 1);

  std::move(rollback).Cancel();

  // Each index records this commit's WAL cursor into its own table before its
  // segment becomes flushable, then commits at the tick. The cursor is this
  // commit's exact WAL position, captured under the WAL lock by the engine:
  // commits overlap, so reading the WAL size here would include later
  // transactions' bytes and over-claim (skipping their re-stream after a
  // crash).
  for (auto& [index_id, entry] : _search_transactions) {
    if (cursor) {
      entry.storage->RecordFlushCursor(last_tick, *cursor);
    }
    if (entry.transaction->Commit(last_tick)) {
      continue;
    }
    SDB_ERROR(SEARCH, "search index commit failed for index '", index_id,
              "' at tick ", last_tick,
              "; the index will be rebuilt from the store on next boot");
    entry.storage->MarkOutOfSync();
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
      THROW_SQL_ERROR(ERR_MSG("Failed to commit search-table WAL: ", e.what()));
    }
  }

  Destroy();
}

void Transaction::Rollback() {
  if (_search_txn) {
    _search_txn->Abort();
  }
  RollbackVariables();
  Destroy();
}

search::InvertedIndexSnapshotPtr Transaction::EnsureSearchSnapshot(
  duckdb::idx_t index_id,
  const std::shared_ptr<search::InvertedIndexStorage>& storage) {
  auto it = _search_snapshots.find(index_id);
  if (it == _search_snapshots.end()) {
    SDB_ASSERT(storage);
    it =
      _search_snapshots.emplace(index_id, storage->GetInvertedIndexSnapshot())
        .first;
  }
  return it->second;
}

void Transaction::Destroy() noexcept {
  _search_transactions.clear();
  _search_snapshots.clear();
  _search_txn.reset();
  _num_log_data_markers = 0;
  _had_query_in_transaction = false;
  _had_dml = false;
  _statement_is_dml = false;
}

}  // namespace sdb::query
