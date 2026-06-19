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
#include "basics/log.h"
#include "catalog/catalog.h"
#include "catalog/store/store.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"
#include "search/tick_domain.h"

namespace sdb::query {

void Transaction::OnNewStatement() {
  auto level = GetIsolationLevel();
  if (level == IsolationLevel::READ_COMMITTED) {
    DropCatalogSnapshot();
    _search_snapshots.clear();
    if (_search_txn) {
      _search_txn->ResetReaders();
    }
  }
}

void Transaction::RefreshReadCommittedSnapshot() {
  if (GetIsolationLevel() != IsolationLevel::READ_COMMITTED) {
    return;
  }
  // Statement-level snapshots over native storage: while the explicit
  // transaction has made no writes, move its read visibility forward so
  // the next statement sees other transactions' committed changes.
  auto& context = GetClientContext();
  auto& txn_ctx = context.transaction;
  if (txn_ctx.HasActiveTransaction() && !txn_ctx.IsAutoCommit() &&
      !txn_ctx.ActiveTransaction().ModifiedDatabase()) {
    txn_ctx.ActiveTransaction().RefreshStartTime();
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

void Transaction::CommitSearch() noexcept {
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

  // This commit's exact store-WAL end offset, with the checkpoint iteration as
  // the generation. We run after the store WAL is durable (inside the engine
  // commit, before the in-commit checkpoint), so GetWALSize() is this
  // transaction's WAL end offset and is constant throughout CommitSearch; read
  // it once. Commits serialize, so ticks and offsets arrive in the same order.
  search::WalCursor cursor;
  bool has_cursor = false;
  if (auto store =
        duckdb::DatabaseManager::Get(DuckDBEngine::Instance().instance())
          .GetDatabase(std::string{catalog::kStoreDatabaseName})) {
    auto& sm = store->GetStorageManager();
    cursor = search::WalCursor{sm.GetBlockManager().GetCheckpointIteration(),
                               sm.GetWALSize()};
    has_cursor = true;
  }

  for (auto& [index_id, entry] : _search_transactions) {
    // Record this commit's WAL cursor into the index's own table BEFORE its
    // segment becomes flushable (Commit below emplaces it into the flush
    // context). A concurrent refresh can only flush this batch after Commit, by
    // which point the offset is already in the table, so the refresh's
    // CursorAtOrBelow(flushed_tick) can never under-claim and re-stream an
    // already-durable insert after a crash.
    if (entry.storage && has_cursor) {
      entry.storage->RecordFlushCursor(last_tick, cursor);
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

Result Transaction::Commit() {
  // Search-table segments commit on the database WAL tick; register their flush
  // up-front -- before any commit point -- so a concurrent background
  // RefreshCommit waits for them. They commit in the WAL block below.
  if (_search_txn) {
    _search_txn->RegisterFlush();
  }

  // Inverted-index trxs: normally already settled inside the engine commit
  // (TransactionPreCheckpoint); this is the fallback for transactions that did
  // not commit the store database.
  CommitSearch();

  // Search-table (TableEngine::Fast) commit point (WAL_DESIGN.md §9): the §9
  // crash boundaries + the single multi-shard WAL fsync that is the atomic
  // commit point live in SearchTableTransaction::Commit.
  if (_search_txn && !_search_txn->Empty()) {
    try {
      _search_txn->Commit();
    } catch (const std::exception& e) {
      _search_txn->Abort();
      Destroy();
      return {ERROR_INTERNAL, "Failed to commit search-table WAL: ", e.what()};
    }
  }

  ApplyTableStatsDiffs();
  Destroy();

  return {};
}

Result Transaction::Rollback() {
  for (auto& search_transaction : _search_transactions) {
    search_transaction.second.transaction->Abort();
  }
  if (_search_txn) {
    _search_txn->Abort();
  }
  RollbackVariables();
  Destroy();
  return {};
}

search::InvertedIndexSnapshotPtr Transaction::EnsureSearchSnapshot(
  ObjectId index_id) {
  auto it = _search_snapshots.find(index_id);
  if (it == _search_snapshots.end()) {
    auto index =
      EnsureCatalogSnapshot()->GetObject<catalog::InvertedIndex>(index_id);
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
}

void Transaction::ApplyTableStatsDiffs() noexcept {
  if (_table_rows_deltas.empty()) {
    return;
  }
  auto snapshot = EnsureCatalogSnapshot();
  for (const auto& [table_id, delta] : _table_rows_deltas) {
    auto table = snapshot->GetObject<catalog::Table>(table_id);
    if (!table) {
      continue;  // table dropped mid-transaction
    }
    // Only Fast tables track row counts here; Transactional store tables
    // maintain their own statistics.
    if (table->GetEngine() == catalog::TableEngine::Fast) {
      if (const auto& search = table->GetData()) {
        search->UpdateNumRows(delta);
      }
    }
  }
  _table_rows_deltas.clear();
}

}  // namespace sdb::query
