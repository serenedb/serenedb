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
#include <duckdb/transaction/meta_transaction.hpp>

#include "basics/assert.h"
#include "catalog/catalog.h"
#include "search/tick_domain.h"
#include "storage_engine/table_shard.h"

namespace sdb::query {

void Transaction::OnNewStatement() {
  auto level = GetIsolationLevel();
  if (level == IsolationLevel::READ_COMMITTED) {
    DropCatalogSnapshot();
    _search_snapshots.clear();
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

Result Transaction::Commit() {
  if (!_search_transactions.empty()) {
    for (auto& search_transaction : _search_transactions) {
      // Tie the iresearch transaction's active segment to the current flush
      // context so a background index commit that starts now waits for this
      // transaction to settle before committing "on tick".
      search_transaction.second->RegisterFlush();
    }
    absl::Cleanup rollback = [&] {
      for (auto& search_transaction : _search_transactions) {
        search_transaction.second->Abort();
      }
      Destroy();
    };

    // Reserve a tick range covering every writer's query (Remove/Replace)
    // count, so that per writer first_tick = last_tick - queries stays
    // strictly above its previously committed tick.
    uint64_t max_queries = 0;
    for (const auto& [id, trx] : _search_transactions) {
      max_queries = std::max<uint64_t>(max_queries, trx->GetQueries());
    }
    const auto last_tick =
      search::TickDomain::Instance().Advance(max_queries + 1);

    std::move(rollback).Cancel();

    for (auto& search_transaction : _search_transactions) {
      search_transaction.second->Commit(last_tick);
    }
  }
  ApplyTableStatsDiffs();
  Destroy();

  return {};
}

Result Transaction::Rollback() {
  for (auto& search_transaction : _search_transactions) {
    search_transaction.second->Abort();
  }
  RollbackVariables();
  Destroy();
  return {};
}

search::InvertedIndexSnapshotPtr Transaction::EnsureSearchSnapshot(
  ObjectId index_id) {
  auto it = _search_snapshots.find(index_id);
  if (it == _search_snapshots.end()) {
    auto index_shard = EnsureCatalogSnapshot()->GetIndexShard(index_id);
    SDB_ASSERT(index_shard);
    SDB_ASSERT(index_shard->GetType() ==
               catalog::ObjectType::InvertedIndexShard);
    auto& inverted_index_shard =
      basics::downCast<search::InvertedIndexShard>(*index_shard.get());
    it = _search_snapshots
           .emplace(index_id, inverted_index_shard.GetInvertedIndexSnapshot())
           .first;
  }
  return it->second;
}

void Transaction::Destroy() noexcept {
  DropCatalogSnapshot();
  _search_transactions.clear();
  _table_rows_deltas.clear();
  _search_snapshots.clear();
  _had_query_in_transaction = false;
}

catalog::TableStats Transaction::GetTableStats(ObjectId table_id) const {
  // TODO(codeworse): manage catalog snapshot in transaction
  auto table_shard = EnsureCatalogSnapshot()->GetTableShard(table_id);
  if (!table_shard) {
    SDB_THROW(ERROR_BAD_PARAMETER,
              "Table shard not found for table id: ", table_id);
  }
  return table_shard->GetTableStats();
}

void Transaction::ApplyTableStatsDiffs() noexcept {
  if (_table_rows_deltas.empty()) {
    return;
  }
  auto snapshot = EnsureCatalogSnapshot();
  for (const auto& [table_id, delta] : _table_rows_deltas) {
    // It's possible that while transaction was active, table got dropped.
    if (!snapshot->GetObject(table_id)) {
      continue;
    }
    auto table_shard = snapshot->GetTableShard(table_id);
    SDB_ASSERT(table_shard);
    if (table_shard) {
      table_shard->UpdateNumRows(delta);
    }
  }
  _table_rows_deltas.clear();
}

}  // namespace sdb::query
