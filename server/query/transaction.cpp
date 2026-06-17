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
#include "search/tick_domain.h"

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
    search_transaction.second.transaction->RegisterFlush();
  }
  absl::Cleanup rollback = [&] {
    for (auto& search_transaction : _search_transactions) {
      search_transaction.second.transaction->Abort();
    }
    _search_transactions.clear();
  };

  uint64_t max_queries = 0;
  for (const auto& [id, entry] : _search_transactions) {
    max_queries =
      std::max<uint64_t>(max_queries, entry.transaction->GetQueries());
  }
  const auto last_tick =
    search::TickDomain::Instance().Advance(max_queries + 1);

  std::move(rollback).Cancel();

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
    if (entry.storage && has_cursor) {
      entry.storage->RecordFlushCursor(last_tick, cursor);
    }
    if (entry.transaction->Commit(last_tick)) {
      continue;
    }
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
  CommitSearch();
  Destroy();

  return {};
}

Result Transaction::Rollback() {
  for (auto& search_transaction : _search_transactions) {
    search_transaction.second.transaction->Abort();
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
  _had_query_in_transaction = false;
}

}  // namespace sdb::query
