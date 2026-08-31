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

#pragma once

#include <functional>
#include <optional>
#include <yaclib/async/future.hpp>

#include "basics/containers/flat_hash_map.h"
#include "catalog1/catalog.h"
#include "query/config.h"
#include "search/inverted_index_storage.h"
#include "search/search_table_transaction.h"

namespace sdb::connector {

struct InvertedFeedSession;

}  // namespace sdb::connector
namespace sdb::query {

class Transaction : public Config {
 public:
  using Config::Config;

#ifdef SDB_DEV
  virtual ~Transaction() {
    // Search transactions have implicit commit in destructor (historical
    // reasons) So if we get here explicit Commit/Rollback should be already
    // called. Otherwise we might have some unexpected data
    SDB_ASSERT(_search_feeds.empty());
    SDB_ASSERT(!_search_txn || _search_txn->Empty());
  }
#endif

  // Per-statement snapshot lifecycle, driven by DuckDB's QueryBegin/QueryEnd.
  // See transaction.cpp for the model.
  void OnStatementBegin();
  void OnStatementEnd();

  // Pre-commit work that needs an active transaction (revert SET LOCAL for
  // custom-impl settings). Runs before the engine commit.
  void PreCommit() noexcept;
  // Pre-rollback counterpart -- restores all SET values.
  void PreRollback() noexcept;

  // Commit the search-index leg synchronously with the store table changes:
  // called by the engine from its TransactionPreCheckpoint hook, on the
  // committing thread while it still holds the transaction lock and the WAL
  // append ordering, right after this commit's WAL flush marker is written --
  // so across connections ticks are handed out strictly in WAL-append order
  // over complete batches and recovery cursors stay monotonic with WAL offsets,
  // even though the group fsyncs (and thus the durable acknowledgements)
  // complete afterwards and out of order. Everything here is memory-only; the
  // background refresh gates its durable cursor on the WAL becoming durable, so
  // a batch is never persisted before its store bytes are. The cursor is this
  // commit's exact store-WAL position; std::nullopt on the fallback path where
  // the transaction did not commit the store database, in which case no
  // recovery cursor is recorded. Idempotent -- a no-op once the staged
  // transactions have been committed (or when there were none).
  void CommitSearch(std::optional<search::WalCursor> cursor) noexcept;

  void Commit();

  void Rollback();

  // True once any statement that reads or writes the current database ran
  // inside the active explicit transaction; gates late SET TRANSACTION
  // ISOLATION LEVEL changes.
  bool HadQueryInTransaction() const noexcept {
    return _had_query_in_transaction;
  }
  void MarkQueryInTransaction() noexcept { _had_query_in_transaction = true; }

  // Mark the in-flight statement as genuine data modification (INSERT/UPDATE/
  // DELETE/COPY FROM/...). Set before the statement runs; folded into the
  // transaction's DML state at OnStatementEnd, where it pins the snapshots.
  // Atomic DDL also reports a modified database but must NOT be marked -- a
  // later statement has to observe the catalog it changed.
  void MarkStatementDml() noexcept { _statement_is_dml = true; }

  // One search snapshot per index per statement, keyed by the index's id. The
  // definition is handed in rather than looked up: the caller is the scan
  // The storage is handed in rather than read off the definition: an open
  // directory is the object's, not something a version of it describes.
  search::InvertedIndexSnapshotPtr EnsureSearchSnapshot(
    duckdb::idx_t index_id,
    const std::shared_ptr<search::InvertedIndexStorage>& storage);

  // Lazily-created search-table (TableEngine::Search) transaction state +
  // commit logic. Engaged on the first search-table write/scan; query::
  // Transaction just delegates RegisterFlush/Commit/Abort to it (see Commit /
  // Rollback). The operator and scan reach the per-shard mutators through here.
  search::SearchTableTransaction& SearchTxn() {
    if (!_search_txn) {
      _search_txn.emplace();
    }
    return *_search_txn;
  }

  void Destroy() noexcept;

  // Register the per-index feed the first time it engages this commit
  // (idempotent). `feed` is the connector-side session, non-owning (it
  // outlives the commit).
  // The session this commit is already feeding for `index_id`, or null. What it
  // answers is what the next chunk of the same commit has to go through: the
  // segments staged so far belong to it, and a session built in its place would
  // leave them to no commit at all.
  std::shared_ptr<connector::InvertedFeedSession> InvertedFeed(
    duckdb::idx_t index_id) const {
    const auto it = _search_feeds.find(index_id);
    return it == _search_feeds.end() ? nullptr : it->second;
  }

  void EngageInvertedFeed(
    duckdb::idx_t index_id,
    std::shared_ptr<connector::InvertedFeedSession> feed) {
    auto& slot = _search_feeds[index_id];
    // One session per index per commit. A second, different session for the
    // same id would displace the first with its segments already registered
    // for flush and never committed or aborted -- the index's flush context
    // then never drains and every later refresh blocks on it.
    SDB_ASSERT(!slot || slot == feed);
    slot = std::move(feed);
  }

 private:
  // The cases a single snapshot serves a whole transaction: an explicit
  // REPEATABLE READ transaction, or any transaction that has performed
  // uncommitted DML. Everything else refreshes per statement.
  bool IsStableSnapshot() const;

  // Out of line: the session is only forward-declared here.
  void AbortInvertedFeeds() noexcept;

  // The inverted-index feeds this transaction wrote through. Every staged
  // segment -- the workers' and the committing thread's -- lives in there, so
  // the transaction only has to drive prepare/commit/abort. Shared with the
  // bound index rather than borrowed: DROP INDEX destroys the index without
  // waiting for a commit that has already engaged its feed.
  containers::FlatHashMap<duckdb::idx_t,
                          std::shared_ptr<connector::InvertedFeedSession>>
    _search_feeds;
  containers::FlatHashMap<duckdb::idx_t, search::InvertedIndexSnapshotPtr>
    _search_snapshots;
  // All search-table (TableEngine::Search) state + WAL commit logic. Engaged
  // lazily via SearchTxn(); reset in Destroy. Separate from the feeds above:
  // those commit on the store-table tick, not the engine WAL tick.
  std::optional<search::SearchTableTransaction> _search_txn;
  uint64_t _num_log_data_markers = 0;
  bool _had_query_in_transaction = false;
  // Set once a statement has performed uncommitted DML; pins all three views
  // for the rest of the transaction. Cleared at commit/rollback.
  bool _had_dml = false;
  // Whether the in-flight statement modifies data; folded into _had_dml at
  // OnStatementEnd. Never spans a statement boundary.
  bool _statement_is_dml = false;
};

}  // namespace sdb::query
