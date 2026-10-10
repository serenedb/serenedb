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

#include <absl/functional/any_invocable.h>

#include <functional>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <optional>
#include <span>
#include <vector>
#include <yaclib/async/future.hpp>

#include "catalog/catalog.h"
#include "connector/duckdb_sink_writer_base.h"
#include "query/config.h"
#include "search/inverted_index_storage.h"
#include "search/search_table_transaction.h"

namespace sdb::catalog {

struct InvertedIndexConfig;

}  // namespace sdb::catalog
namespace sdb::query {

class Transaction : public Config {
 public:
  using Config::Config;

#ifdef SDB_DEV
  virtual ~Transaction() {
    SDB_ASSERT(_search_transactions.empty());
    SDB_ASSERT(!_search_txn || _search_txn->Empty());
  }
#endif

  // Per-statement snapshot lifecycle, driven by DuckDB's QueryBegin/QueryEnd.
  // See transaction.cpp for the model.
  void OnStatementBegin();
  void OnStatementEnd();

  // Pre-commit work that needs an active transaction (revert SET LOCAL for
  // custom-impl settings). Runs before the engine commit.
  // May throw: a hook that refuses the commit rolls the transaction back the
  // way a failed commit does (TransactionContext::Commit), which is what we
  // want if the buffered rows cannot be fed -- nothing has reached the WAL
  // yet, so the statement just fails.
  void PreCommit();
  // Pre-rollback counterpart -- restores all SET values.
  void PreRollback() noexcept { RollbackVariables(); }

  void StageSearch(const irs::SourcePosition& position,
                   duckdb::idx_t database) noexcept;
  void PublishSearch(duckdb::idx_t database) noexcept;

  void Commit();

  void Rollback();

  void DeferToCommit(absl::AnyInvocable<void()> action) {
    _on_commit.push_back(std::move(action));
  }

  void AddCreatedIndex(duckdb::idx_t database,
                       std::shared_ptr<search::InvertedIndexStorage> storage) {
    _created_indexes.emplace_back(database, std::move(storage));
  }
  void RefreshCreatedIndexes(duckdb::idx_t database);

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

  // Drops the pinned search reader for one shard, so the segments it
  // references stop being held. Refuses while the transaction's view has to
  // stay frozen -- REPEATABLE READ, or any uncommitted DML, whose rows are
  // tied to the view they were written through. Returns whether it dropped.
  bool TryDropSearchReader(duckdb::idx_t shard_id);

  void Destroy() noexcept;

  struct SearchSlot {
    std::unique_ptr<irs::IndexWriter::Transaction> transaction;
    std::unique_ptr<connector::DuckDBSinkIndexWriter> writer;
  };

  SearchSlot& EnsureIndexSlot(
    duckdb::idx_t index_id,
    std::shared_ptr<search::InvertedIndexStorage> storage,
    std::shared_ptr<const catalog::InvertedIndexConfig> config,
    size_t slot = 0);

  irs::IndexWriter::Transaction& EnsureIndexTransaction(
    duckdb::idx_t index_id,
    std::shared_ptr<search::InvertedIndexStorage> storage,
    std::shared_ptr<const catalog::InvertedIndexConfig> config) {
    return *EnsureIndexSlot(index_id, std::move(storage), std::move(config))
              .transaction;
  }

  std::span<SearchSlot> IndexSlots(duckdb::idx_t index_id) {
    const auto it = _search_transactions.find(index_id);
    if (it == _search_transactions.end()) {
      return {};
    }
    return it->second.slots;
  }

  void RegisterIndexFlush(duckdb::idx_t index_id) noexcept {
    for (auto& slot : IndexSlots(index_id)) {
      if (slot.transaction) {
        slot.transaction->RegisterFlush();
      }
    }
  }

  const duckdb::Vector& FeedColumn(const void* database,
                                   duckdb::idx_t table_oid,
                                   duckdb::row_t first_row, duckdb::idx_t count,
                                   duckdb::idx_t column,
                                   const duckdb::Vector& source);

 private:
  struct FeedColumns {
    const void* database = nullptr;
    duckdb::idx_t table_oid = 0;
    duckdb::row_t first_row = 0;
    duckdb::idx_t count = 0;
    std::vector<std::pair<duckdb::idx_t, duckdb::Vector>> columns;
  };

  // The cases a single snapshot serves a whole transaction: an explicit
  // REPEATABLE READ transaction, or any transaction that has performed
  // uncommitted DML. Everything else refreshes per statement.
  bool IsStableSnapshot() const;

  struct SearchTransaction {
    std::vector<SearchSlot> slots;
    std::shared_ptr<search::InvertedIndexStorage> storage;
    uint64_t tick = 0;
    irs::SourcePosition position;
  };

  irs::containers::FlatHashMap<duckdb::idx_t, SearchTransaction>
    _search_transactions;
  irs::containers::FlatHashMap<duckdb::idx_t, search::InvertedIndexSnapshotPtr>
    _search_snapshots;
  FeedColumns _feed_columns;
  // All search-table (TableEngine::Search) state + WAL commit logic. Engaged
  // lazily via SearchTxn(); reset in Destroy. The inverted-index trxs above
  // commit on the store-table tick, not the engine WAL tick.
  std::optional<search::SearchTableTransaction> _search_txn;
  std::vector<absl::AnyInvocable<void()>> _on_commit;
  std::vector<
    std::pair<duckdb::idx_t, std::shared_ptr<search::InvertedIndexStorage>>>
    _created_indexes;
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
