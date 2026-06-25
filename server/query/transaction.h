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

#include <iresearch/index/index_writer.hpp>
#include <yaclib/async/future.hpp>

#include "basics/containers/flat_hash_map.h"
#include "basics/down_cast.h"
#include "basics/result.h"
#include "catalog/catalog.h"
#include "query/config.h"
#include "search/inverted_index_storage.h"

namespace sdb::query {

class Transaction : public Config {
 public:
  using Config::Config;

#ifdef SDB_DEV
  virtual ~Transaction() {
    // Search transactions have implicit commit in destructor (historical
    // reasons) So if we get here explicit Commit/Rollback should be already
    // called. Otherwise we might have some unexpected data
    SDB_ASSERT(_search_transactions.empty());
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
  // called inside the engine commit, after store durability but before the
  // in-commit checkpoint, so the checkpoint's force-refresh never waits on an
  // un-committed in-flight batch. Idempotent -- a no-op once the staged
  // transactions have been committed (or when there were none).
  void CommitSearch() noexcept;

  Result Commit();

  Result Rollback();

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

  // Mark the in-flight statement as catalog DDL (CREATE/DROP/ALTER/...).
  // serenedb DDL is atomic and non-transactional, so at OnStatementEnd it drops
  // the catalog snapshot even under REPEATABLE READ (when no uncommitted DML is
  // held), making the change visible to the next statement.
  void MarkStatementDdl() noexcept { _statement_is_ddl = true; }

  search::InvertedIndexSnapshotPtr EnsureSearchSnapshot(ObjectId index_id);

  void EraseSearchTransaction(ObjectId index_id) noexcept {
    _search_transactions.erase(index_id);
  }

  // Pin every staged search transaction into the iresearch flush context so a
  // concurrent refresh waits for it to settle before committing on tick. Call
  // at feed time (after staging this batch, BEFORE the store WAL bytes are
  // written) so the refresh's WAL-offset durable cursor never claims a
  // transaction whose iresearch leg has not been flushed. RegisterFlush is a
  // no-op until an active segment exists (docs staged) and idempotent after,
  // so registering all transactions each feed is safe and cheap.
  void RegisterSearchFlush() noexcept {
    for (auto& [index_id, entry] : _search_transactions) {
      if (entry.transaction) {
        entry.transaction->RegisterFlush();
      }
    }
  }

  void Destroy() noexcept;

  template<typename Visit, typename Filter = std::nullptr_t>
  void EnsureIndexesTransactions(ObjectId table_id, Visit&& visit,
                                 Filter&& filter = nullptr) {
    auto snapshot = EnsureCatalogSnapshot();
    SDB_ASSERT(snapshot->GetObject(table_id)->GetType() ==
               catalog::ObjectType::Table);

    for (auto& index : snapshot->GetIndexesByRelation(table_id)) {
      SDB_ASSERT(index);

      if constexpr (!std::is_same_v<std::decay_t<Filter>, std::nullptr_t>) {
        const auto& referenced = index->GetReferencedColumns();
        if (!filter(referenced)) {
          continue;
        }
      }

      if (index->GetType() != catalog::ObjectType::InvertedIndex) {
        // Secondary indexes are native ART on the store table; nothing to
        // feed here.
        continue;
      }
      auto inverted = basics::downCast<const catalog::InvertedIndex>(index);
      auto storage = inverted->GetData();
      SDB_ASSERT(storage);
      auto& entry =
        _search_transactions.try_emplace(index->GetId()).first->second;
      if (!entry.transaction) {
        entry.transaction = std::make_unique<irs::IndexWriter::Transaction>(
          storage->GetTransaction());
        // Keep the storage alive and reachable for Commit() without a catalog
        // re-lookup.
        entry.storage = storage;
        // Encode this transaction's rows against the InvertedIndex from its own
        // DDL snapshot (the index IS the per-column options); co-owned via the
        // catalog snapshot this transaction holds, so the segment writer can
        // pin it until flush without a live-catalog lookup.
        entry.transaction->SetFieldOptions(std::move(inverted));
      }
      visit(*entry.transaction, *index);
    }
  }

 private:
  // The cases a single snapshot serves a whole transaction: an explicit
  // REPEATABLE READ transaction, or any transaction that has performed
  // uncommitted DML. Everything else refreshes per statement.
  bool IsStableSnapshot() const;

  struct SearchTransaction {
    std::unique_ptr<irs::IndexWriter::Transaction> transaction;
    std::shared_ptr<search::InvertedIndexStorage> storage;
  };

  containers::FlatHashMap<ObjectId, SearchTransaction> _search_transactions;
  containers::FlatHashMap<ObjectId, search::InvertedIndexSnapshotPtr>
    _search_snapshots;
  bool _had_query_in_transaction = false;
  // Set once a statement has performed uncommitted DML; pins all three views
  // for the rest of the transaction. Cleared at commit/rollback.
  bool _had_dml = false;
  // Whether the in-flight statement modifies data; folded into _had_dml at
  // OnStatementEnd. Never spans a statement boundary.
  bool _statement_is_dml = false;
  // Whether the in-flight statement is catalog DDL; consumed at OnStatementEnd
  // to force a catalog refresh. Never spans a statement boundary.
  bool _statement_is_ddl = false;
};

}  // namespace sdb::query
