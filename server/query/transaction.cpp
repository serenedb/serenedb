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

#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/main/client_context.hpp>

#include "basics/assert.h"
#include "catalog/catalog.h"
#include "search/search_table_shard.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/table_shard.h"

namespace sdb::query {

void Transaction::OnNewStatement() {
  auto level = GetIsolationLevel();
  if (level == IsolationLevel::READ_COMMITTED) {
    DropCatalogSnapshot();
    _rocksdb_snapshot = nullptr;
    _search_snapshots.clear();
    _search_table_readers.clear();
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
  uint64_t num_ops = _rocksdb_transaction
                       ? _rocksdb_transaction->GetNumPuts() +
                           _rocksdb_transaction->GetNumDeletes()
                       : 0;
  SDB_ASSERT(!_rocksdb_transaction || _rocksdb_transaction->GetNumMerges() == 0,
             "We do not expect merges for now");
  // Marker-only txns have zero rocksdb ops and would skip the commit block;
  // force a no-op Delete to consume a seq number so markers reach the WAL
  // and the inverted index has a tick to commit on. (CTAS search tables and
  // the inverted-index path still emit rocksdb markers; plain search-table
  // INSERT/COPY no longer does -- it commits via the self-contained WAL
  // below, so a pure plain search INSERT legitimately has num_ops == 0.)
  if (num_ops == 0 && _num_log_data_markers > 0 && _rocksdb_transaction) {
    _rocksdb_transaction->Delete(rocksdb::Slice{});
    ++num_ops;
  }

  // Search-table segments commit on the database WAL tick (not the rocksdb
  // seq), so register their flush up-front -- before any commit point and
  // regardless of whether rocksdb commits -- so a concurrent background
  // RefreshCommit waits for them. They are committed in the WAL block at the
  // end.
  for (auto& [table_id, w] : _search_table_writes) {
    for (auto& trx : w.transactions) {
      trx->RegisterFlush();
    }
  }

  if (num_ops > 0) [[likely]] {
    for (auto& search_transaction : _search_transactions) {
      // tie iresearch transaction's active segment to current flush context in
      // writer and let IndexWriter know that he need to wait for this
      // transaction to settle before proceeding with commit. That is important
      // as we are committing "on tick" and must ensure that if we mark this
      // transaction with tick (see below commit calls) writer will not commit
      // without it. This could happend if background index commit will start
      // AFTER RocksDB commit but before transaction commit. But as we told
      // index writer to wait, we are safe.
      search_transaction.second->RegisterFlush();
    }
    absl::Cleanup rollback = [&] {
      for (auto& search_transaction : _search_transactions) {
        search_transaction.second->Abort();
      }
      for (auto& [table_id, w] : _search_table_writes) {
        for (auto& trx : w.transactions) {
          trx->Abort();
        }
      }
      // PreCommit already ran CommitVariables, which cleared the txn map
      // after restoring SET LOCAL overlays. Nothing left to roll back here
      // on rocksdb commit failure -- plain-SET values have already been
      // accepted as committed.
      Destroy();
    };

    // When updating non-PK columns with a search index, the search engine
    // Remove consumes a tick from the same range as rocksdb seq numbers.
    // With num_ops == _queries, first_tick == committed_tick which violates
    // the strict ordering invariant. Add an extra Delete to bump seq by 1.
    for (const auto& [id, trx] : _search_transactions) {
      SDB_ASSERT(trx->GetQueries() <= num_ops);
      if (trx->GetQueries() == num_ops) {
        SDB_ASSERT(trx->GetQueries() != 0);
        // TODO: I'm not sure in what column family we should write
        _rocksdb_transaction->Delete(rocksdb::Slice{});
        ++num_ops;
        break;
      }
    }
    SDB_ASSERT(absl::c_all_of(_search_transactions, [&](const auto& p) {
      return p.second->GetQueries() < num_ops;
    }));

    SDB_IF_FAILURE("crash_before_rocksdb_commit") { SDB_IMMEDIATE_ABORT(); }
    auto status = _rocksdb_transaction->Commit();
    SDB_IF_FAILURE("crash_after_rocksdb_commit") { SDB_IMMEDIATE_ABORT(); }

    if (!status.ok()) {
      return {ERROR_INTERNAL,
              "Failed to commit RocksDB transaction: ", status.ToString()};
    }

    // id is first write operation seqno in the WAL
    auto post_commit_seq = _rocksdb_transaction->GetId();
    // add number of operations to get last operation seqno
    post_commit_seq += num_ops - 1;

    std::move(rollback).Cancel();

    for (auto& search_transaction : _search_transactions) {
      search_transaction.second->Commit(post_commit_seq);
    }
  }

  // Search-table INSERT/COPY commit point (WAL_DESIGN.md §9). Runs even when
  // num_ops == 0 (a pure search INSERT touches no rocksdb data): append ONE
  // central record covering EVERY search shard the txn wrote, then stamp each
  // shard's per-thread segments with the tick it returns. The chunk files were
  // already fsynced at Combine; AppendCommit's single fsync is the commit point
  // and the multi-shard atomic unit.
  if (!_search_table_writes.empty()) {
    // Insert-only today: zero removes, so all segments share first_tick and one
    // RefreshCommit publishes them (deletes/updates -> §5.5, later).
    SDB_ASSERT(absl::c_all_of(_search_table_writes, [](const auto& e) {
      return absl::c_all_of(e.second.transactions,
                            [](const auto& t) { return t->GetQueries() == 0; });
    }));
    uint64_t tick = 0;
    try {
      tick = CommitSearchTableWal();
    } catch (const std::exception& e) {
      for (auto& [table_id, w] : _search_table_writes) {
        for (auto& trx : w.transactions) {
          trx->Abort();
        }
      }
      Destroy();
      return {ERROR_INTERNAL, "Failed to commit search-table WAL: ", e.what()};
    }
    for (auto& [table_id, w] : _search_table_writes) {
      for (auto& trx : w.transactions) {
        trx->Commit(tick);
      }
    }
  }

  ApplyTableStatsDiffs();
  Destroy();

  return {};
}

uint64_t Transaction::CommitSearchTableWal() {
  SDB_ASSERT(!_search_table_writes.empty());
  // Build one per-shard section for each search table this txn wrote, then
  // append ONE central record across all of them (multi-shard atomicity, §9).
  // All shards are in the same database (the txn scope), so they share one db
  // WAL.
  search::SearchDbWal* wal = nullptr;
  std::vector<search::SearchDbWal::ShardSection> sections;
  sections.reserve(_search_table_writes.size());

  for (auto& [table_id, w] : _search_table_writes) {
    auto& shard = basics::downCast<search::SearchTableShard>(*w.shard);
    auto& shard_wal = shard.Wal();
    if (wal == nullptr) {
      wal = &shard_wal;
    } else {
      SDB_ASSERT(wal == &shard_wal,
                 "all search shards in a txn must share one database WAL");
    }

    // Gather this shard's inline (small-INSERT) buffers, one per inline sink
    // thread / statement, with their parallel per-chunk generated-PK base lists
    // (§5.6). Bulk inserts produced no buffers (they streamed to chunk files
    // during Sink) -- their data + pk_bases are in w.seg_ids / the chunk
    // frames.
    std::vector<duckdb::ColumnDataCollection*> buffers;
    std::vector<std::vector<uint64_t>*> pk_lists;
    auto it = _local_table_changes.find(table_id);
    if (it != _local_table_changes.end()) {
      auto& e = it->second;
      for (size_t i = 0; i < e.insert_collections.size(); ++i) {
        auto& c = e.insert_collections[i];
        if (c && c->Count() > 0) {
          buffers.push_back(c.get());
          pk_lists.push_back(e.insert_pk_bases[i].get());
        }
      }
    }
    SDB_ASSERT(!w.seg_ids.empty() || !buffers.empty(),
               "search-table commit with neither chunk files nor inline rows");

    search::SearchDbWal::ShardSection section;
    section.schema_id = w.schema_id;
    section.table_id = table_id.id();
    section.column_ids = search::SearchDbWal::ColumnIds{w.column_ids};
    if (w.seg_ids.empty() && buffers.size() == 1) {
      // OLTP fast path: one inline buffer, no chunk files -> INLINE section
      // (rows serialised straight into the central record, pk_bases alongside).
      section.inline_data = buffers.front();
      section.inline_pk_bases = std::span<const uint64_t>{*pk_lists.front()};
    } else {
      // General path (bulk, multi-statement inline, or a mix): flush any inline
      // buffers to this shard's chunk files so the section is a single
      // REFERENCE over all of them (single-threaded here; only the multi/mixed
      // case). Each chunk carries its pk_base in the chunk frame.
      for (size_t k = 0; k < buffers.size(); ++k) {
        auto writer = shard.NewChunkWriter();
        auto* pks = pk_lists[k];
        size_t ci = 0;
        for (auto& chunk : buffers[k]->Chunks()) {
          writer.Append(chunk, ci < pks->size() ? (*pks)[ci] : 0);
          ++ci;
        }
        writer.Finish();
        w.seg_ids.push_back(writer.SegId());
      }
      section.seg_ids = std::span<const uint64_t>{w.seg_ids};
    }
    sections.push_back(section);
  }

  SDB_ASSERT(wal != nullptr);
  return wal->AppendCommit(sections);
}

Result Transaction::Rollback() {
  absl::Cleanup rollback = [&] {
    for (auto& search_transaction : _search_transactions) {
      search_transaction.second->Abort();
    }
    for (auto& [table_id, w] : _search_table_writes) {
      for (auto& trx : w.transactions) {
        trx->Abort();
      }
    }
    RollbackVariables();
    Destroy();
  };

  if (_rocksdb_transaction) {
    auto status = _rocksdb_transaction->Rollback();
    if (!status.ok()) {
      return {ERROR_INTERNAL,
              "Failed to rollback RocksDB transaction: ", status.ToString()};
    }
  }
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

void Transaction::EnsureRocksDBSnapshot() {
  if (_rocksdb_snapshot) {
    return;
  }
  SDB_ASSERT(!_storage_snapshot);
  if (_rocksdb_transaction) {
    _rocksdb_transaction->SetSnapshot();
    _rocksdb_snapshot = _rocksdb_transaction->GetSnapshot();
  } else {
    _storage_snapshot = GetServerEngine().currentSnapshot();
    SDB_ASSERT(_storage_snapshot);
    _rocksdb_snapshot = _storage_snapshot->GetSnapshot();
  }
  SDB_ASSERT(_rocksdb_snapshot);
}

void Transaction::EnsureRocksDBTransaction() {
  SDB_ASSERT(!_storage_snapshot);
  if (_rocksdb_transaction) {
    return;
  }
  SDB_ASSERT(!_storage_snapshot);
  SDB_ASSERT(!_rocksdb_snapshot);
  auto* db = GetServerEngine().db();
  SDB_ASSERT(db);
  rocksdb::WriteOptions write_options;
  rocksdb::TransactionOptions txn_options;
  txn_options.skip_concurrency_control = true;
  _rocksdb_transaction.reset(db->BeginTransaction(write_options, txn_options));
  SDB_ASSERT(_rocksdb_transaction);
}

void Transaction::RegisterSearchFlushes() noexcept {
  for (auto& [id, trx] : _search_transactions) {
    trx->RegisterFlush();
  }
}

void Transaction::CommitSearchTransactions(uint64_t post_ingest_seq) noexcept {
  for (auto& [id, trx] : _search_transactions) {
    const auto queries = trx->GetQueries();
    if (!trx->Commit(post_ingest_seq + queries)) {
      trx->Abort();
    }
  }
}

void Transaction::Destroy() noexcept {
  DropCatalogSnapshot();
  _storage_snapshot.reset();
  _rocksdb_transaction.reset();
  _rocksdb_snapshot = nullptr;
  _search_transactions.clear();
  _search_table_writes.clear();
  _table_rows_deltas.clear();
  _search_snapshots.clear();
  _search_table_readers.clear();
  _local_table_changes.clear();
  _num_log_data_markers = 0;
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
