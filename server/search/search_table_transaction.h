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

#include <cstdint>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <memory>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/node_hash_map.h"
#include "catalog/identifiers/object_id.h"
#include "search/search_table_changes.h"

namespace sdb {

class TableShard;

}  // namespace sdb
namespace sdb::search {

// Per-(sdb txn, search shard) writes (WAL_DESIGN.md §3.2/§9). One entry per
// search table the transaction wrote; Commit() turns each into a per-shard
// section of ONE central WAL record (multi-shard atomicity, §9). Keyed by
// table_id in SearchTableTransaction::_writes.
struct SearchShardWrites {
  // Keeps the shard alive past the catalog snapshot; downcast to
  // SearchTableShard at commit to reach the database's shared WAL.
  std::shared_ptr<TableShard> shard;
  // Bulk chunk-file ids (one per sink thread that wrote rows), collected at
  // Combine/Finalize. Empty => this shard's data is in its inline buffers
  // (the per-table entry in `changes`).
  std::vector<uint64_t> seg_ids;
  // Per-shard iresearch transactions, committed on the shared WAL tick at
  // Commit(). A parallel (bulk) INSERT appends one per sink thread; consecutive
  // single-threaded INSERTs reuse the LAST one so they coalesce into a single
  // segment (WAL_DESIGN.md §5.5).
  std::vector<std::unique_ptr<irs::IndexWriter::Transaction>> transactions;
};

// All of a query::Transaction's search-table (StorageKind::kSearch) state and
// commit logic, factored out of query::Transaction. The transaction holds this
// as a std::optional (lazily created on the first search-table touch) and just
// delegates the three lifecycle hooks: RegisterFlush() up-front, Commit() at
// the WAL commit point, Abort() on any rollback path. The inverted-index
// iresearch trxs stay on query::Transaction -- they commit on the rocksdb seq,
// not the engine WAL tick.
class SearchTableTransaction {
 public:
  // --- Operator-side mutators (SereneDBSearchInsert) ---

  // Parallel search-table INSERT: each sink thread creates its own iresearch
  // IndexWriter::Transaction (one segment) and hands it off here at Combine. A
  // single INSERT contributes N transactions on the same shard; they commit
  // together on the shared WAL tick in Commit() -- valid because the writes are
  // insert-only (no removes), so all N segments share first_tick and one
  // RefreshCommit publishes them. The operator serialises calls with its own
  // sink-state mutex. `table_id` routes the trx to its shard's entry.
  void AddParallelSearchTransaction(
    ObjectId table_id, std::unique_ptr<irs::IndexWriter::Transaction> trx) {
    _writes[table_id].transactions.push_back(std::move(trx));
  }

  // Single-threaded INSERT: reuse the LAST iresearch trx on this shard (create
  // one via `make_trx` if the shard has none yet), so consecutive non-bulk
  // statements in this txn coalesce into ONE segment instead of one-per-
  // statement (WAL_DESIGN.md §5.5). The operator calls this lazily on the first
  // row -- a zero-row statement parks nothing -- and the single-threaded path
  // is serialised, so no locking is needed. Bulk statements keep appending
  // fresh per-thread trxs via AddParallelSearchTransaction.
  template<typename Factory>
  irs::IndexWriter::Transaction& EnsureSerialSearchTransaction(
    ObjectId table_id, Factory&& make_trx) {
    auto& trxs = _writes[table_id].transactions;
    if (trxs.empty()) {
      trxs.push_back(
        std::make_unique<irs::IndexWriter::Transaction>(make_trx()));
    }
    return *trxs.back();
  }

  // Record (or extend) a plain search-table statement's writes for this txn
  // (WAL_DESIGN.md §7). Called once per plain INSERT/COPY statement at Finalize
  // -- this is statement end, NOT the commit: the txn's single WAL record is
  // appended later, in Commit(). The first call sets the descriptor; a later
  // statement on the same shard appends its bulk chunk-file `seg_ids` (inline
  // buffers accumulate in `changes`). `table_id` routes to that shard's entry.
  void AddSearchTableStatement(std::shared_ptr<TableShard> shard,
                               ObjectId table_id,
                               std::vector<uint64_t> seg_ids) {
    auto& w = _writes[table_id];
    w.shard = std::move(shard);
    auto& acc = w.seg_ids;
    acc.insert(acc.end(), seg_ids.begin(), seg_ids.end());
  }

  // Pins a SearchTableShard's DirectoryReader for the lifetime of this sdb txn
  // so every scan in the same transaction sees the same view. `make_reader` is
  // invoked only on first call for a given shard_id; subsequent calls return
  // the same reader.
  template<typename Factory>
  std::shared_ptr<irs::DirectoryReader> EnsureSearchTableReader(
    ObjectId shard_id, Factory&& make_reader) {
    auto it = _readers.find(shard_id);
    if (it == _readers.end()) {
      it = _readers
             .emplace(shard_id,
                      std::make_shared<irs::DirectoryReader>(make_reader()))
             .first;
    }
    return it->second;
  }

  // Per-search-table in-flight INSERT buffer (see
  // search/search_table_changes.h). Lazily populated by SereneDBSearchInsert;
  // read back at commit for the INLINE WAL record (and future RYOW overlay).
  LocalTableChanges& Changes() noexcept { return _changes; }

  // --- Lifecycle (query::Transaction delegates) ---

  // True until any search table has been written this txn.
  bool Empty() const noexcept { return _writes.empty(); }

  // Tie every per-thread segment to the current flush context so a concurrent
  // background RefreshCommit waits for them. Called before any commit point,
  // regardless of whether rocksdb commits (WAL_DESIGN.md §9).
  void RegisterFlush() noexcept;

  // The search-table commit point (WAL_DESIGN.md §9): append ONE central WAL
  // record covering every shard this txn wrote (its single fsync is the
  // multi-shard atomic commit point), then stamp each shard's per-thread
  // segments with the tick it returns. Throws on WAL failure (caller aborts).
  void Commit();

  // Abort every per-thread iresearch trx (rollback / commit-failure paths).
  void Abort() noexcept;

  // Drop the pinned readers (READ_COMMITTED starts a fresh snapshot per
  // statement).
  void ResetReaders() noexcept { _readers.clear(); }

 private:
  // Build per-shard sections from _writes + _changes and append the single
  // central record; returns its tick (the engine-global WAL tick).
  uint64_t AppendCommit();

  // Keyed by table_id; consumed by Commit() into ONE central WAL record across
  // all shards.
  containers::NodeHashMap<ObjectId, SearchShardWrites> _writes;
  // Per-(SearchTableShard) DirectoryReader cache; shared_ptr so multiple scans
  // in one txn alias the same reader.
  containers::FlatHashMap<ObjectId, std::shared_ptr<irs::DirectoryReader>>
    _readers;
  LocalTableChanges _changes;
};

}  // namespace sdb::search
