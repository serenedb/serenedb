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
#include <duckdb/common/identifier.hpp>
#include <duckdb/storage/storage_lock.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/segment_reader.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "connector/column_id.h"
#include "connector/primary_key.h"
#include "connector/search_key_index.h"
#include "connector/search_key_unique.h"
#include "search/search_pending_delta.h"
#include "search/search_table_changes.h"
#include "search/search_unpublished.h"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::catalog {

class SearchTableEntry;

}  // namespace sdb::catalog
namespace sdb::search {

class SearchTable;

struct SearchShardWrites {
  std::shared_ptr<SearchTable> shard;
  const catalog::SearchTableEntry* table = nullptr;
  std::vector<std::unique_ptr<irs::IndexWriter::Transaction>> transactions;
  // The transaction the write buffer flushes into once it has overrun, owned by
  // `transactions` above. Exclusive, so its segments can be named in the
  // record. Null while the buffer has never overrun.
  irs::IndexWriter::Transaction* buffer_trx = nullptr;
  // The writer-generation slot this transaction registered in on `shard`,
  // released when the transaction settles. -1 until it registers.
  int writer_slot = -1;
  bool truncate_claim = false;
  std::unique_ptr<connector::SearchKeyIndex> key_index;
  duckdb::unique_ptr<duckdb::StorageLockKey> checkpoint_lock;
  uint64_t first_tick = 0;
  irs::SourcePosition position;
  // The keys of rows this transaction leaves in an unflushed iresearch
  // transaction, taken at PrepareCommit because the buffer that holds their
  // values is emptied there. Rows that reached a segment are not among them:
  // the delta carries those segments and reads their dictionaries instead.
  std::vector<connector::KeyTerms> pending_keys;
  // Readers over the segments this transaction flushed. Opened at
  // PrepareCommit, outside every lock, where the flushed set is already final:
  // Stage only registers the transactions on the flush context, which never
  // flushes. They serve the pre-lock pass, the pass under DuckDB's commit lock
  // -- which must not open a file -- and then the delta generation, so every
  // later certifier reads the segment through the reader this commit already
  // had to open rather than opening its own.
  std::vector<irs::SegmentReader> flushed_segments;
  // The table's key columns, empty when it has none. A search table's primary
  // key is fixed at CREATE TABLE -- ALTER TABLE ADD PRIMARY KEY is refused --
  // so this is settled for the table's life, and a table without one neither
  // certifies nor leaves a generation behind for anyone else to certify
  // against.
  std::vector<connector::primary_key::KeySlot> pk_slots;
  // How far into the delta's install order this transaction's statement-end
  // probes have settled the question, unset until one runs. The lowest of
  // them, since a key written by the first statement is only covered as far as
  // that statement looked.
  std::optional<uint64_t> certified_mark;
  // Whether any statement asserted a key value of its own, rather than only
  // putting back one it was already replacing. False for a transaction made
  // entirely of UPDATEs that leave the key columns alone, which has no key to
  // put to a probe at all.
  bool asserts_new_keys = false;
  // The rows such an UPDATE is replacing: it writes a new row carrying the key
  // the old one had, so the key survives only as long as the old row is this
  // transaction's to remove. Another transaction removing it first turns the
  // rewrite into a resurrection of a key that transaction has released, which
  // no key probe can see -- published state still shows the row, masked by
  // this transaction's own delete set.
  std::vector<RowRange> rewritten_rows;
  // Armed by the pre-lock pass over what it read, released once the generation
  // is installed: what keeps the tail the in-lock pass still has to read from
  // being pruned in between.
  PendingDelta::PruneFloor floor;
  // The WAL arm's equivalent of the floor, and the reason it needs none: where
  // the pre-lock pass stopped reading, which is where the in-lock pass starts.
  // The WAL keeps what it holds whether or not anyone is reading it, so there
  // is nothing to hold back -- only a place to resume.
  std::optional<irs::SourcePosition> wal_boundary;
  // Which source this transaction's unpublished rows come from, settled the
  // first time it is asked -- a probe, or the commit for a transaction that
  // reaches no probe. Every later pass and the install follow what was settled
  // rather than re-reading the setting, so a SET mid-transaction cannot leave
  // one pass looking where another put them. Unset until then, which is also
  // why it is not a plain bool: "not asked yet" and "asked, and the answer was
  // the delta" have to be told apart.
  std::optional<bool> certify_from_wal;
};

// Holds a query::Transaction's search-table (TableEngine::Search) state and
// commit logic.
class SearchTableTransaction {
 public:
  ~SearchTableTransaction();

  // Registers this transaction as a writer of `shard`, once, before anything
  // reads the shard's index config -- so a rebuild that publishes a config can
  // tell whether this transaction predates it. Called from the DML operators'
  // GetGlobalSinkState: the bulk insert path builds its sink there, ahead of
  // the Combine that hands over its iresearch transaction, so registering any
  // later would let it straddle a swap unnoticed.
  void RegisterWriter(const std::shared_ptr<SearchTable>& shard,
                      const catalog::SearchTableEntry& table);

  // Whether this transaction has already written to `shard`. CREATE INDEX
  // refuses to run in such a transaction: the rebuild would wait for writers
  // that predate its config swap, and this one cannot finish until the
  // statement it is running does.
  bool HasWritesFor(duckdb::idx_t shard_id) const noexcept {
    return _writes.contains(shard_id);
  }

  void AddParallelSearchTransaction(
    const std::shared_ptr<SearchTable>& shard,
    std::unique_ptr<irs::IndexWriter::Transaction> trx);

  // The segments a bulk statement flushed + fsynced, for the WAL to reference
  // instead of a second copy of the rows.
  void AddSegments(const std::shared_ptr<SearchTable>& shard,
                   std::vector<std::string>&& segments);

  void AddInlineInsertChunk(const std::shared_ptr<SearchTable>& shard,
                            duckdb::BufferManager& buffer_manager,
                            const duckdb::vector<duckdb::LogicalType>& types,
                            std::span<const connector::ColumnId> column_ids,
                            duckdb::Catalog& catalog, duckdb::DataChunk& chunk,
                            uint64_t pk_base);

  void AddKeys(const std::shared_ptr<SearchTable>& shard,
               duckdb::AttachedDatabase& db,
               std::span<const connector::primary_key::KeySlot> slots,
               duckdb::DataChunk& chunk, uint64_t pk_base);

  // The statement-end probe: this statement's keys against everything
  // published and everything committed but not yet published. Best effort by
  // design -- it holds nothing back, so a commit landing after it is not
  // covered -- but it is what attributes a conflict to the statement that
  // caused it, and it is what leaves the commit passes a tail to read instead
  // of the whole table.
  //
  // `new_keys` is false for a statement that can only put back key values the
  // published state already holds at the rows it is replacing: an UPDATE whose
  // SET list leaves the key columns alone. Such a statement skips the
  // published tier, which is the expensive one, but not the delta.
  void CertifyKeys(duckdb::ClientContext& context,
                   const std::shared_ptr<SearchTable>& shard,
                   std::span<const connector::primary_key::KeySlot> slots,
                   bool new_keys);

  // The pass under DuckDB's commit lock, before the WAL write and before the
  // commit's point of no return: this transaction's keys against the
  // generations installed since its pre-lock pass claimed the delta. That
  // lock is what serializes two committers racing on one key, and throwing
  // from here is what still rolls the loser back.
  void CertifyTail(
    duckdb::idx_t database, duckdb::ClientContext& context,
    duckdb::optional_ptr<duckdb::unique_lock<duckdb::mutex>> commit_lock);

  // Bytes a flush would reclaim for `shard_id`; deletes are not counted.
  uint64_t BufferedBytes(duckdb::idx_t shard_id) const noexcept {
    auto it = _changes.find(shard_id);
    return it == _changes.end() ? 0 : it->second.BufferedBytes();
  }

  // Replays the whole buffer -- rows and removals, in issue order -- into the
  // shard's exclusive transaction, creating it on first use, and empties the
  // row buffer. The rows become that transaction's to make durable, so the
  // record stops carrying a copy of them; the removals stay buffered, since
  // the record has to carry those either way.
  void FlushBuffer(const std::shared_ptr<SearchTable>& shard,
                   duckdb::ClientContext& context);
  void FlushBufferTransaction(duckdb::idx_t shard_id);

  // Rowids to remove, in issue order with the buffered rows. `rewrite` marks
  // them as rows an UPDATE is replacing with new ones carrying the same keys,
  // which is what makes their removal by anyone else a conflict.
  void AddSearchDeletes(const std::shared_ptr<SearchTable>& shard,
                        std::span<const int64_t> rows, bool rewrite);

  void AddSearchTruncate(const std::shared_ptr<SearchTable>& shard,
                         const duckdb::Identifier& table_name,
                         bool clears_shard);

  template<typename Factory>
  std::shared_ptr<irs::DirectoryReader> EnsureSearchTableReader(
    duckdb::idx_t database_id, duckdb::idx_t shard_id, Factory&& make_reader) {
    auto& reader =
      _readers.try_emplace(std::pair{database_id, shard_id}).first->second;
    if (!reader) {
      reader = std::make_shared<irs::DirectoryReader>(make_reader());
    }
    return reader;
  }

  bool Empty() const noexcept { return _writes.empty(); }

  void PrepareCommit(duckdb::ClientContext& context);
  void Stage(const irs::SourcePosition& position,
             duckdb::idx_t database) noexcept;
  void Publish(duckdb::idx_t database) noexcept;

  void Abort() noexcept;

  void ResetReaders() noexcept { _readers.clear(); }

  void ResetReader(duckdb::idx_t database_id, duckdb::idx_t shard_id) noexcept {
    _readers.erase(std::pair{database_id, shard_id});
  }

 private:
  SearchShardWrites& EnsureShardWrites(
    const std::shared_ptr<SearchTable>& shard);
  void ReleaseWriters() noexcept;

  // Replays a buffer into `trx` in issue order, rows and removals interleaved
  // by watermark. Leaves the buffer untouched; the caller decides whether the
  // rows are now somebody else's to make durable.
  static void ReplayBuffer(SearchTable& shard, LocalTableChangesEntry& entry,
                           irs::IndexWriter::Transaction& trx,
                           duckdb::ClientContext& context);

  // The shard's exclusive buffer transaction, created on first overrun.
  irs::IndexWriter::Transaction& EnsureBufferTransaction(
    const std::shared_ptr<SearchTable>& shard);

  // Hands this transaction's rows and removals to the shard's delta, so that a
  // concurrent certifier can see them for as long as the reader cannot.
  void InstallPendingDelta(SearchShardWrites& w, duckdb::idx_t table_id,
                           uint64_t tick) noexcept;

  // The pre-lock pass, run from PrepareCommit: claims the delta's tail above
  // what the statement-end probes settled, arms the floor on it, and puts this
  // transaction's whole key set to what it claimed. Leaves the pass under the
  // commit lock with only the concurrent window to read.
  void CertifyCommit(SearchShardWrites& w, duckdb::idx_t table_id,
                     duckdb::ClientContext& context);

  // The body both commit passes share: this transaction's keys -- its
  // unflushed tail and the segments it flushed -- against `tier`, and against
  // `reader` when the pass has to answer for the published tier too. Takes the
  // tier in its mode-neutral shape, so neither pass knows whether the rows
  // came from the delta or from a read of the WAL.
  void CertifyAgainst(SearchShardWrites& w, duckdb::idx_t table_id,
                      const irs::DirectoryReader* reader, UnpublishedTier tier);

  // Refuses the commit if `removed` has already taken away a row this
  // transaction is rewriting, which would put its key back after the owner
  // released it. Cheap enough to run on every pass: see `rewritten_rows`.
  static void RefuseResurrection(const SearchShardWrites& w,
                                 std::span<const RowRange> removed);

  irs::containers::NodeHashMap<duckdb::idx_t, SearchShardWrites> _writes;
  irs::containers::FlatHashMap<std::pair<duckdb::idx_t, duckdb::idx_t>,
                               std::shared_ptr<irs::DirectoryReader>>
    _readers;
  LocalTableChanges _changes;
};

}  // namespace sdb::search
