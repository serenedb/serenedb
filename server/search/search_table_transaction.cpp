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

#include "search/search_table_transaction.h"

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/storage/storage_lock.hpp>
#include <duckdb/storage/write_ahead_log.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <duckdb/transaction/transaction_log_writer.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <iterator>
#include <span>
#include <string>
#include <vector>

#include "catalog/entry/search_table.h"
#include "connector/column_id.h"
#include "connector/search_key_unique.h"
#include "connector/search_remove_filter.hpp"
#include "connector/search_sink_writer.hpp"
#include "connector/search_unique_probe.h"
#include "search/search_table.h"
#include "search/wal_tail_keys.h"

namespace sdb::search {
namespace {

// What a certifier must consult besides the published reader and its own
// writes: rows that are durable, in no reader, and therefore invisible to a
// probe that reads only the reader. The pointers borrow from the tail they
// were read out of, which has to outlive the probe.
struct UnpublishedTier {
  std::vector<const irs::SubReader*> segments;
  std::vector<const connector::KeyTerms*> keys;
  std::vector<RowRange> removed;

  bool Empty() const noexcept {
    return segments.empty() && keys.empty() && removed.empty();
  }
};

UnpublishedTier Flatten(const WalTail& tail) {
  UnpublishedTier tier;
  tier.segments.reserve(tail.segments.size());
  for (const auto& segment : tail.segments) {
    tier.segments.push_back(&segment);
  }
  tier.keys.reserve(tail.keys.keys.size());
  for (const auto& key : tail.keys.keys) {
    tier.keys.push_back(&key);
  }
  tier.removed = tail.keys.removed;
  return tier;
}

// Both together, wherever the buffer they mirror is dropped: kept apart they
// would have a resume point into rows that are no longer there.
void DropBufferedKeys(SearchShardWrites& w) noexcept {
  w.buffered_keys.clear();
  w.encoded_rows = 0;
}

// A tail that is not whole cannot be certified against. Every one of these
// reads back as an empty tail, which is indistinguishable from "nothing was
// committed" and would admit the duplicate the pass exists to catch, so the
// commit is refused instead.
void RefuseUncertifiableTail(const WalTail& tail, duckdb::idx_t table_id) {
  if (!tail.error.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("Cannot check the primary key of search table ",
                            table_id, ": ", tail.error, "."));
  }
  if (tail.lost) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("Cannot check the primary key of search table ", table_id,
              ": a segment the write-ahead log names is already gone."));
  }
  if (tail.stopped_undecided) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("Cannot check the primary key of search table ", table_id,
              ": the write-ahead log ends in a transaction that has not "
              "decided yet."));
  }
}

uint64_t SerializedSizeHint(const LocalTableChangesEntry& changes) {
  uint64_t size = changes.BufferedBytes();
  for (const auto& op : changes.ops) {
    size += op.delete_ranges.size() * 2 * sizeof(int64_t);
  }
  return size;
}

// The rowid ranges this transaction deleted, taken by a build's log as they
// are.
void RecordDeletesForBuild(SearchTable& shard,
                           const LocalTableChangesEntry& changes,
                           const irs::SourcePosition& position) noexcept {
  std::vector<RowRange> rows;
  bool truncated = false;
  for (const auto& op : changes.ops) {
    truncated |= op.IsTruncate();
    rows.insert(rows.end(), op.delete_ranges.begin(), op.delete_ranges.end());
  }
  shard.AppendDeleteLog(rows);
  if (truncated) {
    shard.RecordTruncateForBuild(position);
  }
}

void ReleaseWriter(SearchShardWrites& w) noexcept {
  if (w.truncate_claim) {
    w.shard->ReleaseTruncate();
    w.truncate_claim = false;
  }
  if (w.writer_slot >= 0) {
    w.shard->DeregisterWriter(static_cast<unsigned>(w.writer_slot));
    w.writer_slot = -1;
  }
}

class SearchTableLogWriter final : public duckdb::TransactionLogWriter {
 public:
  SearchTableLogWriter(const catalog::SearchTableEntry& table,
                       LocalTableChangesEntry& changes)
    : _table{table},
      _entries{table.catalog.GetAttached(), SerializedSizeHint(changes)} {
    const bool truncates =
      !changes.ops.empty() && changes.ops.front().IsTruncate();
    if (truncates) {
      _entries.WriteTruncateTable();
    }
    if (!changes.segments.empty()) {
      // Everything promoted sits ahead of what is left inline: a removal in
      // this record names rows committed before the transaction, never these.
      _entries.WriteAdoptSegments(duckdb::vector<std::string>(
        std::make_move_iterator(changes.segments.begin()),
        std::make_move_iterator(changes.segments.end())));
    }
    changes.Visit(
      truncates ? 1 : 0,
      [&](duckdb::DataChunk& rows, uint64_t pk_base) {
        _entries.WriteInsert(rows, pk_base);
      },
      [&](LocalTableChangesEntry::Op& op) {
        SDB_ASSERT(!op.IsTruncate());
        SDB_IF_FAILURE("search_delete_wal_rowids") {
          WriteDeletedRowids(op.delete_ranges);
          return;
        }
        duckdb::vector<duckdb::row_t> starts;
        duckdb::vector<duckdb::idx_t> counts;
        starts.reserve(op.delete_ranges.size());
        counts.reserve(op.delete_ranges.size());
        for (const auto& range : op.delete_ranges) {
          starts.push_back(range.begin);
          counts.push_back(static_cast<duckdb::idx_t>(range.end - range.begin));
        }
        SDB_IF_FAILURE("search_delete_wal_empty_range") { counts.front() = 0; }
        _entries.WriteDeleteRowRanges(std::move(starts), std::move(counts));
      });
  }

  void WriteToWAL(duckdb::WriteAheadLog& wal) final {
    wal.WriteSetTable(_table);
    wal.WriteEntries(_entries);
  }

 private:
  void WriteDeletedRowids(std::span<const RowRange> ranges) {
    std::vector<int64_t> rowids;
    for (const auto& range : ranges) {
      for (auto row = range.begin; row != range.end; ++row) {
        rowids.push_back(row);
      }
    }
    duckdb::DataChunk rows;
    rows.InitializeEmpty({duckdb::LogicalType::ROW_TYPE});
    rows.data[0].Reference(duckdb::Vector(duckdb::LogicalType::ROW_TYPE,
                                          duckdb::data_ptr_cast(rowids.data()),
                                          rowids.size()));
    rows.SetChildCardinality(rowids.size());
    _entries.WriteDelete(rows);
  }

  const catalog::SearchTableEntry& _table;
  duckdb::WALEntryBuffer _entries;
};

}  // namespace

SearchTableTransaction::~SearchTableTransaction() { ReleaseWriters(); }

SearchShardWrites& SearchTableTransaction::EnsureShardWrites(
  const std::shared_ptr<SearchTable>& shard) {
  auto& w = _writes[shard->GetTableId()];
  if (!w.shard) {
    w.shard = shard;
  }
  return w;
}

void SearchTableTransaction::RegisterWriter(
  const std::shared_ptr<SearchTable>& shard,
  const catalog::SearchTableEntry& table) {
  auto& w = EnsureShardWrites(shard);
  w.table = &table;
  if (w.writer_slot < 0) {
    const auto slot = shard->RegisterWriter();
    if (!slot) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
        ERR_MSG("Attempting to write to table ", table.name.GetIdentifierName(),
                " but another transaction is truncating it"));
    }
    w.writer_slot = static_cast<int>(*slot);
  }
}

void SearchTableTransaction::ReleaseWriters() noexcept {
  for (auto& [table_id, w] : _writes) {
    ReleaseWriter(w);
  }
}

void SearchTableTransaction::AddParallelSearchTransaction(
  const std::shared_ptr<SearchTable>& shard,
  std::unique_ptr<irs::IndexWriter::Transaction> trx) {
  EnsureShardWrites(shard).transactions.push_back(std::move(trx));
}

void SearchTableTransaction::AddSegments(
  const std::shared_ptr<SearchTable>& shard,
  std::vector<std::string>&& segments) {
  _changes[shard->GetTableId()].AppendSegments(std::move(segments));
}

void SearchTableTransaction::AddInlineInsertChunk(
  const std::shared_ptr<SearchTable>& shard,
  duckdb::BufferManager& buffer_manager,
  const duckdb::vector<duckdb::LogicalType>& types,
  std::span<const connector::ColumnId> column_ids, duckdb::Catalog& catalog,
  duckdb::DataChunk& chunk, uint64_t pk_base) {
  _changes[shard->GetTableId()].AppendInsertChunk(
    buffer_manager, types, column_ids, catalog, chunk, pk_base);
}

void SearchTableTransaction::AddKeys(
  const std::shared_ptr<SearchTable>& shard, duckdb::AttachedDatabase& db,
  std::span<const connector::primary_key::KeySlot> slots,
  duckdb::DataChunk& chunk, uint64_t pk_base) {
  if (slots.empty()) {
    return;
  }
  auto& w = EnsureShardWrites(shard);
  if (!w.key_index) {
    w.key_index = std::make_unique<connector::SearchKeyIndex>(db, slots);
  }
  w.key_index->Insert(chunk, pk_base);
}

// Replays a shard's buffer into `trx` in issue order: rows through a sink,
// removals through the transaction, each removal landing after exactly the rows
// that preceded it. Feeding in this order is what reproduces the `_queries`
// stamping the statements would have produced had they written directly.
void SearchTableTransaction::ReplayBuffer(SearchTable& shard,
                                          LocalTableChangesEntry& entry,
                                          irs::IndexWriter::Transaction& trx,
                                          duckdb::ClientContext& context) {
  std::unique_ptr<connector::SearchSinkInsertBaseImpl> sink;
  if (entry.HasBufferedRows()) {
    SDB_ASSERT(entry.catalog != nullptr,
               "buffered rows without the catalog their sink needs");
    sink =
      connector::MakeSearchTableInsertSink(trx, shard, *entry.catalog, context);
  }
  entry.Visit(
    entry.applied_ops,
    [&](duckdb::DataChunk& chunk, uint64_t pk_base) {
      connector::WriteChunkToSearchSink(*sink, chunk, entry.column_ids, pk_base,
                                        shard.GetTableId(), context);
    },
    [&](LocalTableChangesEntry::Op& op) {
      SDB_ASSERT(!op.IsTruncate());
      if (auto removal = connector::MakeRowRangeRemoval(op.delete_ranges)) {
        trx.Remove(std::move(removal));
      }
    });
  entry.applied_ops = entry.ops.size();
}

irs::IndexWriter::Transaction& SearchTableTransaction::EnsureBufferTransaction(
  const std::shared_ptr<SearchTable>& shard) {
  auto& w = EnsureShardWrites(shard);
  if (w.buffer_trx == nullptr) {
    // Exclusive: its segments are named in the record, so they must not also
    // carry another transaction's documents.
    w.transactions.push_back(std::make_unique<irs::IndexWriter::Transaction>(
      shard->GetTransaction(/*exclusive_segment=*/true)));
    w.buffer_trx = w.transactions.back().get();
  }
  return *w.buffer_trx;
}

void SearchTableTransaction::FlushBuffer(
  const std::shared_ptr<SearchTable>& shard, duckdb::ClientContext& context) {
  auto it = _changes.find(shard->GetTableId());
  if (it == _changes.end()) {
    return;
  }
  auto& trx = EnsureBufferTransaction(shard);
  ReplayBuffer(*shard, it->second, trx, context);
  // The rows are this transaction's to make durable now, so the record stops
  // carrying them. The removals stay: no segment holds those.
  it->second.ClearBufferedRows();
  // Their keys stop travelling with it: the segment this flush produces is
  // named in the record, and a probe reads its dictionary instead.
  DropBufferedKeys(EnsureShardWrites(shard));
}

void SearchTableTransaction::RefuseResurrection(
  const SearchShardWrites& w, std::span<const RowRange> removed) {
  if (w.rewritten_rows.empty()) {
    return;
  }
  const auto overlaps = [](const RowRange& lhs, const RowRange& rhs) {
    return lhs.begin < rhs.end && rhs.begin < lhs.end;
  };
  // Ranges on both sides, and a statement rewrites a handful of runs at most,
  // so this is a pair of short walks rather than anything per row.
  for (const auto& gone : removed) {
    for (const auto& row : w.rewritten_rows) {
      if (overlaps(gone, row)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
          ERR_MSG("could not serialize access due to concurrent delete of a "
                  "row being updated on search table ",
                  w.shard->GetTableId()));
      }
    }
  }
}

void SearchTableTransaction::CertifyKeys(
  duckdb::ClientContext& context, const std::shared_ptr<SearchTable>& shard,
  std::span<const connector::primary_key::KeySlot> slots, bool new_keys) {
  if (slots.empty()) {
    return;
  }
  const auto changes = _changes.find(shard->GetTableId());
  if (changes == _changes.end()) {
    return;
  }

  // What this statement wrote is what decides whether there is anything to
  // probe for at all, so it is settled first, and from the flushed set rather
  // than from readers over it: a statement that wrote no row leaves without
  // opening a file.
  // RegisterWriter runs from the operator's GetGlobalSinkState, before any
  // chunk reaches the sink, so a shard with changes always has writes.
  const auto writes = _writes.find(shard->GetTableId());
  SDB_ASSERT(writes != _writes.end());
  auto& w = writes->second;
  bool flushed = false;
  for (const auto& trx : w.transactions) {
    flushed = flushed || !trx->Flushed().empty();
  }

  auto* rows = changes->second.collection.get();
  if (rows != nullptr && rows->Count() == 0) {
    rows = nullptr;
  }
  if (!flushed && rows == nullptr) {
    return;
  }

  auto store = ResolveTailDatabase(*shard);
  if (!store) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("The database holding search table ",
                            shard->GetTableId(), " is no longer attached."));
  }
  // The position first and the reader after it, so the reader can only be
  // ahead of where the log read below starts -- never behind it, which is the
  // way round that would leave rows in neither source.
  auto [published, from] = shard->GetCertificationSnapshot();
  const auto wal =
    ReadUnpublishedTail(*shard, *store, context, from, slots, nullptr);
  RefuseUncertifiableTail(wal, shard->GetTableId());
  // Where the commit pass picks up. The lowest across this transaction's
  // statements: a key the first statement wrote is answered for only as far as
  // that statement looked, so the commit passes start from there for all of
  // them.
  if (!w.certified_tail || wal.boundary < *w.certified_tail) {
    w.certified_tail = wal.boundary;
  }
  UnpublishedTier tier = Flatten(wal);

  // An UPDATE that leaves the key columns alone asserts no key of its own: it
  // puts back, on a new row, the key the row it is replacing already had. So
  // there is nothing to put to a key probe -- published state can only answer
  // with those same rows, which this transaction's delete set masks.
  //
  // What it does have to establish is that those rows are still its to
  // replace. Another transaction that removed one of them first has released
  // that key, and may have given it to a row of its own; putting it back then
  // resurrects a key somebody else owns. That is a question about rowids, not
  // about keys, and answering it directly is both cheaper than a probe and
  // wider than one -- a key probe only sees it when the other transaction
  // happened to re-assert the key, and misses the plain case where it only
  // removed the row.
  if (!new_keys) {
    RefuseResurrection(w, tier.removed);
    return;
  }
  w.asserts_new_keys = true;

  std::vector<irs::SegmentReader> readers;
  for (const auto& trx : w.transactions) {
    for (const auto& segment : trx->Flushed()) {
      // The transaction that flushed them is alive, and the directory it wrote
      // them through references every file it created, so these need no
      // holding off of the cleaner.
      readers.push_back(shard->OpenFlushedSegment(segment.meta));
    }
  }

  std::vector<irs::field_id> key_fields;
  key_fields.reserve(slots.size());
  for (const auto& slot : slots) {
    key_fields.push_back(slot.column);
  }

  // A removal of the unpublished tier is as invisible as an insert of it: the
  // reader still shows the row, and only the publish that applies the removal
  // stops it doing so. So both sets of ranges mask the same probe.
  std::vector<RowRange> deleted = std::move(tier.removed);
  for (const auto& op : changes->second.ops) {
    deleted.insert(deleted.end(), op.delete_ranges.begin(),
                   op.delete_ranges.end());
  }

  connector::UniqueProbe probe{&published, tier.segments, std::move(tier.keys),
                               key_fields, deleted};

  std::vector<const irs::SubReader*> segments;
  segments.reserve(readers.size());
  for (const auto& reader : readers) {
    segments.push_back(&reader);
  }

  // Only what arrived since this transaction's last probe: the keys before it
  // are still valid, and the rows they came from are still buffered.
  if (rows != nullptr) {
    connector::AppendRowKeys(*rows, slots, w.encoded_rows, w.buffered_keys);
    w.encoded_rows = rows->Count();
  }
  if (connector::FindDuplicateKey(segments, w.buffered_keys, slots, probe)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNIQUE_VIOLATION),
                    ERR_MSG("Duplicate key on column \"", slots[0].name,
                            "\" violates primary key constraint."));
  }
}

void SearchTableTransaction::CertifyAgainst(SearchShardWrites& w,
                                            duckdb::idx_t table_id,
                                            const irs::DirectoryReader* reader,
                                            const WalTail& tail) {
  SDB_ASSERT(!w.pk_slots.empty());
  UnpublishedTier tier = Flatten(tail);
  if (reader == nullptr && tier.Empty()) {
    return;
  }

  RefuseResurrection(w, tier.removed);
  if (!w.asserts_new_keys) {
    // Nothing of this transaction's claims a key: every row it wrote carries
    // the key of a row it is replacing, and the check above is what answers
    // for those. There is no key set to walk and no probe to build.
    return;
  }

  std::vector<irs::field_id> key_fields;
  key_fields.reserve(w.pk_slots.size());
  for (const auto& slot : w.pk_slots) {
    key_fields.push_back(slot.column);
  }

  // The tier's own ranges are the start of the merged set rather than a source
  // to copy from: nothing reads them separately past this point.
  std::vector<RowRange> deleted = std::move(tier.removed);
  if (const auto changes = _changes.find(table_id); changes != _changes.end()) {
    for (const auto& op : changes->second.ops) {
      deleted.insert(deleted.end(), op.delete_ranges.begin(),
                     op.delete_ranges.end());
    }
  }

  connector::UniqueProbe probe{reader, tier.segments, std::move(tier.keys),
                               key_fields, deleted};

  std::vector<const irs::SubReader*> segments;
  segments.reserve(w.flushed_segments.size());
  for (const auto& segment : w.flushed_segments) {
    segments.push_back(&segment);
  }

  if (connector::FindDuplicateKey(segments, w.pending_keys, w.pk_slots,
                                  probe)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNIQUE_VIOLATION),
                    ERR_MSG("Duplicate key on column \"", w.pk_slots[0].name,
                            "\" violates primary key constraint."));
  }
}

void SearchTableTransaction::CertifyCommit(SearchShardWrites& w,
                                           duckdb::idx_t table_id,
                                           duckdb::ClientContext& context) {
  auto store = ResolveTailDatabase(*w.shard);
  if (!store) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("The database holding search table ", table_id,
                            " is no longer attached."));
  }
  // Resuming where the statement passes stopped, so this one answers only for
  // what committed since. Starting over instead would re-walk every key this
  // transaction wrote against state already ruled out -- and the rows it is
  // committing are not in the log yet to be found either way, because
  // `SearchTableLogWriter` writes them after this pass.
  //
  // Without a statement pass to resume from -- a transaction of deletes alone
  // -- the reader and its position come from the same place the statement
  // pass takes them, and in the same order and for the same reason.
  irs::DirectoryReader reader;
  irs::SourcePosition from;
  if (w.certified_tail) {
    from = *w.certified_tail;
  } else {
    std::tie(reader, from) = w.shard->GetCertificationSnapshot();
  }
  const auto tail =
    ReadUnpublishedTail(*w.shard, *store, context, from, w.pk_slots, nullptr);
  RefuseUncertifiableTail(tail, table_id);
  // Where the in-lock pass resumes. Set even for an empty tail: the pass still
  // has to cover whatever commits between here and the commit lock.
  w.wal_boundary = tail.boundary;
  // The published reader only when no statement pass has probed it: the
  // published position never goes backwards, so what that pass saw cannot have
  // been missed.
  CertifyAgainst(w, table_id, w.certified_tail ? nullptr : &reader, tail);
}

void SearchTableTransaction::CertifyTail(
  duckdb::idx_t database, duckdb::ClientContext& context,
  duckdb::optional_ptr<duckdb::unique_lock<duckdb::mutex>> commit_lock) {
  for (auto& [table_id, w] : _writes) {
    if (w.shard->GetDatabaseId() != database) {
      continue;
    }
    if (!w.wal_boundary) {
      continue;
    }
    // Resuming where the pre-lock pass stopped, under the caller's commit
    // lock: everything committed in between is between those two positions.
    auto store = ResolveTailDatabase(*w.shard);
    if (!store) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("The database holding search table ", table_id,
                              " is no longer attached."));
    }
    const auto tail = ReadUnpublishedTail(
      *w.shard, *store, context, *w.wal_boundary, w.pk_slots, commit_lock);
    RefuseUncertifiableTail(tail, table_id);
    CertifyAgainst(w, table_id, nullptr, tail);
  }
}

void SearchTableTransaction::FlushBufferTransaction(duckdb::idx_t shard_id) {
  if (const auto it = _writes.find(shard_id);
      it != _writes.end() && it->second.buffer_trx != nullptr) {
    it->second.buffer_trx->Flush();
  }
}

void SearchTableTransaction::AddSearchDeletes(
  const std::shared_ptr<SearchTable>& shard, std::span<const int64_t> rows,
  bool rewrite) {
  auto& w = EnsureShardWrites(shard);
  _changes[shard->GetTableId()].AppendDeletes(rows);
  if (rewrite) {
    AppendRowRanges(w.rewritten_rows, rows);
  }
}

void SearchTableTransaction::AddSearchTruncate(
  const std::shared_ptr<SearchTable>& shard,
  const duckdb::Identifier& table_name, bool clears_shard) {
  auto& w = EnsureShardWrites(shard);
  if (!w.truncate_claim) {
    if (!shard->ClaimTruncate()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
        ERR_MSG("Attempting to truncate table ", table_name.GetIdentifierName(),
                " but another transaction is writing to this table"));
    }
    w.truncate_claim = true;
  }
  w.transactions.clear();
  w.buffer_trx = nullptr;
  if (w.key_index) {
    w.key_index->Clear();
  }
  const bool clears = clears_shard && !shard->BuildInFlight();
  auto& changes = _changes[shard->GetTableId()];
  changes.AppendTruncate(clears);
  DropBufferedKeys(w);
  changes.applied_ops = changes.ops.size();
  if (!clears) {
    auto& trx = *w.transactions.emplace_back(
      std::make_unique<irs::IndexWriter::Transaction>(
        shard->GetTransaction(/*exclusive_segment=*/true)));
    trx.Remove(std::make_shared<irs::All>());
  }
}

void SearchTableTransaction::Abort() noexcept {
  for (auto& [table_id, w] : _writes) {
    for (auto& trx : w.transactions) {
      trx->Abort();
    }
  }
  ReleaseWriters();
  _writes.clear();
  _changes.clear();
  _readers.clear();
}

void SearchTableTransaction::PrepareCommit(duckdb::ClientContext& context) {
  for (auto& [table_id, w] : _writes) {
    w.pk_slots = connector::primary_key::KeySlots(*w.table);
    const auto it = _changes.find(table_id);
    if (it == _changes.end()) {
      continue;
    }
    auto& entry = it->second;
    if (entry.HasBufferedRows() || entry.applied_ops != entry.ops.size()) {
      SDB_IF_FAILURE("search_feed_pending_fails") {
        THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
      }
      if (w.buffer_trx != nullptr) {
        // Into the segment that is about to be flushed, fsynced and named in
        // the record, so the tail travels as a reference instead of a second
        // copy of the rows. Dropping it from the buffer here is what keeps the
        // record from carrying both.
        ReplayBuffer(*w.shard, entry, *w.buffer_trx, context);
        entry.ClearBufferedRows();
        DropBufferedKeys(w);
      } else {
        // No exclusive segment to join: the tail goes to a pooled one, which
        // may hold other transactions' documents and so can never be named.
        // The record carries these rows inline instead.
        if (w.transactions.empty()) {
          w.transactions.push_back(
            std::make_unique<irs::IndexWriter::Transaction>(
              w.shard->GetTransaction()));
        }
        // Last point at which the values are readable: the replay below hands
        // the rows to iresearch, which publishes them only at a later refresh,
        // and nothing between the two can name them. A pooled segment cannot
        // be opened the way a flushed one can, so the keys have to travel.
        if (entry.HasBufferedRows() && !w.pk_slots.empty()) {
          // Whatever no probe has covered yet: a statement that wrote after
          // the last one, or a transaction that reached no probe at all.
          connector::AppendRowKeys(*entry.collection, w.pk_slots,
                                   w.encoded_rows, w.buffered_keys);
          w.pending_keys = std::move(w.buffered_keys);
          DropBufferedKeys(w);
        }
        ReplayBuffer(*w.shard, entry, *w.transactions.back(), context);
      }
    }
    if (w.buffer_trx != nullptr) {
      for (const auto& segment : w.buffer_trx->FlushAndFsync()) {
        entry.segments.push_back(segment.filename);
      }
    }
    duckdb::DuckTransaction::Get(context, w.table->catalog)
      .log_writers.push_back(
        duckdb::make_shared_ptr<SearchTableLogWriter>(*w.table, entry));
    entry.ClearBufferedRows();
    DropBufferedKeys(w);
    if (entry.ClearsShard()) {
      w.checkpoint_lock =
        duckdb::DuckTransactionManager::Get(w.table->catalog.GetAttached())
          .SharedCheckpointLock();
    }
  }
  // A table with no primary key opens nothing: there is nothing to certify.
  for (auto& [table_id, w] : _writes) {
    if (w.pk_slots.empty()) {
      continue;
    }
    for (const auto& trx : w.transactions) {
      for (const auto& flushed : trx->Flushed()) {
        w.flushed_segments.push_back(w.shard->OpenFlushedSegment(flushed.meta));
      }
    }
    // Deletes and truncates alone: this transaction asserts no key, so there
    // is nothing to certify.
    if (w.pending_keys.empty() && w.flushed_segments.empty()) {
      continue;
    }
    CertifyCommit(w, table_id, context);
  }
  // Between the pass that settles against everything committed and the one
  // under the commit lock that settles against whatever committed since: the
  // window the second pass exists for, and the only place a test can hold a
  // committer open in it.
  SDB_PARK_ONCE_ON_FAILURE("pause_search_commit_after_certify");
}

void SearchTableTransaction::Stage(const irs::SourcePosition& position,
                                   duckdb::idx_t database) noexcept {
  for (auto& [table_id, w] : _writes) {
    if (w.shard->GetDatabaseId() != database) {
      continue;
    }
    uint64_t queries = 0;
    for (auto& trx : w.transactions) {
      trx->RegisterFlush();
      queries += trx->GetQueries();
    }
    w.first_tick = w.shard->NextTick(queries) - queries;
    w.position = position;
    w.shard->MarkStaged(position);
  }
}

void SearchTableTransaction::Publish(duckdb::idx_t database) noexcept {
  for (auto& [table_id, w] : _writes) {
    if (w.shard->GetDatabaseId() != database) {
      continue;
    }
    SDB_PARK_ONCE_ON_FAILURE("pause_search_commit_before_irs");
    auto& shard = *w.shard;
    const auto changes = _changes.find(table_id);
    if (changes != _changes.end() && changes->second.ClearsShard()) {
      try {
        shard.Clear(w.position);
      } catch (const std::exception& e) {
        SDB_FATAL(SEARCH, "search-table commit: Clear failed for table ",
                  table_id, ": ", duckdb::ErrorData{e}.RawMessage());
      }
    }
    w.checkpoint_lock.reset();
    SDB_PARK_ONCE_ON_FAILURE("pause_search_commit_before_delete_log");
    if (changes != _changes.end() && !changes->second.ops.empty() &&
        shard.IsDeleteLogOpen()) {
      RecordDeletesForBuild(shard, changes->second, w.position);
    }
    auto tick = w.first_tick;
    for (auto& trx : w.transactions) {
      tick += trx->GetQueries();
      SDB_FATAL_IF(SEARCH, !trx->Commit(tick, w.position),
                   "search-table commit: iresearch trx Commit failed for "
                   "table ",
                   table_id);
    }
    // Tripwire for the ordering above: parking here leaves the removals queued
    // and visible to the next refresh while this commit has not returned. The
    // delete-log record must already have happened, so it has to sit above the
    // loop -- move it below this point and
    // recovery/search_table_backfill_concurrent_dml.test loses a row.
    SDB_WAIT_ON_FAILURE("pause_search_commit_after_irs");
    // Only now: a rebuild waiting on one of these registrations may proceed as
    // soon as it is released, so the rows have to be committed first.
    ReleaseWriter(w);
  }
}

}  // namespace sdb::search
