////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "search/writes.h"

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/storage/write_ahead_log.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <duckdb/transaction/transaction_log_writer.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/file_names.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include "connector/search_key_unique.h"
#include "connector/search_sink_writer.hpp"
#include "connector/search_unique_probe.h"
#include "search/row_position_wal.h"
#include "search/wal_tail_keys.h"

namespace sdb::search {
namespace {

auto Transactions(std::vector<Slot>& slots) {
  return std::views::transform(slots, &Slot::trx);
}

void ReleaseWriter(StoreWrites& w) noexcept {
  w.truncate_claim.reset();
  w.registration.reset();
}

irs::IndexWriter::Transaction& AddTransaction(StoreWrites& w,
                                              bool exclusive_segment) {
  return *w.slots
            .emplace_back(std::make_unique<irs::IndexWriter::Transaction>(
              w.store->GetTransaction(exclusive_segment)))
            .trx;
}

struct Tail {
  irs::IndexWriter::Transaction* trx = nullptr;
  std::unique_ptr<connector::SearchSinkInsertBaseImpl> sink;
};

class SearchTableLogWriter final : public duckdb::TransactionLogWriter {
 public:
  SearchTableLogWriter(StoreWrites& writes, Tail tail,
                       duckdb::ClientContext& context)
    : _writes{writes},
      _tail{std::move(tail)},
      _context{context},
      _entries{writes.table->catalog.GetAttached(),
               writes.changes.BufferedBytes()} {
    auto& changes = writes.changes;
    if (changes.truncate) {
      _entries.WriteTruncateTable();
    }
    if (!changes.segments.empty()) {
      // Everything promoted sits ahead of what is left inline: a removal in
      // this record names rows committed before the transaction, never these.
      _entries.WriteAdoptSegments(std::move(changes.segments));
    }
    if (changes.collection) {
      for (auto& rows : changes.collection->Chunks()) {
        _entries.WriteInsert(rows);
      }
    }
    _removals.reserve(changes.removals.size());
    for (const auto& removal : changes.removals) {
      WriteRemoval(_entries, *removal);
      _removals.push_back(removal.get());
    }
  }

  void Append() final {
    SDB_PARK_ONCE_ON_FAILURE("search_wal_transactions_written");
    auto& changes = _writes.changes;
    if (const auto sink = std::move(_tail.sink)) {
      for (auto& rows : changes.collection->Chunks()) {
        connector::WriteChunkToSearchSink(*sink, rows, changes.column_ids,
                                          _writes.store->GetId(), _context);
      }
    }
    changes.ClearBufferedRows();
    for (const auto& removal : changes.removals) {
      _tail.trx->Remove(removal);
    }
    irs::IndexWriter::Transaction::Stage(Transactions(_writes.slots));
    SDB_PARK_ONCE_ON_FAILURE("search_wal_transactions_registered");
  }

  void WriteToWAL(duckdb::WriteAheadLog& wal) final {
    bool set_table = true;
    SDB_IF_FAILURE("search_delete_wal_no_table") { set_table = false; }
    if (set_table) {
      wal.WriteSetTable(*_writes.table);
    }
    if (!_removals.empty()) {
      _writes.store->VisitDocMaps(_removals,
                                  [&](const std::vector<irs::DocMap>& maps) {
                                    WriteDocMaps(_entries, maps);
                                  });
    }
    SDB_IF_FAILURE("search_delete_wal_malformed_maps") {
      WriteDocMaps(_entries, {irs::DocMap{.source = 1, .target = 1}});
    }
    wal.WriteEntries(_entries);
  }

 private:
  StoreWrites& _writes;
  Tail _tail;
  duckdb::ClientContext& _context;
  duckdb::WALEntryBuffer _entries;
  std::vector<const irs::DocRemoval*> _removals;
};

// Whether a row this transaction is removing is one somebody else removed
// first. Masking its own removals is what lets a transaction write a row back
// under the key it just deleted, and that is only its to do while the row is
// still there: once another transaction has removed it, the key is released,
// and writing it back resurrects a row that transaction deleted.
//
// It is a question about rowids and not about keys, which is why it is asked
// separately: a key probe sees it only where the other transaction happened to
// take the key for a row of its own, and misses the plain case where it only
// removed the row.
void RefuseResurrection(StoreWrites& w, const irs::DirectoryReader& reader,
                        const irs::DocRemoval* removed) {
  if (w.changes.removals.empty()) {
    return;
  }
  // What this transaction means to remove, counted once per document: a row it
  // removes twice is still one row.
  irs::containers::FlatHashMap<uint64_t, irs::DocumentMaskBuilder> sources;
  for (const auto& removal : w.changes.removals) {
    for (const auto& group : removal->groups) {
      sources[group.segment].Merge(group.docs);
    }
  }
  size_t expected = 0;
  for (const auto& [segment, docs] : sources) {
    expected += docs.Count();
  }

  std::vector<irs::DocumentMaskBuilder> ours(reader.size());
  std::vector<irs::DocumentMaskBuilder> gone(reader.size());
  irs::RemovalTargets our_targets;
  irs::RemovalTargets their_targets;
  our_targets.reserve(reader.size());
  their_targets.reserve(reader.size());
  for (size_t i = 0; i != reader.size(); ++i) {
    uint64_t id = 0;
    if (!irs::ParseSegmentId(reader[i].Meta().name, id)) {
      continue;
    }
    our_targets.emplace(
      id, irs::RemovalTarget{.reader = nullptr, .removed = &ours[i]});
    their_targets.emplace(
      id, irs::RemovalTarget{.reader = nullptr, .removed = &gone[i]});
  }
  for (const auto& removal : w.changes.removals) {
    irs::ResolveRemoval(*removal, our_targets);
  }
  if (removed != nullptr) {
    irs::ResolveRemoval(*removed, their_targets);
  }

  // What of it is still there to remove. A document goes missing in three
  // ways, all of them somebody else's removal: it is masked in a segment the
  // reader still holds, the tail says it is gone, or the segment it was in has
  // no live document left and the reader no longer holds it at all -- which is
  // why this counts what arrives rather than looking for what does not.
  size_t mine = 0;
  for (size_t i = 0; i != reader.size(); ++i) {
    if (ours[i].Empty()) {
      continue;
    }
    const auto& segment = reader[i];
    // Everything the reader has already masked is somebody's removal: this
    // transaction's own reach it only when it publishes, which is after this.
    if (const auto* mask = segment.docs_mask()) {
      gone[i].Merge(*mask);
    }
    const auto taken = std::move(ours[i]).Take();
    const auto lost = std::move(gone[i]).Take();
    taken.ForEachRun(&lost, [&](irs::doc_id_t first, irs::doc_id_t last) {
      mine += last - first;
      return true;
    });
  }

  if (mine != expected) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
      ERR_MSG("could not serialize access due to concurrent update of table "
              "\"",
              w.table->name.GetIdentifierName(), "\""));
  }
}

// Everything that holds this table's keys but this transaction, put to the
// keys this transaction writes. The published reader is the settled tier; the
// WAL tail carries what committed since, as bare keys where the transaction
// did not flush and as whole segments where it did. This transaction's own
// rows are not in the tail: its record reaches the WAL after this runs.
//
// `whole` marks the pass that answers for the whole index; the one that only
// catches up leaves the settled tiers out, because what the first pass settled
// stays settled: publishing moves a row from the tail to the reader without
// adding one, and a removal only ever takes a key away.
void CertifyAgainst(StoreWrites& w, duckdb::AttachedDatabase& db,
                    duckdb::ClientContext& context,
                    std::span<WalTailFile> files,
                    const irs::DirectoryReader& reader, bool whole) {
  auto& changes = w.changes;
  if (w.keys.empty() && changes.collection) {
    connector::AppendRowKeys(*changes.collection, w.pk_slots, 0, w.keys);
  }
  std::vector<irs::SegmentReader> flushed;
  flushed.reserve(changes.segments.size());
  for (const auto& segment : changes.segments) {
    flushed.push_back(w.store->OpenSegment(segment));
  }
  // A transaction that only removes rows takes no key back and resurrects
  // nothing, so it has nothing to answer for.
  if (w.keys.empty() && flushed.empty()) {
    return;
  }

  WalTailKeyCollector collector{w.table->oid, w.pk_slots};
  w.certified = ReadTailKeys(db, context, w.certified, files, collector);
  auto tail = collector.TakeKeys();

  // A search table logs its removals as document positions; the rowid-key form
  // belongs to an inverted index replaying a table's deletes, and never
  // reaches this tail.
  const auto tail_removed = std::move(tail.removals).Resolve(reader);

  RefuseResurrection(w, reader, tail_removed.get());

  std::vector<irs::SegmentReader> unpublished;
  unpublished.reserve(tail.segments.size());
  for (const auto& segment : tail.segments) {
    unpublished.push_back(w.store->OpenSegment(segment));
  }
  std::vector<const irs::SubReader*> unpublished_segments;
  unpublished_segments.reserve(unpublished.size());
  for (const auto& segment : unpublished) {
    unpublished_segments.push_back(&segment);
  }
  std::vector<const connector::KeyTerms*> unpublished_keys;
  unpublished_keys.reserve(tail.keys.size());
  for (const auto& key : tail.keys) {
    unpublished_keys.push_back(&key);
  }
  std::vector<const irs::SubReader*> own_segments;
  own_segments.reserve(flushed.size());
  for (const auto& segment : flushed) {
    own_segments.push_back(&segment);
  }

  // This transaction's own removals mask the probe alongside the tail's: an
  // UPDATE deletes the row it rewrites, and by the check above that row was
  // still its own, so the key it writes back is its own to take.
  std::vector<const irs::DocRemoval*> removed_docs;
  removed_docs.reserve(changes.removals.size() + 1);
  for (const auto& removal : changes.removals) {
    removed_docs.push_back(removal.get());
  }
  if (tail_removed) {
    removed_docs.push_back(tail_removed.get());
  }

  std::vector<irs::field_id> fields;
  fields.reserve(w.pk_slots.size());
  for (const auto& slot : w.pk_slots) {
    fields.push_back(slot.column);
  }

  // A TRUNCATE in the tail kills every row the reader still shows, so the tier
  // goes with it rather than being consulted and answering for dead rows.
  connector::UniqueProbe probe{whole && !tail.truncated ? &reader : nullptr,
                               unpublished_segments,
                               std::move(unpublished_keys),
                               fields,
                               {.docs = removed_docs}};
  if (connector::FindDuplicateKey(own_segments, w.keys, w.pk_slots, probe)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNIQUE_VIOLATION),
      ERR_MSG("duplicate key value violates the primary key of table \"",
              w.table->name.GetIdentifierName(), "\""));
  }
}

}  // namespace

StoreWrites& Writes::EnsureStoreWrites(const std::shared_ptr<Store>& store) {
  auto& w = _writes[store->GetId()];
  if (!w.store) {
    w.store = store;
  }
  return w;
}

void Writes::RegisterWriter(const std::shared_ptr<Store>& store,
                            const catalog::SearchTableEntry& table) {
  auto& w = EnsureStoreWrites(store);
  w.table = &table;
  if (!w.registration) {
    w.registration = store->RegisterWriter();
    if (!w.registration) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
        ERR_MSG("Attempting to write to table ", table.name.GetIdentifierName(),
                " but another transaction is truncating it"));
    }
    w.pk_slots = connector::primary_key::KeySlots(table, *store->Config());
  }
}

void Writes::Certify(
  duckdb::AttachedDatabase& db, duckdb::ClientContext& context,
  duckdb::optional_ptr<duckdb::unique_lock<duckdb::mutex>> held) {
  std::optional<duckdb::unique_lock<duckdb::mutex>> taken;
  for (auto& [store_id, w] : _writes) {
    if (!w.table || w.pk_slots.empty() || w.store->GetDatabaseId() != db.oid) {
      continue;
    }
    if (!held && !taken) {
      // The prepared path leaves the lock to the database that owns the
      // catalog, so a pass firing there takes its own; it is not recursive, so
      // only where it was handed none.
      taken = db.GetStorageManager().GetCommitLock();
    }
    if (w.pre_certified) {
      // Only what committed since the pass before the lock: the reader is
      // here to carry removals onto the segments, not as a tier of its own.
      auto files = OpenTailFiles(db, w.certified);
      const auto reader = w.store->GetDirectoryReader();
      CertifyAgainst(w, db, context, files, reader, /*whole=*/false);
      continue;
    }
    auto [reader, position] = w.store->SnapshotWithPosition();
    w.certified = {.generation = position.generation,
                   .offset = position.offset};
    auto files = OpenTailFiles(db, w.certified);
    CertifyAgainst(w, db, context, files, reader, /*whole=*/true);
  }
}

// The pass before the commit lock, where the whole index is answered for and
// the reading is done without holding anything up. Only resolving and opening
// the WAL files needs the lock, and only for as long as that takes.
void Writes::CertifyPrepared(StoreWrites& w, duckdb::ClientContext& context) {
  if (!w.table || w.pk_slots.empty()) {
    return;
  }
  auto& db = w.table->catalog.GetAttached();
  // The reader and the position come from one call: a reader older than the
  // position it is paired with would leave a span of the WAL in neither tier.
  auto [reader, position] = w.store->SnapshotWithPosition();
  w.certified = {.generation = position.generation, .offset = position.offset};
  std::vector<WalTailFile> files;
  {
    // A shared checkpoint key, not the commit lock: what renames or removes a
    // WAL file is a checkpoint, and a checkpoint holds the checkpoint lock
    // exclusively for its whole run, so this excludes the only mutator without
    // standing in the way of other commits. What it does not fix is the end of
    // the live WAL, since commits keep appending -- which costs nothing, as a
    // read that lands in a batch still being decided stops there and says so,
    // and the pass under the commit lock resumes from exactly that point.
    const auto key =
      duckdb::DuckTransactionManager::Get(db).SharedCheckpointLock();
    files = OpenTailFiles(db, w.certified);
  }
  CertifyAgainst(w, db, context, files, reader, /*whole=*/true);
  w.pre_certified = true;
}

void Writes::AddParallelSearchTransaction(
  const std::shared_ptr<Store>& store,
  std::unique_ptr<irs::IndexWriter::Transaction> trx) {
  EnsureStoreWrites(store).slots.emplace_back(std::move(trx));
}

void Writes::AddSegments(const std::shared_ptr<Store>& store,
                         std::vector<std::string>&& segments) {
  EnsureStoreWrites(store).changes.AppendSegments(std::move(segments));
}

void Writes::AddInlineRows(const std::shared_ptr<Store>& store,
                           std::span<const connector::ColumnId> column_ids,
                           duckdb::Catalog& catalog,
                           std::unique_ptr<duckdb::ColumnDataCollection> rows) {
  EnsureStoreWrites(store).changes.AppendInsertRows(std::move(rows), column_ids,
                                                    catalog);
}

const irs::DirectoryReader& Writes::PinnedReader(const Store& store) const {
  const auto pinned =
    _readers.find(std::pair{store.GetDatabaseId(), store.GetId()});
  SDB_ENSURE(pinned != _readers.end(), "search table ", store.GetId(),
             ": deletes without the reader their positions come from");
  return *pinned->second.reader;
}

void Writes::FlushBuffer(const std::shared_ptr<Store>& store,
                         duckdb::ClientContext& context) {
  auto& w = EnsureStoreWrites(store);
  auto& changes = w.changes;
  SDB_ASSERT(changes.HasBufferedRows());
  if (!w.buffer_trx) {
    // Exclusive: its segments are named in the record, so they must not also
    // carry another transaction's documents.
    w.buffer_trx = &AddTransaction(w, /*exclusive_segment=*/true);
  }
  const auto sink = connector::MakeSearchTableInsertSink(
    *w.buffer_trx, *store, *changes.catalog, context);
  for (auto& chunk : changes.collection->Chunks()) {
    connector::WriteChunkToSearchSink(*sink, chunk, changes.column_ids,
                                      store->GetId(), context);
  }
  // The rows are this transaction's to make durable now, so the record stops
  // carrying them. The removals stay: no segment holds those.
  changes.ClearBufferedRows();
}

void Writes::FlushBufferTransaction(duckdb::idx_t store_id) {
  if (const auto it = _writes.find(store_id);
      it != _writes.end() && it->second.buffer_trx) {
    it->second.buffer_trx->Flush();
  }
}

void Writes::AddRemoval(const std::shared_ptr<Store>& store,
                        irs::DocRemovalBuilder&& positions) {
  EnsureStoreWrites(store).changes.removals.push_back(
    std::move(positions).Finish(PinnedReader(*store)));
}

void Writes::AddSearchTruncate(const std::shared_ptr<Store>& store,
                               const duckdb::Identifier& table_name,
                               bool clears_store) {
  auto& w = EnsureStoreWrites(store);
  if (!w.truncate_claim) {
    w.truncate_claim = store->ClaimTruncate();
    if (!w.truncate_claim) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
        ERR_MSG("Attempting to truncate table ", table_name.GetIdentifierName(),
                " but another transaction is writing to this table"));
    }
  }
  w.slots.clear();
  w.buffer_trx = nullptr;
  const bool clears = clears_store && !store->BuildInFlight();
  w.changes.AppendTruncate(clears);
  if (!clears) {
    AddTransaction(w, /*exclusive_segment=*/true).RemoveAll();
  }
}

Slot& Writes::EnsureIndexSlot(
  duckdb::idx_t index_id, std::shared_ptr<Store> store,
  std::shared_ptr<const catalog::InvertedIndexConfig> config, size_t slot) {
  auto& w = _writes[index_id];
  if (!w.store) {
    w.store = std::move(store);
  }
  if (w.slots.size() <= slot) {
    w.slots.resize(slot + 1);
  }
  auto& result = w.slots[slot];
  if (!result.trx) {
    result.trx = std::make_unique<irs::IndexWriter::Transaction>(
      w.store->GetTransaction(/*exclusive_segment=*/slot != 0));
    result.trx->SetFieldOptions(std::move(config));
  }
  return result;
}

const duckdb::Vector& Writes::FeedColumn(
  const void* database, duckdb::idx_t table_oid, duckdb::row_t first_row,
  duckdb::idx_t count, duckdb::idx_t column, const duckdb::Vector& source) {
  if (_feed_columns.database != database ||
      _feed_columns.table_oid != table_oid ||
      _feed_columns.first_row != first_row || _feed_columns.count != count) {
    _feed_columns.database = database;
    _feed_columns.table_oid = table_oid;
    _feed_columns.first_row = first_row;
    _feed_columns.count = count;
    _feed_columns.columns.clear();
  }
  for (const auto& [index, copy] : _feed_columns.columns) {
    if (index == column) {
      return copy;
    }
  }
  auto& copy = _feed_columns.columns
                 .emplace_back(column, duckdb::Vector{source.GetType(), count})
                 .second;
  duckdb::VectorOperations::Copy(source, copy, count, 0, 0);
  duckdb::FlatVector::SetSize(copy, count);
  return copy;
}

void Writes::PrepareCommit(duckdb::ClientContext& context) {
  for (auto& [store_id, w] : _writes) {
    auto& changes = w.changes;
    if (!w.table || (changes.Empty() && !w.buffer_trx)) {
      continue;
    }
    Tail tail;
    if (changes.HasBufferedRows() || !changes.removals.empty()) {
      SDB_IF_FAILURE("search_feed_pending_fails") {
        THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
      }
      tail.trx = &AddTransaction(w, /*exclusive_segment=*/false);
      if (changes.HasBufferedRows()) {
        tail.sink = connector::MakeSearchTableInsertSink(
          *tail.trx, *w.store, *changes.catalog, context);
      }
    }
    if (w.buffer_trx) {
      for (const auto& segment : w.buffer_trx->FlushAndFsync()) {
        changes.segments.push_back(segment.filename);
      }
    }
    CertifyPrepared(w, context);
    duckdb::DuckTransaction::Get(context, w.table->catalog)
      .log_writers.push_back(duckdb::make_shared_ptr<SearchTableLogWriter>(
        w, std::move(tail), context));
    if (changes.ClearsShard()) {
      w.checkpoint_lock =
        duckdb::DuckTransactionManager::Get(w.table->catalog.GetAttached())
          .SharedCheckpointLock();
    }
    SDB_PARK_ONCE_ON_FAILURE("search_commit_prepared");
  }
}

void Writes::Stage(const irs::SourcePosition& position,
                   duckdb::idx_t database) noexcept {
  for (auto& [store_id, w] : _writes) {
    if (w.store->GetDatabaseId() != database) {
      continue;
    }
    w.position = position;
    if (w.table) {
      w.store->MarkStaged(position);
      continue;
    }
    for (auto& slot : w.slots) {
      slot.writer.reset();
    }
    irs::IndexWriter::Transaction::Stage(Transactions(w.slots));
    SDB_PARK_ONCE_ON_FAILURE("inverted_stage_slots_registered");
  }
}

void Writes::Publish(duckdb::idx_t database) noexcept {
  for (auto& [store_id, w] : _writes) {
    if (w.store->GetDatabaseId() != database) {
      continue;
    }
    SDB_PARK_ONCE_ON_FAILURE("pause_search_commit_before_irs");
    auto& store = *w.store;
    if (w.changes.ClearsShard()) {
      try {
        store.Clear(w.position);
      } catch (const std::exception& e) {
        SDB_FATAL(SEARCH, "search-table commit: Clear failed for table ",
                  store_id, ": ", duckdb::ErrorData{e}.RawMessage());
      }
    }
    w.checkpoint_lock.reset();
    SDB_PARK_ONCE_ON_FAILURE("pause_search_commit_before_build_truncate");
    if (w.changes.truncate) {
      store.RecordTruncateForBuild(w.position);
    }
    for (auto& slot : w.slots) {
      if (!slot.trx->Commit(w.position)) {
        SDB_ERROR(SEARCH, "search store ", store_id,
                  ": commit failed; the store replays it from the WAL on "
                  "next boot");
        store.MarkOutOfSync();
      }
    }
    // Parking here leaves the removals queued and visible to the next refresh
    // while this commit has not returned.
    SDB_WAIT_ON_FAILURE("pause_search_commit_after_irs");
    // Only now: a rebuild waiting on one of these registrations may proceed as
    // soon as it is released, so the rows have to be committed first.
    ReleaseWriter(w);
  }
}

void Writes::Abort() noexcept {
  for (auto& [store_id, w] : _writes) {
    w.slots.clear();
    ReleaseWriter(w);
  }
  _writes.clear();
  _readers.clear();
}

}  // namespace sdb::search
