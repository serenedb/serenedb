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
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include "connector/search_sink_writer.hpp"
#include "search/row_position_wal.h"

namespace sdb::search {
namespace {

auto Transactions(std::vector<Slot>& slots) {
  return std::views::transform(slots, &Slot::trx);
}

void ReleaseWriter(StoreWrites& w) noexcept {
  if (w.truncate_claim) {
    w.store->ReleaseTruncate();
    w.truncate_claim = false;
  }
  if (w.writer_slot >= 0) {
    w.store->DeregisterWriter(static_cast<unsigned>(w.writer_slot));
    w.writer_slot = -1;
  }
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
  if (w.writer_slot < 0) {
    const auto slot = store->RegisterWriter();
    if (!slot) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
        ERR_MSG("Attempting to write to table ", table.name.GetIdentifierName(),
                " but another transaction is truncating it"));
    }
    w.writer_slot = static_cast<int>(*slot);
  }
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

Writes::PinnedSearchTable Writes::MakePinnedSearchTable(const Store& store) {
  auto config = store.ScanConfig();
  return {
    .reader =
      std::make_shared<irs::DirectoryReader>(store.GetDirectoryReader()),
    .config = std::move(config),
  };
}

const Writes::PinnedSearchTable& Writes::EnsureSearchTable(const Store& store) {
  auto& pinned =
    _readers.try_emplace(std::pair{store.GetDatabaseId(), store.GetId()})
      .first->second;
  if (!pinned.reader) {
    pinned = MakePinnedSearchTable(store);
  }
  return pinned;
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
    if (!store->ClaimTruncate()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
        ERR_MSG("Attempting to truncate table ", table_name.GetIdentifierName(),
                " but another transaction is writing to this table"));
    }
    w.truncate_claim = true;
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
