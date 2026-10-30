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
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/storage/storage_lock.hpp>
#include <duckdb/storage/write_ahead_log.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <duckdb/transaction/transaction_log_writer.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "catalog/entry/search_table.h"
#include "connector/column_id.h"
#include "connector/search_sink_writer.hpp"
#include "search/row_position_wal.h"
#include "search/search_table.h"
#include "server/utils/primary_key.h"

namespace sdb::search {
namespace {

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
  SearchTableLogWriter(LocalTableChangesEntry& changes,
                       SearchShardWrites& writes)
    : _writes{writes},
      _entries{writes.table->catalog.GetAttached(), changes.BufferedBytes()} {
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

  void WriteToWAL(duckdb::WriteAheadLog& wal) final {
    SDB_PARK_ONCE_ON_FAILURE("search_wal_transactions_written");
    irs::IndexWriter::Transaction::RegisterFlush(_writes.transactions);
    SDB_PARK_ONCE_ON_FAILURE("search_wal_transactions_registered");
    bool set_table = true;
    SDB_IF_FAILURE("search_delete_wal_no_table") { set_table = false; }
    if (set_table) {
      wal.WriteSetTable(*_writes.table);
    }
    if (!_removals.empty()) {
      _writes.shard->VisitDocMaps(_removals,
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
  SearchShardWrites& _writes;
  std::vector<const irs::DocRemoval*> _removals;
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
  duckdb::DataChunk& chunk) {
  _changes[shard->GetTableId()].AppendInsertChunk(buffer_manager, types,
                                                  column_ids, catalog, chunk);
}

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
    for (auto& chunk : entry.collection->Chunks()) {
      connector::WriteChunkToSearchSink(*sink, chunk, entry.column_ids,
                                        shard.GetTableId(), context);
    }
  }
  if (!entry.positions.Empty()) {
    trx.Remove(entry.FinishRemoval(PinnedReader(shard)));
  }
}

const irs::DirectoryReader& SearchTableTransaction::PinnedReader(
  const SearchTable& shard) const {
  const auto pinned =
    _readers.find(std::pair{shard.GetDatabaseId(), shard.GetTableId()});
  SDB_ENSURE(pinned != _readers.end(), "search table ", shard.GetTableId(),
             ": deletes without the reader their positions come from");
  return *pinned->second.reader;
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
}

void SearchTableTransaction::FlushBufferTransaction(duckdb::idx_t shard_id) {
  if (const auto it = _writes.find(shard_id);
      it != _writes.end() && it->second.buffer_trx != nullptr) {
    it->second.buffer_trx->Flush();
  }
}

void SearchTableTransaction::AddSearchDeletes(
  const std::shared_ptr<SearchTable>& shard, const duckdb::Vector& positions) {
  EnsureShardWrites(shard);
  _changes[shard->GetTableId()].AppendDeletes(positions);
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
  const bool clears = clears_shard && !shard->BuildInFlight();
  auto& changes = _changes[shard->GetTableId()];
  changes.AppendTruncate(clears);
  if (!clears) {
    auto& trx = *w.transactions.emplace_back(
      std::make_unique<irs::IndexWriter::Transaction>(
        shard->GetTransaction(/*exclusive_segment=*/true)));
    trx.RemoveAll();
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
    const auto it = _changes.find(table_id);
    if (it == _changes.end()) {
      continue;
    }
    auto& entry = it->second;
    if (entry.HasBufferedRows() || !entry.positions.Empty()) {
      SDB_IF_FAILURE("search_feed_pending_fails") {
        THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
      }
      if (w.transactions.empty() ||
          w.transactions.back().get() == w.buffer_trx) {
        w.transactions.push_back(
          std::make_unique<irs::IndexWriter::Transaction>(
            w.shard->GetTransaction()));
      }
      ReplayBuffer(*w.shard, entry, *w.transactions.back(), context);
    }
    if (w.buffer_trx != nullptr) {
      for (const auto& segment : w.buffer_trx->FlushAndFsync()) {
        entry.segments.push_back(segment.filename);
      }
    }
    duckdb::DuckTransaction::Get(context, w.table->catalog)
      .log_writers.push_back(
        duckdb::make_shared_ptr<SearchTableLogWriter>(entry, w));
    entry.ClearBufferedRows();
    if (entry.ClearsShard()) {
      w.checkpoint_lock =
        duckdb::DuckTransactionManager::Get(w.table->catalog.GetAttached())
          .SharedCheckpointLock();
    }
  }
}

void SearchTableTransaction::Stage(const irs::SourcePosition& position,
                                   duckdb::idx_t database) noexcept {
  for (auto& [table_id, w] : _writes) {
    if (w.shard->GetDatabaseId() != database) {
      continue;
    }
    uint64_t queries = 0;
    for (const auto& trx : w.transactions) {
      queries += trx->GetQueries();
    }
    irs::IndexWriter::Transaction::RegisterFlush(w.transactions);
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
    SDB_PARK_ONCE_ON_FAILURE("pause_search_commit_before_build_truncate");
    if (changes != _changes.end() && changes->second.truncate) {
      shard.RecordTruncateForBuild(w.position);
    }
    auto tick = w.first_tick;
    for (auto& trx : w.transactions) {
      tick += trx->GetQueries();
      SDB_FATAL_IF(SEARCH, !trx->Commit(tick, w.position),
                   "search-table commit: iresearch trx Commit failed for "
                   "table ",
                   table_id);
    }
    // Parking here leaves the removals queued and visible to the next refresh
    // while this commit has not returned.
    SDB_WAIT_ON_FAILURE("pause_search_commit_after_irs");
    // Only now: a rebuild waiting on one of these registrations may proceed as
    // soon as it is released, so the rows have to be committed first.
    ReleaseWriter(w);
  }
}

}  // namespace sdb::search
