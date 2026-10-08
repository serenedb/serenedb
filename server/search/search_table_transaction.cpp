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

#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/storage/write_ahead_log.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <duckdb/transaction/transaction_log_writer.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <span>
#include <string>
#include <vector>

#include "catalog/entry/search_table.h"
#include "connector/column_id.h"
#include "connector/search_sink_writer.hpp"
#include "search/search_table.h"
#include "search/tick_domain.h"
#include "server/utils/primary_key.h"

namespace sdb::search {
namespace {

// Width of this shard's iresearch tick band: sum-over-trxs(GetQueries()+1), so
// each trx gets a tick strictly above its predecessor (iresearch's <= removal
// rule). Pure inserts have GetQueries()==0 -> one tick per trx.
uint64_t ShardTickSpan(const SearchShardWrites& w) {
  uint64_t span = 0;
  for (const auto& trx : w.transactions) {
    span += trx->GetQueries() + 1;
  }
  return span;
}

// The rowids this transaction deleted. The buffer already holds them raw, so a
// build's log takes them as they are.
void RecordDeletesForBuild(SearchTable& shard,
                           const LocalTableChangesEntry& changes,
                           uint64_t record_tick) noexcept {
  std::vector<int64_t> rows;
  bool truncated = false;
  for (const auto& op : changes.ops) {
    truncated |= op.IsTruncate();
    rows.insert(rows.end(), op.delete_rows.begin(), op.delete_rows.end());
  }
  shard.AppendDeleteLog(rows);
  if (truncated) {
    shard.RecordTruncateForBuild(record_tick);
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
  SearchTableLogWriter(SearchShardWrites& writes,
                       LocalTableChangesEntry& changes)
    : _writes{writes}, _changes{changes} {}

  void WriteToWAL(duckdb::WriteAheadLog& wal) final {
    for (auto& trx : _writes.transactions) {
      trx->RegisterFlush();
    }
    // A clearing TRUNCATE adds no trx but needs one tick for its Clear at the
    // band top.
    _tick = TickDomain::Instance().Next(ShardTickSpan(_writes) +
                                        (_changes.ClearsShard() ? 1 : 0));
    wal.WriteSetTable(*_writes.table, _tick);
    if (!_changes.segments.empty()) {
      // Everything promoted sits ahead of what is left inline: a removal in
      // this record names rows committed before the transaction, never these.
      wal.WriteAdoptSegments(duckdb::vector<std::string>(
        _changes.segments.begin(), _changes.segments.end()));
    }
    _changes.Visit(
      0,
      [&](duckdb::DataChunk& rows, uint64_t pk_base) {
        wal.WriteInsert(rows, pk_base);
      },
      [&](LocalTableChangesEntry::Op& op) {
        if (op.IsTruncate()) {
          wal.WriteTruncateTable();
          return;
        }
        duckdb::DataChunk rows;
        rows.InitializeEmpty({duckdb::LogicalType::ROW_TYPE});
        rows.data[0].Reference(duckdb::Vector(
          duckdb::LogicalType::ROW_TYPE,
          duckdb::data_ptr_cast(op.delete_rows.data()), op.delete_rows.size()));
        rows.SetChildCardinality(op.delete_rows.size());
        wal.WriteDelete(rows);
      });
  }

  void OnDurable() noexcept final {
    SDB_ASSERT(_tick != 0);
    SDB_PARK_ONCE_ON_FAILURE("pause_search_commit_before_irs");
    auto& shard = *_writes.shard;
    if (_changes.ClearsShard()) {
      try {
        shard.Clear(_tick);
      } catch (const std::exception& e) {
        SDB_FATAL(SEARCH, "search-table commit: Clear failed for table ",
                  shard.GetTableId(), " tick=", _tick, ": ", e.what());
      }
    }
    SDB_PARK_ONCE_ON_FAILURE("pause_search_commit_before_delete_log");
    if (!_changes.ops.empty() && shard.IsDeleteLogOpen()) {
      RecordDeletesForBuild(shard, _changes, _tick);
    }

    uint64_t tick = _tick;
    for (size_t i = _writes.transactions.size(); i-- > 0;) {
      auto& trx = *_writes.transactions[i];

      const bool committed = trx.Commit(tick);
      SDB_FATAL_IF(
        SEARCH, !committed,
        "search-table commit: iresearch trx Commit failed for table ",
        shard.GetTableId(), " tick=", tick);
      tick -= trx.GetQueries() + 1;
    }

    // Tripwire for the ordering above: parking here leaves the removals queued
    // and visible to the next refresh while this commit has not returned. The
    // delete-log record must already have happened, so it has to sit above the
    // loop -- move it below this point and
    // recovery/search_table_backfill_concurrent_dml.test loses a row.
    SDB_WAIT_ON_FAILURE("pause_search_commit_after_irs");
    // Only now: a rebuild waiting on one of these registrations may proceed as
    // soon as it is released, so the rows have to be committed first.
    ReleaseWriter(_writes);
  }

 private:
  SearchShardWrites& _writes;
  LocalTableChangesEntry& _changes;
  uint64_t _tick = 0;
};

}  // namespace

SearchTableTransaction::~SearchTableTransaction() { ReleaseWriters(); }

void SearchTableTransaction::RegisterWriter(
  const std::shared_ptr<SearchTable>& shard,
  const catalog::SearchTableEntry& table) {
  auto& w = _writes[shard->GetTableId()];
  if (!w.shard) {
    w.shard = shard;
  }
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
  auto& w = _writes[shard->GetTableId()];
  if (!w.shard) {
    w.shard = shard;
  }
  w.transactions.push_back(std::move(trx));
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
      if (!op.IsTruncate()) {
        connector::RemoveGeneratedRows(trx, op.delete_rows);
      } else if (!op.clears_shard) {
        trx.Remove(std::make_shared<irs::All>());
      }
    });
  entry.applied_ops = entry.ops.size();
}

irs::IndexWriter::Transaction& SearchTableTransaction::EnsureBufferTransaction(
  const std::shared_ptr<SearchTable>& shard) {
  auto& w = _writes[shard->GetTableId()];
  if (!w.shard) {
    w.shard = shard;
  }
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

irs::IndexWriter::Transaction&
SearchTableTransaction::EnsureSerialSearchTransaction(
  const std::shared_ptr<SearchTable>& shard,
  absl::AnyInvocable<irs::IndexWriter::Transaction()> make_trx) {
  auto& w = _writes[shard->GetTableId()];
  if (!w.shard) {
    w.shard = shard;
  }
  if (w.transactions.empty()) {
    w.transactions.push_back(
      std::make_unique<irs::IndexWriter::Transaction>(make_trx()));
  }
  return *w.transactions.back();
}

void SearchTableTransaction::AddSearchDeletes(
  const std::shared_ptr<SearchTable>& shard, std::span<const int64_t> rows) {
  auto& w = _writes[shard->GetTableId()];
  if (!w.shard) {
    w.shard = shard;
  }
  _changes[shard->GetTableId()].AppendDeletes(rows);
}

void SearchTableTransaction::AddSearchTruncate(
  const std::shared_ptr<SearchTable>& shard,
  const duckdb::Identifier& table_name, bool clears_shard) {
  auto& w = _writes[shard->GetTableId()];
  if (!w.shard) {
    w.shard = shard;
  }
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
  _changes[shard->GetTableId()].AppendTruncate(clears_shard &&
                                               !shard->BuildInFlight());
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

void SearchTableTransaction::FlushPending(duckdb::ClientContext& context) {
  for (auto& [table_id, w] : _writes) {
    auto it = _changes.find(table_id);
    if (it == _changes.end()) {
      continue;
    }
    auto& entry = it->second;
    if (!entry.HasBufferedRows() && entry.ops.empty()) {
      continue;
    }

    SDB_IF_FAILURE("search_feed_pending_fails") {
      THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
    }
    if (w.buffer_trx != nullptr) {
      // Already overran once: the remainder joins the same transaction, whose
      // segments the record names, so nothing is left to carry inline.
      ReplayBuffer(*w.shard, entry, *w.buffer_trx, context);
      entry.ClearBufferedRows();
      continue;
    }
    // Never overran: a pooled transaction keeps the freelist coalescing that
    // makes small writes cheap, and the record carries the rows inline.
    auto& trx = EnsureSerialSearchTransaction(
      w.shard, [&] { return w.shard->GetTransaction(); });
    ReplayBuffer(*w.shard, entry, trx, context);
  }
}

void SearchTableTransaction::PrepareCommit(duckdb::ClientContext& context) {
  FlushPending(context);
  // The one and the only flush a transaction gets
  for (auto& [table_id, w] : _writes) {
    if (w.buffer_trx == nullptr) {
      continue;
    }
    const auto flushed = w.buffer_trx->FlushAndFsync();
    if (flushed.empty()) {
      continue;
    }
    std::vector<std::string> refs;
    refs.reserve(flushed.size());
    for (const auto& segment : flushed) {
      refs.push_back(segment.filename);
    }
    _changes[table_id].AppendSegments(std::move(refs));
  }

  for (auto& [table_id, w] : _writes) {
    auto changes = _changes.find(table_id);
    if (changes == _changes.end()) {
      continue;
    }
    duckdb::DuckTransaction::Get(context, w.table->catalog)
      .log_writers.push_back(
        duckdb::make_shared_ptr<SearchTableLogWriter>(w, changes->second));
  }
}

}  // namespace sdb::search
