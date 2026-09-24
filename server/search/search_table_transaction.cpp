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

#include "catalog/duckdb_primary_key.h"
#include "connector/search_sink_writer.hpp"
#include "search/search_db_wal.h"
#include "search/search_table.h"
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
                           const LocalTableChangesEntry& changes) {
  std::vector<int64_t> rows;
  for (const auto& op : changes.ops) {
    rows.insert(rows.end(), op.delete_rows.begin(), op.delete_rows.end());
  }
  shard.AppendDeleteLog(rows);
}

}  // namespace

SearchTableTransaction::~SearchTableTransaction() { ReleaseWriters(); }

void SearchTableTransaction::RegisterWriter(
  const std::shared_ptr<SearchTable>& shard) {
  auto& w = _writes[shard->GetTableId()];
  if (!w.shard) {
    w.shard = shard;
  }
  if (w.writer_slot < 0) {
    w.writer_slot = static_cast<int>(shard->RegisterWriter());
  }
}

void SearchTableTransaction::ReleaseWriters() noexcept {
  for (auto& [table_id, w] : _writes) {
    if (w.writer_slot >= 0) {
      w.shard->DeregisterWriter(static_cast<unsigned>(w.writer_slot));
      w.writer_slot = -1;
    }
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
  std::vector<SearchDbWal::SegmentRef>&& segments) {
  _changes[shard->GetTableId()].AppendSegments(std::move(segments));
}

void SearchTableTransaction::AddInlineInsertChunk(
  const std::shared_ptr<SearchTable>& shard,
  duckdb::BufferManager& buffer_manager,
  const duckdb::vector<duckdb::LogicalType>& types,
  std::span<const catalog::ColumnId> column_ids, duckdb::DataChunk& chunk,
  uint64_t pk_base) {
  _changes[shard->GetTableId()].AppendInsertChunk(buffer_manager, types,
                                                  column_ids, chunk, pk_base);
}

// Replays a shard's buffer into `trx` in issue order: rows through a sink,
// removals through the transaction, each removal landing after exactly the rows
// that preceded it. Feeding in this order is what reproduces the `_queries`
// stamping the statements would have produced had they written directly.
void SearchTableTransaction::ReplayBuffer(SearchTable& shard,
                                          LocalTableChangesEntry& entry,
                                          irs::IndexWriter::Transaction& trx,
                                          duckdb::ClientContext& context) {
  // Band index -> rows before it, so an op's watermark can be compared against
  // the running row count of a single forward pass.
  std::vector<uint64_t> op_rows;
  op_rows.reserve(entry.ops.size());
  {
    std::vector<uint64_t> prefix;
    prefix.reserve(entry.pk_segments.size() + 1);
    uint64_t total = 0;
    prefix.push_back(0);
    for (const auto& band : entry.pk_segments) {
      total += band.count;
      prefix.push_back(total);
    }
    for (const auto& op : entry.ops) {
      const auto band = std::min<size_t>(op.band_watermark, prefix.size() - 1);
      op_rows.push_back(prefix[band]);
    }
  }

  const auto table_id = shard.GetTableId();
  connector::SearchSinkDeleteBaseImpl remover{trx};
  std::string key;
  size_t op_idx = entry.applied_ops;
  uint64_t emitted = 0;

  auto apply_op = [&](const LocalTableChangesEntry::Op& op) {
    if (op.IsTruncate()) {
      if (!op.clears_shard) {
        trx.Remove(std::make_shared<irs::All>());
      }
      return;
    }
    remover.InitImpl(op.delete_rows.size());
    for (const auto row : op.delete_rows) {
      key.clear();
      catalog::duckdb_primary_key::AppendGenerated(key,
                                                   static_cast<uint64_t>(row));
      remover.DeleteRowImpl(key);
    }
    remover.FinishImpl();
  };
  auto drain_ops = [&] {
    while (op_idx < entry.ops.size() && op_rows[op_idx] <= emitted) {
      apply_op(entry.ops[op_idx]);
      ++op_idx;
    }
  };

  // Drain removes before the inserts
  drain_ops();
  if (entry.HasBufferedRows()) {
    auto sink = connector::MakeSearchTableInsertSink(trx, shard, context);
    entry.VisitBufferedRows([&](duckdb::DataChunk& chunk, uint64_t pk_base) {
      connector::WriteChunkToSearchSink(*sink, chunk, entry.column_ids, pk_base,
                                        table_id, context);
      emitted += chunk.size();
      // now some removes may become valid - emit them
      drain_ops();
    });
  }
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
  const std::shared_ptr<SearchTable>& shard, bool clears_shard) {
  auto& w = _writes[shard->GetTableId()];
  if (!w.shard) {
    w.shard = shard;
  }
  w.transactions.clear();
  w.buffer_trx = nullptr;
  _changes[shard->GetTableId()].AppendTruncate(clears_shard);
}

void SearchTableTransaction::RegisterFlush() noexcept {
  for (auto& [table_id, w] : _writes) {
    for (auto& trx : w.transactions) {
      trx->RegisterFlush();
    }
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

void SearchTableTransaction::Commit() {
  SDB_ASSERT(!_writes.empty());
  if (_changes.empty()) {
    ReleaseWriters();
    return;
  }
  // The one and the only flush a transaction gets
  for (auto& [table_id, w] : _writes) {
    if (w.buffer_trx == nullptr) {
      continue;
    }
    const auto flushed = w.buffer_trx->FlushAndFsync();
    if (flushed.empty()) {
      continue;
    }
    std::vector<SearchDbWal::SegmentRef> refs;
    refs.reserve(flushed.size());
    for (const auto& segment : flushed) {
      refs.push_back(SearchDbWal::SegmentRef{
        .meta_file = segment.filename,
        .codec = std::string{segment.meta.codec->type()().name()}});
    }
    _changes[table_id].AppendSegments(std::move(refs));
  }

  SDB_IF_FAILURE("crash_before_search_wal_commit") { SDB_IMMEDIATE_ABORT(); }

  const uint64_t record_tick = AppendCommit();
  SDB_IF_FAILURE("crash_after_search_wal_commit") { SDB_IMMEDIATE_ABORT(); }

  for (auto& [table_id, w] : _writes) {
    auto cit = _changes.find(table_id);
    // Before the iresearch commit, never after. Once a removal is queued, the
    // next RefreshCommit applies it -- including the one a running build
    // publishes its own swap with -- and a build that drained the log before
    // this ran would never reissue it, leaving the rows it had already copied
    // resurrected for good. AppendCommit has made these durable, so the log can
    // only ever name rows that are certainly deleted; that, not the position
    // relative to the iresearch commit, is what keeps a reissue from removing a
    // live row.
    if (cit != _changes.end() && w.shard->IsDeleteLogOpen()) {
      RecordDeletesForBuild(*w.shard, cit->second);
    }

    uint64_t tick = record_tick;
    for (size_t i = w.transactions.size(); i-- > 0;) {
      auto& trx = *w.transactions[i];

      const bool committed = trx.Commit(tick);
      SDB_FATAL_IF(
        SEARCH, !committed,
        "search-table commit: iresearch trx Commit failed for table ",
        table_id.id(), " tick=", tick);
      tick -= trx.GetQueries() + 1;
    }

    // Tripwire for the ordering above: parking here leaves the removals queued
    // and visible to the next refresh while this commit has not returned. The
    // delete-log record must already have happened, so it has to sit above the
    // loop -- move it below this point and
    // recovery/search_table_backfill_concurrent_dml.test loses a row.
    SDB_WAIT_ON_FAILURE("pause_search_commit_after_irs");

    if (cit != _changes.end() && cit->second.ClearsShard()) {
      w.shard->Clear(record_tick);
    }
  }
  // Only now: a rebuild waiting on one of these registrations may proceed as
  // soon as it is released, so the rows have to be committed first.
  ReleaseWriters();
}

uint64_t SearchTableTransaction::AppendCommit() {
  SDB_ASSERT(!_writes.empty());
  std::vector<SearchDbWal::ShardSection> sections;
  sections.reserve(_writes.size());
  std::vector<std::vector<SearchDbWal::Entry>> entry_lists;
  entry_lists.reserve(_writes.size());
  // Widest shard band -> ticks this commit reserves; every shard tops out here.
  uint64_t tick_span = 0;
  SearchDbWal* wal = &_writes.begin()->second.shard->Wal();
  for (auto& [table_id, w] : _writes) {
    SDB_ASSERT(wal == &w.shard->Wal(),
               "all search shards in a txn must share one database WAL");
    auto cit = _changes.find(table_id);
    if (cit == _changes.end()) {
      SDB_ASSERT(w.transactions.empty(),
                 "search shard with a trx but no manifest ops");
      continue;
    }
    auto& entry = cit->second;
    // A clearing TRUNCATE adds no trx but needs one tick for its Clear at the
    // band top.
    uint64_t shard_span = ShardTickSpan(w) + (entry.ClearsShard() ? 1 : 0);
    tick_span = std::max(tick_span, shard_span);

    // Entries in issue order: a row run for the bands before each op, then
    // the op. Position is the ordering, so nothing carries a watermark.
    auto& entries = entry_lists.emplace_back();
    entries.reserve(entry.ops.size() * 2 + 2);
    if (!entry.segments.empty()) {
      // Everything promoted sits ahead of what is left inline: a removal in
      // this record names rows committed before the transaction, never these.
      entries.push_back(SearchDbWal::Entry{
        .kind = SearchDbWal::Entry::Kind::kSegments,
        .segments = std::span<const SearchDbWal::SegmentRef>{entry.segments}});
    }
    uint32_t band = 0;
    const auto band_count = static_cast<uint32_t>(entry.pk_segments.size());
    auto emit_rows_to = [&](uint32_t upto) {
      if (band >= upto) {
        return;
      }
      entries.push_back(
        SearchDbWal::Entry{.kind = SearchDbWal::Entry::Kind::kRows,
                           .first_band = band,
                           .last_band = upto});
      band = upto;
    };
    for (const auto& op : entry.ops) {
      emit_rows_to(std::min(op.band_watermark, band_count));
      entries.push_back(SearchDbWal::Entry{
        .kind = op.truncate ? SearchDbWal::Entry::Kind::kTruncate
                            : SearchDbWal::Entry::Kind::kDelete,
        .delete_rows = std::span<const int64_t>{op.delete_rows}});
    }
    emit_rows_to(band_count);

    SearchDbWal::ShardSection section;
    section.table_id = table_id;
    section.inline_data = entry.collection.get();
    section.inline_pks =
      std::span<const SearchDbWal::InlinePk>{entry.pk_segments};
    section.entries = std::span<const SearchDbWal::Entry>{entries};
    SDB_ASSERT(!section.entries.empty(),
               "search-table commit with neither rows, segments nor ops");
    sections.push_back(section);
  }

  SDB_ASSERT(wal != nullptr);
  return wal->AppendCommit(sections, tick_span);
}

}  // namespace sdb::search
