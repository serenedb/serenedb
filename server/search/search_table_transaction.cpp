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
#include <span>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/log.h"
#include "basics/system-compiler.h"
#include "search/search_db_wal.h"
#include "search/search_table_shard.h"
#include "storage_engine/table_shard.h"

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

}  // namespace

void SearchTableTransaction::AddParallelSearchTransaction(
  const std::shared_ptr<TableShard>& shard,
  std::unique_ptr<irs::IndexWriter::Transaction> trx,
  SearchDbWal::PendingChunk chunk) {
  auto& w = _writes[shard->GetTableId()];
  if (!w.shard) {
    w.shard = shard;
  }
  w.transactions.push_back(std::move(trx));
  w.chunks.push_back(std::move(chunk));
}

void SearchTableTransaction::AddReferences(
  const std::shared_ptr<TableShard>& shard, std::span<const uint64_t> seg_ids) {
  // One batched call per bulk statement (after its parallel sinks combine), so
  // the manifest's current insert run is resolved once -- and the manifest is
  // never touched from the multi-threaded Combine path.
  _changes[shard->GetTableId()].AppendReference(seg_ids);
}

void SearchTableTransaction::AddInlineInsertChunk(
  const std::shared_ptr<TableShard>& shard,
  duckdb::BufferManager& buffer_manager,
  const duckdb::vector<duckdb::LogicalType>& types, duckdb::DataChunk& chunk,
  bool uses_generated_pk, uint64_t pk_base) {
  // The destination shard + serial trx are recorded by
  // EnsureSerialSearchTransaction (the inline Sink runs it first); here we only
  // grow the ordered op manifest.
  _changes[shard->GetTableId()].AppendInsertChunk(buffer_manager, types, chunk,
                                                  uses_generated_pk, pk_base);
}

irs::IndexWriter::Transaction&
SearchTableTransaction::EnsureSerialSearchTransaction(
  const std::shared_ptr<TableShard>& shard,
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
  const std::shared_ptr<TableShard>& shard, std::span<const std::string> pks) {
  // The destination shard + serial trx are recorded by
  // EnsureSerialSearchTransaction (the delete Sink runs it first); here we only
  // append the DELETE op, which seals the current insert run.
  _changes[shard->GetTableId()].AppendDeletes(pks);
}

void SearchTableTransaction::AddSearchTruncate(
  const std::shared_ptr<TableShard>& shard) {
  auto& w = _writes[shard->GetTableId()];
  if (!w.shard) {
    w.shard = shard;
  }
  _changes[shard->GetTableId()].AppendTruncate();
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
  _writes.clear();
}

void SearchTableTransaction::Commit() {
  SDB_ASSERT(!_writes.empty());
  SDB_IF_FAILURE("crash_before_search_wal_commit") { SDB_IMMEDIATE_ABORT(); }
  const uint64_t record_tick = AppendCommit();
  SDB_IF_FAILURE("crash_after_search_wal_commit") { SDB_IMMEDIATE_ABORT(); }
  for (auto& [table_id, w] : _writes) {
    for (auto& c : w.chunks) {
      c.MarkCommitted();
    }
    // Assign descending ticks: the last trx commits at the record tick (band
    // top) so the shard's published tick reaches it and recovery never
    // re-replays this record; each earlier trx sits one (GetQueries()+1) step
    // below the next, so a removal's tick exceeds the inserts it masks
    // (iresearch's <= rule). Shards are independent ordering domains, so
    // sharing the top tick across them is fine.
    uint64_t tick = record_tick;
    for (size_t i = w.transactions.size(); i-- > 0;) {
      auto& trx = *w.transactions[i];
      // The WAL record is already fsynced, so a failed iresearch commit here
      // leaves the durable log inconsistent with the live index -- crash so
      // recovery re-applies the record cleanly.
      const bool committed = trx.Commit(tick);
      SDB_FATAL_IF(
        SEARCH, !committed,
        "search-table commit: iresearch trx Commit failed for table ",
        table_id.id(), " tick=", tick);
      tick -= trx.GetQueries() + 1;
    }
    // TRUNCATE: wipe the shard after the WAL fsync (the cleared state publishes
    // at the next RefreshCommit, like inserts). Solitary, so no trx above it;
    // Clear at the record tick (band top) so the published tick reaches it.
    auto cit = _changes.find(table_id);
    if (cit != _changes.end() && cit->second.HasTruncate()) {
      basics::downCast<SearchTableShard>(*w.shard).Clear(record_tick);
    }
  }
}

uint64_t SearchTableTransaction::AppendCommit() {
  SDB_ASSERT(!_writes.empty());
  std::vector<SearchDbWal::ShardSection> sections;
  sections.reserve(_writes.size());
  std::vector<std::vector<SearchDbWal::Op>> op_lists;
  op_lists.reserve(_writes.size());
  // Widest shard band -> ticks this commit reserves; every shard tops out here.
  uint64_t tick_span = 0;
  SearchDbWal* wal =
    &basics::downCast<SearchTableShard>(*_writes.begin()->second.shard).Wal();
  for (auto& [table_id, w] : _writes) {
    SDB_ASSERT(wal == &basics::downCast<SearchTableShard>(*w.shard).Wal(),
               "all search shards in a txn must share one database WAL");
    auto cit = _changes.find(table_id);
    SDB_ASSERT(cit != _changes.end(),
               "search shard with a trx but no manifest ops");
    // A TRUNCATE adds no trx but needs one tick for its Clear at the band top.
    // HasTruncate() also asserts it is the manifest's sole op.
    uint64_t shard_span =
      ShardTickSpan(w) + (cit->second.HasTruncate() ? 1 : 0);
    tick_span = std::max(tick_span, shard_span);
    auto& ops = op_lists.emplace_back();
    // Walk the table's ordered manifest in statement order. An insert run emits
    // an INLINE op (if it buffered rows) and/or a REFERENCE op (its bulk chunk
    // files); a delete run emits a DELETE op; a TRUNCATE emits a bodyless
    // TRUNCATE op (and, being solitary, ends the walk). Recovery replays this
    // exact order into one trx, reproducing the live single-trx `_queries`
    // ordering.
    for (const auto& op : cit->second.ops) {
      if (op.IsTruncate()) {
        ops.push_back(SearchDbWal::Op{nullptr, {}, {}, {}, /*truncate=*/true});
        break;
      }
      if (op.IsDelete()) {
        ops.push_back(SearchDbWal::Op{
          nullptr, {}, {}, std::span<const std::string>{op.delete_pks}});
        continue;
      }
      if (op.collection && op.collection->Count() > 0) {
        ops.push_back(SearchDbWal::Op{
          op.collection.get(),
          op.pk_segments
            ? std::span<const SearchDbWal::InlinePk>{*op.pk_segments}
            : std::span<const SearchDbWal::InlinePk>{},
          {},
          {}});
      }
      if (!op.seg_ids.empty()) {
        ops.push_back(SearchDbWal::Op{
          nullptr, {}, std::span<const uint64_t>{op.seg_ids}, {}});
      }
    }
    SDB_ASSERT(!ops.empty(),
               "search-table commit with neither chunk files nor inline rows");

    SearchDbWal::ShardSection section;
    section.table_id = table_id;
    section.ops = std::span<const SearchDbWal::Op>{ops};
    sections.push_back(section);
  }

  SDB_ASSERT(wal != nullptr);
  return wal->AppendCommit(sections, tick_span);
}

}  // namespace sdb::search
