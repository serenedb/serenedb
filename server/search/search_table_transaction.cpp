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
#include "basics/primary_key.hpp"
#include "basics/system-compiler.h"
#include "search/search_db_wal.h"
#include "search/search_table.h"

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

// The rowids this transaction deleted, decoded back out of the encoded PK terms
// the WAL carries. A build's log holds raw ids (8 bytes, no per-entry string)
// and re-encodes at replay, the way the transactional build's does.
void RecordDeletesForBuild(SearchTable& shard,
                           const LocalTableChangesEntry& changes) {
  std::vector<int64_t> rows;
  for (const auto& op : changes.ops) {
    if (!op.IsDelete()) {
      continue;
    }
    rows.reserve(rows.size() + op.delete_pks.size());
    for (const auto& pk : op.delete_pks) {
      rows.push_back(
        connector::primary_key::ReadSigned<int64_t>(std::string_view{pk}));
    }
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
  const duckdb::vector<duckdb::LogicalType>& types, duckdb::DataChunk& chunk,
  uint64_t pk_base) {
  _changes[shard->GetTableId()].AppendInsertChunk(buffer_manager, types, chunk,
                                                  pk_base);
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
  const std::shared_ptr<SearchTable>& shard, std::span<const std::string> pks) {
  // The destination shard + serial trx are recorded by
  // EnsureSerialSearchTransaction (the delete Sink runs it first); here we only
  // append the DELETE op, which seals the current insert run.
  _changes[shard->GetTableId()].AppendDeletes(pks);
}

void SearchTableTransaction::AddSearchTruncate(
  const std::shared_ptr<SearchTable>& shard) {
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
  ReleaseWriters();
  _writes.clear();
  _changes.clear();
  _readers.clear();
}

void SearchTableTransaction::Commit() {
  SDB_ASSERT(!_writes.empty());
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

    if (cit != _changes.end() && cit->second.HasTruncate()) {
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
  std::vector<std::vector<SearchDbWal::Op>> op_lists;
  op_lists.reserve(_writes.size());
  // Widest shard band -> ticks this commit reserves; every shard tops out here.
  uint64_t tick_span = 0;
  SearchDbWal* wal = &_writes.begin()->second.shard->Wal();
  for (auto& [table_id, w] : _writes) {
    SDB_ASSERT(wal == &w.shard->Wal(),
               "all search shards in a txn must share one database WAL");
    auto cit = _changes.find(table_id);
    SDB_ASSERT(cit != _changes.end(),
               "search shard with a trx but no manifest ops");
    // A TRUNCATE adds no trx but needs one tick for its Clear at the band top.

    uint64_t shard_span =
      ShardTickSpan(w) + (cit->second.HasTruncate() ? 1 : 0);
    tick_span = std::max(tick_span, shard_span);
    auto& ops = op_lists.emplace_back();

    for (auto& op : cit->second.ops) {
      if (op.IsTruncate()) {
        ops.push_back(SearchDbWal::Op{.truncate = true});
        break;
      }
      if (op.IsDelete()) {
        ops.push_back(SearchDbWal::Op{
          .delete_pks = std::span<const std::string>{op.delete_pks}});
        continue;
      }
      if (op.collection && op.collection->Count() > 0) {
        ops.push_back(SearchDbWal::Op{
          .inline_data = op.collection.get(),
          .inline_pks =
            op.pk_segments
              ? std::span<const SearchDbWal::InlinePk>{*op.pk_segments}
              : std::span<const SearchDbWal::InlinePk>{}});
      }
      if (!op.segments.empty()) {
        ops.push_back(SearchDbWal::Op{
          .segments = std::span<const SearchDbWal::SegmentRef>{op.segments}});
      }
    }
    SDB_ASSERT(!ops.empty(),
               "search-table commit with neither segments nor inline rows");

    SearchDbWal::ShardSection section;
    section.table_id = table_id;
    section.ops = std::span<const SearchDbWal::Op>{ops};
    sections.push_back(section);
  }

  SDB_ASSERT(wal != nullptr);
  return wal->AppendCommit(sections, tick_span);
}

}  // namespace sdb::search
