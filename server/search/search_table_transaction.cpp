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

#include <absl/algorithm/container.h>

#include <duckdb/common/types/column/column_data_collection.hpp>
#include <span>
#include <vector>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/system-compiler.h"
#include "search/search_db_wal.h"
#include "search/search_table_shard.h"
#include "storage_engine/table_shard.h"

namespace sdb::search {

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
}

void SearchTableTransaction::Commit() {
  SDB_ASSERT(!_writes.empty());
  // Insert-only today: zero removes, so all segments share first_tick and one
  // RefreshCommit publishes them (deletes/updates -> §5.5, later).
  SDB_ASSERT(absl::c_all_of(_writes, [](const auto& e) {
    return absl::c_all_of(e.second.transactions,
                          [](const auto& t) { return t->GetQueries() == 0; });
  }));
  // Crash boundaries (WAL_DESIGN.md §9): AppendCommit's single fsync IS the
  // commit point. A crash before it loses the whole txn (no central record ->
  // orphan chunks skipped on recovery); a crash after it keeps the txn
  // (recovery replays the record and rebuilds iresearch, even though it was
  // never refresh-published here).
  SDB_IF_FAILURE("crash_before_search_wal_commit") { SDB_IMMEDIATE_ABORT(); }
  const uint64_t tick = AppendCommit();
  SDB_IF_FAILURE("crash_after_search_wal_commit") { SDB_IMMEDIATE_ABORT(); }
  for (auto& [table_id, w] : _writes) {
    for (auto& trx : w.transactions) {
      trx->Commit(tick);
    }
  }
}

uint64_t SearchTableTransaction::AppendCommit() {
  SDB_ASSERT(!_writes.empty());
  // Build one per-shard section for each search table this txn wrote, then
  // append ONE central record across all of them (multi-shard atomicity, §9).
  // All shards are in the same database (the txn scope), so they share one db
  // WAL.
  SearchDbWal* wal = nullptr;
  std::vector<SearchDbWal::ShardSection> sections;
  sections.reserve(_writes.size());

  for (auto& [table_id, w] : _writes) {
    auto& shard = basics::downCast<SearchTableShard>(*w.shard);
    auto& shard_wal = shard.Wal();
    if (wal == nullptr) {
      wal = &shard_wal;
    } else {
      SDB_ASSERT(wal == &shard_wal,
                 "all search shards in a txn must share one database WAL");
    }

    // Gather this shard's inline (small-INSERT) buffers, one per inline sink
    // thread / statement, with their per-Sink-chunk (base, count) segment lists
    // (§5.6). Bulk inserts produced no buffers (they streamed to chunk files
    // during Sink) -- their data + pk_bases are in w.seg_ids / the chunk
    // frames.
    std::vector<duckdb::ColumnDataCollection*> buffers;
    std::vector<std::vector<SearchDbWal::InlinePk>*> pk_lists;
    auto it = _changes.find(table_id);
    if (it != _changes.end()) {
      for (auto& buf : it->second.inserts) {
        if (buf.collection && buf.collection->Count() > 0) {
          buffers.push_back(buf.collection.get());
          pk_lists.push_back(buf.pk_segments.get());
        }
      }
    }
    SDB_ASSERT(!w.seg_ids.empty() || !buffers.empty(),
               "search-table commit with neither chunk files nor inline rows");

    SearchDbWal::ShardSection section;
    section.schema_id = w.schema_id;
    section.table_id = table_id.id();
    section.column_ids = SearchDbWal::ColumnIds{w.column_ids};
    if (w.seg_ids.empty() && buffers.size() == 1) {
      // OLTP fast path: one inline buffer, no chunk files -> INLINE section
      // (rows serialised straight into the central record, segments alongside).
      section.inline_data = buffers.front();
      section.inline_pks =
        std::span<const SearchDbWal::InlinePk>{*pk_lists.front()};
    } else {
      // General path (bulk, multi-statement inline, or a mix): the section is a
      // single REFERENCE over all chunk files. Bulk inserts already streamed
      // their per-thread chunk files concurrently at Sink (w.seg_ids). Any
      // inline buffers are flushed here -- single-threaded commit point, so no
      // Sink-time concurrency to exploit: pack ALL of them into ONE chunk file.
      // Re-slice each buffer by its recorded (base, count) segments so a
      // coalesced collection chunk doesn't smear one base across two Sink
      // chunks' rows (§5.6); each emitted frame carries its own base. The
      // dictionary slices serialize fine -- DataChunk::Serialize resolves the
      // selection (ToUnifiedFormat / Values<T>()), so no Flatten needed.
      if (!buffers.empty()) {
        auto writer = shard.NewChunkWriter();
        for (size_t k = 0; k < buffers.size(); ++k) {
          VisitInlineSegments(*buffers[k], *pk_lists[k],
                              [&](duckdb::DataChunk& chunk, uint64_t base) {
                                writer.Append(chunk, base);
                              });
        }
        writer.Finish();
        w.seg_ids.push_back(writer.SegId());
      }
      section.seg_ids = std::span<const uint64_t>{w.seg_ids};
    }
    sections.push_back(section);
  }

  SDB_ASSERT(wal != nullptr);
  return wal->AppendCommit(sections);
}

}  // namespace sdb::search
