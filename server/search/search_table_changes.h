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

#include <absl/functional/any_invocable.h>

#include <cstdint>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "catalog/column_id.h"
#include "catalog/identifiers/object_id.h"
#include "search/search_db_wal.h"

namespace sdb::search {

// Per-search-table, per-transaction write buffer.
//
// One collection of rows and one vector of delete rowids, both filled in issue
// order; nothing reaches iresearch until the buffer is flushed. `ops` records
// only the things that are not rows -- deletes and truncates -- each carrying
// the number of insert bands that precede it, so replay can interleave them
// back into the row stream. Rows between two ops are implicit.
struct LocalTableChangesEntry {
  struct Op {
    // Insert bands of `pk_segments` that precede this op. Rows the flush
    // already promoted are gone from the buffer, so their ops sit at 0.
    uint32_t band_watermark = 0;
    // Rowids to remove. Empty for a TRUNCATE.
    std::vector<int64_t> delete_rows;
    bool truncate = false;
    bool clears_shard = false;

    bool IsDelete() const noexcept { return !delete_rows.empty(); }
    bool IsTruncate() const noexcept { return truncate; }
  };

  using EmitFn = absl::AnyInvocable<void(duckdb::DataChunk&, uint64_t) const>;

  // The buffer. `pk_segments` bands it one entry per Sink chunk.
  std::unique_ptr<duckdb::ColumnDataCollection> collection;
  std::vector<SearchDbWal::InlinePk> pk_segments;
  // The shard's stored columns, in chunk order -- captured with the first
  // buffered chunk so the rows can be replayed into iresearch later, from a
  // commit that never saw the operator that wrote them.
  std::vector<catalog::ColumnId> column_ids;

  std::vector<Op> ops;
  size_t applied_ops = 0;
  // Segments a bulk statement flushed, for the record to reference by name.
  std::vector<SearchDbWal::SegmentRef> segments;

  void AppendInsertChunk(duckdb::BufferManager& bm,
                         const duckdb::vector<duckdb::LogicalType>& types,
                         std::span<const catalog::ColumnId> cols,
                         duckdb::DataChunk& chunk, uint64_t pk_base) {
    if (collection == nullptr) {
      collection = std::make_unique<duckdb::ColumnDataCollection>(bm, types);
    }
    if (column_ids.empty()) {
      column_ids.assign(cols.begin(), cols.end());
    }
    collection->Append(chunk);
    pk_segments.push_back({pk_base, chunk.size()});
  }

  void AppendDeletes(std::span<const int64_t> rows) {
    if (rows.empty()) {
      return;
    }
    auto& op = ops.emplace_back();
    op.band_watermark = static_cast<uint32_t>(pk_segments.size());
    op.delete_rows.assign(rows.begin(), rows.end());
  }

  // A truncate removes every row, including the ones this transaction has just
  // written, so everything buffered before it is dead and is dropped here. That
  // keeps the invariant a truncate is the FIRST op in the buffer -- which is
  // what stops a segment ever preceding one, since replay adopts a segment
  // whole and could not mask the part the truncate should have taken.
  void AppendTruncate(bool clears_shard) {
    collection.reset();
    pk_segments.clear();
    ops.clear();
    applied_ops = 0;
    segments.clear();
    auto& op = ops.emplace_back();
    op.band_watermark = 0;
    op.truncate = true;
    op.clears_shard = clears_shard;
  }

  void AppendSegments(std::vector<SearchDbWal::SegmentRef>&& refs) {
    if (refs.empty()) {
      return;
    }
    if (segments.empty()) {
      segments = std::move(refs);
      return;
    }
    segments.insert(segments.end(), std::make_move_iterator(refs.begin()),
                    std::make_move_iterator(refs.end()));
  }

  bool HasBufferedRows() const noexcept {
    return collection != nullptr && collection->Count() > 0;
  }

  // Bytes a flush would reclaim. Deletes are deliberately not counted: a flush
  // frees none of them (the record carries them to commit either way), so
  // counting them would fire the threshold on memory it cannot release.
  uint64_t BufferedBytes() const noexcept {
    return collection == nullptr ? 0 : collection->AllocationSize();
  }

  // Replays the buffered rows in append order with the rowid each chunk was
  // keyed from.
  void VisitBufferedRows(const EmitFn& emit) const {
    if (!HasBufferedRows()) {
      return;
    }
    VisitInlineSegments(*collection, pk_segments, emit);
  }

  // Drops the buffered rows once a flush has taken ownership of them. The ops
  // stay: the record has to carry them to commit. `applied_ops` keeps replay
  // from feeding them a second time, so their stale watermarks are never read.
  void ClearBufferedRows() {
    collection.reset();
    pk_segments.clear();
  }

  bool ClearsShard() const noexcept {
    if (ops.empty() || !ops.front().clears_shard) {
      return false;
    }
    SDB_ASSERT(ops.size() == 1,
               "a clearing TRUNCATE must be the only op in its transaction");
    return true;
  }
};

// Node-based: the entry is well past FlatHashMap's size cut-off, and the buffer
// is held by reference across a statement, so the nodes must not move.
using LocalTableChanges =
  irs::containers::NodeHashMap<ObjectId, LocalTableChangesEntry>;

}  // namespace sdb::search
