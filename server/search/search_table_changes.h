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

#include <algorithm>
#include <cstdint>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/wal_entry.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "connector/column_id.h"

namespace duckdb {

class Catalog;

}  // namespace duckdb
namespace sdb::search {

inline void AppendRowRun(duckdb::WALDeleteRowRanges& runs, uint64_t position) {
  if (!runs.starts.empty() &&
      position == runs.starts.back() + runs.counts.back()) {
    ++runs.counts.back();
    return;
  }
  runs.starts.emplace_back(position);
  runs.counts.emplace_back(1);
}

// Per-search-table, per-transaction write buffer.
//
// One collection of rows and one vector of delete row ranges, both filled in
// issue order; nothing reaches iresearch until the buffer is flushed. `ops`
// records only the things that are not rows -- deletes and truncates -- each
// carrying the number of buffered rows that precede it, so replay can
// interleave them back into the row stream. Rows between two ops are implicit.
struct LocalTableChangesEntry {
  struct Op {
    // Buffered rows that precede this op. Rows the flush already promoted are
    // gone from the buffer, so their ops sit at 0.
    uint64_t row_watermark = 0;
    // Runs of row positions to remove. Empty for a TRUNCATE.
    duckdb::WALDeleteRowRanges deletes;
    bool truncate = false;
    bool clears_shard = false;

    bool IsTruncate() const noexcept { return truncate; }
  };

  using EmitFn = absl::AnyInvocable<void(duckdb::DataChunk&) const>;
  using ApplyFn = absl::AnyInvocable<void(Op&) const>;

  std::unique_ptr<duckdb::ColumnDataCollection> collection;
  // The shard's stored columns, in chunk order, and the catalog its sink is
  // built against -- both captured with the first buffered chunk so the rows
  // can be replayed into iresearch later, from a commit that never saw the
  // operator that wrote them.
  std::vector<connector::ColumnId> column_ids;
  duckdb::Catalog* catalog = nullptr;

  std::vector<Op> ops;
  size_t applied_ops = 0;
  // Segments a bulk statement flushed, for the record to reference by name.
  std::vector<std::string> segments;

  void AppendInsertChunk(duckdb::BufferManager& bm,
                         const duckdb::vector<duckdb::LogicalType>& types,
                         std::span<const connector::ColumnId> cols,
                         duckdb::Catalog& cat, duckdb::DataChunk& chunk) {
    if (collection == nullptr) {
      collection = std::make_unique<duckdb::ColumnDataCollection>(bm, types);
    }
    if (column_ids.empty()) {
      column_ids.assign(cols.begin(), cols.end());
      catalog = &cat;
    }
    collection->Append(chunk);
  }

  void AppendDeletes(const duckdb::Vector& positions) {
    auto places = positions.Values<int64_t>();
    if (places.size() == 0) {
      return;
    }
    if (places.CanHaveNull()) {
      for (const auto& place : places) {
        if (!place.IsValid()) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            ERR_MSG("search table: a deleted row has no storage position"));
        }
      }
    }
    const auto rows = BufferedRows();
    if (ops.size() == applied_ops || ops.back().truncate ||
        ops.back().row_watermark != rows) {
      ops.emplace_back().row_watermark = rows;
    }
    auto& deletes = ops.back().deletes;
    for (const auto& place : places) {
      AppendRowRun(deletes, static_cast<uint64_t>(place.GetValue()));
    }
  }

  // A truncate removes every row, including the ones this transaction has just
  // written, so everything buffered before it is dead and is dropped here. That
  // keeps the invariant a truncate is the FIRST op in the buffer -- which is
  // what stops a segment ever preceding one, since replay adopts a segment
  // whole and could not mask the part the truncate should have taken.
  void AppendTruncate(bool clears_shard) {
    collection.reset();
    ops.clear();
    applied_ops = 0;
    segments.clear();
    auto& op = ops.emplace_back();
    op.truncate = true;
    op.clears_shard = clears_shard;
  }

  void AppendSegments(std::vector<std::string>&& refs) {
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

  uint64_t BufferedRows() const noexcept {
    return collection == nullptr ? 0 : collection->Count();
  }

  bool HasBufferedRows() const noexcept { return BufferedRows() > 0; }

  // Bytes a flush would reclaim. Deletes are deliberately not counted: a flush
  // frees none of them (the record carries them to commit either way), so
  // counting them would fire the threshold on memory it cannot release.
  uint64_t BufferedBytes() const noexcept {
    return collection == nullptr ? 0 : collection->AllocationSize();
  }

  void Visit(size_t first_op, const EmitFn& emit, const ApplyFn& apply) {
    auto op = ops.begin() + first_op;
    uint64_t row = 0;
    auto apply_reached = [&] {
      for (; op != ops.end() && op->row_watermark <= row; ++op) {
        apply(*op);
      }
    };
    if (collection != nullptr) {
      for (auto& chunk : collection->Chunks()) {
        for (duckdb::idx_t offset = 0; offset < chunk.size();) {
          apply_reached();
          auto end = chunk.size();
          if (op != ops.end()) {
            end =
              std::min<duckdb::idx_t>(end, offset + (op->row_watermark - row));
          }
          duckdb::DataChunk slice;
          slice.InitializeEmpty(collection->Types());
          slice.Slice(chunk, offset, end);
          emit(slice);
          row += end - offset;
          offset = end;
        }
      }
    }
    for (; op != ops.end(); ++op) {
      apply(*op);
    }
  }

  // Drops the buffered rows once a flush has taken ownership of them. The ops
  // stay: the record has to carry them to commit. `applied_ops` keeps replay
  // from feeding them a second time, so their stale watermarks are never read.
  void ClearBufferedRows() { collection.reset(); }

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
  irs::containers::NodeHashMap<duckdb::idx_t, LocalTableChangesEntry>;

}  // namespace sdb::search
