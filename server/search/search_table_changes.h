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

#include <cstdint>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "connector/column_id.h"
#include "connector/row_position.h"

namespace duckdb {

class Catalog;

}  // namespace duckdb
namespace sdb::search {

// Per-search-table, per-transaction write buffer.
//
// One collection of rows, the positions of the deletes since the last flush and
// the removals the flushes made of the ones before; nothing reaches iresearch
// until the buffer is flushed.
struct LocalTableChangesEntry {
  std::unique_ptr<duckdb::ColumnDataCollection> collection;
  // The shard's stored columns, in chunk order, and the catalog its sink is
  // built against -- both captured with the first buffered chunk so the rows
  // can be replayed into iresearch later, from a commit that never saw the
  // operator that wrote them.
  std::vector<connector::ColumnId> column_ids;
  duckdb::Catalog* catalog = nullptr;

  irs::DocRemovalBuilder positions;
  std::vector<std::shared_ptr<const irs::DocRemoval>> removals;
  bool truncate = false;
  bool clears_shard = false;
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

  void AppendDeletes(const duckdb::Vector& rows) {
    auto places = rows.Values<int64_t>();
    for (duckdb::idx_t i = 0; i < places.size();) {
      const auto first = static_cast<uint64_t>(places.GetValueUnsafe(i));
      auto end = first + 1;
      while (++i < places.size() &&
             static_cast<uint64_t>(places.GetValueUnsafe(i)) == end) {
        ++end;
      }
      const auto doc = connector::RowPositionDoc(first);
      positions.AddRange(connector::RowPositionIndex(first), doc,
                         static_cast<irs::doc_id_t>(doc + (end - first)));
    }
  }

  const std::shared_ptr<const irs::DocRemoval>& FinishRemoval(
    const irs::DirectoryReader& reader) {
    SDB_ASSERT(!positions.Empty());
    return removals.emplace_back(std::move(positions).Finish(reader));
  }

  // A truncate removes every row, including the ones this transaction has just
  // written, so everything buffered before it is dead and is dropped here. The
  // record writes the truncate before anything else -- which is what stops a
  // segment ever preceding one, since replay adopts a segment whole and could
  // not mask the part the truncate should have taken.
  void AppendTruncate(bool clears) {
    collection.reset();
    positions = {};
    removals.clear();
    segments.clear();
    truncate = true;
    clears_shard = clears;
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

  bool HasBufferedRows() const noexcept {
    return collection != nullptr && collection->Count() > 0;
  }

  // Bytes a flush would reclaim. Deletes are deliberately not counted: a flush
  // frees none of them (the record carries them to commit either way), so
  // counting them would fire the threshold on memory it cannot release.
  uint64_t BufferedBytes() const noexcept {
    return collection == nullptr ? 0 : collection->AllocationSize();
  }

  // Drops the buffered rows once a flush has taken ownership of them. The
  // removals stay: the record has to carry them to commit.
  void ClearBufferedRows() { collection.reset(); }

  bool ClearsShard() const noexcept {
    SDB_ASSERT(!clears_shard || (positions.Empty() && removals.empty()),
               "a clearing TRUNCATE must be the only op in its transaction");
    return clears_shard;
  }
};

// Node-based: the entry is well past FlatHashMap's size cut-off, and the buffer
// is held by reference across a statement, so the nodes must not move.
using LocalTableChanges =
  irs::containers::NodeHashMap<duckdb::idx_t, LocalTableChangesEntry>;

}  // namespace sdb::search
