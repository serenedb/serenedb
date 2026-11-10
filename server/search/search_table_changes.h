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
#include <duckdb/common/vector/vector_iterator.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/utils/assert.hpp>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "connector/column_id.h"
#include "connector/row_position.h"
#include "search/table.h"

namespace duckdb {

class Catalog;

}  // namespace duckdb
namespace sdb::search {

inline void AddRowPositions(irs::DocRemovalBuilder& positions,
                            const duckdb::Vector& rows) {
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

// Per-search-table, per-transaction write buffer.
struct LocalTableChangesEntry {
  std::unique_ptr<duckdb::ColumnDataCollection> collection;
  // The shard's stored columns, in chunk order, and the catalog its sink is
  // built against -- both captured with the first buffered chunk so the rows
  // can be replayed into iresearch later, from a commit that never saw the
  // operator that wrote them.
  std::vector<connector::ColumnId> column_ids;
  duckdb::Catalog* catalog = nullptr;

  std::vector<std::shared_ptr<const irs::DocRemoval>> removals;
  std::vector<UnitPatch> patches;
  bool truncate = false;
  bool clears_shard = false;

  void AppendInsertRows(std::unique_ptr<duckdb::ColumnDataCollection> rows,
                        std::span<const connector::ColumnId> cols,
                        duckdb::Catalog& cat) {
    if (collection == nullptr) {
      column_ids.assign(cols.begin(), cols.end());
      catalog = &cat;
      collection = std::move(rows);
    } else {
      collection->Combine(*rows);
    }
  }

  // A truncate removes every row, including the ones this transaction has just
  // written, so everything buffered before it is dead and is dropped here. The
  // record writes the truncate before anything else -- which is what stops a
  // segment ever preceding one, since replay adopts a segment whole and could
  // not mask the part the truncate should have taken.
  void AppendTruncate(bool clears) {
    collection.reset();
    removals.clear();
    patches.clear();
    truncate = true;
    clears_shard = clears;
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
    SDB_ASSERT(!clears_shard || removals.empty(),
               "a clearing TRUNCATE must be the only op in its transaction");
    return clears_shard;
  }

  bool Empty() const noexcept {
    return collection == nullptr && removals.empty() && patches.empty() &&
           !truncate;
  }
};

}  // namespace sdb::search
