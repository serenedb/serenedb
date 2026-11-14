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

#include <absl/functional/function_ref.h>

#include <cstdint>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/segment_reader.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <memory>
#include <span>
#include <string>
#include <utility>
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

inline void ForEachUnitRows(
  const irs::DirectoryReader& reader,
  std::span<const std::pair<uint64_t, duckdb::sel_t>> order,
  absl::FunctionRef<void(uint64_t unit, std::span<const uint64_t> rows,
                         const duckdb::SelectionVector& sel)>
    visit) {
  std::vector<uint64_t> rows;
  duckdb::SelectionVector sel{order.size()};
  for (size_t i = 0; i < order.size();) {
    const auto index = connector::RowPositionIndex(order[i].first);
    rows.clear();
    for (; i < order.size() &&
           connector::RowPositionIndex(order[i].first) == index;
         ++i) {
      sel.set_index(rows.size(), order[i].second);
      rows.push_back(connector::RowPositionDoc(order[i].first) -
                     irs::doc_limits::min());
    }
    visit(
      irs::utils::downCast<irs::SegmentReader>(reader[index]).GetImpl()->Id(),
      rows, sel);
  }
}

// Per-search-table, per-transaction write buffer.
struct LocalTableChangesEntry {
  std::unique_ptr<duckdb::ColumnDataCollection> collection;
  uint64_t bytes = 0;
  // The shard's stored columns, in chunk order, and the catalog its sink is
  // built against -- both captured with the first buffered chunk so the rows
  // can be replayed into iresearch later, from a commit that never saw the
  // operator that wrote them.
  std::vector<connector::ColumnId> column_ids;
  duckdb::Catalog* catalog = nullptr;

  std::vector<std::shared_ptr<const irs::DocRemoval>> removals;
  std::vector<UnitPatch> patches;
  bool truncate = false;

  void AppendInsertRows(std::unique_ptr<duckdb::ColumnDataCollection> rows,
                        uint64_t row_bytes,
                        std::span<const connector::ColumnId> cols,
                        duckdb::Catalog& cat) {
    bytes += row_bytes;
    if (collection == nullptr) {
      column_ids.assign(cols.begin(), cols.end());
      catalog = &cat;
      collection = std::move(rows);
    } else if (rows->Count() < STANDARD_VECTOR_SIZE) {
      for (auto& chunk : rows->Chunks()) {
        collection->Append(chunk);
      }
    } else {
      collection->Combine(*rows);
    }
  }

  void AppendTruncate(std::shared_ptr<const irs::DocRemoval> visible) {
    ClearBufferedRows();
    removals.clear();
    patches.clear();
    if (!visible->groups.empty()) {
      removals.push_back(std::move(visible));
    }
    truncate = true;
  }

  bool HasBufferedRows() const noexcept {
    return collection != nullptr && collection->Count() > 0;
  }

  // Bytes a flush would reclaim. Deletes are deliberately not counted: a flush
  // frees none of them (the record carries them to commit either way), so
  // counting them would fire the threshold on memory it cannot release.
  uint64_t BufferedBytes() const noexcept { return bytes; }

  // Drops the buffered rows once a flush has taken ownership of them. The
  // removals stay: the record has to carry them to commit.
  void ClearBufferedRows() {
    collection.reset();
    bytes = 0;
  }

  bool Empty() const noexcept {
    return collection == nullptr && removals.empty() && patches.empty() &&
           !truncate;
  }
};

}  // namespace sdb::search
