////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "iresearch/index/flush_order.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/common/sorting/row_sort.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/function/create_sort_key.hpp>
#include <utility>

#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/index/merge/sort_spec.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {

std::vector<uint32_t> FlushOrder(const std::vector<SortKey>& spec,
                                 ColWriter& columns, uint64_t rows) {
  std::vector<const ColumnWriter*> keys;
  duckdb::vector<duckdb::LogicalType> types;
  duckdb::vector<duckdb::OrderModifiers> modifiers;
  keys.reserve(spec.size());
  types.reserve(spec.size());
  modifiers.reserve(spec.size());
  for (const auto& key : spec) {
    auto* column = columns.StagedColumn(key.column);
    if (!column) {
      continue;
    }
    column->PadNullsTo(rows);
    modifiers.emplace_back(key.order, key.null_order);
    types.push_back(column->Type());
    keys.push_back(column);
  }
  if (keys.empty() || rows < 2) {
    return {};
  }
  duckdb::RowSort sort{types, std::move(modifiers), rows};
  duckdb::DataChunk chunk;
  chunk.InitializeEmpty(types);
  for (uint64_t first = 0, c = 0; first < rows; ++c) {
    const auto n = static_cast<duckdb::idx_t>(
      std::min<uint64_t>(STANDARD_VECTOR_SIZE, rows - first));
    for (size_t k = 0; k != keys.size(); ++k) {
      const auto& staged = keys[k]->Staged()[c];
      SDB_ASSERT(staged.count == n);
      chunk.data[k].Reference(staged.data);
    }
    chunk.CheckCardinality(n);
    sort.Append(chunk);
    first += n;
  }
  std::vector<uint32_t> new2old(rows);
  sort.Sort(new2old);
  return new2old;
}

void DocOrder::MaskRows(DocumentMaskBuilder& mask, size_t from,
                        size_t to) const {
  if (Empty()) {
    mask.AddRange(static_cast<doc_id_t>(from + doc_limits::min()),
                  static_cast<doc_id_t>(to + doc_limits::min()));
    return;
  }
  for (auto row = from; row != to; ++row) {
    mask.Add(NewDoc(row));
  }
}

DocOrder MakeDocOrder(std::vector<uint32_t> new2old) {
  DocOrder order;
  if (absl::c_is_sorted(new2old)) {
    return order;
  }
  order.old2new.resize(new2old.size());
  for (uint32_t row = 0; const auto old : new2old) {
    order.old2new[old] = row++;
  }
  order.new2old = std::move(new2old);
  return order;
}

}  // namespace irs
