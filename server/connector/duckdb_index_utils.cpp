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

#include "connector/duckdb_index_utils.h"

#include <absl/algorithm/container.h>

#include "basics/assert.h"
#include "catalog/inverted_index.h"
#include "connector/duckdb_table_entry.h"
#include "connector/search_sink_writer.hpp"

namespace sdb::connector {

std::vector<size_t> BuildCreateIndexProjection(
  std::span<const duckdb::LogicalIndex> pk_column_positions,
  std::span<const duckdb::idx_t> index_column_positions) {
  std::vector<size_t> projection;
  projection.reserve(index_column_positions.size() +
                     pk_column_positions.size());

  for (auto pos : index_column_positions) {
    projection.push_back(static_cast<size_t>(pos));
  }
  for (auto pk : pk_column_positions) {
    projection.push_back(static_cast<size_t>(pk.index));
  }
  absl::c_sort(projection);
  projection.erase(std::unique(projection.begin(), projection.end()),
                   projection.end());
  return projection;
}

void FeedChunk(DuckDBSinkIndexWriter& writer, duckdb::idx_t count,
               const PkChunk& pk, duckdb::DataChunk& chunk,
               std::span<const FeedColumn> columns,
               std::span<const ExpressionValue> expression_values,
               uint64_t* commit_on_flush) {
  writer.Init(count, pk, commit_on_flush);
  for (const auto& column : columns) {
    if (column.slot >= chunk.ColumnCount()) {
      continue;
    }
    writer.SwitchColumn(column.desc, chunk.data[column.slot], count);
  }
  for (const auto& value : expression_values) {
    writer.SwitchExpression({value.values->GetType(), value.field_id},
                            *value.values, count);
  }
  writer.Finish();
}

}  // namespace sdb::connector
