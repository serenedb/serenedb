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

#include "connector/counter_row_materializer.h"

#include <algorithm>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/execution_context.hpp>
#include <duckdb/parallel/thread_context.hpp>

#include "basics/assert.h"
#include "connector/duckdb_external_scan.h"
#include "connector/primary_key.hpp"

namespace sdb::connector {

CounterRowMaterializer::CounterRowMaterializer(
  duckdb::ClientContext& context, std::shared_ptr<catalog::Table> table,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids)
  : _context(context),
    _table(std::move(table)),
    _projected_columns(projected_columns.begin(), projected_columns.end()),
    _projected_types(projected_types.begin(), projected_types.end()),
    _bind_column_ids(bind_column_ids.begin(), bind_column_ids.end()) {
  _func = MakeExternalScanFunction(_context, _table,
                                   /*table_entry=*/nullptr, _bind_data);

  // Pre-compute bind_col -> physical parquet/csv column index. External
  // tables declare columns in the same order as the file, so the phys
  // index is just the position in _table->Columns().
  const auto& cols = _table->Columns();
  _bind_to_phys.assign(_bind_column_ids.size(),
                       duckdb::DConstants::INVALID_INDEX);
  for (duckdb::idx_t i = 0; i < _bind_column_ids.size(); ++i) {
    for (duckdb::idx_t p = 0; p < cols.size(); ++p) {
      if (cols[p].id == _bind_column_ids[i]) {
        _bind_to_phys[i] = p;
        break;
      }
    }
  }

  // Eager buffer build -- read the whole file once at construction so
  // Materialize is a pure hash lookup per batch.
  BuildBuffer();
}

void CounterRowMaterializer::BuildBuffer() {
  // Project all declared table columns (parquet / csv / json readers emit
  // them in declaration order).
  const auto& cols = _table->Columns();
  duckdb::vector<duckdb::ColumnIndex> column_indexes;
  column_indexes.reserve(cols.size());
  for (duckdb::idx_t i = 0; i < cols.size(); ++i) {
    column_indexes.emplace_back(i);
  }
  duckdb::vector<duckdb::idx_t> projection_ids;
  duckdb::TableFunctionInitInput init_input(
    _bind_data.get(), column_indexes, projection_ids, /*filters=*/nullptr);

  auto gstate = _func.init_global(_context, init_input);
  duckdb::ThreadContext thread_ctx(_context);
  duckdb::ExecutionContext exec_ctx(_context, thread_ctx, nullptr);
  duckdb::unique_ptr<duckdb::LocalTableFunctionState> lstate;
  if (_func.init_local) {
    lstate = _func.init_local(exec_ctx, init_input, gstate.get());
  }

  duckdb::vector<duckdb::LogicalType> chunk_types;
  chunk_types.reserve(cols.size());
  for (const auto& col : cols) {
    chunk_types.push_back(col.type);
  }

  int64_t total = 0;
  _chunk_starts.push_back(0);
  while (true) {
    auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
    chunk->Initialize(_context, chunk_types);
    duckdb::TableFunctionInput in(_bind_data.get(), lstate.get(), gstate.get());
    _func.function(_context, in, *chunk);
    const auto n = chunk->size();
    if (n == 0) {
      break;
    }
    total += static_cast<int64_t>(n);
    _chunks.push_back(std::move(chunk));
    _chunk_starts.push_back(total);
  }
}

std::pair<duckdb::idx_t, duckdb::idx_t> CounterRowMaterializer::LocateRow(
  int64_t counter) const {
  // Find the chunk whose start <= counter < next start.
  auto it =
    std::upper_bound(_chunk_starts.begin(), _chunk_starts.end(), counter);
  SDB_ASSERT(it != _chunk_starts.begin(),
             "counter out of range (before first chunk)");
  auto chunk_idx =
    static_cast<duckdb::idx_t>(std::distance(_chunk_starts.begin(), it) - 1);
  SDB_ASSERT(chunk_idx < _chunks.size(),
             "counter out of range (beyond last chunk)");
  auto row_idx = static_cast<duckdb::idx_t>(counter - _chunk_starts[chunk_idx]);
  return {chunk_idx, row_idx};
}

void CounterRowMaterializer::Materialize(
  std::span<const std::string_view> pk_bytes, duckdb::DataChunk& output) {
  const auto num_rows = pk_bytes.size();
  if (num_rows == 0) {
    return;
  }

  const int64_t total_rows = _chunk_starts.empty() ? 0 : _chunk_starts.back();

  for (duckdb::idx_t out_row = 0; out_row < num_rows; ++out_row) {
    const int64_t counter = primary_key::ReadSigned<int64_t>(pk_bytes[out_row]);
    if (counter < 0 || counter >= total_rows) {
      // Stale PK -- mark all projected slots NULL for this row.
      for (duckdb::idx_t proj = 0; proj < _projected_columns.size(); ++proj) {
        if (_projected_columns[proj] == duckdb::DConstants::INVALID_INDEX) {
          continue;
        }
        duckdb::FlatVector::ValidityMutable(output.data[proj])
          .SetInvalid(out_row);
      }
      continue;
    }
    auto [chunk_idx, row_in_chunk] = LocateRow(counter);
    auto& src_chunk = *_chunks[chunk_idx];
    for (duckdb::idx_t proj = 0; proj < _projected_columns.size(); ++proj) {
      const auto bind_col = _projected_columns[proj];
      if (bind_col == duckdb::DConstants::INVALID_INDEX) {
        continue;
      }
      const auto phys = _bind_to_phys[bind_col];
      duckdb::VectorOperations::Copy(src_chunk.data[phys], output.data[proj],
                                     row_in_chunk + 1, row_in_chunk, out_row);
    }
  }
}

}  // namespace sdb::connector
