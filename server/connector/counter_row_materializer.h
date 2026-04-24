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

#pragma once

#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <memory>

#include "catalog/table.h"
#include "connector/row_materializer.h"

namespace sdb::connector {

// Materializer for external tables whose reader does not expose a
// row-number virtual column (CSV, JSON, ...). The PK is a monotonic
// counter synthesized during CREATE INDEX backfill; at query time we
// re-scan the file (which must be deterministic), count rows, and emit
// those whose counter is in the current batch's PK set.
//
// The constructor streams the entire file once and buffers its chunks
// in-memory. Subsequent Materialize calls are O(batch_size *
// log(num_chunks)) per row. Memory is O(rows * row_size) -- acceptable
// for small-to-medium text files; large files will want a future
// streaming / seek optimization (patch DuckDB readers to expose byte
// offsets).
class CounterRowMaterializer final : public RowMaterializer {
 public:
  CounterRowMaterializer(duckdb::ClientContext& context,
                         std::shared_ptr<catalog::Table> table,
                         std::span<const duckdb::idx_t> projected_columns,
                         std::span<const duckdb::LogicalType> projected_types,
                         std::span<const catalog::Column::Id> bind_column_ids);

  void Materialize(std::span<const std::string_view> pk_bytes,
                   duckdb::DataChunk& output) override;

 private:
  // Stream the entire file and fill _chunks / _chunk_starts. Called
  // from the constructor -- no lazy first-call gating.
  void BuildBuffer();
  // Map a 0-based counter to (buffered chunk index, row index within).
  std::pair<duckdb::idx_t, duckdb::idx_t> LocateRow(int64_t counter) const;

  duckdb::ClientContext& _context;
  std::shared_ptr<catalog::Table> _table;
  duckdb::TableFunction _func;
  duckdb::unique_ptr<duckdb::FunctionData> _bind_data;

  // Projection layout (fixed at construction).
  std::vector<duckdb::idx_t> _projected_columns;
  std::vector<duckdb::LogicalType> _projected_types;
  std::vector<catalog::Column::Id> _bind_column_ids;
  // Catalog-column-id -> physical (file-column) index, cached.
  std::vector<duckdb::idx_t> _bind_to_phys;

  // Buffered chunks from the external scan; each chunk's row count may
  // vary. Kept as unique_ptrs so move-only DataChunk is safe.
  std::vector<std::unique_ptr<duckdb::DataChunk>> _chunks;
  // Prefix sum of chunk sizes: _chunk_starts[i] = counter of row 0 in
  // chunk i; _chunk_starts[_chunks.size()] = total row count.
  std::vector<int64_t> _chunk_starts;
};

}  // namespace sdb::connector
