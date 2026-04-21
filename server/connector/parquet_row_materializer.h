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

#include <duckdb/execution/execution_context.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parallel/thread_context.hpp>
#include <duckdb/planner/table_filter_set.hpp>
#include <memory>

#include "basics/containers/flat_hash_map.h"
#include "catalog/table.h"
#include "connector/row_materializer.h"

namespace sdb::connector {

// Resolves PK bytes into parquet rows. The PK bytes are sortable
// big-endian + sign-flip encodings of `file_row_number` (written at
// CREATE INDEX time by SereneDBPhysicalCreateIndex).
//
// Lifecycle:
// - Constructor (eager): binds parquet, decodes ALL pks, builds the big
//   `file_row_number IN (...)` filter as a ConjunctionOrFilter of
//   equality predicates, and initializes ONE persistent parquet scan
//   (init_global + init_local). Nothing is lazy afterwards.
// - Materialize: pulls chunks from the live scan as needed to satisfy
//   the batch. Rows that arrive out of iresearch order (because parquet
//   emits in file-row order, iresearch returns in doc/score order) are
//   stashed in an index (`_buffered_row_id_to_slot`) so any future
//   batch that asks for that row_id can pick it up without touching
//   the scan again. Memory ~ O(total_rows * row_size).
class ParquetRowMaterializer final : public RowMaterializer {
 public:
  ParquetRowMaterializer(
    duckdb::ClientContext& context, std::shared_ptr<catalog::Table> table,
    std::span<const std::string> all_pks,
    std::span<const duckdb::idx_t> projected_columns,
    std::span<const duckdb::LogicalType> projected_types,
    std::span<const catalog::Column::Id> bind_column_ids);

  void Materialize(std::span<const std::string_view> pk_bytes,
                   duckdb::DataChunk& output) override;

 private:
  // Pull one chunk from the live parquet scan into `_buffer` and
  // register its rows in `_row_id_to_slot`. Returns false if the scan
  // is exhausted.
  bool PullOneChunk();

  // Copy row `row_idx` of buffered chunk `chunk_idx` into `output` at
  // position `out_row` for every real-column projection slot.
  void CopyBufferedRowToOutput(duckdb::idx_t chunk_idx,
                               duckdb::idx_t row_idx,
                               duckdb::idx_t out_row,
                               duckdb::DataChunk& output);

  duckdb::ClientContext& _context;
  std::shared_ptr<catalog::Table> _table;

  // Wrapper function + its bind_data (owned).
  duckdb::TableFunction _func;
  duckdb::unique_ptr<duckdb::FunctionData> _bind_data;

  // Execution scaffolding kept alive across batches so the persistent
  // scan can resume.
  std::unique_ptr<duckdb::ThreadContext> _thread_ctx;
  std::unique_ptr<duckdb::ExecutionContext> _exec_ctx;
  duckdb::unique_ptr<duckdb::GlobalTableFunctionState> _gstate;
  duckdb::unique_ptr<duckdb::LocalTableFunctionState> _lstate;
  duckdb::TableFilterSet _filter_set;  // referenced by init_input; kept alive
  duckdb::vector<duckdb::ColumnIndex> _column_indexes;
  duckdb::vector<duckdb::idx_t> _projection_ids;

  // Projection layout (fixed at construction).
  std::vector<duckdb::idx_t> _projected_columns;
  std::vector<duckdb::LogicalType> _projected_types;
  std::vector<catalog::Column::Id> _bind_column_ids;

  // For each projection slot with a real column: its index into the
  // parquet chunk's column vector (or INVALID_INDEX if virtual).
  std::vector<duckdb::idx_t> _real_col_to_parquet_slot;
  // Position of the virtual `file_row_number` column inside each
  // parquet chunk (always the last entry in _column_indexes).
  duckdb::idx_t _frn_slot = 0;

  struct Slot {
    duckdb::idx_t chunk;
    duckdb::idx_t row;
  };
  // Pulled chunks live here until the materializer dies. Index into
  // the chunk is kept by `_row_id_to_slot`. For realistic top-k /
  // bounded-filter queries this is bounded by the iresearch result set.
  std::vector<std::unique_ptr<duckdb::DataChunk>> _buffer;
  containers::FlatHashMap<int64_t, Slot> _row_id_to_slot;
  bool _scan_done = false;

  // Per-call scratch -- sized to the output chunk's capacity.
  duckdb::vector<duckdb::LogicalType> _chunk_types;
};

}  // namespace sdb::connector
