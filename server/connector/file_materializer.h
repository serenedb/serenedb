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

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/execution_context.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parallel/thread_context.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <memory>
#include <vector>

#include "catalog/table.h"
#include "connector/row_materializer.h"

namespace sdb::connector {

class FileMaterializer final : public RowMaterializer {
 public:
  FileMaterializer(
    duckdb::ClientContext& context, std::shared_ptr<catalog::Table> table,
    std::span<const duckdb::idx_t> projected_columns,
    std::span<const duckdb::LogicalType> projected_types,
    std::span<const catalog::Column::Id> bind_column_ids);

  void Materialize(std::span<const std::string_view> pk_bytes,
                   duckdb::DataChunk& output) final;

 private:
  duckdb::ClientContext& _context;
  // Constructing a fresh ThreadContext per Materialize call allocates a
  // Logger unique_ptr; hold once across the materializer's lifetime.
  duckdb::ThreadContext _thread_ctx;
  duckdb::ExecutionContext _exec_ctx;
  // Keeps the catalog entry alive for the underlying reader's bind_data.
  std::shared_ptr<catalog::Table> _table;

  // Wrapper function + its bind_data, built once in the constructor and
  // shared across Materialize calls (reader metadata parsed once).
  duckdb::TableFunction _func;
  duckdb::unique_ptr<duckdb::FunctionData> _bind_data;

  // Columns we ask the reader to produce: real projected cols + the
  // trailing file_row_number virtual column at position `_row_number_idx`.
  duckdb::vector<duckdb::ColumnIndex> _column_indexes;
  // Caller-provided projection: _projected_columns[proj] is the bind column
  // index for output slot `proj`, or INVALID_INDEX for virtual slots the
  // caller fills itself (score, rowid, tableoid, ...). Materialize walks it
  // to find which output slots are ours and advances a parallel counter
  // `c` over _chunk columns.
  std::vector<duckdb::idx_t> _projected_columns;
  // Position of file_row_number in _chunk (always the last column).
  duckdb::idx_t _row_number_idx = 0;

  // Reusable reader chunk: initialized once, Reset() between drains.
  duckdb::DataChunk _chunk;

  // Decoded file-offset PKs for the current batch, sorted ascending before
  // handoff (see Materialize): passing a sorted list lets the scan (CSV, ...)
  // dispense offsets via an atomic cursor and turns sequential seeks into
  // forward-only reads. Goes to the scan via
  // TableFunctionInitInput::pk_lookups (CSV/JSON) and also drives the IN
  // filter built in Materialize (parquet's row-group pruning).
  std::vector<int64_t> _pk_lookups;
};

}  // namespace sdb::connector
