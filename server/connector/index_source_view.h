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

#include <absl/functional/function_ref.h>

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <span>
#include <string_view>
#include <vector>

#include "catalog/table_options.h"
#include "connector/index_source.h"
#include "connector/view_fast_path.h"

namespace sdb::connector {

// Shared plumbing for view-backed materializers that fetch source rows by
// numeric PK: projection mapping, scratch/cast handling, and sort scratch.
// Derived classes provide the actual row fetch (lookup TF for file readers,
// DataTable::Fetch for catalog tables).
class ViewIndexSourceBase : public IndexSource {
 protected:
  explicit ViewIndexSourceBase(ViewFastPath fast_path)
    : _fast_path{std::move(fast_path)} {}

  // Maps each projected output column to a source column and builds the
  // cast/scratch state. `col_by_name` resolves a view projection name to a
  // source column index; `add_source_column` records the source column in
  // the derived class's fetch bookkeeping and returns its source type.
  void InitProjection(
    duckdb::ClientContext& context,
    std::span<const duckdb::idx_t> projected_columns,
    std::span<const duckdb::LogicalType> projected_types,
    std::span<const catalog::Column::Id> bind_column_ids,
    absl::FunctionRef<duckdb::idx_t(std::string_view)> col_by_name,
    absl::FunctionRef<duckdb::LogicalType(duckdb::idx_t)> add_source_column);

  // Fetches want sorted lookups (parquet row-group skipping, csv/json
  // forward cursor, DataTable::Fetch result alignment); fills _sorted_rows
  // (+ _sorted_files) and _output_positions.
  void SortRows(const PrimaryKeyI64& pk, duckdb::idx_t start,
                duckdb::idx_t count);
  void SortFilesRows(const PrimaryKeyI64I64& pk, duckdb::idx_t start,
                     duckdb::idx_t count);

  // Match cols alias output directly so fetch writes land there in-place;
  // cast cols keep _tf_target's own (source-typed) buffer for the
  // end-of-call cast.
  void AliasOutput(duckdb::DataChunk& output);
  void RunCastPass(duckdb::DataChunk& output, duckdb::idx_t row_count);

  ViewFastPath _fast_path;
  std::vector<duckdb::idx_t> _real_proj_slots;
  duckdb::vector<duckdb::LogicalType> _scratch_types;
  duckdb::vector<duckdb::LogicalType> _projected_types;
  // nullptr when scratch and projected types match.
  std::vector<duckdb::unique_ptr<duckdb::ExpressionExecutor>> _cast_executors;
  // ExpressionExecutor holds the Expression by ref, must outlive it.
  std::vector<duckdb::unique_ptr<duckdb::Expression>> _cast_expressions;

  // Per-column alias to where the fetch should write. Matching columns alias
  // output directly; cast columns keep their own (source-typed) buffer that
  // RunCastPass converts at end of call.
  duckdb::DataChunk _tf_target;

  // Sort scratch reused per call. Sorting goes through these instead of the
  // caller's PrimaryKeyBatch because ANN / range scans persist their batch
  // across many slices and mutating it would break later slices.
  std::vector<duckdb::idx_t> _sort_perm;
  std::vector<int64_t> _sorted_rows;
  std::vector<int64_t> _sorted_files;
  std::vector<duckdb::idx_t> _output_positions;
};

}  // namespace sdb::connector
