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

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector_cache.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/function/table_function.hpp>
#include <span>
#include <string_view>
#include <vector>

#include "catalog/table_options.h"
#include "connector/index_source.h"
#include "connector/view_fast_path.h"

namespace sdb::connector {

class ViewFileIndexSourceBase : public IndexSource {
 protected:
  ViewFileIndexSourceBase(duckdb::ClientContext& context,
                          ViewFastPath fast_path,
                          std::span<const duckdb::idx_t> projected_columns,
                          std::span<const duckdb::LogicalType> projected_types,
                          std::span<const catalog::Column::Id> bind_column_ids);

  ViewFastPath _fast_path;
  duckdb::TableFunction _lookup_func;
  duckdb::unique_ptr<duckdb::FunctionData> _bind_data;
  duckdb::vector<duckdb::ColumnIndex> _column_indexes;
  std::vector<duckdb::idx_t> _real_proj_slots;
  duckdb::vector<duckdb::LogicalType> _scratch_types;
  duckdb::vector<duckdb::LogicalType> _projected_types;
  // nullptr when scratch and projected types match.
  std::vector<duckdb::unique_ptr<duckdb::ExpressionExecutor>> _cast_executors;
  // ExpressionExecutor holds the Expression by ref, must outlive it.
  std::vector<duckdb::unique_ptr<duckdb::Expression>> _cast_expressions;

  // Per-column alias to where the TF should write. Matching columns alias
  // output directly so the TF's writes land in the caller's buffer; cast
  // columns keep their own (file-typed) buffer that we cast at end of call.
  duckdb::DataChunk _tf_target;

  // Sort scratch reused per call. Sorting goes through these instead of the
  // caller's PrimaryKeyBatch because ANN / range scans persist their batch
  // across many slices and mutating it would break later slices.
  std::vector<duckdb::idx_t> _sort_perm;
  std::vector<int64_t> _sorted_rows;
  std::vector<int64_t> _sorted_files;
  std::vector<duckdb::idx_t> _output_positions;
};

class ViewFileSingleFileIndexSource final : public ViewFileIndexSourceBase {
 public:
  ViewFileSingleFileIndexSource(
    duckdb::ClientContext& context, ViewFastPath fast_path,
    std::span<const duckdb::idx_t> projected_columns,
    std::span<const duckdb::LogicalType> projected_types,
    std::span<const catalog::Column::Id> bind_column_ids);

  PrimaryKeyBatch CreatePkBatch() const override {
    return PrimaryKeyBatch{std::in_place_type<PrimaryKeyI64>};
  }
  void Materialize(duckdb::ClientContext& context, PrimaryKeyBatch& batch,
                   duckdb::idx_t start, duckdb::idx_t count,
                   duckdb::DataChunk& output) override;

 private:
  duckdb::unique_ptr<duckdb::GlobalTableFunctionState> _lookup_gstate;
};

class ViewFileGlobIndexSource final : public ViewFileIndexSourceBase {
 public:
  ViewFileGlobIndexSource(duckdb::ClientContext& context,
                          ViewFastPath fast_path,
                          std::span<const duckdb::idx_t> projected_columns,
                          std::span<const duckdb::LogicalType> projected_types,
                          std::span<const catalog::Column::Id> bind_column_ids);

  PrimaryKeyBatch CreatePkBatch() const override {
    return PrimaryKeyBatch{std::in_place_type<PrimaryKeyI64I64>};
  }
  void Materialize(duckdb::ClientContext& context, PrimaryKeyBatch& batch,
                   duckdb::idx_t start, duckdb::idx_t count,
                   duckdb::DataChunk& output) override;

 private:
  // Per-file lookup state, built lazily on first hit and reused across batches.
  struct CachedFileLookup {
    duckdb::unique_ptr<duckdb::FunctionData> bind_data;
    duckdb::unique_ptr<duckdb::GlobalTableFunctionState> gstate;
  };
  std::vector<CachedFileLookup> _file_cache;
};

}  // namespace sdb::connector
