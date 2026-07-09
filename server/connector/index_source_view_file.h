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
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/table_filter_set.hpp>
#include <span>

#include "connector/index_source_view.h"

namespace sdb::connector {

class ViewFileIndexSourceBase : public ViewIndexSourceBase {
 protected:
  ViewFileIndexSourceBase(duckdb::ClientContext& context,
                          ViewFastPath fast_path,
                          std::span<const duckdb::idx_t> projected_columns,
                          std::span<const duckdb::LogicalType> projected_types,
                          std::span<const catalog::Column::Id> bind_column_ids,
                          duckdb::TableFilterSet* pushed_filters);

  duckdb::TableFunction _lookup_func;
  duckdb::unique_ptr<duckdb::FunctionData> _bind_data;
  duckdb::vector<duckdb::ColumnIndex> _column_indexes;
  // Row-fetch filters pushed into the underlying reader for row-group pruning
  // (parquet); null when none. Pruning is advisory -- pruned rows come back
  // NULL and ScanFilter re-checks, so results are unaffected.
  duckdb::TableFilterSet* _pushed_filters = nullptr;
};

class ViewFileSingleFileIndexSource final : public ViewFileIndexSourceBase {
 public:
  ViewFileSingleFileIndexSource(
    duckdb::ClientContext& context, ViewFastPath fast_path,
    std::span<const duckdb::idx_t> projected_columns,
    std::span<const duckdb::LogicalType> projected_types,
    std::span<const catalog::Column::Id> bind_column_ids,
    duckdb::TableFilterSet* pushed_filters = nullptr);

  PrimaryKeyBatch::Kind PkKind() const final {
    return PrimaryKeyBatch::Kind::I64;
  }
  void Materialize(duckdb::ClientContext& context, PrimaryKeyBatch& batch,
                   duckdb::idx_t start, duckdb::idx_t count,
                   duckdb::DataChunk& output) final;

 private:
  duckdb::unique_ptr<duckdb::GlobalTableFunctionState> _lookup_gstate;
};

class ViewFileGlobIndexSource final : public ViewFileIndexSourceBase {
 public:
  ViewFileGlobIndexSource(duckdb::ClientContext& context,
                          ViewFastPath fast_path,
                          std::span<const duckdb::idx_t> projected_columns,
                          std::span<const duckdb::LogicalType> projected_types,
                          std::span<const catalog::Column::Id> bind_column_ids,
                          duckdb::TableFilterSet* pushed_filters = nullptr);

  PrimaryKeyBatch::Kind PkKind() const final {
    return PrimaryKeyBatch::Kind::I64I64;
  }
  void Materialize(duckdb::ClientContext& context, PrimaryKeyBatch& batch,
                   duckdb::idx_t start, duckdb::idx_t count,
                   duckdb::DataChunk& output) final;

 private:
  // Per-file lookup state, built lazily on first hit and reused across batches.
  struct CachedFileLookup {
    duckdb::unique_ptr<duckdb::FunctionData> bind_data;
    duckdb::unique_ptr<duckdb::GlobalTableFunctionState> gstate;
  };
  std::vector<CachedFileLookup> _file_cache;
};

}  // namespace sdb::connector
