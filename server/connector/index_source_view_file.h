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
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter_set.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <optional>
#include <span>

#include "connector/column_id.h"
#include "connector/file_manifest.h"
#include "connector/index_source_view.h"

namespace sdb::connector {

class ViewFileIndexSourceBase : public ViewIndexSourceBase {
 protected:
  ViewFileIndexSourceBase(duckdb::ClientContext& context,
                          ViewFastPath fast_path,
                          std::span<const duckdb::idx_t> projected_columns,
                          std::span<const duckdb::LogicalType> projected_types,
                          std::span<const ColumnId> bind_column_ids,
                          duckdb::TableFilterSet* pushed_filters);

  // Re-keys the scan's output-slot-keyed filters onto this reader's projected
  // columns, dropping non-lookup slots (e.g. the score) that have no source
  // column -- forwarding those to the reader mismatches column types.
  void BuildPushedFilters(duckdb::ClientContext& context,
                          const duckdb::TableFilterSet* input_filters);

  struct FileConstant {
    duckdb::idx_t column;
    std::optional<std::string> partition;
    duckdb::unique_ptr<duckdb::ExpressionFilter> filter;
  };

  struct RowNumber {
    duckdb::idx_t column;
    duckdb::unique_ptr<duckdb::ExpressionFilter> filter;
    duckdb::unique_ptr<duckdb::TableFilterState> filter_state;
  };

  bool HasDerivedColumns() const noexcept {
    return !_file_constants.empty() || _row_number;
  }
  duckdb::vector<duckdb::LogicalType> LookupTypes() const;
  bool BindFileConstants(duckdb::ClientContext& context,
                         const std::string& path,
                         std::vector<duckdb::Value>& values) const;
  std::span<const int64_t> SelectRows(std::span<const int64_t> rows);
  duckdb::idx_t SelectedPosition(duckdb::idx_t row) const;
  void CopyLookupColumns(duckdb::DataChunk& source, duckdb::idx_t count,
                         duckdb::idx_t offset);
  void FillDerivedColumns(std::span<const duckdb::Value> constants,
                          std::span<const int64_t> rows,
                          std::span<const duckdb::idx_t> survivors,
                          duckdb::idx_t offset);

  duckdb::TableFunction _lookup_func;
  duckdb::unique_ptr<duckdb::FunctionData> _bind_data;
  duckdb::vector<duckdb::ColumnIndex> _column_indexes;
  std::vector<duckdb::idx_t> _lookup_columns;
  std::vector<FileConstant> _file_constants;
  std::optional<RowNumber> _row_number;
  std::vector<int64_t> _selected_rows;
  std::vector<duckdb::idx_t> _selected_positions;
  // Lookup-column filters forwarded to the underlying reader (parquet row-group
  // pruning + native FilterSelection); the lookup scan compacts to survivors.
  // Null when none.
  duckdb::unique_ptr<duckdb::TableFilterSet> _pushed_filters;
};

class ViewFileSingleFileIndexSource final : public ViewFileIndexSourceBase {
 public:
  ViewFileSingleFileIndexSource(
    duckdb::ClientContext& context, ViewFastPath fast_path,
    std::span<const duckdb::idx_t> projected_columns,
    std::span<const duckdb::LogicalType> projected_types,
    std::span<const ColumnId> bind_column_ids,
    duckdb::TableFilterSet* pushed_filters = nullptr);

  duckdb::idx_t Materialize(duckdb::ClientContext& context, duckdb::Vector& pk,
                            duckdb::idx_t count,
                            duckdb::DataChunk& output) final;

 private:
  duckdb::unique_ptr<duckdb::GlobalTableFunctionState> _lookup_gstate;
  duckdb::DataChunk _lookup_target;
  std::vector<duckdb::Value> _constants;
  bool _constants_match = true;
};

class ViewFileGlobIndexSource final : public ViewFileIndexSourceBase {
 public:
  ViewFileGlobIndexSource(
    duckdb::ClientContext& context, ViewFastPath fast_path,
    std::span<const duckdb::idx_t> projected_columns,
    std::span<const duckdb::LogicalType> projected_types,
    std::span<const ColumnId> bind_column_ids,
    duckdb::TableFilterSet* pushed_filters = nullptr,
    std::shared_ptr<const search::FileManifest> file_manifest = nullptr);

  duckdb::idx_t Materialize(duckdb::ClientContext& context, duckdb::Vector& pk,
                            duckdb::idx_t count,
                            duckdb::DataChunk& output) final;

 private:
  // Per-file lookup state, built lazily on first hit and reused across batches.
  struct CachedFileLookup {
    duckdb::unique_ptr<duckdb::FunctionData> bind_data;
    duckdb::unique_ptr<duckdb::GlobalTableFunctionState> gstate;
    std::vector<duckdb::Value> constants;
    bool constants_match = true;
  };
  irs::containers::FlatHashMap<uint64_t, CachedFileLookup> _file_cache;
  // The pinned snapshot's source manifest: docs store manifest file_ids, so
  // paths resolve through it (never through the live glob expansion).
  std::shared_ptr<const search::FileManifest> _file_manifest;

  // Each per-file lookup writes its survivors compactly from row 0 (the lookup
  // TF's per-call contract). We run each file into `_file_target` and append
  // its survivors into `_tf_target` at the running offset, so the batch
  // accumulates across files instead of each file overwriting the last.
  duckdb::DataChunk _file_target;
  std::vector<duckdb::idx_t> _file_survivor_idx;
};

}  // namespace sdb::connector
