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

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/table_filter_set.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "connector/column_id.h"
#include "connector/file_manifest.h"
#include "connector/index_source_view.h"

namespace sdb::connector {

class NativeIndexSource final : public ViewIndexSourceBase {
 public:
  NativeIndexSource(duckdb::ClientContext& context, ViewFastPath fast_path,
                    duckdb::optional_ptr<duckdb::TableCatalogEntry> table,
                    std::shared_ptr<const search::FileManifest> file_manifest,
                    std::span<const duckdb::idx_t> projected_columns,
                    std::span<const duckdb::LogicalType> projected_types,
                    std::span<const ColumnId> bind_column_ids,
                    duckdb::TableFilterSet* pushed_filters);

  duckdb::idx_t Materialize(duckdb::ClientContext& context, duckdb::Vector& pk,
                            duckdb::idx_t count,
                            duckdb::DataChunk& output) final;

 private:
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

  struct FileConstants {
    std::vector<duckdb::Value> values;
    bool match = true;
  };

  void BindTable(duckdb::ClientContext& context,
                 duckdb::TableCatalogEntry& table,
                 std::span<const duckdb::idx_t> projected_columns,
                 std::span<const duckdb::LogicalType> projected_types,
                 std::span<const ColumnId> bind_column_ids, bool by_id);
  void BindFiles(duckdb::ClientContext& context,
                 const search::FileManifest* file_manifest,
                 std::span<const duckdb::idx_t> projected_columns,
                 std::span<const duckdb::LogicalType> projected_types,
                 std::span<const ColumnId> bind_column_ids);
  // Re-keys the scan's output-slot-keyed filters onto this reader's projected
  // columns, dropping non-lookup slots (e.g. the score) that have no source
  // column -- forwarding those to the reader mismatches column types.
  void BuildPushedFilters(duckdb::ClientContext& context,
                          const duckdb::TableFilterSet* input_filters);
  bool HasDerivedColumns() const noexcept {
    return !_file_constants.empty() || _row_number;
  }
  const FileConstants& ConstantsOf(duckdb::ClientContext& context,
                                   duckdb::idx_t file);
  void SelectFileKeys(duckdb::ClientContext& context, duckdb::Vector& pk,
                      duckdb::idx_t count);
  void FillDerivedColumns(duckdb::ClientContext& context, duckdb::idx_t rows);

  duckdb::unique_ptr<duckdb::LogicalGet> _source;
  duckdb::TableFunction _lookup;
  duckdb::unique_ptr<duckdb::GlobalTableFunctionState> _global;
  duckdb::unique_ptr<duckdb::LocalTableFunctionState> _local;
  // Lookup-column filters forwarded to the underlying reader (parquet row-group
  // pruning + native FilterSelection); the lookup scan compacts to survivors.
  // Null when none.
  duckdb::unique_ptr<duckdb::TableFilterSet> _pushed_filters;
  bool _by_file = false;
  bool _no_files = false;
  std::vector<duckdb::idx_t> _lookup_columns;
  std::vector<FileConstant> _file_constants;
  std::optional<RowNumber> _row_number;
  std::vector<std::string> _paths;
  irs::containers::FlatHashMap<uint64_t, duckdb::idx_t> _file_positions;
  irs::containers::FlatHashMap<duckdb::idx_t, FileConstants> _constants;
  duckdb::DataChunk _keys;
  duckdb::DataChunk _lookup_target;
  std::vector<duckdb::idx_t> _selected;
  std::vector<duckdb::idx_t> _key_files;
  std::vector<int64_t> _key_rows;
};

}  // namespace sdb::connector
