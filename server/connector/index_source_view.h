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
#include <duckdb/planner/table_filter_set.hpp>
#include <iresearch/index/index_source.hpp>
#include <span>
#include <string_view>
#include <vector>

#include "catalog/table_options.h"
#include "connector/view_fast_path.h"

namespace sdb::connector {

class ViewIndexSourceBase : public IndexSource {
 protected:
  explicit ViewIndexSourceBase(ViewFastPath fast_path)
    : _fast_path{std::move(fast_path)} {}

  void InitProjection(
    duckdb::ClientContext& context,
    std::span<const duckdb::idx_t> projected_columns,
    std::span<const duckdb::LogicalType> projected_types,
    std::span<const catalog::Column::Id> bind_column_ids,
    absl::FunctionRef<duckdb::idx_t(std::string_view)> col_by_name,
    absl::FunctionRef<duckdb::LogicalType(duckdb::idx_t)> add_source_column);

  void SortRows(const PrimaryKeyBatch& pk, duckdb::idx_t start,
                duckdb::idx_t count);
  void SortFilesRows(const PrimaryKeyBatch& pk, duckdb::idx_t start,
                     duckdb::idx_t count);

  void AliasOutput(duckdb::DataChunk& output);
  void RunCastPass(duckdb::DataChunk& output, duckdb::idx_t row_count);
  // Reorder the doc-id-keyed output columns (every slot the lookup did not
  // write) into survivor order, matching the compact lookup emit.
  // `survivor_idx` maps each output row to the requested-pk index it came from
  // (always set).
  void GatherNonLookupColumns(duckdb::DataChunk& output, duckdb::idx_t count,
                              const duckdb::idx_t* survivor_idx);

  ViewFastPath _fast_path;
  std::vector<duckdb::idx_t> _real_proj_slots;
  duckdb::vector<duckdb::LogicalType> _scratch_types;
  duckdb::vector<duckdb::LogicalType> _projected_types;
  std::vector<duckdb::unique_ptr<duckdb::ExpressionExecutor>> _cast_executors;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> _cast_expressions;

  duckdb::DataChunk _tf_target;

  std::vector<duckdb::idx_t> _sort_perm;
  std::vector<int64_t> _sorted_rows;
  std::vector<int64_t> _sorted_files;
  // Doc-id-keyed (non-lookup) output slots + a reusable selection, computed
  // once at init: GatherNonLookupColumns reorders exactly these slots each
  // batch, so neither is rebuilt per batch.
  std::vector<duckdb::idx_t> _non_lookup_slots;
  duckdb::SelectionVector _gather_sel;
  // Filled by a filtered/compacted lookup: dense survivor row -> sorted-pk
  // index (feeds GatherNonLookupColumns). Empty when the lookup kept every pk.
  std::vector<duckdb::idx_t> _survivor_idx;
};

}  // namespace sdb::connector
