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

#include "connector/index_source_view_file.h"

#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"

namespace sdb::connector {

ViewFileIndexSourceBase::ViewFileIndexSourceBase(
  duckdb::ClientContext& context, ViewFastPath fast_path,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids,
  const duckdb::TableFilterSet* pushed_filters)
  : ViewIndexSourceBase{std::move(fast_path)} {
  _bind_data = BindFastPathSource(context, _fast_path);
  _lookup_func = MakeFastPathLookupFunction(_fast_path);

  auto& multi_bd = _bind_data->Cast<duckdb::MultiFileBindData>();
  containers::FlatHashMap<std::string_view, duckdb::idx_t> name_to_file_col;
  if (!_fast_path.projection_columns.empty()) {
    name_to_file_col.reserve(multi_bd.names.size());
    for (duckdb::idx_t i = 0; i < multi_bd.names.size(); ++i) {
      name_to_file_col.emplace(multi_bd.names[i].GetIdentifierName(), i);
    }
  }
  _column_indexes.reserve(projected_columns.size());
  InitProjection(
    context, projected_columns, projected_types, bind_column_ids,
    [&](std::string_view name) {
      auto it = name_to_file_col.find(name);
      SDB_ASSERT(it != name_to_file_col.end());
      return it->second;
    },
    [&](duckdb::idx_t file_col_idx) {
      SDB_ASSERT(file_col_idx < multi_bd.types.size());
      _column_indexes.emplace_back(file_col_idx);
      return multi_bd.types[file_col_idx];
    });
  SDB_ASSERT(_real_proj_slots.size() == _column_indexes.size());
  BuildPushedFilters(pushed_filters, {});
}

ViewFileSingleFileIndexSource::ViewFileSingleFileIndexSource(
  duckdb::ClientContext& context, ViewFastPath fast_path,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids,
  const duckdb::TableFilterSet* pushed_filters)
  : ViewFileIndexSourceBase(context, std::move(fast_path), projected_columns,
                            projected_types, bind_column_ids, pushed_filters) {
  duckdb::TableFunctionInitInput init(_bind_data.get(), _column_indexes,
                                      /*projection_ids=*/{},
                                      _pushed_filters.get());
  _lookup_gstate = _lookup_func.init_global(context, init);
}

duckdb::idx_t ViewFileSingleFileIndexSource::Materialize(
  duckdb::ClientContext& context, PrimaryKeyBatch& batch, duckdb::idx_t start,
  duckdb::idx_t count, duckdb::DataChunk& output) {
  if (count == 0) {
    return 0;
  }
  auto& pk = batch;
  SDB_ASSERT(start + count <= pk.rows.size());

  SortRows(pk, start, count);

  AliasOutput(output);
  // Dense: the lookup TF applies the pushed filters natively and appends
  // survivors from size 0, then reports the survivor count via the chunk's
  // cardinality and the sorted-pk index of each via pk_survivors.
  _tf_target.SetCardinality(0);
  _survivor_idx.resize(count);

  duckdb::TableFunctionInput in(_bind_data.get(), /*local_state=*/nullptr,
                                _lookup_gstate.get());
  in.pk_lookups = _sorted_rows;
  in.pk_survivors = _survivor_idx;
  _lookup_func.function(context, in, _tf_target);
  const auto rows = _tf_target.size();

  RunCastPass(output, rows);
  GatherNonLookupColumns(output, rows, _survivor_idx.data());
  return rows;
}

ViewFileGlobIndexSource::ViewFileGlobIndexSource(
  duckdb::ClientContext& context, ViewFastPath fast_path,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids,
  const duckdb::TableFilterSet* pushed_filters)
  : ViewFileIndexSourceBase(context, std::move(fast_path), projected_columns,
                            projected_types, bind_column_ids, pushed_filters) {}

duckdb::idx_t ViewFileGlobIndexSource::Materialize(
  duckdb::ClientContext& context, PrimaryKeyBatch& batch, duckdb::idx_t start,
  duckdb::idx_t count, duckdb::DataChunk& output) {
  if (count == 0) {
    return 0;
  }
  auto& pk = batch;
  SDB_ASSERT(start + count <= pk.rows.size());

  SortFilesRows(pk, start, count);

  auto& multi_bd = _bind_data->Cast<duckdb::MultiFileBindData>();
  SDB_ASSERT(multi_bd.file_list);
  std::ignore = multi_bd.file_list->GetTotalFileCount();
  auto files = multi_bd.file_list->GetAllFiles();
  if (_file_cache.size() < files.size()) {
    _file_cache.resize(files.size());
  }

  AliasOutput(output);
  if (_file_target.ColumnCount() == 0) {
    _file_target.Initialize(context, _scratch_types);
  }
  _survivor_idx.resize(count);

  // Each per-file lookup writes its survivors compactly from row 0 into
  // _file_target (the lookup TF's per-call contract: pk_survivors is sized to
  // that call's pk_lookups and filled 0-based). Copy each file's survivors into
  // _tf_target at the running offset so the batch accumulates across files
  // instead of each file overwriting the last.
  duckdb::idx_t total = 0;
  size_t i = 0;
  while (i < count) {
    size_t j = i;
    while (j < count && _sorted_files[j] == _sorted_files[i]) {
      ++j;
    }
    const auto fi = static_cast<size_t>(_sorted_files[i]);
    SDB_ASSERT(fi < files.size());
    auto& cached = _file_cache[fi];
    if (!cached.bind_data) {
      ViewFastPath single_fp = _fast_path;
      single_fp.args.clear();
      single_fp.args.push_back(duckdb::Value{files[fi].path});
      single_fp.is_glob = false;
      if (single_fp.function_name == "iceberg_scan") {
        single_fp.function_name = "read_parquet";
        single_fp.named_params.clear();
        single_fp.catalog_ref.reset();
      }
      cached.bind_data = BindFastPathSource(context, single_fp);
      duckdb::TableFunctionInitInput init(
        cached.bind_data.get(), _column_indexes,
        /*projection_ids=*/{}, _pushed_filters.get());
      cached.gstate = _lookup_func.init_global(context, init);
    }

    const auto file_count = j - i;
    _file_survivor_idx.resize(file_count);
    _file_target.Reset();
    _file_target.SetCardinality(0);

    duckdb::TableFunctionInput in(cached.bind_data.get(),
                                  /*local_state=*/nullptr, cached.gstate.get());
    in.pk_lookups =
      std::span<const int64_t>{_sorted_rows.data() + i, file_count};
    in.pk_survivors = _file_survivor_idx;
    _lookup_func.function(context, in, _file_target);

    const auto file_rows = _file_target.size();
    for (duckdb::idx_t c = 0; c < _real_proj_slots.size(); ++c) {
      duckdb::VectorOperations::Copy(_file_target.data[c], _tf_target.data[c],
                                     file_rows, /*source_offset=*/0,
                                     /*target_offset=*/total);
    }
    // The reader wrote survivors as 0-based indices into this file's pk_lookups
    // span (which starts at _sorted_rows[i]); shift to batch-global sorted-pk
    // indices so GatherNonLookupColumns can reorder the doc-id-keyed columns.
    for (duckdb::idx_t k = 0; k < file_rows; ++k) {
      _survivor_idx[total + k] = i + _file_survivor_idx[k];
    }
    total += file_rows;
    i = j;
  }
  _tf_target.SetCardinality(total);

  RunCastPass(output, total);
  GatherNonLookupColumns(output, total, _survivor_idx.data());
  return total;
}

}  // namespace sdb::connector
