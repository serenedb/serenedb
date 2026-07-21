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

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"

namespace sdb::connector {

ViewFileIndexSourceBase::ViewFileIndexSourceBase(
  duckdb::ClientContext& context, ViewFastPath fast_path,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids)
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
}

ViewFileSingleFileIndexSource::ViewFileSingleFileIndexSource(
  duckdb::ClientContext& context, ViewFastPath fast_path,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids)
  : ViewFileIndexSourceBase(context, std::move(fast_path), projected_columns,
                            projected_types, bind_column_ids) {
  duckdb::TableFunctionInitInput init(_bind_data.get(), _column_indexes,
                                      /*projection_ids=*/{},
                                      /*filters=*/nullptr);
  _lookup_gstate = _lookup_func.init_global(context, init);
}

void ViewFileSingleFileIndexSource::Materialize(duckdb::ClientContext& context,
                                                PrimaryKeyBatch& batch,
                                                duckdb::idx_t start,
                                                duckdb::idx_t count,
                                                duckdb::DataChunk& output) {
  if (count == 0) {
    return;
  }
  auto& pk = batch;
  SDB_ASSERT(start + count <= pk.rows.size());

  SortRows(pk, start, count);

  AliasOutput(output);
  _tf_target.SetCardinality(count);

  duckdb::TableFunctionInput in(_bind_data.get(), /*local_state=*/nullptr,
                                _lookup_gstate.get());
  in.pk_lookups = _sorted_rows;
  in.pk_output_positions = _output_positions;
  _lookup_func.function(context, in, _tf_target);

  RunCastPass(output, count);
}

ViewFileGlobIndexSource::ViewFileGlobIndexSource(
  duckdb::ClientContext& context, ViewFastPath fast_path,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids)
  : ViewFileIndexSourceBase(context, std::move(fast_path), projected_columns,
                            projected_types, bind_column_ids) {}

void ViewFileGlobIndexSource::Materialize(duckdb::ClientContext& context,
                                          PrimaryKeyBatch& batch,
                                          duckdb::idx_t start,
                                          duckdb::idx_t count,
                                          duckdb::DataChunk& output) {
  if (count == 0) {
    return;
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
  _tf_target.SetCardinality(count);

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
      duckdb::TableFunctionInitInput init(cached.bind_data.get(),
                                          _column_indexes,
                                          /*projection_ids=*/{},
                                          /*filters=*/nullptr);
      cached.gstate = _lookup_func.init_global(context, init);
    }

    duckdb::TableFunctionInput in(cached.bind_data.get(),
                                  /*local_state=*/nullptr, cached.gstate.get());
    const auto file_count = j - i;
    in.pk_lookups =
      std::span<const int64_t>{_sorted_rows.data() + i, file_count};
    in.pk_output_positions =
      std::span<const duckdb::idx_t>{_output_positions.data() + i, file_count};
    _lookup_func.function(context, in, _tf_target);
    i = j;
  }

  RunCastPass(output, count);
}

}  // namespace sdb::connector
