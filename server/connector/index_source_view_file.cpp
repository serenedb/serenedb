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

#include <algorithm>
#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <numeric>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"

namespace sdb::connector {

ViewFileIndexSourceBase::ViewFileIndexSourceBase(
  duckdb::ClientContext& context, ViewFastPath fast_path,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids)
  : _fast_path{std::move(fast_path)} {
  _bind_data = BindFastPathSource(context, _fast_path);
  _lookup_func = MakeFastPathLookupFunction(_fast_path);

  containers::FlatHashMap<std::string_view, duckdb::idx_t> name_to_file_col;
  auto& multi_bd = _bind_data->Cast<duckdb::MultiFileBindData>();
  if (!_fast_path.projection_columns.empty()) {
    name_to_file_col.reserve(multi_bd.names.size());
    for (duckdb::idx_t i = 0; i < multi_bd.names.size(); ++i) {
      name_to_file_col.emplace(multi_bd.names[i], i);
    }
  }
  _column_indexes.reserve(projected_columns.size());
  _real_proj_slots.reserve(projected_columns.size());
  _scratch_types.reserve(projected_columns.size());
  _projected_types.reserve(projected_columns.size());
  for (duckdb::idx_t proj = 0; proj < projected_columns.size(); ++proj) {
    const auto bind_col = projected_columns[proj];
    if (bind_col == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    SDB_ASSERT(bind_col < bind_column_ids.size());
    duckdb::idx_t file_col_idx;
    if (_fast_path.projection_columns.empty()) {
      file_col_idx = static_cast<duckdb::idx_t>(bind_column_ids[bind_col]);
    } else {
      const auto view_col_idx =
        static_cast<duckdb::idx_t>(bind_column_ids[bind_col]);
      SDB_ASSERT(view_col_idx < _fast_path.projection_columns.size());
      const auto& reader_name = _fast_path.projection_columns[view_col_idx];
      auto it = name_to_file_col.find(reader_name);
      SDB_ASSERT(it != name_to_file_col.end());
      file_col_idx = it->second;
    }
    SDB_ASSERT(file_col_idx < multi_bd.types.size());
    _column_indexes.emplace_back(file_col_idx);
    _real_proj_slots.push_back(proj);
    _scratch_types.push_back(multi_bd.types[file_col_idx]);
    _projected_types.push_back(projected_types[proj]);
  }
  _cast_executors.resize(_scratch_types.size());
  for (size_t c = 0; c < _scratch_types.size(); ++c) {
    if (_scratch_types[c] == _projected_types[c]) {
      continue;
    }
    auto ref = duckdb::make_uniq<duckdb::BoundReferenceExpression>(
      _scratch_types[c], static_cast<duckdb::idx_t>(c));
    auto cast_expr = duckdb::BoundCastExpression::AddCastToType(
      context, std::move(ref), _projected_types[c]);
    auto exec = duckdb::make_uniq<duckdb::ExpressionExecutor>(context);
    exec->AddExpression(*cast_expr);
    _cast_expressions.push_back(std::move(cast_expr));
    _cast_executors[c] = std::move(exec);
  }
  _tf_target.Initialize(context, _scratch_types);
}

namespace {

// Match cols alias output directly so TF writes land there in-place; cast
// cols keep _tf_target's own (file-typed) buffer for the end-of-call cast.
void AliasTfTarget(
  duckdb::DataChunk& tf_target, duckdb::DataChunk& output,
  const std::vector<duckdb::idx_t>& real_proj_slots,
  const std::vector<duckdb::unique_ptr<duckdb::ExpressionExecutor>>&
    cast_executors) {
  tf_target.Reset();
  for (duckdb::idx_t c = 0; c < real_proj_slots.size(); ++c) {
    if (cast_executors[c]) {
      continue;
    }
    tf_target.data[c].Reference(output.data[real_proj_slots[c]]);
  }
}

void RunCastPass(
  duckdb::DataChunk& tf_target, duckdb::DataChunk& output,
  duckdb::idx_t row_count,
  const duckdb::vector<duckdb::LogicalType>& scratch_types,
  const std::vector<duckdb::idx_t>& real_proj_slots,
  const std::vector<duckdb::unique_ptr<duckdb::ExpressionExecutor>>&
    cast_executors) {
  const bool has_cast =
    std::any_of(cast_executors.begin(), cast_executors.end(),
                [](const auto& e) { return e != nullptr; });
  if (!has_cast || row_count == 0) {
    return;
  }
  duckdb::DataChunk cast_input;
  cast_input.InitializeEmpty(scratch_types);
  for (duckdb::idx_t c = 0; c < real_proj_slots.size(); ++c) {
    if (cast_executors[c]) {
      cast_input.data[c].Reference(tf_target.data[c]);
    }
  }
  cast_input.SetCardinality(row_count);
  for (duckdb::idx_t c = 0; c < real_proj_slots.size(); ++c) {
    if (cast_executors[c]) {
      cast_executors[c]->ExecuteExpression(cast_input,
                                           output.data[real_proj_slots[c]]);
    }
  }
}

}  // namespace

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
  auto& pk = std::get<PrimaryKeyI64>(batch);
  SDB_ASSERT(start + count <= pk.rows.size());

  // TF wants sorted lookups (parquet row-group skipping, csv/json forward
  // cursor).
  _sort_perm.resize(count);
  std::iota(_sort_perm.begin(), _sort_perm.end(), duckdb::idx_t{0});
  std::sort(_sort_perm.begin(), _sort_perm.end(),
            [&](duckdb::idx_t a, duckdb::idx_t b) {
              return pk.rows[start + a] < pk.rows[start + b];
            });
  _sorted_rows.resize(count);
  _output_positions.resize(count);
  for (duckdb::idx_t k = 0; k < count; ++k) {
    _sorted_rows[k] = pk.rows[start + _sort_perm[k]];
    _output_positions[k] = _sort_perm[k];
  }

  AliasTfTarget(_tf_target, output, _real_proj_slots, _cast_executors);
  _tf_target.SetCardinality(count);

  duckdb::TableFunctionInput in(_bind_data.get(), /*local_state=*/nullptr,
                                _lookup_gstate.get());
  in.pk_lookups = _sorted_rows;
  in.pk_output_positions = _output_positions;
  _lookup_func.function(context, in, _tf_target);

  RunCastPass(_tf_target, output, count, _scratch_types, _real_proj_slots,
              _cast_executors);
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
  auto& pk = std::get<PrimaryKeyI64I64>(batch);
  SDB_ASSERT(start + count <= pk.rows.size());

  _sort_perm.resize(count);
  std::iota(_sort_perm.begin(), _sort_perm.end(), duckdb::idx_t{0});
  std::sort(_sort_perm.begin(), _sort_perm.end(),
            [&](duckdb::idx_t a, duckdb::idx_t b) {
              if (pk.files[start + a] != pk.files[start + b]) {
                return pk.files[start + a] < pk.files[start + b];
              }
              return pk.rows[start + a] < pk.rows[start + b];
            });
  _sorted_files.resize(count);
  _sorted_rows.resize(count);
  _output_positions.resize(count);
  for (duckdb::idx_t k = 0; k < count; ++k) {
    _sorted_files[k] = pk.files[start + _sort_perm[k]];
    _sorted_rows[k] = pk.rows[start + _sort_perm[k]];
    _output_positions[k] = _sort_perm[k];
  }

  auto& multi_bd = _bind_data->Cast<duckdb::MultiFileBindData>();
  SDB_ASSERT(multi_bd.file_list);
  // Force iceberg's lazy manifest expansion (no-op for eager globs).
  (void)multi_bd.file_list->GetTotalFileCount();
  auto files = multi_bd.file_list->GetAllFiles();
  if (_file_cache.size() < files.size()) {
    _file_cache.resize(files.size());
  }

  AliasTfTarget(_tf_target, output, _real_proj_slots, _cast_executors);
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

  RunCastPass(_tf_target, output, count, _scratch_types, _real_proj_slots,
              _cast_executors);
}

}  // namespace sdb::connector
