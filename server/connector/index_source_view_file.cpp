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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/common/hive_partitioning.hpp>
#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <iresearch/utils/assert.hpp>

namespace sdb::connector {

ViewFileIndexSourceBase::ViewFileIndexSourceBase(
  duckdb::ClientContext& context, ViewFastPath fast_path,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const ColumnId> bind_column_ids,
  duckdb::TableFilterSet* pushed_filters)
  : ViewIndexSourceBase{std::move(fast_path)} {
  _bind_data = BindFastPathSource(context, _fast_path);
  _lookup_func = MakeFastPathLookupFunction(_fast_path);

  auto& multi_bd = _bind_data->Cast<duckdb::MultiFileBindData>();
  const auto& reader_bind = multi_bd.reader_bind;
  const auto& partitions = reader_bind.hive_partitioning_indexes;
  const auto partition_of = [&](duckdb::idx_t file_col_idx) {
    return absl::c_find_if(partitions,
                           [&](const duckdb::HivePartitioningIndex& candidate) {
                             return candidate.index == file_col_idx;
                           });
  };
  const auto is_derived = [&](duckdb::idx_t file_col_idx) {
    return reader_bind.filename_idx == file_col_idx ||
           reader_bind.file_row_number_idx == file_col_idx ||
           partition_of(file_col_idx) != partitions.end();
  };
  _column_indexes.reserve(projected_columns.size());
  duckdb::idx_t column = 0;
  InitProjection(
    context, projected_columns, projected_types, bind_column_ids,
    SourceColumns{multi_bd.names, &duckdb::Identifier::GetIdentifierName},
    [&](duckdb::idx_t file_col_idx) {
      SDB_ASSERT(file_col_idx < multi_bd.types.size());
      if (reader_bind.file_row_number_idx == file_col_idx) {
        _row_number.emplace(RowNumber{.column = column});
      } else if (reader_bind.filename_idx == file_col_idx) {
        _file_constants.push_back({.column = column});
      } else if (const auto partition = partition_of(file_col_idx);
                 partition != partitions.end()) {
        _file_constants.push_back(
          {.column = column, .partition = partition->value});
      } else {
        _lookup_columns.push_back(column);
        _column_indexes.emplace_back(file_col_idx);
      }
      ++column;
      return multi_bd.types[file_col_idx];
    });
  if (_column_indexes.empty() && HasDerivedColumns()) {
    for (duckdb::idx_t file_col_idx = 0; file_col_idx < multi_bd.types.size();
         ++file_col_idx) {
      if (!is_derived(file_col_idx)) {
        _lookup_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        _column_indexes.emplace_back(file_col_idx);
        break;
      }
    }
  }
  BuildPushedFilters(context, pushed_filters);
}

void ViewFileIndexSourceBase::BuildPushedFilters(
  duckdb::ClientContext& context, const duckdb::TableFilterSet* input_filters) {
  if (!input_filters || !input_filters->HasFilters()) {
    return;
  }
  SDB_ASSERT(_lookup_columns.size() == _column_indexes.size());
  const auto filter_of =
    [&](duckdb::idx_t column) -> duckdb::unique_ptr<duckdb::ExpressionFilter> {
    const auto filter = input_filters->TryGetFilterByColumnIndex(
      duckdb::ProjectionIndex(_real_proj_slots[column]));
    if (!filter) {
      return nullptr;
    }
    return duckdb::ExpressionFilter::GetExpressionFilter(
             *filter, "ViewFileIndexSourceBase::BuildPushedFilters")
      .Copy();
  };
  auto set = duckdb::make_uniq<duckdb::TableFilterSet>();
  for (duckdb::idx_t k = 0; k < _lookup_columns.size(); ++k) {
    if (_lookup_columns[k] == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    if (auto filter = filter_of(_lookup_columns[k])) {
      set->PushFilter(duckdb::ProjectionIndex(k), std::move(filter));
    }
  }
  for (auto& constant : _file_constants) {
    constant.filter = filter_of(constant.column);
  }
  if (_row_number) {
    _row_number->filter = filter_of(_row_number->column);
    if (_row_number->filter) {
      _row_number->filter_state =
        duckdb::TableFilterState::Initialize(context, *_row_number->filter);
    }
  }
  if (set->HasFilters()) {
    _pushed_filters = std::move(set);
  }
}

duckdb::vector<duckdb::LogicalType> ViewFileIndexSourceBase::LookupTypes()
  const {
  const auto& multi_bd = _bind_data->Cast<duckdb::MultiFileBindData>();
  duckdb::vector<duckdb::LogicalType> types;
  types.reserve(_column_indexes.size());
  for (const auto& column : _column_indexes) {
    types.push_back(multi_bd.types[column.GetPrimaryIndex()]);
  }
  return types;
}

bool ViewFileIndexSourceBase::BindFileConstants(
  duckdb::ClientContext& context, const std::string& path,
  std::vector<duckdb::Value>& values) const {
  if (_file_constants.empty()) {
    return true;
  }
  const auto& multi_bd = _bind_data->Cast<duckdb::MultiFileBindData>();
  const auto partitions = duckdb::HivePartitioning::Parse(path);
  values.clear();
  values.reserve(_file_constants.size());
  bool match = true;
  for (const auto& constant : _file_constants) {
    const auto& type = _scratch_types[constant.column];
    duckdb::Value value{type};
    if (!constant.partition) {
      value = duckdb::Value(path);
    } else if (const auto entry = partitions.find(*constant.partition);
               entry != partitions.end()) {
      value =
        multi_bd.file_options
          .GetHivePartitionValue(entry->second, *constant.partition, context)
          .DefaultCastAs(type);
    }
    if (constant.filter &&
        !constant.filter->EvaluateWithConstant(context, value)) {
      match = false;
    }
    values.push_back(std::move(value));
  }
  return match;
}

std::span<const int64_t> ViewFileIndexSourceBase::SelectRows(
  std::span<const int64_t> rows) {
  if (!_row_number || !_row_number->filter_state) {
    return rows;
  }
  _selected_rows.clear();
  _selected_positions.clear();
  for (duckdb::idx_t offset = 0; offset < rows.size();
       offset += STANDARD_VECTOR_SIZE) {
    const auto count =
      std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, rows.size() - offset);
    duckdb::Vector numbers(duckdb::LogicalType::BIGINT, count);
    {
      auto writer = duckdb::FlatVector::Writer<int64_t>(numbers, count);
      for (duckdb::idx_t k = 0; k < count; ++k) {
        writer.WriteValue(rows[offset + k]);
      }
    }
    duckdb::SelectionVector sel;
    auto approved = count;
    duckdb::ColumnSegment::FilterSelection(
      sel, numbers, *_row_number->filter_state, count, approved);
    for (duckdb::idx_t k = 0; k < approved; ++k) {
      const auto position = offset + sel.get_index(k);
      _selected_rows.push_back(rows[position]);
      _selected_positions.push_back(position);
    }
  }
  return _selected_rows;
}

duckdb::idx_t ViewFileIndexSourceBase::SelectedPosition(
  duckdb::idx_t row) const {
  if (!_row_number || !_row_number->filter_state) {
    return row;
  }
  return _selected_positions[row];
}

void ViewFileIndexSourceBase::CopyLookupColumns(duckdb::DataChunk& source,
                                                duckdb::idx_t count,
                                                duckdb::idx_t offset) {
  for (duckdb::idx_t k = 0; k < _lookup_columns.size(); ++k) {
    if (_lookup_columns[k] == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    duckdb::VectorOperations::Copy(
      source.data[k], _tf_target.data[_lookup_columns[k]], count, 0, offset);
  }
}

void ViewFileIndexSourceBase::FillDerivedColumns(
  std::span<const duckdb::Value> constants, std::span<const int64_t> rows,
  std::span<const duckdb::idx_t> survivors, duckdb::idx_t offset) {
  const auto count = survivors.size();
  for (size_t c = 0; c < _file_constants.size(); ++c) {
    duckdb::Vector constant(constants[c], duckdb::count_t(count));
    duckdb::VectorOperations::Copy(
      constant, _tf_target.data[_file_constants[c].column], count, 0, offset);
  }
  if (_row_number) {
    auto numbers = duckdb::FlatVector::Writer<int64_t>(
      _tf_target.data[_row_number->column], count, offset);
    for (const auto survivor : survivors) {
      numbers.WriteValue(rows[survivor]);
    }
  }
}

ViewFileSingleFileIndexSource::ViewFileSingleFileIndexSource(
  duckdb::ClientContext& context, ViewFastPath fast_path,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const ColumnId> bind_column_ids,
  duckdb::TableFilterSet* pushed_filters)
  : ViewFileIndexSourceBase(context, std::move(fast_path), projected_columns,
                            projected_types, bind_column_ids, pushed_filters) {
  duckdb::TableFunctionInitInput init(_bind_data.get(), _column_indexes,
                                      /*projection_ids=*/{},
                                      _pushed_filters.get());
  _lookup_gstate = _lookup_func.init_global(context, init);
  if (HasDerivedColumns()) {
    _lookup_target.Initialize(context, LookupTypes());
    _constants_match =
      BindFileConstants(context,
                        _bind_data->Cast<duckdb::MultiFileBindData>()
                          .file_list->GetFirstFile()
                          .path,
                        _constants);
  }
}

duckdb::idx_t ViewFileSingleFileIndexSource::Materialize(
  duckdb::ClientContext& context, duckdb::Vector& pk, duckdb::idx_t count,
  duckdb::DataChunk& output) {
  if (count == 0) {
    return 0;
  }

  const auto keys = SortRows(pk, count);

  AliasOutput(output);
  _tf_target.SetCardinality(0);
  _survivor_idx.resize(count);
  if (!_constants_match) {
    return 0;
  }
  const auto selected = SelectRows(keys);
  auto& target = HasDerivedColumns() ? _lookup_target : _tf_target;
  if (HasDerivedColumns()) {
    _lookup_target.Reset();
    _lookup_target.SetCardinality(0);
  }

  duckdb::TableFunctionInput in(_bind_data.get(), /*local_state=*/nullptr,
                                _lookup_gstate.get());
  in.pk_lookups = selected;
  in.pk_survivors = std::span{_survivor_idx}.first(selected.size());
  _lookup_func.function(context, in, target);
  const auto rows = target.size();
  const auto survivors = std::span{_survivor_idx}.first(rows);
  if (HasDerivedColumns()) {
    CopyLookupColumns(_lookup_target, rows, 0);
    FillDerivedColumns(_constants, selected, survivors, 0);
    _tf_target.SetCardinality(rows);
  }
  for (auto& survivor : survivors) {
    survivor = SelectedPosition(survivor);
  }

  RunCastPass(output, rows);
  GatherNonLookupColumns(output, rows);
  return rows;
}

ViewFileGlobIndexSource::ViewFileGlobIndexSource(
  duckdb::ClientContext& context, ViewFastPath fast_path,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const ColumnId> bind_column_ids,
  duckdb::TableFilterSet* pushed_filters,
  std::shared_ptr<const search::FileManifest> file_manifest)
  : ViewFileIndexSourceBase(context, std::move(fast_path), projected_columns,
                            projected_types, bind_column_ids, pushed_filters),
    _file_manifest(std::move(file_manifest)) {}

duckdb::idx_t ViewFileGlobIndexSource::Materialize(
  duckdb::ClientContext& context, duckdb::Vector& pk, duckdb::idx_t count,
  duckdb::DataChunk& output) {
  if (count == 0) {
    return 0;
  }

  SortFilesRows(pk, count);

  SDB_ASSERT(_file_manifest);

  AliasOutput(output);
  if (_file_target.ColumnCount() == 0) {
    _file_target.Initialize(context, LookupTypes());
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
    const uint64_t fi = _sorted_files[i];
    const auto* entry = _file_manifest->FindById(fi);
    if (!entry) {
      // A doc committed by an in-flight (or died) pass: its id is not part
      // of the published manifest version, so it is not readable until the
      // pass's Finalize publishes -- reads reflect complete versions only.
      i = j;
      continue;
    }
    const std::string& file_path = entry->path;
    auto& cached = _file_cache[fi];
    if (!cached.bind_data) {
      ViewFastPath single_fp = _fast_path;
      single_fp.args.clear();
      single_fp.args.push_back(duckdb::Value{file_path});
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
      cached.constants_match =
        BindFileConstants(context, file_path, cached.constants);
    }
    if (!cached.constants_match) {
      i = j;
      continue;
    }

    const auto selected =
      SelectRows(std::span<const int64_t>{_sorted_rows.data() + i, j - i});
    _file_survivor_idx.resize(selected.size());
    _file_target.Reset();
    _file_target.SetCardinality(0);

    duckdb::TableFunctionInput in(cached.bind_data.get(),
                                  /*local_state=*/nullptr, cached.gstate.get());
    in.pk_lookups = selected;
    in.pk_survivors = _file_survivor_idx;
    _lookup_func.function(context, in, _file_target);

    const auto file_rows = _file_target.size();
    const auto survivors = std::span{_file_survivor_idx}.first(file_rows);
    CopyLookupColumns(_file_target, file_rows, total);
    FillDerivedColumns(cached.constants, selected, survivors, total);
    for (duckdb::idx_t k = 0; k < file_rows; ++k) {
      _survivor_idx[total + k] = i + SelectedPosition(survivors[k]);
    }
    total += file_rows;
    i = j;
  }
  _tf_target.SetCardinality(total);

  RunCastPass(output, total);
  GatherNonLookupColumns(output, total);
  return total;
}

}  // namespace sdb::connector
