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

#include <duckdb/common/hive_partitioning.hpp>
#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
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
  const auto& partitions = multi_bd.reader_bind.hive_partitioning_indexes;
  const auto partition_of = [&](duckdb::idx_t file_col_idx) {
    return absl::c_find_if(partitions,
                           [&](const duckdb::HivePartitioningIndex& candidate) {
                             return candidate.index == file_col_idx;
                           });
  };
  _column_indexes.reserve(projected_columns.size());
  duckdb::idx_t column = 0;
  InitProjection(
    context, projected_columns, projected_types, bind_column_ids,
    SourceColumns{multi_bd.names, &duckdb::Identifier::GetIdentifierName},
    [&](duckdb::idx_t file_col_idx) {
      SDB_ASSERT(file_col_idx < multi_bd.types.size());
      if (const auto partition = partition_of(file_col_idx);
          partition != partitions.end()) {
        _partition_columns.push_back(
          {.column = column, .key = partition->value});
      } else {
        _lookup_columns.push_back(column);
        _column_indexes.emplace_back(file_col_idx);
      }
      ++column;
      return multi_bd.types[file_col_idx];
    });
  if (_column_indexes.empty() && !_partition_columns.empty()) {
    for (duckdb::idx_t file_col_idx = 0; file_col_idx < multi_bd.types.size();
         ++file_col_idx) {
      if (partition_of(file_col_idx) == partitions.end()) {
        _lookup_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        _column_indexes.emplace_back(file_col_idx);
        break;
      }
    }
  }
  BuildPushedFilters(pushed_filters);
}

void ViewFileIndexSourceBase::BuildPushedFilters(
  const duckdb::TableFilterSet* input_filters) {
  if (!input_filters || !input_filters->HasFilters()) {
    return;
  }
  SDB_ASSERT(_lookup_columns.size() == _column_indexes.size());
  const auto filter_of = [&](duckdb::idx_t column) {
    return input_filters->TryGetFilterByColumnIndex(
      duckdb::ProjectionIndex(_real_proj_slots[column]));
  };
  auto set = duckdb::make_uniq<duckdb::TableFilterSet>();
  for (duckdb::idx_t k = 0; k < _lookup_columns.size(); ++k) {
    if (_lookup_columns[k] == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    auto filter = filter_of(_lookup_columns[k]);
    if (!filter) {
      continue;
    }
    const auto& expr_filter = duckdb::ExpressionFilter::GetExpressionFilter(
      *filter, "ViewFileIndexSourceBase::BuildPushedFilters");
    set->PushFilter(duckdb::ProjectionIndex(k), expr_filter.Copy());
  }
  for (auto& partition : _partition_columns) {
    if (auto filter = filter_of(partition.column)) {
      partition.filter =
        duckdb::ExpressionFilter::GetExpressionFilter(
          *filter, "ViewFileIndexSourceBase::BuildPushedFilters")
          .Copy();
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

bool ViewFileIndexSourceBase::BindPartitionValues(
  duckdb::ClientContext& context, const std::string& path,
  std::vector<duckdb::Value>& values) const {
  if (_partition_columns.empty()) {
    return true;
  }
  const auto& multi_bd = _bind_data->Cast<duckdb::MultiFileBindData>();
  const auto partitions = duckdb::HivePartitioning::Parse(path);
  values.clear();
  values.reserve(_partition_columns.size());
  bool match = true;
  for (const auto& partition : _partition_columns) {
    const auto& type = _scratch_types[partition.column];
    const auto entry = partitions.find(partition.key);
    auto value =
      entry == partitions.end()
        ? duckdb::Value(type)
        : multi_bd.file_options
            .GetHivePartitionValue(entry->second, partition.key, context)
            .DefaultCastAs(type);
    if (partition.filter &&
        !partition.filter->EvaluateWithConstant(context, value)) {
      match = false;
    }
    values.push_back(std::move(value));
  }
  return match;
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

void ViewFileIndexSourceBase::FillPartitionColumns(
  std::span<const duckdb::Value> values, duckdb::idx_t count,
  duckdb::idx_t offset) {
  for (size_t p = 0; p < _partition_columns.size(); ++p) {
    duckdb::Vector constant(values[p], duckdb::count_t(count));
    duckdb::VectorOperations::Copy(
      constant, _tf_target.data[_partition_columns[p].column], count, 0,
      offset);
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
  if (!_partition_columns.empty()) {
    _lookup_target.Initialize(context, LookupTypes());
    _partitions_match =
      BindPartitionValues(context,
                          _bind_data->Cast<duckdb::MultiFileBindData>()
                            .file_list->GetFirstFile()
                            .path,
                          _partition_values);
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
  // Dense: the lookup TF applies the pushed filters natively and appends
  // survivors from size 0, then reports the survivor count via the chunk's
  // cardinality and the sorted-pk index of each via pk_survivors.
  _tf_target.SetCardinality(0);
  _survivor_idx.resize(count);
  if (!_partitions_match) {
    return 0;
  }
  auto& target = _partition_columns.empty() ? _tf_target : _lookup_target;
  if (!_partition_columns.empty()) {
    _lookup_target.Reset();
    _lookup_target.SetCardinality(0);
  }

  duckdb::TableFunctionInput in(_bind_data.get(), /*local_state=*/nullptr,
                                _lookup_gstate.get());
  in.pk_lookups = keys;
  in.pk_survivors = _survivor_idx;
  _lookup_func.function(context, in, target);
  const auto rows = target.size();
  if (!_partition_columns.empty()) {
    CopyLookupColumns(_lookup_target, rows, 0);
    FillPartitionColumns(_partition_values, rows, 0);
    _tf_target.SetCardinality(rows);
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
      cached.partitions_match =
        BindPartitionValues(context, file_path, cached.partition_values);
    }
    if (!cached.partitions_match) {
      i = j;
      continue;
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
    CopyLookupColumns(_file_target, file_rows, total);
    FillPartitionColumns(cached.partition_values, file_rows, total);
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
  GatherNonLookupColumns(output, total);
  return total;
}

}  // namespace sdb::connector
