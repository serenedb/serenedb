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

#include "connector/index_source_native.h"

#include <absl/algorithm/container.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/hive_partitioning.hpp>
#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/execution/execution_context.hpp>
#include <duckdb/parallel/thread_context.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <ranges>

namespace sdb::connector {
namespace {

duckdb::TableCatalogEntry& ResolveTableEntry(duckdb::ClientContext& context,
                                             const ViewFastPath& fast_path) {
  auto& entry =
    duckdb::Catalog::GetEntry(
      context, duckdb::CatalogType::TABLE_ENTRY,
      duckdb::QualifiedName(duckdb::Identifier{fast_path.catalog_ref->catalog},
                            duckdb::Identifier{fast_path.catalog_ref->schema},
                            duckdb::Identifier{fast_path.catalog_ref->table}))
      .Cast<duckdb::TableCatalogEntry>();
  if (!entry.IsDuckTable()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("cannot materialise rows from \"", entry.name.GetIdentifierName(),
              "\" -- the attached source table has no native storage"));
  }
  return entry;
}

}  // namespace

NativeIndexSource::NativeIndexSource(
  duckdb::ClientContext& context, ViewFastPath fast_path,
  duckdb::optional_ptr<duckdb::TableCatalogEntry> table,
  std::shared_ptr<const search::FileManifest> file_manifest,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const ColumnId> bind_column_ids,
  duckdb::TableFilterSet* pushed_filters)
  : ViewIndexSourceBase{std::move(fast_path)} {
  if (table) {
    BindTable(context, *table, projected_columns, projected_types,
              bind_column_ids, true);
  } else if (_fast_path.catalog_ref &&
             _fast_path.pk_spec == PkSpec::DuckDBRowId) {
    BindTable(context, ResolveTableEntry(context, _fast_path),
              projected_columns, projected_types, bind_column_ids, false);
  } else {
    BindFiles(context, file_manifest.get(), projected_columns, projected_types,
              bind_column_ids);
  }
  if (_no_files) {
    return;
  }
  BuildPushedFilters(context, pushed_filters);
  SDB_ASSERT(_source->function.get_lookup_function);
  _lookup = _source->function.get_lookup_function(context, *_source);
  duckdb::TableFunctionInitInput init(_source->bind_data.get(),
                                      _source->GetColumnIds(), {},
                                      _pushed_filters.get());
  if (_lookup.init_global) {
    _global = _lookup.init_global(context, init);
  }
  if (_lookup.init_local) {
    duckdb::ThreadContext thread{context};
    duckdb::ExecutionContext exec{context, thread, nullptr};
    _local = _lookup.init_local(exec, init, _global.get());
  }
  if (_by_file) {
    _keys.Initialize(context, duckdb::vector<duckdb::LogicalType>{
                                duckdb::LogicalType::UBIGINT,
                                duckdb::LogicalType::BIGINT,
                              });
  } else {
    _keys.InitializeEmpty(
      duckdb::vector<duckdb::LogicalType>{duckdb::LogicalType::ROW_TYPE});
  }
  if (HasDerivedColumns()) {
    duckdb::vector<duckdb::LogicalType> types;
    for (const auto& column : _source->GetColumnIds()) {
      types.push_back(_source->GetColumnType(column));
    }
    _lookup_target.Initialize(context, types);
  }
}

void NativeIndexSource::BindTable(
  duckdb::ClientContext& context, duckdb::TableCatalogEntry& table,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const ColumnId> bind_column_ids, bool by_id) {
  // Registers the attached database with the meta transaction, keeping it
  // alive for the query even if it is detached concurrently.
  duckdb::DuckTransaction::Get(context, table.ParentCatalog());
  _source = BindTableScan(
    context, table,
    duckdb::EntryLookupInfo{duckdb::CatalogType::TABLE_ENTRY,
                            duckdb::QualifiedName(table.name), nullptr,
                            duckdb::QueryErrorContext{}});
  const auto& columns = table.GetColumns();
  auto& column_ids = _source->GetMutableColumnIds();
  const auto add_column = [&](duckdb::LogicalIndex index) {
    const auto& column = columns.GetColumn(index);
    if (column.Category() == duckdb::TableColumnType::GENERATED_VIRTUAL) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("cannot materialise generated column \"",
                column.Name().GetIdentifierName(), "\" of \"",
                table.name.GetIdentifierName(), "\" through an index lookup"));
    }
    _lookup_columns.push_back(column_ids.size());
    column_ids.emplace_back(index.index);
    return column.Type();
  };
  if (!by_id) {
    InitProjection(
      context, projected_columns, projected_types, bind_column_ids,
      SourceColumns{
        std::views::iota(duckdb::idx_t{0}, columns.LogicalColumnCount()) |
        std::views::transform([&](duckdb::idx_t i) -> std::string_view {
          return columns.GetColumn(duckdb::LogicalIndex(i))
            .Name()
            .GetIdentifierName();
        })},
      [&](duckdb::idx_t table_col_idx) {
        SDB_ASSERT(table_col_idx < columns.LogicalColumnCount());
        return add_column(duckdb::LogicalIndex(table_col_idx));
      });
    return;
  }
  // The table is its own storage now -- there is no separate store relation to
  // resolve -- so physical positions are the entry's own column order minus
  // the generated PK.
  irs::containers::FlatHashMap<duckdb::idx_t, duckdb::idx_t> id_to_pos;
  id_to_pos.reserve(columns.LogicalColumnCount());
  duckdb::idx_t pos = 0;
  for (const auto& col : columns.Logical()) {
    if (TableColumnId(col) == kGeneratedPKId) {
      continue;
    }
    id_to_pos.emplace(TableColumnId(col), pos++);
  }
  InitProjection(
    context, projected_columns, projected_types, bind_column_ids,
    [&](std::string_view) -> duckdb::idx_t {
      SDB_ASSERT(false, "table index sources resolve columns by id");
      return 0;
    },
    [&](duckdb::idx_t col_id) {
      auto it = id_to_pos.find(col_id);
      SDB_ENSURE(it != id_to_pos.end(), "column id is not on the store table");
      SDB_ASSERT(it->second < columns.LogicalColumnCount());
      return add_column(duckdb::LogicalIndex(it->second));
    });
}

void NativeIndexSource::BindFiles(
  duckdb::ClientContext& context, const search::FileManifest* file_manifest,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const ColumnId> bind_column_ids) {
  _by_file = true;
  auto fast_path = _fast_path;
  if (IsGlobPK(fast_path.pk_spec)) {
    SDB_ASSERT(file_manifest);
    std::vector<const search::FileManifestEntry*> files;
    files.reserve(file_manifest->entries.size());
    for (const auto& [id, entry] : file_manifest->entries) {
      files.push_back(&entry);
    }
    if (files.empty()) {
      _no_files = true;
      return;
    }
    absl::c_sort(files, [](const auto* a, const auto* b) {
      return a->file_id < b->file_id;
    });
    duckdb::vector<duckdb::Value> paths;
    paths.reserve(files.size());
    for (const auto* file : files) {
      _file_positions.emplace(file->file_id, _paths.size());
      _paths.push_back(file->path);
      paths.emplace_back(file->path);
    }
    fast_path.args = {
      duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, std::move(paths))};
    fast_path.is_glob = false;
    if (fast_path.function_name == "iceberg_scan") {
      if (fast_path.catalog_ref) {
        BindFastPathSource(context, _fast_path);
      }
      fast_path.function_name = "read_parquet";
      fast_path.named_params.clear();
      fast_path.catalog_ref.reset();
    }
  }
  _source = BindFastPathSource(context, fast_path);
  const auto& multi_bd = _source->bind_data->Cast<duckdb::MultiFileBindData>();
  if (_paths.empty()) {
    _paths.push_back(multi_bd.file_list->GetFirstFile().path);
  }
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
  auto& column_ids = _source->GetMutableColumnIds();
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
        column_ids.emplace_back(file_col_idx);
      }
      ++column;
      return multi_bd.types[file_col_idx];
    });
  if (column_ids.empty() && HasDerivedColumns()) {
    for (duckdb::idx_t file_col_idx = 0; file_col_idx < multi_bd.types.size();
         ++file_col_idx) {
      if (!is_derived(file_col_idx)) {
        _lookup_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        column_ids.emplace_back(file_col_idx);
        break;
      }
    }
  }
}

void NativeIndexSource::BuildPushedFilters(
  duckdb::ClientContext& context, const duckdb::TableFilterSet* input_filters) {
  if (!input_filters || !input_filters->HasFilters()) {
    return;
  }
  const auto filter_of =
    [&](duckdb::idx_t column) -> duckdb::unique_ptr<duckdb::ExpressionFilter> {
    const auto filter = input_filters->TryGetFilterByColumnIndex(
      duckdb::ProjectionIndex(_real_proj_slots[column]));
    if (!filter) {
      return nullptr;
    }
    return duckdb::ExpressionFilter::GetExpressionFilter(
             *filter, "NativeIndexSource::BuildPushedFilters")
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

const NativeIndexSource::FileConstants& NativeIndexSource::ConstantsOf(
  duckdb::ClientContext& context, duckdb::idx_t file) {
  const auto [it, inserted] = _constants.try_emplace(file);
  auto& constants = it->second;
  if (!inserted || _file_constants.empty()) {
    return constants;
  }
  const auto& multi_bd = _source->bind_data->Cast<duckdb::MultiFileBindData>();
  const auto& path = _paths[file];
  const auto partitions = duckdb::HivePartitioning::Parse(path);
  constants.values.reserve(_file_constants.size());
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
      constants.match = false;
    }
    constants.values.push_back(std::move(value));
  }
  return constants;
}

void NativeIndexSource::SelectFileKeys(duckdb::ClientContext& context,
                                       duckdb::Vector& pk,
                                       duckdb::idx_t count) {
  _selected.clear();
  _key_files.clear();
  _key_rows.clear();
  const uint64_t* files = nullptr;
  const int64_t* rows = nullptr;
  if (IsGlobPK(_fast_path.pk_spec)) {
    const auto& entries = duckdb::StructVector::GetEntries(pk);
    SDB_ASSERT(entries.size() == 2);
    files = duckdb::FlatVector::GetData<uint64_t>(entries[0]);
    rows = duckdb::FlatVector::GetData<int64_t>(entries[1]);
  } else {
    rows = duckdb::FlatVector::GetData<int64_t>(pk);
  }
  for (duckdb::idx_t i = 0; i < count; ++i) {
    duckdb::idx_t file = 0;
    if (files) {
      const auto it = _file_positions.find(files[i]);
      if (it == _file_positions.end()) {
        // A doc committed by an in-flight (or died) pass: its id is not part
        // of the published manifest version, so it is not readable until the
        // pass's Finalize publishes -- reads reflect complete versions only.
        continue;
      }
      file = it->second;
    }
    if (!ConstantsOf(context, file).match) {
      continue;
    }
    _selected.push_back(i);
    _key_files.push_back(file);
    _key_rows.push_back(rows[i]);
  }
  if (!_row_number || !_row_number->filter_state || _selected.empty()) {
    return;
  }
  const auto selected = _selected.size();
  duckdb::Vector numbers(duckdb::LogicalType::BIGINT, selected);
  {
    auto writer = duckdb::FlatVector::Writer<int64_t>(numbers, selected);
    for (const auto row : _key_rows) {
      writer.WriteValue(row);
    }
  }
  duckdb::SelectionVector sel;
  auto approved = selected;
  duckdb::ColumnSegment::FilterSelection(
    sel, numbers, *_row_number->filter_state, selected, approved);
  for (duckdb::idx_t k = 0; k < approved; ++k) {
    const auto position = sel.get_index(k);
    _selected[k] = _selected[position];
    _key_files[k] = _key_files[position];
    _key_rows[k] = _key_rows[position];
  }
  _selected.resize(approved);
  _key_files.resize(approved);
  _key_rows.resize(approved);
}

void NativeIndexSource::FillDerivedColumns(duckdb::ClientContext& context,
                                           duckdb::idx_t rows) {
  for (duckdb::idx_t k = 0; k < _lookup_columns.size(); ++k) {
    if (_lookup_columns[k] != duckdb::DConstants::INVALID_INDEX) {
      duckdb::VectorOperations::Copy(_lookup_target.data[k],
                                     _tf_target.data[_lookup_columns[k]], rows,
                                     0, 0);
    }
  }
  for (size_t c = 0; c < _file_constants.size(); ++c) {
    auto& target = _tf_target.data[_file_constants[c].column];
    for (duckdb::idx_t r = 0; r < rows;) {
      const auto file = _key_files[_survivor_idx[r]];
      auto end = r + 1;
      while (end < rows && _key_files[_survivor_idx[end]] == file) {
        ++end;
      }
      duckdb::Vector constant(ConstantsOf(context, file).values[c],
                              duckdb::count_t(end - r));
      duckdb::VectorOperations::Copy(constant, target, end - r, 0, r);
      r = end;
    }
  }
  if (_row_number) {
    auto numbers = duckdb::FlatVector::Writer<int64_t>(
      _tf_target.data[_row_number->column], rows);
    for (duckdb::idx_t r = 0; r < rows; ++r) {
      numbers.WriteValue(_key_rows[_survivor_idx[r]]);
    }
  }
}

duckdb::idx_t NativeIndexSource::Materialize(duckdb::ClientContext& context,
                                             duckdb::Vector& pk,
                                             duckdb::idx_t count,
                                             duckdb::DataChunk& output) {
  if (count == 0 || _no_files) {
    return 0;
  }
  SDB_ASSERT(count <= STANDARD_VECTOR_SIZE);
  AliasOutput(output);
  _survivor_idx.resize(count);
  if (_by_file) {
    SelectFileKeys(context, pk, count);
    if (_selected.empty()) {
      return 0;
    }
    const auto keys = _selected.size();
    _keys.Reset();
    {
      auto files = duckdb::FlatVector::Writer<uint64_t>(_keys.data[0], keys);
      for (const auto file : _key_files) {
        files.WriteValue(file);
      }
    }
    {
      auto rows = duckdb::FlatVector::Writer<int64_t>(_keys.data[1], keys);
      for (const auto row : _key_rows) {
        rows.WriteValue(row);
      }
    }
    _keys.SetCardinality(keys);
  } else {
    _keys.data[0].Reference(pk);
    _keys.SetCardinality(count);
  }

  auto& target = HasDerivedColumns() ? _lookup_target : _tf_target;
  if (HasDerivedColumns()) {
    _lookup_target.Reset();
  }
  duckdb::ThreadContext thread{context};
  duckdb::ExecutionContext exec{context, thread, nullptr};
  duckdb::TableFunctionInput in(_source->bind_data.get(), _local.get(),
                                _global.get());
  in.pk_survivors = std::span{_survivor_idx}.first(_keys.size());
  _lookup.in_out_function(exec, in, _keys, target);
  const auto rows = target.size();
  if (HasDerivedColumns()) {
    FillDerivedColumns(context, rows);
  }
  if (_by_file) {
    for (duckdb::idx_t r = 0; r < rows; ++r) {
      _survivor_idx[r] = _selected[_survivor_idx[r]];
    }
  }
  _tf_target.SetCardinality(rows);

  RunCastPass(output, rows);
  GatherNonLookupColumns(output, rows);
  return rows;
}

}  // namespace sdb::connector
