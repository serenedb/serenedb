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

#include "connector/file_materializer.h"

#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/common/types/data_chunk.hpp>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "connector/duckdb_external_scan.h"
#include "connector/primary_key.hpp"

namespace sdb::connector {

FileMaterializer::FileMaterializer(
  duckdb::ClientContext& context, std::shared_ptr<catalog::Table> table,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids)
  : _context(context),
    _thread_ctx(context),
    _exec_ctx(context, _thread_ctx, nullptr),
    _table(std::move(table)) {
  // Bind the external reader once. Parses file metadata (expensive for
  // parquet / csv sniffing); the function + bind_data are shared across all
  // Materialize calls.
  _func = MakeExternalScanFunction(_context, _table,
                                   /*table_entry=*/nullptr, _bind_data);

  // Resolve projection slots -> reader physical column indices. External
  // tables declare columns in the same order as the file, so the physical
  // index is the position in _table->Columns(). Build a catalog-id ->
  // physical-index lookup up-front so resolution is O(projected) rather
  // than O(projected * catalog_cols).
  const auto& catalog_cols = _table->Columns();
  containers::FlatHashMap<catalog::Column::Id, duckdb::idx_t> id_to_phys;
  id_to_phys.reserve(catalog_cols.size());
  for (duckdb::idx_t i = 0; i < catalog_cols.size(); ++i) {
    id_to_phys.emplace(catalog_cols[i].id, i);
  }

  // Build _column_indexes (what the reader emits) and collect scratch-chunk
  // types for real projections. _projected_columns is stored verbatim for
  // Materialize() to walk.
  const auto n = projected_columns.size();
  _projected_columns.assign(projected_columns.begin(), projected_columns.end());
  _column_indexes.reserve(n + 1);
  duckdb::vector<duckdb::LogicalType> chunk_types;
  chunk_types.reserve(n + 1);
  for (duckdb::idx_t proj = 0; proj < n; ++proj) {
    const auto bind_col = _projected_columns[proj];
    if (bind_col == duckdb::DConstants::INVALID_INDEX) {
      continue;  // virtual output slot -- caller fills it
    }
    auto it = id_to_phys.find(bind_column_ids[bind_col]);
    SDB_ASSERT(it != id_to_phys.end(),
               "catalog column id not found in external table");
    _column_indexes.emplace_back(it->second);
    chunk_types.push_back(projected_types[proj]);
  }
  _row_number_idx = _column_indexes.size();
  _column_indexes.emplace_back(
    duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER);
  chunk_types.push_back(duckdb::LogicalType::BIGINT);

  _chunk.Initialize(_context, chunk_types);
}

void FileMaterializer::Materialize(std::span<const std::string_view> pk_bytes,
                                   duckdb::DataChunk& output) {
  const auto num_rows = pk_bytes.size();
  if (num_rows == 0) {
    return;
  }

  for (duckdb::idx_t proj = 0; proj < _projected_columns.size(); ++proj) {
    if (_projected_columns[proj] == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    duckdb::FlatVector::ValidityMutable(output.data[proj])
      .SetAllInvalid(num_rows);
  }

  _pk_lookups.resize(num_rows);
  for (duckdb::idx_t i = 0; i < num_rows; ++i) {
    _pk_lookups[i] = primary_key::ReadSigned<int64_t>(pk_bytes[i]);
  }
  std::ranges::sort(_pk_lookups);

  duckdb::TableFunctionInitInput init_input(_bind_data.get(), _column_indexes,
                                            {}, nullptr);
  init_input.pk_lookups = _pk_lookups;
  auto gstate = _func.init_global(_context, init_input);
  duckdb::unique_ptr<duckdb::LocalTableFunctionState> lstate;
  if (_func.init_local) {
    lstate = _func.init_local(_exec_ctx, init_input, gstate.get());
  }

  duckdb::TableFunctionInput in(_bind_data.get(), lstate.get(), gstate.get());
  duckdb::idx_t total = 0;
  while (total < num_rows) {
    _chunk.Reset();
    _func.function(_context, in, _chunk);
    const auto scanned = _chunk.size();
    if (scanned == 0) {
      break;
    }
    duckdb::idx_t c = 0;
    for (duckdb::idx_t proj = 0; proj < _projected_columns.size(); ++proj) {
      if (_projected_columns[proj] == duckdb::DConstants::INVALID_INDEX) {
        continue;
      }
      duckdb::VectorOperations::Copy(_chunk.data[c], output.data[proj], scanned,
                                     0, total);
      ++c;
    }
    total += scanned;
  }
}

}  // namespace sdb::connector
