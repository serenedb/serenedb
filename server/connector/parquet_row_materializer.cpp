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

#include "connector/parquet_row_materializer.h"

#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/planner/filter/conjunction_filter.hpp>
#include <duckdb/planner/filter/constant_filter.hpp>
#include <duckdb/planner/table_filter_set.hpp>

#include "basics/assert.h"
#include "connector/duckdb_external_scan.h"
#include "connector/primary_key.hpp"

namespace sdb::connector {

ParquetRowMaterializer::ParquetRowMaterializer(
  duckdb::ClientContext& context, std::shared_ptr<catalog::Table> table,
  std::span<const std::string> all_pks,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids)
  : _context(context),
    _table(std::move(table)),
    _projected_columns(projected_columns.begin(), projected_columns.end()),
    _projected_types(projected_types.begin(), projected_types.end()),
    _bind_column_ids(bind_column_ids.begin(), bind_column_ids.end()) {
  // 1. Bind parquet once.
  _func = MakeExternalScanFunction(_context, _table,
                                   /*table_entry=*/nullptr, _bind_data);

  // 2. Resolve projection slots -> parquet physical column indices.
  // External tables declare columns in the same order as the parquet
  // file, so the physical index is the position in _table->Columns().
  const auto& catalog_cols = _table->Columns();
  auto find_phys_idx = [&](catalog::Column::Id cid) -> duckdb::idx_t {
    for (duckdb::idx_t i = 0; i < catalog_cols.size(); ++i) {
      if (catalog_cols[i].id == cid) {
        return i;
      }
    }
    SDB_ASSERT(false, "catalog column id not found in external table");
    return duckdb::DConstants::INVALID_INDEX;
  };

  _real_col_to_parquet_slot.assign(_projected_columns.size(),
                                   duckdb::DConstants::INVALID_INDEX);
  for (duckdb::idx_t proj = 0; proj < _projected_columns.size(); ++proj) {
    const auto bind_col = _projected_columns[proj];
    if (bind_col == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    const auto phys = find_phys_idx(_bind_column_ids[bind_col]);
    _real_col_to_parquet_slot[proj] = _column_indexes.size();
    _column_indexes.emplace_back(phys);
  }
  _frn_slot = _column_indexes.size();
  _column_indexes.emplace_back(
    duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER);

  // 3. Big OR-equality filter over all file_row_numbers we ever need.
  // TableFilterState::Initialize rejects InFilter; ConjunctionOrFilter
  // of ConstantFilter(=id) has equivalent semantics and IS supported.
  auto or_filter = duckdb::make_uniq<duckdb::ConjunctionOrFilter>();
  or_filter->child_filters.reserve(all_pks.size());
  for (const auto& pk : all_pks) {
    or_filter->child_filters.push_back(duckdb::make_uniq<duckdb::ConstantFilter>(
      duckdb::ExpressionType::COMPARE_EQUAL,
      duckdb::Value::BIGINT(primary_key::ReadSigned<int64_t>(pk))));
  }
  _filter_set.PushFilter(duckdb::ProjectionIndex(_frn_slot),
                         std::move(or_filter));

  // 4. Init one persistent parquet scan.
  duckdb::TableFunctionInitInput init_input(
    _bind_data.get(), _column_indexes, _projection_ids, &_filter_set);
  _gstate = _func.init_global(_context, init_input);
  _thread_ctx = std::make_unique<duckdb::ThreadContext>(_context);
  _exec_ctx =
    std::make_unique<duckdb::ExecutionContext>(_context, *_thread_ctx, nullptr);
  if (_func.init_local) {
    _lstate = _func.init_local(*_exec_ctx, init_input, _gstate.get());
  }

  // 5. Precompute chunk types so every new buffered chunk can be
  // Initialize()d with the same layout.
  _chunk_types.reserve(_column_indexes.size());
  for (duckdb::idx_t proj = 0; proj < _projected_columns.size(); ++proj) {
    if (_projected_columns[proj] == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    _chunk_types.push_back(_projected_types[proj]);
  }
  _chunk_types.push_back(duckdb::LogicalType::BIGINT);

  // Reserve buffer proportional to result size (rough upper bound).
  _row_id_to_slot.reserve(all_pks.size());
}

bool ParquetRowMaterializer::PullOneChunk() {
  if (_scan_done) {
    return false;
  }
  auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
  chunk->Initialize(_context, _chunk_types);
  duckdb::TableFunctionInput in(_bind_data.get(), _lstate.get(), _gstate.get());
  _func.function(_context, in, *chunk);
  const auto scanned = chunk->size();
  if (scanned == 0) {
    _scan_done = true;
    return false;
  }

  // Register the rows in `_row_id_to_slot`.
  auto& frn_vec = chunk->data[_frn_slot];
  frn_vec.Flatten(scanned);
  auto* frn_ptr = duckdb::FlatVector::GetData<int64_t>(frn_vec);
  const auto chunk_idx = _buffer.size();
  for (duckdb::idx_t r = 0; r < scanned; ++r) {
    _row_id_to_slot[frn_ptr[r]] = Slot{chunk_idx, r};
  }
  _buffer.push_back(std::move(chunk));
  return true;
}

void ParquetRowMaterializer::CopyBufferedRowToOutput(
  duckdb::idx_t chunk_idx, duckdb::idx_t row_idx, duckdb::idx_t out_row,
  duckdb::DataChunk& output) {
  auto& src_chunk = *_buffer[chunk_idx];
  for (duckdb::idx_t proj = 0; proj < _projected_columns.size(); ++proj) {
    const auto pslot = _real_col_to_parquet_slot[proj];
    if (pslot == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    duckdb::VectorOperations::Copy(src_chunk.data[pslot], output.data[proj],
                                   row_idx + 1, row_idx, out_row);
    duckdb::FlatVector::Validity(output.data[proj]).SetValid(out_row);
  }
}

void ParquetRowMaterializer::Materialize(
  std::span<const std::string_view> pk_bytes, duckdb::DataChunk& output) {
  const auto num_rows = pk_bytes.size();
  if (num_rows == 0) {
    return;
  }

  // Start every row NULL; found rows overwrite their slot.
  for (duckdb::idx_t proj = 0; proj < _projected_columns.size(); ++proj) {
    if (_projected_columns[proj] == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    auto& vec = output.data[proj];
    auto& validity = duckdb::FlatVector::Validity(vec);
    for (duckdb::idx_t i = 0; i < num_rows; ++i) {
      validity.SetInvalid(i);
    }
  }

  // Stream rows: for each batch pk, if it's already buffered from an
  // earlier pull, copy it immediately. Otherwise pull more chunks
  // until we find it or the scan ends.
  for (duckdb::idx_t out_row = 0; out_row < num_rows; ++out_row) {
    const int64_t row_id =
      primary_key::ReadSigned<int64_t>(pk_bytes[out_row]);

    auto it = _row_id_to_slot.find(row_id);
    while (it == _row_id_to_slot.end() && !_scan_done) {
      if (!PullOneChunk()) {
        break;
      }
      it = _row_id_to_slot.find(row_id);
    }
    if (it == _row_id_to_slot.end()) {
      // Row didn't materialise (filter mismatch / stale pk). Leave
      // NULL -- same as RocksDBRowMaterializer's Missing-Key behaviour.
      continue;
    }
    CopyBufferedRowToOutput(it->second.chunk, it->second.row, out_row, output);
  }
}

}  // namespace sdb::connector
