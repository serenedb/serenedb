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

#include <concepts>
#include <duckdb.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <ranges>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "catalog/identifiers/revision_id.h"
#include "catalog/table_options.h"
#include "connector/duckdb_rocksdb_writer.h"
#include "connector/duckdb_table_entry.h"
#include "connector/primary_key.hpp"  // for AppendSigned, ReadSigned

namespace sdb::connector::duckdb_primary_key {

// Column mapping for PK construction from DuckDB DataChunk
struct PKColumn {
  size_t input_col_idx;
  duckdb::LogicalType type;
};

// Build PK column mappings from a chunk-ordered range of catalog columns.
// Maps each PK column ID to its position in `columns` + DuckDB type.
//
// Accepts any random-access range whose elements are catalog::Column (vector,
// span, or a lazy views::transform of an index list, etc.) so the caller can
// describe a projected/reordered chunk without materialising a copy.
template<std::ranges::random_access_range R>
  requires std::same_as<std::remove_cvref_t<std::ranges::range_reference_t<R>>,
                        catalog::Column>
inline std::vector<PKColumn> BuildPKColumns(
  R&& columns, std::span<const catalog::Column::Id> pk_col_ids) {
  std::vector<PKColumn> result;
  result.reserve(pk_col_ids.size());
  const auto begin = std::ranges::begin(columns);
  const auto n = std::ranges::size(columns);
  for (auto pk_id : pk_col_ids) {
    for (size_t i = 0; i < n; ++i) {
      const catalog::Column& c = begin[i];
      if (c.id == pk_id) {
        result.push_back(PKColumn{
          .input_col_idx = i,
          .type = c.type,
        });
        break;
      }
    }
  }
  return result;
}

// Convenience: PK columns laid out in catalog order (used by paths whose
// chunk shape mirrors `table.Columns()`).
inline std::vector<PKColumn> BuildPKColumns(const catalog::Table& table) {
  return BuildPKColumns(table.Columns(), table.PKColumns());
}

inline void PreparePKFormats(
  const duckdb::DataChunk& chunk, std::span<const PKColumn> pk_columns,
  std::vector<duckdb::UnifiedVectorFormat>& pk_formats) {
  const auto num_rows = chunk.size();
  pk_formats.resize(pk_columns.size());
  for (size_t c = 0; c < pk_columns.size(); ++c) {
    chunk.data[pk_columns[c].input_col_idx].ToUnifiedFormat(num_rows,
                                                            pk_formats[c]);
  }
}

inline void Create(std::span<const duckdb::UnifiedVectorFormat> pk_formats,
                   std::span<const PKColumn> pk_columns, duckdb::idx_t row_idx,
                   std::string& key) {
  SDB_ASSERT(pk_formats.size() == pk_columns.size());
  for (size_t c = 0; c < pk_columns.size(); ++c) {
    AppendPKValue(key, pk_formats[c], row_idx, pk_columns[c].type);
  }
}

// Generate a monotonic PK (for tables without explicit PK).
inline void CreateGenerated(std::string& key) {
  auto generated_pk = std::bit_cast<int64_t>(RevisionId::create().id());
  primary_key::AppendSigned(key, generated_pk);
}

// Build PK keys for all rows in a DataChunk.
// For explicit PKs: encodes from input columns.
// For generated PKs (pk_columns empty): generates monotonic IDs.
inline void CreateBatch(const duckdb::DataChunk& chunk,
                        std::span<const PKColumn> pk_columns,
                        std::vector<std::string>& keys) {
  const auto num_rows = chunk.size();
  keys.resize(num_rows);

  if (pk_columns.empty()) {
    // Generated PKs -- monotonic
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      keys[row].clear();
      CreateGenerated(keys[row]);
    }
  } else {
    // Explicit PKs from input columns
    std::vector<duckdb::UnifiedVectorFormat> pk_formats;
    PreparePKFormats(chunk, pk_columns, pk_formats);

    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      keys[row].clear();
      Create(pk_formats, pk_columns, row, keys[row]);
    }
  }
}

// Prepare buffer for column key and call 'row_key_handle' on row_key.
// Layout during construction: [ColumnId(reserved)][ObjectId][PK bytes]
// Callback receives row_key = [ObjectId][PK bytes] (for locking).
// Final layout: [ObjectId][ColumnId(reserved)][PK bytes].
// Use key_utils::SetupColumnForKey() to fill in ColumnId per column -- no copy.
//
// `pk_formats` must be pre-built via PreparePKFormats once per chunk; it
// is unused (and may be empty) when pk_columns is empty.
template<typename Func>
void MakeColumnKey(std::span<const duckdb::UnifiedVectorFormat> pk_formats,
                   std::span<const PKColumn> pk_columns, duckdb::idx_t row_idx,
                   std::string_view object_id, Func&& row_key_handle,
                   std::string& key_buffer) {
  SDB_ASSERT(object_id.size() == sizeof(ObjectId));
  basics::StrResize(key_buffer, sizeof(catalog::Column::Id) + sizeof(ObjectId));
  std::memcpy(key_buffer.data() + sizeof(catalog::Column::Id), object_id.data(),
              sizeof(ObjectId));

  if (pk_columns.empty()) {
    CreateGenerated(key_buffer);
  } else {
    Create(pk_formats, pk_columns, row_idx, key_buffer);
  }

  row_key_handle(std::string_view{
    key_buffer.begin() + sizeof(catalog::Column::Id), key_buffer.end()});
  std::memcpy(key_buffer.data(), object_id.data(), sizeof(ObjectId));
}

}  // namespace sdb::connector::duckdb_primary_key
