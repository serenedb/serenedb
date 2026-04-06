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

#include <duckdb.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <span>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "catalog/identifiers/revision_id.h"
#include "catalog/table_options.h"
#include "connector/duckdb_rocksdb_writer.h"
#include "connector/duckdb_table_entry.h"  // for VeloxTypeToDuckDB
#include "connector/primary_key.hpp"       // for AppendSigned, ReadSigned

namespace sdb::connector::duckdb_primary_key {

// Column mapping for PK construction from DuckDB DataChunk
struct PKColumn {
  size_t input_col_idx;
  duckdb::LogicalType type;
};

// Build PK column mappings from table metadata.
// Maps each PK column ID to its position in the table column list + DuckDB
// type.
inline std::vector<PKColumn> BuildPKColumns(const catalog::Table& table) {
  const auto& columns = table.Columns();
  const auto& pk_col_ids = table.PKColumns();
  std::vector<PKColumn> result;
  result.reserve(pk_col_ids.size());

  for (auto pk_id : pk_col_ids) {
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].id == pk_id) {
        result.push_back(PKColumn{
          .input_col_idx = i,
          .type = VeloxTypeToDuckDB(columns[i].type),
        });
        break;
      }
    }
  }
  return result;
}

// Append PK bytes from a DuckDB DataChunk row.
// Uses sortable encoding (big-endian + sign-bit-flip).
inline void Create(const duckdb::DataChunk& chunk,
                   std::span<const PKColumn> pk_columns, duckdb::idx_t row_idx,
                   std::string& key) {
  for (const auto& pk : pk_columns) {
    AppendPKValueFromDuckDB(key, chunk.data[pk.input_col_idx], row_idx,
                            pk.type);
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
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      keys[row].clear();
      Create(chunk, pk_columns, row, keys[row]);
    }
  }
}

// Build pre-formatted row keys with reserved ColumnId slot.
// Layout: [ColumnId(8B reserved)][ObjectId(8B)][PK bytes]
// Use key_utils::SetupColumnForKey() to fill in ColumnId per column — no copy.
// This mirrors the Velox MakeColumnKey pattern.
inline void CreateBatchWithColumnSlot(
  const duckdb::DataChunk& chunk, std::span<const PKColumn> pk_columns,
  std::string_view object_id, std::vector<std::string>& keys) {
  SDB_ASSERT(object_id.size() == sizeof(ObjectId));
  const auto num_rows = chunk.size();
  keys.resize(num_rows);

  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    auto& key = keys[row];
    // Reserve [ColumnId][ObjectId] prefix, then append PK
    basics::StrResize(key, sizeof(catalog::Column::Id) + sizeof(ObjectId));
    std::memcpy(key.data() + sizeof(catalog::Column::Id), object_id.data(),
                sizeof(ObjectId));

    if (pk_columns.empty()) {
      CreateGenerated(key);
    } else {
      Create(chunk, pk_columns, row, key);
    }

    // Copy ObjectId to offset 0 — SetupColumnForKey will overwrite offset 8.
    // Final layout: [ObjectId][ColumnId][PK] — matches RocksDB key format.
    std::memcpy(key.data(), object_id.data(), sizeof(ObjectId));
  }
}

}  // namespace sdb::connector::duckdb_primary_key
