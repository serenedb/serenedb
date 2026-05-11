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
#include "catalog/table_options.h"
#include "connector/duckdb_rocksdb_writer.h"
#include "connector/duckdb_table_entry.h"
#include "connector/primary_key.hpp"  // for AppendSigned, ReadSigned

namespace sdb::connector::duckdb_primary_key {

struct PKColumn {
  size_t input_col_idx;
  duckdb::LogicalType type;
};

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
          .type = columns[i].type,
        });
        break;
      }
    }
  }
  return result;
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

// Sortable signed encoding -- caller must have reserved the id.
inline void AppendGenerated(std::string& key, uint64_t generated_id) {
  primary_key::AppendSigned(key, std::bit_cast<int64_t>(generated_id));
}

inline void CreateBatch(const duckdb::DataChunk& chunk,
                        std::span<const PKColumn> pk_columns,
                        uint64_t generated_pk_base,
                        std::vector<std::string>& keys) {
  const auto num_rows = chunk.size();
  keys.resize(num_rows);

  if (pk_columns.empty()) {
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      keys[row].clear();
      AppendGenerated(keys[row], generated_pk_base + row);
    }
  } else {
    std::vector<duckdb::UnifiedVectorFormat> pk_formats;
    PreparePKFormats(chunk, pk_columns, pk_formats);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      keys[row].clear();
      Create(pk_formats, pk_columns, row, keys[row]);
    }
  }
}

// Build buffer as [ColumnId(reserved)][ObjectId][PK]; pass [ObjectId][PK] to
// the callback (used for row-level locking); finalise to
// [ObjectId][ColumnId(reserved)][PK]. Per-column ColumnId is filled in later
// by key_utils::SetupColumnForKey() without a copy.
//
// `generated_id` is consulted only when `pk_columns` is empty.
template<typename Func>
void MakeColumnKey(std::span<const duckdb::UnifiedVectorFormat> pk_formats,
                   std::span<const PKColumn> pk_columns, duckdb::idx_t row_idx,
                   uint64_t generated_id, std::string_view object_id,
                   Func&& row_key_handle, std::string& key_buffer) {
  SDB_ASSERT(object_id.size() == sizeof(ObjectId));
  basics::StrResize(key_buffer, sizeof(catalog::Column::Id) + sizeof(ObjectId));
  std::memcpy(key_buffer.data() + sizeof(catalog::Column::Id), object_id.data(),
              sizeof(ObjectId));

  if (pk_columns.empty()) {
    AppendGenerated(key_buffer, generated_id);
  } else {
    Create(pk_formats, pk_columns, row_idx, key_buffer);
  }

  row_key_handle(std::string_view{
    key_buffer.begin() + sizeof(catalog::Column::Id), key_buffer.end()});
  std::memcpy(key_buffer.data(), object_id.data(), sizeof(ObjectId));
}

// Overload for paths that operate on existing rows (UPDATE/DELETE/CREATE INDEX)
template<typename Func>
void MakeColumnKey(std::span<const duckdb::UnifiedVectorFormat> pk_formats,
                   std::span<const PKColumn> pk_columns, duckdb::idx_t row_idx,
                   std::string_view object_id, Func&& row_key_handle,
                   std::string& key_buffer) {
  SDB_ASSERT(!pk_columns.empty());
  MakeColumnKey(pk_formats, pk_columns, row_idx, 0, object_id,
                std::forward<Func>(row_key_handle), key_buffer);
}

}  // namespace sdb::connector::duckdb_primary_key
