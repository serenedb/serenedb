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
#include <duckdb/common/types/vector.hpp>

#include "rocksdb/iterator.h"

namespace sdb::connector {

// Read up to max_rows from a RocksDB column iterator into a DuckDB Vector.
// Type dispatch happens once per call (per column), not per row.
// Returns the number of rows actually read.
duckdb::idx_t ReadColumnIntoDuckDB(rocksdb::Iterator& it,
                                   duckdb::Vector& output,
                                   const duckdb::LogicalType& type,
                                   duckdb::idx_t max_rows);

// Read up to max_rows from a RocksDB column iterator, extracting the PK bytes
// from each key and storing them in `rowid_output` as BLOB values.
// Also reads column values into `col_output`. Returns number of rows read.
// Key format: [ObjectId(8)][ColumnId(8)][PK bytes...]
// Deserialize a single RocksDB value into a DuckDB Vector at position idx.
// Read-path counterpart to DuckDBColumnSerializer on the write path.
// Handles NULL (empty value), strings (leading \0 byte), scalars (memcpy).
void DeserializeValueIntoDuckDB(std::string_view value, duckdb::Vector& output,
                                const duckdb::LogicalType& type,
                                duckdb::idx_t idx);

}  // namespace sdb::connector
