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
#include <string>

#include "rocksdb/slice.h"

namespace sdb::connector {

// Serialize a single value from a DuckDB Vector at row `idx` into `buffer`.
// Returns a Slice pointing into `buffer`. If the value is NULL, returns
// an empty Slice (zero length) -- matching RocksDB NULL convention.
// Type dispatch happens at the caller (once per column).
rocksdb::Slice SerializeScalarValue(const duckdb::Vector& vec,
                                    duckdb::idx_t idx,
                                    const duckdb::LogicalType& type,
                                    std::string& buffer);

// Append a PK column value from a DuckDB Vector to a key buffer.
// Uses big-endian + sign-bit-flip encoding for correct byte-order sorting
// (same as primary_key::AppendKeyValue for Velox vectors).
void AppendPKValueFromDuckDB(std::string& key, const duckdb::Vector& vec,
                             duckdb::idx_t idx,
                             const duckdb::LogicalType& type);

}  // namespace sdb::connector
