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

#include <duckdb/common/types.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <string>

namespace sdb::connector::key_encoding {

// Order-preserving binary encoding of column values: memcmp order of the
// encoded bytes equals the value order. The single source of truth for the
// scalar key format (encoded row keys and encoded-key index expressions
// share it).
//
// Scalars: big-endian with sign-bit flip for ints/dates/timestamps,
// Ftoi32/Dtoi64 with zero/NaN normalization for floats (NaN greatest),
// `\0`-escaped + `\0\0`-terminated for VARCHAR/BLOB.
void AppendScalarValue(std::string& key, const duckdb::UnifiedVectorFormat& fmt,
                       duckdb::idx_t row_idx, const duckdb::LogicalType& type);

// Recursive variant covering nested types. Top-level NULL is the caller's
// concern; nested element/field NULLs are encoded with per-element markers
// (NOT NULL = \x01 + payload, NULL = \x02, nulls greatest -- matching
// Postgres array comparison). Lists terminate with \x00 so a shorter list
// orders before its extensions; structs and fixed-size arrays have a fixed
// element count and need no terminator.
void AppendValue(std::string& key,
                 const duckdb::RecursiveUnifiedVectorFormat& vec,
                 duckdb::idx_t row_idx);

}  // namespace sdb::connector::key_encoding
