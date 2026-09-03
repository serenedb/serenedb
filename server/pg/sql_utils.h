////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <cstdint>
#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/enums/catalog_type.hpp>
#include <string_view>
#include <vector>

#include "basics/assert.h"
#include "catalog1/permissions.h"

namespace duckdb {

class TableCatalogEntry;
class UniqueConstraint;

}  // namespace duckdb
namespace sdb::pg {

// Pair of (schema, name) parsed out of a qualified PG object name.
struct ObjectName {
  std::string_view schema;
  std::string_view relation;
};

// "[schema.]name" -> ObjectName. Unqualified names take `default_schema`.
ObjectName ParseObjectName(std::string_view name,
                           std::string_view default_schema);

// The noun an error message uses for a kind of catalog entry. "object" for the
// kinds no statement names.
std::string_view ToPgObjectTypeName(duckdb::CatalogType t) noexcept;

constexpr duckdb::CatalogType FromPgObjectTypeName(
  std::string_view word) noexcept {
  using enum duckdb::CatalogType;
  if (word == "TABLE") {
    return TABLE_ENTRY;
  }
  if (word == "VIEW") {
    return VIEW_ENTRY;
  }
  if (word == "SEQUENCE") {
    return SEQUENCE_ENTRY;
  }
  if (word == "FUNCTION") {
    return MACRO_ENTRY;
  }
  if (word == "DATABASE") {
    return DATABASE_ENTRY;
  }
  if (word == "SCHEMA") {
    return SCHEMA_ENTRY;
  }
  if (word == "TYPE") {
    return TYPE_ENTRY;
  }
  if (word == "FOREIGN SERVER") {
    return FOREIGN_SERVER_ENTRY;
  }
  return INVALID;
}

static constexpr size_t kSqlStateSize = 5;

// Unpack MAKE_SQLSTATE code.
template<typename T>
void UnpackSqlState(T& buf, int sql_state) {
  if constexpr (requires(T c) { std::size(buf); }) {
    SDB_ASSERT(std::size(buf) >= kSqlStateSize);
  }

  for (size_t i = 0; i < 5; i++) {
    buf[i] = (sql_state & 0x3F) + '0';
    sql_state >>= 6;
  }
}

int16_t TableEntryAttnum(const duckdb::TableCatalogEntry& table,
                         duckdb::idx_t column_id);

std::vector<int16_t> KeyConstraintAttnums(
  const duckdb::TableCatalogEntry& table,
  const duckdb::UniqueConstraint& constraint);

}  // namespace sdb::pg
