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

#include <duckdb/common/enums/catalog_type.hpp>
#include <string_view>

#include "basics/assert.h"
#include "catalog/object.h"

namespace sdb::pg {

// Pair of (schema, name) parsed out of a qualified PG object name.
struct ObjectName {
  std::string_view schema;
  std::string_view relation;
};

// "[schema.]name" -> ObjectName. Unqualified names take `default_schema`.
ObjectName ParseObjectName(std::string_view name,
                           std::string_view default_schema);

std::string_view ToPgObjectTypeName(duckdb::CatalogType t) noexcept;

constexpr std::string_view ToPgObjectTypeName(catalog::ObjectType t) noexcept {
  switch (t) {
    using enum catalog::ObjectType;
    case Table:
      return "table";
    case PgSqlView:
      return "view";
    case SecondaryIndex:
    case InvertedIndex:
      return "index";
    case PgSqlFunction:
      return "function";
    case Schema:
      return "schema";
    case Database:
      return "database";
    case Tokenizer:
      return "text search dictionary";
    case PgSqlType:
      return "type";
    default:
      return "object";
  }
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

}  // namespace sdb::pg
