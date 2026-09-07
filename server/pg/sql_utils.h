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
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/enums/catalog_type.hpp>
#include <vector>

#include "basics/assert.h"

namespace duckdb {

class TableCatalogEntry;
class UniqueConstraint;

}  // namespace duckdb
namespace sdb::pg {

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
