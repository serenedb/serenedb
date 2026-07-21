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

#include <cstring>
#include <duckdb/common/vector/struct_vector.hpp>
#include <string_view>
#include <type_traits>

#include "basics/assert.h"
#include "connector/index_source.h"
#include "connector/primary_key.hpp"
#include "connector/search_pk_lookup.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {

inline void AppendPrimaryKeysFromVector(PrimaryKeyBatch& pk,
                                        const duckdb::Vector& vec,
                                        size_t count) {
  const auto type_id = vec.GetType().id();
  if (pk.kind == PrimaryKeyBatch::Kind::I64 &&
      type_id == duckdb::LogicalTypeId::BIGINT) {
    const auto* data = duckdb::FlatVector::GetData<int64_t>(vec);
    for (size_t k = 0; k < count; ++k) {
      pk.Append(data[k]);
    }
    return;
  }
  if (pk.kind == PrimaryKeyBatch::Kind::I64I64 &&
      type_id == duckdb::LogicalTypeId::STRUCT) {
    const auto& entries = duckdb::StructVector::GetEntries(vec);
    SDB_ASSERT(entries.size() == 2);
    const auto* hi = duckdb::FlatVector::GetData<int64_t>(entries[0]);
    const auto* lo = duckdb::FlatVector::GetData<int64_t>(entries[1]);
    for (size_t k = 0; k < count; ++k) {
      pk.Append(hi[k], lo[k]);
    }
    return;
  }
  THROW_SQL_ERROR(
    ERR_MSG("stored PK column type ", vec.GetType().ToString(),
            " does not match the index source's expected key shape"));
}

}  // namespace sdb::connector
