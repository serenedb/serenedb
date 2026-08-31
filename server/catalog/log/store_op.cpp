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

#include "catalog/log/store_op.h"

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <utility>

#include "basics/serialization.h"

namespace sdb::catalog::store_op {

bool IsDestructive(const Targeted& op) noexcept {
  if (op.info->info_type == duckdb::ParseInfoType::DROP_INFO) {
    return true;
  }
  if (op.info->info_type != duckdb::ParseInfoType::ALTER_INFO) {
    return false;
  }
  const auto& alter = op.info->Cast<duckdb::AlterInfo>();
  if (alter.type != duckdb::AlterType::ALTER_TABLE) {
    return false;
  }
  switch (alter.Cast<duckdb::AlterTableInfo>().alter_table_type) {
    using enum duckdb::AlterTableType;
    case REMOVE_COLUMN:
    case DROP_NOT_NULL:
    case DROP_CONSTRAINT:
      return true;
    default:
      return false;
  }
}

}  // namespace sdb::catalog::store_op
