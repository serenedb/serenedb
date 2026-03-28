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

#include "pg/system_table.h"

namespace sdb::pg {

// https://www.postgresql.org/docs/18/infoschema-sql-parts.html
// NOLINTBEGIN
struct SqlParts {
  static constexpr uint64_t kId = 167;
  static constexpr std::string_view kName = "sql_parts";

  CharacterData feature_id;
  CharacterData feature_name;
  YesOrNo is_supported;
  CharacterData is_verified_by;
  CharacterData comments;
};
// NOLINTEND

template<>
std::vector<velox::VectorPtr> SystemTableSnapshot<SqlParts>::GetTableData(
  velox::memory::MemoryPool& pool);

}  // namespace sdb::pg
