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

#include "pg/system_table.h"

namespace sdb::pg {

// Single underlying table for all pg_stat_progress_* views.
// Matches PostgreSQL's pg_stat_get_progress_info() design:
// one generic table with numbered param slots, views do the mapping.
// NOLINTBEGIN
struct SdbStatProgress {
  static constexpr uint64_t kId = 999997;
  static constexpr std::string_view kName = "sdb_stat_progress";

  int64_t pid;
  Oid datid;
  std::string datname;
  Oid relid;
  std::string command;
  int64_t param1;
  int64_t param2;
  int64_t param3;
  int64_t param4;
  int64_t param5;
  int64_t param6;
  int64_t param7;
  int64_t param8;
  int64_t param9;
  int64_t param10;
  int64_t param11;
  int64_t param12;
  int64_t param13;
  int64_t param14;
  int64_t param15;
  int64_t param16;
  int64_t param17;
  int64_t param18;
  int64_t param19;
  int64_t param20;
};
// NOLINTEND

template<>
std::vector<velox::VectorPtr>
SystemTableSnapshot<SdbStatProgress>::GetTableData(
  velox::memory::MemoryPool& pool);

}  // namespace sdb::pg
