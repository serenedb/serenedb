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

// NOLINTBEGIN
struct SdbSearchTasksStatus {
  static constexpr uint64_t kId = 999996;  // TODO(codeworse): assign proper OID
  static constexpr std::string_view kName = "sdb_search_tasks_status";

  Text task_type;
  uint64_t active_tasks;
  uint64_t pending_tasks;
  uint64_t threads;
};
// NOLINTEND

template<>
std::vector<velox::VectorPtr>
SystemTableSnapshot<SdbSearchTasksStatus>::GetTableData(
  velox::memory::MemoryPool& pool);

}  // namespace sdb::pg
