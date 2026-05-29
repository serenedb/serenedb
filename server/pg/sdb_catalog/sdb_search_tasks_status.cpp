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

#include "sdb_search_tasks_status.h"

#include <magic_enum/magic_enum.hpp>

#include "app/app_server.h"
#include "app/logger_feature.h"
#include "basics/logger/logger.h"
#include "storage_engine/search_engine.h"
namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNonNulls({
  GetIndex(&SdbSearchTasksStatus::task_type),
  GetIndex(&SdbSearchTasksStatus::active_tasks),
  GetIndex(&SdbSearchTasksStatus::pending_tasks),
  GetIndex(&SdbSearchTasksStatus::threads),
});

}

template<>
catalog::MaterializedData
SystemTableSnapshot<SdbSearchTasksStatus>::GetTableData() {
  if (!SerenedServer::Instance()
         .getFeature<search::SearchEngine>()
         .isEnabled()) {
    return {CreateColumns<SdbSearchTasksStatus>(0), 0};
  }
  constexpr auto kThreadGroups =
    magic_enum::enum_entries<search::ThreadGroup>();
  std::vector<SdbSearchTasksStatus> values;
  values.reserve(kThreadGroups.size());
  for (const auto& [thread_group, thread_group_name] : kThreadGroups) {
    auto [active, pending, threads] =
      SerenedServer::Instance().getFeature<search::SearchEngine>().stats(
        thread_group);

    values.push_back({
      .task_type = thread_group_name,
      .active_tasks = active,
      .pending_tasks = pending,
      .threads = threads,
    });
  }

  auto result = CreateColumns<SdbSearchTasksStatus>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row);
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
