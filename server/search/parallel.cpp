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

#include "search/parallel.h"

#include <algorithm>
#include <atomic>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>

#include "iresearch/utils/duckdb_engine.hpp"

namespace sdb::search {
namespace {

struct ParallelForTask final : duckdb::BaseExecutorTask {
  ParallelForTask(duckdb::TaskExecutor& executor, absl::FunctionRef<void()> run)
    : BaseExecutorTask{executor}, run{run} {}

  void ExecuteTask() final { run(); }

  absl::FunctionRef<void()> run;
};

template<typename MakeExecutor>
void Run(size_t threads, size_t count, absl::FunctionRef<void(size_t)> body,
         const MakeExecutor& make) {
  const auto tasks = std::min(count, threads);
  if (tasks < 2) {
    for (size_t i = 0; i != count; ++i) {
      body(i);
    }
    return;
  }
  std::atomic<size_t> next = 0;
  const auto drain = [&] {
    for (auto i = next.fetch_add(1, std::memory_order_relaxed); i < count;
         i = next.fetch_add(1, std::memory_order_relaxed)) {
      body(i);
    }
  };
  auto executor = make();
  for (size_t task = 0; task != tasks; ++task) {
    executor.ScheduleTask(duckdb::make_uniq<ParallelForTask>(executor, drain));
  }
  executor.WorkOnTasks();
}

}  // namespace

void ParallelFor(duckdb::ClientContext& context, size_t count,
                 absl::FunctionRef<void(size_t)> body) {
  Run(static_cast<size_t>(duckdb::TaskScheduler::QueryThreads(context)), count,
      body, [&] {
        return duckdb::TaskExecutor{
          duckdb::TaskScheduler::GetScheduler(context),
          context.GetSchedulerAccount()};
      });
}

void ParallelFor(duckdb::DatabaseInstance& db, size_t count,
                 absl::FunctionRef<void(size_t)> body) {
  auto& scheduler = duckdb::TaskScheduler::GetScheduler(db);
  Run(static_cast<size_t>(scheduler.NumberOfThreads()), count, body,
      [&] { return duckdb::TaskExecutor{scheduler}; });
}

void EngineParallelFor(size_t count, absl::FunctionRef<void(size_t)> body) {
  ParallelFor(irs::DuckDBEngine::Instance().instance(), count, body);
}

}  // namespace sdb::search
