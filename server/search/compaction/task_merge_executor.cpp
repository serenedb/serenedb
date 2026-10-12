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

#include "search/compaction/task_merge_executor.h"

#include <algorithm>
#include <chrono>
#include <duckdb/parallel/task.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <memory>
#include <mutex>

#include "search/compaction/merge_unit_run.h"

namespace sdb::search {
namespace {

class UnitTask final : public duckdb::Task {
 public:
  explicit UnitTask(std::shared_ptr<MergeUnitRun> run) noexcept
    : _run{std::move(run)} {}

  duckdb::TaskExecutionResult Execute(duckdb::TaskExecutionMode) final {
    _run->Drain();
    std::lock_guard lock{_run->mutex};
    --_run->active;
    _run->wake.notify_all();
    return duckdb::TaskExecutionResult::TASK_FINISHED;
  }

 private:
  std::shared_ptr<MergeUnitRun> _run;
};

}  // namespace

bool TaskMergeExecutor::Run(irs::MergePlan& plan) {
  const auto run = std::make_shared<MergeUnitRun>(plan);
  const auto producer = _scheduler.CreateProducer();
  const size_t helpers = _threads > 1 ? _threads - 1 : 0;
  const auto spawn = [&] {
    size_t count = 0;
    {
      std::lock_guard lock{run->mutex};
      count = std::min(helpers - run->active, run->schedule.Ready());
      run->active += count;
    }
    for (size_t i = 0; i != count; ++i) {
      try {
        _scheduler.ScheduleTask(*producer,
                                duckdb::make_shared_ptr<UnitTask>(run));
      } catch (...) {
        std::lock_guard lock{run->mutex};
        run->active -= count - i;
        return;
      }
    }
  };
  while (const auto unit = run->Next()) {
    spawn();
    run->Run(*unit);
  }
  while (true) {
    {
      std::lock_guard lock{run->mutex};
      if (run->active == 0) {
        break;
      }
    }
    duckdb::shared_ptr<duckdb::Task> task;
    if (_scheduler.GetTaskFromProducer(*producer, task)) {
      task->Execute(duckdb::TaskExecutionMode::PROCESS_ALL);
      continue;
    }
    std::unique_lock lock{run->mutex};
    run->wake.wait_for(lock, std::chrono::milliseconds{1},
                       [&] { return run->active == 0; });
  }
  return run->Finish();
}

}  // namespace sdb::search
