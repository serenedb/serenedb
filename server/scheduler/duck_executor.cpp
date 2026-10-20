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

#include "scheduler/duck_executor.h"

#include <duckdb/parallel/task.hpp>

namespace sdb {

class DuckExecutor::JobTask final : public duckdb::Task {
 public:
  JobTask(DuckExecutor& executor, yaclib::Job& job) noexcept
    : _executor{executor}, _job{job} {}

  duckdb::TaskExecutionResult Execute(duckdb::TaskExecutionMode) final {
    _job.Call();
    _executor.Finish();
    return duckdb::TaskExecutionResult::TASK_FINISHED;
  }

  std::string TaskType() const final { return "DuckExecutorJob"; }

 private:
  DuckExecutor& _executor;
  yaclib::Job& _job;
};

DuckExecutor::DuckExecutor(duckdb::TaskScheduler& scheduler,
                           duckdb::SchedulerClass scheduler_class)
  : _scheduler{scheduler},
    _producer{
      scheduler.CreateProducer(scheduler.CreateAccount(scheduler_class))} {}

DuckExecutor::~DuckExecutor() { Drain(); }

void DuckExecutor::Submit(yaclib::Job& job) noexcept {
  _state.fetch_add(kJob, std::memory_order_relaxed);
  _scheduler.ScheduleTask(*_producer,
                          duckdb::make_shared_ptr<JobTask>(*this, job),
                          duckdb::TaskSchedulerType::ASYNC);
}

void DuckExecutor::Finish() noexcept {
  if (_state.fetch_sub(kJob, std::memory_order_acq_rel) == (kJob | kDraining)) {
    _state.notify_all();
  }
}

void DuckExecutor::Drain() noexcept {
  auto state =
    _state.fetch_or(kDraining, std::memory_order_acq_rel) | kDraining;
  while (state != kDraining) {
    _state.wait(state, std::memory_order_acquire);
    state = _state.load(std::memory_order_acquire);
  }
  _state.fetch_and(~kDraining, std::memory_order_relaxed);
}

}  // namespace sdb
