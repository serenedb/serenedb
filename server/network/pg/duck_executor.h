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

#include <yaclib/exe/executor.hpp>
#include <yaclib/exe/job.hpp>

#include <duckdb/common/helper.hpp>
#include <duckdb/parallel/task.hpp>
#include <duckdb/parallel/task_scheduler.hpp>

namespace sdb::network::pg {

class DuckExecutor : public yaclib::IExecutor {
 public:
  explicit DuckExecutor(duckdb::TaskScheduler& scheduler)
    : _scheduler{scheduler}, _producer{scheduler.CreateProducer()} {}

  Type Tag() const noexcept override { return Type::Custom; }

  bool Alive() const noexcept override { return true; }

  void Submit(yaclib::Job& job) noexcept override {
    _scheduler.ScheduleTask(*_producer, duckdb::make_shared_ptr<JobTask>(job));
  }

 private:
  struct JobTask final : duckdb::Task {
    explicit JobTask(yaclib::Job& job) noexcept : job{job} {}

    duckdb::TaskExecutionResult Execute(duckdb::TaskExecutionMode) override {
      job.Call();
      return duckdb::TaskExecutionResult::TASK_FINISHED;
    }

    yaclib::Job& job;
  };

  duckdb::TaskScheduler& _scheduler;
  duckdb::unique_ptr<duckdb::ProducerToken> _producer;
};

}  // namespace sdb::network::pg
