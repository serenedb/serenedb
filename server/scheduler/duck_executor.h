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

#include <atomic>
#include <cstdint>
#include <duckdb/parallel/task_scheduler.hpp>
#include <yaclib/exe/executor.hpp>
#include <yaclib/exe/job.hpp>

namespace sdb {

class DuckExecutor final : public yaclib::IExecutor {
 public:
  DuckExecutor(duckdb::TaskScheduler& scheduler,
               duckdb::SchedulerClass scheduler_class);
  ~DuckExecutor() override;

  Type Tag() const noexcept final { return Type::Custom; }

  bool Alive() const noexcept final { return true; }

  void Submit(yaclib::Job& job) noexcept final;

  void Drain() noexcept;

 private:
  class JobTask;

  static constexpr uint32_t kDraining = 1;
  static constexpr uint32_t kJob = 2;

  void Finish() noexcept;

  duckdb::TaskScheduler& _scheduler;
  duckdb::unique_ptr<duckdb::ProducerToken> _producer;
  std::atomic<uint32_t> _state{0};
};

}  // namespace sdb
