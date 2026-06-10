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

#include <duckdb/common/helper.hpp>
#include <duckdb/parallel/task.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <yaclib/exe/executor.hpp>
#include <yaclib/exe/job.hpp>

#include "network/io_executor.h"

namespace sdb::network::pg {

// Runs yaclib coroutine continuations on DuckDB's scheduler worker threads, so
// query work stays on threads DuckDB manages (per-thread allocator caches).
//
// The task object is a plain member: every yaclib hop submits the SAME Job (the
// coroutine's promise), and a session has at most one hop in flight at a time,
// so we just retarget the reused JobTask and re-enqueue it. We hand the queue a
// non-owning shared_ptr built with the aliasing constructor over an empty
// control block, so the copy into the queue and the worker's drop do no atomic
// refcounting and the queue never deletes _task -- this executor owns it for
// the session's lifetime, which outlives every in-flight hop. Safe because the
// task always finishes in one Execute (never reschedules / calls
// shared_from_this).
//
// `final` so that `co_await yaclib::On(duck)` (templated On over the concrete
// type) devirtualizes the Submit call. Held by value (std::optional) on the
// session -- no per-connection heap allocation.
class DuckExecutor final : public yaclib::IExecutor {
 public:
  DuckExecutor(duckdb::TaskScheduler& scheduler, IoExecutor& io_worker)
    : _scheduler{scheduler}, _io_worker{io_worker} {}

  Type Tag() const noexcept override { return Type::Custom; }

  bool Alive() const noexcept override { return true; }

  void Submit(yaclib::Job& job) noexcept override {
    _task.job = &job;
    // One producer per io worker, shared by all its sessions. DuckDB's
    // ProducerToken is guarded by its own mutex (ProducerToken::producer_lock),
    // so enqueueing through it is thread-safe: the common io->duck hop runs on
    // this session's io thread, and the rare reschedule that fires on a
    // duck-worker thread can use the same producer -- no per-duck producer.
    _scheduler.ScheduleTask(
      _io_worker.DuckProducer(_scheduler),
      duckdb::shared_ptr<duckdb::Task>{duckdb::shared_ptr<duckdb::Task>{}, &_task});
  }

 private:
  struct JobTask final : duckdb::Task {
    duckdb::TaskExecutionResult Execute(duckdb::TaskExecutionMode) override {
      job->Call();
      return duckdb::TaskExecutionResult::TASK_FINISHED;
    }

    yaclib::Job* job = nullptr;
  };

  duckdb::TaskScheduler& _scheduler;
  IoExecutor& _io_worker;
  JobTask _task;
};

}  // namespace sdb::network::pg
