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
#include <coroutine>
#include <cstdint>
#include <duckdb/parallel/task.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <yaclib/exe/job.hpp>

#include "network/io_executor.h"

namespace sdb::network {

// Hosts one long-lived coroutine (a session's duck-side half) as a duckdb::Task
// on the worker pool. It never migrates: it suspends (Park/Yield) and every
// resume is a fresh Execute() frame the scheduler pulls off its run queue.
//
// _run coalesces wakes from any thread, wait-free (one atomic, RMWs, no CAS
// loop -- yaclib BaseCore style). 0 = parked; odd = running; even > 0 =
// scheduled. A wake adds 2 and enqueues only on the 0-> edge, so exactly one
// scheduler entry is ever live -- the same coroutine can't run on two workers.
// Execute exchanges to 1 on entry; for a park it fetch_sub(1) on exit: landing
// on 0 means nobody woke (block), even means a wake arrived (requeue).
//
// All acq_rel, no seq_cst / fence: a wake (fetch_add) and a run (exchange /
// fetch_sub) are RMWs on the SAME word, so the run always reads-from the wake
// in the modification order. That read-from is the synchronizes-with edge, and
// it carries the woken data (published release-before the fetch_add) into the
// run -- so there is no cross-variable store/load race for seq_cst to guard.
// The entry op is an exchange, not a store, precisely so this read-from exists.
//
// Standalone shared_ptr, not embedded in the session and holding no session
// reference: the session keeps one ref, each Enqueue hands the scheduler
// another via shared_from_this() (duckdb::Task is itself an
// enable_shared_from_this). Execute touches only this object's own state after
// Call() returns, so the scheduler co-owns the resumer, never the session --
// the session's lifetime is independent, owned by Run.
class CpuResumer final : public duckdb::Task {
 public:
  CpuResumer(duckdb::TaskScheduler& scheduler, IoExecutor& io) noexcept
    : _scheduler{scheduler}, _io{io} {}

  // The coroutine's two suspends. Park lets Execute decide park-vs-requeue;
  // Yield asks Execute to run it again (fairness between query slices). The
  // first suspend is a Park -- the spawner does the one bootstrap RequestRun.
  class [[nodiscard]] Awaiter {
   public:
    Awaiter(CpuResumer& runner, bool yield) noexcept
      : _runner{runner}, _yield{yield} {}
    bool await_ready() const noexcept { return false; }
    template<typename Promise>
    void await_suspend(std::coroutine_handle<Promise> handle) noexcept {
      _runner._job = &handle.promise();
      if (_yield) {
        _runner._result = duckdb::TaskExecutionResult::TASK_NOT_FINISHED;
      }
    }
    void await_resume() const noexcept {}

   private:
    CpuResumer& _runner;
    bool _yield;
  };

  Awaiter Park() noexcept { return Awaiter{*this, false}; }
  Awaiter Yield() noexcept { return Awaiter{*this, true}; }

  // Called right before the coroutine's final co_return.
  void Finish() noexcept {
    _result = duckdb::TaskExecutionResult::TASK_FINISHED;
  }

  // The one wake primitive, callable from any thread. Only the 0-> edge
  // enqueues; while running/scheduled a wake just bumps so Execute requeues.
  void RequestRun() noexcept {
    if (_run.fetch_add(2, std::memory_order_acq_rel) == 0) {
      _scheduler.ScheduleTask(_io.DuckProducer(_scheduler), shared_from_this());
    }
  }

  duckdb::TaskExecutionResult Execute(duckdb::TaskExecutionMode) override {
    _run.exchange(1, std::memory_order_acq_rel);
    _result = duckdb::TaskExecutionResult::TASK_BLOCKED;
    _job->Call();
    // Yield / Finish set _result during the Call; otherwise the coroutine
    // parked and the counter decides.
    if (_result != duckdb::TaskExecutionResult::TASK_BLOCKED) {
      return _result;
    }
    return _run.fetch_sub(1, std::memory_order_acq_rel) == 1
             ? duckdb::TaskExecutionResult::TASK_BLOCKED
             : duckdb::TaskExecutionResult::TASK_NOT_FINISHED;
  }

  void Deschedule() override {}

 private:
  duckdb::TaskScheduler& _scheduler;
  IoExecutor& _io;
  yaclib::Job* _job = nullptr;
  duckdb::TaskExecutionResult _result =
    duckdb::TaskExecutionResult::TASK_BLOCKED;
  std::atomic<uint32_t> _run{0};
};

}  // namespace sdb::network
