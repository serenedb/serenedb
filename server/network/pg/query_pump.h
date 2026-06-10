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
#include <duckdb/common/enums/pending_execution_result.hpp>
#include <duckdb/common/helper.hpp>
#include <duckdb/main/pending_query_result.hpp>
#include <duckdb/parallel/task.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <yaclib/exe/executor.hpp>
#include <yaclib/exe/job.hpp>

#include "network/io_executor.h"

namespace sdb::network::pg {

// Drives one DuckDB query to completion as a DuckDB scheduler task, then resumes
// the awaiting io coroutine once.
//
// The pump IS a duckdb::Task. The io coroutine (already hopped onto a DuckDB
// worker) does `co_await pump.Drive(pending)`: the first task slice runs INLINE
// on that worker, so a query that finishes immediately (the common prepared-OLTP
// case) never leaves the thread -- no enqueue, no suspend.
//
// When the query is not done in one slice the pump does NOT busy-poll. It only
// keeps running while ExecuteTask makes progress (RESULT_NOT_READY). The moment
// it has no runnable task (NO_TASKS_AVAILABLE -- the worker pool is executing the
// query) or is blocked (BLOCKED), it PARKS: it returns TASK_BLOCKED and suspends
// until the executor wakes it. The executor fires our reschedule callback when
// the query reaches a state the driver cares about -- a result chunk is ready
// (streaming collector blocks), the query finishes, it errors, or a blocked task
// unblocks. So instead of ~thousands of NO_TASKS reschedules per analytical query
// the pump sleeps through execution and wakes a handful of times. (This relies on
// the executor patch that stores on_reschedule on NO_TASKS and fires it on
// completion / result-ready / error; see Executor::ExecuteTask / CompletePipeline
// / AddToBeRescheduled / PushError.)
//
// Wakes arrive on arbitrary worker threads, so _run guards against the pump being
// run concurrently with itself: a wake that lands while we are mid-Execute is
// folded into a single re-run rather than a second concurrent Execute.
//
// `final` so the Task vtable call from ExecuteForever is the only indirection.
// Held by value on the session (one query in flight at a time), so there is no
// per-query allocation; the queue gets a non-owning aliasing shared_ptr (empty
// control block, zero refcount) and never deletes the pump.
class QueryPump final : public duckdb::Task {
 public:
  QueryPump(duckdb::TaskScheduler& scheduler, IoExecutor& io_worker,
            yaclib::IExecutor& resume) noexcept
    : _scheduler{scheduler}, _io_worker{io_worker}, _resume{resume} {}

  // Awaitable returned by Drive(). await_suspend returns false when the query
  // already finished on the inline first slice (resume in place, no hop).
  class [[nodiscard]] DriveAwaiter {
   public:
    DriveAwaiter(QueryPump& pump, duckdb::PendingQueryResult& pending) noexcept
      : _pump{pump}, _pending{pending} {}

    bool await_ready() const noexcept { return false; }

    // The yaclib coroutine promise IS a yaclib::Job, so we hand the pump the
    // promise directly (like yaclib's own On awaiter) instead of a
    // coroutine_handle: resuming is then _resume.Submit(job) -- no allocated
    // wrapper job, no handle.resume() trampoline frame. The coroutine's executor
    // is already *_resume (the dispatch hopped onto it before Drive), so we need
    // not reset promise._executor.
    template <typename Promise>
    bool await_suspend(std::coroutine_handle<Promise> handle) noexcept {
      return _pump.Start(_pending, handle.promise());
    }

    duckdb::PendingExecutionResult await_resume() const noexcept {
      return _pump._result;
    }

   private:
    QueryPump& _pump;
    duckdb::PendingQueryResult& _pending;
  };

  DriveAwaiter Drive(duckdb::PendingQueryResult& pending) noexcept {
    return DriveAwaiter{*this, pending};
  }

 private:
  // Run-concurrency states. kQueued: in the scheduler queue (or about to be).
  // kRunning: Execute is in flight. kRunningWoke: a wake landed during Execute.
  // kParked: suspended, waiting for a wake to re-enqueue us.
  enum Run : uint8_t { kParked, kQueued, kRunning, kRunningWoke };
  // Suspend-handshake states for the awaiting coroutine.
  enum Done : uint8_t { kInit, kSuspended, kCompleted };

  // Runs the first slice inline. Returns true if the caller must suspend (the
  // pump will resume it once the query terminates), false if the query already
  // terminated inline and the caller should continue without suspending.
  bool Start(duckdb::PendingQueryResult& pending, yaclib::Job& job) noexcept {
    _pending = &pending;
    _job = &job;
    _done.store(kInit, std::memory_order_relaxed);
    _run.store(kRunning, std::memory_order_relaxed);
    const auto status = Step();
    if (duckdb::PendingQueryResult::IsResultReady(status)) {
      _result = status;
      return false;
    }
    if (status == duckdb::PendingExecutionResult::RESULT_NOT_READY) {
      _run.store(kQueued, std::memory_order_release);
      Enqueue();
    } else {
      // NO_TASKS / BLOCKED: park unless a wake already raced in (kRunningWoke).
      uint8_t expect = kRunning;
      if (!_run.compare_exchange_strong(expect, kParked, std::memory_order_acq_rel)) {
        _run.store(kQueued, std::memory_order_release);
        Enqueue();
      }
    }
    return _done.exchange(kSuspended, std::memory_order_acq_rel) != kCompleted;
  }

  duckdb::TaskExecutionResult Execute(duckdb::TaskExecutionMode) override {
    _run.store(kRunning, std::memory_order_release);
    const auto status = Step();
    if (duckdb::PendingQueryResult::IsResultReady(status)) {
      _result = status;
      if (_done.exchange(kCompleted, std::memory_order_acq_rel) == kSuspended) {
        _resume.Submit(*_job);
      }
      return duckdb::TaskExecutionResult::TASK_FINISHED;
    }
    if (status == duckdb::PendingExecutionResult::RESULT_NOT_READY) {
      // Made progress; yield and run again via the scheduler.
      _run.store(kQueued, std::memory_order_release);
      return duckdb::TaskExecutionResult::TASK_NOT_FINISHED;
    }
    // NO_TASKS / BLOCKED: park. A wake during Step() set kRunningWoke instead.
    uint8_t expect = kRunning;
    if (_run.compare_exchange_strong(expect, kParked, std::memory_order_acq_rel)) {
      return duckdb::TaskExecutionResult::TASK_BLOCKED;
    }
    _run.store(kQueued, std::memory_order_release);
    return duckdb::TaskExecutionResult::TASK_NOT_FINISHED;
  }

  // The session owns the pump; we never store a self-ptr to be rescheduled from.
  // RequestRun re-enqueues a fresh aliasing ptr instead.
  void Deschedule() override {}

  // Reschedule callback handed to ExecuteTask. The executor fires it (on result-
  // ready / completion / error / unblock) possibly while we are still running, so
  // coalesce: enqueue only from kParked; a wake during kRunning is remembered so
  // Execute re-runs instead of parking.
  void RequestRun() {
    uint8_t s = _run.load(std::memory_order_acquire);
    for (;;) {
      if (s == kParked) {
        if (_run.compare_exchange_weak(s, kQueued, std::memory_order_acq_rel)) {
          Enqueue();
          return;
        }
      } else if (s == kRunning) {
        if (_run.compare_exchange_weak(s, kRunningWoke, std::memory_order_acq_rel)) {
          return;
        }
      } else {  // kQueued or kRunningWoke: already going to run
        return;
      }
    }
  }

  duckdb::PendingExecutionResult Step() {
    return _pending->ExecuteTask([this] { RequestRun(); });
  }

  void Enqueue() {
    _scheduler.ScheduleTask(
      _io_worker.DuckProducer(_scheduler),
      duckdb::shared_ptr<duckdb::Task>{duckdb::shared_ptr<duckdb::Task>{}, this});
  }

  duckdb::TaskScheduler& _scheduler;
  IoExecutor& _io_worker;
  yaclib::IExecutor& _resume;
  duckdb::PendingQueryResult* _pending = nullptr;
  yaclib::Job* _job = nullptr;
  duckdb::PendingExecutionResult _result{};
  std::atomic<uint8_t> _run{kParked};
  std::atomic<uint8_t> _done{kInit};
};

}  // namespace sdb::network::pg
