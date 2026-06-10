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
#include <yaclib/exe/submit.hpp>

#include "network/io_executor.h"

namespace sdb::network::pg {

// Drives one DuckDB query to completion as a DuckDB scheduler task, then resumes
// the awaiting io coroutine once.
//
// The pump IS a duckdb::Task. The io coroutine (already hopped onto a DuckDB
// worker) does `co_await pump.Drive(pending)`: the first task slice runs INLINE
// on that worker, so a query that finishes immediately (the common prepared-OLTP
// case) never leaves the thread -- no enqueue, no suspend. If the query is not
// done, the pump enqueues itself and the rest of the query is driven by the
// scheduler's native reschedule loop (ExecuteForever re-enqueues a
// TASK_NOT_FINISHED task via its own token), so the per-slice churn stays inside
// the worker pool instead of round-tripping a coroutine resume through the io
// executor per slice. When the query reaches a terminal status the pump resumes
// the io coroutine on `resume` and the coroutine reads the status from Result().
//
// `final` so the Task vtable call from ExecuteForever is the only indirection.
// Held by value on the session (a session has one query in flight at a time), so
// there is no per-query allocation; the queue gets a non-owning aliasing
// shared_ptr (empty control block, zero refcount) and never deletes the pump.
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

    bool await_suspend(std::coroutine_handle<> handle) noexcept {
      return _pump.Start(_pending, handle);
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
  enum State : uint8_t { kInit, kSuspended, kCompleted };

  // Runs the first slice inline. Returns true if the caller must suspend (the
  // pump will resume it once the query terminates), false if the query already
  // terminated inline and the caller should continue without suspending.
  bool Start(duckdb::PendingQueryResult& pending,
             std::coroutine_handle<> handle) noexcept {
    _pending = &pending;
    _handle = handle;
    _state.store(kInit, std::memory_order_relaxed);
    const auto status = Step();
    if (duckdb::PendingQueryResult::IsResultReady(status)) {
      _result = status;
      return false;
    }
    // Keep driving on the pool. NO_TASKS / NOT_READY: enqueue ourselves now.
    // BLOCKED: the query executor stored our on_reschedule and re-enqueues us
    // when it unblocks, so we must NOT also enqueue here.
    if (status != duckdb::PendingExecutionResult::BLOCKED) {
      Enqueue();
    }
    // Publish the suspend; if the pump already completed (it ran to terminal on
    // another worker before we got here) it left kCompleted -> resume in place.
    return _state.exchange(kSuspended, std::memory_order_acq_rel) != kCompleted;
  }

  duckdb::TaskExecutionResult Execute(duckdb::TaskExecutionMode) override {
    const auto status = Step();
    switch (status) {
      case duckdb::PendingExecutionResult::RESULT_NOT_READY:
      case duckdb::PendingExecutionResult::NO_TASKS_AVAILABLE:
        // Native reschedule: ExecuteForever re-enqueues us via task->token.
        return duckdb::TaskExecutionResult::TASK_NOT_FINISHED;
      case duckdb::PendingExecutionResult::BLOCKED:
        // Descheduled; our on_reschedule re-enqueues us when the query unblocks.
        return duckdb::TaskExecutionResult::TASK_BLOCKED;
      default:
        _result = status;
        if (_state.exchange(kCompleted, std::memory_order_acq_rel) == kSuspended) {
          yaclib::Submit(_resume, [handle = _handle] { handle.resume(); });
        }
        return duckdb::TaskExecutionResult::TASK_FINISHED;
    }
  }

  // The session owns the pump; we never store a self-ptr to be rescheduled from.
  // On unblock, our on_reschedule enqueues a fresh aliasing ptr instead.
  void Deschedule() override {}

  duckdb::PendingExecutionResult Step() {
    return _pending->ExecuteTask([this] { Enqueue(); });
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
  std::coroutine_handle<> _handle{};
  duckdb::PendingExecutionResult _result{};
  std::atomic<uint8_t> _state{kInit};
};

}  // namespace sdb::network::pg
