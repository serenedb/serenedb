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
#include <duckdb/parallel/task.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <yaclib/exe/job.hpp>

#include "network/io_executor.h"

namespace sdb::network::pg {

// Hosts ONE endless coroutine as a duckdb::Task on the DuckDB scheduler --
// the pg session's duck-side half. The coroutine never migrates executors:
// it suspends (Park/Yield) and every resume is a fresh Execute() stack frame
// from the scheduler's ExecuteForever, so resumes can never recurse no matter
// how many wake/park cycles happen.
//
// Wakes are reason-agnostic: message arrived (io), query progressed (the
// executor's on_reschedule patch), send drained below high-water (io writer)
// all call RequestRun(), coalesced by the kParked/kQueued/kRunning/
// kRunningWoke state machine (lifted from the former QueryPump). The hosted
// coroutine re-checks its own condition on resume; spurious wakes are
// harmless.
//
// Concurrency discipline: the parked state is published by Execute() AFTER
// the coroutine suspended (never by the awaiter), so no thread can resume the
// coroutine while a Call() is still on some worker's stack. While in
// kRunning/kRunningWoke a RequestRun only flags; the requeue decision is
// Execute()'s alone.
//
// Held by value on the session; the scheduler gets a non-owning aliasing
// shared_ptr (empty control block) and never deletes it.
class TaskRunner final : public duckdb::Task {
  enum Run : uint8_t { kParked, kQueued, kRunning, kRunningWoke };
  enum Suspend : uint8_t { kPark, kYield };

 public:
  TaskRunner(duckdb::TaskScheduler& scheduler, IoExecutor& io_worker) noexcept
    : _scheduler{scheduler}, _io_worker{io_worker} {}

  // First await of the hosted coroutine: captures the promise-as-Job and
  // moves the body onto a duck worker. The caller's thread runs nothing past
  // this point.
  class [[nodiscard]] BeginAwaiter {
   public:
    explicit BeginAwaiter(TaskRunner& runner) noexcept : _runner{runner} {}
    bool await_ready() const noexcept { return false; }
    template<typename Promise>
    void await_suspend(std::coroutine_handle<Promise> handle) noexcept {
      _runner._job = &handle.promise();
      _runner._run.store(kQueued, std::memory_order_release);
      _runner.Enqueue();
    }
    void await_resume() const noexcept {}

   private:
    TaskRunner& _runner;
  };

  // Suspend until the next RequestRun. Captures the CURRENT coroutine's
  // promise-as-Job: parks happen in nested coroutines (frame waits, query
  // drives, backpressure), and the wake must resume the innermost suspended
  // one -- its completion then propagates up the future chain naturally.
  // Execute() publishes the parked state after the suspension completed.
  class [[nodiscard]] ParkAwaiter {
   public:
    explicit ParkAwaiter(TaskRunner& runner) noexcept : _runner{runner} {}
    bool await_ready() const noexcept { return false; }
    template<typename Promise>
    void await_suspend(std::coroutine_handle<Promise> handle) noexcept {
      _runner._job = &handle.promise();
      _runner._suspend = kPark;
    }
    void await_resume() const noexcept {}

   private:
    TaskRunner& _runner;
  };

  // Suspend and immediately requeue: gives the worker back to the scheduler
  // between query slices that made progress (scheduler fairness), without
  // waiting for a wake.
  class [[nodiscard]] YieldAwaiter {
   public:
    explicit YieldAwaiter(TaskRunner& runner) noexcept : _runner{runner} {}
    bool await_ready() const noexcept { return false; }
    template<typename Promise>
    void await_suspend(std::coroutine_handle<Promise> handle) noexcept {
      _runner._job = &handle.promise();
      _runner._suspend = kYield;
    }
    void await_resume() const noexcept {}

   private:
    TaskRunner& _runner;
  };

  BeginAwaiter Begin() noexcept { return BeginAwaiter{*this}; }
  ParkAwaiter Park() noexcept { return ParkAwaiter{*this}; }
  YieldAwaiter Yield() noexcept { return YieldAwaiter{*this}; }

  // The hosted coroutine must call this right before its final co_return so
  // the last Execute() returns TASK_FINISHED instead of re-parking.
  void Finish() noexcept { _finished = true; }

  // The one wake primitive, callable from any thread.
  void RequestRun() noexcept {
    // Dekker handshake with Execute(): the caller's preceding publication
    // (e.g. the recv watermark) must be globally visible BEFORE _run is read.
    // Without this, a no-op on kQueued can race a run whose condition check
    // already loaded pre-publication state and is about to park -- with
    // nobody left to wake it (the store-load reordering through the store
    // buffer). Execute()'s entry exchange is the mirror barrier.
    std::atomic_thread_fence(std::memory_order_seq_cst);
    uint8_t s = _run.load(std::memory_order_acquire);
    for (;;) {
      if (s == kParked) {
        if (_run.compare_exchange_weak(s, kQueued, std::memory_order_acq_rel)) {
          Enqueue();
          return;
        }
      } else if (s == kRunning) {
        if (_run.compare_exchange_weak(s, kRunningWoke,
                                       std::memory_order_acq_rel)) {
          return;
        }
      } else {  // kQueued or kRunningWoke: already going to run
        return;
      }
    }
  }

  duckdb::TaskExecutionResult Execute(duckdb::TaskExecutionMode) override {
    // Full RMW, not a store: kRunning must be globally visible before the
    // coroutine's condition checks load anything, so a concurrent RequestRun
    // either sees kRunning (and flags kRunningWoke -> we requeue) or its
    // publication is visible to our checks. See RequestRun's fence.
    _run.exchange(kRunning, std::memory_order_seq_cst);
    _job->Call();
    // The coroutine is suspended (Park/Yield) or done; nobody can resume it
    // until we publish kParked below, so _suspend/_finished reads are safe.
    if (_finished) {
      return duckdb::TaskExecutionResult::TASK_FINISHED;
    }
    if (_suspend == kYield) {
      _run.store(kQueued, std::memory_order_release);
      return duckdb::TaskExecutionResult::TASK_NOT_FINISHED;
    }
    uint8_t expect = kRunning;
    if (_run.compare_exchange_strong(expect, kParked,
                                     std::memory_order_acq_rel)) {
      return duckdb::TaskExecutionResult::TASK_BLOCKED;
    }
    // A wake raced in while running: requeue instead of parking.
    _run.store(kQueued, std::memory_order_release);
    return duckdb::TaskExecutionResult::TASK_NOT_FINISHED;
  }

  // The session owns the runner; wakes re-enqueue a fresh aliasing ptr.
  void Deschedule() override {}

 private:
  void Enqueue() {
    _scheduler.ScheduleTask(_io_worker.DuckProducer(_scheduler),
                            duckdb::shared_ptr<duckdb::Task>{
                              duckdb::shared_ptr<duckdb::Task>{}, this});
  }

  duckdb::TaskScheduler& _scheduler;
  IoExecutor& _io_worker;
  yaclib::Job* _job = nullptr;
  Suspend _suspend = kPark;
  bool _finished = false;
  std::atomic<uint8_t> _run{kParked};
};

}  // namespace sdb::network::pg
