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

#include <coroutine>
#include <duckdb/common/enums/pending_execution_result.hpp>
#include <duckdb/main/pending_query_result.hpp>
#include <yaclib/async/future.hpp>
#include <yaclib/coro/coro.hpp>
#include <yaclib/coro/future.hpp>
#include <yaclib/coro/on.hpp>
#include <yaclib/exe/executor.hpp>
#include <yaclib/exe/submit.hpp>

namespace sdb::network::pg {

// Runs one DuckDB task slice. On BLOCKED (a source/sink waiting on async IO or
// consumer backpressure) the coroutine suspends and DuckDB's on_reschedule
// callback resumes it on `executor` when the task becomes runnable again -- no
// polling. BLOCKED is only returned when the awaited work has NOT completed
// yet, so the callback always fires strictly after await_suspend returns; there
// is no resume race. Any other status resumes inline without a hop.
class ExecuteTaskAwaiter final {
 public:
  ExecuteTaskAwaiter(yaclib::IExecutor& executor,
                     duckdb::PendingQueryResult& pending) noexcept
    : _executor{executor}, _pending{pending} {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> handle) noexcept {
    _handle = handle;
    _status = _pending.ExecuteTask([this] {
      yaclib::Submit(_executor, [handle = _handle] { handle.resume(); });
    });
    return _status == duckdb::PendingExecutionResult::BLOCKED;
  }

  duckdb::PendingExecutionResult await_resume() const noexcept {
    return _status;
  }

 private:
  yaclib::IExecutor& _executor;
  duckdb::PendingQueryResult& _pending;
  std::coroutine_handle<> _handle{};
  duckdb::PendingExecutionResult _status{};
};

inline yaclib::Future<duckdb::PendingExecutionResult> DrivePending(
  yaclib::IExecutor& executor, duckdb::PendingQueryResult& pending) {
  // Caller already runs on `executor` (the dispatch hops to *_duck before
  // invoking us, and RunSimpleQuery re-hops per statement), so the first task
  // slice runs inline -- we only re-hop on NO_TASKS_AVAILABLE below. Saves one
  // scheduler enqueue + worker wake per Execute.
  for (;;) {
    const auto status = co_await ExecuteTaskAwaiter{executor, pending};
    switch (status) {
      case duckdb::PendingExecutionResult::RESULT_READY:
      case duckdb::PendingExecutionResult::EXECUTION_FINISHED:
      case duckdb::PendingExecutionResult::EXECUTION_ERROR:
        co_return status;
      case duckdb::PendingExecutionResult::NO_TASKS_AVAILABLE:
        co_await yaclib::On(executor);
        break;
      case duckdb::PendingExecutionResult::RESULT_NOT_READY:
      case duckdb::PendingExecutionResult::BLOCKED:
        break;
    }
  }
}

}  // namespace sdb::network::pg
