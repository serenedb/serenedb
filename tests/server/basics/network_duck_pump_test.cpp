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

#include <gtest/gtest.h>

#include <atomic>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/materialized_query_result.hpp>
#include <duckdb/main/pending_query_result.hpp>
#include <duckdb/main/prepared_statement.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <thread>
#include <yaclib/async/future.hpp>
#include <yaclib/coro/coro.hpp>
#include <yaclib/coro/future.hpp>

#include "basics/duckdb_engine.h"
#include "network/cpu_resumer.h"
#include "network/io_context.h"

using namespace sdb;

namespace {

// Hosted coroutine: begins on a duck worker, drives one query with
// Yield/Park exactly like the session's DriveQuery, then parks until an
// external RequestRun releases it.
yaclib::Future<> HostedBody(network::CpuResumer& task,
                            duckdb::PendingQueryResult& pending,
                            std::atomic<int>& phase, std::thread::id test_tid) {
  co_await task.Park();
  EXPECT_NE(std::this_thread::get_id(), test_tid);

  for (;;) {
    const auto status = pending.ExecuteTask([&task] { task.RequestRun(); });
    if (duckdb::PendingQueryResult::IsResultReady(status)) {
      break;
    }
    if (status == duckdb::PendingExecutionResult::RESULT_NOT_READY) {
      co_await task.Yield();
    } else {
      co_await task.Park();
    }
  }
  phase.store(1, std::memory_order_release);

  // Park until the test thread wakes us; resume must again be a duck worker.
  while (phase.load(std::memory_order_acquire) != 2) {
    co_await task.Park();
  }
  EXPECT_NE(std::this_thread::get_id(), test_tid);
  phase.store(3, std::memory_order_release);
  task.Finish();
  co_return {};
}

}  // namespace

TEST(NetworkCpuResumer, DrivesQueryAndParksOffTestThread) {
  auto connection = DuckDBEngine::Instance().CreateConnection();
  auto prepared = connection->Prepare("SELECT 42");
  ASSERT_FALSE(prepared->HasError());
  duckdb::vector<duckdb::Value> params;
  auto pending = prepared->PendingQuery(params, /*allow_stream_result=*/false);
  ASSERT_FALSE(pending->HasError());

  auto& scheduler =
    duckdb::TaskScheduler::GetScheduler(DuckDBEngine::Instance().instance());
  network::IoThreadPool pool{1};
  pool.Start();
  // shared_ptr-managed: CpuResumer enqueues itself via shared_from_this().
  auto task =
    duckdb::make_shared_ptr<network::CpuResumer>(scheduler, pool.Next());

  std::atomic<int> phase{0};
  auto future = HostedBody(*task, *pending, phase, std::this_thread::get_id());
  // First Park sets the resume job; one bootstrap kick schedules it.
  task->RequestRun();

  while (phase.load(std::memory_order_acquire) != 1) {
    std::this_thread::yield();
  }
  auto result = pending->Execute();
  ASSERT_FALSE(result->HasError());
  auto& materialized = result->Cast<duckdb::MaterializedQueryResult>();
  EXPECT_EQ(materialized.GetValue(0, 0).GetValue<int64_t>(), 42);

  // Wake the parked coroutine from a foreign thread.
  phase.store(2, std::memory_order_release);
  task->RequestRun();
  [[maybe_unused]] const auto done = std::move(future).Get();
  EXPECT_EQ(phase.load(std::memory_order_acquire), 3);
  pool.Stop();
}
