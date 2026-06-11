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

#include <duckdb/parallel/task_scheduler.hpp>
#include <thread>
#include <yaclib/async/future.hpp>
#include <yaclib/coro/coro.hpp>
#include <yaclib/coro/future.hpp>
#include <yaclib/coro/on.hpp>

#include "basics/duckdb_engine.h"
#include "network/io_context.h"
#include "network/pg/duck_executor.h"

using namespace sdb;

TEST(NetworkDuckExecutor, ResumesOnWorkerThread) {
  auto& scheduler =
    duckdb::TaskScheduler::GetScheduler(DuckDBEngine::Instance().instance());
  network::IoThreadPool pool{1};
  pool.Start();
  network::pg::DuckExecutor executor{scheduler, pool.Next()};

  const auto test_tid = std::this_thread::get_id();
  std::thread::id worker_tid{};
  auto future = [&]() -> yaclib::Future<> {
    co_await yaclib::On(executor);
    worker_tid = std::this_thread::get_id();
    co_return {};
  }();
  [[maybe_unused]] const auto result = std::move(future).Get();

  EXPECT_NE(worker_tid, std::thread::id{});
  EXPECT_NE(worker_tid, test_tid);
  pool.Stop();
}

TEST(NetworkIoExecutor, ResumesOnIoThread) {
  network::IoThreadPool pool{1};
  pool.Start();
  auto& executor = pool.Next();

  const auto test_tid = std::this_thread::get_id();
  std::thread::id io_tid{};
  auto future = [&]() -> yaclib::Future<> {
    co_await yaclib::On(executor);
    io_tid = std::this_thread::get_id();
    co_return {};
  }();
  [[maybe_unused]] const auto result = std::move(future).Get();

  EXPECT_NE(io_tid, std::thread::id{});
  EXPECT_NE(io_tid, test_tid);
  pool.Stop();
}
