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

#include <duckdb/main/connection.hpp>
#include <duckdb/main/materialized_query_result.hpp>
#include <duckdb/main/pending_query_result.hpp>
#include <duckdb/main/prepared_statement.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <yaclib/async/future.hpp>
#include <yaclib/coro/coro.hpp>
#include <yaclib/coro/future.hpp>
#include <yaclib/coro/on.hpp>

#include "basics/duckdb_engine.h"
#include "network/io_context.h"
#include "network/pg/duck_executor.h"
#include "network/pg/query_pump.h"

using namespace sdb;

TEST(NetworkDuckPump, RunsSelectOffIoThread) {
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
  auto& io_worker = pool.Next();
  network::pg::DuckExecutor duck{scheduler, io_worker};
  network::pg::QueryPump pump{scheduler, io_worker, duck};

  // Mirror the session: hop onto a DuckDB worker first -- Drive runs the
  // first task slice inline on the calling thread.
  auto future = [&]() -> yaclib::Future<duckdb::PendingExecutionResult> {
    co_await yaclib::On(duck);
    co_return co_await pump.Drive(*pending);
  }();
  const auto status = std::move(future).Get().Ok();
  ASSERT_NE(status, duckdb::PendingExecutionResult::EXECUTION_ERROR);

  auto result = pending->Execute();
  ASSERT_FALSE(result->HasError());
  auto& materialized = result->Cast<duckdb::MaterializedQueryResult>();
  EXPECT_EQ(materialized.GetValue(0, 0).GetValue<int64_t>(), 42);
  pool.Stop();
}
