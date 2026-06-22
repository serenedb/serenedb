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

// Microbenchmark isolating a MERGE on a serenedb facade table vs a plain
// (native) attached duckdb table, IN-PROCESS: no pg-wire listener, no SSL, no
// request handling. Goal: reproduce and dissect the facade-vs-native MERGE gap
// without the connection/protocol layer. Both tables are exercised through a
// raw duckdb::Connection, which does NOT install the per-session
// SereneDBClientState transaction hooks -- so this also strips that layer.

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>
#include <benchmark/benchmark.h>

#include <filesystem>
#include <future>
#include <optional>
#include <string>

#include "basics/duckdb_engine.h"
#include "catalog/catalog.h"
#include "catalog/store/store.h"
#include "connector/duckdb_client_state.h"
#include "general_server/scheduler.h"
#include "general_server/scheduler_feature.h"
#include "pg/connection_context.h"
#include "query/server_engine.h"
#include "rest_server/database_path_feature.h"
#include "storage_engine/search_engine.h"

ABSL_DECLARE_FLAG(std::string, server_directory);

namespace {

constexpr int kRows = 10'000'000;

std::string MergeSql(std::string_view table) {
  return "MERGE INTO " + std::string{table} +
         " AS t USING (SELECT i AS id, i*2 AS v FROM "
         "generate_series(5000001,15000000) s(i)) AS src ON t.id=src.id "
         "WHEN MATCHED THEN UPDATE SET v=src.v "
         "WHEN NOT MATCHED THEN INSERT (id,v) VALUES (src.id,src.v);";
}

std::string ResetSql(std::string_view table) {
  return "INSERT INTO " + std::string{table} +
         " SELECT i,i FROM generate_series(1," + std::to_string(kRows) +
         ") t(i);";
}

// One-time, process-wide engine + serenedb catalog bootstrap. Mirrors serened's
// startup up through InitCatalog + search, but never starts ssl/general/pg.
struct Harness {
  std::optional<sdb::DatabasePathFeature> db_path;
  std::optional<sdb::catalog::CatalogStore> store;
  std::optional<sdb::SchedulerFeature> scheduler;
  std::optional<sdb::search::SearchEngine> search;
  duckdb::unique_ptr<duckdb::Connection> conn;
  std::shared_ptr<sdb::ConnectionContext>
    conn_ctx;  // keeps session state alive

  Harness() {
    const auto dir =
      (std::filesystem::temp_directory_path() / "sdb_facade_merge_bench")
        .string();
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    absl::SetFlag(&FLAGS_server_directory, dir);

    auto& engine = sdb::DuckDBEngine::Instance();
    engine.Initialize(&sdb::server::query::ConfigureServerDBConfig);
    sdb::server::query::RegisterServerExtensions(engine.instance());

    db_path.emplace();
    store.emplace();
    store->Initialize(dir);
    scheduler.emplace();
    scheduler->start();
    sdb::catalog::InitCatalog();
    search.emplace();
    search->start();

    conn = engine.CreateConnection();
    // The facade catalog resolves schemas/tables through the per-connection
    // ConnectionContext (GetSereneDBContext asserts it), so register one. The
    // pg send buffer / copy queue are nullptr -- only result delivery / COPY
    // use them, which a timing benchmark ignores.
    auto snapshot = sdb::catalog::GetCatalog().GetCatalogSnapshot();
    auto database = snapshot->GetDatabase("postgres");
    conn_ctx = std::make_shared<sdb::ConnectionContext>(
      *conn->context, "postgres", "postgres", database->GetId(),
      std::move(database), nullptr, nullptr);
    sdb::connector::SereneDBClientState::Register(*conn->context, conn_ctx);
    conn->context->session_user = "postgres";

    Run("ATTACH '" + dir + "/native.db' AS ddb;");
    // Facade table lives in the serenedb 'postgres' catalog (data in
    // __sdb_store).
    Run("CREATE TABLE postgres.public.f3 (id BIGINT, v BIGINT);");
    // Native table lives in a plain attached duckdb file.
    Run("CREATE TABLE ddb.n3 (id BIGINT, v BIGINT);");
  }

  void Run(const std::string& sql) {
    auto r = conn->Query(sql);
    if (r->HasError()) {
      throw std::runtime_error("setup query failed: " + sql + " -> " +
                               r->GetError());
    }
  }
};

Harness& GetHarness() {
  static Harness h;
  return h;
}

void RunMergeBench(benchmark::State& state, std::string_view table) {
  auto& h = GetHarness();
  const auto merge = MergeSql(table);
  const auto reset = ResetSql(table);
  const auto truncate = "TRUNCATE " + std::string{table} + ";";
  for (auto _ : state) {
    state.PauseTiming();
    h.Run(truncate);
    h.Run(reset);
    state.ResumeTiming();
    auto r = h.conn->Query(merge);
    benchmark::DoNotOptimize(r);
    if (r->HasError()) {
      state.SkipWithError(r->GetError().c_str());
      break;
    }
  }
}

// Same merge, but coordinated on a folly CPU-pool thread instead of the main
// thread -- this mimics pg-wire, where the connection runs on a folly thread.
// If the facade-vs-native gap reappears here (and not in the main-thread
// variant above), the differential is the coordinator thread / its jemalloc
// arena, not the merge itself.
void RunMergeOnFolly(benchmark::State& state, std::string_view table) {
  auto& h = GetHarness();
  auto& exec = sdb::GetScheduler()->GetCPUExecutor();
  const auto merge = MergeSql(table);
  const auto reset = ResetSql(table);
  const auto truncate = "TRUNCATE " + std::string{table} + ";";
  for (auto _ : state) {
    state.PauseTiming();
    h.Run(truncate);
    h.Run(reset);
    state.ResumeTiming();
    std::promise<void> done;
    auto fut = done.get_future();
    exec.add([&] {
      auto r = h.conn->Query(merge);
      benchmark::DoNotOptimize(r);
      if (r->HasError()) {
        state.SkipWithError(r->GetError().c_str());
      }
      done.set_value();
    });
    fut.wait();
  }
}

void BmFacadeMerge(benchmark::State& state) {
  RunMergeBench(state, "postgres.public.f3");
}
void BmNativeMerge(benchmark::State& state) { RunMergeBench(state, "ddb.n3"); }
void BmFacadeMergeFolly(benchmark::State& state) {
  RunMergeOnFolly(state, "postgres.public.f3");
}
void BmNativeMergeFolly(benchmark::State& state) {
  RunMergeOnFolly(state, "ddb.n3");
}

BENCHMARK(BmFacadeMerge)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BmNativeMerge)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BmFacadeMergeFolly)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BmNativeMergeFolly)->Unit(benchmark::kMillisecond)->Iterations(10);

}  // namespace

BENCHMARK_MAIN();
