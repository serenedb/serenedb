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

// THREAD SWEEP. {threads, external_threads} come from the benchmark Args; we
// sweep 1/8/16/32 internal workers and external_threads 0 vs 1 (DuckDB default
// 1). external_threads=0 makes NumberOfThreads()==threads exactly. SereneDB
// clamps a zero internal-worker request up to 1 (RelaunchThreadsInternal), so
// threads=1/external=1 is skipped (it duplicates threads=1/external=0).

#include <benchmark/benchmark.h>
#include <sys/resource.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <duckdb.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/query_result.hpp>
#include <duckdb/main/query_result_stream.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <string>

namespace {

// Row count is env-tunable (SDB_COLLECTOR_ROWS) so the table can be grown
// without recompiling. Default 20M makes each drain ~0.1-0.5s at the thread
// counts under test.
int64_t Rows() {
  static const int64_t rows = [] {
    if (const char* e = std::getenv("SDB_COLLECTOR_ROWS")) {
      return static_cast<int64_t>(std::strtoll(e, nullptr, 10));
    }
    return static_cast<int64_t>(20'000'000);
  }();
  return rows;
}

// One shared in-memory DB with the test table built once. Per-query settings
// (threads, preserve_insertion_order) are applied on the per-benchmark
// connection, so the table stays put across cases.
duckdb::DuckDB& Db() {
  static duckdb::DuckDB db = [] {
    duckdb::DuckDB d(nullptr);
    duckdb::Connection con(d);
    // A bare in-memory DuckDB under the SereneDB build has no schema selected
    // for CREATE; create one and pin the search path before building the table.
    con.Query("CREATE SCHEMA IF NOT EXISTS main");
    con.Query("SET search_path = 'main'");
    auto r = con.Query(
      "CREATE TABLE main.t AS SELECT i::BIGINT k, (i*2)::BIGINT a, "
      "'row_' || i::VARCHAR s FROM range(" +
      std::to_string(Rows()) + ") tbl(i)");
    if (r->HasError()) {
      std::fprintf(stderr, "table build failed: %s\n", r->GetError().c_str());
      std::abort();
    }
    return d;
  }();
  return db;
}

// Approximate the bytes a wire encoder would push: 2 BIGINTs + the string. Used
// only to turn the row count into a MB/s figure; not a wire-format size.
double EstimateBytes(int64_t rows) {
  // 8 + 8 for the two bigints, ~9 chars avg for 'row_<=7 digits>'.
  return static_cast<double>(rows) * (8 + 8 + 9);
}

struct DrainStats {
  int64_t rows = 0;
};

DrainStats DrainOnce(duckdb::ClientContext& ctx, const std::string& sql) {
  auto result = ctx.Submit(sql, duckdb::QueryParameters{});
  if (result->HasError()) {
    std::fprintf(stderr, "submit error: %s\n", result->GetError().c_str());
    std::abort();
  }
  duckdb::QueryResultStream<duckdb::ChunkFormat> stream{std::move(result)};
  DrainStats stats;
  for (;;) {
    auto chunk = stream.Fetch();
    if (!chunk || chunk->size() == 0) {
      break;
    }
    stats.rows += static_cast<int64_t>(chunk->size());
    benchmark::DoNotOptimize(chunk->data.data());
  }
  return stats;
}

enum class Order {
  Plain,
  OrderBy,
};
enum class Mode {
  ParallelUnordered,
  Ordered,
};

const char* SqlFor(Order order) {
  switch (order) {
    case Order::Plain:
      return "SELECT k, a, s FROM main.t";
    case Order::OrderBy:
      return "SELECT k, a, s FROM main.t ORDER BY k";
  }
  return "";
}

// threads/external_threads are passed via the benchmark Args() so a single
// case sweeps thread counts. external_threads=0 lets `SET threads=N` reach
// NumberOfThreads()==N exactly; external_threads=1 (DuckDB's default, one
// caller thread) leaves N-1 internal workers + 1 external => N reported, but
// the external/io thread runs no query tasks under SereneDB, so it effectively
// has N-1 workers. We sweep both to see which the collector prefers.
void Configure(duckdb::Connection& con, Mode mode, int threads,
               int external_threads) {
  con.Query("SET external_threads = " + std::to_string(external_threads));
  con.Query("SET threads = " + std::to_string(threads));
  switch (mode) {
    case Mode::ParallelUnordered:
      con.Query("SET preserve_insertion_order = false");
      break;
    case Mode::Ordered:
      con.Query("SET preserve_insertion_order = true");
      break;
  }
}

double CpuSecondsSelf() {
  struct rusage ru{};
  getrusage(RUSAGE_SELF, &ru);
  auto to_s = [](const struct timeval& tv) {
    return static_cast<double>(tv.tv_sec) +
           static_cast<double>(tv.tv_usec) / 1e6;
  };
  return to_s(ru.ru_utime) + to_s(ru.ru_stime);
}

void RunCase(benchmark::State& state, Mode mode, Order order) {
  const int threads = static_cast<int>(state.range(0));
  const int external_threads = static_cast<int>(state.range(1));
  duckdb::Connection con(Db());
  Configure(con, mode, threads, external_threads);
  const std::string sql = SqlFor(order);

  int64_t total_rows = 0;
  double cpu_total = 0;
  double wall_total = 0;
  for (auto _ : state) {
    const double cpu0 = CpuSecondsSelf();
    const auto wall0 = std::chrono::steady_clock::now();
    auto stats = DrainOnce(*con.context, sql);
    const auto wall1 = std::chrono::steady_clock::now();
    const double cpu1 = CpuSecondsSelf();
    cpu_total += cpu1 - cpu0;
    wall_total += std::chrono::duration<double>(wall1 - wall0).count();
    total_rows += stats.rows;
    if (stats.rows != Rows()) {
      state.SkipWithError("unexpected row count");
      break;
    }
  }

  state.SetItemsProcessed(total_rows);
  state.SetBytesProcessed(static_cast<int64_t>(EstimateBytes(total_rows)));
  state.counters["rows_per_s"] = benchmark::Counter(
    static_cast<double>(total_rows), benchmark::Counter::kIsRate);
  state.counters["MB_per_s"] = benchmark::Counter(
    EstimateBytes(total_rows) / 1e6, benchmark::Counter::kIsRate);
  state.counters["threads"] =
    duckdb::TaskScheduler::GetScheduler(*con.context).NumberOfThreads();
  // CPU cores ~ process CPU-seconds / wall-seconds across the drain region.
  state.counters["cpu_cores"] = wall_total > 0 ? cpu_total / wall_total : 0.0;
}

}  // namespace

// Thread sweep: {threads, external_threads}. external_threads=0 makes
// NumberOfThreads()==threads exactly. We sweep 1/8/16/32 internal workers
// and, for the higher counts, both external=0 and external=1 (DuckDB default).
void ApplySweep(benchmark::internal::Benchmark* b) {
  b->Unit(benchmark::kMillisecond)->UseRealTime();
  b->ArgNames({"threads", "ext"});
  for (int ext : {0, 1}) {
    for (int t : {1, 8, 16, 32}) {
      if (t == 1 && ext == 1) {
        continue;  // threads=1/ext=1 => 0 internal workers; SereneDB clamps to
                   // 1 anyway, so it duplicates t=1/ext=0 conceptually.
      }
      b->Args({t, ext});
    }
  }
}

#define COLLECTOR_CASE(name, mode, order)                             \
  void name(benchmark::State& state) { RunCase(state, mode, order); } \
  BENCHMARK(name)->Apply(ApplySweep)

COLLECTOR_CASE(parallel_unordered_plain, Mode::ParallelUnordered, Order::Plain);
COLLECTOR_CASE(parallel_unordered_orderby, Mode::ParallelUnordered,
               Order::OrderBy);
COLLECTOR_CASE(ordered_plain, Mode::Ordered, Order::Plain);
COLLECTOR_CASE(ordered_orderby, Mode::Ordered, Order::OrderBy);

BENCHMARK_MAIN();
