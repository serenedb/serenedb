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

// Microbenchmark: drain throughput of DuckDB's THREE streaming result
// collectors, to settle whether DuckDB's own order-preserving streaming is
// inherently single-cursor-bound -- independent of SereneDB's wire collector.
//
// The three collectors (src/execution/operator/helper/, main/buffered_data/):
//   * PhysicalBufferedCollector(.., parallel=false) -- single-threaded,
//     order-preserving (SimpleBufferedData, one sink thread).
//   * PhysicalBufferedCollector(.., parallel=true)  -- parallel, NOT
//     order-preserving (SimpleBufferedData, completion order).
//   * PhysicalBufferedBatchCollector(..)            -- parallel AND
//     order-preserving (BatchedBufferedData, min_batch_index single cursor).
//
// COLLECTOR SELECTION. ClientConfig::get_result_collector (the override hook)
// is normally ONLY consulted for non-streaming queries (client_context.cpp:
// `if (!stream_result && client_config.get_result_collector)`), so a streaming
// drain normally goes through PhysicalResultCollector::GetResultCollector,
// which keys off exactly two things:
//   PreserveInsertionOrder(plan)  -- FIXED_ORDER (ORDER BY) => true always;
//                                    else the preserve_insertion_order setting.
//   UseBatchIndex(plan)           -- NumberOfThreads()>1 AND all sources
//                                    support a batch index.
// The ParallelUnordered / Ordered modes drive the choice through query shape +
// settings, which is the production path:
//   ParallelUnordered : preserve_insertion_order=false  -> plain SELECT becomes
//                       NO_ORDER -> BufferedCollector(parallel=true).
//   Ordered           : preserve_insertion_order=true   ->
//   threads>1+batch-index
//                       source -> BufferedBatchCollector; threads==1 ->
//                       BufferedCollector(parallel=false).
// The single-thread collector is, by DuckDB's own logic, ONLY reachable when
// UseBatchIndex is false (threads==1). The ForcedParallel mode installs the
// get_result_collector hook to force BufferedCollector(parallel=true) over ANY
// plan (incl. ORDER BY, whose output it then does NOT preserve -- WRONG
// results, but it isolates the parallel collector's drain cost); reaching the
// hook under streaming needs the TEMP[collector-bench] guard relaxation in
// client_context.cpp. Selection was verified with a temporary stderr probe in
// GetResultCollector during development.
//
// THREAD SWEEP. {threads, external_threads} come from the benchmark Args; we
// sweep 1/8/16/32 internal workers and external_threads 0 vs 1 (DuckDB default
// 1). external_threads=0 makes NumberOfThreads()==threads exactly. SereneDB
// clamps a zero internal-worker request up to 1 (RelaunchThreadsInternal), so
// threads=1/external=1 is skipped (it duplicates threads=1/external=0).
//
// Drain: con.context->PendingQuery(sql, allow_stream_result=true), ExecuteTask
// to RESULT_READY, then Execute() -> StreamQueryResult, then Fetch() chunk by
// chunk to completion (StreamQueryResult::FetchNextInternal ->
// BufferedData::ReplenishBuffer pumps the pipeline / worker pool while we
// consume, so backpressure + the cursor are genuinely exercised). Nothing is
// materialized.

#include <benchmark/benchmark.h>
#include <sys/resource.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <duckdb.hpp>
#include <duckdb/execution/operator/helper/physical_buffered_batch_collector.hpp>
#include <duckdb/execution/operator/helper/physical_buffered_collector.hpp>
#include <duckdb/main/client_config.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/pending_query_result.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <duckdb/main/query_result.hpp>
#include <duckdb/main/stream_query_result.hpp>
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
  auto pending = ctx.PendingQuery(sql, /*allow_stream_result=*/true);
  if (pending->HasError()) {
    std::fprintf(stderr, "pending error: %s\n", pending->GetError().c_str());
    std::abort();
  }
  for (;;) {
    auto status = pending->ExecuteTask();
    if (duckdb::PendingQueryResult::IsResultReady(status)) {
      break;
    }
    if (status == duckdb::PendingExecutionResult::EXECUTION_ERROR) {
      std::fprintf(stderr, "execute task error: %s\n",
                   pending->GetError().c_str());
      std::abort();
    }
    if (status == duckdb::PendingExecutionResult::NO_TASKS_AVAILABLE ||
        status == duckdb::PendingExecutionResult::BLOCKED) {
      pending->WaitForTask();
    }
  }
  auto result = pending->Execute();
  if (result->HasError()) {
    std::fprintf(stderr, "result error: %s\n", result->GetError().c_str());
    std::abort();
  }
  if (result->type != duckdb::QueryResultType::STREAM_RESULT) {
    std::fprintf(stderr,
                 "NOT A STREAM RESULT (type=%d) -- collector did not "
                 "produce a streaming result\n",
                 static_cast<int>(result->type));
    std::abort();
  }
  DrainStats stats;
  for (;;) {
    auto chunk = result->Fetch();
    if (!chunk || chunk->size() == 0) {
      break;
    }
    stats.rows += static_cast<int64_t>(chunk->size());
    benchmark::DoNotOptimize(chunk->data.data());
  }
  return stats;
}

enum class Order { Plain, OrderBy };
enum class Mode {
  // preserve_insertion_order=false: plain SELECT becomes a NO_ORDER plan ->
  // BufferedCollector(parallel=true). (ORDER BY still forces FIXED_ORDER.)
  ParallelUnordered,
  // preserve_insertion_order=true: order-preserving. threads>1 + batch-index
  // source -> BufferedBatchCollector; threads==1 -> BufferedCollector(false).
  Ordered,
  // Force PhysicalBufferedCollector(parallel=true) via the get_result_collector
  // hook, regardless of plan shape -- so we can measure the parallel
  // SimpleBufferedData collector's overhead even over an ORDER BY plan (whose
  // output it does NOT preserve: results are unordered/WRONG, but the goal is
  // the collector's cost in isolation, not correctness). Requires the
  // TEMP[collector-bench] guard relaxation in client_context.cpp that makes the
  // hook fire under streaming.
  ForcedParallel,
  // Force PhysicalBufferedCollector(parallel=false) -- the single-threaded
  // ORDERED collector -- via the hook, at ANY thread count. This is the FAIR
  // comparison the natural Ordered/threads=1 case can't give: it keeps query
  // parallelism (parallel sort, threads=8) but a single-threaded ordered drain,
  // isolating the collector from the sort. Same hook requirement as above.
  ForcedSingleOrdered,
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
    case Mode::ForcedParallel:
      con.Query("SET preserve_insertion_order = true");
      duckdb::ClientConfig::GetConfig(*con.context).get_result_collector =
        [](duckdb::ClientContext& ctx, duckdb::PreparedStatementData& data)
        -> duckdb::unique_ptr<duckdb::PhysicalOperator> {
        auto& physical_plan = *data.physical_plan;
        return duckdb::make_uniq<duckdb::PhysicalBufferedCollector>(
          physical_plan, data, /*parallel=*/true);
      };
      break;
    case Mode::ForcedSingleOrdered:
      con.Query("SET preserve_insertion_order = true");
      duckdb::ClientConfig::GetConfig(*con.context).get_result_collector =
        [](duckdb::ClientContext& ctx, duckdb::PreparedStatementData& data)
        -> duckdb::unique_ptr<duckdb::PhysicalOperator> {
        auto& physical_plan = *data.physical_plan;
        return duckdb::make_uniq<duckdb::PhysicalBufferedCollector>(
          physical_plan, data, /*parallel=*/false);
      };
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
// NumberOfThreads()==threads exactly (the single-thread ordered collector is
// only reachable at threads=1/external=0). We sweep 1/8/16/32 internal workers
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

// Parallel SimpleBufferedData collector forced over both shapes. The orderby
// variant intentionally drops the ordering (wrong results) to isolate the
// parallel collector's drain cost from the ordered-cursor constraint.
// DISABLED: these need the get_result_collector hook to fire under streaming,
// which DuckDB gates off (`if (!stream_result && ...)` in client_context.cpp).
// To run them, re-add the 5-line TEMP[collector-bench] guard relaxation in
// third_party/duckdb/src/main/client_context.cpp and flip to #if 1.
#if 0
COLLECTOR_CASE(forced_parallel_plain, Mode::ForcedParallel, Order::Plain);
COLLECTOR_CASE(forced_parallel_orderby, Mode::ForcedParallel, Order::OrderBy);
// The fair single-threaded-ordered-collector comparison: at threads=8 this is a
// parallel sort feeding a single-threaded ordered drain, so its gap to
// ordered_orderby (BufferedBatchCollector, threads=8) is the collector's cost
// alone -- not the sort. (threads=1 here == the natural single-thread case.)
COLLECTOR_CASE(forced_single_ordered_plain, Mode::ForcedSingleOrdered,
               Order::Plain);
COLLECTOR_CASE(forced_single_ordered_orderby, Mode::ForcedSingleOrdered,
               Order::OrderBy);
#endif

BENCHMARK_MAIN();
