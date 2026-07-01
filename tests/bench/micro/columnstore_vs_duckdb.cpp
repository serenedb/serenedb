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

// Baseline: serenedb's iresearch columnstore (read + write) vs a native DuckDB
// in-memory column over the SAME data, in one process. Measures the four read
// shapes (full scan / sparse gather / point lookup) + write/seal for the
// iresearch side, and a streaming full-scan drain for the native-DuckDB side.
//
// This is the "before" measurement the columnstore rewrite must beat. The
// iresearch read loop deliberately REUSES one output Vector across 2048-row
// batches (the production materializer / merge shape), so the per-row validity
// reset cost is included, not hidden by an artificially fresh vector.
//
// Build (from build_perf):
//   cmake .                                    # only because CMakeLists
//   changed ninja serenedb-bench-micro-columnstore_vs_duckdb
//   SDB_BENCH_ROWS=2000000 ./bin/serenedb-bench-micro-columnstore_vs_duckdb
//
// build_perf is RelWithDebInfo/-O3: correct for A/B deltas; re-run headline
// absolute numbers under the `bench` preset before publishing a claim.

#include <benchmark/benchmark.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <duckdb.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/pending_query_result.hpp>
#include <duckdb/main/stream_query_result.hpp>
#include <memory>
#include <string>
#include <vector>

#include "basics/duckdb_engine.h"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/store/memory_directory.hpp"

namespace {

enum class Shape { Bigint, VarcharDict };

// Row count is env-tunable so the column can be grown without recompiling.
// Default 2M => ~16 row groups of DEFAULT_ROW_GROUP_SIZE and ~1000 vectors.
uint64_t Rows() {
  static const uint64_t rows = [] {
    if (const char* e = std::getenv("SDB_BENCH_ROWS")) {
      return static_cast<uint64_t>(std::strtoull(e, nullptr, 10));
    }
    return static_cast<uint64_t>(2'000'000);
  }();
  return rows;
}

constexpr irs::field_id kField = 0;
constexpr std::string_view kSeg = "bench_seg";

duckdb::LogicalType TypeOf(Shape s) {
  return s == Shape::Bigint ? duckdb::LogicalType::BIGINT
                            : duckdb::LogicalType::VARCHAR;
}

bool IsNull(uint64_t g, bool nullable) { return nullable && (g % 10 == 0); }

// ---- iresearch columnstore side -------------------------------------------

// The process-wide DuckDBEngine instance (brought up in main()) supplies the
// DatabaseInstance the columnstore codecs read their compression registry from.
duckdb::DatabaseInstance& CsDb() {
  return sdb::DuckDBEngine::Instance().instance();
}

struct Seg {
  irs::MemoryDirectory dir{};
  std::unique_ptr<irs::ColReader> reader;
  const irs::ColumnReader* col = nullptr;
  uint64_t rows = 0;
};

void FillBatch(duckdb::Vector& vec, Shape shape, bool nullable, uint64_t base,
               uint64_t take) {
  auto& val = duckdb::FlatVector::ValidityMutable(vec);
  val.Reset(STANDARD_VECTOR_SIZE);
  if (shape == Shape::Bigint) {
    auto* d = duckdb::FlatVector::GetDataMutable<int64_t>(vec);
    for (uint64_t k = 0; k < take; ++k) {
      const auto g = base + k;
      if (IsNull(g, nullable)) {
        val.SetInvalid(k);
      } else {
        d[k] = static_cast<int64_t>(g);
      }
    }
  } else {
    auto* d = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec);
    for (uint64_t k = 0; k < take; ++k) {
      const auto g = base + k;
      if (IsNull(g, nullable)) {
        val.SetInvalid(k);
      } else {
        const auto str = "cat_" + std::to_string(g % 50);
        d[k] = duckdb::StringVector::AddString(vec, str);
      }
    }
  }
}

// Build (and seal) a one-column .col segment in a fresh MemoryDirectory.
std::unique_ptr<Seg> BuildSeg(Shape shape, bool nullable) {
  auto seg = std::make_unique<Seg>();
  seg->rows = Rows();
  const auto type = TypeOf(shape);
  {
    irs::ColWriter w{seg->dir, kSeg, CsDb()};
    auto& cw = w.OpenColumn(kField, type);
    uint64_t pos = 0;
    while (pos < seg->rows) {
      const auto take =
        std::min<uint64_t>(seg->rows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector vec{type, STANDARD_VECTOR_SIZE,
                         duckdb::VectorDataInitialization::UNINITIALIZED};
      FillBatch(vec, shape, nullable, pos, take);
      cw.Append(pos, vec, take);
      pos += take;
    }
    w.Commit(seg->rows);
  }
  seg->reader =
    std::make_unique<irs::ColReader>(seg->dir, std::string{kSeg}, CsDb());
  seg->col = seg->reader->Column(kField);
  if (seg->col == nullptr) {
    std::fprintf(stderr, "BuildSeg: column missing\n");
    std::abort();
  }
  return seg;
}

const Seg& GetSeg(Shape shape, bool nullable) {
  static std::array<std::unique_ptr<Seg>, 4> cache;
  const int key = static_cast<int>(shape) * 2 + (nullable ? 1 : 0);
  if (!cache[key]) {
    cache[key] = BuildSeg(shape, nullable);
  }
  return *cache[key];
}

// Scattered, sorted (forward-only) doc ids: run length 1 => the sparse-gather
// degenerate shape. ~5% of rows.
const std::vector<uint64_t>& ScatteredRows() {
  static const std::vector<uint64_t> v = [] {
    std::vector<uint64_t> out;
    const uint64_t n = Rows();
    for (uint64_t r = 0; r < n; r += 37) {
      out.push_back(r);
    }
    return out;
  }();
  return v;
}

// Non-contiguous DocIds duck-type (no contiguous_range_tag) => routes through
// the run-coalescing gather path, not the IotaRange fast path.
struct Rows2 {
  const uint64_t* p;
  size_t n;
  size_t size() const noexcept { return n; }
  uint64_t operator[](size_t i) const noexcept { return p[i]; }
};

void IrsFullScan(benchmark::State& state, Shape shape, bool nullable) {
  const auto& seg = GetSeg(shape, nullable);
  const auto type = TypeOf(shape);
  irs::ReadContext ctx{*seg.reader};
  for (auto _ : state) {
    state.PauseTiming();
    auto st = seg.col->InitScan(ctx);
    duckdb::Vector batch{type, STANDARD_VECTOR_SIZE,
                         duckdb::VectorDataInitialization::UNINITIALIZED};
    state.ResumeTiming();
    uint64_t pos = 0;
    while (pos < seg.rows) {
      const auto take =
        std::min<uint64_t>(seg.rows - pos, STANDARD_VECTOR_SIZE);
      seg.col->Scan(st, batch, take);
      benchmark::DoNotOptimize(batch);
      pos += take;
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(seg.rows));
}

void IrsSparseGather(benchmark::State& state, Shape shape, bool nullable) {
  const auto& seg = GetSeg(shape, nullable);
  const auto type = TypeOf(shape);
  const auto& rows = ScatteredRows();
  irs::ReadContext ctx{*seg.reader};
  for (auto _ : state) {
    state.PauseTiming();
    auto st = seg.col->InitScan(ctx);
    duckdb::Vector batch{type, STANDARD_VECTOR_SIZE,
                         duckdb::VectorDataInitialization::UNINITIALIZED};
    state.ResumeTiming();
    size_t i = 0;
    while (i < rows.size()) {
      const auto take = std::min<size_t>(rows.size() - i, STANDARD_VECTOR_SIZE);
      const Rows2 sub{&rows[i], take};
      seg.col->Gather(st, sub, batch, /*out_offset=*/0);
      benchmark::DoNotOptimize(batch);
      i += take;
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(rows.size()));
}

void IrsPointLookup(benchmark::State& state, Shape shape, bool nullable) {
  const auto& seg = GetSeg(shape, nullable);
  const auto type = TypeOf(shape);
  const auto& rows = ScatteredRows();
  for (auto _ : state) {
    state.PauseTiming();
    irs::ColumnReader::PointReader cursor{*seg.reader, *seg.col};
    duckdb::Vector out{type, 1};
    state.ResumeTiming();
    for (const auto r : rows) {
      duckdb::FlatVector::ValidityMutable(out).Reset();
      cursor.FetchRow(r, out, 0);
      benchmark::DoNotOptimize(out);
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(rows.size()));
}

void IrsWriteSeal(benchmark::State& state, Shape shape, bool nullable) {
  const auto type = TypeOf(shape);
  const uint64_t n = Rows();
  for (auto _ : state) {
    irs::MemoryDirectory dir{};
    irs::ColWriter w{dir, kSeg, CsDb()};
    auto& cw = w.OpenColumn(kField, type);
    uint64_t pos = 0;
    while (pos < n) {
      const auto take = std::min<uint64_t>(n - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector vec{type, STANDARD_VECTOR_SIZE,
                         duckdb::VectorDataInitialization::UNINITIALIZED};
      FillBatch(vec, shape, nullable, pos, take);
      cw.Append(pos, vec, take);
      pos += take;
    }
    w.Commit(n);
    benchmark::DoNotOptimize(&dir);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(n));
}

// ---- native DuckDB side ----------------------------------------------------

const char* TableName(Shape shape, bool nullable) {
  if (shape == Shape::Bigint) {
    return nullable ? "bi_n" : "bi_v";
  }
  return nullable ? "vc_n" : "vc_v";
}

duckdb::Connection& NativeCon() {
  static duckdb::DuckDB db(nullptr);
  static std::unique_ptr<duckdb::Connection> con = [] {
    auto c = std::make_unique<duckdb::Connection>(db);
    c->Query("CREATE SCHEMA IF NOT EXISTS main");
    c->Query("SET search_path = 'main'");
    const auto n = std::to_string(Rows());
    auto mk = [&](const char* name, const std::string& expr) {
      auto r =
        c->Query("CREATE TABLE main." + std::string{name} + " AS SELECT " +
                 expr + " c FROM range(" + n + ") tbl(i)");
      if (r->HasError()) {
        std::fprintf(stderr, "native table %s failed: %s\n", name,
                     r->GetError().c_str());
        std::abort();
      }
    };
    mk("bi_v", "i::BIGINT");
    mk("bi_n", "(CASE WHEN i % 10 = 0 THEN NULL ELSE i END)::BIGINT");
    mk("vc_v", "('cat_' || (i % 50))::VARCHAR");
    mk("vc_n",
       "(CASE WHEN i % 10 = 0 THEN NULL ELSE ('cat_' || (i % 50)) "
       "END)::VARCHAR");
    return c;
  }();
  return *con;
}

uint64_t StreamDrain(duckdb::ClientContext& ctx, const std::string& sql) {
  auto pending = ctx.PendingQuery(sql, /*allow_stream_result=*/true);
  if (pending->HasError()) {
    std::fprintf(stderr, "pending: %s\n", pending->GetError().c_str());
    std::abort();
  }
  for (;;) {
    auto status = pending->ExecuteTask();
    if (duckdb::PendingQueryResult::IsResultReady(status)) {
      break;
    }
    if (status == duckdb::PendingExecutionResult::EXECUTION_ERROR) {
      std::fprintf(stderr, "execute: %s\n", pending->GetError().c_str());
      std::abort();
    }
    if (status == duckdb::PendingExecutionResult::NO_TASKS_AVAILABLE ||
        status == duckdb::PendingExecutionResult::BLOCKED) {
      pending->WaitForTask();
    }
  }
  auto result = pending->Execute();
  uint64_t rows = 0;
  for (;;) {
    auto chunk = result->Fetch();
    if (!chunk || chunk->size() == 0) {
      break;
    }
    rows += chunk->size();
    benchmark::DoNotOptimize(chunk->data.data());
  }
  return rows;
}

void DuckFullScan(benchmark::State& state, Shape shape, bool nullable) {
  auto& con = NativeCon();
  const std::string sql =
    "SELECT c FROM main." + std::string{TableName(shape, nullable)};
  // Single-threaded so it is comparable to the single-threaded iresearch scan.
  con.Query("SET threads = 1");
  uint64_t total = 0;
  for (auto _ : state) {
    total += StreamDrain(*con.context, sql);
  }
  benchmark::DoNotOptimize(total);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(Rows()));
}

}  // namespace

#define IRS_CASES(fn)                                            \
  BENCHMARK_CAPTURE(fn, bigint_valid, Shape::Bigint, false)      \
    ->Unit(benchmark::kMillisecond)                              \
    ->UseRealTime();                                             \
  BENCHMARK_CAPTURE(fn, bigint_null10, Shape::Bigint, true)      \
    ->Unit(benchmark::kMillisecond)                              \
    ->UseRealTime();                                             \
  BENCHMARK_CAPTURE(fn, vcdict_valid, Shape::VarcharDict, false) \
    ->Unit(benchmark::kMillisecond)                              \
    ->UseRealTime();                                             \
  BENCHMARK_CAPTURE(fn, vcdict_null10, Shape::VarcharDict, true) \
    ->Unit(benchmark::kMillisecond)                              \
    ->UseRealTime()

IRS_CASES(IrsFullScan);
IRS_CASES(IrsSparseGather);
IRS_CASES(IrsPointLookup);
IRS_CASES(IrsWriteSeal);
IRS_CASES(DuckFullScan);

int main(int argc, char** argv) {
  sdb::DuckDBEngine::Instance().Initialize();
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  sdb::DuckDBEngine::Instance().Shutdown();
  return 0;
}
