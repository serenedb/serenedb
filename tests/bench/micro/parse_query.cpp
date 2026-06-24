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

// Microbenchmark: isolate the per-query SQL parse + bind cost.
//
// The wire bench showed simple/extended SELECT 1 at ~597k cyc/query vs ~197k
// for prepared -- a ~400k cyc/query gap that is entirely parse + bind (prepared
// pays it once, simple/extended every time). Postgres has no such gap. Before
// deciding the fix (parse cache vs grammar/matcher fix vs arena alloc) we must
// know:
//
//   1. ABSOLUTE cost of parsing "SELECT 1" (~8 tokens). At ~3 GHz, 400k cyc is
//      ~130us; if a bare parse already costs that, the parser has huge per-call
//      fixed overhead (setup/keyword tables) -- reuse/cache territory. If it is
//      a few us, most of the 400k is bind/plan, not the PEG parse.
//   2. PARSE vs BIND split: ExtractStatements (parse only) vs Prepare
//      (parse+bind+plan), the two calls the simple-query path makes per
//      statement.
//   3. SCALING with query length: parse "SELECT 1, 1, ... (N copies)". Linear
//   in N
//      => per-token cost, no pathology. Super-linear (N^2) => PEG backtracking
//      blowup (memoize/packrat or fix the offending rule). This is the decisive
//      grammar-blowup-vs-fixed-cost test.
//
// Mirrors the server's parse path: Connection::ExtractStatements then Prepare
// (pg_wire_session RunSimpleQuery / pg_comm_task). Uses a plain in-memory
// DuckDB connection -- parsing SELECT-style literals needs no SereneDB catalog
// state, and the patched grammar is compiled into the linked duckdb.
//
// Flat profile of a single case, e.g.:
//   perf record -g --call-graph fp -- \
//     ./build_perf/tests/bench/micro/serenedb-bench-micro-parse_query \
//     --benchmark_filter='parse_select1' --benchmark_min_time=5s
//   perf report --no-children --stdio | head -40

#include <benchmark/benchmark.h>

#include <duckdb.hpp>
#include <string>

namespace {

duckdb::DuckDB& Db() {
  static duckdb::DuckDB db(nullptr);
  return db;
}

// "SELECT 1, 1, ... " with n projection items -- a token count that grows
// linearly in n while keeping the grammar shape trivial, so parse-time growth
// isolates the per-token vs super-linear question.
std::string SelectN(int n) {
  std::string sql = "SELECT 1";
  for (int i = 1; i < n; ++i) {
    sql += ", 1";
  }
  return sql;
}

// Like SelectN but with distinct identifiers instead of constants -- exercises
// the identifier path (PG-default lowercasing into the arena) that number
// literals don't.
std::string SelectIdentsN(int n) {
  std::string sql = "SELECT c0";
  for (int i = 1; i < n; ++i) {
    sql += ", c" + std::to_string(i);
  }
  return sql;
}

constexpr const char* kRows =
  "SELECT i, i::text AS s, i::float8 AS f FROM generate_series(1,1000) g(i)";

// Parse only: PEG parse + AST build (Connection::ExtractStatements).
void Parse(benchmark::State& state, std::string sql) {
  duckdb::Connection con{Db()};
  // SereneDB lowercases unquoted identifiers by default
  // (preserve_identifier_case=false, like PG).
  con.Query("SET preserve_identifier_case = false;");
  for (auto _ : state) {
    auto stmts = con.ExtractStatements(sql);
    benchmark::DoNotOptimize(stmts.data());
  }
}

// Parse + bind + plan (ExtractStatements + Prepare) -- the full per-statement
// cost the simple/extended path pays before execution.
void Prepare(benchmark::State& state, std::string sql) {
  duckdb::Connection con{Db()};
  // SereneDB lowercases unquoted identifiers by default
  // (preserve_identifier_case=false, like PG).
  con.Query("SET preserve_identifier_case = false;");
  for (auto _ : state) {
    auto prepared = con.Prepare(sql);
    benchmark::DoNotOptimize(prepared.get());
  }
}

}  // namespace

// Length sweep: linear vs super-linear growth decides grammar blowup vs fixed
// cost.
BENCHMARK_CAPTURE(Parse, parse_select1, std::string{"SELECT 1"});
BENCHMARK_CAPTURE(Parse, parse_select_n4, SelectN(4));
BENCHMARK_CAPTURE(Parse, parse_select_n16, SelectN(16));
BENCHMARK_CAPTURE(Parse, parse_select_n64, SelectN(64));
BENCHMARK_CAPTURE(Parse, parse_select_n256, SelectN(256));
BENCHMARK_CAPTURE(Parse, parse_idents_n4, SelectIdentsN(4));
BENCHMARK_CAPTURE(Parse, parse_idents_n16, SelectIdentsN(16));
BENCHMARK_CAPTURE(Parse, parse_idents_n64, SelectIdentsN(64));
BENCHMARK_CAPTURE(Parse, parse_rows, std::string{kRows});

// Parse vs parse+bind+plan, same statements: the gap is bind/plan.
BENCHMARK_CAPTURE(Prepare, prepare_select1, std::string{"SELECT 1"});
BENCHMARK_CAPTURE(Prepare, prepare_rows, std::string{kRows});

BENCHMARK_MAIN();
