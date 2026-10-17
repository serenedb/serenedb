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

#include <absl/strings/str_cat.h>
#include <benchmark/benchmark.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <fstream>
#include <iresearch/analysis/delimited_tokenizer.hpp>
#include <iresearch/analysis/keyword_tokenizer.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge/sort_spec.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <limits>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "insert_field.hpp"

extern "C" int JemallocControl(const char* name, void* old_value,
                               size_t* old_size, void* new_value,
                               size_t new_size) __asm__("mallctl")
  __attribute__((weak));

namespace {

constexpr irs::field_id kIdField = 1;
constexpr irs::field_id kServiceField = 2;
constexpr irs::field_id kBodyField = 3;
constexpr irs::field_id kService = 100;
constexpr irs::field_id kTimestamp = 101;
constexpr irs::field_id kBody = 102;
constexpr size_t kRows = 1'000'000;
constexpr size_t kServices = 40;

enum class Order : int64_t {
  Sorted,
  Clustered,
  Random,
};

struct Row {
  uint32_t service;
  int64_t timestamp;
  std::string id;
  std::string body;
};

constexpr std::array<std::string_view, 32> kWords{
  "request", "handled", "user",   "order",   "payment", "failed",  "retry",
  "cart",    "item",    "added",  "removed", "session", "expired", "token",
  "cache",   "miss",    "hit",    "latency", "slow",    "query",   "db",
  "timeout", "ok",      "status", "error",   "warn",    "info",    "debug",
  "span",    "trace",   "export", "batch"};

uint64_t Mix(uint64_t x) noexcept {
  x += 0x9E3779B97F4A7C15ULL;
  x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
  x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
  return x ^ (x >> 31);
}

std::string ServiceName(uint32_t service) {
  return absl::StrCat("service-", absl::Dec(service, absl::kZeroPad2));
}

const std::vector<Row>& RowsOf(Order order) {
  static std::map<Order, std::vector<Row>> gCache;
  auto& rows = gCache[order];
  if (!rows.empty()) {
    return rows;
  }
  rows.reserve(kRows);
  for (uint64_t i = 0; i != kRows; ++i) {
    auto& row = rows.emplace_back();
    const auto mixed = Mix(i);
    row.service = static_cast<uint32_t>(mixed % kServices);
    row.timestamp = static_cast<int64_t>(i * 1000 + (mixed >> 54));
    row.id = absl::StrCat(absl::Hex(Mix(mixed)));
    const auto words = 8 + (mixed >> 60);
    for (uint64_t w = 0; w != words; ++w) {
      if (w != 0) {
        row.body.push_back(' ');
      }
      row.body += kWords[Mix(mixed + w) % kWords.size()];
    }
  }
  if (order == Order::Sorted) {
    std::ranges::stable_sort(rows, [](const Row& lhs, const Row& rhs) {
      return lhs.service < rhs.service ||
             (lhs.service == rhs.service && lhs.timestamp < rhs.timestamp);
    });
  } else if (order == Order::Random) {
    std::mt19937_64 rng{42};
    std::ranges::shuffle(rows, rng);
  }
  return rows;
}

struct KeywordField {
  irs::field_id Id() const noexcept { return id; }

  irs::analysis::Tokenizer& GetTokens() const { return stream; }

  std::string_view Value() const noexcept { return value; }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq;
  }

  irs::field_id id{irs::field_limits::invalid()};
  std::string_view value;
  mutable irs::KeywordTokenizer stream;
};

struct BodyField {
  irs::field_id Id() const noexcept { return kBodyField; }

  irs::analysis::Tokenizer& GetTokens() const { return stream; }

  std::string_view Value() const noexcept { return value; }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
  }

  std::string_view value;
  mutable irs::analysis::DelimitedTokenizer stream{" "};
};

uint64_t StatusKb(std::string_view key) {
  std::ifstream status{"/proc/self/status"};
  std::string line;
  while (std::getline(status, line)) {
    if (line.starts_with(key)) {
      return std::stoull(line.substr(key.size()));
    }
  }
  return 0;
}

uint64_t ResetPeakKb() {
  if (JemallocControl != nullptr) {
    JemallocControl("thread.tcache.flush", nullptr, nullptr, nullptr, 0);
    JemallocControl("arena.4096.purge", nullptr, nullptr, nullptr, 0);
  }
  std::ofstream{"/proc/self/clear_refs"} << "5";
  return StatusKb("VmRSS:");
}

void AppendColumns(irs::ColWriter& columns, std::span<const Row> rows,
                   uint64_t first) {
  const auto n = static_cast<duckdb::idx_t>(rows.size());
  duckdb::Vector service{duckdb::LogicalType::VARCHAR, n};
  duckdb::Vector timestamp{duckdb::LogicalType::BIGINT, n};
  duckdb::Vector body{duckdb::LogicalType::VARCHAR, n};
  auto* services =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(service);
  auto* timestamps = duckdb::FlatVector::GetDataMutable<int64_t>(timestamp);
  auto* bodies = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(body);
  for (duckdb::idx_t i = 0; i != n; ++i) {
    services[i] =
      duckdb::StringVector::AddString(service, ServiceName(rows[i].service));
    timestamps[i] = rows[i].timestamp;
    bodies[i] = duckdb::StringVector::AddString(body, rows[i].body);
  }
  for (auto* vector : {&service, &timestamp, &body}) {
    duckdb::FlatVector::SetSize(*vector, n);
  }
  columns.OpenColumn(kService, duckdb::LogicalType::VARCHAR)
    .Append(first, service, n);
  columns.OpenColumn(kTimestamp, duckdb::LogicalType::BIGINT)
    .Append(first, timestamp, n);
  columns.OpenColumn(kBody, duckdb::LogicalType::VARCHAR)
    .Append(first, body, n);
}

void BM_Flush(benchmark::State& state) {
  const auto order = static_cast<Order>(state.range(0));
  const bool sorted = state.range(1) != 0;
  const auto& rows = RowsOf(order);
  const irs::SortSpec spec{{{.column = kService}, {.column = kTimestamp}}};
  uint64_t insert_nanos = 0;
  uint64_t flush_nanos = 0;
  uint64_t peak_kb = 0;
  uint64_t bytes = 0;
  for (auto _ : state) {
    state.PauseTiming();
    const auto base_kb = ResetPeakKb();
    state.ResumeTiming();
    irs::MemoryDirectory dir;
    auto* db = &irs::DuckDBEngine::Instance().instance();
    irs::IndexWriterOptions options;
    options.db = db;
    options.reader_options.db = db;
    options.segment_memory_max = std::numeric_limits<size_t>::max();
    options.column_options = [](irs::field_id) -> irs::ColumnOptions {
      return {};
    };
    if (sorted) {
      options.sort = spec;
    }
    auto writer =
      irs::IndexWriter::Make(dir, irs::kOmCreate, std::move(options));
    const auto begin = std::chrono::steady_clock::now();
    {
      auto trx = writer->GetBatch();
      for (size_t first = 0; first < rows.size();
           first += STANDARD_VECTOR_SIZE) {
        const auto block = std::span{rows}.subspan(
          first, std::min<size_t>(STANDARD_VECTOR_SIZE, rows.size() - first));
        irs::ColWriter* columns = nullptr;
        for (const auto& row : block) {
          auto doc = trx.Insert();
          const KeywordField id{.id = kIdField, .value = row.id};
          tests::InsertField(doc, id);
          const auto service_name = ServiceName(row.service);
          const KeywordField service{.id = kServiceField,
                                     .value = service_name};
          tests::InsertField(doc, service);
          const BodyField body{.value = row.body};
          tests::InsertField(doc, body);
          columns = doc.GetColWriter();
        }
        AppendColumns(*columns, block, first);
      }
      trx.Commit();
    }
    const auto inserted = std::chrono::steady_clock::now();
    writer->RefreshCommit();
    const auto flushed = std::chrono::steady_clock::now();
    insert_nanos += static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(inserted - begin)
        .count());
    flush_nanos += static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(flushed - inserted)
        .count());
    state.PauseTiming();
    peak_kb = std::max(peak_kb, StatusKb("VmHWM:") - base_kb);
    const auto snapshot = writer->GetSnapshot();
    if (snapshot.size() != 1 || snapshot.docs_count() != rows.size()) {
      state.SkipWithError("the flush lost rows");
      return;
    }
    bytes = snapshot[0].Meta().byte_size;
    state.ResumeTiming();
  }
  const auto per_iteration = [&](uint64_t nanos) {
    return static_cast<double>(nanos) / 1e6 /
           static_cast<double>(state.iterations());
  };
  state.SetItemsProcessed(state.iterations() * rows.size());
  state.counters["insert_ms"] = per_iteration(insert_nanos);
  state.counters["flush_ms"] = per_iteration(flush_nanos);
  state.counters["peak_mb"] = static_cast<double>(peak_kb) / 1024;
  state.counters["segment_mb"] = static_cast<double>(bytes) / (1 << 20);
}

BENCHMARK(BM_Flush)
  ->ArgsProduct({{0, 1, 2}, {0, 1}})
  ->ArgNames({"order", "sorted"})
  ->Unit(benchmark::kMillisecond)
  ->UseRealTime()
  ->Iterations(3);

int Main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  irs::DuckDBEngine::Instance().Shutdown();
  return 0;
}

[[maybe_unused]] const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);

}  // namespace
