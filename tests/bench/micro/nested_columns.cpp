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

#include <benchmark/benchmark.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <duckdb.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/column_writer.hpp>
#include <iresearch/formats/column/internal/gather_arms.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <memory>
#include <string>
#include <vector>

namespace {

enum class Shape : uint8_t {
  ListRepeated,
  ListUnique,
  MapRepeated,
  MapUnique,
  StructRepeated,
};

constexpr size_t kShapes = 5;
constexpr irs::field_id kField = 0;
constexpr std::string_view kSeg = "bench_seg";

uint64_t Rows() {
  static const uint64_t rows = [] {
    if (const char* e = std::getenv("SDB_BENCH_ROWS")) {
      return static_cast<uint64_t>(std::strtoull(e, nullptr, 10));
    }
    return static_cast<uint64_t>(1'000'000);
  }();
  return rows;
}

duckdb::DatabaseInstance& CsDb() {
  return irs::DuckDBEngine::Instance().instance();
}

std::string ValueSql(Shape shape) {
  switch (shape) {
    case Shape::ListRepeated:
      return "['service.' || (i % 50), 'env.' || (i % 7), 'zone.' || ((i // "
             "11) % 3)]";
    case Shape::ListUnique:
      return "['id.' || i, 'env.' || (i % 7), 'trace.' || ((i * 7919) % "
             "1000003)]";
    case Shape::MapRepeated:
      return "map_from_entries(list_transform(range(8), lambda k: {'key': "
             "'attribute.' || k, 'value': 'v' || ((i // 37) % 300) || '-' || "
             "k}))";
    case Shape::MapUnique:
      return "map_from_entries(list_transform(range(8), lambda k: {'key': "
             "'attribute.' || k, 'value': 'v' || i || '-' || k}))";
    case Shape::StructRepeated:
      return "{'tags': " + ValueSql(Shape::ListRepeated) +
             ", 'attrs': " + ValueSql(Shape::MapRepeated) + "}";
  }
  return {};
}

std::string EmptySql(Shape shape) {
  if (shape == Shape::StructRepeated) {
    return "{'tags': []::VARCHAR[], 'attrs': map([], [])::MAP(VARCHAR, "
           "VARCHAR)}";
  }
  if (shape == Shape::MapRepeated || shape == Shape::MapUnique) {
    return "map([], [])::MAP(VARCHAR, VARCHAR)";
  }
  return "[]::VARCHAR[]";
}

struct Data {
  duckdb::LogicalType type;
  std::vector<duckdb::unique_ptr<duckdb::DataChunk>> chunks;
};

const Data& GetData(Shape shape) {
  static std::array<std::unique_ptr<Data>, kShapes> cache;
  auto& slot = cache[static_cast<size_t>(shape)];
  if (slot) {
    return *slot;
  }
  slot = std::make_unique<Data>();
  duckdb::Connection con{CsDb()};
  auto result =
    con.Query("SELECT CASE WHEN i % 20 = 0 THEN NULL WHEN i % 13 = 0 THEN " +
              EmptySql(shape) + " ELSE " + ValueSql(shape) +
              " END FROM range(" + std::to_string(Rows()) + ") t(i)");
  if (result->HasError()) {
    std::fprintf(stderr, "data query failed: %s\n", result->GetError().c_str());
    std::abort();
  }
  slot->type = result->types[0];
  while (auto chunk = result->Fetch()) {
    if (chunk->size() == 0) {
      continue;
    }
    slot->chunks.emplace_back(std::move(chunk));
  }
  return *slot;
}

void Write(irs::Directory& dir, const Data& data, uint64_t gap = 0) {
  irs::ColWriter w{dir, kSeg, CsDb()};
  auto& cw = w.OpenColumn(kField, data.type);
  uint64_t row = 0;
  for (size_t i = 0; i < data.chunks.size(); ++i) {
    const auto& chunk = data.chunks[i];
    if (gap == 0) {
      cw.Append(chunk->data[0], chunk->size());
    } else {
      if (i % 3 == 0) {
        row += gap;
      }
      cw.Append(row, chunk->data[0], chunk->size());
    }
    row += chunk->size();
  }
  w.Commit(row);
}

struct Seg {
  irs::MemoryDirectory dir{};
  std::unique_ptr<irs::ColReader> reader;
  const irs::ColumnReader* col = nullptr;
};

const Seg& GetSeg(Shape shape) {
  static std::array<std::unique_ptr<Seg>, kShapes> cache;
  auto& slot = cache[static_cast<size_t>(shape)];
  if (slot) {
    return *slot;
  }
  slot = std::make_unique<Seg>();
  Write(slot->dir, GetData(shape));
  slot->reader =
    std::make_unique<irs::ColReader>(slot->dir, std::string{kSeg}, CsDb());
  slot->col = slot->reader->Column(kField);
  if (slot->col == nullptr) {
    std::fprintf(stderr, "column missing\n");
    std::abort();
  }
  return *slot;
}

uint64_t DirBytes(const irs::MemoryDirectory& dir) {
  uint64_t total = 0;
  dir.visit([&](std::string_view name) {
    uint64_t size = 0;
    if (dir.length(size, name)) {
      total += size;
    }
    return true;
  });
  return total;
}

const std::vector<uint64_t>& ScatteredRows() {
  static const std::vector<uint64_t> v = [] {
    std::vector<uint64_t> out;
    for (uint64_t r = 0; r < Rows(); r += 37) {
      out.emplace_back(r);
    }
    return out;
  }();
  return v;
}

struct RowSpan {
  const uint64_t* p;
  size_t n;
  size_t size() const noexcept { return n; }
  uint64_t operator[](size_t i) const noexcept { return p[i]; }
};

void WriteSealGap(benchmark::State& state, Shape shape, uint64_t gap) {
  const auto& data = GetData(shape);
  uint64_t bytes = 0;
  for (auto _ : state) {
    irs::MemoryDirectory dir{};
    Write(dir, data, gap);
    bytes = DirBytes(dir);
    benchmark::DoNotOptimize(&dir);
  }
  state.counters["bytes"] = static_cast<double>(bytes);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(Rows()));
}

void WriteSeal(benchmark::State& state, Shape shape) {
  WriteSealGap(state, shape, 0);
}

void WriteSealSparse(benchmark::State& state, Shape shape) {
  WriteSealGap(state, shape, 1500);
}

void FullScan(benchmark::State& state, Shape shape) {
  const auto& seg = GetSeg(shape);
  const auto rows = seg.col->RowCount();
  irs::ReadContext ctx{*seg.reader};
  for (auto _ : state) {
    auto st = seg.col->InitScan(ctx);
    uint64_t pos = 0;
    while (pos < rows) {
      duckdb::Vector batch{seg.col->Type(), STANDARD_VECTOR_SIZE};
      const auto take = std::min<uint64_t>(rows - pos, STANDARD_VECTOR_SIZE);
      seg.col->Scan(st, batch, take);
      benchmark::DoNotOptimize(batch);
      pos += take;
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(rows));
}

void Rewrite(benchmark::State& state, Shape shape) {
  const auto& seg = GetSeg(shape);
  const auto rows = seg.col->RowCount();
  irs::ReadContext ctx{*seg.reader};
  uint64_t bytes = 0;
  for (auto _ : state) {
    irs::MemoryDirectory dir{};
    irs::ColWriter w{dir, kSeg, CsDb()};
    auto& cw = w.OpenColumn(kField, seg.col->Type());
    auto st = seg.col->InitScan(ctx);
    uint64_t pos = 0;
    while (pos < rows) {
      duckdb::Vector batch{seg.col->Type(), STANDARD_VECTOR_SIZE};
      const auto take = std::min<uint64_t>(rows - pos, STANDARD_VECTOR_SIZE);
      seg.col->Scan(st, batch, take);
      cw.Append(batch, take);
      pos += take;
    }
    w.Commit(rows);
    bytes = DirBytes(dir);
    benchmark::DoNotOptimize(&dir);
  }
  state.counters["bytes"] = static_cast<double>(bytes);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(rows));
}

void SparseGather(benchmark::State& state, Shape shape) {
  const auto& seg = GetSeg(shape);
  const auto& rows = ScatteredRows();
  irs::ReadContext ctx{*seg.reader};
  for (auto _ : state) {
    auto st = seg.col->InitScan(ctx);
    size_t i = 0;
    while (i < rows.size()) {
      duckdb::Vector batch{seg.col->Type(), STANDARD_VECTOR_SIZE};
      const auto take = std::min<size_t>(rows.size() - i, STANDARD_VECTOR_SIZE);
      irs::column_internal::GatherRows(*seg.col, st, RowSpan{&rows[i], take},
                                       batch, 0);
      benchmark::DoNotOptimize(batch);
      i += take;
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(rows.size()));
}

}  // namespace

#define NESTED_CASES(fn)                                        \
  BENCHMARK_CAPTURE(fn, list_repeated, Shape::ListRepeated)     \
    ->Unit(benchmark::kMillisecond)                             \
    ->UseRealTime();                                            \
  BENCHMARK_CAPTURE(fn, list_unique, Shape::ListUnique)         \
    ->Unit(benchmark::kMillisecond)                             \
    ->UseRealTime();                                            \
  BENCHMARK_CAPTURE(fn, map_repeated, Shape::MapRepeated)       \
    ->Unit(benchmark::kMillisecond)                             \
    ->UseRealTime();                                            \
  BENCHMARK_CAPTURE(fn, map_unique, Shape::MapUnique)           \
    ->Unit(benchmark::kMillisecond)                             \
    ->UseRealTime();                                            \
  BENCHMARK_CAPTURE(fn, struct_repeated, Shape::StructRepeated) \
    ->Unit(benchmark::kMillisecond)                             \
    ->UseRealTime()

NESTED_CASES(WriteSeal);
NESTED_CASES(WriteSealSparse);
NESTED_CASES(Rewrite);
NESTED_CASES(FullScan);
NESTED_CASES(SparseGather);

int main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  irs::DuckDBEngine::Instance().Shutdown();
  return 0;
}
