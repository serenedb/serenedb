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

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <duckdb.hpp>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <memory>
#include <random>
#include <string>
#include <vector>

namespace {

constexpr irs::field_id kField = 0;
constexpr std::string_view kSegName = "bench_seg";
constexpr uint64_t kWindow = STANDARD_VECTOR_SIZE;
constexpr uint64_t kTargetBytes = 48ULL << 20;

duckdb::DatabaseInstance& CsDb() {
  return irs::DuckDBEngine::Instance().instance();
}

struct Shape {
  const char* name;
  size_t length;
  const char* codec;
};

constexpr Shape kShapes[] = {
  {"key25", 0, "auto"},
  {"key25", 0, "zstd"},
  {"text64", 64, "auto"},
  {"text64", 64, "zstd"},
  {"text512", 512, "auto"},
  {"text512", 512, "zstd"},
  {"text4096", 4096, "auto"},
  {"text4096", 4096, "zstd"},
  {"text512", 512, "uncompressed"},
  {"text4096", 4096, "uncompressed"},
};

constexpr uint32_t kPercents[] = {1,  2,  5,  10, 20, 30, 40,
                                  50, 60, 70, 80, 90, 95, 100};

class Words {
 public:
  Words() {
    std::mt19937_64 rng{7};
    _words.reserve(5000);
    std::vector<double> weights;
    for (size_t r = 0; r < 5000; ++r) {
      std::string w(2 + rng() % 9, ' ');
      for (auto& c : w) {
        c = static_cast<char>('a' + rng() % 26);
      }
      _words.push_back(std::move(w));
      weights.push_back(1.0 / static_cast<double>(r + 1));
    }
    _pick = std::discrete_distribution<size_t>{weights.begin(), weights.end()};
  }

  std::string Text(std::mt19937_64& rng, size_t length) {
    std::string s;
    const auto target = length / 2 + rng() % (length + 1);
    while (s.size() < target) {
      if (!s.empty()) {
        s.push_back(' ');
      }
      s += _words[_pick(rng)];
    }
    return s;
  }

 private:
  std::vector<std::string> _words;
  std::discrete_distribution<size_t> _pick;
};

std::string Key(uint64_t i) {
  std::mt19937_64 rng{i};
  std::string s = "user-" + std::to_string((i * 7919) % 1000003) + "-";
  for (int k = 0; k < 12; ++k) {
    s.push_back("0123456789abcdef"[rng() % 16]);
  }
  return s;
}

struct Column {
  irs::MemoryDirectory dir{};
  std::unique_ptr<irs::ColReader> reader;
  std::unique_ptr<irs::ReadContext> ctx;
  const irs::ColumnReader* col = nullptr;
  uint64_t windows = 0;
};

std::unique_ptr<Column> Build(const Shape& shape) {
  auto c = std::make_unique<Column>();
  const auto avg = shape.length ? shape.length : 25;
  const auto rows = std::min<uint64_t>(
    64 * kWindow, std::max<uint64_t>(8 * kWindow, kTargetBytes / avg));
  c->windows = rows / kWindow;
  duckdb::Connection con{CsDb()};
  con.Query(std::string{"SET force_compression = '"} + shape.codec + "'");
  Words words;
  std::mt19937_64 rng{11};
  {
    irs::ColWriter writer{c->dir, kSegName, CsDb()};
    auto& cw = writer.OpenColumn(kField, duckdb::LogicalType::VARCHAR);
    for (uint64_t pos = 0; pos < c->windows * kWindow; pos += kWindow) {
      duckdb::Vector v{duckdb::LogicalType::VARCHAR, kWindow};
      auto* d = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v);
      for (uint64_t k = 0; k < kWindow; ++k) {
        d[k] = duckdb::StringVector::AddString(
          v, shape.length ? words.Text(rng, shape.length) : Key(pos + k));
      }
      duckdb::FlatVector::SetSize(v, kWindow);
      cw.Append(pos, v, kWindow);
    }
    writer.Commit(c->windows * kWindow);
  }
  con.Query("SET force_compression = 'auto'");
  c->reader =
    std::make_unique<irs::ColReader>(c->dir, std::string{kSegName}, CsDb());
  c->col = c->reader->Column(kField);
  if (!c->col) {
    std::fprintf(stderr, "Build(%s): column missing\n", shape.name);
    std::abort();
  }
  c->ctx = std::make_unique<irs::ReadContext>(*c->reader);
  const auto blocks = c->col->DataBlocks();
  std::fprintf(
    stderr, "shape %-9s %-4s rows=%llu blocks=%zu codec=%s\n", shape.name,
    shape.codec, static_cast<unsigned long long>(c->windows * kWindow),
    blocks.size(),
    duckdb::EnumUtil::ToChars<duckdb::CompressionType>(blocks[0].codec->type));
  return c;
}

const Column& Get(size_t shape) {
  static std::unique_ptr<Column> gColumns[std::size(kShapes)];
  if (!gColumns[shape]) {
    gColumns[shape] = Build(kShapes[shape]);
  }
  return *gColumns[shape];
}

struct Window {
  duckdb::SelectionVector sel{kWindow};
  duckdb::idx_t hits = 0;
  duckdb::idx_t span = 0;
};

std::vector<Window> Windows(uint64_t count, uint32_t percent, uint32_t run) {
  std::vector<Window> out(count);
  std::mt19937_64 rng{percent * 31 + run};
  for (auto& w : out) {
    for (duckdb::idx_t i = 0; i < kWindow;) {
      if (rng() % (100 * run) < percent) {
        for (uint32_t k = 0; k < run && i < kWindow; ++k, ++i) {
          w.sel.set_index(w.hits++, i);
        }
      } else {
        ++i;
      }
    }
    if (w.hits == 0) {
      w.sel.set_index(w.hits++, kWindow / 2);
    }
    w.span = w.sel.get_index(w.hits - 1) + 1;
  }
  return out;
}

void Bench(benchmark::State& state, size_t shape, bool dense) {
  const auto& c = Get(shape);
  const auto percent = static_cast<uint32_t>(state.range(0));
  const auto run = static_cast<uint32_t>(state.range(1));
  const auto windows = Windows(c.windows, percent, run);
  irs::ColumnReader::VectorScratch scratch{duckdb::LogicalType::VARCHAR};
  uint64_t bytes = 0;
  for (auto _ : state) {
    auto scan = c.col->InitScan(*c.ctx);
    for (uint64_t k = 0; k < windows.size(); ++k) {
      const auto& w = windows[k];
      auto& out = scratch.Reset();
      if (dense) {
        c.col->GatherDense(scan, k * kWindow, w.sel, w.hits, w.span, out);
      } else {
        c.col->GatherScatter(scan, k * kWindow, w.sel, w.hits, out, 0);
      }
      bytes += out.GetValue(0).ToString().size();
    }
  }
  benchmark::DoNotOptimize(bytes);
  uint64_t hits = 0;
  for (const auto& w : windows) {
    hits += w.hits;
  }
  state.counters["ns_per_hit"] = benchmark::Counter(
    static_cast<double>(hits), benchmark::Counter::kIsIterationInvariantRate |
                                 benchmark::Counter::kInvert);
}

void RegisterAll() {
  for (size_t s = 0; s < std::size(kShapes); ++s) {
    for (const bool dense : {true, false}) {
      const auto name = std::string{kShapes[s].name} + "/" + kShapes[s].codec +
                        "/" + (dense ? "dense" : "scatter");
      auto* b = benchmark::RegisterBenchmark(
        name.c_str(),
        [s, dense](benchmark::State& st) { Bench(st, s, dense); });
      for (const int64_t run : {1, 12}) {
        for (const auto p : kPercents) {
          b->Args({p, run});
        }
      }
      b->Unit(benchmark::kMicrosecond);
    }
  }
}

}  // namespace

static int Main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();
  RegisterAll();
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  irs::DuckDBEngine::Instance().Shutdown();
  return 0;
}

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
