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

#include <absl/base/internal/endian.h>
#include <benchmark/benchmark.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <random>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/formats/norm_reader_impl.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/store/mmap_directory.hpp"
#include "iresearch/utils/duckdb_engine.hpp"
#include "iresearch/utils/resource_manager.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace {

constexpr size_t kBlock = irs::kPostingBlock;
constexpr size_t kBlocks = 4096;
constexpr uint32_t kRgRows = 122880;
constexpr uint32_t kEscape = 255;
constexpr size_t kSlack = 8;

enum class Shape : int {
  Game,
  Otel,
  OtelTail,
  Index,
};
enum class Layout : int {
  Main,
  Packed,
};
enum class Mode : int {
  Fetch,
  Score,
};

const std::vector<uint32_t>& IndexNorms() {
  static const std::vector<uint32_t> values = [] {
    std::vector<uint32_t> out;
    const char* path = std::getenv("NORM_INDEX");
    if (path == nullptr) {
      return out;
    }
    const char* field = std::getenv("NORM_FIELD");
    irs::MMapDirectory dir{path};
    irs::DirectoryReader reader{
      dir,
      irs::IndexReaderOptions{.db = &irs::DuckDBEngine::Instance().instance()}};
    for (const auto& segment : reader) {
      for (const auto id : segment.field_ids()) {
        if (field != nullptr && id != std::strtoul(field, nullptr, 10)) {
          continue;
        }
        const auto* terms = segment.field(id);
        if (terms == nullptr || !irs::field_limits::valid(terms->meta().norm)) {
          continue;
        }
        auto norms = segment.norms(terms->meta().norm);
        if (!norms) {
          continue;
        }
        const auto docs = static_cast<uint32_t>(segment.docs_count());
        for (uint32_t d = 0; d != docs; ++d) {
          out.push_back(norms->Get(irs::doc_limits::min() + d));
        }
      }
    }
    return out;
  }();
  return values;
}

std::vector<uint32_t> MakeValues(Shape shape, size_t rows) {
  if (shape == Shape::Index) {
    const auto& norms = IndexNorms();
    std::vector<uint32_t> values(rows);
    for (size_t r = 0; r != rows; ++r) {
      values[r] = norms[r % norms.size()];
    }
    return values;
  }
  std::mt19937_64 rng{static_cast<uint64_t>(shape) * 7919 + rows};
  const bool game = shape == Shape::Game;
  std::lognormal_distribution<double> dist{std::log(game ? 90.0 : 7.0),
                                           game ? 1.443 : 0.854};
  const uint32_t max = game ? 44543 : 137;
  std::vector<uint32_t> values(rows);
  for (auto& v : values) {
    v = std::clamp<uint32_t>(static_cast<uint32_t>(std::lround(dist(rng))), 1,
                             max);
  }
  if (shape == Shape::OtelTail) {
    std::uniform_int_distribution<size_t> pick{0, rows - 1};
    std::uniform_int_distribution<uint32_t> big{255, 469};
    for (size_t i = 0, n = rows * 11 / 10000; i != n; ++i) {
      values[pick(rng)] = big(rng);
    }
  }
  return values;
}

struct MainColumn {
  std::vector<uint8_t> data;
  std::vector<const uint8_t*> bases;
  std::vector<uint8_t> widths;
  std::vector<std::vector<std::pair<uint32_t, uint32_t>>> exceptions;
  size_t bytes = 0;
  uint32_t first = 1;
  uint32_t end = 1;
  size_t rg = 0;

  void Build(const std::vector<uint32_t>& values) {
    const size_t rgs = (values.size() + kRgRows - 1) / kRgRows;
    std::vector<uint64_t> offsets;
    exceptions.resize(rgs);
    for (size_t g = 0; g != rgs; ++g) {
      const size_t lo = g * kRgRows;
      const size_t rows = std::min<size_t>(kRgRows, values.size() - lo);
      uint32_t max = 0;
      size_t wide = 0;
      for (size_t r = 0; r != rows; ++r) {
        max = std::max(max, values[lo + r]);
        wide += values[lo + r] >= kEscape;
      }
      uint32_t width = max < 256 ? 1 : (max < 65536 ? 2 : 4);
      const bool escapes = width != 1 && wide * 256 <= rows;
      if (escapes) {
        width = 1;
      }
      widths.push_back(static_cast<uint8_t>(width));
      offsets.push_back(data.size());
      for (size_t r = 0; r != rows; ++r) {
        uint32_t v = values[lo + r];
        if (escapes && v >= kEscape) {
          exceptions[g].emplace_back(
            static_cast<uint32_t>(lo + r + irs::doc_limits::min()), v);
          v = kEscape;
        }
        const auto at = data.size();
        data.resize(at + width);
        std::memcpy(data.data() + at, &v, width);
      }
      bytes += rows * width + exceptions[g].size() * 8;
    }
    data.resize(data.size() + kSlack, 0);
    for (size_t g = 0; g != rgs; ++g) {
      bases.push_back(data.data() + offsets[g] -
                      (g * kRgRows + irs::doc_limits::min()) * widths[g]);
    }
  }

  size_t Bytes() const noexcept { return bytes; }

  void Enter(uint32_t doc) noexcept {
    rg = (doc - irs::doc_limits::min()) / kRgRows;
    first = static_cast<uint32_t>(rg * kRgRows + irs::doc_limits::min());
    end = first + kRgRows;
  }

  IRS_FORCE_INLINE void FetchRun(const uint32_t* IRS_RESTRICT docs,
                                 uint32_t* IRS_RESTRICT out,
                                 size_t n) const noexcept {
    const uint8_t* base = bases[rg];
    switch (widths[rg]) {
      case 1:
        for (size_t i = 0; i != n; ++i) {
          out[i] = base[docs[i]];
        }
        break;
      case 2:
        for (size_t i = 0; i != n; ++i) {
          out[i] = absl::little_endian::Load16(base + size_t{docs[i]} * 2);
        }
        break;
      default:
        for (size_t i = 0; i != n; ++i) {
          out[i] = absl::little_endian::Load32(base + size_t{docs[i]} * 4);
        }
    }
    const auto& list = exceptions[rg];
    if (list.empty()) {
      return;
    }
    bool escaped = false;
    for (size_t i = 0; i != n; ++i) {
      escaped |= out[i] == kEscape;
    }
    if (escaped) [[unlikely]] {
      for (size_t i = 0; i != n; ++i) {
        if (out[i] == kEscape) {
          out[i] = std::lower_bound(list.begin(), list.end(),
                                    std::pair{docs[i], uint32_t{0}})
                     ->second;
        }
      }
    }
  }

  IRS_FORCE_INLINE void Fetch(const uint32_t* IRS_RESTRICT docs,
                              uint32_t* IRS_RESTRICT out) noexcept {
    if (docs[0] >= first && docs[kBlock - 1] < end) [[likely]] {
      return FetchRun(docs, out, kBlock);
    }
    for (size_t i = 0; i != kBlock;) {
      if (docs[i] < first || docs[i] >= end) {
        Enter(docs[i]);
      }
      size_t j = i + 1;
      while (j != kBlock && docs[j] < end) {
        ++j;
      }
      FetchRun(docs + i, out + i, j - i);
      i = j;
    }
  }
};

struct PackedColumn {
  irs::MemoryFile file{irs::IResourceManager::gNoop};
  std::unique_ptr<irs::MemoryIndexInput> in;
  std::unique_ptr<irs::NormColumnReader> column;
  irs::NormReader::ptr reader;
  size_t bytes = 0;

  void Build(const std::vector<uint32_t>& values) {
    irs::MemoryIndexOutput out{file};
    irs::NormColumnWriter writer{1, kRgRows, out};
    writer.AppendValues(0, values);
    writer.Finalize();
    out.Flush();
    bytes = writer.Meta().size;
    in = std::make_unique<irs::MemoryIndexInput>(file);
    column = std::make_unique<irs::NormColumnReader>(1, writer.Meta(), *in);
    reader = irs::MakePersistedNormReader(*column);
  }

  size_t Bytes() const noexcept { return bytes; }

  IRS_FORCE_INLINE void Fetch(const uint32_t* IRS_RESTRICT docs,
                              uint32_t* IRS_RESTRICT out) noexcept {
    reader->GetPostingBlock(
      std::span<const irs::doc_id_t, kBlock>{docs, kBlock},
      std::span<uint32_t, kBlock>{out, kBlock});
  }
};

template<typename Column>
struct Lazy {
  Column column;
  bool built = false;

  Column& Get(const std::vector<uint32_t>& values) {
    if (!built) {
      column.Build(values);
      built = true;
    }
    return column;
  }
};

struct Fixture {
  std::vector<uint32_t> values;
  Lazy<MainColumn> main;
  Lazy<PackedColumn> packed;
  std::map<uint32_t, std::vector<uint32_t>> blocks;

  const std::vector<uint32_t>& Blocks(uint32_t span) {
    auto& docs = blocks[span];
    if (!docs.empty()) {
      return docs;
    }
    const auto rows = static_cast<uint32_t>(values.size());
    std::mt19937_64 rng{uint64_t{span} * 104729 + rows};
    std::uniform_int_distribution<uint32_t> start{0, rows - span};
    std::vector<uint32_t> starts(kBlocks);
    for (auto& s : starts) {
      s = start(rng);
    }
    std::sort(starts.begin(), starts.end());
    docs.resize(kBlocks * kBlock);
    std::vector<uint32_t> pool(span);
    for (size_t b = 0; b != kBlocks; ++b) {
      auto* out = docs.data() + b * kBlock;
      if (span == kBlock) {
        for (uint32_t i = 0; i != kBlock; ++i) {
          out[i] = starts[b] + i;
        }
      } else if (span <= 65536) {
        for (uint32_t i = 0; i != span; ++i) {
          pool[i] = i;
        }
        for (uint32_t i = 0; i != kBlock; ++i) {
          std::uniform_int_distribution<uint32_t> pick{i, span - 1};
          std::swap(pool[i], pool[pick(rng)]);
          out[i] = starts[b] + pool[i];
        }
      } else {
        std::uniform_int_distribution<uint32_t> pick{0, span - 1};
        size_t n = 0;
        while (n != kBlock) {
          out[n++] = starts[b] + pick(rng);
          if (n == kBlock) {
            std::sort(out, out + kBlock);
            n = static_cast<size_t>(std::unique(out, out + kBlock) - out);
          }
        }
      }
      std::sort(out, out + kBlock);
      for (uint32_t i = 0; i != kBlock; ++i) {
        out[i] += irs::doc_limits::min();
      }
    }
    return docs;
  }
};

Fixture& GetFixture(Shape shape, uint32_t rows) {
  static std::map<std::pair<int, uint32_t>, std::unique_ptr<Fixture>> cache;
  auto& f = cache[{static_cast<int>(shape), rows}];
  if (!f) {
    f = std::make_unique<Fixture>();
    f->values = MakeValues(shape, rows);
  }
  return *f;
}

IRS_FORCE_INLINE float Score(const uint32_t* IRS_RESTRICT norms,
                             const float* IRS_RESTRICT freqs,
                             float avg) noexcept {
  constexpr float kK1 = 1.2f;
  constexpr float kB = 0.75f;
  float sum = 0;
  for (size_t i = 0; i != kBlock; ++i) {
    const float f = freqs[i];
    sum += f / (f + kK1 * (1 - kB + kB * static_cast<float>(norms[i]) / avg));
  }
  return sum;
}

template<Mode M, typename Column>
void Run(benchmark::State& state, Column& column,
         const std::vector<uint32_t>& docs) {
  alignas(64) uint32_t out[kBlock];
  alignas(64) float freqs[kBlock];
  for (size_t i = 0; i != kBlock; ++i) {
    freqs[i] = static_cast<float>(1 + i % 4);
  }
  size_t b = 0;
  for (auto _ : state) {
    column.Fetch(docs.data() + b * kBlock, out);
    if constexpr (M == Mode::Score) {
      benchmark::DoNotOptimize(Score(out, freqs, 60.f));
    } else {
      benchmark::DoNotOptimize(out);
      benchmark::ClobberMemory();
    }
    if (++b == kBlocks) {
      b = 0;
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kBlock);
}

template<typename Column>
bool Check(Column& column, const Fixture& f,
           const std::vector<uint32_t>& docs) {
  alignas(64) uint32_t out[kBlock];
  for (size_t b = 0; b < kBlocks; b += 97) {
    const auto* block = docs.data() + b * kBlock;
    column.Fetch(block, out);
    for (size_t i = 0; i != kBlock; ++i) {
      if (out[i] != f.values[block[i] - irs::doc_limits::min()]) {
        return false;
      }
    }
  }
  return true;
}

template<Mode M>
void Bench(benchmark::State& state) {
  const auto shape = static_cast<Shape>(state.range(0));
  const auto layout = static_cast<Layout>(state.range(1));
  const auto rows = static_cast<uint32_t>(state.range(2));
  const auto span = static_cast<uint32_t>(state.range(3));
  auto& f = GetFixture(shape, rows);
  const auto& docs = f.Blocks(span);
  const auto dispatch = [&](auto& column) {
    if (!Check(column, f, docs)) {
      state.SkipWithError("mismatch");
      return;
    }
    Run<M>(state, column, docs);
    state.counters["bits_per_row"] =
      static_cast<double>(column.Bytes()) * 8 / static_cast<double>(rows);
  };
  switch (layout) {
    case Layout::Main:
      return dispatch(f.main.Get(f.values));
    case Layout::Packed:
      return dispatch(f.packed.Get(f.values));
  }
}

void Args(benchmark::internal::Benchmark* b) {
  for (int shape : {0, 1, 2, 3}) {
    if (shape == 3 && std::getenv("NORM_INDEX") == nullptr) {
      continue;
    }
    for (int layout : {0, 1}) {
      for (int64_t rows :
           {int64_t{1} << 20, int64_t{1} << 24, int64_t{1} << 28}) {
        for (int64_t span : {256, 4096, 32768, 262144}) {
          b->Args({shape, layout, rows, span});
        }
      }
    }
  }
  b->ArgNames({"shape", "layout", "rows", "span"});
}

BENCHMARK(Bench<Mode::Fetch>)->Apply(Args);
BENCHMARK(Bench<Mode::Score>)->Apply(Args);

}  // namespace

int main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  irs::DuckDBEngine::Instance().Shutdown();
  return 0;
}
