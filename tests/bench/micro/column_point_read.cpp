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
#include <array>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <duckdb.hpp>
#include <filesystem>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/column_writer.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <memory>
#include <random>
#include <string>
#include <vector>

namespace {

enum class Codec {
  BitpackingFor,
  BitpackingDeltaFor,
  BitpackingConstantDelta,
  Rle,
  Alp,
  AlpRd,
  DictFsst,
  Zstd,
  Uncompressed,
  NullsSparse,
  NullsDense,
  kCount,
};

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
constexpr std::string_view kSeg = "point_seg";
constexpr size_t kSample = 1 << 17;
constexpr size_t kColdRows = 2048;

duckdb::LogicalType TypeOf(Codec c) {
  switch (c) {
    case Codec::Alp:
    case Codec::AlpRd:
      return duckdb::LogicalType::DOUBLE;
    case Codec::DictFsst:
    case Codec::Zstd:
      return duckdb::LogicalType::VARCHAR;
    default:
      return duckdb::LogicalType::BIGINT;
  }
}

duckdb::CompressionType CompressionOf(Codec c) {
  switch (c) {
    case Codec::BitpackingFor:
    case Codec::BitpackingDeltaFor:
    case Codec::BitpackingConstantDelta:
    case Codec::NullsSparse:
    case Codec::NullsDense:
      return duckdb::CompressionType::COMPRESSION_BITPACKING;
    case Codec::Rle:
      return duckdb::CompressionType::COMPRESSION_RLE;
    case Codec::Alp:
      return duckdb::CompressionType::COMPRESSION_ALP;
    case Codec::AlpRd:
      return duckdb::CompressionType::COMPRESSION_ALPRD;
    case Codec::DictFsst:
      return duckdb::CompressionType::COMPRESSION_DICT_FSST;
    case Codec::Zstd:
      return duckdb::CompressionType::COMPRESSION_ZSTD;
    case Codec::Uncompressed:
    case Codec::kCount:
      return duckdb::CompressionType::COMPRESSION_UNCOMPRESSED;
  }
  return duckdb::CompressionType::COMPRESSION_AUTO;
}

uint64_t Mix(uint64_t x) {
  x ^= x >> 33;
  x *= 0xff51afd7ed558ccdULL;
  x ^= x >> 33;
  x *= 0xc4ceb9fe1a85ec53ULL;
  x ^= x >> 33;
  return x;
}

struct Generator {
  Codec codec;
  int64_t running = 0;
  int64_t run_value = 0;
  uint64_t run_left = 0;

  bool IsNull(uint64_t g) const {
    if (codec == Codec::NullsSparse) {
      return Mix(g) % 100 == 0;
    }
    if (codec == Codec::NullsDense) {
      return Mix(g) % 100 < 30;
    }
    return false;
  }

  void Fill(duckdb::Vector& vec, uint64_t base, uint64_t take) {
    auto& validity = duckdb::FlatVector::ValidityMutable(vec);
    validity.Reset(STANDARD_VECTOR_SIZE);
    switch (codec) {
      case Codec::Alp:
      case Codec::AlpRd: {
        auto* d = duckdb::FlatVector::GetDataMutable<double>(vec);
        for (uint64_t k = 0; k < take; ++k) {
          const auto g = base + k;
          if (codec == Codec::Alp) {
            d[k] = g % 97 == 0 ? M_PI * static_cast<double>(g)
                               : static_cast<double>(Mix(g) % 100000) / 8.0;
          } else {
            d[k] = std::sin(static_cast<double>(g));
          }
        }
        return;
      }
      case Codec::DictFsst:
      case Codec::Zstd: {
        auto* d = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec);
        for (uint64_t k = 0; k < take; ++k) {
          const auto g = base + k;
          std::string str;
          if (codec == Codec::DictFsst) {
            str = "language_" + std::to_string(Mix(g) % 30);
          } else {
            const auto h = Mix(g);
            str = "document title " + std::to_string(h % 1000) +
                  " about topic " + std::to_string((h >> 20) % 5000) +
                  " in section " + std::to_string((h >> 40) % 300);
          }
          d[k] = duckdb::StringVector::AddString(vec, str);
        }
        return;
      }
      default: {
        auto* d = duckdb::FlatVector::GetDataMutable<int64_t>(vec);
        for (uint64_t k = 0; k < take; ++k) {
          const auto g = base + k;
          if (IsNull(g)) {
            validity.SetInvalid(k);
            continue;
          }
          switch (codec) {
            case Codec::BitpackingDeltaFor:
              running += static_cast<int64_t>(Mix(g) % 1000);
              d[k] = running;
              break;
            case Codec::BitpackingConstantDelta:
              d[k] = static_cast<int64_t>(g) * 3;
              break;
            case Codec::Rle:
              if (run_left == 0) {
                run_left = 1 + Mix(g) % 50;
                run_value = static_cast<int64_t>(Mix(g + 1) % 1000);
              }
              --run_left;
              d[k] = run_value;
              break;
            default:
              d[k] = static_cast<int64_t>(Mix(g) % 1'000'000);
              break;
          }
        }
        return;
      }
    }
  }
};

duckdb::DatabaseInstance& CsDb() {
  return irs::DuckDBEngine::Instance().instance();
}

std::filesystem::path BenchDir(Codec codec) {
  std::filesystem::path root = std::filesystem::temp_directory_path();
  if (const char* e = std::getenv("SDB_BENCH_DIR")) {
    root = e;
  }
  auto dir =
    root / ("column_point_read_" + std::to_string(static_cast<int>(codec)));
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  return dir;
}

struct Seg {
  explicit Seg(Codec codec) : dir{BenchDir(codec)} {}

  irs::MMapDirectory dir;
  std::unique_ptr<irs::ColReader> reader;
  const irs::ColumnReader* col = nullptr;
};

std::unique_ptr<Seg> BuildSeg(Codec codec) {
  auto seg = std::make_unique<Seg>(codec);
  const auto rows = Rows();
  const auto type = TypeOf(codec);
  {
    irs::ColWriter w{seg->dir, kSeg, CsDb()};
    auto& cw = w.OpenColumn(kField, type, false, DEFAULT_ROW_GROUP_SIZE,
                            CompressionOf(codec));
    Generator gen{codec};
    uint64_t pos = 0;
    while (pos < rows) {
      const auto take = std::min<uint64_t>(rows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector vec{type, STANDARD_VECTOR_SIZE,
                         duckdb::VectorDataInitialization::UNINITIALIZED};
      gen.Fill(vec, pos, take);
      cw.Append(pos, vec, take);
      pos += take;
    }
    w.Commit(rows);
  }
  seg->reader =
    std::make_unique<irs::ColReader>(seg->dir, std::string{kSeg}, CsDb());
  seg->col = seg->reader->Column(kField);
  if (seg->col == nullptr) {
    std::fprintf(stderr, "BuildSeg: column missing\n");
    std::abort();
  }
  std::string codecs;
  for (const auto& block : seg->col->DataBlocks()) {
    codecs += duckdb::CompressionTypeToString(block.codec->type) + " ";
  }
  if (const auto* validity = seg->col->Validity()) {
    codecs += "| validity ";
    for (const auto& block : validity->DataBlocks()) {
      codecs += duckdb::CompressionTypeToString(block.codec->type) + " ";
    }
  }
  std::fprintf(stderr, "codec %d: %s\n", static_cast<int>(codec),
               codecs.c_str());
  return seg;
}

const Seg& GetSeg(Codec codec) {
  static std::array<std::unique_ptr<Seg>, static_cast<size_t>(Codec::kCount)>
    cache;
  auto& seg = cache[static_cast<size_t>(codec)];
  if (!seg) {
    seg = BuildSeg(codec);
  }
  return *seg;
}

const std::vector<uint64_t>& RandomRows() {
  static const std::vector<uint64_t> rows = [] {
    std::vector<uint64_t> out(kSample);
    std::mt19937_64 rng{42};
    for (auto& r : out) {
      r = rng() % Rows();
    }
    return out;
  }();
  return rows;
}

const std::vector<uint64_t>& StrideRows() {
  static const std::vector<uint64_t> rows = [] {
    std::vector<uint64_t> out;
    for (uint64_t r = 0; r < Rows() && out.size() < kSample; r += 37) {
      out.push_back(r);
    }
    return out;
  }();
  return rows;
}

const std::vector<uint64_t>& DenseRows() {
  static const std::vector<uint64_t> rows = [] {
    std::vector<uint64_t> out;
    for (uint64_t r = Rows() / 3; r < Rows() && out.size() < kSample; ++r) {
      out.push_back(r);
    }
    return out;
  }();
  return rows;
}

void FetchAll(irs::ColumnReader::PointReader& reader,
              const std::vector<uint64_t>& rows, size_t begin, size_t end,
              duckdb::Vector& out) {
  for (size_t i = begin; i < end; ++i) {
    duckdb::FlatVector::ValidityMutable(out).Reset();
    benchmark::DoNotOptimize(reader.FetchRow(rows[i], out, 0));
    if (out.GetType().InternalType() == duckdb::PhysicalType::VARCHAR &&
        (i & 1023) == 0) {
      duckdb::StringVector::GetStringAllocator(out).Reset();
    }
  }
}

void Warm(benchmark::State& state, Codec codec,
          const std::vector<uint64_t>& (*rows_of)()) {
  const auto& seg = GetSeg(codec);
  const auto& rows = rows_of();
  irs::ColumnReader::PointReader reader{*seg.reader, *seg.col};
  duckdb::Vector out{TypeOf(codec), 1};
  FetchAll(reader, rows, 0, rows.size(), out);
  for (auto _ : state) {
    FetchAll(reader, rows, 0, rows.size(), out);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(rows.size()));
}

void Cold(benchmark::State& state, Codec codec) {
  const auto& seg = GetSeg(codec);
  const auto& rows = RandomRows();
  duckdb::Vector out{TypeOf(codec), 1};
  size_t begin = 0;
  for (auto _ : state) {
    irs::ColumnReader::PointReader reader{*seg.reader, *seg.col};
    FetchAll(reader, rows, begin, begin + kColdRows, out);
    begin = (begin + kColdRows) % (rows.size() - kColdRows);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(kColdRows));
}

void RandomWarm(benchmark::State& state, Codec codec) {
  Warm(state, codec, &RandomRows);
}

void StrideWarm(benchmark::State& state, Codec codec) {
  Warm(state, codec, &StrideRows);
}

void DenseWarm(benchmark::State& state, Codec codec) {
  Warm(state, codec, &DenseRows);
}

}  // namespace

#define POINT_CASES(fn)                                                  \
  BENCHMARK_CAPTURE(fn, for, Codec::BitpackingFor);                      \
  BENCHMARK_CAPTURE(fn, delta_for, Codec::BitpackingDeltaFor);           \
  BENCHMARK_CAPTURE(fn, constant_delta, Codec::BitpackingConstantDelta); \
  BENCHMARK_CAPTURE(fn, rle, Codec::Rle);                                \
  BENCHMARK_CAPTURE(fn, alp, Codec::Alp);                                \
  BENCHMARK_CAPTURE(fn, alprd, Codec::AlpRd);                            \
  BENCHMARK_CAPTURE(fn, dict_fsst, Codec::DictFsst);                     \
  BENCHMARK_CAPTURE(fn, zstd, Codec::Zstd);                              \
  BENCHMARK_CAPTURE(fn, uncompressed, Codec::Uncompressed);              \
  BENCHMARK_CAPTURE(fn, nulls_1pct, Codec::NullsSparse);                 \
  BENCHMARK_CAPTURE(fn, nulls_30pct, Codec::NullsDense)

POINT_CASES(RandomWarm);
POINT_CASES(Cold);
POINT_CASES(StrideWarm);
POINT_CASES(DenseWarm);

static int Main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  irs::DuckDBEngine::Instance().Shutdown();
  return 0;
}

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
