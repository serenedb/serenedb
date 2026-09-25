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

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <duckdb.hpp>
#include <duckdb/common/types/vector_cache.hpp>
#include <iresearch/formats/column/codecs/fsst_codec.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/column_writer.hpp>
#include <iresearch/formats/column/internal/gather_arms.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace {

enum class Corpus {
  Dict,
  Text,
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
constexpr std::string_view kSeg = "bench_seg";

duckdb::DatabaseInstance& CsDb() {
  return irs::DuckDBEngine::Instance().instance();
}

struct Arm {
  const char* name;
  duckdb::CompressionType codec;
  uint8_t level;
  irs::AutoObjective objective = irs::AutoObjective::Balanced;
};

constexpr Arm kArms[] = {
  {"dict_fsst", duckdb::CompressionType::COMPRESSION_DICT_FSST, 0},
  {"dict_lz4", duckdb::CompressionType::COMPRESSION_DICT_LZ4, 0},
  {"dict_zstd1", duckdb::CompressionType::COMPRESSION_DICT_ZSTD, 1},
  {"dict_zstd3", duckdb::CompressionType::COMPRESSION_DICT_ZSTD, 3},
  {"lz4", duckdb::CompressionType::COMPRESSION_LZ4, 0},
  {"dict_zxc3", duckdb::CompressionType::COMPRESSION_DICT_ZXC, 3},
  {"zxc3", duckdb::CompressionType::COMPRESSION_ZXC, 3},
  {"fsst", duckdb::CompressionType::COMPRESSION_FSST, 0},
  {"auto", duckdb::CompressionType::COMPRESSION_AUTO, 0},
  {"auto_size", duckdb::CompressionType::COMPRESSION_AUTO, 0,
   irs::AutoObjective::Size},
  {"auto_speed", duckdb::CompressionType::COMPRESSION_AUTO, 0,
   irs::AutoObjective::Speed},
  {"zstd1", duckdb::CompressionType::COMPRESSION_ZSTD, 1},
  {"zstd3", duckdb::CompressionType::COMPRESSION_ZSTD, 3},
  {"uncompressed", duckdb::CompressionType::COMPRESSION_UNCOMPRESSED, 0},
};

std::string Value(Corpus corpus, uint64_t g) {
  if (corpus == Corpus::Dict) {
    return "https://example.org/section/" + std::to_string(g % 5000) +
           "/page?id=" + std::to_string(g % 97);
  }
  std::string s;
  s.reserve(160);
  for (int k = 0; k < 6; ++k) {
    s += "event " + std::to_string((g * 7919 + k * 104729) % 1000003) +
         " host-" + std::to_string(g % 1024) + " status ok; ";
  }
  return s;
}

void FillBatch(duckdb::Vector& vec, Corpus corpus, uint64_t base,
               uint64_t take) {
  duckdb::FlatVector::ValidityMutable(vec).Reset(STANDARD_VECTOR_SIZE);
  auto* d = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec);
  for (uint64_t k = 0; k < take; ++k) {
    d[k] = duckdb::StringVector::AddString(vec, Value(corpus, base + k));
  }
}

struct Seg {
  irs::MemoryDirectory dir{};
  std::unique_ptr<irs::ColReader> reader;
  const irs::ColumnReader* col = nullptr;
  uint64_t rows = 0;
  uint64_t bytes = 0;
};

uint64_t Build(irs::Directory& dir, Corpus corpus, const Arm& arm,
               uint64_t rows) {
  irs::ColWriter w{dir, kSeg, CsDb()};
  auto& cw = w.OpenColumn(kField, duckdb::LogicalType::VARCHAR,
                          /*skip_validity=*/false, DEFAULT_ROW_GROUP_SIZE,
                          arm.codec, /*hyperloglog=*/false,
                          irs::ColCodecParams{.compression_level = arm.level,
                                              .objective = arm.objective});
  uint64_t pos = 0;
  while (pos < rows) {
    const auto take = std::min<uint64_t>(rows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector vec{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE,
                       duckdb::VectorDataInitialization::UNINITIALIZED};
    FillBatch(vec, corpus, pos, take);
    cw.Append(pos, vec, take);
    pos += take;
  }
  w.Commit(rows);
  uint64_t bytes = 0;
  for (const auto& block : cw.Meta().data) {
    bytes += block.byte_size;
  }
  return bytes;
}

const Seg& GetSeg(Corpus corpus, size_t arm) {
  static std::map<std::pair<int, size_t>, std::unique_ptr<Seg>> cache;
  auto& slot = cache[{static_cast<int>(corpus), arm}];
  if (!slot) {
    slot = std::make_unique<Seg>();
    slot->rows = Rows();
    slot->bytes = Build(slot->dir, corpus, kArms[arm], slot->rows);
    slot->reader =
      std::make_unique<irs::ColReader>(slot->dir, std::string{kSeg}, CsDb());
    slot->col = slot->reader->Column(kField);
    if (slot->col == nullptr) {
      std::fprintf(stderr, "col_codecs: column missing\n");
      std::abort();
    }
  }
  return *slot;
}

const std::vector<uint64_t>& ScatteredRows() {
  static const std::vector<uint64_t> v = [] {
    std::vector<uint64_t> out;
    for (uint64_t r = 0; r < Rows(); r += 37) {
      out.push_back(r);
    }
    return out;
  }();
  return v;
}

struct Rows2 {
  const uint64_t* p;
  size_t n;
  size_t size() const noexcept { return n; }
  uint64_t operator[](size_t i) const noexcept { return p[i]; }
};

void Seal(benchmark::State& state, Corpus corpus, size_t arm) {
  const uint64_t n = Rows();
  uint64_t bytes = 0;
  for (auto _ : state) {
    irs::MemoryDirectory dir{};
    bytes = Build(dir, corpus, kArms[arm], n);
    benchmark::DoNotOptimize(&dir);
  }
  state.counters["bytes"] = static_cast<double>(bytes);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(n));
}

void FullScan(benchmark::State& state, Corpus corpus, size_t arm) {
  const auto& seg = GetSeg(corpus, arm);
  irs::ReadContext ctx{*seg.reader};
  for (auto _ : state) {
    state.PauseTiming();
    auto st = seg.col->InitScan(ctx);
    duckdb::VectorCache cache{duckdb::Allocator::DefaultAllocator(),
                              duckdb::LogicalType::VARCHAR,
                              STANDARD_VECTOR_SIZE};
    duckdb::Vector batch{cache};
    state.ResumeTiming();
    uint64_t pos = 0;
    while (pos < seg.rows) {
      const auto take =
        std::min<uint64_t>(seg.rows - pos, STANDARD_VECTOR_SIZE);
      batch.ResetFromCache(cache);
      seg.col->Scan(st, batch, take);
      benchmark::DoNotOptimize(batch);
      pos += take;
    }
  }
  state.counters["bytes"] = static_cast<double>(seg.bytes);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(seg.rows));
}

void SparseGather(benchmark::State& state, Corpus corpus, size_t arm) {
  const auto& seg = GetSeg(corpus, arm);
  const auto& rows = ScatteredRows();
  irs::ReadContext ctx{*seg.reader};
  for (auto _ : state) {
    state.PauseTiming();
    auto st = seg.col->InitScan(ctx);
    duckdb::VectorCache cache{duckdb::Allocator::DefaultAllocator(),
                              duckdb::LogicalType::VARCHAR,
                              STANDARD_VECTOR_SIZE};
    duckdb::Vector batch{cache};
    state.ResumeTiming();
    size_t i = 0;
    while (i < rows.size()) {
      const auto take = std::min<size_t>(rows.size() - i, STANDARD_VECTOR_SIZE);
      const Rows2 sub{&rows[i], take};
      batch.ResetFromCache(cache);
      irs::column_internal::GatherRows(*seg.col, st, sub, batch, 0);
      benchmark::DoNotOptimize(batch);
      i += take;
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(rows.size()));
}

void PointLookup(benchmark::State& state, Corpus corpus, size_t arm) {
  const auto& seg = GetSeg(corpus, arm);
  const auto& rows = ScatteredRows();
  for (auto _ : state) {
    state.PauseTiming();
    irs::ColumnReader::PointReader cursor{*seg.reader, *seg.col};
    duckdb::Vector out{duckdb::LogicalType::VARCHAR, 1};
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

struct FsstFrames {
  std::vector<std::string> strings;
  std::vector<std::string_view> views;
  std::string encoded;
  std::vector<uint32_t> lengths;
  irs::codecs::FsstEncoder encoder;
  irs::codecs::FsstDecoder decoder;
  size_t raw = 0;
  static constexpr size_t kPerFrame = 400;
  static constexpr size_t kFrames = 64;

  FsstFrames() {
    for (size_t i = 0; i < kPerFrame * kFrames; ++i) {
      strings.push_back(Value(Corpus::Dict, i));
      raw += strings.back().size();
    }
    for (const auto& s : strings) {
      views.emplace_back(s);
    }
    encoder.Encode(views, encoded, lengths);
    decoder.Import(encoder.SymbolTable());
  }
};

const FsstFrames& Frames() {
  static const FsstFrames frames;
  return frames;
}

void FsstDecodePerEntry(benchmark::State& state) {
  const auto& f = Frames();
  std::string out(f.raw + 64, '\0');
  for (auto _ : state) {
    size_t in = 0;
    size_t off = 0;
    for (size_t i = 0; i < f.lengths.size(); ++i) {
      off += f.decoder.Decode(f.encoded.data() + in, f.lengths[i],
                              out.data() + off, out.size() - off);
      in += f.lengths[i];
    }
    benchmark::DoNotOptimize(out.data());
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * f.raw));
}

void FsstDecodeBulk(benchmark::State& state) {
  const auto& f = Frames();
  std::string out(f.raw + 64, '\0');
  for (auto _ : state) {
    size_t in = 0;
    size_t off = 0;
    for (size_t fr = 0; fr < FsstFrames::kFrames; ++fr) {
      size_t comp = 0;
      for (size_t i = fr * FsstFrames::kPerFrame;
           i < (fr + 1) * FsstFrames::kPerFrame; ++i) {
        comp += f.lengths[i];
      }
      off += f.decoder.Decode(f.encoded.data() + in, comp, out.data() + off,
                              out.size() - off);
      in += comp;
    }
    benchmark::DoNotOptimize(out.data());
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * f.raw));
}

void FsstDecodeBulkCopy(benchmark::State& state) {
  const auto& f = Frames();
  std::string out(f.raw + 64, '\0');
  std::string scratch(f.raw + 64, '\0');
  for (auto _ : state) {
    size_t in = 0;
    size_t off = 0;
    for (size_t fr = 0; fr < FsstFrames::kFrames; ++fr) {
      size_t comp = 0;
      for (size_t i = fr * FsstFrames::kPerFrame;
           i < (fr + 1) * FsstFrames::kPerFrame; ++i) {
        comp += f.lengths[i];
      }
      f.decoder.Decode(f.encoded.data() + in, comp, scratch.data(),
                       scratch.size());
      in += comp;
      size_t soff = 0;
      for (size_t i = fr * FsstFrames::kPerFrame;
           i < (fr + 1) * FsstFrames::kPerFrame; ++i) {
        const auto len = f.strings[i].size();
        std::memcpy(out.data() + off, scratch.data() + soff, len);
        soff += len;
        off += len;
      }
    }
    benchmark::DoNotOptimize(out.data());
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * f.raw));
}

void Register() {
  benchmark::RegisterBenchmark("FsstDecode/per-entry", FsstDecodePerEntry)
    ->Unit(benchmark::kMicrosecond);
  benchmark::RegisterBenchmark("FsstDecode/bulk", FsstDecodeBulk)
    ->Unit(benchmark::kMicrosecond);
  benchmark::RegisterBenchmark("FsstDecode/bulk-copy", FsstDecodeBulkCopy)
    ->Unit(benchmark::kMicrosecond);
  for (const auto corpus : {Corpus::Dict, Corpus::Text}) {
    const std::string c = corpus == Corpus::Dict ? "dict" : "text";
    for (size_t arm = 0; arm < std::size(kArms); ++arm) {
      const std::string suffix = "/" + c + "/" + kArms[arm].name;
      benchmark::RegisterBenchmark(("Seal" + suffix).c_str(), Seal, corpus, arm)
        ->Unit(benchmark::kMillisecond);
      benchmark::RegisterBenchmark(("FullScan" + suffix).c_str(), FullScan,
                                   corpus, arm)
        ->Unit(benchmark::kMillisecond);
      benchmark::RegisterBenchmark(("SparseGather" + suffix).c_str(),
                                   SparseGather, corpus, arm)
        ->Unit(benchmark::kMillisecond);
      benchmark::RegisterBenchmark(("PointLookup" + suffix).c_str(),
                                   PointLookup, corpus, arm)
        ->Unit(benchmark::kMillisecond);
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();
  Register();
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  irs::DuckDBEngine::Instance().Shutdown();
  return 0;
}
