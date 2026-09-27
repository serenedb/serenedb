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
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <duckdb.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/common/types/vector_cache.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/main/appender.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/object_cache.hpp>
#include <duckdb/storage/storage_index.hpp>
#include <duckdb/storage/table/scan_state.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <filesystem>
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

uint64_t RequestedRows() {
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

struct Column {
  std::vector<std::unique_ptr<duckdb::Vector>> vectors;
  std::vector<uint64_t> counts;
  uint64_t rows = 0;
  uint64_t raw_bytes = 0;
};

const Column& Data() {
  static const Column column = [] {
    Column out;
    const char* path = std::getenv("SDB_BENCH_PARQUET");
    const char* name = std::getenv("SDB_BENCH_COLUMN");
    if (!path || !name) {
      std::fprintf(stderr, "set SDB_BENCH_PARQUET and SDB_BENCH_COLUMN\n");
      std::abort();
    }
    duckdb::Connection con{CsDb()};
    con.Query("SET threads = 1");
    auto result = con.Query(std::string{"SELECT \""} + name +
                            "\" FROM read_parquet('" + path + "') LIMIT " +
                            std::to_string(RequestedRows()));
    if (result->HasError()) {
      std::fprintf(stderr, "load: %s\n", result->GetError().c_str());
      std::abort();
    }
    if (result->types[0] != duckdb::LogicalType::VARCHAR) {
      std::fprintf(stderr, "column %s is %s, not VARCHAR\n", name,
                   result->types[0].ToString().c_str());
      std::abort();
    }
    while (auto chunk = result->Fetch()) {
      if (chunk->size() == 0) {
        break;
      }
      const auto count = chunk->size();
      auto vec = std::make_unique<duckdb::Vector>(duckdb::LogicalType::VARCHAR,
                                                  count);
      duckdb::VectorOperations::Copy(chunk->data[0], *vec, count, 0, 0);
      duckdb::UnifiedVectorFormat format;
      vec->ToUnifiedFormat(format);
      const auto* strings =
        duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(format);
      for (duckdb::idx_t i = 0; i < count; ++i) {
        const auto idx = format.sel->get_index(i);
        if (format.validity.RowIsValid(idx)) {
          out.raw_bytes += strings[idx].GetSize();
        }
      }
      out.counts.push_back(count);
      out.rows += count;
      out.vectors.push_back(std::move(vec));
    }
    return out;
  }();
  return column;
}

const std::vector<uint64_t>& ScatteredRows() {
  static const std::vector<uint64_t> rows = [] {
    std::vector<uint64_t> out;
    for (uint64_t r = 0; r < Data().rows; r += 37) {
      out.push_back(r);
    }
    return out;
  }();
  return rows;
}

struct Rows2 {
  const uint64_t* p;
  size_t n;
  size_t size() const noexcept { return n; }
  uint64_t operator[](size_t i) const noexcept { return p[i]; }
};

struct ColArm {
  const char* name;
  duckdb::CompressionType codec;
  uint8_t level;
  irs::AutoObjective objective = irs::AutoObjective::Balanced;
};

constexpr ColArm kColArms[] = {
  {"auto", duckdb::CompressionType::COMPRESSION_AUTO, 0},
  {"auto_speed", duckdb::CompressionType::COMPRESSION_AUTO, 0,
   irs::AutoObjective::Speed},
  {"auto_size", duckdb::CompressionType::COMPRESSION_AUTO, 0,
   irs::AutoObjective::Size},
  {"dict_fsst", duckdb::CompressionType::COMPRESSION_DICT_FSST, 0},
  {"fsst", duckdb::CompressionType::COMPRESSION_FSST, 0},
  {"dict_lz4", duckdb::CompressionType::COMPRESSION_DICT_LZ4, 0},
  {"dict_lz4_hc9", duckdb::CompressionType::COMPRESSION_DICT_LZ4, 9},
  {"lz4", duckdb::CompressionType::COMPRESSION_LZ4, 0},
  {"lz4_hc9", duckdb::CompressionType::COMPRESSION_LZ4, 9},
  {"dict_zstd1", duckdb::CompressionType::COMPRESSION_DICT_ZSTD, 1},
  {"dict_zstd3", duckdb::CompressionType::COMPRESSION_DICT_ZSTD, 3},
  {"dict_zstd9", duckdb::CompressionType::COMPRESSION_DICT_ZSTD, 9},
  {"zstd1", duckdb::CompressionType::COMPRESSION_ZSTD, 1},
  {"zstd3", duckdb::CompressionType::COMPRESSION_ZSTD, 3},
  {"dict_zxc3", duckdb::CompressionType::COMPRESSION_DICT_ZXC, 3},
  {"dict_zxc5", duckdb::CompressionType::COMPRESSION_DICT_ZXC, 5},
  {"zxc3", duckdb::CompressionType::COMPRESSION_ZXC, 3},
  {"zxc5", duckdb::CompressionType::COMPRESSION_ZXC, 5},
  {"uncompressed", duckdb::CompressionType::COMPRESSION_UNCOMPRESSED, 0},
};

struct ColSeg {
  irs::MemoryDirectory dir{};
  std::unique_ptr<irs::ColReader> reader;
  const irs::ColumnReader* col = nullptr;
  uint64_t bytes = 0;
};

uint64_t ColBuild(irs::Directory& dir, const ColArm& arm) {
  const auto& data = Data();
  irs::ColWriter w{dir, kSeg, CsDb()};
  auto& cw = w.OpenColumn(kField, duckdb::LogicalType::VARCHAR,
                          /*skip_validity=*/false, DEFAULT_ROW_GROUP_SIZE,
                          arm.codec, /*hyperloglog=*/false,
                          irs::ColCodecParams{.compression_level = arm.level,
                                              .objective = arm.objective});
  uint64_t pos = 0;
  for (size_t i = 0; i < data.vectors.size(); ++i) {
    cw.Append(pos, *data.vectors[i], data.counts[i]);
    pos += data.counts[i];
  }
  w.Commit(pos);
  uint64_t bytes = 0;
  for (const auto& block : cw.Meta().data) {
    bytes += block.byte_size;
  }
  for (const auto& block : cw.Meta().validity) {
    bytes += block.byte_size;
  }
  return bytes;
}

const ColSeg& GetColSeg(size_t arm) {
  static std::map<size_t, std::unique_ptr<ColSeg>> cache;
  auto& slot = cache[arm];
  if (!slot) {
    slot = std::make_unique<ColSeg>();
    slot->bytes = ColBuild(slot->dir, kColArms[arm]);
    slot->reader =
      std::make_unique<irs::ColReader>(slot->dir, std::string{kSeg}, CsDb());
    slot->col = slot->reader->Column(kField);
    if (slot->col == nullptr) {
      std::fprintf(stderr, "col_codecs_real: column missing\n");
      std::abort();
    }
  }
  return *slot;
}

void Counters(benchmark::State& state, uint64_t bytes) {
  state.counters["bytes"] = static_cast<double>(bytes);
  state.counters["raw"] = static_cast<double>(Data().raw_bytes);
  state.counters["rows"] = static_cast<double>(Data().rows);
}

void ColSeal(benchmark::State& state, size_t arm) {
  uint64_t bytes = 0;
  for (auto _ : state) {
    irs::MemoryDirectory dir{};
    bytes = ColBuild(dir, kColArms[arm]);
    benchmark::DoNotOptimize(&dir);
  }
  Counters(state, bytes);
}

void ColScan(benchmark::State& state, size_t arm, bool flat) {
  const auto& seg = GetColSeg(arm);
  const auto rows = Data().rows;
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
    while (pos < rows) {
      const auto take = std::min<uint64_t>(rows - pos, STANDARD_VECTOR_SIZE);
      batch.ResetFromCache(cache);
      seg.col->Scan(st, batch, take);
      if (flat) {
        batch.Flatten(take);
      }
      benchmark::DoNotOptimize(batch);
      pos += take;
    }
  }
  Counters(state, seg.bytes);
}

void ColGather(benchmark::State& state, size_t arm) {
  const auto& seg = GetColSeg(arm);
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
  Counters(state, seg.bytes);
}

void ColPoint(benchmark::State& state, size_t arm) {
  const auto& seg = GetColSeg(arm);
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
  Counters(state, seg.bytes);
  state.counters["lookups"] = static_cast<double>(rows.size());
}

struct DuckArm {
  const char* name;
  const char* compression;
  const char* mode;
};

constexpr DuckArm kDuckArms[] = {
  {"auto", nullptr, nullptr},
  {"dict_fsst", "dict_fsst", nullptr},
  {"dict_fsst_native", "dict_fsst", "AUTO_NATIVE"},
  {"dictionary", "dict_fsst", "DICTIONARY"},
  {"dict_fsst_mode", "dict_fsst", "DICT_FSST"},
  {"fsst_only", "dict_fsst", "FSST_ONLY"},
  {"dict_fsst_plus", "dict_fsst", "DICT_FSST_PLUS"},
  {"fsst_plus", "dict_fsst", "FSST_PLUS"},
  {"zstd", "zstd", nullptr},
  {"uncompressed", "uncompressed", nullptr},
};

struct DuckSeg {
  std::string path;
  std::unique_ptr<duckdb::DuckDB> db;
  std::unique_ptr<duckdb::Connection> con;
  uint64_t bytes = 0;
};

std::vector<std::string>& DuckFiles() {
  static std::vector<std::string> files;
  return files;
}

void DuckQuery(duckdb::Connection& con, const std::string& sql) {
  auto result = con.Query(sql);
  if (result->HasError()) {
    std::fprintf(stderr, "%s: %s\n", sql.c_str(), result->GetError().c_str());
    std::abort();
  }
}

void RemoveDuckFile(const std::string& path) {
  std::error_code ec;
  std::filesystem::remove(path, ec);
  std::filesystem::remove(path + ".wal", ec);
}

void DuckBuild(const DuckArm& arm, DuckSeg& seg) {
  const auto& data = Data();
  seg.con.reset();
  seg.db.reset();
  RemoveDuckFile(seg.path);
  seg.db = std::make_unique<duckdb::DuckDB>(seg.path);
  seg.con = std::make_unique<duckdb::Connection>(*seg.db);
  DuckQuery(*seg.con, "CREATE SCHEMA IF NOT EXISTS main");
  DuckQuery(*seg.con, "SET search_path = 'main'");
  DuckQuery(*seg.con, "SET threads = 1");
  if (arm.mode) {
    DuckQuery(*seg.con,
              std::string{"SET force_dict_fsst_mode = '"} + arm.mode + "'");
  }
  DuckQuery(*seg.con, arm.compression
                        ? std::string{"CREATE TABLE t (v VARCHAR USING "
                                      "COMPRESSION "} +
                            arm.compression + ")"
                        : std::string{"CREATE TABLE t (v VARCHAR)"});
  {
    duckdb::Appender appender{*seg.con, "t"};
    duckdb::DataChunk chunk;
    chunk.InitializeEmpty(duckdb::vector<duckdb::LogicalType>{
      duckdb::LogicalType::VARCHAR});
    for (size_t i = 0; i < data.vectors.size(); ++i) {
      chunk.Reset();
      chunk.data[0].Reference(*data.vectors[i]);
      chunk.SetCardinality(data.counts[i]);
      appender.AppendDataChunk(chunk);
    }
    appender.Close();
  }
  DuckQuery(*seg.con, "CHECKPOINT");
  seg.bytes = std::filesystem::file_size(seg.path);
}

DuckSeg& GetDuckSeg(size_t arm) {
  static std::map<size_t, std::unique_ptr<DuckSeg>> cache;
  auto& slot = cache[arm];
  if (!slot) {
    slot = std::make_unique<DuckSeg>();
    slot->path = "/dev/shm/sdb_ccr_" + std::to_string(::getpid()) + "_" +
                 kDuckArms[arm].name + ".db";
    DuckFiles().push_back(slot->path);
    DuckBuild(kDuckArms[arm], *slot);
  }
  return *slot;
}

void DuckSeal(benchmark::State& state, size_t arm) {
  DuckSeg seg;
  seg.path = "/dev/shm/sdb_ccr_" + std::to_string(::getpid()) + "_seal_" +
             kDuckArms[arm].name + ".db";
  DuckFiles().push_back(seg.path);
  for (auto _ : state) {
    DuckBuild(kDuckArms[arm], seg);
  }
  Counters(state, seg.bytes);
  seg.con.reset();
  seg.db.reset();
  RemoveDuckFile(seg.path);
}

template<typename F>
void WithStorage(DuckSeg& seg, F&& f) {
  seg.con->BeginTransaction();
  auto& ctx = *seg.con->context;
  auto& entry = duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
    ctx, duckdb::QualifiedName{duckdb::Identifier{"t"}});
  auto& storage = entry.Cast<duckdb::DuckTableEntry>().GetStorage();
  auto& tx = duckdb::DuckTransaction::Get(ctx, entry.ParentCatalog());
  f(ctx, storage, tx);
  seg.con->Commit();
}

void DuckScan(benchmark::State& state, size_t arm, bool flat) {
  auto& seg = GetDuckSeg(arm);
  const duckdb::vector<duckdb::StorageIndex> columns{duckdb::StorageIndex{0}};
  WithStorage(seg, [&](duckdb::ClientContext& ctx, duckdb::DataTable& storage,
                       duckdb::DuckTransaction& tx) {
    duckdb::DataChunk chunk;
    chunk.Initialize(duckdb::Allocator::DefaultAllocator(),
                     duckdb::vector<duckdb::LogicalType>{
                       duckdb::LogicalType::VARCHAR});
    for (auto _ : state) {
      state.PauseTiming();
      duckdb::TableScanState scan;
      storage.InitializeScan(ctx, tx, scan, columns);
      state.ResumeTiming();
      for (;;) {
        chunk.Reset();
        storage.Scan(tx, chunk, scan);
        if (chunk.size() == 0) {
          break;
        }
        if (flat) {
          chunk.data[0].Flatten(chunk.size());
        }
        benchmark::DoNotOptimize(chunk.data.data());
      }
    }
  });
  Counters(state, seg.bytes);
}

void DuckFetch(benchmark::State& state, size_t arm, bool point) {
  auto& seg = GetDuckSeg(arm);
  const auto& rows = ScatteredRows();
  std::vector<duckdb::row_t> ids(rows.begin(), rows.end());
  const duckdb::vector<duckdb::StorageIndex> columns{duckdb::StorageIndex{0}};
  WithStorage(seg, [&](duckdb::ClientContext&, duckdb::DataTable& storage,
                       duckdb::DuckTransaction& tx) {
    duckdb::DataChunk out;
    out.Initialize(duckdb::Allocator::DefaultAllocator(),
                   duckdb::vector<duckdb::LogicalType>{
                     duckdb::LogicalType::VARCHAR});
    for (auto _ : state) {
      duckdb::ColumnFetchState fetch;
      const size_t step = point ? 1 : STANDARD_VECTOR_SIZE;
      for (size_t i = 0; i < ids.size(); i += step) {
        const auto take = std::min(step, ids.size() - i);
        duckdb::Vector row_ids{duckdb::LogicalType::ROW_TYPE,
                               reinterpret_cast<duckdb::data_ptr_t>(&ids[i]),
                               take};
        out.Reset();
        storage.Fetch(tx, out, columns, row_ids, take, fetch);
        benchmark::DoNotOptimize(out.data.data());
      }
    }
  });
  Counters(state, seg.bytes);
  state.counters["lookups"] = static_cast<double>(rows.size());
}

void Register() {
  for (size_t arm = 0; arm < std::size(kColArms); ++arm) {
    const std::string suffix = std::string{"/col/"} + kColArms[arm].name;
    benchmark::RegisterBenchmark(("Seal" + suffix).c_str(), ColSeal, arm)
      ->Unit(benchmark::kMillisecond);
    benchmark::RegisterBenchmark(("Scan" + suffix).c_str(), ColScan, arm, false)
      ->Unit(benchmark::kMillisecond);
    benchmark::RegisterBenchmark(("ScanFlat" + suffix).c_str(), ColScan, arm,
                                 true)
      ->Unit(benchmark::kMillisecond);
    benchmark::RegisterBenchmark(("Gather" + suffix).c_str(), ColGather, arm)
      ->Unit(benchmark::kMillisecond);
    benchmark::RegisterBenchmark(("Point" + suffix).c_str(), ColPoint, arm)
      ->Unit(benchmark::kMillisecond);
  }
  for (size_t arm = 0; arm < std::size(kDuckArms); ++arm) {
    const std::string suffix = std::string{"/duck/"} + kDuckArms[arm].name;
    benchmark::RegisterBenchmark(("Seal" + suffix).c_str(), DuckSeal, arm)
      ->Unit(benchmark::kMillisecond);
    benchmark::RegisterBenchmark(("Scan" + suffix).c_str(), DuckScan, arm,
                                 false)
      ->Unit(benchmark::kMillisecond);
    benchmark::RegisterBenchmark(("ScanFlat" + suffix).c_str(), DuckScan, arm,
                                 true)
      ->Unit(benchmark::kMillisecond);
    benchmark::RegisterBenchmark(("Gather" + suffix).c_str(), DuckFetch, arm,
                                 false)
      ->Unit(benchmark::kMillisecond);
    benchmark::RegisterBenchmark(("Point" + suffix).c_str(), DuckFetch, arm,
                                 true)
      ->Unit(benchmark::kMillisecond);
  }
}

}  // namespace

int main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();
  if (std::getenv("SDB_BENCH_NO_DICT_CACHE")) {
    CsDb().GetObjectCache().SetMaxMemory(0);
  }
  Register();
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  for (const auto& path : DuckFiles()) {
    RemoveDuckFile(path);
  }
  irs::DuckDBEngine::Instance().Shutdown();
  return 0;
}
