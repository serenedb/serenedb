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
#include <fcntl.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <duckdb.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/common/types/vector_cache.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/scalar/string_functions.hpp>
#include <duckdb/main/appender.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
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
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
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
  duckdb::LogicalType type;
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
    const char* offset = std::getenv("SDB_BENCH_OFFSET");
    auto result =
      con.Query(std::string{"SELECT \""} + name + "\" FROM read_parquet('" +
                path + "') LIMIT " + std::to_string(RequestedRows()) +
                " OFFSET " + (offset ? offset : "0"));
    if (result->HasError()) {
      std::fprintf(stderr, "load: %s\n", result->GetError().c_str());
      std::abort();
    }
    out.type = result->GetTypes()[0];
    const bool strings_column =
      out.type.InternalType() == duckdb::PhysicalType::VARCHAR;
    while (auto chunk = result->Fetch()) {
      if (chunk->size() == 0) {
        break;
      }
      const auto count = chunk->size();
      auto vec = std::make_unique<duckdb::Vector>(out.type, count);
      duckdb::VectorOperations::Copy(chunk->data[0], *vec, count, 0, 0);
      duckdb::UnifiedVectorFormat format;
      vec->ToUnifiedFormat(format);
      const auto* strings =
        strings_column
          ? duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(format)
          : nullptr;
      for (duckdb::idx_t i = 0; i < count; ++i) {
        const auto idx = format.sel->get_index(i);
        if (format.validity.RowIsValid(idx)) {
          out.raw_bytes += strings
                             ? strings[idx].GetSize()
                             : duckdb::GetTypeIdSize(out.type.InternalType());
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
    uint64_t stride = 37;
    if (const char* e = std::getenv("SDB_BENCH_STRIDE")) {
      stride = std::max<uint64_t>(1, std::strtoull(e, nullptr, 10));
    }
    for (uint64_t r = 0; r < Data().rows; r += stride) {
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
};

constexpr ColArm kNumArms[] = {
  {"auto", duckdb::CompressionType::COMPRESSION_AUTO, 0},
  {"bitpacking", duckdb::CompressionType::COMPRESSION_BITPACKING, 0},
  {"rle", duckdb::CompressionType::COMPRESSION_RLE, 0},
  {"alp", duckdb::CompressionType::COMPRESSION_ALP, 0},
  {"alprd", duckdb::CompressionType::COMPRESSION_ALPRD, 0},
  {"uncompressed", duckdb::CompressionType::COMPRESSION_UNCOMPRESSED, 0},
};

constexpr ColArm kColArms[] = {
  {"auto", duckdb::CompressionType::COMPRESSION_AUTO, 0},
  {"dict_fsst", duckdb::CompressionType::COMPRESSION_DICT_FSST, 0},
  {"fsst", duckdb::CompressionType::COMPRESSION_FSST, 0},
  {"dict_lz4", duckdb::CompressionType::COMPRESSION_DICT_LZ4, 0},
  {"dict_lz4_hc2", duckdb::CompressionType::COMPRESSION_DICT_LZ4, 2},
  {"dict_lz4_hc4", duckdb::CompressionType::COMPRESSION_DICT_LZ4, 4},
  {"dict_lz4_hc6", duckdb::CompressionType::COMPRESSION_DICT_LZ4, 6},
  {"dict_lz4_hc9", duckdb::CompressionType::COMPRESSION_DICT_LZ4, 9},
  {"dict_lz4_hc12", duckdb::CompressionType::COMPRESSION_DICT_LZ4, 12},
  {"lz4", duckdb::CompressionType::COMPRESSION_LZ4, 0},
  {"lz4_hc4", duckdb::CompressionType::COMPRESSION_LZ4, 4},
  {"lz4_hc9", duckdb::CompressionType::COMPRESSION_LZ4, 9},
  {"lz4_hc12", duckdb::CompressionType::COMPRESSION_LZ4, 12},
  {"dict_zstd1", duckdb::CompressionType::COMPRESSION_DICT_ZSTD, 1},
  {"dict_zstd2", duckdb::CompressionType::COMPRESSION_DICT_ZSTD, 2},
  {"dict_zstd3", duckdb::CompressionType::COMPRESSION_DICT_ZSTD, 3},
  {"dict_zstd6", duckdb::CompressionType::COMPRESSION_DICT_ZSTD, 6},
  {"dict_zstd9", duckdb::CompressionType::COMPRESSION_DICT_ZSTD, 9},
  {"dict_zstd12", duckdb::CompressionType::COMPRESSION_DICT_ZSTD, 12},
  {"dict_zstd15", duckdb::CompressionType::COMPRESSION_DICT_ZSTD, 15},
  {"dict_zstd19", duckdb::CompressionType::COMPRESSION_DICT_ZSTD, 19},
  {"zstd1", duckdb::CompressionType::COMPRESSION_ZSTD, 1},
  {"zstd3", duckdb::CompressionType::COMPRESSION_ZSTD, 3},
  {"zstd6", duckdb::CompressionType::COMPRESSION_ZSTD, 6},
  {"zstd9", duckdb::CompressionType::COMPRESSION_ZSTD, 9},
  {"zstd12", duckdb::CompressionType::COMPRESSION_ZSTD, 12},
  {"zstd19", duckdb::CompressionType::COMPRESSION_ZSTD, 19},
  {"dict_zxc1", duckdb::CompressionType::COMPRESSION_DICT_ZXC, 1},
  {"dict_zxc2", duckdb::CompressionType::COMPRESSION_DICT_ZXC, 2},
  {"dict_zxc3", duckdb::CompressionType::COMPRESSION_DICT_ZXC, 3},
  {"dict_zxc4", duckdb::CompressionType::COMPRESSION_DICT_ZXC, 4},
  {"dict_zxc5", duckdb::CompressionType::COMPRESSION_DICT_ZXC, 5},
  {"dict_zxc6", duckdb::CompressionType::COMPRESSION_DICT_ZXC, 6},
  {"dict_zxc7", duckdb::CompressionType::COMPRESSION_DICT_ZXC, 7},
  {"zxc1", duckdb::CompressionType::COMPRESSION_ZXC, 1},
  {"zxc3", duckdb::CompressionType::COMPRESSION_ZXC, 3},
  {"zxc5", duckdb::CompressionType::COMPRESSION_ZXC, 5},
  {"zxc7", duckdb::CompressionType::COMPRESSION_ZXC, 7},
  {"uncompressed", duckdb::CompressionType::COMPRESSION_UNCOMPRESSED, 0},
};

uint32_t SegmentTarget() {
  static const uint32_t target = [] {
    if (const char* e = std::getenv("SDB_BENCH_SEGMENT_TARGET")) {
      return static_cast<uint32_t>(std::strtoul(e, nullptr, 10));
    }
    return irs::kDefaultColSegmentTarget;
  }();
  return target;
}

irs::WriteTier Tier() {
  return std::getenv("SDB_BENCH_REFRESH") ? irs::WriteTier::Flush
                                          : irs::WriteTier::Merge;
}

bool Cold() { return std::getenv("SDB_BENCH_COLD") != nullptr; }

struct ColSeg {
  std::unique_ptr<irs::Directory> dir;
  std::filesystem::path path;
  std::unique_ptr<irs::ColReader> reader;
  const irs::ColumnReader* col = nullptr;
  uint64_t bytes = 0;
  uint64_t ours = 0;
};

void MakeSegDirectory(ColSeg& seg, const ColArm& arm) {
  const char* root = std::getenv("SDB_BENCH_MMAP_DIR");
  if (root == nullptr) {
    seg.dir = std::make_unique<irs::MemoryDirectory>();
    return;
  }
  const bool numeric =
    &arm >= std::begin(kNumArms) && &arm < std::end(kNumArms);
  seg.path = std::filesystem::path{root} /
             (std::string{numeric ? "num_" : "col_"} + arm.name);
  std::filesystem::remove_all(seg.path);
  std::filesystem::create_directories(seg.path);
  seg.dir = std::make_unique<irs::MMapDirectory>(seg.path);
}

void OpenReader(ColSeg& seg) {
  seg.reader =
    std::make_unique<irs::ColReader>(*seg.dir, std::string{kSeg}, CsDb());
  seg.col = seg.reader->Column(kField);
  if (seg.col == nullptr) {
    std::fprintf(stderr, "col_codecs_real: column missing\n");
    std::abort();
  }
}

void Evict(ColSeg& seg) {
  if (!Cold() || seg.path.empty()) {
    return;
  }
  seg.col = nullptr;
  seg.reader.reset();
  for (const auto& entry : std::filesystem::directory_iterator(seg.path)) {
    const int fd = ::open(entry.path().c_str(), O_RDONLY);
    if (fd < 0) {
      continue;
    }
    ::fdatasync(fd);
    ::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
    ::close(fd);
  }
  OpenReader(seg);
}

struct Built {
  uint64_t bytes = 0;
  uint64_t ours = 0;
};

Built ColBuild(irs::Directory& dir, const ColArm& arm) {
  const auto& data = Data();
  irs::ColWriter w{dir, kSeg, CsDb()};
  auto& cw = w.OpenColumn(kField, data.type,
                          /*skip_validity=*/false, DEFAULT_ROW_GROUP_SIZE,
                          arm.codec, /*hyperloglog=*/false,
                          irs::ColCodecParams{.compression_level = arm.level,
                                              .segment_target = SegmentTarget(),
                                              .tier = Tier()});
  uint64_t pos = 0;
  for (size_t i = 0; i < data.vectors.size(); ++i) {
    cw.Append(pos, *data.vectors[i], data.counts[i]);
    pos += data.counts[i];
  }
  w.Commit(pos);
  if (std::getenv("SDB_BENCH_BREAKDOWN") &&
      data.type.InternalType() == duckdb::PhysicalType::VARCHAR) {
    auto in = dir.open(irs::FileName(kSeg), irs::IOAdvice::NORMAL);
    std::vector<uint8_t> file(in->Length());
    in->ReadData(file.data(), file.size());
    uint64_t parts[8]{};
    for (const auto& block : cw.Meta().data) {
      if (!block.codec || block.byte_size == 0 ||
          !duckdb::IsSereneDBCompressionType(block.codec->type)) {
        continue;
      }
      const auto* p = file.data() + block.file_offset;
      const auto at = [&](size_t off) {
        return duckdb::Load<uint32_t>(p + off);
      };
      parts[0] += at(24);
      parts[1] += at(28) - at(24);
      parts[2] += at(32) - at(28);
      parts[3] += at(36) - at(32);
      parts[4] += at(40) - at(36);
      parts[5] += at(44) - at(40);
      parts[6] += at(52) - at(44);
      parts[7] += at(56);
    }
    uint64_t dictionaries = 0;
    for (const auto& d : cw.Meta().dictionaries) {
      dictionaries += d.byte_size;
    }
    std::fprintf(stderr,
                 "breakdown %s header %llu frames %llu lengths %llu lcps %llu "
                 "codes %llu runs %llu symtab %llu data %llu trained %llu\n",
                 arm.name, static_cast<unsigned long long>(parts[0]),
                 static_cast<unsigned long long>(parts[1]),
                 static_cast<unsigned long long>(parts[2]),
                 static_cast<unsigned long long>(parts[3]),
                 static_cast<unsigned long long>(parts[4]),
                 static_cast<unsigned long long>(parts[5]),
                 static_cast<unsigned long long>(parts[6]),
                 static_cast<unsigned long long>(parts[7]),
                 static_cast<unsigned long long>(dictionaries));
  }
  Built built;
  for (const auto& block : cw.Meta().data) {
    built.bytes += block.byte_size;
    if (block.codec &&
        block.codec->type == duckdb::CompressionType::COMPRESSION_COL_NUMERIC) {
      built.ours += block.tuple_count;
    }
  }
  for (const auto& block : cw.Meta().validity) {
    built.bytes += block.byte_size;
  }
  return built;
}

duckdb::Value ValueAt(uint64_t row);
std::unique_ptr<duckdb::TableFilter> MakeFilter(bool like);

duckdb::Value SourceAt(uint64_t row) {
  const auto& data = Data();
  for (size_t v = 0; v < data.vectors.size(); ++v) {
    if (row < data.counts[v]) {
      return data.vectors[v]->GetValue(row);
    }
    row -= data.counts[v];
  }
  return duckdb::Value{data.type};
}

void Mismatch(const ColArm& arm, const char* path, uint64_t row,
              const duckdb::Value& got) {
  std::fprintf(stderr, "verify %s/%s row %llu: got %s want %s\n", arm.name,
               path, static_cast<unsigned long long>(row),
               got.ToString().c_str(), SourceAt(row).ToString().c_str());
  std::abort();
}

void Verify(const ColArm& arm, const ColSeg& seg) {
  if (!std::getenv("SDB_BENCH_VERIFY")) {
    return;
  }
  const auto& data = Data();
  irs::ReadContext ctx{*seg.reader};
  {
    auto st = seg.col->InitScan(ctx);
    duckdb::VectorCache cache{duckdb::Allocator::DefaultAllocator(), data.type,
                              STANDARD_VECTOR_SIZE};
    duckdb::Vector batch{cache};
    uint64_t row = 0;
    for (size_t v = 0; v < data.vectors.size(); ++v) {
      batch.ResetFromCache(cache);
      seg.col->Scan(st, batch, data.counts[v]);
      for (duckdb::idx_t i = 0; i < data.counts[v]; ++i) {
        const auto got = batch.GetValue(i);
        if (!duckdb::Value::NotDistinctFrom(got,
                                            data.vectors[v]->GetValue(i))) {
          Mismatch(arm, "scan", row + i, got);
        }
      }
      row += data.counts[v];
    }
  }
  {
    const auto& rows = ScatteredRows();
    auto st = seg.col->InitScan(ctx);
    duckdb::VectorCache cache{duckdb::Allocator::DefaultAllocator(), data.type,
                              STANDARD_VECTOR_SIZE};
    duckdb::Vector batch{cache};
    for (size_t i = 0; i < rows.size(); i += STANDARD_VECTOR_SIZE) {
      const auto take = std::min<size_t>(rows.size() - i, STANDARD_VECTOR_SIZE);
      batch.ResetFromCache(cache);
      irs::column_internal::GatherRows(*seg.col, st, Rows2{&rows[i], take},
                                       batch, 0);
      for (size_t k = 0; k < take; ++k) {
        const auto got = batch.GetValue(k);
        if (!duckdb::Value::NotDistinctFrom(got, SourceAt(rows[i + k]))) {
          Mismatch(arm, "gather", rows[i + k], got);
        }
      }
    }
  }
  irs::ColumnReader::PointReader cursor{*seg.reader, *seg.col};
  duckdb::Vector out{data.type, 1};
  for (uint64_t r = 0; r < data.rows; r += 997) {
    duckdb::FlatVector::ValidityMutable(out).Reset();
    cursor.FetchRow(r, out, 0);
    const auto got = out.GetValue(0);
    if (!duckdb::Value::NotDistinctFrom(got, SourceAt(r))) {
      Mismatch(arm, "point", r, got);
    }
  }
  const auto& stats = seg.col->MergedStatistics();
  if (data.type.InternalType() != duckdb::PhysicalType::VARCHAR) {
    duckdb::Value lo;
    duckdb::Value hi;
    bool any_null = false;
    for (size_t v = 0; v < data.vectors.size(); ++v) {
      for (duckdb::idx_t i = 0; i < data.counts[v]; ++i) {
        const auto value = data.vectors[v]->GetValue(i);
        if (value.IsNull()) {
          any_null = true;
          continue;
        }
        if (lo.IsNull() || value < lo) {
          lo = value;
        }
        if (hi.IsNull() || hi < value) {
          hi = value;
        }
      }
    }
    const bool ok = stats.CanHaveNoNull() == !lo.IsNull() &&
                    (!any_null || stats.CanHaveNull()) &&
                    (lo.IsNull() || (duckdb::NumericStats::HasMinMax(stats) &&
                                     duckdb::NumericStats::Min(stats) == lo &&
                                     duckdb::NumericStats::Max(stats) == hi));
    if (!ok) {
      std::fprintf(stderr, "verify %s/stats got %s want min %s max %s\n",
                   arm.name, stats.ToString().c_str(), lo.ToString().c_str(),
                   hi.ToString().c_str());
      std::abort();
    }
  }
  const auto needle = ValueAt(data.rows / 3);
  uint64_t want = 0;
  for (size_t v = 0; v < data.vectors.size(); ++v) {
    for (duckdb::idx_t i = 0; i < data.counts[v]; ++i) {
      want += data.vectors[v]->GetValue(i) == needle;
    }
  }
  const auto filter = MakeFilter(false);
  duckdb::Connection con{CsDb()};
  auto filter_state =
    duckdb::TableFilterState::Initialize(*con.context, *filter);
  auto st = seg.col->InitScan(ctx);
  duckdb::VectorCache cache{duckdb::Allocator::DefaultAllocator(), data.type,
                            STANDARD_VECTOR_SIZE};
  duckdb::Vector filtered{cache};
  uint64_t kept = 0;
  for (uint64_t anchor = 0; anchor < data.rows;
       anchor += STANDARD_VECTOR_SIZE) {
    const auto span = static_cast<duckdb::idx_t>(
      std::min<uint64_t>(data.rows - anchor, STANDARD_VECTOR_SIZE));
    duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
    for (duckdb::idx_t i = 0; i < span; ++i) {
      sel.set_index(i, i);
    }
    filtered.ResetFromCache(cache);
    const auto hits =
      seg.col->GatherFilter(st, anchor, span, sel, span, *filter, *filter_state,
                            irs::NullCheckKind::None, filtered);
    for (duckdb::idx_t k = 0; k < hits; ++k) {
      const auto row = sel.get_index(k);
      if (!duckdb::Value::NotDistinctFrom(filtered.GetValue(row), needle)) {
        Mismatch(arm, "filter", anchor + row, filtered.GetValue(row));
      }
    }
    kept += hits;
  }
  if (kept != want) {
    std::fprintf(stderr, "verify %s/filter kept %llu want %llu\n", arm.name,
                 static_cast<unsigned long long>(kept),
                 static_cast<unsigned long long>(want));
    std::abort();
  }
}

void PrintSegmentInfo(const ColArm& arm, const ColSeg& seg) {
  if (!std::getenv("SDB_BENCH_SEGINFO")) {
    return;
  }
  std::map<std::string, uint64_t> rows;
  irs::ReadContext ctx{*seg.reader};
  irs::BlockWindow window{};
  uint64_t row = 0;
  for (const auto& block : seg.col->DataBlocks()) {
    std::string key = duckdb::CompressionTypeToString(block.codec->type);
    if (block.codec->get_segment_info && block.byte_size != 0) {
      window = seg.col->Locate(row, window);
      auto segment = seg.col->OpenSegment(window.block, ctx);
      auto info =
        block.codec->get_segment_info(duckdb::QueryContext{}, *segment);
      for (const auto* name : {"transform", "leaf", "codec", "shape",
                               "dictionary", "codes", "codes_transform"}) {
        if (info.contains(name)) {
          key += std::string{" "} + name + "=" + info[name];
        }
      }
    }
    rows[key] += block.tuple_count;
    row += block.tuple_count;
  }
  for (const auto& [key, count] : rows) {
    std::fprintf(stderr, "seginfo %s %s rows %llu\n", arm.name, key.c_str(),
                 static_cast<unsigned long long>(count));
  }
}

ColSeg& GetColSeg(const ColArm* arm) {
  static std::map<const ColArm*, std::unique_ptr<ColSeg>> cache;
  auto& slot = cache[arm];
  if (!slot) {
    slot = std::make_unique<ColSeg>();
    MakeSegDirectory(*slot, *arm);
    const auto built = ColBuild(*slot->dir, *arm);
    slot->bytes = built.bytes;
    slot->ours = built.ours;
    OpenReader(*slot);
    Verify(*arm, *slot);
    PrintSegmentInfo(*arm, *slot);
  }
  return *slot;
}

void Counters(benchmark::State& state, uint64_t bytes, uint64_t ours = 0) {
  state.counters["bytes"] = static_cast<double>(bytes);
  state.counters["raw"] = static_cast<double>(Data().raw_bytes);
  state.counters["rows"] = static_cast<double>(Data().rows);
  state.counters["ours"] = static_cast<double>(ours);
}

void Counters(benchmark::State& state, const ColSeg& seg) {
  Counters(state, seg.bytes, seg.ours);
}

void ColSeal(benchmark::State& state, const ColArm* arm) {
  Built built;
  for (auto _ : state) {
    irs::MemoryDirectory dir{};
    built = ColBuild(dir, *arm);
    benchmark::DoNotOptimize(&dir);
  }
  Counters(state, built.bytes, built.ours);
}

void ScanRange(const irs::ColumnReader& col, irs::ReadContext& ctx,
               uint64_t begin, uint64_t end) {
  auto st = col.InitScan(ctx);
  if (begin != 0) {
    col.Skip(st, begin);
  }
  duckdb::VectorCache cache{duckdb::Allocator::DefaultAllocator(), Data().type,
                            STANDARD_VECTOR_SIZE};
  duckdb::Vector batch{cache};
  for (uint64_t pos = begin; pos < end;) {
    const auto take = std::min<uint64_t>(end - pos, STANDARD_VECTOR_SIZE);
    batch.ResetFromCache(cache);
    col.Scan(st, batch, take);
    benchmark::DoNotOptimize(batch);
    pos += take;
  }
}

void ColScanParallel(benchmark::State& state, const ColArm* arm,
                     size_t threads) {
  auto& seg = GetColSeg(arm);
  const auto rows = Data().rows;
  const uint64_t vectors =
    (rows + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
  for (auto _ : state) {
    state.PauseTiming();
    Evict(seg);
    state.ResumeTiming();
    std::vector<std::thread> pool;
    for (size_t t = 0; t < threads; ++t) {
      const uint64_t begin =
        std::min(rows, vectors * t / threads * STANDARD_VECTOR_SIZE);
      const uint64_t end =
        std::min(rows, vectors * (t + 1) / threads * STANDARD_VECTOR_SIZE);
      pool.emplace_back([&, begin, end] {
        irs::ReadContext ctx{*seg.reader};
        ScanRange(*seg.col, ctx, begin, end);
      });
    }
    for (auto& t : pool) {
      t.join();
    }
  }
  Counters(state, seg);
}

duckdb::Value ValueAt(uint64_t row) {
  const auto& data = Data();
  const bool strings_column =
    data.type.InternalType() == duckdb::PhysicalType::VARCHAR;
  for (size_t v = 0, base = 0; v < data.vectors.size();
       base += data.counts[v], ++v) {
    if (row >= base + data.counts[v]) {
      continue;
    }
    for (auto i = row - base; i < data.counts[v]; ++i) {
      auto value = data.vectors[v]->GetValue(i);
      if (!value.IsNull() &&
          (!strings_column || !value.GetValue<std::string>().empty())) {
        return value;
      }
    }
    row = base + data.counts[v];
  }
  return duckdb::Value{data.type};
}

std::unique_ptr<duckdb::TableFilter> MakeFilter(bool like) {
  auto column =
    duckdb::make_uniq<duckdb::BoundReferenceExpression>(Data().type, 0);
  if (!like) {
    return std::make_unique<duckdb::ExpressionFilter>(
      duckdb::BoundComparisonExpression::Create(
        duckdb::ExpressionType::COMPARE_EQUAL, std::move(column),
        duckdb::make_uniq<duckdb::BoundConstantExpression>(
          ValueAt(Data().rows / 3))));
  }
  const auto source = ValueAt(Data().rows / 2).GetValue<std::string>();
  const auto piece = source.substr(source.size() / 3, 4);
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> args;
  args.push_back(std::move(column));
  args.push_back(
    duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value{piece}));
  return std::make_unique<duckdb::ExpressionFilter>(
    duckdb::make_uniq<duckdb::BoundFunctionExpression>(
      duckdb::BoundScalarFunction(
        duckdb::ContainsFun::GetFunctions().GetFunctionByOffset(0)),
      std::move(args), nullptr));
}

void ColFilter(benchmark::State& state, const ColArm* arm, bool like) {
  auto& seg = GetColSeg(arm);
  const auto rows = Data().rows;
  const auto filter = MakeFilter(like);
  duckdb::Connection con{CsDb()};
  auto filter_state =
    duckdb::TableFilterState::Initialize(*con.context, *filter);
  uint64_t kept = 0;
  for (auto _ : state) {
    state.PauseTiming();
    Evict(seg);
    irs::ReadContext ctx{*seg.reader};
    auto st = seg.col->InitScan(ctx);
    duckdb::VectorCache cache{duckdb::Allocator::DefaultAllocator(),
                              Data().type, STANDARD_VECTOR_SIZE};
    duckdb::Vector out{cache};
    state.ResumeTiming();
    kept = 0;
    for (uint64_t anchor = 0; anchor < rows; anchor += STANDARD_VECTOR_SIZE) {
      const auto span = static_cast<duckdb::idx_t>(
        std::min<uint64_t>(rows - anchor, STANDARD_VECTOR_SIZE));
      duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
      for (duckdb::idx_t i = 0; i < span; ++i) {
        sel.set_index(i, i);
      }
      out.ResetFromCache(cache);
      kept +=
        seg.col->GatherFilter(st, anchor, span, sel, span, *filter,
                              *filter_state, irs::NullCheckKind::None, out);
    }
    benchmark::DoNotOptimize(kept);
  }
  Counters(state, seg);
  state.counters["kept"] = static_cast<double>(kept);
}

void ColScan(benchmark::State& state, const ColArm* arm, bool flat) {
  auto& seg = GetColSeg(arm);
  const auto rows = Data().rows;
  for (auto _ : state) {
    state.PauseTiming();
    Evict(seg);
    irs::ReadContext ctx{*seg.reader};
    auto st = seg.col->InitScan(ctx);
    duckdb::VectorCache cache{duckdb::Allocator::DefaultAllocator(),
                              Data().type, STANDARD_VECTOR_SIZE};
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
  Counters(state, seg);
}

void ColGather(benchmark::State& state, const ColArm* arm) {
  auto& seg = GetColSeg(arm);
  const auto& rows = ScatteredRows();
  for (auto _ : state) {
    state.PauseTiming();
    Evict(seg);
    irs::ReadContext ctx{*seg.reader};
    auto st = seg.col->InitScan(ctx);
    duckdb::VectorCache cache{duckdb::Allocator::DefaultAllocator(),
                              Data().type, STANDARD_VECTOR_SIZE};
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
  Counters(state, seg);
}

void ColPoint(benchmark::State& state, const ColArm* arm) {
  auto& seg = GetColSeg(arm);
  const auto& rows = ScatteredRows();
  for (auto _ : state) {
    state.PauseTiming();
    Evict(seg);
    irs::ColumnReader::PointReader cursor{*seg.reader, *seg.col};
    duckdb::Vector out{Data().type, 1};
    state.ResumeTiming();
    for (const auto r : rows) {
      duckdb::FlatVector::ValidityMutable(out).Reset();
      cursor.FetchRow(r, out, 0);
      benchmark::DoNotOptimize(out);
    }
  }
  Counters(state, seg);
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
  const auto type = data.type.ToString();
  DuckQuery(*seg.con, arm.compression
                        ? "CREATE TABLE t (v " + type + " USING COMPRESSION " +
                            arm.compression + ")"
                        : "CREATE TABLE t (v " + type + ")");
  {
    duckdb::Appender appender{*seg.con, "t"};
    duckdb::DataChunk chunk;
    chunk.InitializeEmpty(duckdb::vector<duckdb::LogicalType>{data.type});
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
                     duckdb::vector<duckdb::LogicalType>{Data().type});
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
                   duckdb::vector<duckdb::LogicalType>{Data().type});
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

void RegisterColArm(const std::string& suffix, const ColArm* arm,
                    bool strings) {
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
  benchmark::RegisterBenchmark(("FilterEq" + suffix).c_str(), ColFilter, arm,
                               false)
    ->Unit(benchmark::kMillisecond);
  if (strings) {
    benchmark::RegisterBenchmark(("FilterLike" + suffix).c_str(), ColFilter,
                                 arm, true)
      ->Unit(benchmark::kMillisecond);
  }
  benchmark::RegisterBenchmark(("ScanPar16" + suffix).c_str(), ColScanParallel,
                               arm, size_t{16})
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
}

void Register() {
  for (const auto& arm : kColArms) {
    RegisterColArm(std::string{"/col/"} + arm.name, &arm, true);
  }
  for (const auto& arm : kNumArms) {
    RegisterColArm(std::string{"/num/"} + arm.name, &arm, false);
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

static int Main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();
  if (std::getenv("SDB_BENCH_NO_DICT_CACHE") || Cold()) {
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

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
