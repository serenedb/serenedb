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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <duckdb.hpp>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/function/compression_function.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iresearch/formats/column/codecs/dictionary_cache.hpp>
#include <iresearch/formats/column/codecs/numeric_layout.hpp>
#include <iresearch/formats/column/codecs/string_layout.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/internal/gather_arms.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "tests_shared.hpp"

namespace {

constexpr irs::field_id kField = 3;
constexpr std::string_view kSeg = "seg";

using Gen = std::function<std::optional<int64_t>(uint64_t)>;

struct Rows2 {
  const uint64_t* p;
  size_t n;
  size_t size() const noexcept { return n; }
  uint64_t operator[](size_t i) const noexcept { return p[i]; }
};

uint64_t Mix(uint64_t g) noexcept {
  g += 0x9E3779B97F4A7C15ULL;
  g = (g ^ (g >> 30)) * 0xBF58476D1CE4E5B9ULL;
  g = (g ^ (g >> 27)) * 0x94D049BB133111EBULL;
  return g ^ (g >> 31);
}

struct Shape {
  const char* name;
  Gen gen;
};

const std::vector<Shape>& Shapes() {
  static const std::vector<Shape> kShapes{
    {"sorted", [](uint64_t g) { return static_cast<int64_t>(g / 37); }},
    {"clustered",
     [](uint64_t g) {
       return static_cast<int64_t>(1'000'000 + g * 3 + Mix(g) % 5);
     }},
    {"low_cardinality",
     [](uint64_t g) -> std::optional<int64_t> {
       if (g % 11 == 0) {
         return std::nullopt;
       }
       return static_cast<int64_t>(Mix(g / 3) % 17) * 1000 - 3000;
     }},
    {"small_range",
     [](uint64_t g) { return static_cast<int64_t>(Mix(g) % 3000) + 7; }},
    {"random", [](uint64_t g) { return static_cast<int64_t>(Mix(g)); }},
    {"runs",
     [](uint64_t g) -> std::optional<int64_t> {
       if (g / 700 % 5 == 2) {
         return std::nullopt;
       }
       return static_cast<int64_t>(Mix(g / 4000) % 100000);
     }},
    {"periodic_runs",
     [](uint64_t g) { return static_cast<int64_t>(g / 7 % 4) * 1'000'003; }},
    {"hashes_with_repeats",
     [](uint64_t g) { return static_cast<int64_t>(Mix(Mix(g) % 5000)); }},
    {"negative", [](uint64_t g) { return -static_cast<int64_t>(g * 977); }},
    {"second_timestamps",
     [](uint64_t g) -> std::optional<int64_t> {
       if (g % 13 == 5) {
         return std::nullopt;
       }
       return static_cast<int64_t>(1'700'000'000 + g / 3 + Mix(g) % 7) *
              1'000'000;
     }},
    {"negative_multiples",
     [](uint64_t g) {
       return (static_cast<int64_t>(Mix(g) % 101) - 50) * 86'400;
     }},
    {"skewed_offsets",
     [](uint64_t g) {
       const auto m = Mix(g);
       return static_cast<int64_t>(1'000'000'000'000LL +
                                   (m % 100 < 95 ? m % 256 : m % 65536));
     }},
    {"drifting_runs",
     [](uint64_t g) {
       static const auto kRuns = [] {
         std::vector<int64_t> values;
         for (uint64_t r = 0; values.size() < 200'000; ++r) {
           const auto length = 4 + Mix(r) % 2;
           const auto v =
             static_cast<int64_t>(r / 4096 * 50'000 + Mix(r ^ 0x5bd1e995) % 16);
           values.insert(values.end(), length, v);
         }
         return values;
       }();
       return kRuns[g % kRuns.size()];
     }},
    {"sparse_values",
     [](uint64_t g) {
       return static_cast<int64_t>(Mix(g) % 120) * 104'729 - 50'000;
     }},
  };
  return kShapes;
}

std::vector<duckdb::LogicalType> Types() {
  return {duckdb::LogicalType::TINYINT,       duckdb::LogicalType::SMALLINT,
          duckdb::LogicalType::INTEGER,       duckdb::LogicalType::BIGINT,
          duckdb::LogicalType::UTINYINT,      duckdb::LogicalType::USMALLINT,
          duckdb::LogicalType::UINTEGER,      duckdb::LogicalType::UBIGINT,
          duckdb::LogicalType::FLOAT,         duckdb::LogicalType::DOUBLE,
          duckdb::LogicalType::DATE,          duckdb::LogicalType::TIMESTAMP,
          duckdb::LogicalType::DECIMAL(18, 3)};
}

duckdb::Value Cast(int64_t v, const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::TINYINT:
      return duckdb::Value::TINYINT(static_cast<int8_t>(v));
    case duckdb::LogicalTypeId::SMALLINT:
      return duckdb::Value::SMALLINT(static_cast<int16_t>(v));
    case duckdb::LogicalTypeId::INTEGER:
      return duckdb::Value::INTEGER(static_cast<int32_t>(v));
    case duckdb::LogicalTypeId::BIGINT:
      return duckdb::Value::BIGINT(v);
    case duckdb::LogicalTypeId::UTINYINT:
      return duckdb::Value::UTINYINT(static_cast<uint8_t>(v));
    case duckdb::LogicalTypeId::USMALLINT:
      return duckdb::Value::USMALLINT(static_cast<uint16_t>(v));
    case duckdb::LogicalTypeId::UINTEGER:
      return duckdb::Value::UINTEGER(static_cast<uint32_t>(v));
    case duckdb::LogicalTypeId::UBIGINT:
      return duckdb::Value::UBIGINT(static_cast<uint64_t>(v));
    case duckdb::LogicalTypeId::FLOAT:
      return duckdb::Value::FLOAT(static_cast<float>(v % 100000) / 8.0f);
    case duckdb::LogicalTypeId::DOUBLE:
      return duckdb::Value::DOUBLE(static_cast<double>(v) / 16.0);
    case duckdb::LogicalTypeId::DATE:
      return duckdb::Value::DATE(
        duckdb::date_t{static_cast<int32_t>(v % 1'000'000)});
    case duckdb::LogicalTypeId::TIMESTAMP:
      return duckdb::Value::TIMESTAMP(duckdb::timestamp_t{v / 2});
    default:
      return duckdb::Value::DECIMAL(
        static_cast<int64_t>((v / 4) % 900'000'000'000'000'000LL), 18, 3);
  }
}

class ColNumericCodecTest : public TestBase {
 protected:
  duckdb::DatabaseInstance& Db() { return *_db.instance; }

  std::optional<duckdb::Value> At(const Gen& gen,
                                  const duckdb::LogicalType& type, uint64_t g) {
    const auto v = gen(g);
    if (!v) {
      return std::nullopt;
    }
    return Cast(*v, type);
  }

  void Write(irs::Directory& dir, const duckdb::LogicalType& type,
             irs::ColCodecParams params, uint64_t rows, uint32_t rg_size,
             const Gen& gen) {
    irs::ColWriter w{dir, kSeg, Db()};
    auto& cw = w.OpenColumn(kField, type, /*skip_validity=*/false, rg_size,
                            duckdb::CompressionType::COMPRESSION_AUTO,
                            /*hyperloglog=*/false, params);
    uint64_t pos = 0;
    while (pos < rows) {
      const auto take = std::min<uint64_t>(rows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector vec{type, STANDARD_VECTOR_SIZE};
      for (uint64_t k = 0; k < take; ++k) {
        const auto v = At(gen, type, pos + k);
        vec.SetValue(k, v ? *v : duckdb::Value{type});
      }
      duckdb::FlatVector::SetSize(vec, take);
      cw.Append(vec, take);
      pos += take;
    }
    ASSERT_TRUE(w.Commit(0));
  }

  void ExpectRow(const duckdb::Vector& out, duckdb::idx_t k, uint64_t g,
                 const Gen& gen, const duckdb::LogicalType& type) {
    const auto expected = At(gen, type, g);
    const auto got = out.GetValue(k);
    if (!expected) {
      EXPECT_TRUE(got.IsNull()) << "row " << g;
      return;
    }
    EXPECT_TRUE(duckdb::Value::NotDistinctFrom(got, *expected))
      << "row " << g << " got " << got.ToString() << " want "
      << expected->ToString();
  }

  void Verify(irs::Directory& dir, const duckdb::LogicalType& type,
              uint64_t rows, const Gen& gen) {
    irs::ColReader r{dir, std::string{kSeg}, Db()};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    ASSERT_EQ(col->RowCount(), rows);
    {
      auto state = col->InitScan(r.Ctx());
      for (uint64_t pos = 0; pos < rows;) {
        const auto take = std::min<uint64_t>(rows - pos, STANDARD_VECTOR_SIZE);
        duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
        col->Scan(state, out, take);
        out.Flatten(take);
        for (duckdb::idx_t k = 0; k < take; ++k) {
          ExpectRow(out, k, pos + k, gen, type);
        }
        pos += take;
      }
    }
    {
      auto state = col->InitScan(r.Ctx());
      col->Skip(state, 5);
      for (uint64_t pos = 5; pos < rows;) {
        const auto take = std::min<uint64_t>(rows - pos, 700);
        duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
        duckdb::FlatVector::ValidityMutable(out).Reset(STANDARD_VECTOR_SIZE);
        col->ScanCount(state, out, take, 0);
        for (duckdb::idx_t k = 0; k < take; ++k) {
          ExpectRow(out, k, pos + k, gen, type);
        }
        pos += take;
      }
    }
    for (const uint64_t stride : {uint64_t{3}, uint64_t{41}, uint64_t{5003}}) {
      std::vector<uint64_t> picked;
      for (uint64_t g = stride / 2; g < rows; g += stride) {
        picked.push_back(g);
      }
      auto state = col->InitScan(r.Ctx());
      for (size_t i = 0; i < picked.size();) {
        const auto take =
          std::min<size_t>(picked.size() - i, STANDARD_VECTOR_SIZE);
        duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
        duckdb::FlatVector::ValidityMutable(out).Reset(STANDARD_VECTOR_SIZE);
        irs::column_internal::GatherRows(*col, state, Rows2{&picked[i], take},
                                         out, 0, /*whole_output=*/true);
        out.Flatten(take);
        for (size_t k = 0; k < take; ++k) {
          ExpectRow(out, k, picked[i + k], gen, type);
        }
        i += take;
      }
    }
    {
      irs::ColumnReader::PointReader cursor{r, *col};
      duckdb::Vector out{type, 1};
      for (uint64_t g = 0; g < rows; g += 97) {
        duckdb::FlatVector::ValidityMutable(out).Reset();
        EXPECT_EQ(cursor.FetchRow(g, out, 0), gen(g).has_value()) << g;
        ExpectRow(out, 0, g, gen, type);
      }
      duckdb::FlatVector::ValidityMutable(out).Reset();
      cursor.FetchRow(rows - 1, out, 0);
      ExpectRow(out, 0, rows - 1, gen, type);
    }
    std::optional<duckdb::Value> lo;
    std::optional<duckdb::Value> hi;
    bool any_null = false;
    for (uint64_t g = 0; g < rows; ++g) {
      const auto v = At(gen, type, g);
      if (!v) {
        any_null = true;
        continue;
      }
      if (!lo || *v < *lo) {
        lo = v;
      }
      if (!hi || *hi < *v) {
        hi = v;
      }
    }
    const auto& stats = col->MergedStatistics();
    EXPECT_EQ(stats.CanHaveNoNull(), lo.has_value());
    EXPECT_EQ(stats.CanHaveNull(), any_null);
    if (lo && type.id() != duckdb::LogicalTypeId::FLOAT &&
        type.id() != duckdb::LogicalTypeId::DOUBLE) {
      ASSERT_TRUE(duckdb::NumericStats::HasMinMax(stats));
      EXPECT_EQ(duckdb::NumericStats::Min(stats), *lo);
      EXPECT_EQ(duckdb::NumericStats::Max(stats), *hi);
    }
  }

  std::vector<std::string> SegmentInfo(irs::Directory& dir,
                                       const std::string& key) {
    irs::ColReader r{dir, std::string{kSeg}, Db()};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    std::vector<std::string> out;
    irs::ReadContext ctx{r};
    irs::BlockWindow window{};
    uint64_t row = 0;
    for (const auto& block : col->DataBlocks()) {
      if (block.codec->type ==
          duckdb::CompressionType::COMPRESSION_COL_NUMERIC) {
        window = col->Locate(row, window);
        auto seg = col->OpenSegment(window.block, ctx);
        out.emplace_back(seg->GetCompressionFunction().get_segment_info(
          duckdb::QueryContext{}, *seg)[key]);
      }
      row += block.tuple_count;
    }
    return out;
  }

  uint64_t NumericBytes(irs::Directory& dir) {
    irs::ColReader r{dir, std::string{kSeg}, Db()};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    uint64_t bytes = 0;
    for (const auto& block : col->DataBlocks()) {
      bytes += block.byte_size;
    }
    return bytes;
  }

  std::vector<std::string> Transforms(irs::Directory& dir) {
    irs::ColReader r{dir, std::string{kSeg}, Db()};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    std::vector<std::string> out;
    irs::ReadContext ctx{r};
    irs::BlockWindow window{};
    uint64_t row = 0;
    for (const auto& block : col->DataBlocks()) {
      if (block.codec->type ==
          duckdb::CompressionType::COMPRESSION_COL_NUMERIC) {
        window = col->Locate(row, window);
        auto seg = col->OpenSegment(window.block, ctx);
        auto info = seg->GetCompressionFunction().get_segment_info(
          duckdb::QueryContext{}, *seg);
        out.emplace_back(info["transform"] + "/" + info["leaf"]);
      }
      row += block.tuple_count;
    }
    return out;
  }

  duckdb::DuckDB _db;
};

TEST_F(ColNumericCodecTest, EveryTypeEveryShapeEveryTier) {
  constexpr uint64_t kRows = 50000;
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    for (const auto& type : Types()) {
      for (const auto& shape : Shapes()) {
        SCOPED_TRACE(std::string{shape.name} + " " + type.ToString() + " " +
                     std::to_string(static_cast<int>(tier)));
        irs::MemoryDirectory dir{};
        Write(dir, type, {.tier = tier}, kRows, 8192, shape.gen);
        Verify(dir, type, kRows, shape.gen);
      }
    }
  }
}

TEST_F(ColNumericCodecTest, EveryTransformIsReachable) {
  constexpr uint64_t kRows = 40000;
  std::set<std::string> seen;
  for (const auto& shape : Shapes()) {
    for (const auto& type :
         {duckdb::LogicalType::BIGINT, duckdb::LogicalType::INTEGER}) {
      irs::MemoryDirectory dir{};
      Write(dir, type, {}, kRows, 8192, shape.gen);
      for (const auto& t : Transforms(dir)) {
        seen.insert(t.substr(0, t.find('/')));
      }
    }
  }
  for (const auto* transform : {"for", "delta", "rle", "dict_ffor"}) {
    EXPECT_TRUE(seen.contains(transform)) << transform;
  }
}

TEST_F(ColNumericCodecTest, PacksRunsAndCodesBelowByteWidths) {
  constexpr uint64_t kRows = 120000;
  struct Case {
    std::string_view shape;
    std::string_view expected;
    std::vector<irs::WriteTier> tiers;
  };
  const Case cases[] = {
    {"drifting_runs", "rle_ffor/none", {irs::WriteTier::Flush}},
    {"sparse_values",
     "dict_ffor/none",
     {irs::WriteTier::Flush, irs::WriteTier::Merge}},
  };
  for (const auto& [name, expected, tiers] : cases) {
    const auto& shape = *std::ranges::find_if(
      Shapes(), [&](const auto& s) { return s.name == name; });
    const duckdb::LogicalType types[] = {duckdb::LogicalType::INTEGER,
                                         duckdb::LogicalType::BIGINT};
    for (const auto tier : tiers) {
      for (const auto& type : types) {
        SCOPED_TRACE(std::string{name} + " " + type.ToString() + " " +
                     std::to_string(static_cast<int>(tier)));
        irs::MemoryDirectory dir{};
        Write(dir, type, {.tier = tier}, kRows, 65536, shape.gen);
        Verify(dir, type, kRows, shape.gen);
        const auto transforms = Transforms(dir);
        ASSERT_FALSE(transforms.empty());
        for (const auto& t : transforms) {
          EXPECT_EQ(t, expected);
        }
      }
    }
  }
}

TEST_F(ColNumericCodecTest, CorruptedRunCountIsRejected) {
  const auto path = test_dir() / "col_numeric_corrupt_runs";
  std::filesystem::create_directories(path);
  const auto& shape = *std::ranges::find_if(Shapes(), [](const auto& s) {
    return s.name == std::string_view{"drifting_runs"};
  });
  uint64_t offset = 0;
  uint64_t size = 0;
  {
    irs::MMapDirectory dir{path};
    Write(dir, duckdb::LogicalType::BIGINT, {.tier = irs::WriteTier::Flush},
          60000, 60000, shape.gen);
    irs::ColReader r{dir, std::string{kSeg}, Db()};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    const auto& block = col->DataBlocks().front();
    ASSERT_EQ(block.codec->type,
              duckdb::CompressionType::COMPRESSION_COL_NUMERIC);
    offset = block.file_offset;
    size = block.byte_size;
  }
  const auto file = path / irs::FileName(kSeg);
  std::string bytes;
  {
    std::ifstream in{file, std::ios::binary};
    bytes.assign(std::istreambuf_iterator<char>{in}, {});
  }
  ASSERT_LE(offset + size, bytes.size());
  auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
  const auto h = irs::codecs::NumericHeader::Parse(block, size);
  ASSERT_EQ(h.transform, irs::codecs::NumericTransform::RleFfor);
  auto meta = irs::codecs::NumericFrameMeta::Load(block + h.off_frames);
  meta.base += 1;
  meta.Store(block + h.off_frames);
  {
    std::ofstream out{file, std::ios::binary | std::ios::trunc};
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  }
  irs::MMapDirectory dir{path};
  const auto scan = [&] {
    irs::ColReader r{dir, std::string{kSeg}, Db()};
    const auto* col = r.Column(kField);
    auto state = col->InitScan(r.Ctx());
    duckdb::Vector out{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    col->Scan(state, out, STANDARD_VECTOR_SIZE);
  };
#ifdef SDB_DEV
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(scan(), "numeric codec: corrupted");
#else
  EXPECT_ANY_THROW(scan());
#endif
}

TEST_F(ColNumericCodecTest, FforPacksEveryWidth) {
  constexpr uint64_t kRows = 40000;
  const std::pair<duckdb::LogicalType, std::vector<unsigned>> cases[] = {
    {duckdb::LogicalType::TINYINT, {1, 3, 5, 7}},
    {duckdb::LogicalType::UTINYINT, {1, 3, 6, 7}},
    {duckdb::LogicalType::SMALLINT, {1, 5, 9, 13, 15}},
    {duckdb::LogicalType::USMALLINT, {2, 11, 15}},
    {duckdb::LogicalType::INTEGER, {1, 7, 13, 19, 25, 31}},
    {duckdb::LogicalType::UINTEGER, {3, 17, 29, 31}},
    {duckdb::LogicalType::BIGINT, {1, 9, 21, 33, 47, 63}},
    {duckdb::LogicalType::UBIGINT, {5, 27, 41, 63}},
  };
  for (const auto& [type, widths] : cases) {
    const bool is_signed = type.IsSigned();
    for (const auto w : widths) {
      SCOPED_TRACE(type.ToString() + " width " + std::to_string(w));
      const Gen gen = [w, is_signed](uint64_t g) -> std::optional<int64_t> {
        const auto block = g / 1024;
        if (block % 5 == 3) {
          return static_cast<int64_t>(block);
        }
        const unsigned bits = block % 2 == 0 ? w : (w + 1) / 2;
        const auto off = static_cast<int64_t>(Mix(g) >> (64 - bits));
        const auto shift = static_cast<int64_t>(block % 3);
        return is_signed ? off - (int64_t{1} << (bits - 1)) + shift
                         : off + shift;
      };
      irs::MemoryDirectory dir{};
      Write(dir, type, {.tier = irs::WriteTier::Flush}, kRows, 32768, gen);
      Verify(dir, type, kRows, gen);
      const auto transforms = Transforms(dir);
      EXPECT_TRUE(std::ranges::find(transforms, "ffor/none") !=
                  transforms.end());
    }
  }
}

TEST_F(ColNumericCodecTest, RunsOfEveryLength) {
  constexpr uint64_t kRows = 50000;
  std::vector<int64_t> values;
  values.reserve(kRows);
  for (uint64_t run = 0; values.size() < kRows; ++run) {
    const auto length = 1 + run % 17;
    const auto v = static_cast<int64_t>(run % 4) * 1'000'003;
    for (uint64_t i = 0; i < length && values.size() < kRows; ++i) {
      values.push_back(v);
    }
  }
  const Gen gen = [&](uint64_t g) -> std::optional<int64_t> {
    return values[g];
  };
  const duckdb::LogicalType types[] = {duckdb::LogicalType::INTEGER,
                                       duckdb::LogicalType::BIGINT};
  for (const auto& type : types) {
    SCOPED_TRACE(type.ToString());
    irs::MemoryDirectory dir{};
    Write(dir, type, {}, kRows, 32768, gen);
    Verify(dir, type, kRows, gen);
    const auto transforms = Transforms(dir);
    EXPECT_TRUE(std::ranges::any_of(
      transforms, [](const auto& t) { return t.starts_with("rle/"); }));
  }
}

TEST_F(ColNumericCodecTest, CompactsClusteredTimestamps) {
  constexpr uint64_t kRows = 60000;
  const Gen clustered = [](uint64_t g) {
    return static_cast<int64_t>(1'700'000'000'000'000LL + (g / 4) * 1000 +
                                Mix(g / 4) % 3 * 100);
  };
  irs::MemoryDirectory dir{};
  Write(dir, duckdb::LogicalType::BIGINT, {}, kRows, 16384, clustered);
  irs::ColReader r{dir, std::string{kSeg}, Db()};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  uint64_t bytes = 0;
  for (const auto& block : col->DataBlocks()) {
    EXPECT_EQ(block.codec->type,
              duckdb::CompressionType::COMPRESSION_COL_NUMERIC);
    bytes += block.byte_size;
  }
  EXPECT_LT(bytes, kRows * 2);
}

TEST_F(ColNumericCodecTest, DividesOutACommonFactor) {
  constexpr uint64_t kRows = 60000;
  const Gen seconds = [](uint64_t g) -> std::optional<int64_t> {
    if (g % 13 == 5) {
      return std::nullopt;
    }
    return static_cast<int64_t>(1'700'000'000 + g / 3 + Mix(g) % 7);
  };
  const Gen micros = [&](uint64_t g) -> std::optional<int64_t> {
    const auto v = seconds(g);
    if (!v) {
      return std::nullopt;
    }
    return *v * 1'000'000;
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(static_cast<int>(tier));
    irs::MemoryDirectory plain_dir{};
    Write(plain_dir, duckdb::LogicalType::BIGINT, {.tier = tier}, kRows, 16384,
          seconds);
    irs::MemoryDirectory scaled_dir{};
    Write(scaled_dir, duckdb::LogicalType::BIGINT, {.tier = tier}, kRows, 16384,
          micros);
    Verify(scaled_dir, duckdb::LogicalType::BIGINT, kRows, micros);
    const auto scales = SegmentInfo(scaled_dir, "scale");
    ASSERT_FALSE(scales.empty());
    for (const auto& scale : scales) {
      EXPECT_EQ(scale, "1000000");
    }
    for (const auto& scale : SegmentInfo(plain_dir, "scale")) {
      EXPECT_EQ(scale, "1");
    }
    EXPECT_LE(NumericBytes(scaled_dir), NumericBytes(plain_dir) + 64 * 8);
  }
}

TEST_F(ColNumericCodecTest, SparseReadsCacheDecodedFrames) {
  constexpr uint64_t kRows = 60000;
  const Gen clustered = [](uint64_t g) {
    return static_cast<int64_t>(1'700'000'000'000'000LL + (g / 4) * 1000 +
                                Mix(g / 4) % 3 * 100);
  };
  irs::MemoryDirectory dir{};
  Write(dir, duckdb::LogicalType::BIGINT, {}, kRows, 65536, clustered);
  const auto transforms = Transforms(dir);
  ASSERT_TRUE(std::ranges::any_of(transforms, [](const auto& t) {
    return t.ends_with("/zstd") || t.ends_with("/lz4");
  }));
  auto& cache = Db().GetObjectCache();
  irs::ColReader r{dir, std::string{kSeg}, Db()};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  const auto cached_frames = [&] {
    size_t n = 0;
    for (size_t b = 0; b < col->DataBlocks().size(); ++b) {
      for (uint32_t f = 0; f < 256; ++f) {
        n += cache.Get<irs::codecs::DecodedFrame>(irs::codecs::FrameCacheKey(
               col->DictionaryCacheKey(b), f)) != nullptr;
      }
    }
    return n;
  };
  const auto lookups = [&] {
    irs::ColumnReader::PointReader cursor{r, *col};
    duckdb::Vector out{duckdb::LogicalType::BIGINT, 1};
    for (uint64_t g = 2501; g < kRows; g += 5003) {
      duckdb::FlatVector::ValidityMutable(out).Reset();
      ASSERT_TRUE(cursor.FetchRow(g, out, 0));
      ExpectRow(out, 0, g, clustered, duckdb::LogicalType::BIGINT);
    }
  };
  lookups();
  EXPECT_EQ(cached_frames(), 0u);
  {
    auto state = col->InitScan(r.Ctx());
    duckdb::Vector out{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      col->Scan(state, out,
                std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE));
    }
  }
  EXPECT_EQ(cached_frames(), 0u);
  lookups();
  const auto admitted = cached_frames();
  EXPECT_GT(admitted, 0u);
  lookups();
  EXPECT_EQ(cached_frames(), admitted);
}

TEST_F(ColNumericCodecTest, FiltersMatchTheValues) {
  constexpr uint64_t kRows = 60000;
  duckdb::Connection con{_db};
  using Cmp = duckdb::ExpressionType;
  const auto holds = [](Cmp cmp, int64_t v, int64_t key) {
    switch (cmp) {
      case Cmp::COMPARE_EQUAL:
        return v == key;
      case Cmp::COMPARE_NOTEQUAL:
        return v != key;
      case Cmp::COMPARE_LESSTHAN:
        return v < key;
      case Cmp::COMPARE_LESSTHANOREQUALTO:
        return v <= key;
      case Cmp::COMPARE_GREATERTHAN:
        return v > key;
      default:
        return v >= key;
    }
  };
  for (const auto& shape : Shapes()) {
    const auto type = duckdb::LogicalType::BIGINT;
    irs::MemoryDirectory dir{};
    Write(dir, type, {}, kRows, 8192, shape.gen);
    irs::ColReader r{dir, std::string{kSeg}, Db()};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    std::vector<int64_t> keys{0, 7};
    for (const uint64_t g : {uint64_t{1}, kRows / 2, kRows - 3}) {
      if (const auto v = shape.gen(g)) {
        keys.push_back(*v);
      }
    }
    for (const auto key : keys) {
      for (const auto cmp :
           {Cmp::COMPARE_EQUAL, Cmp::COMPARE_NOTEQUAL, Cmp::COMPARE_LESSTHAN,
            Cmp::COMPARE_LESSTHANOREQUALTO, Cmp::COMPARE_GREATERTHAN,
            Cmp::COMPARE_GREATERTHANOREQUALTO}) {
        const duckdb::ExpressionFilter filter{
          duckdb::BoundComparisonExpression::Create(
            cmp, duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0),
            duckdb::make_uniq<duckdb::BoundConstantExpression>(
              duckdb::Value::BIGINT(key)))};
        auto filter_state =
          duckdb::TableFilterState::Initialize(*con.context, filter);
        auto state = col->InitScan(r.Ctx());
        for (uint64_t anchor = 0; anchor < kRows;
             anchor += STANDARD_VECTOR_SIZE) {
          const auto span =
            std::min<uint64_t>(kRows - anchor, STANDARD_VECTOR_SIZE);
          duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
          for (duckdb::idx_t i = 0; i < span; ++i) {
            sel.set_index(i, i);
          }
          duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
          const auto kept =
            col->GatherFilter(state, anchor, span, sel, span, filter,
                              *filter_state, irs::NullCheckKind::None, out);
          duckdb::idx_t next = 0;
          for (duckdb::idx_t i = 0; i < span; ++i) {
            const auto v = shape.gen(anchor + i);
            const bool expected = v && holds(cmp, *v, key);
            const bool got = next < kept && sel.get_index(next) == i;
            next += got ? 1 : 0;
            ASSERT_EQ(got, expected)
              << shape.name << " row " << anchor + i << " key " << key
              << " cmp " << duckdb::EnumUtil::ToString(cmp);
            if (got) {
              ASSERT_EQ(out.GetValue(i), duckdb::Value::BIGINT(*v));
            }
          }
        }
      }
    }
  }
}

TEST_F(ColNumericCodecTest, AllNullAndSingleRow) {
  const Gen all_null = [](uint64_t) { return std::optional<int64_t>{}; };
  const Gen one = [](uint64_t g) { return static_cast<int64_t>(g + 41); };
  for (const auto& [gen, rows] :
       {std::pair{all_null, uint64_t{5000}}, std::pair{one, uint64_t{1}}}) {
    irs::MemoryDirectory dir{};
    Write(dir, duckdb::LogicalType::BIGINT, {}, rows, 4096, gen);
    Verify(dir, duckdb::LogicalType::BIGINT, rows, gen);
  }
}

TEST_F(ColNumericCodecTest, CorruptedFrameTableIsRejected) {
  const auto path = test_dir() / "col_numeric_corrupt";
  std::filesystem::create_directories(path);
  const Gen clustered = [](uint64_t g) {
    return static_cast<int64_t>(1'000'000 + g * 7 + Mix(g) % 3);
  };
  uint64_t offset = 0;
  uint64_t size = 0;
  {
    irs::MMapDirectory dir{path};
    Write(dir, duckdb::LogicalType::BIGINT, {}, 20000, 20000, clustered);
    irs::ColReader r{dir, std::string{kSeg}, Db()};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    const auto& block = col->DataBlocks().front();
    ASSERT_EQ(block.codec->type,
              duckdb::CompressionType::COMPRESSION_COL_NUMERIC);
    offset = block.file_offset;
    size = block.byte_size;
  }
  const auto file = path / irs::FileName(kSeg);
  std::string bytes;
  {
    std::ifstream in{file, std::ios::binary};
    bytes.assign(std::istreambuf_iterator<char>{in}, {});
  }
  ASSERT_LE(offset + size, bytes.size());
  auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
  const auto h = irs::codecs::NumericHeader::Parse(block, size);
  ASSERT_GT(h.frame_count, 0);
  auto meta = irs::codecs::NumericFrameMeta::Load(block + h.off_frames);
  meta.frame.comp_len = h.data_size + 1;
  meta.Store(block + h.off_frames);
  {
    std::ofstream out{file, std::ios::binary | std::ios::trunc};
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  }
  irs::MMapDirectory dir{path};
  const auto scan = [&] {
    irs::ColReader r{dir, std::string{kSeg}, Db()};
    const auto* col = r.Column(kField);
    auto state = col->InitScan(r.Ctx());
    duckdb::Vector out{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    col->Scan(state, out, STANDARD_VECTOR_SIZE);
  };
#ifdef SDB_DEV
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(scan(), "numeric codec: corrupted frame table");
#else
  EXPECT_ANY_THROW(scan());
#endif
}

}  // namespace
