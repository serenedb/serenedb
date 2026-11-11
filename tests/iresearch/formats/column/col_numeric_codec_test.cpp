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
#include <bit>
#include <cstdint>
#include <cstring>
#include <duckdb.hpp>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
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
#include <iresearch/formats/column/codecs/numeric.hpp>
#include <iresearch/formats/column/codecs/numeric_kernels.hpp>
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
#include <iterator>
#include <limits>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
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
    {"short_hash_runs",
     [](uint64_t g) { return static_cast<int64_t>(Mix(g / 3)); }},
    {"extremes",
     [](uint64_t g) -> std::optional<int64_t> {
       static constexpr int64_t kValues[] = {
         std::numeric_limits<int64_t>::min(),
         std::numeric_limits<int64_t>::max(),
         0,
         -1,
         1,
         -128,
         127,
         255,
         -32768,
         32767,
         65535,
         std::numeric_limits<int32_t>::min(),
         std::numeric_limits<int32_t>::max(),
         std::numeric_limits<uint32_t>::max(),
       };
       if (g % 7 == 3) {
         return std::nullopt;
       }
       const auto run = g % 3 == 0 ? g / 64 : Mix(g);
       return kValues[run % std::size(kValues)];
     }},
    {"min_and_zero",
     [](uint64_t g) {
       return g % 3 == 0 ? std::numeric_limits<int64_t>::min() : int64_t{0};
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
             const Gen& gen,
             duckdb::CompressionType compression =
               duckdb::CompressionType::COMPRESSION_AUTO) {
    irs::ColWriter w{dir, kSeg, Db(), params.tier};
    auto& cw = w.OpenColumn(kField, type,
                            {.row_group_size = rg_size,
                             .compression = compression,
                             .codec_params = params});
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
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
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
    for (const uint64_t stride : {uint64_t{2}, uint64_t{41}, uint64_t{5003}}) {
      irs::ColumnReader::PointReader cursor{r, *col};
      std::vector<uint64_t> picked;
      for (uint64_t g = stride / 2; g < rows; g += stride) {
        picked.push_back(g);
        if (g % 7 == 0 && g + 1 < rows) {
          picked.push_back(g + 1);
        }
      }
      for (size_t i = 0; i < picked.size(); i += STANDARD_VECTOR_SIZE) {
        const auto take =
          std::min<size_t>(picked.size() - i, STANDARD_VECTOR_SIZE);
        duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
        cursor.FetchRows(std::span<const uint64_t>{&picked[i], take}, out, 0);
        for (size_t k = 0; k < take; ++k) {
          ExpectRow(out, k, picked[i + k], gen, type);
        }
      }
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
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
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
        out.emplace_back(seg->function.get_segment_info(*seg)[key]);
      }
      row += block.tuple_count;
    }
    return out;
  }

  uint64_t NumericBytes(irs::Directory& dir) {
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    uint64_t bytes = 0;
    for (const auto& block : col->DataBlocks()) {
      bytes += block.byte_size;
    }
    return bytes;
  }

  std::vector<std::string> Transforms(irs::Directory& dir) {
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
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
        auto info = seg->function.get_segment_info(*seg);
        out.emplace_back(info["transform"] + "/" + info["leaf"]);
      }
      row += block.tuple_count;
    }
    return out;
  }

  template<typename F>
  void ForEachFrameKey(irs::ColReader& reader, const irs::ColumnReader& col,
                       F&& f) {
    const auto file_id = irs::tests::ColFileId(reader, col);
    std::string key;
    for (const auto& block : col.DataBlocks()) {
      const auto segment =
        irs::codecs::DictionaryCacheKey(file_id, block.file_offset);
      for (uint32_t frame = 0; frame < 256; ++frame) {
        irs::codecs::FrameCacheKey(segment, frame, key);
        f(key);
      }
    }
  }

  std::vector<duckdb::shared_ptr<irs::codecs::DecodedFrame>> CachedFrames(
    irs::ColReader& reader, const irs::ColumnReader& col) {
    auto& cache = Db().GetObjectCache();
    std::vector<duckdb::shared_ptr<irs::codecs::DecodedFrame>> out;
    ForEachFrameKey(reader, col, [&](const std::string& key) {
      if (auto frame = cache.Get<irs::codecs::DecodedFrame>(key)) {
        out.emplace_back(std::move(frame));
      }
    });
    return out;
  }

  uint64_t CachedBytes(irs::ColReader& reader, const irs::ColumnReader& col) {
    uint64_t bytes = 0;
    for (const auto& frame : CachedFrames(reader, col)) {
      bytes += frame->Bytes().size();
    }
    return bytes;
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
    {"short_hash_runs", {}, {irs::WriteTier::Flush, irs::WriteTier::Merge}},
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
        if (expected.empty()) {
          EXPECT_LT(NumericBytes(dir) * 5,
                    kRows * duckdb::GetTypeIdSize(type.InternalType()) * 2);
          continue;
        }
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
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
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
  auto meta = duckdb::Load<irs::codecs::NumericFrameMeta>(block + h.off_frames);
  meta.base += 1;
  duckdb::Store(meta, block + h.off_frames);
  {
    std::ofstream out{file, std::ios::binary | std::ios::trunc};
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  }
  irs::MMapDirectory dir{path};
  const auto scan = [&] {
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
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

TEST_F(ColNumericCodecTest, DictFforBaseThatWrapsTheCodeRangeIsRejected) {
  const auto path = test_dir() / "col_numeric_corrupt_dict_ffor";
  std::filesystem::create_directories(path);
  const auto& shape = *std::ranges::find_if(Shapes(), [](const auto& s) {
    return s.name == std::string_view{"sparse_values"};
  });
  uint64_t offset = 0;
  uint64_t size = 0;
  {
    irs::MMapDirectory dir{path};
    Write(dir, duckdb::LogicalType::BIGINT, {.tier = irs::WriteTier::Flush},
          60000, 60000, shape.gen);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
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
  ASSERT_EQ(h.transform, irs::codecs::NumericTransform::DictFfor);
  const auto frame =
    duckdb::Load<irs::codecs::NumericFrameMeta>(block + h.off_frames).frame;
  auto* first_block = block + h.off_data + frame.comp_off;
  const unsigned bits = first_block[sizeof(uint64_t)];
  ASSERT_GT(bits, 0U);
  duckdb::Store<uint64_t>(uint64_t{0} - ((uint64_t{1} << bits) - 1),
                          first_block);
  {
    std::ofstream out{file, std::ios::binary | std::ios::trunc};
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  }
  irs::MMapDirectory dir{path};
  const auto scan = [&] {
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    auto state = col->InitScan(r.Ctx());
    duckdb::Vector out{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    col->Scan(state, out, STANDARD_VECTOR_SIZE);
  };
#ifdef SDB_DEV
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(scan(), "numeric codec: corrupted codes");
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
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
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

TEST_F(ColNumericCodecTest, AutoFollowsTheDataAcrossRowGroups) {
  constexpr uint32_t kRowGroup = 2048;
  constexpr uint64_t kFirst = 20 * kRowGroup;
  constexpr uint64_t kRandom = 20 * kRowGroup;
  constexpr uint64_t kRows = kFirst + kRandom + 40 * kRowGroup;
  constexpr uint64_t kMaxLag = 17 * kRowGroup;
  const Gen gen = [](uint64_t g) -> std::optional<int64_t> {
    if (g >= kFirst && g < kFirst + kRandom) {
      return static_cast<int64_t>(Mix(g));
    }
    return static_cast<int64_t>(g % 1000) * 1'000'003;
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Flush ? "flush" : "merge");
    irs::MemoryDirectory dir{};
    Write(dir, duckdb::LogicalType::BIGINT, {.tier = tier}, kRows, kRowGroup,
          gen);
    Verify(dir, duckdb::LogicalType::BIGINT, kRows, gen);

    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    std::optional<uint64_t> back;
    uint64_t row = 0;
    for (const auto& block : col->DataBlocks()) {
      const bool numeric =
        block.codec->type == duckdb::CompressionType::COMPRESSION_COL_NUMERIC;
      if (row < kFirst) {
        EXPECT_TRUE(numeric) << "row " << row;
      } else if (row < kFirst + kRandom) {
        if (row >= kFirst + kRowGroup) {
          EXPECT_FALSE(numeric) << "row " << row;
        }
      } else if (back) {
        EXPECT_TRUE(numeric) << "row " << row;
      } else if (numeric) {
        back = row;
      }
      row += block.tuple_count;
    }
    ASSERT_TRUE(back.has_value());
    EXPECT_LE(*back, kFirst + kRandom + kMaxLag);
  }
}

TEST_F(ColNumericCodecTest,
       CompactionReturnsToTheNumericCodecWhenTheDataChanges) {
  constexpr uint32_t kRowGroup = 2048;
  constexpr uint64_t kRandom = 40 * kRowGroup;
  constexpr uint64_t kRows = kRandom + 16 * kRowGroup;
  const Gen gen = [](uint64_t g) -> std::optional<int64_t> {
    if (g < kRandom) {
      return static_cast<int64_t>(Mix(g));
    }
    return static_cast<int64_t>(g % 1000) * 1'000'003;
  };
  irs::MemoryDirectory dir{};
  Write(dir, duckdb::LogicalType::BIGINT, {.tier = irs::WriteTier::Merge},
        kRows, kRowGroup, gen);
  Verify(dir, duckdb::LogicalType::BIGINT, kRows, gen);

  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  uint64_t row = 0;
  uint64_t changed = 0;
  for (const auto& block : col->DataBlocks()) {
    const bool numeric =
      block.codec->type == duckdb::CompressionType::COMPRESSION_COL_NUMERIC;
    if (row < kRandom) {
      EXPECT_FALSE(numeric) << "row " << row;
    } else {
      ++changed;
      EXPECT_TRUE(numeric) << "row " << row;
    }
    row += block.tuple_count;
  }
  EXPECT_GT(changed, 0u);
}

TEST_F(ColNumericCodecTest, ForcedRleLeavesRowGroupsWithoutRepeatsPlain) {
  constexpr uint32_t kRowGroup = 4096;
  constexpr uint64_t kRows = 5 * kRowGroup + 700;
  const Gen unique = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(g * 3 + 1);
  };
  const Gen runs = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(g / 64);
  };
  const Gen one_repeat = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(g == 9000 ? 8999 * 3 + 1 : g * 3 + 1);
  };
  const Gen one_null = [](uint64_t g) -> std::optional<int64_t> {
    if (g == 9000) {
      return std::nullopt;
    }
    return static_cast<int64_t>(g * 3 + 1);
  };
  const auto codecs = [&](const Gen& gen, irs::WriteTier tier) {
    irs::MemoryDirectory dir{};
    Write(dir, duckdb::LogicalType::BIGINT, {.tier = tier}, kRows, kRowGroup,
          gen, duckdb::CompressionType::COMPRESSION_RLE);
    Verify(dir, duckdb::LogicalType::BIGINT, kRows, gen);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    std::vector<duckdb::CompressionType> types;
    for (const auto& block : col->DataBlocks()) {
      types.push_back(block.codec->type);
    }
    return types;
  };
  using enum duckdb::CompressionType;
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Flush ? "flush" : "merge");
    EXPECT_EQ(codecs(unique, tier), std::vector(6, COMPRESSION_UNCOMPRESSED));
    EXPECT_EQ(codecs(runs, tier), std::vector(6, COMPRESSION_RLE));
    const std::vector mixed{COMPRESSION_UNCOMPRESSED, COMPRESSION_UNCOMPRESSED,
                            COMPRESSION_RLE,          COMPRESSION_UNCOMPRESSED,
                            COMPRESSION_UNCOMPRESSED, COMPRESSION_UNCOMPRESSED};
    EXPECT_EQ(codecs(one_repeat, tier), mixed);
    EXPECT_EQ(codecs(one_null, tier), mixed);
  }
}

TEST_F(ColNumericCodecTest, ForcedBitpackingLeavesAnOverflowingRangePlain) {
  constexpr uint32_t kRowGroup = 4096;
  constexpr uint64_t kRows = 4 * kRowGroup;
  const Gen wide = [](uint64_t g) -> std::optional<int64_t> {
    return g % 3 == 0 ? std::numeric_limits<int64_t>::min()
                      : std::numeric_limits<int64_t>::max() -
                          static_cast<int64_t>(g % 5);
  };
  const Gen narrow = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g) % 3000) - 1500;
  };
  const auto codecs = [&](const duckdb::LogicalType& type, const Gen& gen) {
    irs::MemoryDirectory dir{};
    Write(dir, type, {}, kRows, kRowGroup, gen,
          duckdb::CompressionType::COMPRESSION_BITPACKING);
    Verify(dir, type, kRows, gen);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    std::set<duckdb::CompressionType> types;
    for (const auto& block : col->DataBlocks()) {
      types.insert(block.codec->type);
    }
    return types;
  };
  using enum duckdb::CompressionType;
  EXPECT_EQ(codecs(duckdb::LogicalType::BIGINT, wide),
            std::set{COMPRESSION_UNCOMPRESSED});
  EXPECT_EQ(codecs(duckdb::LogicalType::BIGINT, narrow),
            std::set{COMPRESSION_BITPACKING});
  EXPECT_EQ(codecs(duckdb::LogicalType::UBIGINT, wide),
            std::set{COMPRESSION_BITPACKING});
}

TEST_F(ColNumericCodecTest, ValidityIsRoaringOnlyBelowHalfTheBitmap) {
  constexpr uint32_t kRowGroup = 8192;
  constexpr uint64_t kRows = 4 * kRowGroup;
  const Gen sparse = [](uint64_t g) -> std::optional<int64_t> {
    if (g % 1000 == 7) {
      return std::nullopt;
    }
    return static_cast<int64_t>(g);
  };
  const Gen scattered = [](uint64_t g) -> std::optional<int64_t> {
    if (Mix(g) % 2 == 0) {
      return std::nullopt;
    }
    return static_cast<int64_t>(g);
  };
  const auto validity = [&](const Gen& gen) {
    irs::MemoryDirectory dir{};
    Write(dir, duckdb::LogicalType::BIGINT, {}, kRows, kRowGroup, gen);
    Verify(dir, duckdb::LogicalType::BIGINT, kRows, gen);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    EXPECT_NE(col->Validity(), nullptr);
    std::set<duckdb::CompressionType> types;
    for (const auto& block : col->Validity()->DataBlocks()) {
      types.insert(block.codec->type);
    }
    return types;
  };
  using enum duckdb::CompressionType;
  EXPECT_EQ(validity(sparse), std::set{COMPRESSION_ROARING});
  EXPECT_EQ(validity(scattered), std::set{COMPRESSION_UNCOMPRESSED});
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
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
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
  EXPECT_TRUE(CachedFrames(r, *col).empty());
  lookups();
  const auto admitted = CachedFrames(r, *col);
  EXPECT_FALSE(admitted.empty());
  lookups();
  EXPECT_EQ(CachedFrames(r, *col), admitted);
}

TEST_F(ColNumericCodecTest, PointReadsInDescendingOrder) {
  constexpr uint64_t kRows = 50000;
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    for (const auto& shape : Shapes()) {
      SCOPED_TRACE(std::string{shape.name} + " " +
                   std::to_string(static_cast<int>(tier)));
      irs::MemoryDirectory dir{};
      Write(dir, duckdb::LogicalType::BIGINT, {.tier = tier}, kRows, 8192,
            shape.gen);
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      for (int pass = 0; pass < 3; ++pass) {
        irs::ColumnReader::PointReader cursor{r, *col};
        duckdb::Vector out{duckdb::LogicalType::BIGINT, 1};
        for (uint64_t g = kRows; g != 0;) {
          g = g > 31 ? g - 31 : 0;
          duckdb::FlatVector::ValidityMutable(out).Reset();
          EXPECT_EQ(cursor.FetchRow(g, out, 0), shape.gen(g).has_value()) << g;
          ExpectRow(out, 0, g, shape.gen, duckdb::LogicalType::BIGINT);
        }
      }
    }
  }
}

TEST_F(ColNumericCodecTest, ScansCacheDecodedFrames) {
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
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  const auto scan = [&] {
    auto state = col->InitScan(r.Ctx());
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      const auto take = std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector out{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, take);
      out.Flatten(take);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        ExpectRow(out, k, pos + k, clustered, duckdb::LogicalType::BIGINT);
      }
    }
  };
  scan();
  EXPECT_TRUE(CachedFrames(r, *col).empty());
  scan();
  const auto admitted = CachedFrames(r, *col);
  ASSERT_FALSE(admitted.empty());
  scan();
  EXPECT_EQ(CachedFrames(r, *col), admitted);
}

TEST_F(ColNumericCodecTest, ScansLeaveHalfTheFrameCacheToPointReads) {
  constexpr uint64_t kRows = 4 * 65536;
  const Gen clustered = [](uint64_t g) {
    return static_cast<int64_t>(1'700'000'000'000'000LL + (g / 4) * 1000 +
                                Mix(g / 4) % 3 * 100);
  };
  irs::MemoryDirectory dir{};
  Write(dir, duckdb::LogicalType::BIGINT, {}, kRows, 65536, clustered);
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  const auto scan = [&] {
    auto state = col->InitScan(r.Ctx());
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector out{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      col->Scan(state, out,
                std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE));
    }
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
  scan();
  scan();
  ASSERT_GT(CachedFrames(r, *col).size(), 2u);
  const auto frames = CachedBytes(r, *col);
  auto& cache = Db().GetObjectCache();
  ForEachFrameKey(r, *col, [&](const std::string& key) { cache.Delete(key); });
  ASSERT_EQ(CachedBytes(r, *col), 0u);
  cache.SetMaxMemory(2 * cache.GetCurrentMemory() + frames);
  scan();
  EXPECT_GT(CachedBytes(r, *col), 0u);
  EXPECT_LE(cache.GetCurrentMemory(), cache.GetMaxMemory() / 2);
  lookups();
  EXPECT_GT(cache.GetCurrentMemory(), cache.GetMaxMemory() / 2);
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
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
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

TEST_F(ColNumericCodecTest, ConstantRowGroupsAreConstantBlocks) {
  constexpr uint32_t kRowGroup = 2048;
  constexpr bool kConstant[] = {true,  true, true,  false,
                                false, true, false, true};
  constexpr uint64_t kRows = std::size(kConstant) * kRowGroup;
  const Gen gen = [](uint64_t g) -> std::optional<int64_t> {
    const auto row = static_cast<int64_t>(g % kRowGroup);
    switch (g / kRowGroup) {
      case 0:
        return 7;
      case 1:
        if (row % 3 == 0) {
          return std::nullopt;
        }
        return 9;
      case 2:
        return std::nullopt;
      case 3:
        return static_cast<int64_t>(Mix(g) % 1000);
      case 4:
        return row + 1 == kRowGroup ? 12 : 11;
      case 5:
        return 13;
      case 6:
        return row;
      default:
        return 7;
    }
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    for (const auto& type : Types()) {
      SCOPED_TRACE(type.ToString() +
                   (tier == irs::WriteTier::Flush ? " flush" : " merge"));
      irs::MemoryDirectory dir{};
      Write(dir, type, {.tier = tier}, kRows, kRowGroup, gen);
      Verify(dir, type, kRows, gen);

      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      uint64_t row = 0;
      for (const auto& block : col->DataBlocks()) {
        const bool constant =
          block.codec->type == duckdb::CompressionType::COMPRESSION_CONSTANT;
        EXPECT_EQ(constant, kConstant[row / kRowGroup]) << "row " << row;
        if (constant) {
          EXPECT_EQ(block.byte_size, 0u) << "row " << row;
        }
        row += block.tuple_count;
      }
      EXPECT_EQ(row, kRows);
    }
  }
}

TEST_F(ColNumericCodecTest, ConstantRowGroupsKeepZeroSigns) {
  constexpr uint32_t kRowGroup = 2048;
  constexpr bool kConstant[] = {true, false, true};
  constexpr uint64_t kRows = std::size(kConstant) * kRowGroup;
  const auto check = [&]<typename T, typename Bits>(
                       const duckdb::LogicalType& type, irs::WriteTier tier) {
    SCOPED_TRACE(type.ToString() +
                 (tier == irs::WriteTier::Flush ? " flush" : " merge"));
    const auto at = [](uint64_t g) -> T {
      switch (g / kRowGroup) {
        case 0:
          return -T{0};
        case 1:
          return g % 2 == 0 ? T{0} : -T{0};
        default:
          return T{0};
      }
    };
    irs::MemoryDirectory dir{};
    {
      irs::ColWriter w{dir, kSeg, Db(), tier};
      auto& cw = w.OpenColumn(
        kField, type,
        {.row_group_size = kRowGroup, .codec_params = {.tier = tier}});
      for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
        duckdb::Vector vec{type, STANDARD_VECTOR_SIZE};
        auto* data = duckdb::FlatVector::GetDataMutable<T>(vec);
        for (uint64_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
          data[k] = at(pos + k);
        }
        duckdb::FlatVector::SetSize(vec, STANDARD_VECTOR_SIZE);
        cw.Append(vec, STANDARD_VECTOR_SIZE);
      }
      ASSERT_TRUE(w.Commit(0));
    }
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    uint64_t row = 0;
    for (const auto& block : col->DataBlocks()) {
      EXPECT_EQ(
        block.codec->type == duckdb::CompressionType::COMPRESSION_CONSTANT,
        kConstant[row / kRowGroup])
        << "row " << row;
      row += block.tuple_count;
    }
    auto state = col->InitScan(r.Ctx());
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
      out.Flatten(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
        EXPECT_EQ(std::bit_cast<Bits>(duckdb::FlatVector::GetData<T>(out)[k]),
                  std::bit_cast<Bits>(at(pos + k)))
          << "row " << pos + k;
      }
    }
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    check.template operator()<float, uint32_t>(duckdb::LogicalType::FLOAT,
                                               tier);
    check.template operator()<double, uint64_t>(duckdb::LogicalType::DOUBLE,
                                                tier);
  }
}

TEST_F(ColNumericCodecTest, FloatSpecialsKeepTheirBits) {
  constexpr uint64_t kRows = 20000;
  const auto check = [&]<typename T, typename Bits>(
                       const duckdb::LogicalType& type, irs::WriteTier tier) {
    SCOPED_TRACE(type.ToString() + " " +
                 std::to_string(static_cast<int>(tier)));
    const T values[] = {
      std::numeric_limits<T>::quiet_NaN(),
      -std::numeric_limits<T>::quiet_NaN(),
      T{0},
      -T{0},
      std::numeric_limits<T>::infinity(),
      -std::numeric_limits<T>::infinity(),
      std::numeric_limits<T>::denorm_min(),
      std::numeric_limits<T>::max(),
      std::numeric_limits<T>::lowest(),
      T{1.5},
    };
    const auto at = [&](uint64_t g) -> std::optional<T> {
      if (g % 11 == 4) {
        return std::nullopt;
      }
      return values[(g % 2 == 0 ? g / 50 : Mix(g)) % std::size(values)];
    };
    irs::MemoryDirectory dir{};
    {
      irs::ColWriter w{dir, kSeg, Db(), tier};
      auto& cw = w.OpenColumn(
        kField, type, {.row_group_size = 4096, .codec_params = {.tier = tier}});
      for (uint64_t pos = 0; pos < kRows;) {
        const auto take = std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE);
        duckdb::Vector vec{type, STANDARD_VECTOR_SIZE};
        auto* data = duckdb::FlatVector::GetDataMutable<T>(vec);
        auto& validity = duckdb::FlatVector::ValidityMutable(vec);
        validity.Reset(STANDARD_VECTOR_SIZE);
        for (uint64_t k = 0; k < take; ++k) {
          if (const auto v = at(pos + k)) {
            data[k] = *v;
          } else {
            validity.SetInvalid(k);
          }
        }
        duckdb::FlatVector::SetSize(vec, take);
        cw.Append(vec, take);
        pos += take;
      }
      ASSERT_TRUE(w.Commit(0));
    }
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    const auto expect = [&](const duckdb::Vector& out, duckdb::idx_t k,
                            uint64_t g) {
      const auto want = at(g);
      ASSERT_EQ(duckdb::FlatVector::Validity(out).RowIsValid(k),
                want.has_value())
        << "row " << g;
      if (want) {
        EXPECT_EQ(std::bit_cast<Bits>(duckdb::FlatVector::GetData<T>(out)[k]),
                  std::bit_cast<Bits>(*want))
          << "row " << g;
      }
    };
    auto state = col->InitScan(r.Ctx());
    for (uint64_t pos = 0; pos < kRows;) {
      const auto take = std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, take);
      out.Flatten(take);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        expect(out, k, pos + k);
      }
      pos += take;
    }
    irs::ColumnReader::PointReader cursor{r, *col};
    duckdb::Vector one{type, 1};
    for (uint64_t g = 0; g < kRows; g += 13) {
      duckdb::FlatVector::ValidityMutable(one).Reset();
      cursor.FetchRow(g, one, 0);
      expect(one, 0, g);
    }
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    check.template operator()<float, uint32_t>(duckdb::LogicalType::FLOAT,
                                               tier);
    check.template operator()<double, uint64_t>(duckdb::LogicalType::DOUBLE,
                                                tier);
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
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
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
  auto meta = duckdb::Load<irs::codecs::NumericFrameMeta>(block + h.off_frames);
  meta.frame.comp_len = h.data_size + 1;
  duckdb::Store(meta, block + h.off_frames);
  {
    std::ofstream out{file, std::ios::binary | std::ios::trunc};
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  }
  irs::MMapDirectory dir{path};
  const auto scan = [&] {
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
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

TEST(ColNumericCodesTest, EstimateFollowsWhatTheEncoderWrote) {
  constexpr uint64_t kEntries = 300;
  for (const uint64_t run : {uint64_t{1}, uint64_t{64}}) {
    SCOPED_TRACE(run);
    std::vector<uint32_t> codes(16384);
    uint64_t runs = 0;
    for (size_t i = 0; i != codes.size(); ++i) {
      codes[i] = static_cast<uint32_t>((i / run) * 7 % kEntries);
      runs += i == 0 || codes[i] != codes[i - 1] ? 1 : 0;
    }
    irs::codecs::NumericTuning tuning;
    const auto bytes =
      irs::codecs::EncodeCodes(codes, kEntries, runs, tuning).size();
    const auto estimate =
      irs::codecs::EstimateCodes(tuning, codes.size(), kEntries, runs);
    EXPECT_LE(estimate, bytes);
    EXPECT_GE(estimate + 1, bytes);
  }
}

TEST(ColNumericKernelsTest, PrefixSumOfEveryWidthAndLength) {
  static constexpr uint8_t kWidths[] = {1, 2, 4, 8};
  const auto check = []<typename U>() {
    for (const auto stored : kWidths) {
      if (stored > sizeof(U)) {
        continue;
      }
      for (size_t n = 0; n <= 37; ++n) {
        SCOPED_TRACE(testing::Message()
                     << sizeof(U) << " " << int{stored} << " " << n);
        std::vector<uint8_t> in(n * stored);
        for (size_t i = 0; i != in.size(); ++i) {
          in[i] = static_cast<uint8_t>(Mix(i + n * 131 + stored));
        }
        const auto acc = static_cast<U>(Mix(n));
        const auto offset = static_cast<U>(~U{0} - stored);
        std::vector<U> out(n + 1, U{0x5a});
        const auto last = irs::codecs::numeric::WidenPrefixSum<true>(
          in.data(), n, stored, acc, offset, out.data());
        U expected = acc;
        for (size_t i = 0; i != n; ++i) {
          U value = 0;
          std::memcpy(&value, in.data() + i * stored, stored);
          expected = static_cast<U>(expected + static_cast<U>(value + offset));
          ASSERT_EQ(out[i], expected) << i;
        }
        EXPECT_EQ(last, expected);
        EXPECT_EQ(out[n], U{0x5a});
        EXPECT_EQ(
          irs::codecs::numeric::WidenPrefixSum<false>(
            in.data(), n, stored, acc, offset, static_cast<U*>(nullptr)),
          expected);
      }
    }
  };
  check.template operator()<uint64_t>();
  check.template operator()<uint32_t>();
}

}  // namespace
