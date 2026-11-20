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

#include <absl/algorithm/container.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <duckdb.hpp>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iresearch/formats/column/codecs/dictionary_cache.hpp>
#include <iresearch/formats/column/codecs/numeric/numeric_kernels.hpp>
#include <iresearch/formats/column/codecs/numeric/numeric_layout.hpp>
#include <iresearch/formats/column/codecs/registry.hpp>
#include <iresearch/formats/column/codecs/text/string_layout.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/internal/gather_arms.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iterator>
#include <limits>
#include <magic_enum/magic_enum.hpp>
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
    case duckdb::LogicalTypeId::BOOLEAN:
      return duckdb::Value::BOOLEAN(v != 0);
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
    case duckdb::LogicalTypeId::HUGEINT:
      return duckdb::Value::HUGEINT(duckdb::hugeint_t{v});
    case duckdb::LogicalTypeId::UHUGEINT:
      return duckdb::Value::UHUGEINT(
        duckdb::uhugeint_t{static_cast<uint64_t>(v)});
    case duckdb::LogicalTypeId::UUID:
      return duckdb::Value::UUID(
        duckdb::hugeint_t{v, Mix(static_cast<uint64_t>(v))});
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

template<typename Read>
void ExpectCorrupted(Read&& read, std::string_view message) {
  try {
    read();
    ADD_FAILURE() << "a damaged segment was read";
  } catch (const irs::SqlException& e) {
    EXPECT_EQ(e.error().errcode, ERRCODE_INDEX_CORRUPTED) << e.what();
    EXPECT_EQ(e.message(), message);
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

  void Write(
    irs::Directory& dir, const duckdb::LogicalType& type,
    irs::ColCodecParams params, uint64_t rows, uint32_t rg_size, const Gen& gen,
    irs::codecs::CodecType compression = irs::codecs::CodecType::Auto) {
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
      std::vector<uint32_t> picked;
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
        cursor.FetchRows(std::span<const uint32_t>{&picked[i], take}, 0, out,
                         0);
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
      if (irs::codecs::IsNumeric(block.codec->type)) {
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
      if (irs::codecs::IsNumeric(block.codec->type)) {
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
  for (const auto* transform : {"for", "rle", "dict_bitpacking"}) {
    EXPECT_TRUE(seen.contains(transform)) << transform;
  }
}

TEST_F(ColNumericCodecTest, EveryNamedCodecStoresWhatItCanAndReadsBack) {
  constexpr uint64_t kRows = 20000;
  using irs::codecs::CodecType;
  for (const auto codec :
       {CodecType::Bitpacking, CodecType::Lz4, CodecType::Zstd,
        CodecType::Delta, CodecType::DeltaLz4, CodecType::DeltaZstd,
        CodecType::Rle, CodecType::RleLz4, CodecType::RleZstd, CodecType::Dict,
        CodecType::DictLz4, CodecType::DictZstd}) {
    uint64_t named = 0;
    for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
      for (const auto& type :
           {duckdb::LogicalType::BIGINT, duckdb::LogicalType::INTEGER,
            duckdb::LogicalType::DOUBLE}) {
        for (const auto& shape : Shapes()) {
          SCOPED_TRACE(std::string{magic_enum::enum_name(codec)} + " " +
                       shape.name + " " + duckdb::LogicalType{type}.ToString() +
                       " " + std::to_string(static_cast<int>(tier)));
          irs::MemoryDirectory dir{};
          Write(dir, type, {.tier = tier}, kRows, 4096, shape.gen, codec);
          Verify(dir, type, kRows, shape.gen);
          irs::ColReader r{dir, std::string{kSeg}, Db(),
                           irs::IOAdvice::SEQUENTIAL};
          const auto* col = r.Column(kField);
          ASSERT_NE(col, nullptr);
          for (const auto& block : col->DataBlocks()) {
            const auto stored = block.codec->type;
            EXPECT_TRUE(stored == codec || stored == CodecType::Constant ||
                        stored == CodecType::Uncompressed)
              << magic_enum::enum_name(stored);
            named += stored == codec;
          }
        }
      }
    }
    EXPECT_GT(named, 0u) << magic_enum::enum_name(codec);
  }
}

TEST_F(ColNumericCodecTest, ANamedLeafCodecCompressesAtTheColumnsLevel) {
  constexpr uint64_t kRows = 100000;
  std::string words;
  for (uint64_t k = 0; words.size() < kRows; ++k) {
    const auto word = Mix(k) % 512;
    for (uint64_t i = 0, n = 3 + Mix(word) % 7; i < n; ++i) {
      words += static_cast<char>('a' + Mix(word * 16 + i) % 26);
    }
    words += ' ';
  }
  const Gen gen = [&words](uint64_t g) -> std::optional<int64_t> {
    return words[g / 3];
  };
  using irs::codecs::CodecType;
  const auto type = duckdb::LogicalType::BIGINT;
  for (const auto codec : {CodecType::Zstd, CodecType::RleLz4}) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    uint64_t bytes[2] = {};
    for (const uint8_t level : {uint8_t{0}, uint8_t{9}}) {
      irs::MemoryDirectory dir{};
      Write(dir, type, {.compression_level = level}, kRows, 16384, gen, codec);
      Verify(dir, type, kRows, gen);
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      irs::ReadContext ctx{r};
      irs::BlockWindow window{};
      uint64_t row = 0;
      for (const auto& block : col->DataBlocks()) {
        ASSERT_EQ(block.codec->type, codec);
        window = col->Locate(row, window);
        auto seg = col->OpenSegment(window.block, ctx);
        auto info = seg->function.get_segment_info(*seg);
        EXPECT_EQ(info["level"], level ? "9" : "1");
        bytes[level != 0] += std::stoull(info["data_bytes"]);
        row += block.tuple_count;
      }
    }
    EXPECT_LT(bytes[1], bytes[0]);
  }
}

TEST_F(ColNumericCodecTest, ANamedLeafCodecHoldsItsLayoutUntilItsBytesGrow) {
  constexpr uint32_t kRowGroup = 4096;
  constexpr uint64_t kSwitch = 8 * kRowGroup;
  constexpr uint64_t kRows = 2 * kSwitch;
  const Gen gen = [](uint64_t g) -> std::optional<int64_t> {
    const auto value = static_cast<int64_t>(Mix(g) % 200);
    return g < kSwitch ? value : value << 24 | static_cast<int64_t>(g & 1);
  };
  const auto type = duckdb::LogicalType::BIGINT;
  irs::MemoryDirectory dir{};
  Write(dir, type, {}, kRows, kRowGroup, gen, irs::codecs::CodecType::Zstd);
  Verify(dir, type, kRows, gen);
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  irs::ReadContext ctx{r};
  irs::BlockWindow window{};
  uint64_t row = 0;
  std::vector<std::string> layouts;
  for (const auto& block : col->DataBlocks()) {
    ASSERT_EQ(block.codec->type, irs::codecs::CodecType::Zstd);
    window = col->Locate(row, window);
    auto seg = col->OpenSegment(window.block, ctx);
    auto info = seg->function.get_segment_info(*seg);
    layouts.push_back(info["stored"] + "/" + info["shuffled"]);
    row += block.tuple_count;
  }
  ASSERT_EQ(layouts.size(), kRows / kRowGroup);
  const auto first = layouts.begin() + kSwitch / kRowGroup;
  EXPECT_TRUE(std::all_of(layouts.begin(), first,
                          [](const auto& l) { return l == "1/false"; }))
    << ::testing::PrintToString(layouts);
  EXPECT_EQ(*first, "4/false") << ::testing::PrintToString(layouts);
  EXPECT_TRUE(std::all_of(first + 1, layouts.end(),
                          [](const auto& l) { return l.ends_with("/true"); }))
    << ::testing::PrintToString(layouts);
}

TEST_F(ColNumericCodecTest, ANamedCodecStoresEveryRowGroupItCan) {
  using irs::codecs::CodecType;
  const Gen narrow = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g) % 200);
  };
  const Gen pairs = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g % 3 == 1 ? g - 1 : g) % 100000);
  };
  const Gen apart = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(g * 977);
  };
  const auto type = duckdb::LogicalType::BIGINT;
  const auto stored = [&](const Gen& gen, CodecType codec, uint64_t rows,
                          uint32_t rg_size) {
    irs::MemoryDirectory dir{};
    Write(dir, type, {}, rows, rg_size, gen, codec);
    Verify(dir, type, rows, gen);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    std::set<CodecType> codecs;
    for (const auto& block : col->DataBlocks()) {
      codecs.insert(block.codec->type);
    }
    return codecs;
  };
  EXPECT_EQ(stored(narrow, CodecType::Dict, 40000, 16384),
            std::set{CodecType::Dict});
  EXPECT_EQ(stored(narrow, CodecType::DictZstd, 40000, 16384),
            std::set{CodecType::DictZstd});
  EXPECT_EQ(stored(pairs, CodecType::Rle, 40000, 16384),
            std::set{CodecType::Rle});
  EXPECT_EQ(stored(pairs, CodecType::RleLz4, 40000, 16384),
            std::set{CodecType::RleLz4});
  EXPECT_EQ(stored(apart, CodecType::Rle, 40000, 16384),
            std::set{CodecType::Uncompressed});
  EXPECT_EQ(stored(apart, CodecType::Dict, 262144, 131072),
            std::set{CodecType::Uncompressed});
}

TEST_F(ColNumericCodecTest, ClusteredValuesPackTheirDeltas) {
  constexpr uint64_t kRows = 100000;
  const Gen steps = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(1'000'000 + g * 3 + Mix(g) % 5);
  };
  const Gen falling = [](uint64_t g) -> std::optional<int64_t> {
    if (g % 97 == 13) {
      return std::nullopt;
    }
    return -static_cast<int64_t>(g * 977 + Mix(g) % 11);
  };
  struct Case {
    Gen gen;
    uint64_t most_bytes;
  };
  for (const auto& [gen, most_bytes] :
       {Case{steps, kRows * 6 / 8}, Case{falling, kRows * 12 / 8}}) {
    for (const auto& type :
         {duckdb::LogicalType::BIGINT, duckdb::LogicalType::INTEGER,
          duckdb::LogicalType::UBIGINT}) {
      SCOPED_TRACE(duckdb::LogicalType{type}.ToString());
      irs::MemoryDirectory packed{};
      Write(packed, type, {.tier = irs::WriteTier::Flush}, kRows, 16384, gen,
            irs::codecs::CodecType::Delta);
      Verify(packed, type, kRows, gen);
      const auto transforms = Transforms(packed);
      ASSERT_FALSE(transforms.empty());
      for (const auto& transform : transforms) {
        EXPECT_EQ(transform, "delta_bitpacking/none");
      }
      EXPECT_LT(NumericBytes(packed), most_bytes);
      irs::MemoryDirectory plain{};
      Write(plain, type, {.tier = irs::WriteTier::Flush}, kRows, 16384, gen,
            irs::codecs::CodecType::Bitpacking);
      Verify(plain, type, kRows, gen);
      if (gen(13)) {
        EXPECT_LE(NumericBytes(plain), NumericBytes(packed));
      }
    }
  }
}

TEST_F(ColNumericCodecTest, ABackwardStepWidensOnlyItsBlock) {
  constexpr uint64_t kRows = 32768;
  const Gen steady = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(1'700'000'000'000'000LL + g * 1000 +
                                Mix(g) % 16);
  };
  const Gen jumped = [&](uint64_t g) -> std::optional<int64_t> {
    return *steady(g) - (g >= 20000 ? 3'600'000'000LL : 0);
  };
  const auto data_bytes = [&](const Gen& gen) {
    const auto type = duckdb::LogicalType::BIGINT;
    irs::MemoryDirectory dir{};
    Write(dir, type, {.tier = irs::WriteTier::Flush}, kRows, kRows, gen,
          irs::codecs::CodecType::Delta);
    Verify(dir, type, kRows, gen);
    EXPECT_EQ(Transforms(dir),
              std::vector<std::string>{"delta_bitpacking/none"});
    return std::stoull(SegmentInfo(dir, "data_bytes").front());
  };
  const auto smooth = data_bytes(steady);
  EXPECT_LT(smooth, kRows);
  EXPECT_LT(data_bytes(jumped), smooth + 8 * 1024);
}

TEST_F(ColNumericCodecTest, BlocksOfEqualStepsReadBack) {
  constexpr uint64_t kRows = 100000;
  const Gen steps = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(g * 977 + g / 5000 * 1'000'000);
  };
  const Gen scaled = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>((g * 3 + g / 5000 * 1000) * 64);
  };
  const Gen sequence = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(g);
  };
  const Gen falling = [](uint64_t g) -> std::optional<int64_t> {
    return 1'000'000 - static_cast<int64_t>(g) * 3;
  };
  const Gen crossing = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(g) - 5;
  };
  const Gen faster_block = [](uint64_t g) -> std::optional<int64_t> {
    const uint64_t faster = g < 7168 ? 0 : std::min<uint64_t>(g - 7168, 1024);
    return static_cast<int64_t>(g * 3 + faster * 2);
  };
  for (const auto& gen :
       {steps, scaled, sequence, falling, crossing, faster_block}) {
    for (const auto& type :
         {duckdb::LogicalType::BIGINT, duckdb::LogicalType::INTEGER,
          duckdb::LogicalType::UBIGINT}) {
      SCOPED_TRACE(duckdb::LogicalType{type}.ToString());
      irs::MemoryDirectory dir{};
      Write(dir, type, {.tier = irs::WriteTier::Flush}, kRows, 16384, gen,
            irs::codecs::CodecType::Delta);
      Verify(dir, type, kRows, gen);
      for (const auto& transform : Transforms(dir)) {
        EXPECT_EQ(transform, "delta_bitpacking/none");
      }
    }
  }
  const auto data_bytes = [&](const Gen& gen) {
    irs::MemoryDirectory dir{};
    Write(dir, duckdb::LogicalType::BIGINT, {.tier = irs::WriteTier::Flush},
          kRows, 16384, gen, irs::codecs::CodecType::Delta);
    return SegmentInfo(dir, "data_bytes");
  };
  for (const auto& bytes : data_bytes(sequence)) {
    EXPECT_EQ(bytes, "0");
  }
  EXPECT_EQ(
    data_bytes(faster_block).front(),
    std::to_string(
      16 * (irs::codecs::numeric::BytesFor(uint64_t{16383 * 3 + 2048}) + 1) +
      sizeof(uint64_t)));
}

TEST_F(ColNumericCodecTest, PacksRunsAndCodesBelowByteWidths) {
  constexpr uint64_t kRows = 120000;
  struct Case {
    std::string_view shape;
    std::string_view expected;
    std::vector<irs::WriteTier> tiers;
  };
  const Case cases[] = {
    {"drifting_runs", "rle_bitpacking/none", {irs::WriteTier::Flush}},
    {"sparse_values",
     "dict_bitpacking/none",
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

TEST_F(ColNumericCodecTest, FrameHeadersWithImpossibleWidthsAreRejected) {
  struct Corruption {
    std::string_view name;
    duckdb::LogicalType type;
    irs::codecs::CodecType codec;
    Gen gen;
    std::function<void(irs::codecs::FrameHeader&)> patch;
  };
  const Gen narrow = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g) % 200);
  };
  const Corruption cases[] = {
    {"a value width of three bytes", duckdb::LogicalType::BIGINT,
     irs::codecs::CodecType::Bitpacking, narrow,
     [](irs::codecs::FrameHeader& h) { h.width = 3; }},
    {"two-byte codes of a two-byte dictionary", duckdb::LogicalType::SMALLINT,
     irs::codecs::CodecType::DictLz4, narrow,
     [](irs::codecs::FrameHeader& h) {
       ASSERT_EQ(irs::codecs::TransformName(irs::codecs::Shape::Dictionary, h),
                 "dict");
       h.stored = 2;
     }},
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    const auto path = test_dir() / "col_numeric_corrupt_widths";
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    uint64_t offset = 0;
    uint64_t size = 0;
    {
      irs::MMapDirectory dir{path};
      Write(dir, c.type, {.tier = irs::WriteTier::Flush}, 4096, 4096, c.gen,
            c.codec);
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      const auto& block = col->DataBlocks().front();
      ASSERT_EQ(block.codec->type, c.codec);
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
    auto h = irs::codecs::ParseFrames(block, size, 4096, c.codec);
    ASSERT_NO_FATAL_FAILURE(c.patch(h));
    std::memcpy(block, &h, irs::codecs::kNumericHeaderSize);
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    irs::MMapDirectory dir{path};
    const auto scan = [&] {
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
    };
    ExpectCorrupted(scan, "Corrupted numeric segment: header is inconsistent");
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
  uint64_t rows = 0;
  irs::codecs::CodecType block_codec = irs::codecs::CodecType::Auto;
  {
    irs::MMapDirectory dir{path};
    Write(dir, duckdb::LogicalType::BIGINT, {.tier = irs::WriteTier::Flush},
          60000, 60000, shape.gen);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    const auto& block = col->DataBlocks().front();
    ASSERT_TRUE(irs::codecs::IsNumeric(block.codec->type));
    offset = block.file_offset;
    size = block.byte_size;
    rows = block.tuple_count;
    block_codec = block.codec->type;
  }
  const auto file = path / irs::FileName(kSeg);
  std::string bytes;
  {
    std::ifstream in{file, std::ios::binary};
    bytes.assign(std::istreambuf_iterator<char>{in}, {});
  }
  ASSERT_LE(offset + size, bytes.size());
  auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
  const auto h = irs::codecs::ParseFrames(block, size, rows, block_codec);
  ASSERT_EQ(irs::codecs::TransformName(irs::codecs::ShapeOf(block_codec), h),
            "rle_bitpacking");
  auto* runs = block + h.off_frames + 2 * sizeof(uint32_t);
  duckdb::Store(static_cast<uint16_t>(duckdb::Load<uint16_t>(runs) + 1), runs);
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
  ExpectCorrupted(scan, "Corrupted numeric segment: runs are inconsistent");
}

TEST_F(ColNumericCodecTest, DictBitpackingBaseBeyondTheDictionaryIsRejected) {
  const auto path = test_dir() / "col_numeric_corrupt_dict_bitpacking";
  std::filesystem::create_directories(path);
  const auto& shape = *std::ranges::find_if(Shapes(), [](const auto& s) {
    return s.name == std::string_view{"sparse_values"};
  });
  uint64_t offset = 0;
  uint64_t size = 0;
  uint64_t rows = 0;
  irs::codecs::CodecType block_codec = irs::codecs::CodecType::Auto;
  {
    irs::MMapDirectory dir{path};
    Write(dir, duckdb::LogicalType::BIGINT, {.tier = irs::WriteTier::Flush},
          60000, 60000, shape.gen);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    const auto& block = col->DataBlocks().front();
    ASSERT_TRUE(irs::codecs::IsNumeric(block.codec->type));
    offset = block.file_offset;
    size = block.byte_size;
    rows = block.tuple_count;
    block_codec = block.codec->type;
  }
  const auto file = path / irs::FileName(kSeg);
  std::string bytes;
  {
    std::ifstream in{file, std::ios::binary};
    bytes.assign(std::istreambuf_iterator<char>{in}, {});
  }
  ASSERT_LE(offset + size, bytes.size());
  auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
  const auto h = irs::codecs::ParseFrames(block, size, rows, block_codec);
  ASSERT_EQ(irs::codecs::TransformName(irs::codecs::ShapeOf(block_codec), h),
            "dict_bitpacking");
  auto* bases =
    block + h.off_data + duckdb::Load<uint32_t>(block + h.off_frames);
  const size_t blocks =
    irs::codecs::kBitpackingFrameRows / irs::codecs::numeric::kBlockValues;
  const size_t base_bytes = irs::codecs::numeric::BytesFor(
    duckdb::Load<irs::codecs::DictionaryHeader>(
      block + irs::codecs::OwnHeaderOffset(h.flags))
      .count -
    1);
  const unsigned bits = bases[blocks * base_bytes];
  ASSERT_GT(bits, 0U);
  std::memset(bases, 0xFF, base_bytes);
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
  ExpectCorrupted(scan,
                  "Corrupted numeric segment: dictionary code is out of range");
}

TEST_F(ColNumericCodecTest, PackedBlockBasesTakeTheBytesOfTheFrameRange) {
  constexpr uint64_t kRows = 16384;
  constexpr uint64_t kBlocks = 16;
  struct Case {
    duckdb::LogicalType type;
    Gen gen;
    uint64_t base_bytes;
  };
  const Gen flags = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g) & 1);
  };
  const Gen far_apart = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>((Mix(g) & 1) + g / 1024 * 70000);
  };
  const Case cases[] = {
    {duckdb::LogicalType::TINYINT, flags, 1},
    {duckdb::LogicalType::SMALLINT, flags, 1},
    {duckdb::LogicalType::INTEGER, flags, 1},
    {duckdb::LogicalType::BIGINT, flags, 1},
    {duckdb::LogicalType::INTEGER, far_apart, 4},
    {duckdb::LogicalType::BIGINT, far_apart, 4},
  };
  for (const auto& [type, gen, base_bytes] : cases) {
    SCOPED_TRACE(type.ToString());
    irs::MemoryDirectory dir{};
    Write(dir, type, {.tier = irs::WriteTier::Flush}, kRows, kRows, gen,
          irs::codecs::CodecType::Bitpacking);
    Verify(dir, type, kRows, gen);
    ASSERT_EQ(Transforms(dir), std::vector<std::string>{"bitpacking/none"});
    EXPECT_EQ(SegmentInfo(dir, "data_bytes"),
              std::vector<std::string>{std::to_string(
                irs::codecs::BlockMetaBytes(kBlocks, base_bytes) +
                kBlocks * 1024 / 8)});
  }
}

TEST_F(ColNumericCodecTest, BitpackingPacksEveryWidth) {
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
      EXPECT_TRUE(std::ranges::find(transforms, "bitpacking/none") !=
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
    EXPECT_TRUE(irs::codecs::IsNumeric(block.codec->type));
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
      const bool numeric = irs::codecs::IsNumeric(block.codec->type);
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
    EXPECT_EQ(*back, kFirst + kRandom);
  }
}

TEST_F(ColNumericCodecTest,
       AutoLeavesAHeldBitPackedCodecWhenBytesStartToRepeat) {
  constexpr uint32_t kRowGroup = 4096;
  constexpr uint64_t kSwitch = 9 * kRowGroup;
  constexpr uint64_t kRows = kSwitch + 4 * kRowGroup;
  const Gen gen = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g < kSwitch ? g : g / 16) % 100000);
  };
  using irs::codecs::CodecType;
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Flush ? "flush" : "merge");
    const auto type = duckdb::LogicalType::BIGINT;
    irs::MemoryDirectory dir{};
    Write(dir, type, {.tier = tier}, kRows, kRowGroup, gen, CodecType::Auto);
    Verify(dir, type, kRows, gen);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    std::vector<CodecType> codecs;
    for (const auto& block : col->DataBlocks()) {
      codecs.push_back(block.codec->type);
    }
    ASSERT_EQ(codecs.size(), kRows / kRowGroup);
    const auto first = codecs.begin() + kSwitch / kRowGroup;
    EXPECT_TRUE(std::all_of(
      codecs.begin(), first,
      [](CodecType codec) { return codec == CodecType::Bitpacking; }))
      << ::testing::PrintToString(codecs);
    EXPECT_NE(*first, CodecType::Bitpacking)
      << ::testing::PrintToString(codecs);
  }
}

TEST_F(ColNumericCodecTest, AutoPacksTheDeltasOfSortedIntegers) {
  constexpr uint32_t kRowGroup = 4096;
  constexpr uint64_t kRows = 4 * kRowGroup;
  const Gen clustered = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(1'700'000'000'000'000LL + g * 1000 +
                                Mix(g) % 300);
  };
  const Gen sequence = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(g * 7 + 3);
  };
  const auto delta = [](irs::codecs::CodecType type) {
    return irs::codecs::IsNumeric(type) &&
           irs::codecs::ShapeOf(type) == irs::codecs::Shape::Delta;
  };
  const auto codecs = [&](const duckdb::LogicalType& type, const Gen& gen,
                          irs::WriteTier tier,
                          irs::codecs::CodecType compression) {
    irs::MemoryDirectory dir{};
    Write(dir, type, {.tier = tier}, kRows, kRowGroup, gen, compression);
    Verify(dir, type, kRows, gen);
    for (const auto& transform : Transforms(dir)) {
      EXPECT_NE(transform, "delta/none");
    }
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    std::set<irs::codecs::CodecType> types;
    for (const auto& block : col->DataBlocks()) {
      types.insert(block.codec->type);
    }
    return types;
  };
  using irs::codecs::CodecType;
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Flush ? "flush" : "merge");
    for (const auto& gen : {clustered, sequence}) {
      for (const duckdb::LogicalType type :
           {duckdb::LogicalType::BIGINT, duckdb::LogicalType::TIMESTAMP}) {
        SCOPED_TRACE(type.ToString());
        EXPECT_EQ(codecs(type, gen, tier, CodecType::Auto),
                  std::set{CodecType::Bitpacking});
        irs::MemoryDirectory chosen{};
        Write(chosen, type, {.tier = tier}, kRows, kRowGroup, gen,
              CodecType::Auto);
        irs::MemoryDirectory differences{};
        Write(differences, type, {.tier = tier}, kRows, kRowGroup, gen,
              CodecType::Delta);
        EXPECT_LE(NumericBytes(chosen), NumericBytes(differences));
      }
      for (const auto stored :
           codecs(duckdb::LogicalType::DOUBLE, gen, tier, CodecType::Auto)) {
        EXPECT_FALSE(delta(stored)) << magic_enum::enum_name(stored);
      }
    }
    EXPECT_EQ(
      codecs(duckdb::LogicalType::BIGINT, clustered, tier, CodecType::DeltaLz4),
      std::set{CodecType::DeltaLz4});
  }
}

TEST_F(ColNumericCodecTest, BooleansUseTheOneByteTransforms) {
  constexpr uint32_t kRowGroup = 4096;
  constexpr uint64_t kRows = 4 * kRowGroup;
  const auto type = duckdb::LogicalType::BOOLEAN;
  const Gen flags = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g) & 1);
  };
  const Gen runs = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(g / 500 % 2);
  };
  const Gen sparse = [](uint64_t g) -> std::optional<int64_t> {
    if (g % 97 == 13) {
      return std::nullopt;
    }
    return static_cast<int64_t>(Mix(g) % 50 == 0);
  };
  const Gen constant = [](uint64_t) -> std::optional<int64_t> { return 1; };
  using irs::codecs::CodecType;
  const auto codecs = [&](const Gen& gen, irs::WriteTier tier,
                          CodecType codec) {
    irs::MemoryDirectory dir{};
    Write(dir, type, {.tier = tier}, kRows, kRowGroup, gen, codec);
    Verify(dir, type, kRows, gen);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    std::set<CodecType> types;
    for (const auto& block : col->DataBlocks()) {
      types.insert(block.codec->type);
    }
    return types;
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Flush ? "flush" : "merge");
    for (const auto& gen : {flags, runs, sparse}) {
      for (const auto codec : codecs(gen, tier, CodecType::Auto)) {
        EXPECT_TRUE(codec == CodecType::Bitpacking || codec == CodecType::Rle ||
                    codec == CodecType::Roaring)
          << magic_enum::enum_name(codec);
      }
      EXPECT_EQ(codecs(gen, tier, CodecType::Bitpacking),
                std::set{CodecType::Bitpacking});
    }
    EXPECT_EQ(codecs(runs, tier, CodecType::Rle), std::set{CodecType::Rle});
    EXPECT_EQ(codecs(constant, tier, CodecType::Auto),
              std::set{CodecType::Constant});
  }
}

TEST_F(ColNumericCodecTest, WideIntegersUseTheNumericTransforms) {
  constexpr uint32_t kRowGroup = 4096;
  constexpr uint64_t kRows = 4 * kRowGroup;
  const Gen small = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g) % 1000) - 500;
  };
  const Gen sequence = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(g);
  };
  const Gen runs = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g / 64) % 30000);
  };
  const Gen sparse = [](uint64_t g) -> std::optional<int64_t> {
    if (g % 7 == 3) {
      return std::nullopt;
    }
    return static_cast<int64_t>(Mix(g) % 100000);
  };
  const Gen random = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g));
  };
  const Gen few_random = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(Mix(g) % 1000));
  };
  const Gen upper_48_bits = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g) >> 16);
  };
  const Gen upper_49_bits = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g) >> 15);
  };
  using irs::codecs::CodecType;
  const auto codecs = [&](const duckdb::LogicalType& type, const Gen& gen,
                          irs::WriteTier tier, CodecType codec) {
    irs::MemoryDirectory dir{};
    Write(dir, type, {.tier = tier}, kRows, kRowGroup, gen, codec);
    Verify(dir, type, kRows, gen);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    std::set<CodecType> types;
    for (const auto& block : col->DataBlocks()) {
      types.insert(block.codec->type);
    }
    return types;
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Flush ? "flush" : "merge");
    for (const duckdb::LogicalType type :
         {duckdb::LogicalType::HUGEINT, duckdb::LogicalType::UHUGEINT}) {
      SCOPED_TRACE(type.ToString());
      for (const auto& gen : {small, sequence, runs, sparse}) {
        for (const auto codec : codecs(type, gen, tier, CodecType::Auto)) {
          EXPECT_TRUE(irs::codecs::IsNumeric(codec))
            << magic_enum::enum_name(codec);
        }
      }
      EXPECT_EQ(codecs(type, small, tier, CodecType::Bitpacking),
                std::set{CodecType::Bitpacking});
      EXPECT_EQ(codecs(type, sequence, tier, CodecType::Delta),
                std::set{CodecType::Delta});
      EXPECT_EQ(codecs(type, runs, tier, CodecType::Rle),
                std::set{CodecType::Rle});
    }
    const auto uuid = duckdb::LogicalType::UUID;
    for (const auto codec : codecs(uuid, sequence, tier, CodecType::Auto)) {
      EXPECT_TRUE(irs::codecs::IsNumeric(codec))
        << magic_enum::enum_name(codec);
    }
    EXPECT_EQ(codecs(uuid, sequence, tier, CodecType::Delta),
              std::set{CodecType::Delta});
    EXPECT_EQ(codecs(uuid, random, tier, CodecType::Auto),
              std::set{CodecType::Uncompressed});
    EXPECT_EQ(codecs(uuid, random, tier, CodecType::Bitpacking),
              std::set{CodecType::Uncompressed});
    EXPECT_EQ(codecs(uuid, few_random, tier, CodecType::Bitpacking),
              std::set{CodecType::Uncompressed});
    EXPECT_EQ(codecs(uuid, few_random, tier, CodecType::Delta),
              std::set{CodecType::Uncompressed});
    EXPECT_EQ(codecs(uuid, few_random, tier, CodecType::Dict),
              std::set{CodecType::Dict});
    EXPECT_EQ(codecs(uuid, few_random, tier, CodecType::Auto),
              std::set{CodecType::Dict});
    EXPECT_EQ(codecs(uuid, few_random, tier, CodecType::Rle),
              std::set{CodecType::Uncompressed});
    EXPECT_EQ(codecs(uuid, random, tier, CodecType::Rle),
              std::set{CodecType::Uncompressed});
    EXPECT_EQ(codecs(uuid, random, tier, CodecType::Dict),
              std::set{CodecType::Uncompressed});
    EXPECT_EQ(codecs(uuid, upper_48_bits, tier, CodecType::Bitpacking),
              std::set{CodecType::Bitpacking});
    for (const auto codec :
         codecs(uuid, upper_48_bits, tier, CodecType::Auto)) {
      EXPECT_TRUE(irs::codecs::IsNumeric(codec))
        << magic_enum::enum_name(codec);
    }
    EXPECT_EQ(codecs(uuid, upper_49_bits, tier, CodecType::Bitpacking),
              std::set{CodecType::Uncompressed});
    EXPECT_EQ(
      codecs(duckdb::LogicalType::HUGEINT, sequence, tier, CodecType::Rle),
      std::set{CodecType::Uncompressed});
  }
}

TEST_F(ColNumericCodecTest, WideValuesRepeatedAcrossARowGroupTakeADictionary) {
  constexpr uint32_t kRowGroup = 65536;
  constexpr uint64_t kRows = 2 * kRowGroup;
  const Gen repeated = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(Mix(g) % 8000));
  };
  using irs::codecs::CodecType;
  const auto uuid = duckdb::LogicalType::UUID;
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Flush ? "flush" : "merge");
    irs::MemoryDirectory dir{};
    Write(dir, uuid, {.tier = tier}, kRows, kRowGroup, repeated,
          CodecType::Dict);
    Verify(dir, uuid, kRows, repeated);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    for (const auto& block : col->DataBlocks()) {
      EXPECT_EQ(block.codec->type, CodecType::Dict);
    }
    irs::MemoryDirectory auto_dir{};
    Write(auto_dir, uuid, {.tier = tier}, kRows, kRowGroup, repeated);
    Verify(auto_dir, uuid, kRows, repeated);
    irs::ColReader auto_reader{auto_dir, std::string{kSeg}, Db(),
                               irs::IOAdvice::SEQUENTIAL};
    const auto* auto_col = auto_reader.Column(kField);
    ASSERT_NE(auto_col, nullptr);
    for (const auto& block : auto_col->DataBlocks()) {
      EXPECT_EQ(block.codec->type, CodecType::Uncompressed);
    }
  }
}

TEST_F(ColNumericCodecTest, WideRowGroupsOfLongRunsReadBack) {
  constexpr uint32_t kRowGroup = 16384;
  constexpr uint64_t kRows = 2 * kRowGroup + 5000;
  const Gen long_runs = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g / 500) % 30000);
  };
  const Gen both_signs = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(g / 300 % 11) - 5;
  };
  const Gen multiples = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g / 700) % 9) * 1000003;
  };
  const Gen one_value = [](uint64_t) -> std::optional<int64_t> { return 42; };
  const Gen runs_at_block_starts = [](uint64_t g) -> std::optional<int64_t> {
    return g % 1024 < 64 ? 7 : static_cast<int64_t>(Mix(g) % 1000);
  };
  using irs::codecs::CodecType;
  const auto codecs = [&](const duckdb::LogicalType& type, const Gen& gen,
                          irs::WriteTier tier, CodecType codec) {
    irs::MemoryDirectory dir{};
    Write(dir, type, {.tier = tier}, kRows, kRowGroup, gen, codec);
    Verify(dir, type, kRows, gen);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    std::set<CodecType> types;
    for (const auto& block : col->DataBlocks()) {
      types.insert(block.codec->type);
    }
    return types;
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Flush ? "flush" : "merge");
    for (const duckdb::LogicalType type :
         {duckdb::LogicalType::HUGEINT, duckdb::LogicalType::UHUGEINT,
          duckdb::LogicalType::UUID}) {
      SCOPED_TRACE(type.ToString());
      for (const auto& gen : {long_runs, both_signs, multiples, one_value,
                              runs_at_block_starts}) {
        for (const auto codec : {CodecType::Bitpacking, CodecType::Delta,
                                 CodecType::Dict, CodecType::Auto}) {
          codecs(type, gen, tier, codec);
        }
      }
      for (const auto& gen : {long_runs, both_signs, multiples}) {
        EXPECT_EQ(codecs(type, gen, tier, CodecType::Rle),
                  std::set{CodecType::Rle});
      }
      codecs(type, one_value, tier, CodecType::Rle);
      codecs(type, runs_at_block_starts, tier, CodecType::Rle);
    }
  }
}

TEST_F(ColNumericCodecTest, WideRowGroupsWithNullsReadBack) {
  constexpr uint32_t kRowGroup = 16384;
  constexpr uint64_t kRows = 2 * kRowGroup + 5000;
  const Gen late = [](uint64_t g) -> std::optional<int64_t> {
    if (g < 5000 || g % 7 == 3) {
      return std::nullopt;
    }
    return static_cast<int64_t>(1'000'000 + Mix(g) % 5000);
  };
  const Gen late_runs = [](uint64_t g) -> std::optional<int64_t> {
    if (g < 3000 || g % 11 == 5) {
      return std::nullopt;
    }
    return static_cast<int64_t>(g / 300);
  };
  const Gen signs = [](uint64_t g) -> std::optional<int64_t> {
    if (g % 5 == 0) {
      return std::nullopt;
    }
    return static_cast<int64_t>(Mix(g) % 2000) - 1000;
  };
  const Gen scattered = [](uint64_t g) -> std::optional<int64_t> {
    if (g % 9 == 4) {
      return std::nullopt;
    }
    return static_cast<int64_t>(Mix(g % 3000));
  };
  const Gen none = [](uint64_t) -> std::optional<int64_t> {
    return std::nullopt;
  };
  for (const duckdb::LogicalType type :
       {duckdb::LogicalType::HUGEINT, duckdb::LogicalType::UHUGEINT,
        duckdb::LogicalType::UUID}) {
    for (const auto& gen : {late, late_runs, signs, scattered, none}) {
      for (const auto codec :
           {irs::codecs::CodecType::Bitpacking, irs::codecs::CodecType::Delta,
            irs::codecs::CodecType::Rle, irs::codecs::CodecType::Dict,
            irs::codecs::CodecType::Auto}) {
        SCOPED_TRACE(type.ToString() + " " +
                     std::string{magic_enum::enum_name(codec)});
        irs::MemoryDirectory dir{};
        Write(dir, type, {}, kRows, kRowGroup, gen, codec);
        Verify(dir, type, kRows, gen);
      }
    }
  }
}

TEST_F(ColNumericCodecTest, BlocksStoreDifferencesWhereTheyAreSmaller) {
  constexpr uint64_t kRows = 16 * 1024;
  const auto type = duckdb::LogicalType::BIGINT;
  const Gen mixed = [](uint64_t g) {
    return g / 1024 % 2 == 0
             ? static_cast<int64_t>(1'000'000'000 + g * 1000 + Mix(g) % 16)
             : static_cast<int64_t>(Mix(g) % 1024);
  };
  const auto bytes = [&](irs::codecs::CodecType codec) {
    irs::MemoryDirectory dir;
    Write(dir, type, {}, kRows, 16384, mixed, codec);
    Verify(dir, type, kRows, mixed);
    return NumericBytes(dir);
  };
  const auto bitpacking = bytes(irs::codecs::CodecType::Bitpacking);
  EXPECT_LT(bitpacking, bytes(irs::codecs::CodecType::Delta));
  EXPECT_LT(bitpacking, kRows * 9 / 8);
}

TEST_F(ColNumericCodecTest, ASequenceUnderBitpackingStoresNoData) {
  constexpr uint64_t kRows = 60000;
  const auto type = duckdb::LogicalType::BIGINT;
  const Gen sequence = [](uint64_t g) {
    return static_cast<int64_t>(7 + g * 3);
  };
  irs::MemoryDirectory dir;
  Write(dir, type, {}, kRows, 60000, sequence,
        irs::codecs::CodecType::Bitpacking);
  Verify(dir, type, kRows, sequence);
  EXPECT_EQ(Transforms(dir), std::vector<std::string>{"bitpacking/none"});
  EXPECT_EQ(SegmentInfo(dir, "data_bytes"), std::vector<std::string>{"0"});
}

TEST_F(ColNumericCodecTest, SequencesWithACommonFactorStoreNoData) {
  constexpr uint64_t kRows = 40000;
  struct Case {
    const char* name;
    duckdb::LogicalType type;
    Gen gen;
    const char* scale;
  };
  const Case cases[] = {
    {"ascending", duckdb::LogicalType::BIGINT,
     [](uint64_t g) { return static_cast<int64_t>(14 + g * 21); }, "7"},
    {"descending", duckdb::LogicalType::BIGINT,
     [](uint64_t g) { return 3'500'000 - static_cast<int64_t>(g * 35); }, "35"},
    {"negative", duckdb::LogicalType::INTEGER,
     [](uint64_t g) { return -static_cast<int64_t>(600 + g * 12); }, "12"},
    {"unsigned descending", duckdb::LogicalType::UINTEGER,
     [](uint64_t g) { return static_cast<int64_t>(4'000'000'000ULL - g * 10); },
     "10"},
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    irs::MemoryDirectory dir;
    Write(dir, c.type, {}, kRows, kRows, c.gen,
          irs::codecs::CodecType::Bitpacking);
    Verify(dir, c.type, kRows, c.gen);
    EXPECT_EQ(Transforms(dir), std::vector<std::string>{"bitpacking/none"});
    EXPECT_EQ(SegmentInfo(dir, "data_bytes"), std::vector<std::string>{"0"});
    EXPECT_EQ(SegmentInfo(dir, "scale"), std::vector<std::string>{c.scale});
  }
  const Gen wrapping = [](uint64_t g) {
    return static_cast<int64_t>(g * 100 % 256);
  };
  irs::MemoryDirectory dir;
  Write(dir, duckdb::LogicalType::UTINYINT, {}, kRows, kRows, wrapping,
        irs::codecs::CodecType::Bitpacking);
  Verify(dir, duckdb::LogicalType::UTINYINT, kRows, wrapping);
  EXPECT_NE(SegmentInfo(dir, "data_bytes"), std::vector<std::string>{"0"});
}

TEST_F(ColNumericCodecTest, RowsOfDifferenceBlocksReadInAnyOrder) {
  constexpr uint64_t kRows = 9 * 1024 + 300;
  const Gen rising = [](uint64_t g) {
    return static_cast<int64_t>(1'000'000 + g * 1000 + Mix(g) % 16);
  };
  for (const auto id :
       {duckdb::LogicalType::BIGINT, duckdb::LogicalType::INTEGER,
        duckdb::LogicalType::UINTEGER}) {
    const duckdb::LogicalType type{id};
    SCOPED_TRACE(type.ToString());
    irs::MemoryDirectory dir;
    Write(dir, type, {}, kRows, 16384, rising, irs::codecs::CodecType::Delta);
    ASSERT_EQ(Transforms(dir),
              std::vector<std::string>{"delta_bitpacking/none"});
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    duckdb::Vector one{type, 1};
    const auto expect_row = [&](irs::ColumnReader::PointReader& cursor,
                                uint64_t g) {
      duckdb::FlatVector::ValidityMutable(one).Reset();
      EXPECT_TRUE(cursor.FetchRow(g, one, 0)) << g;
      ExpectRow(one, 0, g, rising, type);
    };
    {
      irs::ColumnReader::PointReader cursor{r, *col};
      for (uint64_t g = 5; g < kRows; g += 37) {
        expect_row(cursor, g);
      }
    }
    {
      irs::ColumnReader::PointReader cursor{r, *col};
      for (uint64_t g = kRows; g-- > 0;) {
        if (g % 53 == 0) {
          expect_row(cursor, g);
          expect_row(cursor, g);
        }
      }
    }
    {
      irs::ColumnReader::PointReader cursor{r, *col};
      for (uint64_t g = 2000; g < 3500; ++g) {
        expect_row(cursor, g);
      }
    }
    {
      irs::ColumnReader::PointReader cursor{r, *col};
      for (uint64_t i = 0; i < 3000; ++i) {
        expect_row(cursor, Mix(i) % kRows);
      }
    }
    for (const uint64_t stride :
         {uint64_t{1}, uint64_t{2}, uint64_t{37}, uint64_t{700}}) {
      SCOPED_TRACE(stride);
      irs::ColumnReader::PointReader cursor{r, *col};
      std::vector<uint32_t> picked;
      for (uint64_t g = stride / 2; g < kRows; g += stride) {
        picked.push_back(static_cast<uint32_t>(g));
      }
      for (size_t i = 0; i < picked.size(); i += STANDARD_VECTOR_SIZE) {
        const auto take =
          std::min<size_t>(picked.size() - i, STANDARD_VECTOR_SIZE);
        duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
        cursor.FetchRows(std::span<const uint32_t>{&picked[i], take}, 0, out,
                         0);
        for (size_t k = 0; k < take; ++k) {
          ExpectRow(out, k, picked[i + k], rising, type);
        }
      }
    }
  }
}

TEST_F(ColNumericCodecTest, FloatRowGroupsKeepTheirMinAndMax) {
  constexpr uint64_t kRows = 5000;
  const auto check = [&]<typename T, typename Bits>(
                       const duckdb::LogicalType& type,
                       irs::codecs::CodecType codec, const char* name,
                       const std::function<std::optional<T>(uint64_t)>& at) {
    SCOPED_TRACE(type.ToString() + " " + name + " " +
                 std::string{magic_enum::enum_name(codec)});
    const auto write = [&](irs::Directory& dir,
                           irs::codecs::CodecType compression) {
      irs::ColWriter w{dir, kSeg, Db(), irs::WriteTier::Merge};
      auto& cw = w.OpenColumn(
        kField, type, {.row_group_size = 8192, .compression = compression});
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
    };
    irs::MemoryDirectory dir{};
    write(dir, codec);
    irs::MemoryDirectory plain_dir{};
    write(plain_dir, irs::codecs::CodecType::Uncompressed);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    irs::ColReader plain_r{plain_dir, std::string{kSeg}, Db(),
                           irs::IOAdvice::SEQUENTIAL};
    const auto* plain = plain_r.Column(kField);
    ASSERT_NE(plain, nullptr);
    const auto& stats = col->MergedStatistics();
    const auto& want = plain->MergedStatistics();
    EXPECT_EQ(stats.CanHaveNull(), want.CanHaveNull());
    EXPECT_EQ(stats.CanHaveNoNull(), want.CanHaveNoNull());
    ASSERT_EQ(duckdb::NumericStats::HasMinMax(stats),
              duckdb::NumericStats::HasMinMax(want));
    if (!duckdb::NumericStats::HasMinMax(want)) {
      return;
    }
    const auto same = [](T got, T expected) {
      return std::isnan(expected)
               ? std::isnan(got)
               : std::bit_cast<Bits>(got) == std::bit_cast<Bits>(expected);
    };
    EXPECT_TRUE(same(duckdb::NumericStats::GetMin<T>(stats),
                     duckdb::NumericStats::GetMin<T>(want)))
      << duckdb::NumericStats::Min(stats).ToString() << " against "
      << duckdb::NumericStats::Min(want).ToString();
    EXPECT_TRUE(same(duckdb::NumericStats::GetMax<T>(stats),
                     duckdb::NumericStats::GetMax<T>(want)))
      << duckdb::NumericStats::Max(stats).ToString() << " against "
      << duckdb::NumericStats::Max(want).ToString();
  };
  const auto every = [&]<typename T, typename Bits>(
                       const duckdb::LogicalType& type) {
    const T specials[] = {
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
      T{-2.25},
    };
    for (const auto codec :
         {irs::codecs::CodecType::Bitpacking, irs::codecs::CodecType::Rle,
          irs::codecs::CodecType::Dict}) {
      check.template operator()<T, Bits>(
        type, codec, "specials with nulls",
        [&](uint64_t g) -> std::optional<T> {
          if (g % 11 == 4) {
            return std::nullopt;
          }
          return specials[Mix(g / 3) % std::size(specials)];
        });
      check.template operator()<T, Bits>(
        type, codec, "numbers", [](uint64_t g) -> std::optional<T> {
          return static_cast<T>(Mix(g / 5) % 1000) / T{7} - T{60};
        });
      check.template operator()<T, Bits>(
        type, codec, "zeros", [](uint64_t g) -> std::optional<T> {
          return g / 40 % 2 == 0 ? T{0} : -T{0};
        });
      check.template operator()<T, Bits>(type, codec, "negative zeros and less",
                                         [](uint64_t g) -> std::optional<T> {
                                           return g / 40 % 2 == 0 ? -T{0}
                                                                  : T{-3};
                                         });
      check.template operator()<T, Bits>(
        type, codec, "nan", [](uint64_t g) -> std::optional<T> {
          return g / 9 % 2 == 0 ? std::numeric_limits<T>::quiet_NaN()
                                : -std::numeric_limits<T>::quiet_NaN();
        });
      check.template operator()<T, Bits>(
        type, codec, "nan and a number", [](uint64_t g) -> std::optional<T> {
          return g == 4321 ? T{7} : std::numeric_limits<T>::quiet_NaN();
        });
      check.template operator()<T, Bits>(
        type, codec, "negative infinity", [](uint64_t) -> std::optional<T> {
          return -std::numeric_limits<T>::infinity();
        });
      check.template operator()<T, Bits>(
        type, codec, "nulls",
        [](uint64_t) -> std::optional<T> { return std::nullopt; });
    }
  };
  every.template operator()<float, uint32_t>(duckdb::LogicalType::FLOAT);
  every.template operator()<double, uint64_t>(duckdb::LogicalType::DOUBLE);
}

TEST_F(ColNumericCodecTest, FloatsWithoutRunsLeaveTheRunCodecUncompressed) {
  constexpr uint64_t kRows = 10000;
  const auto type = duckdb::LogicalType::DOUBLE;
  const Gen distinct = [](uint64_t g) {
    return static_cast<int64_t>(g * 3 + 1);
  };
  const Gen repeated_between_chunks = [](uint64_t g) {
    return static_cast<int64_t>((g == STANDARD_VECTOR_SIZE ? g - 1 : g) * 3 +
                                1);
  };
  const Gen with_a_null = [](uint64_t g) -> std::optional<int64_t> {
    if (g == 4099) {
      return std::nullopt;
    }
    return static_cast<int64_t>(g * 3 + 1);
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(static_cast<int>(tier));
    irs::MemoryDirectory dir;
    Write(dir, type, {.tier = tier}, kRows, 8192, distinct,
          irs::codecs::CodecType::Rle);
    Verify(dir, type, kRows, distinct);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    for (const auto& block : col->DataBlocks()) {
      EXPECT_EQ(block.codec->type, irs::codecs::CodecType::Uncompressed);
    }
    for (const auto& gen : {repeated_between_chunks, with_a_null}) {
      irs::MemoryDirectory other;
      Write(other, type, {.tier = tier}, kRows, 8192, gen,
            irs::codecs::CodecType::Rle);
      Verify(other, type, kRows, gen);
    }
  }
}

TEST_F(ColNumericCodecTest, NamedBitpackingKeepsOffsetsInBlocksThatHoldNulls) {
  constexpr uint64_t kRows = 16 * 1024;
  const auto type = duckdb::LogicalType::BIGINT;
  const Gen rising = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(1'000'000'000 + g * 1000 + Mix(g) % 16);
  };
  const Gen nulls_everywhere = [&](uint64_t g) -> std::optional<int64_t> {
    if (g % 5 == 2) {
      return std::nullopt;
    }
    return rising(g);
  };
  const Gen nulls_in_one_block = [&](uint64_t g) -> std::optional<int64_t> {
    if (g / 1024 == 3 && g % 7 == 0) {
      return std::nullopt;
    }
    return rising(g);
  };
  const auto bytes = [&](const Gen& gen, irs::codecs::CodecType codec) {
    irs::MemoryDirectory dir;
    Write(dir, type, {}, kRows, 16384, gen, codec);
    Verify(dir, type, kRows, gen);
    return NumericBytes(dir);
  };
  const auto differences = bytes(rising, irs::codecs::CodecType::Bitpacking);
  EXPECT_LT(differences, kRows * 9 / 8);
  EXPECT_GT(bytes(nulls_everywhere, irs::codecs::CodecType::Bitpacking),
            kRows * 2);
  EXPECT_LT(bytes(nulls_everywhere, irs::codecs::CodecType::Delta), kRows * 2);
  const auto one_block =
    bytes(nulls_in_one_block, irs::codecs::CodecType::Bitpacking);
  EXPECT_GT(one_block, differences + 1000);
  EXPECT_LT(one_block, differences + 4000);
}

TEST_F(ColNumericCodecTest, DifferencesAsWideAsTheTypeAreStoredAsOffsets) {
  constexpr uint64_t kRows = 5000;
  const auto type = duckdb::LogicalType::BIGINT;
  const Gen swings = [](uint64_t g) {
    return g % 2 == 0 ? int64_t{0} : std::numeric_limits<int64_t>::max();
  };
  irs::MemoryDirectory dir;
  Write(dir, type, {}, kRows, 8192, swings, irs::codecs::CodecType::Delta);
  Verify(dir, type, kRows, swings);
  EXPECT_EQ(Transforms(dir), std::vector<std::string>{"delta_bitpacking/none"});
}

TEST_F(ColNumericCodecTest, ShortRunsOfUnevenLengthsReadBack) {
  constexpr uint64_t kRows = 70000;
  const Gen uneven = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g >> (1 + Mix(g >> 6) % 3)) % 100);
  };
  const Gen uneven_with_nulls = [&](uint64_t g) -> std::optional<int64_t> {
    if (Mix(g) % 9 == 0) {
      return std::nullopt;
    }
    return uneven(g);
  };
  for (const auto id :
       {duckdb::LogicalType::BIGINT, duckdb::LogicalType::SMALLINT,
        duckdb::LogicalType::UTINYINT}) {
    const duckdb::LogicalType type{id};
    for (const auto& gen : {uneven, uneven_with_nulls}) {
      SCOPED_TRACE(type.ToString());
      irs::MemoryDirectory dir;
      Write(dir, type, {}, kRows, 16384, gen, irs::codecs::CodecType::Rle);
      Verify(dir, type, kRows, gen);
      for (const auto& transform : Transforms(dir)) {
        EXPECT_EQ(transform, "rle_bitpacking/none");
      }
    }
  }
}

TEST_F(ColNumericCodecTest, RunsOfTwoValuesStoreNoValueBits) {
  constexpr uint64_t kRows = 200000;
  const auto type = duckdb::LogicalType::SMALLINT;
  const Gen two = [](uint64_t g) -> std::optional<int64_t> {
    return g / 50 % 2 == 0 ? 3 : 9;
  };
  const Gen three = [](uint64_t g) -> std::optional<int64_t> {
    constexpr int64_t kCycle[] = {3, 9, 5};
    return kCycle[g / 50 % 3];
  };
  const auto bytes = [&](const Gen& gen) {
    irs::MemoryDirectory dir;
    Write(dir, type, {}, kRows, 200000, gen, irs::codecs::CodecType::Rle);
    Verify(dir, type, kRows, gen);
    EXPECT_EQ(Transforms(dir), std::vector<std::string>{"rle_bitpacking/none"});
    return NumericBytes(dir);
  };
  EXPECT_LT(bytes(two), 400U);
  EXPECT_GT(bytes(three), 1000U);
}

TEST_F(ColNumericCodecTest, RunValuesInOrderStoreDifferences) {
  constexpr uint64_t kRows = 300000;
  const auto type = duckdb::LogicalType::BIGINT;
  const Gen even = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(1'000'000 + 3 * (g / 100));
  };
  const Gen uneven = [](uint64_t g) -> std::optional<int64_t> {
    const auto run = g / 100;
    return static_cast<int64_t>(1'000'000 + 3 * run + Mix(run) % 3);
  };
  const auto bytes = [&](const Gen& gen) {
    irs::MemoryDirectory dir;
    Write(dir, type, {}, kRows, 300000, gen, irs::codecs::CodecType::Rle);
    Verify(dir, type, kRows, gen);
    EXPECT_EQ(Transforms(dir), std::vector<std::string>{"rle_bitpacking/none"});
    return NumericBytes(dir);
  };
  EXPECT_LT(bytes(even), 500U);
  EXPECT_LT(bytes(uneven), 2000U);
}

TEST_F(ColNumericCodecTest, RunValuesOneBitShortOfAWordStoreWholeWords) {
  constexpr uint64_t kRuns = 3 * 1024 + 350;
  constexpr uint64_t kRows = 2 * kRuns;
  const auto type = duckdb::LogicalType::BIGINT;
  const Gen gen = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g / 2) >> 1);
  };
  irs::MemoryDirectory dir;
  Write(dir, type, {}, kRows, 16384, gen, irs::codecs::CodecType::Rle);
  Verify(dir, type, kRows, gen);
  EXPECT_EQ(Transforms(dir), std::vector<std::string>{"rle_bitpacking/none"});
  EXPECT_GE(NumericBytes(dir), kRuns * sizeof(int64_t));
}

TEST_F(ColNumericCodecTest, ARunThatCoversAVectorReadsAsAConstant) {
  constexpr uint64_t kRows = 40000;
  constexpr uint64_t kRun = 5000;
  const Gen gen = [](uint64_t g) { return static_cast<int64_t>(g / kRun % 2); };
  const auto one_run = [](uint64_t pos) {
    return pos + STANDARD_VECTOR_SIZE <= kRows &&
           pos / kRun == (pos + STANDARD_VECTOR_SIZE - 1) / kRun;
  };
  for (const duckdb::LogicalType type :
       {duckdb::LogicalType::BOOLEAN, duckdb::LogicalType::BIGINT,
        duckdb::LogicalType::DOUBLE, duckdb::LogicalType::HUGEINT,
        duckdb::LogicalType::UUID}) {
    SCOPED_TRACE(type.ToString());
    irs::MemoryDirectory dir;
    Write(dir, type, {}, kRows, 16384, gen, irs::codecs::CodecType::Rle);
    for (const auto& transform : Transforms(dir)) {
      EXPECT_TRUE(transform.starts_with("rle")) << transform;
    }
    Verify(dir, type, kRows, gen);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    auto state = col->InitScan(r.Ctx());
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      const auto take = std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, take);
      EXPECT_EQ(out.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR,
                one_run(pos))
        << pos;
      out.Flatten(take);
      for (duckdb::idx_t k = 0; k < take; k += 331) {
        ExpectRow(out, k, pos + k, gen, type);
      }
    }
    size_t constants = 0;
    for (uint64_t pos = 0; pos + STANDARD_VECTOR_SIZE <= kRows;
         pos += STANDARD_VECTOR_SIZE) {
      std::vector<uint64_t> picked;
      for (uint64_t k = 0; k < STANDARD_VECTOR_SIZE; k += 8) {
        picked.push_back(pos + k);
      }
      picked.push_back(pos + STANDARD_VECTOR_SIZE - 1);
      auto gather = col->InitScan(r.Ctx());
      duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
      duckdb::FlatVector::ValidityMutable(out).Reset(STANDARD_VECTOR_SIZE);
      irs::column_internal::GatherRows(*col, gather,
                                       Rows2{picked.data(), picked.size()}, out,
                                       0, /*whole_output=*/true);
      if (out.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR) {
        EXPECT_TRUE(one_run(pos)) << pos;
        ++constants;
      }
      out.Flatten(picked.size());
      for (size_t k = 0; k < picked.size(); ++k) {
        ExpectRow(out, k, picked[k], gen, type);
      }
    }
    EXPECT_GT(constants, 0);
  }
}

TEST_F(ColNumericCodecTest, ConstantBlocksThatCoverAVectorReadAsAConstant) {
  constexpr uint64_t kRows = 40000;
  constexpr uint64_t kRun = 5000;
  const Gen gen = [](uint64_t g) { return static_cast<int64_t>(g / kRun % 2); };
  const auto one_run = [](uint64_t pos) {
    return pos + STANDARD_VECTOR_SIZE <= kRows &&
           pos / kRun == (pos + STANDARD_VECTOR_SIZE - 1) / kRun;
  };
  for (const duckdb::LogicalType type :
       {duckdb::LogicalType::BOOLEAN, duckdb::LogicalType::BIGINT,
        duckdb::LogicalType::HUGEINT, duckdb::LogicalType::UUID}) {
    for (const auto codec :
         {irs::codecs::CodecType::Bitpacking, irs::codecs::CodecType::Delta}) {
      SCOPED_TRACE(type.ToString() + " " +
                   std::string{magic_enum::enum_name(codec)});
      irs::MemoryDirectory dir;
      Write(dir, type, {}, kRows, 16384, gen, codec);
      for (const auto& transform : Transforms(dir)) {
        EXPECT_TRUE(transform.contains("bitpacking")) << transform;
      }
      Verify(dir, type, kRows, gen);
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      auto state = col->InitScan(r.Ctx());
      for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
        const auto take = std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE);
        duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
        col->Scan(state, out, take);
        EXPECT_EQ(out.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR,
                  one_run(pos))
          << pos;
        out.Flatten(take);
        for (duckdb::idx_t k = 0; k < take; k += 331) {
          ExpectRow(out, k, pos + k, gen, type);
        }
      }
      size_t constants = 0;
      for (uint64_t pos = 0; pos + STANDARD_VECTOR_SIZE <= kRows;
           pos += STANDARD_VECTOR_SIZE) {
        std::vector<uint64_t> picked;
        for (uint64_t k = 0; k < STANDARD_VECTOR_SIZE; k += 8) {
          picked.push_back(pos + k);
        }
        picked.push_back(pos + STANDARD_VECTOR_SIZE - 1);
        auto gather = col->InitScan(r.Ctx());
        duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
        duckdb::FlatVector::ValidityMutable(out).Reset(STANDARD_VECTOR_SIZE);
        irs::column_internal::GatherRows(*col, gather,
                                         Rows2{picked.data(), picked.size()},
                                         out, 0, /*whole_output=*/true);
        if (out.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR) {
          EXPECT_TRUE(one_run(pos)) << pos;
          ++constants;
        }
        out.Flatten(picked.size());
        for (size_t k = 0; k < picked.size(); ++k) {
          ExpectRow(out, k, picked[k], gen, type);
        }
      }
      EXPECT_GT(constants, 0);
    }
  }
}

TEST_F(ColNumericCodecTest, AutoStoresRunsOfEveryIntegerTypeWithTheRunCodecs) {
  constexpr uint32_t kRowGroup = 4096;
  constexpr uint64_t kRows = 4 * kRowGroup;
  const Gen long_runs = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g / 64) % 30000);
  };
  const Gen short_runs = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g - (g % 3 == 1 ? 1 : 0)) % 30000);
  };
  const auto codecs = [&](const duckdb::LogicalType& type, const Gen& gen,
                          irs::WriteTier tier) {
    irs::MemoryDirectory dir{};
    Write(dir, type, {.tier = tier}, kRows, kRowGroup, gen);
    Verify(dir, type, kRows, gen);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    std::set<irs::codecs::CodecType> types;
    for (const auto& block : col->DataBlocks()) {
      types.insert(block.codec->type);
    }
    return types;
  };
  using irs::codecs::CodecType;
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Flush ? "flush" : "merge");
    for (const duckdb::LogicalType type :
         {duckdb::LogicalType::SMALLINT, duckdb::LogicalType::INTEGER,
          duckdb::LogicalType::UBIGINT, duckdb::LogicalType::TIMESTAMP,
          duckdb::LogicalType::HUGEINT}) {
      SCOPED_TRACE(type.ToString());
      for (const auto type_of_block : codecs(type, long_runs, tier)) {
        EXPECT_TRUE(irs::codecs::IsNumeric(type_of_block))
          << magic_enum::enum_name(type_of_block);
      }
      EXPECT_FALSE(codecs(type, short_runs, tier).contains(CodecType::Rle));
    }
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
    const bool numeric = irs::codecs::IsNumeric(block.codec->type);
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

TEST_F(ColNumericCodecTest,
       AHeldCodecWaitsForItsContestWhenBitPackingIsDisabled) {
  constexpr uint32_t kRowGroup = 2048;
  constexpr uint64_t kRandom = 40 * kRowGroup;
  constexpr uint64_t kRows = kRandom + 32 * kRowGroup;
  constexpr uint64_t kMaxLag = 16 * kRowGroup;
  const Gen gen = [](uint64_t g) -> std::optional<int64_t> {
    if (g < kRandom) {
      return static_cast<int64_t>(Mix(g));
    }
    return static_cast<int64_t>(g % 1000) * 1'000'003;
  };
  duckdb::DBConfig::GetConfig(Db()).SetDisabledCompressionMethods(
    {duckdb::CompressionType::COMPRESSION_BITPACKING});
  irs::MemoryDirectory dir{};
  Write(dir, duckdb::LogicalType::BIGINT, {.tier = irs::WriteTier::Merge},
        kRows, kRowGroup, gen);
  Verify(dir, duckdb::LogicalType::BIGINT, kRows, gen);

  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  std::optional<uint64_t> back;
  uint64_t row = 0;
  for (const auto& block : col->DataBlocks()) {
    const bool numeric = irs::codecs::IsNumeric(block.codec->type);
    EXPECT_NE(block.codec->type, irs::codecs::CodecType::Bitpacking);
    if (row < kRandom) {
      EXPECT_FALSE(numeric) << "row " << row;
    } else if (back) {
      EXPECT_TRUE(numeric) << "row " << row;
    } else if (numeric) {
      back = row;
    }
    row += block.tuple_count;
  }
  ASSERT_TRUE(back.has_value());
  EXPECT_GT(*back, kRandom);
  EXPECT_LE(*back, kRandom + kMaxLag);
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
          gen, irs::codecs::CodecType::Rle);
    Verify(dir, duckdb::LogicalType::BIGINT, kRows, gen);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    std::vector<irs::codecs::CodecType> types;
    for (const auto& block : col->DataBlocks()) {
      types.push_back(block.codec->type);
    }
    return types;
  };
  using irs::codecs::CodecType;
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Flush ? "flush" : "merge");
    EXPECT_EQ(codecs(unique, tier), std::vector(6, CodecType::Uncompressed));
    EXPECT_EQ(codecs(runs, tier), std::vector(6, CodecType::Rle));
    const std::vector mixed{CodecType::Uncompressed, CodecType::Uncompressed,
                            CodecType::Rle,          CodecType::Uncompressed,
                            CodecType::Uncompressed, CodecType::Uncompressed};
    EXPECT_EQ(codecs(one_repeat, tier), mixed);
    EXPECT_EQ(codecs(one_null, tier), mixed);
  }
}

TEST_F(ColNumericCodecTest, NamedNumericTakesEveryRowGroupWithAValue) {
  constexpr uint32_t kRowGroup = 4096;
  constexpr uint64_t kRows = 5 * kRowGroup + 700;
  const Gen unique = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g));
  };
  const Gen runs = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(g / 64);
  };
  const Gen null_group = [](uint64_t g) -> std::optional<int64_t> {
    if (g / kRowGroup == 2) {
      return std::nullopt;
    }
    return static_cast<int64_t>(g * 3 + 1);
  };
  const auto codecs = [&](const Gen& gen, irs::WriteTier tier) {
    irs::MemoryDirectory dir{};
    Write(dir, duckdb::LogicalType::BIGINT, {.tier = tier}, kRows, kRowGroup,
          gen, irs::codecs::CodecType::Bitpacking);
    Verify(dir, duckdb::LogicalType::BIGINT, kRows, gen);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    std::vector<irs::codecs::CodecType> types;
    for (const auto& block : col->DataBlocks()) {
      types.push_back(block.codec->type);
    }
    return types;
  };
  using irs::codecs::CodecType;
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Flush ? "flush" : "merge");
    EXPECT_EQ(codecs(unique, tier), std::vector(6, CodecType::Bitpacking));
    EXPECT_EQ(codecs(runs, tier), std::vector(6, CodecType::Bitpacking));
    const std::vector without_values{
      CodecType::Bitpacking, CodecType::Bitpacking, CodecType::Constant,
      CodecType::Bitpacking, CodecType::Bitpacking, CodecType::Bitpacking};
    EXPECT_EQ(codecs(null_group, tier), without_values);
  }
}

TEST_F(ColNumericCodecTest, ForcedBitpackingStoresARangeThatOverflowsTheType) {
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
          irs::codecs::CodecType::Bitpacking);
    Verify(dir, type, kRows, gen);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    std::set<irs::codecs::CodecType> types;
    for (const auto& block : col->DataBlocks()) {
      types.insert(block.codec->type);
    }
    return types;
  };
  using irs::codecs::CodecType;
  EXPECT_EQ(codecs(duckdb::LogicalType::BIGINT, wide),
            std::set{CodecType::Bitpacking});
  EXPECT_EQ(codecs(duckdb::LogicalType::BIGINT, narrow),
            std::set{CodecType::Bitpacking});
  EXPECT_EQ(codecs(duckdb::LogicalType::UBIGINT, wide),
            std::set{CodecType::Bitpacking});
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
    std::set<irs::codecs::CodecType> types;
    for (const auto& block : col->Validity()->DataBlocks()) {
      types.insert(block.codec->type);
    }
    return types;
  };
  using irs::codecs::CodecType;
  EXPECT_EQ(validity(sparse), std::set{CodecType::Roaring});
  EXPECT_EQ(validity(scattered), std::set{CodecType::Uncompressed});
}

TEST_F(ColNumericCodecTest, SparseReadsCacheDecodedFrames) {
  constexpr uint64_t kRows = 60000;
  struct Case {
    const char* name;
    Gen gen;
    irs::WriteTier tier;
    std::string_view stored;
    irs::codecs::CodecType codec = irs::codecs::CodecType::Auto;
  };
  const Case cases[] = {
    {"clustered runs",
     [](uint64_t g) {
       return static_cast<int64_t>(1'700'000'000'000'000LL + (g / 4) * 1000 +
                                   Mix(g / 4) % 3 * 100);
     },
     irs::WriteTier::Merge,
     {},
     irs::codecs::CodecType::RleZstd},
    {"periodic steps",
     [](uint64_t g) { return static_cast<int64_t>(g * 1000 + g % 7); },
     irs::WriteTier::Flush, "delta/lz4", irs::codecs::CodecType::DeltaLz4},
    {"periodic steps over two frames",
     [](uint64_t g) { return static_cast<int64_t>(g * 1000 + g % 7 * 301); },
     irs::WriteTier::Flush, "delta/lz4", irs::codecs::CodecType::DeltaLz4},
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    irs::MemoryDirectory dir{};
    Write(dir, duckdb::LogicalType::BIGINT, {.tier = c.tier}, kRows, 65536,
          c.gen, c.codec);
    const auto transforms = Transforms(dir);
    ASSERT_TRUE(std::ranges::any_of(transforms, [&](const auto& t) {
      return c.stored.empty() ? t.ends_with("/zstd") || t.ends_with("/lz4")
                              : t == c.stored;
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
        ExpectRow(out, 0, g, c.gen, duckdb::LogicalType::BIGINT);
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
}

TEST_F(ColNumericCodecTest, PointReadsOnByteShuffledFrames) {
  constexpr uint64_t kRows = 60000;
  struct Case {
    const char* name;
    Gen gen;
    std::string_view transform;
    duckdb::LogicalType type = duckdb::LogicalType::BIGINT;
    irs::codecs::CodecType codec = irs::codecs::CodecType::Auto;
  };
  const auto entry = [](uint64_t k) {
    return static_cast<int64_t>((k << 48) + (Mix(k) & 0xFFFFFFFFFFFFULL));
  };
  const Case cases[] = {
    {"noisy steps with jumps",
     [](uint64_t g) {
       return static_cast<int64_t>(g * 1000 + Mix(g) % 256 +
                                   (g / 997) * 1'000'000'000);
     },
     "delta", duckdb::LogicalType::BIGINT, irs::codecs::CodecType::DeltaLz4},
    {"descending noisy steps with jumps",
     [](uint64_t g) {
       return -static_cast<int64_t>(g * 1000 + Mix(g) % 256 +
                                    (g / 997) * 1'000'000'000);
     },
     "delta", duckdb::LogicalType::BIGINT, irs::codecs::CodecType::DeltaLz4},
    {"wide noisy steps with jumps",
     [](uint64_t g) {
       return static_cast<int64_t>(g * 100'000 + Mix(g) % 65536 +
                                   (g / 997) * 1'000'000'000'000LL);
     },
     "delta", duckdb::LogicalType::BIGINT, irs::codecs::CodecType::DeltaLz4},
    {"offsets on two levels",
     [](uint64_t g) {
       return static_cast<int64_t>(1'000'000 + (Mix(g / 64) % 200) * 256 +
                                   Mix(g) % 4);
     },
     "for"},
    {"offsets on two levels in 32 bits",
     [](uint64_t g) {
       return static_cast<int64_t>(1'000'000 + (Mix(g / 64) % 200) * 256 +
                                   Mix(g) % 4);
     },
     "for", duckdb::LogicalType::INTEGER},
    {"hashes with repeating low bytes",
     [](uint64_t g) {
       return static_cast<int64_t>((Mix(g / 16) << 16) | (g % 7));
     },
     "raw"},
    {"dictionary entries in clusters",
     [&](uint64_t g) { return entry(Mix(g / 7) % 16 * 256 + g % 251); },
     "dict"},
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    for (const auto& c : cases) {
      SCOPED_TRACE(std::string{c.name} + " " +
                   std::to_string(static_cast<int>(tier)));
      irs::MemoryDirectory dir{};
      Write(dir, c.type, {.tier = tier}, kRows, 65536, c.gen, c.codec);
      const auto transforms = Transforms(dir);
      ASSERT_EQ(transforms.size(), 1);
      ASSERT_EQ(transforms[0].substr(0, transforms[0].find('/')), c.transform);
      ASSERT_EQ(SegmentInfo(dir, "shuffled"), std::vector<std::string>{"true"});
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      irs::ColumnReader::PointReader cursor{r, *col};
      duckdb::Vector out{c.type, 1};
      const auto expect = [&](uint64_t g) {
        duckdb::FlatVector::ValidityMutable(out).Reset();
        ASSERT_TRUE(cursor.FetchRow(g, out, 0)) << g;
        ExpectRow(out, 0, g, c.gen, c.type);
      };
      for (uint64_t g = 3; g < kRows; g += 1531) {
        expect(g);
      }
      for (uint64_t g = kRows - 1; g > 2777; g -= 2777) {
        expect(g);
      }
      for (const uint64_t g :
           {uint64_t{0}, kRows - 1, uint64_t{0}, uint64_t{100}, uint64_t{101},
            uint64_t{164}, uint64_t{40000}, uint64_t{40001}, uint64_t{99}}) {
        expect(g);
      }
      std::vector<uint32_t> picked;
      for (uint64_t g = 7; g < kRows; g += 1531) {
        picked.push_back(static_cast<uint32_t>(g));
      }
      duckdb::Vector batch{c.type, STANDARD_VECTOR_SIZE};
      cursor.FetchRows(std::span<const uint32_t>{picked}, 0, batch, 0);
      for (size_t k = 0; k < picked.size(); ++k) {
        ExpectRow(batch, k, picked[k], c.gen, c.type);
      }
    }
  }
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
  Write(dir, duckdb::LogicalType::BIGINT, {}, kRows, 65536, clustered,
        irs::codecs::CodecType::RleZstd);
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
  Write(dir, duckdb::LogicalType::BIGINT, {}, kRows, 65536, clustered,
        irs::codecs::CodecType::RleZstd);
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

TEST_F(ColNumericCodecTest, ConstantBlocksAnswerFiltersAsAWhole) {
  constexpr uint64_t kRows = 12 * 1024 + 500;
  duckdb::Connection con{_db};
  using Cmp = duckdb::ExpressionType;
  const auto unit = [](uint64_t g) -> int64_t {
    switch (g / 1024) {
      case 0:
      case 1:
      case 2:
      case 3:
        return 5;
      case 4:
        return 9;
      case 5:
        return static_cast<int64_t>(Mix(g) % 12);
      case 8:
        return g % 1024 < 1000 ? 5 : 6;
      case 9:
        return static_cast<int64_t>(100 + g % 1024);
      default:
        return 5;
    }
  };
  const auto holds = [](Cmp cmp, int64_t v, int64_t key) {
    switch (cmp) {
      case Cmp::COMPARE_EQUAL:
        return v == key;
      case Cmp::COMPARE_NOTEQUAL:
        return v != key;
      case Cmp::COMPARE_LESSTHAN:
        return v < key;
      default:
        return v >= key;
    }
  };
  struct Case {
    duckdb::LogicalType type;
    int64_t factor;
  };
  for (const auto& [type, factor] : {Case{duckdb::LogicalType::BIGINT, 1},
                                     Case{duckdb::LogicalType::SMALLINT, 1},
                                     Case{duckdb::LogicalType::BIGINT, 10}}) {
    const Gen gen = [&](uint64_t g) -> std::optional<int64_t> {
      return unit(g) * factor;
    };
    for (const auto codec :
         {irs::codecs::CodecType::Bitpacking, irs::codecs::CodecType::Delta,
          irs::codecs::CodecType::Auto}) {
      SCOPED_TRACE(type.ToString() + " x" + std::to_string(factor) + " " +
                   std::string{magic_enum::enum_name(codec)});
      irs::MemoryDirectory dir{};
      Write(dir, type, {}, kRows, 16384, gen, codec);
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      for (const int64_t key :
           {5 * factor, 9 * factor, 6 * factor, 77 * factor}) {
        for (const auto cmp :
             {Cmp::COMPARE_EQUAL, Cmp::COMPARE_NOTEQUAL, Cmp::COMPARE_LESSTHAN,
              Cmp::COMPARE_GREATERTHANOREQUALTO}) {
          const duckdb::ExpressionFilter filter{
            duckdb::BoundComparisonExpression::Create(
              cmp, duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0),
              duckdb::make_uniq<duckdb::BoundConstantExpression>(
                Cast(key, type)))};
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
              const auto v = *gen(anchor + i);
              const bool expected = holds(cmp, v, key);
              const bool got = next < kept && sel.get_index(next) == i;
              next += got ? 1 : 0;
              ASSERT_EQ(got, expected)
                << "row " << anchor + i << " key " << key << " cmp "
                << duckdb::EnumUtil::ToString(cmp);
              if (got) {
                ASSERT_EQ(out.GetValue(i), Cast(v, type));
              }
            }
          }
        }
      }
    }
  }
}

TEST_F(ColNumericCodecTest, FiltersDecidedByBlockBoundsMatchTheValues) {
  constexpr uint64_t kRows = 40000;
  constexpr int64_t kTop = std::numeric_limits<int64_t>::max();
  constexpr int64_t kBottom = std::numeric_limits<int64_t>::min();
  duckdb::Connection con{_db};
  using Cmp = duckdb::ExpressionType;
  struct Case {
    const char* name;
    duckdb::LogicalType type;
    Gen gen;
  };
  const Case cases[] = {
    {"sequence", duckdb::LogicalType::BIGINT,
     [](uint64_t g) { return static_cast<int64_t>(1000 + 3 * g); }},
    {"sequence", duckdb::LogicalType::UINTEGER,
     [](uint64_t g) { return static_cast<int64_t>(1000 + 3 * g); }},
    {"descending", duckdb::LogicalType::INTEGER,
     [](uint64_t g) { return 10'000'000 - static_cast<int64_t>(7 * g); }},
    {"sorted with noise", duckdb::LogicalType::BIGINT,
     [](uint64_t g) { return static_cast<int64_t>(5 * g + Mix(g) % 4); }},
    {"clustered blocks", duckdb::LogicalType::INTEGER,
     [](uint64_t g) {
       return static_cast<int64_t>(g / 1024 * 100'000 + Mix(g) % 50);
     }},
    {"up to the maximum", duckdb::LogicalType::BIGINT,
     [](uint64_t g) {
       return kTop - static_cast<int64_t>(kRows) + static_cast<int64_t>(g);
     }},
    {"down to the minimum", duckdb::LogicalType::BIGINT,
     [](uint64_t g) { return kBottom + static_cast<int64_t>(kRows - g); }},
    {"between the extremes", duckdb::LogicalType::BIGINT,
     [](uint64_t g) {
       return g % 2 == 0 ? kBottom + static_cast<int64_t>(g % 5)
                         : kTop - static_cast<int64_t>(g % 7);
     }},
    {"a common factor", duckdb::LogicalType::BIGINT,
     [](uint64_t g) {
       return static_cast<int64_t>(40 * g + 40 * (Mix(g) % 3));
     }},
  };
  const auto holds = [](Cmp cmp, int64_t v, const std::vector<int64_t>& keys) {
    switch (cmp) {
      case Cmp::COMPARE_EQUAL:
        return v == keys.front();
      case Cmp::COMPARE_NOTEQUAL:
        return v != keys.front();
      case Cmp::COMPARE_LESSTHAN:
        return v < keys.front();
      case Cmp::COMPARE_GREATERTHANOREQUALTO:
        return v >= keys.front();
      default:
        return absl::c_linear_search(keys, v);
    }
  };
  for (const auto& [name, type, gen] : cases) {
    std::vector<int64_t> sampled;
    for (const uint64_t row :
         {uint64_t{0}, uint64_t{1023}, uint64_t{1024}, uint64_t{2048},
          uint64_t{16383}, uint64_t{16384}, uint64_t{30001}, kRows - 1}) {
      sampled.push_back(*gen(row));
    }
    std::vector<std::vector<int64_t>> probes{sampled};
    for (const auto key : {sampled[1], sampled[3], sampled[5], sampled[7]}) {
      probes.push_back({key});
      if (key != kTop) {
        probes.push_back({key + 1});
      }
    }
    for (const auto codec :
         {irs::codecs::CodecType::Bitpacking, irs::codecs::CodecType::Delta,
          irs::codecs::CodecType::Auto}) {
      SCOPED_TRACE(std::string{name} + " " + type.ToString() + " " +
                   std::string{magic_enum::enum_name(codec)});
      irs::MemoryDirectory dir{};
      Write(dir, type, {}, kRows, 32768, gen, codec);
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      for (const auto& keys : probes) {
        for (const auto cmp :
             {Cmp::COMPARE_EQUAL, Cmp::COMPARE_NOTEQUAL, Cmp::COMPARE_LESSTHAN,
              Cmp::COMPARE_GREATERTHANOREQUALTO, Cmp::COMPARE_IN}) {
          if ((cmp == Cmp::COMPARE_IN) != (keys.size() > 1)) {
            continue;
          }
          duckdb::unique_ptr<duckdb::Expression> expr;
          if (cmp == Cmp::COMPARE_IN) {
            auto in = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
              cmp, duckdb::LogicalType::BOOLEAN);
            in->GetChildrenMutable().push_back(
              duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0));
            for (const auto key : keys) {
              in->GetChildrenMutable().push_back(
                duckdb::make_uniq<duckdb::BoundConstantExpression>(
                  Cast(key, type)));
            }
            expr = std::move(in);
          } else {
            expr = duckdb::BoundComparisonExpression::Create(
              cmp, duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0),
              duckdb::make_uniq<duckdb::BoundConstantExpression>(
                Cast(keys.front(), type)));
          }
          const duckdb::ExpressionFilter filter{std::move(expr)};
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
              const auto v = *gen(anchor + i);
              const bool expected = holds(cmp, v, keys);
              const bool got = next < kept && sel.get_index(next) == i;
              next += got ? 1 : 0;
              ASSERT_EQ(got, expected)
                << "row " << anchor + i << " key " << keys.front() << " cmp "
                << duckdb::EnumUtil::ToString(cmp);
              if (got && (anchor + i) % 257 == 0) {
                ASSERT_EQ(out.GetValue(i), Cast(v, type));
              }
            }
          }
        }
      }
    }
  }
}

TEST_F(ColNumericCodecTest, RunAndDictionaryFiltersMatchTheValues) {
  constexpr uint64_t kRows = 20000;
  duckdb::Connection con{_db};
  using Cmp = duckdb::ExpressionType;
  using irs::codecs::CodecType;
  const auto holds = [](Cmp cmp, int64_t v, int64_t key) {
    switch (cmp) {
      case Cmp::COMPARE_EQUAL:
        return v == key;
      case Cmp::COMPARE_NOTEQUAL:
        return v != key;
      case Cmp::COMPARE_LESSTHAN:
        return v < key;
      default:
        return v >= key;
    }
  };
  const auto type = duckdb::LogicalType::BIGINT;
  std::vector<Shape> shapes = Shapes();
  shapes.push_back(
    {"flags", [](uint64_t g) { return static_cast<int64_t>(Mix(g / 3) % 2); }});
  shapes.push_back({"small_signed_runs", [](uint64_t g) {
                      return static_cast<int64_t>(Mix(g / 40) % 7) - 3;
                    }});
  shapes.push_back({"small_values", [](uint64_t g) {
                      return static_cast<int64_t>(Mix(g) % 200) + 1000;
                    }});
  for (const auto codec :
       {CodecType::Auto, CodecType::Bitpacking, CodecType::Rle,
        CodecType::RleZstd, CodecType::Dict, CodecType::DictZstd}) {
    uint64_t named = 0;
    for (const auto& shape : shapes) {
      irs::MemoryDirectory dir{};
      Write(dir, type, {}, kRows, 4096, shape.gen, codec);
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      for (const auto& block : col->DataBlocks()) {
        named += block.codec->type == codec;
      }
      std::vector<int64_t> keys{std::numeric_limits<int64_t>::min()};
      for (const uint64_t g : {uint64_t{1}, kRows / 2, kRows - 3}) {
        if (const auto v = shape.gen(g)) {
          keys.push_back(*v);
        }
      }
      for (const auto key : keys) {
        for (const auto cmp :
             {Cmp::COMPARE_EQUAL, Cmp::COMPARE_NOTEQUAL, Cmp::COMPARE_LESSTHAN,
              Cmp::COMPARE_GREATERTHANOREQUALTO}) {
          const duckdb::ExpressionFilter filter{
            duckdb::BoundComparisonExpression::Create(
              cmp, duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0),
              duckdb::make_uniq<duckdb::BoundConstantExpression>(
                duckdb::Value::BIGINT(key)))};
          auto filter_state =
            duckdb::TableFilterState::Initialize(*con.context, filter);
          for (const duckdb::idx_t stride : {1, 3}) {
            auto state = col->InitScan(r.Ctx());
            for (uint64_t anchor = 0; anchor < kRows;
                 anchor += STANDARD_VECTOR_SIZE) {
              const auto span =
                std::min<uint64_t>(kRows - anchor, STANDARD_VECTOR_SIZE);
              duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
              duckdb::idx_t offered = 0;
              for (duckdb::idx_t i = 0; i < span; i += stride) {
                sel.set_index(offered++, i);
              }
              duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
              const auto kept =
                col->GatherFilter(state, anchor, span, sel, offered, filter,
                                  *filter_state, irs::NullCheckKind::None, out);
              duckdb::idx_t next = 0;
              for (duckdb::idx_t i = 0; i < span; i += stride) {
                const auto v = shape.gen(anchor + i);
                const bool expected = v && holds(cmp, *v, key);
                const bool got = next < kept && sel.get_index(next) == i;
                next += got ? 1 : 0;
                ASSERT_EQ(got, expected)
                  << magic_enum::enum_name(codec) << " " << shape.name
                  << " row " << anchor + i << " key " << key << " cmp "
                  << duckdb::EnumUtil::ToString(cmp) << " stride " << stride;
                if (got) {
                  ASSERT_EQ(out.GetValue(i), duckdb::Value::BIGINT(*v));
                }
              }
              ASSERT_EQ(next, kept);
            }
          }
        }
      }
    }
    EXPECT_TRUE(named != 0 || codec == CodecType::Auto)
      << magic_enum::enum_name(codec);
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
          block.codec->type == irs::codecs::CodecType::Constant;
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
      EXPECT_EQ(block.codec->type == irs::codecs::CodecType::Constant,
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
  uint64_t rows = 0;
  irs::codecs::CodecType block_codec = irs::codecs::CodecType::Auto;
  {
    irs::MMapDirectory dir{path};
    Write(dir, duckdb::LogicalType::BIGINT, {}, 20000, 20000, clustered,
          irs::codecs::CodecType::DeltaLz4);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    const auto& block = col->DataBlocks().front();
    ASSERT_TRUE(irs::codecs::IsNumeric(block.codec->type));
    offset = block.file_offset;
    size = block.byte_size;
    rows = block.tuple_count;
    block_codec = block.codec->type;
  }
  const auto file = path / irs::FileName(kSeg);
  std::string bytes;
  {
    std::ifstream in{file, std::ios::binary};
    bytes.assign(std::istreambuf_iterator<char>{in}, {});
  }
  ASSERT_LE(offset + size, bytes.size());
  auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
  const auto h = irs::codecs::ParseFrames(block, size, rows, block_codec);
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
  ExpectCorrupted(scan,
                  "Corrupted numeric segment: frame table is inconsistent");
}

TEST_F(ColNumericCodecTest, RareWidthsRunLengthsAndDictionarySizes) {
  const auto adjacent = [](uint64_t k) {
    return static_cast<int64_t>(((uint64_t{1} << 52) + k) * 16);
  };
  std::vector<uint32_t> ragged(20000);
  for (uint32_t row = 0, run = 0; row != ragged.size(); ++run) {
    const auto length = 1 + Mix(run) % 255;
    for (uint64_t i = 0; i != length && row != ragged.size(); ++i) {
      ragged[row++] = run;
    }
  }
  struct Case {
    const char* name;
    std::vector<duckdb::LogicalType> types;
    uint32_t rg_size;
    uint64_t rows;
    Gen gen;
    std::string_view flush;
    std::string_view merge;
    irs::codecs::CodecType codec = irs::codecs::CodecType::Auto;
  };
  const Case cases[] = {
    {"adjacent doubles",
     {duckdb::LogicalType::DOUBLE},
     8192,
     20000,
     [&](uint64_t g) { return adjacent(Mix(g) % 60000); },
     "for/none@2",
     "for/none@2"},
    {"drifting adjacent doubles",
     {duckdb::LogicalType::DOUBLE},
     8192,
     20000,
     [&](uint64_t g) { return adjacent(g * 300 + Mix(g) % 5000); },
     "delta/none@2",
     "delta/none@2",
     irs::codecs::CodecType::Delta},
    {"runs of adjacent doubles",
     {duckdb::LogicalType::DOUBLE},
     8192,
     20000,
     [&](uint64_t g) { return adjacent(Mix(g / 8) % 60000); },
     "rle_bitpacking/none@8",
     "rle_bitpacking/none@8"},
    {"runs of nearby doubles",
     {duckdb::LogicalType::DOUBLE},
     8192,
     20000,
     [&](uint64_t g) { return adjacent(Mix(g / 8) % (uint64_t{1} << 31)); },
     "rle_bitpacking/none@8",
     "rle_bitpacking/none@8"},
    {"short runs of adjacent doubles",
     {duckdb::LogicalType::DOUBLE},
     8192,
     20000,
     [&](uint64_t g) { return adjacent(Mix(g / 3) % 60000); },
     "rle_bitpacking/none@8",
     "rle_bitpacking/none@8"},
    {"ragged runs that fill two bytes",
     {duckdb::LogicalType::DOUBLE},
     8192,
     20000,
     [&](uint64_t g) { return adjacent(Mix(ragged[g] + 1000) % 65536); },
     "rle/none@2",
     "rle/none@2"},
    {"ragged runs that fill four bytes",
     {duckdb::LogicalType::DOUBLE},
     8192,
     20000,
     [&](uint64_t g) {
       return adjacent(Mix(ragged[g] + 1000) % (uint64_t{1} << 32));
     },
     "rle/none@4",
     "rle/none@4"},
    {"short runs in a two byte range",
     {duckdb::LogicalType::UBIGINT},
     8192,
     20000,
     [](uint64_t g) {
       return static_cast<int64_t>((uint64_t{1} << 40) + Mix(g / 3) % 60000);
     },
     "rle/lz4@2",
     "rle/lz4@2"},
    {"adjacent floats",
     {duckdb::LogicalType::FLOAT},
     8192,
     20000,
     [](uint64_t g) { return static_cast<int64_t>(65536 + Mix(g) % 512); },
     "for/none@2",
     "for/none@2"},
    {"one long run",
     {duckdb::LogicalType::FLOAT, duckdb::LogicalType::DOUBLE,
      duckdb::LogicalType::UBIGINT},
     131072,
     140000,
     [](uint64_t g) {
       return g < 70000 ? int64_t{7} : static_cast<int64_t>(Mix(g / 2) % 1000);
     },
     {},
     {}},
    {"runs in a four byte range",
     {duckdb::LogicalType::UBIGINT},
     8192,
     20000,
     [](uint64_t g) {
       return static_cast<int64_t>((uint64_t{1} << 40) +
                                   Mix(g / 8) % 3'000'000'000ULL);
     },
     {},
     {}},
    {"256 wide values",
     {duckdb::LogicalType::BIGINT},
     8192,
     20000,
     [](uint64_t g) { return static_cast<int64_t>(Mix(Mix(g) % 256)); },
     "dict/none@1",
     "dict/none@1"},
    {"200 wide values",
     {duckdb::LogicalType::BIGINT},
     8192,
     20000,
     [](uint64_t g) { return static_cast<int64_t>(Mix(Mix(g) % 200)); },
     "dict/none@1",
     "dict/none@1"},
    {"noise then a pattern",
     {duckdb::LogicalType::BIGINT},
     40000,
     32768 + 100,
     [](uint64_t g) {
       return static_cast<int64_t>(g % 32768 < 16384 ? Mix(g) : Mix(g % 8));
     },
     "raw/lz4@8",
     "raw/lz4@8"},
    {"many distinct values",
     {duckdb::LogicalType::DOUBLE, duckdb::LogicalType::BIGINT,
      duckdb::LogicalType::UBIGINT},
     131072,
     131072,
     [](uint64_t g) { return static_cast<int64_t>(Mix(Mix(g) % 40000)); },
     "dict_bitpacking/none@8",
     "dict_bitpacking/none@8"},
    {"periodic small values",
     {duckdb::LogicalType::BIGINT},
     65536,
     60000,
     [](uint64_t g) { return static_cast<int64_t>(5000 + g % 7 * 3); },
     "for/lz4@1",
     "for/zstd@1"},
    {"periodic small values as deltas",
     {duckdb::LogicalType::BIGINT},
     65536,
     60000,
     [](uint64_t g) { return static_cast<int64_t>(5000 + g % 7 * 3); },
     "delta/lz4@1",
     "delta/lz4@1",
     irs::codecs::CodecType::DeltaLz4},
    {"cycle of wide values",
     {duckdb::LogicalType::BIGINT},
     65536,
     60000,
     [](uint64_t g) { return static_cast<int64_t>(Mix(g % 1000)); },
     "dict/lz4@2",
     "dict/lz4@2"},
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    for (const auto& c : cases) {
      for (const auto& type : c.types) {
        SCOPED_TRACE(std::string{c.name} + " " + type.ToString() + " " +
                     std::to_string(static_cast<int>(tier)));
        irs::MemoryDirectory dir{};
        Write(dir, type, {.tier = tier}, c.rows, c.rg_size, c.gen, c.codec);
        Verify(dir, type, c.rows, c.gen);
        const auto expected = tier == irs::WriteTier::Flush ? c.flush : c.merge;
        if (expected.empty()) {
          continue;
        }
        const auto transforms = Transforms(dir);
        const auto stored = SegmentInfo(dir, "stored");
        ASSERT_EQ(transforms.size(), stored.size());
        std::set<std::string> seen;
        for (size_t i = 0; i < transforms.size(); ++i) {
          seen.insert(transforms[i] + "@" + stored[i]);
        }
        EXPECT_TRUE(seen.contains(expected))
          << expected << " not among " << absl::StrJoin(seen, " ");
      }
    }
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

TEST(ColNumericKernelsTest, PacksEveryWidthOfEveryType) {
  const auto check = []<typename U>() {
    using Word = irs::codecs::numeric::LaneWord<U>;
    constexpr auto kValues = irs::codecs::numeric::kBlockValues;
    constexpr auto kMaxBits = irs::codecs::numeric::kMaxBits<U>;
    for (unsigned bits = 0; bits <= kMaxBits; ++bits) {
      SCOPED_TRACE(testing::Message() << sizeof(U) << " " << bits);
      const auto mask = bits == kMaxBits
                          ? static_cast<U>(~U{0})
                          : static_cast<U>((uint64_t{1} << bits) - 1);
      const auto base = static_cast<U>(Mix(bits + 77 * sizeof(U)));
      std::vector<U> values(kValues);
      for (size_t i = 0; i != kValues; ++i) {
        values[i] =
          static_cast<U>(base + (static_cast<U>(Mix(i * 131 + bits)) & mask));
      }
      values[1] = base;
      values[2] = static_cast<U>(base + mask);
      std::vector<Word> packed(
        irs::codecs::numeric::PackedBytes(bits) / sizeof(Word) +
          irs::codecs::numeric::kLanes<U>,
        Word{0});
      irs::codecs::numeric::Pack<U>(bits, values.data(), packed.data(), base);
      std::vector<U> unpacked(kValues, static_cast<U>(~base));
      irs::codecs::numeric::Unpack<U>(bits, packed.data(), unpacked.data(),
                                      base);
      ASSERT_EQ(unpacked, values);
      const auto* bytes = reinterpret_cast<const uint8_t*>(packed.data());
      for (uint32_t i = 0; i < kValues; i += 37) {
        ASSERT_EQ(irs::codecs::numeric::UnpackValue<U>(bytes, bits, i, base),
                  values[i])
          << i;
      }
    }
  };
  check.template operator()<uint8_t>();
  check.template operator()<uint16_t>();
  check.template operator()<uint32_t>();
  check.template operator()<uint64_t>();
}

TEST(ColNumericKernelsTest, NarrowsAndWidensEveryStoredWidth) {
  static constexpr uint8_t kWidths[] = {1, 2, 4, 8};
  const auto check = []<typename U>() {
    for (const auto stored : kWidths) {
      if (stored > sizeof(U)) {
        continue;
      }
      for (const size_t n : {size_t{0}, size_t{1}, size_t{33}, size_t{1000}}) {
        SCOPED_TRACE(testing::Message()
                     << sizeof(U) << " " << int{stored} << " " << n);
        const auto limit =
          stored == 8 ? ~uint64_t{0} : (uint64_t{1} << (8 * stored)) - 1;
        std::vector<U> values(n);
        for (size_t i = 0; i != n; ++i) {
          values[i] = static_cast<U>(Mix(i + n + stored) & limit);
        }
        std::vector<uint8_t> narrow(n * stored + 1, uint8_t{0x5a});
        irs::codecs::numeric::Narrow<sizeof(U)>(
          n, stored, narrow.data(), [&](size_t i) { return values[i]; });
        EXPECT_EQ(narrow[n * stored], 0x5a);
        std::vector<U> wide(n + 1, U{0x5a});
        irs::codecs::numeric::Widen<U>(narrow.data(), n, stored, wide.data());
        const auto base = static_cast<U>(Mix(n * 7 + stored));
        std::vector<U> added(n + 1, U{0x5a});
        irs::codecs::numeric::WidenAdd<U>(narrow.data(), n, stored, base,
                                          added.data());
        for (size_t i = 0; i != n; ++i) {
          ASSERT_EQ(wide[i], values[i]) << i;
          ASSERT_EQ(added[i], static_cast<U>(values[i] + base)) << i;
        }
        EXPECT_EQ(wide[n], U{0x5a});
        EXPECT_EQ(added[n], U{0x5a});
      }
    }
  };
  check.template operator()<uint8_t>();
  check.template operator()<uint16_t>();
  check.template operator()<uint32_t>();
  check.template operator()<uint64_t>();
}

TEST(ColNumericKernelsTest, ShufflesAndUnshufflesEveryStoredWidth) {
  static constexpr uint8_t kWidths[] = {1, 2, 4, 8};
  for (const auto stored : kWidths) {
    for (const size_t n : {size_t{0}, size_t{1}, size_t{15}, size_t{16},
                           size_t{17}, size_t{33}, size_t{1000}}) {
      SCOPED_TRACE(testing::Message() << int{stored} << " " << n);
      std::vector<uint8_t> values(n * stored);
      for (size_t i = 0; i != values.size(); ++i) {
        values[i] = static_cast<uint8_t>(Mix(i + n * 131 + stored));
      }
      std::vector<uint8_t> lanes(n * stored + 1, uint8_t{0x5a});
      irs::codecs::numeric::Shuffle(values.data(), n, stored, lanes.data());
      EXPECT_EQ(lanes[n * stored], 0x5a);
      for (size_t i = 0; i != n; ++i) {
        for (size_t b = 0; b != stored; ++b) {
          ASSERT_EQ(lanes[b * n + i], values[i * stored + b]) << i << " " << b;
        }
      }
      std::vector<uint8_t> back(n * stored + 1, uint8_t{0x5a});
      irs::codecs::numeric::Unshuffle(lanes.data(), n, stored, back.data());
      EXPECT_EQ(back[n * stored], 0x5a);
      back.pop_back();
      EXPECT_EQ(back, values);
    }
  }
}

TEST(ColNumericKernelsTest, UnshufflesAndWidensInOnePass) {
  const auto check = []<typename U>() {
    for (const uint8_t stored : {uint8_t{1}, uint8_t{2}, uint8_t{4}}) {
      if (stored >= sizeof(U)) {
        continue;
      }
      for (const size_t n : {size_t{0}, size_t{1}, size_t{15}, size_t{16},
                             size_t{17}, size_t{33}, size_t{1000}}) {
        SCOPED_TRACE(testing::Message()
                     << sizeof(U) << " " << int{stored} << " " << n);
        std::vector<uint8_t> lanes(n * stored);
        for (size_t i = 0; i != lanes.size(); ++i) {
          lanes[i] = static_cast<uint8_t>(Mix(i + n * 131 + stored));
        }
        const auto base = static_cast<U>(Mix(n * 7 + stored));
        std::vector<uint8_t> items(n * stored);
        irs::codecs::numeric::Unshuffle(lanes.data(), n, stored, items.data());
        std::vector<U> expected(n);
        irs::codecs::numeric::WidenAdd<U>(items.data(), n, stored, base,
                                          expected.data());
        std::vector<U> added(n + 1, U{0x5a});
        irs::codecs::numeric::UnshuffleWidenAdd<U>(lanes.data(), n, stored,
                                                   base, added.data());
        EXPECT_EQ(added[n], U{0x5a});
        added.pop_back();
        EXPECT_EQ(added, expected);
      }
    }
  };
  check.template operator()<uint16_t>();
  check.template operator()<uint32_t>();
  check.template operator()<uint64_t>();
}

}  // namespace
