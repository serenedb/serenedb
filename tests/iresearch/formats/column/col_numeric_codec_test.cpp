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
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <duckdb.hpp>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
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
#include <iresearch/formats/column/codecs/numeric/numeric_reader.hpp>
#include <iresearch/formats/column/codecs/numeric/plain.hpp>
#include <iresearch/formats/column/codecs/numeric/wide.hpp>
#include <iresearch/formats/column/codecs/registry.hpp>
#include <iresearch/formats/column/codecs/text/string_layout.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/internal/gather_arms.hpp>
#include <iresearch/formats/column/internal/write_context.hpp>
#include <iresearch/formats/column/list_ingest.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/column_info.hpp>
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
#include "index_dir.hpp"
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
    const tests::IndexDir& dir, const duckdb::LogicalType& type,
    irs::ColCodecParams params, uint64_t rows, uint32_t rg_size, const Gen& gen,
    irs::codecs::CodecType compression = irs::codecs::CodecType::Auto) {
    irs::ColWriter w{Db(), dir.Tracker(kSeg), params.tier};
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

  void Verify(const tests::IndexDir& dir, const duckdb::LogicalType& type,
              uint64_t rows, const Gen& gen) {
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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

  std::vector<std::string> SegmentInfo(const tests::IndexDir& dir,
                                       const std::string& key) {
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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

  uint64_t NumericBytes(const tests::IndexDir& dir) {
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    uint64_t bytes = 0;
    for (const auto& block : col->DataBlocks()) {
      bytes += block.byte_size;
    }
    return bytes;
  }

  std::vector<std::string> Transforms(const tests::IndexDir& dir) {
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
        irs::codecs::SegmentCacheKey<irs::codecs::DecodedFrame>(
          file_id, block.file_offset);
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
        tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
          tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
          Write(dir, type, {.tier = tier}, kRows, 4096, shape.gen, codec);
          Verify(dir, type, kRows, shape.gen);
          irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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

TEST_F(ColNumericCodecTest, EveryNamedCodecStoresEveryOtherTypeAndReadsBack) {
  constexpr uint64_t kRows = 10000;
  using irs::codecs::CodecType;
  const Gen random = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g));
  };
  const Gen narrow = [](uint64_t g) -> std::optional<int64_t> {
    return 1000 + static_cast<int64_t>(Mix(g) % 200);
  };
  const Gen slow_high = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>((g / 64) * 256 + Mix(g) % 256);
  };
  const Gen steps = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(g * 3 + Mix(g) % 2);
  };
  const Gen few = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g) % 40) * 1013;
  };
  const Gen runs = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g / 23) % 300);
  };
  const Gen nulls = [](uint64_t g) -> std::optional<int64_t> {
    if (Mix(g) % 5 == 0) {
      return std::nullopt;
    }
    return static_cast<int64_t>(Mix(g / 7) % 5000);
  };
  const std::pair<std::string_view, const Gen*> shapes[] = {
    {"random", &random}, {"narrow", &narrow}, {"slow_high", &slow_high},
    {"steps", &steps},   {"few", &few},       {"runs", &runs},
    {"nulls", &nulls},
  };
  for (const auto codec :
       {CodecType::Bitpacking, CodecType::Lz4, CodecType::Zstd,
        CodecType::Delta, CodecType::DeltaLz4, CodecType::DeltaZstd,
        CodecType::Rle, CodecType::RleLz4, CodecType::RleZstd, CodecType::Dict,
        CodecType::DictLz4, CodecType::DictZstd, CodecType::Alp,
        CodecType::Alprd}) {
    for (const auto id :
         {duckdb::LogicalType::TINYINT, duckdb::LogicalType::UTINYINT,
          duckdb::LogicalType::SMALLINT, duckdb::LogicalType::USMALLINT,
          duckdb::LogicalType::UINTEGER, duckdb::LogicalType::UBIGINT,
          duckdb::LogicalType::FLOAT, duckdb::LogicalType::HUGEINT,
          duckdb::LogicalType::UHUGEINT}) {
      const duckdb::LogicalType type{id};
      const bool applies =
        irs::codecs::GetCodec(codec, type.InternalType()) != nullptr;
      for (const auto& [name, gen] : shapes) {
        SCOPED_TRACE(std::string{magic_enum::enum_name(codec)} + " " +
                     std::string{name} + " " + type.ToString());
        tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
        Write(dir, type, {.tier = irs::WriteTier::Flush}, kRows, 4096, *gen,
              codec);
        Verify(dir, type, kRows, *gen);
        if (!applies) {
          continue;
        }
        irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
        const auto* col = r.Column(kField);
        ASSERT_NE(col, nullptr);
        for (const auto& block : col->DataBlocks()) {
          const auto stored = block.codec->type;
          EXPECT_TRUE(stored == codec || stored == CodecType::Constant ||
                      stored == CodecType::Uncompressed)
            << magic_enum::enum_name(stored);
        }
      }
    }
  }
}

TEST_F(ColNumericCodecTest,
       LeafCodecsReadBackRowGroupsOfMoreFramesThanTheySample) {
  using irs::codecs::CodecType;
  const Gen random = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g));
  };
  const Gen pairs = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g / 2));
  };
  const Gen few = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g) % 200) * 97;
  };
  const Gen hundreds = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g) % 300) * 211;
  };
  const Gen narrow = [](uint64_t g) -> std::optional<int64_t> {
    return 1000 + static_cast<int64_t>(Mix(g) % 200);
  };
  const Gen narrow_words = [](uint64_t g) -> std::optional<int64_t> {
    return 1000 + static_cast<int64_t>(Mix(g) % 60000);
  };
  const Gen narrow_floats = [](uint64_t g) -> std::optional<int64_t> {
    return 8192 + static_cast<int64_t>(Mix(g) % 60);
  };
  const Gen narrow_doubles = [](uint64_t g) -> std::optional<int64_t> {
    return (int64_t{1} << 44) + static_cast<int64_t>(Mix(g) % 60);
  };
  const Gen float_runs = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g / 16) % 50000);
  };
  struct Case {
    duckdb::LogicalType type;
    CodecType codec;
    const Gen& gen;
    uint64_t rows;
  };
  const Case cases[] = {
    {duckdb::LogicalType::TINYINT, CodecType::DeltaLz4, random, 70000},
    {duckdb::LogicalType::UTINYINT, CodecType::DeltaLz4, random, 70000},
    {duckdb::LogicalType::SMALLINT, CodecType::DeltaLz4, random, 40000},
    {duckdb::LogicalType::USMALLINT, CodecType::DeltaLz4, random, 40000},
    {duckdb::LogicalType::INTEGER, CodecType::DeltaLz4, random, 20000},
    {duckdb::LogicalType::UINTEGER, CodecType::DeltaLz4, random, 20000},
    {duckdb::LogicalType::UBIGINT, CodecType::DeltaLz4, random, 10000},
    {duckdb::LogicalType::FLOAT, CodecType::DeltaLz4, random, 90000},
    {duckdb::LogicalType::DOUBLE, CodecType::DeltaLz4, random, 45000},
    {duckdb::LogicalType::SMALLINT, CodecType::DictLz4, few, 70000},
    {duckdb::LogicalType::USMALLINT, CodecType::DictLz4, few, 70000},
    {duckdb::LogicalType::UINTEGER, CodecType::DictLz4, hundreds, 40000},
    {duckdb::LogicalType::FLOAT, CodecType::DictLz4, hundreds, 40000},
    {duckdb::LogicalType::DOUBLE, CodecType::DictLz4, hundreds, 40000},
    {duckdb::LogicalType::USMALLINT, CodecType::Lz4, narrow, 70000},
    {duckdb::LogicalType::UINTEGER, CodecType::Lz4, narrow_words, 40000},
    {duckdb::LogicalType::FLOAT, CodecType::Lz4, narrow_floats, 40000},
    {duckdb::LogicalType::FLOAT, CodecType::Lz4, random, 90000},
    {duckdb::LogicalType::DOUBLE, CodecType::Lz4, narrow_doubles, 40000},
    {duckdb::LogicalType::DOUBLE, CodecType::Lz4, random, 45000},
    {duckdb::LogicalType::FLOAT, CodecType::RleLz4, float_runs, 20000},
    {duckdb::LogicalType::TINYINT, CodecType::Auto, pairs, 70000},
    {duckdb::LogicalType::UTINYINT, CodecType::Auto, pairs, 70000},
    {duckdb::LogicalType::SMALLINT, CodecType::Auto, pairs, 50000},
    {duckdb::LogicalType::USMALLINT, CodecType::Auto, pairs, 50000},
    {duckdb::LogicalType::UINTEGER, CodecType::Auto, pairs, 30000},
  };
  for (const auto& [type, codec, gen, rows] : cases) {
    SCOPED_TRACE(type.ToString() + " " +
                 std::string{magic_enum::enum_name(codec)});
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {}, rows, static_cast<uint32_t>(rows), gen, codec);
    Verify(dir, type, rows, gen);
    if (codec == CodecType::Auto) {
      continue;
    }
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    for (const auto& block : col->DataBlocks()) {
      EXPECT_EQ(block.codec->type, codec)
        << magic_enum::enum_name(block.codec->type);
    }
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
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, type, {.compression_level = level}, kRows, 16384, gen, codec);
      Verify(dir, type, kRows, gen);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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

TEST_F(ColNumericCodecTest, ANamedLeafCodecMovesToABetterLayoutWithItsValues) {
  constexpr uint32_t kRowGroup = 4096;
  constexpr uint64_t kSwitch = 8 * kRowGroup;
  constexpr uint64_t kRows = 2 * kSwitch;
  const Gen gen = [](uint64_t g) -> std::optional<int64_t> {
    const auto value = static_cast<int64_t>(Mix(g) % 200);
    return g < kSwitch ? value : value << 24 | static_cast<int64_t>(g & 1);
  };
  const auto type = duckdb::LogicalType::BIGINT;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, type, {}, kRows, kRowGroup, gen, irs::codecs::CodecType::Zstd);
  Verify(dir, type, kRows, gen);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
  EXPECT_TRUE(std::all_of(first, layouts.end(),
                          [](const auto& l) { return l.ends_with("/true"); }))
    << ::testing::PrintToString(layouts);
}

TEST_F(ColNumericCodecTest,
       ACodecKeepsItsLayoutWhileItsValuesNeedTheSameBytes) {
  constexpr uint32_t kRowGroup = 4096;
  constexpr uint64_t kSwitch = 4 * kRowGroup;
  constexpr uint64_t kRefresh = irs::codecs::kOldHint + 1;
  constexpr uint64_t kRows = (kRefresh + 4) * kRowGroup;
  const Gen gen = [](uint64_t g) -> std::optional<int64_t> {
    if (g < kSwitch) {
      return static_cast<int64_t>(Mix(g / 8) & 0xFFFFFFFF);
    }
    return static_cast<int64_t>(Mix(g) & 0xFF) |
           static_cast<int64_t>(Mix(g / 64) & 0xFF) << 24;
  };
  const auto type = duckdb::LogicalType::BIGINT;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, type, {}, kRows, kRowGroup, gen, irs::codecs::CodecType::Zstd);
  Verify(dir, type, kRows, gen);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  irs::ReadContext ctx{r};
  irs::BlockWindow window{};
  uint64_t row = 0;
  std::vector<std::string> layouts;
  for (const auto& block : col->DataBlocks()) {
    window = col->Locate(row, window);
    auto seg = col->OpenSegment(window.block, ctx);
    auto info = seg->function.get_segment_info(*seg);
    layouts.push_back(info["stored"] + "/" + info["shuffled"]);
    row += block.tuple_count;
  }
  ASSERT_EQ(layouts.size(), kRows / kRowGroup);
  const auto refreshed = layouts.begin() + kRefresh;
  EXPECT_TRUE(std::all_of(layouts.begin(), refreshed,
                          [](const auto& l) { return l == "4/false"; }))
    << ::testing::PrintToString(layouts);
  EXPECT_TRUE(std::all_of(refreshed, layouts.end(),
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {}, rows, rg_size, gen, codec);
    Verify(dir, type, rows, gen);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
      tests::IndexDir packed{Db(), TestEnv::MemoryRoot()};
      Write(packed, type, {.tier = irs::WriteTier::Flush}, kRows, 16384, gen,
            irs::codecs::CodecType::Delta);
      Verify(packed, type, kRows, gen);
      const auto transforms = Transforms(packed);
      ASSERT_FALSE(transforms.empty());
      for (const auto& transform : transforms) {
        EXPECT_EQ(transform, "delta_bitpacking/none");
      }
      EXPECT_LT(NumericBytes(packed), most_bytes);
      tests::IndexDir plain{Db(), TestEnv::MemoryRoot()};
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, type, {.tier = irs::WriteTier::Flush}, kRows, 16384, gen,
            irs::codecs::CodecType::Delta);
      Verify(dir, type, kRows, gen);
      for (const auto& transform : Transforms(dir)) {
        EXPECT_EQ(transform, "delta_bitpacking/none");
      }
    }
  }
  const auto data_bytes = [&](const Gen& gen) {
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
        tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
  const Gen packed = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g) % 100);
  };
  const auto bitpacked = [](irs::codecs::FrameHeader& h) {
    ASSERT_EQ(irs::codecs::TransformName(irs::codecs::Shape::Plain, h),
              "bitpacking");
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
    {"a stored width of three bytes", duckdb::LogicalType::INTEGER,
     irs::codecs::CodecType::Lz4, narrow,
     [](irs::codecs::FrameHeader& h) { h.stored = 3; }},
    {"a stored width wider than the value", duckdb::LogicalType::INTEGER,
     irs::codecs::CodecType::Lz4, narrow,
     [](irs::codecs::FrameHeader& h) { h.stored = 8; }},
    {"bit-packed frames under a byte codec", duckdb::LogicalType::BIGINT,
     irs::codecs::CodecType::Lz4, narrow,
     [](irs::codecs::FrameHeader& h) {
       h.flags |= irs::codecs::kNumericPacked;
     }},
    {"bit-packed frames in byte planes", duckdb::LogicalType::BIGINT,
     irs::codecs::CodecType::Bitpacking, packed,
     [&](irs::codecs::FrameHeader& h) {
       ASSERT_NO_FATAL_FAILURE(bitpacked(h));
       h.flags |= irs::codecs::kNumericShuffled;
     }},
    {"bit-packed frames narrower than the value", duckdb::LogicalType::BIGINT,
     irs::codecs::CodecType::Bitpacking, packed,
     [&](irs::codecs::FrameHeader& h) {
       ASSERT_NO_FATAL_FAILURE(bitpacked(h));
       h.stored = 4;
     }},
    {"bit-packed frames of another frame size", duckdb::LogicalType::BIGINT,
     irs::codecs::CodecType::Bitpacking, packed,
     [&](irs::codecs::FrameHeader& h) {
       ASSERT_NO_FATAL_FAILURE(bitpacked(h));
       h.frame_log2 = irs::codecs::kNumericLeafFrameLog2;
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
      tests::IndexDir dir{Db(), path.string()};
      Write(dir, c.type, {.tier = irs::WriteTier::Flush}, 4096, 4096, c.gen,
            c.codec);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    tests::IndexDir dir{Db(), path.string()};
    const auto scan = [&] {
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
    };
    ExpectCorrupted(scan, "Corrupted numeric segment: header is inconsistent");
  }
}

TEST_F(ColNumericCodecTest,
       FrameHeadersOfAnotherWidthThanTheColumnAreRejected) {
  const auto path = test_dir() / "col_numeric_other_width";
  std::filesystem::remove_all(path);
  std::filesystem::create_directories(path);
  const Gen narrow = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g) % 200);
  };
  uint64_t offset = 0;
  uint64_t size = 0;
  {
    tests::IndexDir dir{Db(), path.string()};
    Write(dir, duckdb::LogicalType::INTEGER, {.tier = irs::WriteTier::Flush},
          4096, 4096, narrow, irs::codecs::CodecType::Lz4);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    const auto& block = col->DataBlocks().front();
    ASSERT_EQ(block.codec->type, irs::codecs::CodecType::Lz4);
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
  auto h =
    irs::codecs::ParseFrames(block, size, 4096, irs::codecs::CodecType::Lz4);
  ASSERT_EQ(h.width, sizeof(int32_t));
  ASSERT_LE(h.stored, sizeof(int16_t));
  h.width = sizeof(int16_t);
  std::memcpy(block, &h, irs::codecs::kNumericHeaderSize);
  {
    std::ofstream out{file, std::ios::binary | std::ios::trunc};
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  }
  tests::IndexDir dir{Db(), path.string()};
  const auto scan = [&] {
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    auto state = col->InitScan(r.Ctx());
    duckdb::Vector out{duckdb::LogicalType::INTEGER, STANDARD_VECTOR_SIZE};
    col->Scan(state, out, STANDARD_VECTOR_SIZE);
  };
  ExpectCorrupted(
    scan, "Corrupted numeric segment: value width does not match the column");
}

TEST_F(ColNumericCodecTest, FrameHeadersOfBitPackedFloatsAreRejected) {
  struct Case {
    irs::codecs::CodecType codec;
    Gen gen;
    std::string_view message;
  };
  const Case cases[] = {
    {irs::codecs::CodecType::Bitpacking,
     [](uint64_t g) -> std::optional<int64_t> {
       return static_cast<int64_t>(Mix(g) % 100000);
     },
     "Corrupted numeric segment: a float column has bit-packed frames"},
    {irs::codecs::CodecType::Delta,
     [](uint64_t g) -> std::optional<int64_t> {
       return static_cast<int64_t>(8192 + g);
     },
     "Corrupted numeric segment: a float column has bit-packed steps"},
  };
  for (const auto& [codec, gen, message] : cases) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    const auto path = test_dir() / "col_numeric_packed_floats";
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    uint64_t offset = 0;
    uint64_t size = 0;
    {
      tests::IndexDir dir{Db(), path.string()};
      Write(dir, duckdb::LogicalType::FLOAT, {.tier = irs::WriteTier::Flush},
            4096, 4096, gen, codec);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      const auto& block = col->DataBlocks().front();
      ASSERT_EQ(block.codec->type, codec);
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
    auto h = irs::codecs::ParseFrames(block, size, 4096, codec);
    ASSERT_EQ(h.flags & irs::codecs::kNumericPacked, 0);
    h.flags = irs::codecs::kNumericPacked;
    h.stored = h.width;
    h.frame_log2 = irs::codecs::kBitpackingFrameLog2;
    h.frame_count = 1;
    h.data_size = 0;
    std::memcpy(block, &h, irs::codecs::kNumericHeaderSize);
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    tests::IndexDir dir{Db(), path.string()};
    const auto scan = [&] {
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{duckdb::LogicalType::FLOAT, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
    };
    ExpectCorrupted(scan, message);
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
    tests::IndexDir dir{Db(), path.string()};
    Write(dir, duckdb::LogicalType::BIGINT, {.tier = irs::WriteTier::Flush},
          60000, 60000, shape.gen);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
  tests::IndexDir dir{Db(), path.string()};
  const auto scan = [&] {
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    tests::IndexDir dir{Db(), path.string()};
    Write(dir, duckdb::LogicalType::BIGINT, {.tier = irs::WriteTier::Flush},
          60000, 60000, shape.gen);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
  tests::IndexDir dir{Db(), path.string()};
  const auto scan = [&] {
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, duckdb::LogicalType::BIGINT, {}, kRows, 16384, clustered);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    tests::IndexDir plain_dir{Db(), TestEnv::MemoryRoot()};
    Write(plain_dir, duckdb::LogicalType::BIGINT, {.tier = tier}, kRows, 16384,
          seconds);
    tests::IndexDir scaled_dir{Db(), TestEnv::MemoryRoot()};
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, duckdb::LogicalType::BIGINT, {.tier = tier}, kRows, kRowGroup,
          gen);
    Verify(dir, duckdb::LogicalType::BIGINT, kRows, gen);

    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {.tier = tier}, kRows, kRowGroup, gen, CodecType::Auto);
    Verify(dir, type, kRows, gen);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {.tier = tier}, kRows, kRowGroup, gen, compression);
    Verify(dir, type, kRows, gen);
    for (const auto& transform : Transforms(dir)) {
      EXPECT_NE(transform, "delta/none");
    }
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
        tests::IndexDir chosen{Db(), TestEnv::MemoryRoot()};
        Write(chosen, type, {.tier = tier}, kRows, kRowGroup, gen,
              CodecType::Auto);
        tests::IndexDir differences{Db(), TestEnv::MemoryRoot()};
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {.tier = tier}, kRows, kRowGroup, gen, codec);
    Verify(dir, type, kRows, gen);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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

TEST_F(ColNumericCodecTest, ANamedLz4StoresBooleansAndReadsThemBack) {
  constexpr uint32_t kRowGroup = 4096;
  constexpr uint64_t kRows = 4 * kRowGroup;
  const auto type = duckdb::LogicalType::BOOLEAN;
  const Gen flags = [](uint64_t g) -> std::optional<int64_t> {
    if (g % 97 == 13) {
      return std::nullopt;
    }
    return static_cast<int64_t>(Mix(g) % 7 == 0);
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Flush ? "flush" : "merge");
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {.tier = tier}, kRows, kRowGroup, flags,
          irs::codecs::CodecType::Lz4);
    Verify(dir, type, kRows, flags);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    ASSERT_EQ(col->DataBlocks().size(), kRows / kRowGroup);
    for (const auto& block : col->DataBlocks()) {
      EXPECT_EQ(block.codec->type, irs::codecs::CodecType::Lz4)
        << magic_enum::enum_name(block.codec->type);
    }
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {.tier = tier}, kRows, kRowGroup, gen, codec);
    Verify(dir, type, kRows, gen);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, uuid, {.tier = tier}, kRows, kRowGroup, repeated,
          CodecType::Dict);
    Verify(dir, uuid, kRows, repeated);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    for (const auto& block : col->DataBlocks()) {
      EXPECT_EQ(block.codec->type, CodecType::Dict);
    }
    tests::IndexDir auto_dir{Db(), TestEnv::MemoryRoot()};
    Write(auto_dir, uuid, {.tier = tier}, kRows, kRowGroup, repeated);
    Verify(auto_dir, uuid, kRows, repeated);
    irs::ColReader auto_reader = irs::tests::OpenColReader(auto_dir, kSeg);
    const auto* auto_col = auto_reader.Column(kField);
    ASSERT_NE(auto_col, nullptr);
    for (const auto& block : auto_col->DataBlocks()) {
      EXPECT_EQ(block.codec->type, CodecType::Uncompressed);
    }
  }
}

TEST_F(ColNumericCodecTest, FewWideValuesTakeADictionaryOfWholeValues) {
  constexpr uint32_t kRowGroup = 16384;
  constexpr uint64_t kRows = 2 * kRowGroup + 5000;
  const Gen few = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(Mix(g) % 1000));
  };
  const Gen few_runs = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g / 300 % 40));
  };
  const Gen few_with_nulls = [](uint64_t g) -> std::optional<int64_t> {
    if (g < 3000 || g % 9 == 4) {
      return std::nullopt;
    }
    return static_cast<int64_t>(Mix(Mix(g) % 700));
  };
  using irs::codecs::CodecType;
  const std::vector<std::string> entries(3, "entries");
  for (const duckdb::LogicalType type :
       {duckdb::LogicalType::UUID, duckdb::LogicalType::HUGEINT,
        duckdb::LogicalType::UHUGEINT}) {
    for (const auto& [gen, auto_entries] :
         {std::pair{few, true}, std::pair{few_runs, false},
          std::pair{few_with_nulls, false}}) {
      for (const auto codec : {CodecType::Dict, CodecType::Auto}) {
        SCOPED_TRACE(type.ToString() + " " +
                     std::string{magic_enum::enum_name(codec)});
        tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
        Write(dir, type, {}, kRows, kRowGroup, gen, codec);
        Verify(dir, type, kRows, gen);
        if (type.id() != duckdb::LogicalTypeId::UUID ||
            (codec == CodecType::Auto && !auto_entries)) {
          continue;
        }
        irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
        const auto* col = r.Column(kField);
        ASSERT_NE(col, nullptr);
        for (const auto& block : col->DataBlocks()) {
          EXPECT_EQ(block.codec->type, CodecType::Dict);
        }
        EXPECT_EQ(SegmentInfo(dir, "wide"), entries);
      }
    }
  }
}

TEST_F(ColNumericCodecTest,
       ANamedDictionaryStoresARowGroupItsHintedFunctionDeclines) {
  constexpr uint32_t kRowGroup = 16384;
  constexpr uint64_t kRows = 4 * kRowGroup;
  const Gen gen = [](uint64_t g) -> std::optional<int64_t> {
    const uint64_t distinct = g / kRowGroup < 3 ? 1000 : 3000;
    return static_cast<int64_t>(Mix(Mix(g) % distinct));
  };
  const auto type = duckdb::LogicalType::UUID;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, type, {}, kRows, kRowGroup, gen, irs::codecs::CodecType::Dict);
  Verify(dir, type, kRows, gen);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->DataBlocks().size(), kRows / kRowGroup);
  for (const auto& block : col->DataBlocks()) {
    EXPECT_EQ(block.codec->type, irs::codecs::CodecType::Dict)
      << magic_enum::enum_name(block.codec->type);
  }
  EXPECT_EQ(
    SegmentInfo(dir, "wide"),
    (std::vector<std::string>{"entries", "entries", "entries", "halves"}));
}

TEST_F(ColNumericCodecTest, WholeWideValueDictionariesScanAsDictionaryVectors) {
  constexpr uint64_t kRows = 3 * STANDARD_VECTOR_SIZE + 100;
  const Gen few = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(Mix(g) % 300));
  };
  const Gen few_with_nulls = [](uint64_t g) -> std::optional<int64_t> {
    if (g % 5 == 1 && g < STANDARD_VECTOR_SIZE) {
      return std::nullopt;
    }
    return static_cast<int64_t>(Mix(Mix(g) % 300));
  };
  const auto type = duckdb::LogicalType::UUID;
  for (const auto& [gen, with_nulls] :
       {std::pair{few, false}, std::pair{few_with_nulls, true}}) {
    SCOPED_TRACE(with_nulls ? "with nulls" : "without nulls");
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {}, kRows, kRows, gen, irs::codecs::CodecType::Dict);
    Verify(dir, type, kRows, gen);
    EXPECT_EQ(SegmentInfo(dir, "wide"), std::vector<std::string>{"entries"});
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    auto state = col->InitScan(r.Ctx());
    std::vector<duckdb::VectorType> kinds;
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      const auto take = std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, take);
      kinds.push_back(out.GetVectorType());
      out.Flatten(take);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        ExpectRow(out, k, pos + k, gen, type);
      }
    }
    using duckdb::VectorType;
    const auto whole =
      with_nulls ? VectorType::FLAT_VECTOR : VectorType::DICTIONARY_VECTOR;
    EXPECT_EQ(kinds,
              (std::vector{whole, whole, whole, VectorType::FLAT_VECTOR}));
  }
}

TEST_F(ColNumericCodecTest, WideDictionariesOfHalvesStillReadBack) {
  constexpr std::string_view kHalves = "irs::WideDictionaryOfHalves";
  if (!duckdb::FaultPoints::Arm(kHalves)) {
    GTEST_SKIP() << "this build has no fault injection";
  }
  absl::Cleanup clear = [] { duckdb::FaultPoints::Clear(); };
  constexpr uint32_t kRowGroup = 16384;
  constexpr uint64_t kRows = 2 * kRowGroup + 5000;
  const Gen few = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(Mix(g) % 1000));
  };
  const auto type = duckdb::LogicalType::UUID;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, type, {}, kRows, kRowGroup, few, irs::codecs::CodecType::Dict);
  duckdb::FaultPoints::Clear();
  Verify(dir, type, kRows, few);
  EXPECT_EQ(SegmentInfo(dir, "wide"), std::vector<std::string>(3, "halves"));
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {.tier = tier}, kRows, kRowGroup, gen, codec);
    Verify(dir, type, kRows, gen);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
        tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
        Write(dir, type, {}, kRows, kRowGroup, gen, codec);
        Verify(dir, type, kRows, gen);
      }
    }
  }
}

TEST_F(
  ColNumericCodecTest,
  ANamedDeltaLeavesWideValuesPlainWhenTheirHalvesCannotPackBelowTheirWidth) {
  constexpr uint32_t kRowGroup = 4096;
  constexpr uint64_t kRows = 2 * kRowGroup;
  const Gen apart = [](uint64_t g) -> std::optional<int64_t> {
    return (g % 2 == 0 ? int64_t{0} : int64_t{1} << 47) +
           static_cast<int64_t>(g);
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, duckdb::LogicalType::UUID, {}, kRows, kRowGroup, apart,
        irs::codecs::CodecType::Delta);
  Verify(dir, duckdb::LogicalType::UUID, kRows, apart);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->DataBlocks().size(), 2u);
  for (const auto& block : col->DataBlocks()) {
    EXPECT_EQ(block.codec->type, irs::codecs::CodecType::Uncompressed);
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, duckdb::LogicalType::UTINYINT, {}, kRows, kRows, wrapping,
        irs::codecs::CodecType::Bitpacking);
  Verify(dir, duckdb::LogicalType::UTINYINT, kRows, wrapping);
  EXPECT_NE(SegmentInfo(dir, "data_bytes"), std::vector<std::string>{"0"});
}

TEST_F(ColNumericCodecTest, ANamedDeltaStoresEvenlySpacedFloatsAsSteps) {
  constexpr uint64_t kRows = 8192;
  const Gen spaced = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(8192 + g);
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, duckdb::LogicalType::FLOAT, {}, kRows, kRows, spaced,
        irs::codecs::CodecType::Delta);
  Verify(dir, duckdb::LogicalType::FLOAT, kRows, spaced);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  for (const auto& block : col->DataBlocks()) {
    EXPECT_EQ(block.codec->type, irs::codecs::CodecType::Delta);
  }
  EXPECT_LT(NumericBytes(dir), kRows * sizeof(float) / 2);
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {}, kRows, 16384, rising, irs::codecs::CodecType::Delta);
    ASSERT_EQ(Transforms(dir),
              std::vector<std::string>{"delta_bitpacking/none"});
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    const auto write = [&](const tests::IndexDir& dir,
                           irs::codecs::CodecType compression) {
      irs::ColWriter w{Db(), dir.Tracker(kSeg), irs::WriteTier::Merge};
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    write(dir, codec);
    tests::IndexDir plain_dir{Db(), TestEnv::MemoryRoot()};
    write(plain_dir, irs::codecs::CodecType::Uncompressed);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    irs::ColReader plain_r = irs::tests::OpenColReader(plain_dir, kSeg);
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {.tier = tier}, kRows, 8192, distinct,
          irs::codecs::CodecType::Rle);
    Verify(dir, type, kRows, distinct);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    for (const auto& block : col->DataBlocks()) {
      EXPECT_EQ(block.codec->type, irs::codecs::CodecType::Uncompressed);
    }
    for (const auto& gen : {repeated_between_chunks, with_a_null}) {
      tests::IndexDir other{Db(), TestEnv::MemoryRoot()};
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, type, {}, kRows, 16384, gen, irs::codecs::CodecType::Rle);
      Verify(dir, type, kRows, gen);
      for (const auto& transform : Transforms(dir)) {
        EXPECT_EQ(transform, "rle_bitpacking/none");
      }
    }
  }
}

TEST_F(ColNumericCodecTest, RunsOfMixedLengthsReadBackInEveryWidth) {
  constexpr uint64_t kRows = 40000;
  struct Lengths {
    const char* name;
    std::function<uint64_t(uint64_t)> of;
  };
  const Lengths mixes[] = {
    {"short with a tail",
     [](uint64_t run) {
       return Mix(run) % 16 == 0 ? 9 + Mix(run * 7) % 40 : 1 + Mix(run) % 3;
     }},
    {"uneven", [](uint64_t run) { return 1 + Mix(run) % 40; }},
    {"long with short ones",
     [](uint64_t run) {
       return Mix(run) % 4 == 0 ? 1 + Mix(run * 7) % 5 : 60 + Mix(run) % 400;
     }},
  };
  for (const auto& mix : mixes) {
    SCOPED_TRACE(mix.name);
    std::vector<int64_t> values;
    values.reserve(kRows);
    for (uint64_t run = 0; values.size() < kRows; ++run) {
      const auto v = static_cast<int64_t>(run % 2 * 50 + Mix(run) % 50);
      for (uint64_t i = mix.of(run); i != 0 && values.size() < kRows; --i) {
        values.push_back(v);
      }
    }
    const Gen gen = [&](uint64_t g) -> std::optional<int64_t> {
      return values[g];
    };
    for (const duckdb::LogicalType type :
         {duckdb::LogicalType::TINYINT, duckdb::LogicalType::UTINYINT,
          duckdb::LogicalType::SMALLINT, duckdb::LogicalType::INTEGER,
          duckdb::LogicalType::BIGINT, duckdb::LogicalType::FLOAT,
          duckdb::LogicalType::DOUBLE}) {
      SCOPED_TRACE(type.ToString());
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, type, {}, kRows, 16384, gen, irs::codecs::CodecType::Rle);
      Verify(dir, type, kRows, gen);
      for (const auto& transform : Transforms(dir)) {
        EXPECT_TRUE(transform.starts_with("rle")) << transform;
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {}, kRows, 16384, gen, irs::codecs::CodecType::Rle);
    for (const auto& transform : Transforms(dir)) {
      EXPECT_TRUE(transform.starts_with("rle")) << transform;
    }
    Verify(dir, type, kRows, gen);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, type, {}, kRows, 16384, gen, codec);
      for (const auto& transform : Transforms(dir)) {
        EXPECT_TRUE(transform.contains("bitpacking")) << transform;
      }
      Verify(dir, type, kRows, gen);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {.tier = tier}, kRows, kRowGroup, gen);
    Verify(dir, type, kRows, gen);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, duckdb::LogicalType::BIGINT, {.tier = irs::WriteTier::Merge},
        kRows, kRowGroup, gen);
  Verify(dir, duckdb::LogicalType::BIGINT, kRows, gen);

  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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

TEST_F(ColNumericCodecTest, AutoFollowsTheValuesWhenBitPackingIsDisabled) {
  constexpr uint32_t kRowGroup = 2048;
  constexpr uint64_t kRandom = 40 * kRowGroup;
  constexpr uint64_t kRows = kRandom + 32 * kRowGroup;
  const Gen gen = [](uint64_t g) -> std::optional<int64_t> {
    if (g < kRandom) {
      return static_cast<int64_t>(Mix(g));
    }
    return static_cast<int64_t>(g % 1000) * 1'000'003;
  };
  duckdb::DBConfig::GetConfig(Db()).SetDisabledCompressionMethods(
    {duckdb::CompressionType::COMPRESSION_BITPACKING});
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, duckdb::LogicalType::BIGINT, {.tier = irs::WriteTier::Merge},
        kRows, kRowGroup, gen);
  Verify(dir, duckdb::LogicalType::BIGINT, kRows, gen);

  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
  EXPECT_EQ(*back, kRandom);
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, duckdb::LogicalType::BIGINT, {.tier = tier}, kRows, kRowGroup,
          gen, irs::codecs::CodecType::Rle);
    Verify(dir, duckdb::LogicalType::BIGINT, kRows, gen);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, duckdb::LogicalType::BIGINT, {.tier = tier}, kRows, kRowGroup,
          gen, irs::codecs::CodecType::Bitpacking);
    Verify(dir, duckdb::LogicalType::BIGINT, kRows, gen);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {}, kRows, kRowGroup, gen,
          irs::codecs::CodecType::Bitpacking);
    Verify(dir, type, kRows, gen);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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

TEST_F(ColNumericCodecTest, FullWidthIntegersUnderBitpackingStoreOneRawFrame) {
  constexpr uint32_t kRowGroup = 16384;
  constexpr uint64_t kRows = 3 * kRowGroup + 1000;
  const Gen full = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g));
  };
  using Cmp = duckdb::ExpressionType;
  duckdb::Connection con{_db};
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Flush ? "flush" : "merge");
    for (const duckdb::LogicalType type :
         {duckdb::LogicalType::TINYINT, duckdb::LogicalType::SMALLINT,
          duckdb::LogicalType::INTEGER, duckdb::LogicalType::BIGINT,
          duckdb::LogicalType::UINTEGER, duckdb::LogicalType::UBIGINT}) {
      SCOPED_TRACE(type.ToString());
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, type, {.tier = tier}, kRows, kRowGroup, full,
            irs::codecs::CodecType::Bitpacking);
      Verify(dir, type, kRows, full);
      for (const auto& transform : Transforms(dir)) {
        EXPECT_EQ(transform, "raw/none");
      }
      for (const auto& frames : SegmentInfo(dir, "frames")) {
        EXPECT_EQ(frames, "1");
      }
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      const uint64_t width = duckdb::GetTypeIdSize(type.InternalType());
      for (const auto& block : col->DataBlocks()) {
        EXPECT_EQ(block.byte_size, irs::codecs::kNumericHeaderSize +
                                     sizeof(irs::codecs::PlainHeader) +
                                     irs::codecs::kNumericFrameMetaSize +
                                     block.tuple_count * width);
      }
      const auto key = *At(full, type, kRows / 2);
      for (const auto cmp : {Cmp::COMPARE_EQUAL, Cmp::COMPARE_GREATERTHAN}) {
        const duckdb::ExpressionFilter filter{
          duckdb::BoundComparisonExpression::Create(
            cmp, duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0),
            duckdb::make_uniq<duckdb::BoundConstantExpression>(key))};
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
            const auto v = *At(full, type, anchor + i);
            const bool expected =
              cmp == Cmp::COMPARE_EQUAL ? v == key : key < v;
            const bool got = next < kept && sel.get_index(next) == i;
            next += got ? 1 : 0;
            ASSERT_EQ(got, expected) << "row " << anchor + i << " cmp "
                                     << duckdb::EnumUtil::ToString(cmp);
            if (got) {
              ASSERT_EQ(out.GetValue(i), v);
            }
          }
        }
      }
    }
  }
}

TEST_F(ColNumericCodecTest, ReadsAcrossTheEndOfARawFrameReadBothFrames) {
  constexpr uint32_t kRowGroup = 40 * STANDARD_VECTOR_SIZE;
  constexpr uint64_t kRawRows = 65536;
  constexpr uint64_t kStep = 700;
  const Gen gen = [](uint64_t g) -> std::optional<int64_t> {
    return g < kRawRows ? static_cast<int64_t>(Mix(g)) : int64_t{7};
  };
  const auto type = duckdb::LogicalType::TINYINT;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, type, {}, kRowGroup, kRowGroup, gen, irs::codecs::CodecType::Lz4);
  for (const auto& frames : SegmentInfo(dir, "frames")) {
    EXPECT_EQ(frames, "2");
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  for (const auto& block : col->DataBlocks()) {
    EXPECT_EQ(block.codec->type, irs::codecs::CodecType::Lz4);
  }
  for (const bool whole : {false, true}) {
    SCOPED_TRACE(whole ? "scan" : "scan count");
    auto state = col->InitScan(r.Ctx());
    for (uint64_t pos = 0; pos < kRowGroup; pos += kStep) {
      const auto take = std::min<uint64_t>(kRowGroup - pos, kStep);
      duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
      if (whole) {
        col->Scan(state, out, take);
      } else {
        col->ScanCount(state, out, take, 0);
      }
      for (uint64_t k = 0; k < take; ++k) {
        ASSERT_EQ(out.GetValue(k), *At(gen, type, pos + k))
          << "row " << pos + k;
      }
    }
  }
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, duckdb::LogicalType::BIGINT, {}, kRows, kRowGroup, gen);
    Verify(dir, duckdb::LogicalType::BIGINT, kRows, gen);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, duckdb::LogicalType::BIGINT, {.tier = c.tier}, kRows, 65536,
          c.gen, c.codec);
    const auto transforms = Transforms(dir);
    ASSERT_TRUE(std::ranges::any_of(transforms, [&](const auto& t) {
      return c.stored.empty() ? t.ends_with("/zstd") || t.ends_with("/lz4")
                              : t == c.stored;
    }));
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, c.type, {.tier = tier}, kRows, 65536, c.gen, c.codec);
      const auto transforms = Transforms(dir);
      ASSERT_EQ(transforms.size(), 1);
      ASSERT_EQ(transforms[0].substr(0, transforms[0].find('/')), c.transform);
      ASSERT_EQ(SegmentInfo(dir, "shuffled"), std::vector<std::string>{"true"});
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, duckdb::LogicalType::BIGINT, {.tier = tier}, kRows, 8192,
            shape.gen);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, duckdb::LogicalType::BIGINT, {}, kRows, 65536, clustered,
        irs::codecs::CodecType::RleZstd);
  const auto transforms = Transforms(dir);
  ASSERT_TRUE(std::ranges::any_of(transforms, [](const auto& t) {
    return t.ends_with("/zstd") || t.ends_with("/lz4");
  }));
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, duckdb::LogicalType::BIGINT, {}, kRows, 65536, clustered,
        irs::codecs::CodecType::RleZstd);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {}, kRows, 8192, shape.gen);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, type, {}, kRows, 16384, gen, codec);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, type, {}, kRows, 32768, gen, codec);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
  shapes.push_back({"one_value", [](uint64_t) { return int64_t{42}; }});
  for (const auto codec :
       {CodecType::Auto, CodecType::Bitpacking, CodecType::Rle,
        CodecType::RleZstd, CodecType::Dict, CodecType::DictZstd}) {
    uint64_t named = 0;
    for (const auto& shape : shapes) {
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, type, {}, kRows, 4096, shape.gen, codec);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, type, {.tier = tier}, kRows, kRowGroup, gen);
      Verify(dir, type, kRows, gen);

      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    {
      irs::ColWriter w{Db(), dir.Tracker(kSeg), tier};
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
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    {
      irs::ColWriter w{Db(), dir.Tracker(kSeg), tier};
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
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    tests::IndexDir dir{Db(), path.string()};
    Write(dir, duckdb::LogicalType::BIGINT, {}, 20000, 20000, clustered,
          irs::codecs::CodecType::DeltaLz4);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
  tests::IndexDir dir{Db(), path.string()};
  const auto scan = [&] {
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    auto state = col->InitScan(r.Ctx());
    duckdb::Vector out{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    col->Scan(state, out, STANDARD_VECTOR_SIZE);
  };
  ExpectCorrupted(scan,
                  "Corrupted numeric segment: frame table is inconsistent");
}

TEST_F(ColNumericCodecTest, DamagedFrameTablesAndFramesAreRejected) {
  using irs::codecs::CodecType;
  using irs::codecs::NumericFrameMeta;
  struct Block {
    duckdb::data_ptr_t bytes;
    irs::codecs::FrameHeader header;
    uint64_t rows;
  };
  struct Corruption {
    std::string_view name;
    duckdb::LogicalType type;
    CodecType codec;
    Gen gen;
    uint64_t rows;
    std::string_view transform;
    std::function<void(Block&)> patch;
    std::string_view message;
    std::optional<uint64_t> fetched;
  };
  const Gen random = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g));
  };
  const Gen packed = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g) % 100);
  };
  const Gen four_byte_doubles = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(
      ((uint64_t{1} << 52) + Mix(g) % (uint64_t{1} << 32)) * 16);
  };
  const Gen noise_then_pattern = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(g % 32768 < 16384 ? Mix(g) : Mix(g % 8));
  };
  const auto& drifting_runs =
    std::ranges::find_if(Shapes(), [](const auto& s) {
      return s.name == std::string_view{"drifting_runs"};
    })->gen;
  std::vector<uint32_t> ragged(20000);
  for (uint32_t row = 0, run = 0; row != ragged.size(); ++run) {
    const auto length = 1 + Mix(run) % 255;
    for (uint64_t i = 0; i != length && row != ragged.size(); ++i) {
      ragged[row++] = run;
    }
  }
  const Gen ragged_runs = [&](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(
      ((uint64_t{1} << 52) + Mix(ragged[g] + 1000) % 65536) * 16);
  };
  const Gen every_entry_twice = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g % irs::codecs::kNumericDictMax));
  };
  const Gen drifting_runs_of_four = [](uint64_t g) -> std::optional<int64_t> {
    const auto run = g / 4;
    return static_cast<int64_t>(run * 1000 + Mix(run) % 4);
  };
  const Gen two_paces = [](uint64_t g) -> std::optional<int64_t> {
    return g < irs::codecs::numeric::kBlockValues
             ? static_cast<int64_t>(g * 3 + Mix(g) % 2)
             : static_cast<int64_t>(5000 + g * 1000 + Mix(g) % 2);
  };
  const Gen byte_codes = [](uint64_t g) -> std::optional<int64_t> {
    const uint64_t at = g % irs::codecs::numeric::kBlockValues;
    const uint64_t code = at == 0 ? 0 : at == 1 ? 255 : Mix(g) % 256;
    return static_cast<int64_t>(code * 7 + 1);
  };
  const auto own = [](Block& b) {
    return b.bytes + irs::codecs::OwnHeaderOffset(b.header.flags);
  };
  const auto frame = [](Block& b, uint32_t f) {
    return b.bytes + b.header.off_frames +
           f * irs::codecs::kNumericFrameMetaSize;
  };
  const auto put_header = [](Block& b) {
    std::memcpy(b.bytes, &b.header, irs::codecs::kNumericHeaderSize);
  };
  const Corruption cases[] = {
    {"frames that cover fewer rows than the block", duckdb::LogicalType::DOUBLE,
     CodecType::Bitpacking, four_byte_doubles, 8192, "for",
     [&](Block& b) {
       ASSERT_EQ(b.header.frame_count, 2);
       b.header.frame_count = 1;
       put_header(b);
     },
     "Corrupted numeric segment: frame table is inconsistent"},
    {"a frame that does not start at its row", duckdb::LogicalType::DOUBLE,
     CodecType::Bitpacking, four_byte_doubles, 8192, "for",
     [&](Block& b) {
       ASSERT_EQ(b.header.frame_count, 2);
       auto m = duckdb::Load<NumericFrameMeta>(frame(b, 1));
       ++m.frame.first_entry;
       duckdb::Store(m, frame(b, 1));
     },
     "Corrupted numeric segment: frame table is inconsistent"},
    {"a frame without a byte codec stored at another size",
     duckdb::LogicalType::DOUBLE, CodecType::Bitpacking, random, 4096, "raw",
     [&](Block& b) {
       auto m = duckdb::Load<NumericFrameMeta>(frame(b, 0));
       m.frame.comp_len -= sizeof(double);
       duckdb::Store(m, frame(b, 0));
     },
     "Corrupted numeric segment: frame table is inconsistent"},
    {"a frame of another size than its rows", duckdb::LogicalType::DOUBLE,
     CodecType::Bitpacking, random, 4096, "raw",
     [&](Block& b) {
       auto m = duckdb::Load<NumericFrameMeta>(frame(b, 0));
       m.frame.raw_len -= sizeof(double);
       m.frame.comp_len -= sizeof(double);
       duckdb::Store(m, frame(b, 0));
     },
     "Corrupted numeric segment: frame table is inconsistent"},
    {"an integer frame of another size than its rows",
     duckdb::LogicalType::INTEGER, CodecType::Dict, byte_codes, 16384, "dict",
     [&](Block& b) {
       ASSERT_EQ(b.header.stored, 1);
       auto m = duckdb::Load<NumericFrameMeta>(frame(b, 0));
       --m.frame.raw_len;
       --m.frame.comp_len;
       duckdb::Store(m, frame(b, 0));
     },
     "Corrupted numeric segment: frame table is inconsistent"},
    {"a run frame that starts past the block", duckdb::LogicalType::BIGINT,
     CodecType::Rle, drifting_runs, 8192, "rle_bitpacking",
     [&](Block& b) {
       ASSERT_GE(b.header.frame_count, 2);
       const auto meta = irs::codecs::FrameMetaSize(irs::codecs::Shape::Runs,
                                                    true, sizeof(int64_t));
       duckdb::Store(static_cast<uint32_t>(b.rows),
                     b.bytes + b.header.off_frames + meta + sizeof(uint32_t));
     },
     "Corrupted numeric segment: frame table is inconsistent"},
    {"a block step cut off its frame", duckdb::LogicalType::BIGINT,
     CodecType::Delta, two_paces, 2048, "delta_bitpacking",
     [&](Block& b) {
       ASSERT_EQ(b.header.frame_count, 1);
       const auto* meta = b.bytes + b.header.off_frames;
       const auto comp_off = duckdb::Load<uint32_t>(meta);
       const auto min = duckdb::Load<uint64_t>(meta + 12);
       const auto max = duckdb::Load<uint64_t>(meta + 20);
       const auto* frame = b.bytes + b.header.off_data + comp_off;
       const auto blocks = size_t{2};
       const size_t base_bytes = irs::codecs::numeric::BytesFor(max - min);
       const auto* widths = frame + blocks * base_bytes;
       auto off = irs::codecs::BlockMetaBytes(blocks, base_bytes);
       constexpr uint8_t kOwnStep =
         irs::codecs::kBlockDelta | irs::codecs::kBlockOwnStep;
       for (size_t k = 0; k < blocks; ++k) {
         const uint8_t width = widths[k];
         if ((width & kOwnStep) == kOwnStep) {
           b.header.data_size = static_cast<uint32_t>(comp_off + off + 4);
           put_header(b);
           return;
         }
         off += irs::codecs::numeric::PackedBytes<uint64_t>(
           width & irs::codecs::kBlockDelta
             ? width & irs::codecs::kBlockDeltaBits
             : width,
           irs::codecs::numeric::kBlockValues);
       }
       FAIL() << "no block keeps its own step";
     },
     "Corrupted numeric segment: blocks are inconsistent"},
    {"bit-packed blocks cut before their bases", duckdb::LogicalType::BIGINT,
     CodecType::Bitpacking, packed, 4096, "bitpacking",
     [&](Block& b) {
       ASSERT_EQ(b.header.frame_count, 1);
       b.header.data_size = 4;
       put_header(b);
     },
     "Corrupted numeric segment: blocks are inconsistent"},
    {"an lz4 frame that a point read cannot decode",
     duckdb::LogicalType::BIGINT, CodecType::Lz4, noise_then_pattern,
     32768 + 100, "raw",
     [&](Block& b) {
       ASSERT_EQ(b.header.flags & irs::codecs::kNumericShuffled, 0);
       const auto m = duckdb::Load<NumericFrameMeta>(frame(b, 2));
       ASSERT_LT(m.frame.comp_len, m.frame.raw_len);
       std::memset(b.bytes + b.header.off_data + m.frame.comp_off, 0xFF,
                   m.frame.comp_len);
     },
     "Corrupted numeric segment: frame is inconsistent", 16384 + 5},
    {"run lengths wider than four bytes", duckdb::LogicalType::DOUBLE,
     CodecType::Rle, ragged_runs, 20000, "rle",
     [&](Block& b) {
       auto runs = duckdb::Load<irs::codecs::RunsHeader>(own(b));
       ASSERT_NE(runs.run_width, 0);
       runs.run_width = sizeof(uint64_t);
       duckdb::Store(runs, own(b));
     },
     "Corrupted numeric segment: header is inconsistent"},
    {"a run frame cut inside a run", duckdb::LogicalType::DOUBLE,
     CodecType::Rle, ragged_runs, 20000, "rle",
     [&](Block& b) {
       auto m = duckdb::Load<NumericFrameMeta>(frame(b, 0));
       --m.frame.raw_len;
       --m.frame.comp_len;
       duckdb::Store(m, frame(b, 0));
     },
     "Corrupted numeric segment: runs are inconsistent"},
    {"a stepped block of a single run", duckdb::LogicalType::BIGINT,
     CodecType::Rle, drifting_runs_of_four, 4 * 1025, "rle_bitpacking",
     [&](Block& b) {
       ASSERT_EQ(b.header.frame_count, 2);
       const auto meta = irs::codecs::FrameMetaSize(irs::codecs::Shape::Runs,
                                                    true, sizeof(int64_t));
       const auto* at = b.bytes + b.header.off_frames + meta;
       ASSERT_EQ(duckdb::Load<uint16_t>(at + 2 * sizeof(uint32_t)), 1);
       auto* widths = b.bytes + b.header.off_data + duckdb::Load<uint32_t>(at) +
                      sizeof(int64_t);
       ASSERT_EQ(*widths, 0);
       *widths = irs::codecs::kBlockDelta;
     },
     "Corrupted numeric segment: blocks are inconsistent"},
    {"a run frame without runs", duckdb::LogicalType::DOUBLE, CodecType::Rle,
     ragged_runs, 20000, "rle",
     [&](Block& b) {
       auto m = duckdb::Load<NumericFrameMeta>(frame(b, 0));
       m.frame.raw_len = 0;
       m.frame.comp_len = 0;
       duckdb::Store(m, frame(b, 0));
     },
     "Corrupted numeric segment: runs are inconsistent"},
    {"a dictionary of more entries than a dictionary holds",
     duckdb::LogicalType::BIGINT, CodecType::Dict, every_entry_twice,
     2 * uint64_t{irs::codecs::kNumericDictMax}, "dict_bitpacking",
     [&](Block& b) {
       auto dictionary = duckdb::Load<irs::codecs::DictionaryHeader>(own(b));
       ASSERT_EQ(dictionary.count, irs::codecs::kNumericDictMax);
       ++dictionary.count;
       duckdb::Store(dictionary, own(b));
       b.header.data_size -= sizeof(int64_t);
       put_header(b);
     },
     "Corrupted numeric segment: header is inconsistent"},
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    const auto path = test_dir() / "col_numeric_damaged_frames";
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    uint64_t offset = 0;
    uint64_t size = 0;
    CodecType block_codec = CodecType::Auto;
    {
      tests::IndexDir dir{Db(), path.string()};
      Write(dir, c.type, {.tier = irs::WriteTier::Flush}, c.rows,
            static_cast<uint32_t>(c.rows), c.gen, c.codec);
      Verify(dir, c.type, c.rows, c.gen);
      const auto transforms = Transforms(dir);
      ASSERT_EQ(transforms.size(), 1);
      ASSERT_EQ(transforms[0].substr(0, transforms[0].find('/')), c.transform);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      const auto& block = col->DataBlocks().front();
      offset = block.file_offset;
      size = block.byte_size;
      block_codec = block.codec->type;
    }
    const auto file = path / irs::FileName(kSeg);
    std::string bytes;
    {
      std::ifstream in{file, std::ios::binary};
      bytes.assign(std::istreambuf_iterator<char>{in}, {});
    }
    ASSERT_LE(offset + size, bytes.size());
    Block block{
      .bytes = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset),
      .rows = c.rows,
    };
    block.header =
      irs::codecs::ParseFrames(block.bytes, size, c.rows, block_codec);
    ASSERT_NO_FATAL_FAILURE(c.patch(block));
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    tests::IndexDir dir{Db(), path.string()};
    const auto read = [&] {
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      if (c.fetched) {
        irs::ColumnReader::PointReader cursor{r, *col};
        duckdb::Vector out{c.type, 1};
        cursor.FetchRow(*c.fetched, out, 0);
        return;
      }
      auto state = col->InitScan(r.Ctx());
      for (uint64_t pos = 0; pos < c.rows; pos += STANDARD_VECTOR_SIZE) {
        duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
        col->Scan(state, out,
                  std::min<uint64_t>(c.rows - pos, STANDARD_VECTOR_SIZE));
      }
    };
    ExpectCorrupted(read, c.message);
  }
}

TEST_F(ColNumericCodecTest, SegmentsWithOffsetsOutsideTheirBytesAreRejected) {
  using irs::codecs::CodecType;
  struct Corruption {
    std::string_view name;
    duckdb::LogicalType type;
    CodecType codec;
    Gen gen;
    std::function<void(duckdb::data_ptr_t block, uint64_t size)> patch;
    std::string_view message;
  };
  const Gen decimals = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g) % 100000);
  };
  const Gen two_highs = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(g / 1000 % 2 * 2 + 1);
  };
  const Gen few_wide = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(Mix(g) % 1000));
  };
  const auto whole_entries = [](duckdb::data_ptr_t block) {
    const auto header = duckdb::Load<irs::codecs::numeric::WideHeader>(block);
    EXPECT_EQ(header.form, irs::codecs::numeric::WideForm::Entries);
    return header;
  };
  const Corruption cases[] = {
    {"a dictionary of whole values whose entries run past the segment",
     duckdb::LogicalType::UUID, CodecType::Dict, few_wide,
     [&](duckdb::data_ptr_t block, uint64_t size) {
       auto header = whole_entries(block);
       header.entries = static_cast<uint32_t>(size / sizeof(duckdb::hugeint_t));
       duckdb::Store(header, block);
     },
     "Corrupted numeric segment: header is inconsistent"},
    {"a dictionary of whole values with a code past its entries",
     duckdb::LogicalType::UUID, CodecType::Dict, few_wide,
     [&](duckdb::data_ptr_t block, uint64_t) {
       const auto header = whole_entries(block);
       ASSERT_EQ(header.codes, CodecType::Bitpacking);
       auto* codes = block + sizeof(header) +
                     size_t{header.entries} * sizeof(duckdb::hugeint_t);
       const auto frames = duckdb::Load<irs::codecs::FrameHeader>(codes);
       ASSERT_NE(frames.flags & irs::codecs::kNumericPacked, 0);
       const auto data = duckdb::AlignValue<size_t, 8>(
         irs::codecs::kNumericHeaderSize + sizeof(irs::codecs::PlainHeader) +
         size_t{frames.frame_count} *
           irs::codecs::FrameMetaSize(irs::codecs::Shape::Plain, true,
                                      sizeof(uint32_t)));
       codes[data] = 0xFF;
       codes[data + 1] = 0xFF;
     },
     "Corrupted numeric segment: dictionary code is out of range"},
    {"an alp vector of no bytes", duckdb::LogicalType::DOUBLE, CodecType::Alp,
     decimals,
     [](duckdb::data_ptr_t block, uint64_t) {
       const auto end = duckdb::Load<uint32_t>(block);
       const auto first =
         duckdb::Load<uint32_t>(block + end - sizeof(uint32_t));
       duckdb::Store(first, block + end - 2 * sizeof(uint32_t));
     },
     "Corrupted ALP segment: vector offsets do not describe a data range"},
    {"alprd vectors that end inside the dictionary",
     duckdb::LogicalType::DOUBLE, CodecType::Alprd, decimals,
     [](duckdb::data_ptr_t block, uint64_t) {
       constexpr uint32_t kDictionaryAt = sizeof(uint32_t) + 3;
       ASSERT_GT(block[kDictionaryAt - 1], 0);
       duckdb::Store(kDictionaryAt, block);
     },
     "Corrupted ALPRD segment: metadata offset table exceeds the segment"},
    {"a high half that starts past its segment", duckdb::LogicalType::UUID,
     CodecType::Bitpacking, two_highs,
     [](duckdb::data_ptr_t block, uint64_t size) {
       auto header = duckdb::Load<irs::codecs::numeric::WideHeader>(block);
       ASSERT_EQ(header.form, irs::codecs::numeric::WideForm::Halves);
       header.low_size = size - 1;
       duckdb::Store(header, block);
     },
     "Corrupted numeric segment: header is inconsistent"},
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    const auto path = test_dir() / "col_numeric_offsets_outside";
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    constexpr uint64_t kRows = 4096;
    uint64_t offset = 0;
    uint64_t size = 0;
    {
      tests::IndexDir dir{Db(), path.string()};
      Write(dir, c.type, {.tier = irs::WriteTier::Flush}, kRows, kRows, c.gen,
            c.codec);
      Verify(dir, c.type, kRows, c.gen);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      ASSERT_EQ(col->DataBlocks().size(), 1);
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
    ASSERT_NO_FATAL_FAILURE(c.patch(
      reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset), size));
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    tests::IndexDir dir{Db(), path.string()};
    const auto scan = [&] {
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      auto state = col->InitScan(r.Ctx());
      for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
        duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
        col->Scan(state, out, STANDARD_VECTOR_SIZE);
      }
    };
    ExpectCorrupted(scan, c.message);
  }
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
        tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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

TEST(ColNumericKernelsTest, RunEndsOfEveryWidthAndLength) {
  using irs::codecs::numeric::PrefixRunEnds;
  for (const uint8_t width : {1, 2, 4}) {
    for (size_t n = 0; n <= 37; ++n) {
      SCOPED_TRACE(testing::Message() << int{width} << " " << n);
      std::vector<uint8_t> in(n * width);
      std::vector<uint32_t> lengths(n);
      for (size_t i = 0; i != n; ++i) {
        lengths[i] = 1 + static_cast<uint32_t>(Mix(i + n * 131 + width) % 200);
        std::memcpy(in.data() + i * width, &lengths[i], width);
      }
      std::vector<uint32_t> out(n + 1, 0x5a);
      const auto ends = PrefixRunEnds(in.data(), n, width, out.data());
      uint64_t total = 0;
      uint32_t shortest = n == 0 ? 0 : std::numeric_limits<uint32_t>::max();
      for (size_t i = 0; i != n; ++i) {
        total += lengths[i];
        shortest = std::min(shortest, lengths[i]);
        ASSERT_EQ(out[i], total) << i;
      }
      EXPECT_EQ(ends.total, total);
      EXPECT_EQ(ends.shortest, shortest);
      EXPECT_EQ(out[n], 0x5a);
      if (n != 0) {
        in[(n / 2) * width] = 0;
        if (width > 1) {
          in[(n / 2) * width + 1] = 0;
        }
        EXPECT_EQ(PrefixRunEnds(in.data(), n, width, out.data()).shortest, 0);
      }
    }
  }
}

TEST(ColNumericKernelsTest, RunEndsThatOverflowAreBroken) {
  using irs::codecs::numeric::kBrokenRuns;
  using irs::codecs::numeric::PrefixRunEnds;
  for (const size_t n : {3, 9, 17}) {
    SCOPED_TRACE(n);
    std::vector<uint32_t> lengths(n, 1U << 30);
    std::vector<uint32_t> out(n);
    const auto ends =
      PrefixRunEnds(reinterpret_cast<const uint8_t*>(lengths.data()), n,
                    sizeof(uint32_t), out.data());
    EXPECT_EQ(ends.total, n < 4 ? uint64_t{n} << 30 : kBrokenRuns);
    EXPECT_EQ(ends.shortest, 1U << 30);
  }
  std::vector<uint32_t> in_place(9, 1U << 30);
  const auto ends =
    PrefixRunEnds(reinterpret_cast<const uint8_t*>(in_place.data()),
                  in_place.size(), sizeof(uint32_t), in_place.data());
  EXPECT_EQ(ends.total, kBrokenRuns);
}

TEST(ColNumericKernelsTest, RunEndsOfLongListsThatFitAreNotBroken) {
  using irs::codecs::numeric::PrefixRunEnds;
  constexpr size_t kRuns = std::numeric_limits<uint32_t>::max() / 255 + 2;
  std::vector<uint8_t> in(kRuns, 1);
  in.front() = 255;
  in.back() = 255;
  std::vector<uint32_t> out(kRuns);
  const auto ends = PrefixRunEnds(in.data(), kRuns, 1, out.data());
  EXPECT_EQ(ends.total, uint64_t{kRuns} - 2 + 2 * 255);
  EXPECT_EQ(ends.shortest, 1);
  EXPECT_EQ(out.front(), 255);
  EXPECT_EQ(out.back(), ends.total);
}

TEST(ColNumericKernelsTest, ExpandsRunsOfEveryWidthAndLength) {
  const auto check = []<typename U, typename T>() {
    for (const uint64_t one_in : {0, 1, 2, 5, 23}) {
      for (size_t rows = 0; rows <= 70; ++rows) {
        SCOPED_TRACE(testing::Message()
                     << sizeof(U) << " " << one_in << " " << rows);
        std::vector<uint8_t> ended(rows);
        std::vector<U> runs{static_cast<U>(Mix(rows + one_in))};
        std::vector<U> expected;
        for (size_t i = 0; i != rows; ++i) {
          expected.push_back(runs.back());
          if (one_in != 0 && Mix(i * 131 + rows + one_in) % one_in == 0) {
            ended[i] = 0xFF;
            runs.push_back(static_cast<U>(Mix(runs.size() * 977 + rows)));
          }
        }
        runs.resize(runs.size() + irs::codecs::numeric::kRunPad);
        expected.push_back(U{0x5a});
        std::vector<T> out(rows + 1, std::bit_cast<T>(U{0x5a}));
        irs::codecs::numeric::ExpandMarkedRuns(ended.data(), runs.data(), rows,
                                               out.data());
        ASSERT_EQ(
          std::memcmp(out.data(), expected.data(), expected.size() * sizeof(U)),
          0);
      }
    }
  };
  check.template operator()<uint8_t, int8_t>();
  check.template operator()<uint16_t, uint16_t>();
  check.template operator()<uint32_t, int32_t>();
  check.template operator()<uint32_t, float>();
  check.template operator()<uint64_t, uint64_t>();
  check.template operator()<uint64_t, double>();
}

TEST(ColNumericKernelsTest, ExpandsRunsFromAnyRowOverAnyCount) {
  constexpr uint32_t kRows = 3 * STANDARD_VECTOR_SIZE + 77;
  const auto check = []<typename U>() {
    std::vector<uint32_t> ends;
    std::vector<U> values;
    std::vector<U> rows;
    std::vector<uint32_t> run_of;
    while (rows.size() < kRows) {
      const auto run = static_cast<uint32_t>(ends.size());
      const auto length =
        Mix(run) % 7 == 0 ? 20 + Mix(run * 31) % 3000 : 1 + Mix(run * 31) % 4;
      values.push_back(static_cast<U>(Mix(run * 977 + sizeof(U))));
      for (uint64_t i = 0; i != length && rows.size() < kRows; ++i) {
        rows.push_back(values.back());
        run_of.push_back(run);
      }
      ends.push_back(static_cast<uint32_t>(rows.size()));
    }
    const auto runs = static_cast<uint32_t>(ends.size());
    run_of.push_back(runs);
    values.resize(values.size() + irs::codecs::numeric::kRunPad);
    for (const uint32_t row : {0U, 1U, 7U, 2047U, 2048U, 2049U, 3001U, 6000U}) {
      for (const uint32_t count :
           {0U, 1U, 15U, 16U, 17U, 2047U, 2048U, 2049U, 4096U, kRows}) {
        const auto take = std::min(count, kRows - row);
        SCOPED_TRACE(testing::Message()
                     << sizeof(U) << " " << row << " " << take);
        std::vector<U> out(size_t{take} + 1, U{0x5a});
        const auto next = irs::codecs::numeric::ExpandRuns(
          std::span<const uint32_t>{ends}, values.data(), run_of[row], row,
          take, out.data());
        EXPECT_EQ(next, run_of[row + take]);
        EXPECT_EQ(out[take], U{0x5a});
        ASSERT_EQ(std::memcmp(out.data(), rows.data() + row, take * sizeof(U)),
                  0);
      }
    }
  };
  check.template operator()<uint8_t>();
  check.template operator()<uint16_t>();
  check.template operator()<uint32_t>();
  check.template operator()<uint64_t>();
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

struct VariantSegment {
  std::string bytes;
  uint64_t rows = 0;
};

VariantSegment CompressVariant(duckdb::DatabaseInstance& db,
                               irs::codecs::CodecType codec_type,
                               size_t variant,
                               const std::vector<int64_t>& values) {
  const duckdb::LogicalType type = duckdb::LogicalType::BIGINT;
  std::vector<duckdb::Vector> vectors;
  for (size_t pos = 0; pos < values.size(); pos += STANDARD_VECTOR_SIZE) {
    const auto take =
      std::min<size_t>(values.size() - pos, STANDARD_VECTOR_SIZE);
    auto& v = vectors.emplace_back(type, STANDARD_VECTOR_SIZE);
    std::copy_n(values.data() + pos, take,
                duckdb::FlatVector::GetDataMutable<int64_t>(v));
    duckdb::FlatVector::SetSize(v, take);
  }
  std::vector<irs::WriteChunk> chunks;
  for (const auto& v : vectors) {
    chunks.push_back(irs::RefChunk(v, v.size()));
  }
  duckdb::MemoryStream out;
  irs::WriteContext ctx{db.GetBufferManager(), out};
  VariantSegment segment{.rows = values.size()};
  size_t flushed = 0;
  const auto flush = [&](const irs::codecs::Codec&, duckdb::BaseStatistics,
                         uint64_t rows, std::string_view bytes,
                         std::string_view) {
    EXPECT_EQ(rows, values.size());
    segment.bytes.assign(bytes);
    ++flushed;
  };
  irs::codecs::ColumnTuning tuning;
  irs::codecs::ColumnDataCheckpointData data{
    .type = type,
    .ctx = &ctx,
    .flush = flush,
    .tuning = tuning,
    .rows = {chunks, values.size()},
  };
  const auto& codec = *irs::codecs::GetCodec(codec_type, type.InternalType());
  const auto& fn = codec.compression[variant];
  if (fn.analyze) {
    EXPECT_LT(fn.analyze(data, irs::codecs::kNoScore), irs::codecs::kNoScore);
  }
  EXPECT_TRUE(fn.compress(codec, data));
  EXPECT_EQ(flushed, 1);
  return segment;
}

}  // namespace

TEST_F(ColNumericCodecTest, AFrameOpenedByItsStartIsInflatedWholeForAScan) {
  std::vector<int64_t> values(3000);
  for (size_t g = 0; g < values.size(); ++g) {
    values[g] = static_cast<int64_t>(Mix(g % 64) % 3000);
  }
  const auto segment =
    CompressVariant(Db(), irs::codecs::CodecType::Lz4, 2, values);
  const auto* base =
    reinterpret_cast<duckdb::const_data_ptr_t>(segment.bytes.data());
  const auto frames = irs::codecs::ParseFrames(
    base, segment.bytes.size(), segment.rows, irs::codecs::CodecType::Lz4);
  ASSERT_EQ(irs::codecs::TransformName(irs::codecs::Shape::Plain, frames),
            "for");
  ASSERT_EQ(frames.flags & irs::codecs::kNumericShuffled, 0);
  irs::codecs::numeric::PlainReader<int64_t> reader{
    base, frames, irs::codecs::CodecType::Lz4};
  EXPECT_EQ(reader.FetchApart(5, irs::codecs::kNoRow), values[5]);
  std::vector<int64_t> read(values.size());
  reader.Read(0, read.size(), read.data());
  EXPECT_EQ(read, values);
}

TEST_F(ColNumericCodecTest, AConstantCheckInsideOneBlockReadsThatBlockAlone) {
  constexpr uint64_t kBlock = irs::codecs::numeric::kBlockValues;
  std::vector<int64_t> values(3 * kBlock);
  for (size_t g = 0; g < values.size(); ++g) {
    values[g] = g < kBlock ? 7 : static_cast<int64_t>(Mix(g) % 1000);
  }
  const auto segment =
    CompressVariant(Db(), irs::codecs::CodecType::Bitpacking, 2, values);
  const auto* base =
    reinterpret_cast<duckdb::const_data_ptr_t>(segment.bytes.data());
  const auto frames =
    irs::codecs::ParseFrames(base, segment.bytes.size(), segment.rows,
                             irs::codecs::CodecType::Bitpacking);
  ASSERT_EQ(irs::codecs::TransformName(irs::codecs::Shape::Plain, frames),
            "bitpacking");
  irs::codecs::numeric::PlainReader<int64_t> reader{
    base, frames, irs::codecs::CodecType::Bitpacking};
  int64_t value = 0;
  EXPECT_TRUE(reader.ConstantRows(10, 100, value));
  EXPECT_EQ(value, 7);
  EXPECT_FALSE(reader.ConstantRows(kBlock - 24, 100, value));
}

TEST_F(ColNumericCodecTest, ABitPackedRowFetchedApartIsUnpackedFromItsBlock) {
  constexpr uint64_t kBlock = irs::codecs::numeric::kBlockValues;
  std::vector<int64_t> values(3 * kBlock);
  for (size_t g = 0; g < values.size(); ++g) {
    values[g] = g < kBlock ? 7 : static_cast<int64_t>(Mix(g) % 1000);
  }
  const auto segment =
    CompressVariant(Db(), irs::codecs::CodecType::Bitpacking, 2, values);
  const auto* base =
    reinterpret_cast<duckdb::const_data_ptr_t>(segment.bytes.data());
  const auto frames =
    irs::codecs::ParseFrames(base, segment.bytes.size(), segment.rows,
                             irs::codecs::CodecType::Bitpacking);
  ASSERT_EQ(irs::codecs::TransformName(irs::codecs::Shape::Plain, frames),
            "bitpacking");
  irs::codecs::numeric::PlainReader<int64_t> reader{
    base, frames, irs::codecs::CodecType::Bitpacking};
  for (const uint64_t row :
       {kBlock + 5, uint64_t{3}, 2 * kBlock + 100, 3 * kBlock - 1}) {
    EXPECT_EQ(reader.FetchApart(row, irs::codecs::kNoRow), values[row])
      << "row " << row;
  }
}

TEST_F(ColNumericCodecTest, BitPackedFramesAtAnUnalignedAddressAreRejected) {
  std::vector<int64_t> values(3000);
  for (size_t g = 0; g < values.size(); ++g) {
    values[g] = static_cast<int64_t>(Mix(g) % 1000);
  }
  const auto segment =
    CompressVariant(Db(), irs::codecs::CodecType::Bitpacking, 2, values);
  const auto size = segment.bytes.size();
  std::vector<uint64_t> words(size / sizeof(uint64_t) + 2);
  auto* aligned = reinterpret_cast<uint8_t*>(words.data());
  std::memcpy(aligned, segment.bytes.data(), size);
  const auto frames = irs::codecs::ParseFrames(
    aligned, size, segment.rows, irs::codecs::CodecType::Bitpacking);
  ASSERT_EQ(irs::codecs::TransformName(irs::codecs::Shape::Plain, frames),
            "bitpacking");
  std::memmove(aligned + 1, aligned, size);
  try {
    irs::codecs::ParseFrames(aligned + 1, size, segment.rows,
                             irs::codecs::CodecType::Bitpacking);
    ADD_FAILURE() << "an unaligned bit-packed segment was accepted";
  } catch (const irs::SqlException& e) {
    EXPECT_EQ(e.error().errcode, ERRCODE_INDEX_CORRUPTED);
    EXPECT_EQ(e.message(), "Corrupted numeric segment: header is inconsistent");
  }
}

TEST_F(ColNumericCodecTest, WideValuesGiveEmptyStatisticsForANonNumericType) {
  const duckdb::LogicalType type = duckdb::LogicalType::HUGEINT;
  duckdb::MemoryStream out;
  irs::WriteContext ctx{Db().GetBufferManager(), out};
  const auto flush = [](const irs::codecs::Codec&, duckdb::BaseStatistics,
                        uint64_t, std::string_view, std::string_view) {};
  irs::codecs::ColumnTuning tuning;
  irs::codecs::ColumnDataCheckpointData data{
    .type = type,
    .ctx = &ctx,
    .flush = flush,
    .tuning = tuning,
  };
  irs::codecs::numeric::WideValues<duckdb::hugeint_t> wide{data};
  EXPECT_EQ(wide.Stats(duckdb::LogicalType::VARCHAR).GetStatsType(),
            duckdb::StatisticsType::STRING_STATS);
}

TEST_F(ColNumericCodecTest, FiltersIntoAVectorThatHeldNullsLeaveNoStaleNulls) {
  constexpr uint64_t kRows = 4 * STANDARD_VECTOR_SIZE;
  const Gen gen = [](uint64_t g) -> std::optional<int64_t> {
    if (g < 2 * STANDARD_VECTOR_SIZE && g % 3 == 0) {
      return std::nullopt;
    }
    return static_cast<int64_t>(Mix(g) % 1000);
  };
  const auto type = duckdb::LogicalType::BIGINT;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, type, {}, kRows, STANDARD_VECTOR_SIZE, gen);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  duckdb::Connection con{_db};
  const duckdb::ExpressionFilter filter{
    duckdb::BoundComparisonExpression::Create(
      duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO,
      duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0),
      duckdb::make_uniq<duckdb::BoundConstantExpression>(
        duckdb::Value::BIGINT(0)))};
  auto filter_state =
    duckdb::TableFilterState::Initialize(*con.context, filter);
  for (const auto check :
       {irs::NullCheckKind::None, irs::NullCheckKind::IsNotNull}) {
    auto state = col->InitScan(r.Ctx());
    duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
    for (uint64_t anchor = 0; anchor < kRows; anchor += STANDARD_VECTOR_SIZE) {
      duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
      for (duckdb::idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
        sel.set_index(i, i);
      }
      const auto kept = col->GatherFilter(state, anchor, STANDARD_VECTOR_SIZE,
                                          sel, STANDARD_VECTOR_SIZE, filter,
                                          *filter_state, check, out);
      duckdb::idx_t next = 0;
      for (duckdb::idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
        const auto v = gen(anchor + i);
        const bool got = next < kept && sel.get_index(next) == i;
        next += got ? 1 : 0;
        ASSERT_EQ(got, v.has_value()) << "row " << anchor + i;
        if (got && check == irs::NullCheckKind::None) {
          ASSERT_EQ(out.GetValue(i), duckdb::Value::BIGINT(*v))
            << "row " << anchor + i;
        }
      }
    }
  }
}

namespace {

using ValueAt = std::function<duckdb::Value(uint64_t)>;

void WriteValues(duckdb::DatabaseInstance& db, const tests::IndexDir& dir,
                 const duckdb::LogicalType& type, irs::WriteTier tier,
                 uint64_t rows, uint32_t rg_size, irs::codecs::CodecType codec,
                 const ValueAt& value,
                 irs::DistinctStats distinct = irs::DistinctStats::None) {
  irs::ColWriter w{db, dir.Tracker(kSeg), tier};
  auto& cw = w.OpenColumn(kField, type,
                          {.row_group_size = rg_size,
                           .compression = codec,
                           .distinct = distinct,
                           .codec_params = {.tier = tier}});
  for (uint64_t pos = 0; pos < rows;) {
    const auto take = std::min<uint64_t>(rows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector vec{type, STANDARD_VECTOR_SIZE};
    for (uint64_t k = 0; k < take; ++k) {
      vec.SetValue(k, value(pos + k));
    }
    duckdb::FlatVector::SetSize(vec, take);
    cw.Append(vec, take);
    pos += take;
  }
  ASSERT_TRUE(w.Commit(0));
}

void ExpectValues(const tests::IndexDir& dir, const duckdb::LogicalType& type,
                  uint64_t rows, const ValueAt& value) {
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), rows);
  auto state = col->InitScan(r.Ctx());
  for (uint64_t pos = 0; pos < rows;) {
    const auto take = std::min<uint64_t>(rows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
    col->Scan(state, out, take);
    out.Flatten(take);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      const auto want = value(pos + k);
      const auto got = out.GetValue(k);
      ASSERT_EQ(got.ToString(), want.ToString()) << "row " << pos + k;
    }
    pos += take;
  }
}

}  // namespace

TEST_F(ColNumericCodecTest,
       ANamedDictKeepsByteCodesInFramesWhenPackingThemSavesNothing) {
  constexpr uint64_t kRows = 16384;
  const Gen every_code_in_every_block =
    [](uint64_t g) -> std::optional<int64_t> {
    const uint64_t at = g % irs::codecs::numeric::kBlockValues;
    const uint64_t code = at == 0 ? 0 : at == 1 ? 255 : Mix(g) % 256;
    return static_cast<int64_t>(code * 7 + 1);
  };
  for (const auto& type :
       {duckdb::LogicalType::SMALLINT, duckdb::LogicalType::USMALLINT,
        duckdb::LogicalType::INTEGER, duckdb::LogicalType::UINTEGER,
        duckdb::LogicalType::FLOAT}) {
    SCOPED_TRACE(duckdb::LogicalType{type}.ToString());
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {}, kRows, kRows, every_code_in_every_block,
          irs::codecs::CodecType::Dict);
    Verify(dir, type, kRows, every_code_in_every_block);
    EXPECT_EQ(Transforms(dir), std::vector<std::string>{"dict/none"});
    EXPECT_EQ(SegmentInfo(dir, "stored"), std::vector<std::string>{"1"});
  }
}

TEST_F(ColNumericCodecTest, ANamedZstdShufflesTheTwoByteOffsetsOfNearbyFloats) {
  constexpr uint64_t kRows = 40000;
  const Gen nearby = [](uint64_t g) -> std::optional<int64_t> {
    return 65536 + 256 + static_cast<int64_t>(Mix(g) % 512);
  };
  const auto type = duckdb::LogicalType::FLOAT;
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(static_cast<int>(tier));
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {.tier = tier}, kRows, kRows, nearby,
          irs::codecs::CodecType::Zstd);
    Verify(dir, type, kRows, nearby);
    EXPECT_EQ(Transforms(dir), std::vector<std::string>{"for/zstd"});
    EXPECT_EQ(SegmentInfo(dir, "stored"), std::vector<std::string>{"2"});
    EXPECT_EQ(SegmentInfo(dir, "shuffled"), std::vector<std::string>{"true"});
  }
}

TEST_F(ColNumericCodecTest,
       ANamedRunCodecWithALeafNarrowsFloatRunsToTheirRange) {
  constexpr uint64_t kRows = 20000;
  struct Case {
    duckdb::LogicalType type;
    Gen gen;
    std::string_view stored;
  };
  const Case cases[] = {
    {duckdb::LogicalType::FLOAT,
     [](uint64_t g) -> std::optional<int64_t> {
       return 65536 + static_cast<int64_t>(g / 10 % 2);
     },
     "1"},
    {duckdb::LogicalType::FLOAT,
     [](uint64_t g) -> std::optional<int64_t> {
       return 65536 + static_cast<int64_t>(g / 10 % 2) * 300;
     },
     "2"},
    {duckdb::LogicalType::DOUBLE,
     [](uint64_t g) -> std::optional<int64_t> {
       return static_cast<int64_t>(((uint64_t{1} << 52) + g / 10 % 3) * 16);
     },
     "1"},
  };
  for (const auto& [type, gen, stored] : cases) {
    SCOPED_TRACE(type.ToString() + " " + std::string{stored});
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {}, kRows, 8192, gen, irs::codecs::CodecType::RleLz4);
    Verify(dir, type, kRows, gen);
    const auto transforms = Transforms(dir);
    ASSERT_EQ(transforms.size(), 3);
    for (const auto& transform : transforms) {
      EXPECT_EQ(transform, "rle/lz4");
    }
    for (const auto& width : SegmentInfo(dir, "stored")) {
      EXPECT_EQ(width, stored);
    }
  }
}

TEST_F(ColNumericCodecTest, ANamedDeltaLz4StoresStepsThatNeedFourBytes) {
  constexpr uint64_t kRows = 20000;
  const Gen far_steps = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(g * 3'000'000 + Mix(g) % 1'000'000);
  };
  const auto type = duckdb::LogicalType::UBIGINT;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, type, {}, kRows, kRows, far_steps,
        irs::codecs::CodecType::DeltaLz4);
  Verify(dir, type, kRows, far_steps);
  EXPECT_EQ(Transforms(dir), std::vector<std::string>{"delta/lz4"});
  EXPECT_EQ(SegmentInfo(dir, "stored"), std::vector<std::string>{"4"});
}

TEST_F(ColNumericCodecTest, NestedListElementsReadBack) {
  constexpr uint64_t kRows = 4 * STANDARD_VECTOR_SIZE;
  for (const auto& [id, nulls] :
       {std::pair{duckdb::LogicalTypeId::INTEGER, true},
        std::pair{duckdb::LogicalTypeId::HUGEINT, false}}) {
    const duckdb::LogicalType element{id};
    const auto inner = duckdb::LogicalType::LIST(element);
    const auto type = duckdb::LogicalType::LIST(inner);
    const ValueAt row = [&](uint64_t g) {
      duckdb::vector<duckdb::Value> lists;
      for (uint64_t i = 0, n = 1 + Mix(g) % 2; i < n; ++i) {
        duckdb::vector<duckdb::Value> items;
        for (uint64_t j = 0, m = 1 + Mix(g * 3 + i) % 9; j < m; ++j) {
          const auto at = Mix(g * 16 + i * 8 + j);
          items.push_back(nulls && at % 13 == 0
                            ? duckdb::Value{element}
                            : Cast(static_cast<int64_t>(at % 100000), element));
        }
        lists.push_back(duckdb::Value::LIST(element, items));
      }
      return duckdb::Value::LIST(inner, lists);
    };
    for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
      SCOPED_TRACE(element.ToString() + " " +
                   std::to_string(static_cast<int>(tier)));
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      ASSERT_NO_FATAL_FAILURE(WriteValues(Db(), dir, type, tier, kRows, kRows,
                                          irs::codecs::CodecType::Auto, row));
      ExpectValues(dir, type, kRows, row);
    }
  }
}

TEST_F(ColNumericCodecTest, RowGroupsOfEmptyListsReadBackInEveryNumericType) {
  constexpr uint32_t kRowGroup = STANDARD_VECTOR_SIZE;
  constexpr uint64_t kRows = 3 * kRowGroup;
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    for (const auto id :
         {duckdb::LogicalType::TINYINT, duckdb::LogicalType::SMALLINT,
          duckdb::LogicalType::INTEGER, duckdb::LogicalType::BIGINT,
          duckdb::LogicalType::UTINYINT, duckdb::LogicalType::USMALLINT,
          duckdb::LogicalType::UINTEGER, duckdb::LogicalType::UBIGINT,
          duckdb::LogicalType::HUGEINT, duckdb::LogicalType::UHUGEINT,
          duckdb::LogicalType::FLOAT, duckdb::LogicalType::DOUBLE}) {
      const duckdb::LogicalType element{id};
      SCOPED_TRACE(element.ToString() + " " +
                   std::to_string(static_cast<int>(tier)));
      const auto type = duckdb::LogicalType::LIST(element);
      const ValueAt row = [&](uint64_t g) {
        duckdb::vector<duckdb::Value> items;
        if (g / kRowGroup == 1) {
          for (uint64_t j = 0; j <= g % 3; ++j) {
            items.push_back(
              Cast(static_cast<int64_t>(Mix(g * 4 + j) % 1000), element));
          }
        } else if (g / kRowGroup == 2 && g % 2 == 0) {
          return duckdb::Value{type};
        }
        return duckdb::Value::LIST(element, items);
      };
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      ASSERT_NO_FATAL_FAILURE(WriteValues(Db(), dir, type, tier, kRows,
                                          kRowGroup,
                                          irs::codecs::CodecType::Auto, row));
      ExpectValues(dir, type, kRows, row);
    }
  }
}

TEST_F(ColNumericCodecTest, ADictionaryOfValuesFirstSeenOutOfOrderReadsBack) {
  constexpr uint64_t kRows = 4 * STANDARD_VECTOR_SIZE;
  const Gen descending = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>((7 - g % 8) * 1000 + 3);
  };
  for (const auto& type :
       {duckdb::LogicalType::SMALLINT, duckdb::LogicalType::INTEGER,
        duckdb::LogicalType::UINTEGER, duckdb::LogicalType::BIGINT}) {
    SCOPED_TRACE(duckdb::LogicalType{type}.ToString());
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {}, kRows, kRows, descending,
          irs::codecs::CodecType::Dict);
    {
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      ASSERT_FALSE(col->DataBlocks().empty());
      for (const auto& block : col->DataBlocks()) {
        ASSERT_EQ(block.codec->type, irs::codecs::CodecType::Dict);
      }
    }
    Verify(dir, type, kRows, descending);
  }
}

TEST_F(ColNumericCodecTest, RunsOfFloatsAcrossSeveralLz4FramesReadBack) {
  constexpr uint64_t kRows = DEFAULT_ROW_GROUP_SIZE;
  const Gen pairs = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(g / 2);
  };
  for (const auto& type :
       {duckdb::LogicalType::FLOAT, duckdb::LogicalType::DOUBLE}) {
    SCOPED_TRACE(duckdb::LogicalType{type}.ToString());
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {}, kRows, kRows, pairs, irs::codecs::CodecType::RleLz4);
    {
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      ASSERT_EQ(col->DataBlocks().size(), 1);
      ASSERT_EQ(col->DataBlocks().front().codec->type,
                irs::codecs::CodecType::RleLz4);
    }
    Verify(dir, type, kRows, pairs);
  }
}

TEST_F(ColNumericCodecTest, AWideRunApartFromItsSampleKeepsBothHalves) {
  constexpr uint64_t kBlock = irs::codecs::numeric::kBlockValues;
  constexpr uint64_t kRows = 128 * kBlock;
  const Gen two_runs = [](uint64_t g) -> std::optional<int64_t> {
    return g / kBlock == 3 ? 7 : 5;
  };
  const auto type = duckdb::LogicalType::UUID;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, type, {}, kRows, kRows, two_runs, irs::codecs::CodecType::Rle);
  Verify(dir, type, kRows, two_runs);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->DataBlocks().size(), 1);
  EXPECT_EQ(col->DataBlocks().front().codec->type, irs::codecs::CodecType::Rle);
  EXPECT_EQ(SegmentInfo(dir, "wide"), std::vector<std::string>{"halves"});
}

TEST_F(ColNumericCodecTest, ANamedRleStoresWideValuesOnlyWhenNeighboursRepeat) {
  constexpr uint64_t kRows = 2 * STANDARD_VECTOR_SIZE;
  const Gen distinct = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(g * 3 + 1);
  };
  const Gen repeat_across_vectors = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>((g == STANDARD_VECTOR_SIZE ? g - 1 : g) * 3 +
                                1);
  };
  using irs::codecs::CodecType;
  for (const auto& type :
       {duckdb::LogicalType::HUGEINT, duckdb::LogicalType::UHUGEINT}) {
    for (const auto& [gen, stored] :
         {std::pair{distinct, CodecType::Uncompressed},
          std::pair{repeat_across_vectors, CodecType::Rle}}) {
      SCOPED_TRACE(duckdb::LogicalType{type}.ToString() + " " +
                   std::string{magic_enum::enum_name(stored)});
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, type, {}, kRows, kRows, gen, CodecType::Rle);
      Verify(dir, type, kRows, gen);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      ASSERT_EQ(col->DataBlocks().size(), 1);
      EXPECT_EQ(col->DataBlocks().front().codec->type, stored);
    }
  }
}

TEST_F(ColNumericCodecTest, WideHalvesHoldARunOrAConstantOnlyWhenBothHalvesDo) {
  constexpr uint64_t kRows = 2 * STANDARD_VECTOR_SIZE;
  struct Case {
    std::string wide;
    ValueAt value;
  };
  const Case cases[] = {
    {"halves",
     [](uint64_t g) {
       return duckdb::Value::HUGEINT(
         duckdb::hugeint_t{g < 1000 ? int64_t{1} : int64_t{2}, uint64_t{7}});
     }},
    {"offsets",
     [](uint64_t g) {
       return duckdb::Value::HUGEINT(
         duckdb::hugeint_t{int64_t{1}, g < 1000 ? uint64_t{7} : uint64_t{9}});
     }},
  };
  const auto type = duckdb::LogicalType::HUGEINT;
  using irs::codecs::CodecType;
  for (const auto& [wide, value] : cases) {
    for (const auto codec : {CodecType::Rle, CodecType::Bitpacking}) {
      SCOPED_TRACE(wide + " " + std::string{magic_enum::enum_name(codec)});
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      ASSERT_NO_FATAL_FAILURE(WriteValues(
        Db(), dir, type, irs::WriteTier::Flush, kRows, kRows, codec, value));
      ExpectValues(dir, type, kRows, value);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      ASSERT_EQ(col->DataBlocks().size(), 1);
      EXPECT_EQ(col->DataBlocks().front().codec->type, codec);
      EXPECT_EQ(SegmentInfo(dir, "wide"), std::vector<std::string>{wide});
      irs::ColumnReader::PointReader cursor{r, *col};
      duckdb::Vector out{type, 1};
      for (const uint64_t g : {uint64_t{0}, uint64_t{999}, uint64_t{1000},
                               uint64_t{2048}, kRows - 1}) {
        duckdb::FlatVector::ValidityMutable(out).Reset();
        ASSERT_TRUE(cursor.FetchRow(g, out, 0)) << g;
        EXPECT_EQ(out.GetValue(0).ToString(), value(g).ToString()) << g;
      }
    }
  }
}

TEST_F(ColNumericCodecTest, AFrameCachedAtAnotherSizeIsDecodedAgain) {
  constexpr uint64_t kRows = 60000;
  const Gen clustered = [](uint64_t g) {
    return static_cast<int64_t>(1'700'000'000'000'000LL + (g / 4) * 1000 +
                                Mix(g / 4) % 3 * 100);
  };
  const auto type = duckdb::LogicalType::BIGINT;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, type, {}, kRows, 65536, clustered,
        irs::codecs::CodecType::RleZstd);
  ASSERT_EQ(Transforms(dir), std::vector<std::string>{"rle/zstd"});
  const auto frames = std::stoul(SegmentInfo(dir, "frames").front());
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  auto& cache = Db().GetObjectCache();
  const auto segment = irs::codecs::SegmentCacheKey<irs::codecs::DecodedFrame>(
    irs::tests::ColFileId(r, *col), col->DataBlocks().front().file_offset);
  std::vector<std::string> keys(frames);
  for (uint32_t f = 0; f < frames; ++f) {
    irs::codecs::FrameCacheKey(segment, f, keys[f]);
    cache.Put(keys[f], duckdb::make_shared_ptr<irs::codecs::DecodedFrame>(
                         irs::ScratchVector<uint8_t>(3, 0x5a)));
  }
  const auto scan = [&] {
    auto state = col->InitScan(r.Ctx());
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      const auto take = std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, take);
      out.Flatten(take);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        ExpectRow(out, k, pos + k, clustered, type);
      }
    }
  };
  scan();
  scan();
  size_t decoded = 0;
  for (const auto& key : keys) {
    const auto frame = cache.Get<irs::codecs::DecodedFrame>(key);
    decoded += frame && frame->Bytes().size() != 3;
  }
  EXPECT_GT(decoded, 0u);
}

TEST_F(ColNumericCodecTest, PointReadsOfNeighboursInTwoFramesReadBack) {
  constexpr uint64_t kFrameRows =
    (uint64_t{1} << irs::codecs::kNumericFrameLog2) / sizeof(uint32_t);
  constexpr uint64_t kRows = 4 * kFrameRows;
  const Gen four_bytes = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(
      ((uint64_t{1} << 52) + Mix(g) % (uint64_t{1} << 32)) * 16);
  };
  const auto type = duckdb::LogicalType::DOUBLE;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, type, {}, kRows, kRows, four_bytes,
        irs::codecs::CodecType::Bitpacking);
  Verify(dir, type, kRows, four_bytes);
  ASSERT_EQ(Transforms(dir), std::vector<std::string>{"for/none"});
  ASSERT_EQ(SegmentInfo(dir, "frames"), std::vector<std::string>{"4"});
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  irs::ColumnReader::PointReader cursor{r, *col};
  std::vector<uint32_t> picked;
  for (uint64_t f = 1; f < 4; ++f) {
    for (const uint64_t g :
         {f * kFrameRows - 1, f * kFrameRows + 1, f * kFrameRows + 2}) {
      picked.push_back(static_cast<uint32_t>(g));
    }
  }
  duckdb::Vector batch{type, STANDARD_VECTOR_SIZE};
  cursor.FetchRows(std::span<const uint32_t>{picked}, 0, batch, 0);
  for (size_t k = 0; k < picked.size(); ++k) {
    ExpectRow(batch, k, picked[k], four_bytes, type);
  }
}

TEST_F(ColNumericCodecTest, UnsignedWideValuesReadBackInHalvesAndPlanes) {
  constexpr uint64_t kRows = 2 * STANDARD_VECTOR_SIZE;
  using irs::codecs::CodecType;
  const auto type = duckdb::LogicalType::UHUGEINT;
  const ValueAt halves = [](uint64_t g) {
    return duckdb::Value::UHUGEINT(
      duckdb::uhugeint_t{1 + g / 1000 % 2, uint64_t{g * 3}});
  };
  const ValueAt planes = [](uint64_t g) {
    return duckdb::Value::UHUGEINT(
      duckdb::uhugeint_t{0, (g / 64 % 200) << 56 | (Mix(g) & uint64_t{0xFF})});
  };
  struct Case {
    CodecType codec;
    const ValueAt& value;
    std::string_view wide;
    bool shuffled;
  };
  for (const auto& [codec, value, wide, shuffled] :
       {Case{CodecType::Bitpacking, halves, "halves", false},
        Case{CodecType::Delta, halves, "halves", false},
        Case{CodecType::Lz4, planes, "offsets", true},
        Case{CodecType::Zstd, planes, "offsets", true}}) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    ASSERT_NO_FATAL_FAILURE(WriteValues(Db(), dir, type, irs::WriteTier::Flush,
                                        kRows, kRows, codec, value));
    ExpectValues(dir, type, kRows, value);
    EXPECT_EQ(SegmentInfo(dir, "wide"),
              std::vector<std::string>{std::string{wide}});
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    ASSERT_EQ(col->DataBlocks().size(), 1);
    ASSERT_EQ(col->DataBlocks().front().codec->type, codec);
    irs::ReadContext ctx{r};
    const auto segment = col->OpenSegment(0, ctx);
    const auto handle = segment->Pin();
    constexpr auto kHeader = sizeof(irs::codecs::numeric::WideHeader);
    const auto low = irs::codecs::ParseFrames(
      handle.Ptr() + kHeader, segment->size - kHeader, kRows, codec);
    EXPECT_EQ((low.flags & irs::codecs::kNumericShuffled) != 0, shuffled);
    irs::ColumnReader::PointReader cursor{r, *col};
    duckdb::Vector out{type, 1};
    for (const uint64_t g :
         {uint64_t{0}, uint64_t{999}, uint64_t{1000}, kRows - 1}) {
      duckdb::FlatVector::ValidityMutable(out).Reset();
      ASSERT_TRUE(cursor.FetchRow(g, out, 0)) << g;
      EXPECT_EQ(out.GetValue(0).ToString(), value(g).ToString()) << g;
    }
  }
}

TEST_F(ColNumericCodecTest, RunFiltersOverShortVectorTailsMatchTheValues) {
  duckdb::Connection con{_db};
  using Cmp = duckdb::ExpressionType;
  using irs::codecs::CodecType;
  const auto type = duckdb::LogicalType::BIGINT;
  const Gen runs = [](uint64_t g) -> std::optional<int64_t> {
    return static_cast<int64_t>(Mix(g / 9) % 7) - 3;
  };
  for (const auto codec : {CodecType::Rle, CodecType::RleZstd}) {
    for (const uint32_t tail : {uint32_t{3}, uint32_t{20}}) {
      SCOPED_TRACE(std::string{magic_enum::enum_name(codec)} + " " +
                   std::to_string(tail));
      const uint32_t rg_size = STANDARD_VECTOR_SIZE + tail;
      const uint64_t rows = 3 * uint64_t{rg_size};
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, type, {}, rows, rg_size, runs, codec);
      Verify(dir, type, rows, runs);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      for (const auto& block : col->DataBlocks()) {
        ASSERT_EQ(block.codec->type, codec);
      }
      for (const int64_t key : {int64_t{-3}, int64_t{0}, int64_t{2}}) {
        for (const auto cmp : {Cmp::COMPARE_EQUAL, Cmp::COMPARE_NOTEQUAL,
                               Cmp::COMPARE_GREATERTHANOREQUALTO}) {
          const duckdb::ExpressionFilter filter{
            duckdb::BoundComparisonExpression::Create(
              cmp, duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0),
              duckdb::make_uniq<duckdb::BoundConstantExpression>(
                duckdb::Value::BIGINT(key)))};
          auto filter_state =
            duckdb::TableFilterState::Initialize(*con.context, filter);
          auto state = col->InitScan(r.Ctx());
          for (uint64_t anchor = 0; anchor < rows;
               anchor += STANDARD_VECTOR_SIZE) {
            const auto span =
              std::min<uint64_t>(rows - anchor, STANDARD_VECTOR_SIZE);
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
              const auto v = *runs(anchor + i);
              const bool expected = cmp == Cmp::COMPARE_EQUAL      ? v == key
                                    : cmp == Cmp::COMPARE_NOTEQUAL ? v != key
                                                                   : v >= key;
              const bool got = next < kept && sel.get_index(next) == i;
              next += got ? 1 : 0;
              ASSERT_EQ(got, expected)
                << "row " << anchor + i << " key " << key << " cmp "
                << duckdb::EnumUtil::ToString(cmp);
              if (got) {
                ASSERT_EQ(out.GetValue(i), duckdb::Value::BIGINT(v));
              }
            }
            ASSERT_EQ(next, kept);
          }
        }
      }
    }
  }
}

TEST_F(ColNumericCodecTest,
       DeltaBlocksOfEveryIntegerWidthAnswerScansAndFilters) {
  constexpr uint64_t kRows = 4 * STANDARD_VECTOR_SIZE;
  duckdb::Connection con{_db};
  using Cmp = duckdb::ExpressionType;
  const Gen constant_then_saw = [](uint64_t g) -> std::optional<int64_t> {
    return g < STANDARD_VECTOR_SIZE ? int64_t{5}
                                    : static_cast<int64_t>(5 + g % 64);
  };
  for (const auto& type :
       {duckdb::LogicalType::TINYINT, duckdb::LogicalType::SMALLINT,
        duckdb::LogicalType::INTEGER, duckdb::LogicalType::BIGINT,
        duckdb::LogicalType::UTINYINT, duckdb::LogicalType::USMALLINT,
        duckdb::LogicalType::UINTEGER, duckdb::LogicalType::UBIGINT}) {
    SCOPED_TRACE(duckdb::LogicalType{type}.ToString());
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, type, {}, kRows, kRows, constant_then_saw,
          irs::codecs::CodecType::Delta);
    Verify(dir, type, kRows, constant_then_saw);
    ASSERT_EQ(Transforms(dir),
              std::vector<std::string>{"delta_bitpacking/none"});
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    for (const int64_t key : {int64_t{5}, int64_t{30}}) {
      for (const auto cmp : {Cmp::COMPARE_EQUAL, Cmp::COMPARE_LESSTHAN,
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
          duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
          for (duckdb::idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
            sel.set_index(i, i);
          }
          duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
          const auto kept = col->GatherFilter(
            state, anchor, STANDARD_VECTOR_SIZE, sel, STANDARD_VECTOR_SIZE,
            filter, *filter_state, irs::NullCheckKind::None, out);
          duckdb::idx_t next = 0;
          for (duckdb::idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
            const auto v = *constant_then_saw(anchor + i);
            const bool expected = cmp == Cmp::COMPARE_EQUAL      ? v == key
                                  : cmp == Cmp::COMPARE_LESSTHAN ? v < key
                                                                 : v >= key;
            const bool got = next < kept && sel.get_index(next) == i;
            next += got ? 1 : 0;
            ASSERT_EQ(got, expected)
              << "row " << anchor + i << " key " << key << " cmp "
              << duckdb::EnumUtil::ToString(cmp);
            if (got) {
              ASSERT_EQ(out.GetValue(i), Cast(v, type));
            }
          }
          ASSERT_EQ(next, kept);
        }
      }
    }
  }
}

TEST_F(ColNumericCodecTest, AConstantBlockBesideAnArithmeticBlockReadsBack) {
  constexpr uint64_t kBlock = irs::codecs::numeric::kBlockValues;
  constexpr uint64_t kRows = 16 * kBlock;
  const Gen constant_then_steps = [](uint64_t g) -> std::optional<int64_t> {
    if (g >= 4 * kBlock && g < 7 * kBlock) {
      return 5;
    }
    if (g >= 7 * kBlock && g < 9 * kBlock) {
      return static_cast<int64_t>(5 + (g - 7 * kBlock) * 3);
    }
    return static_cast<int64_t>(Mix(g) % 1000);
  };
  const auto type = duckdb::LogicalType::BIGINT;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, type, {}, kRows, kRows, constant_then_steps,
        irs::codecs::CodecType::Bitpacking);
  Verify(dir, type, kRows, constant_then_steps);
  EXPECT_EQ(Transforms(dir), std::vector<std::string>{"bitpacking/none"});
}

TEST_F(ColNumericCodecTest,
       NamedWideCodecsStoreListsWhoseRowGroupsHaveNoElements) {
  constexpr uint32_t kRowGroup = STANDARD_VECTOR_SIZE;
  constexpr uint64_t kRows = 3 * kRowGroup;
  for (const auto id :
       {duckdb::LogicalTypeId::HUGEINT, duckdb::LogicalTypeId::UHUGEINT}) {
    const duckdb::LogicalType element{id};
    const auto type = duckdb::LogicalType::LIST(element);
    const ValueAt row = [&](uint64_t g) {
      duckdb::vector<duckdb::Value> items;
      if (g / kRowGroup == 1) {
        for (uint64_t j = 0; j <= g % 3; ++j) {
          items.push_back(
            Cast(static_cast<int64_t>(1000 + g / 256 % 4), element));
        }
      } else if (g / kRowGroup == 2 && g % 2 == 0) {
        return duckdb::Value{type};
      }
      return duckdb::Value::LIST(element, items);
    };
    for (const auto codec :
         {irs::codecs::CodecType::Bitpacking, irs::codecs::CodecType::Delta,
          irs::codecs::CodecType::Rle, irs::codecs::CodecType::Dict,
          irs::codecs::CodecType::Lz4}) {
      SCOPED_TRACE(element.ToString() + " " +
                   std::string{magic_enum::enum_name(codec)});
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      ASSERT_NO_FATAL_FAILURE(WriteValues(
        Db(), dir, type, irs::WriteTier::Flush, kRows, kRowGroup, codec, row));
      ExpectValues(dir, type, kRows, row);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      const auto* items = col->Child();
      ASSERT_NE(items, nullptr);
      ASSERT_FALSE(items->DataBlocks().empty());
      for (const auto& block : items->DataBlocks()) {
        EXPECT_EQ(block.codec->type, codec);
      }
    }
  }
}

TEST_F(ColNumericCodecTest,
       ListElementsWithANullInAShortChunkAcrossABlockReadBack) {
  constexpr uint64_t kFull = STANDARD_VECTOR_SIZE;
  constexpr uint64_t kRows = kFull + 15;
  const duckdb::LogicalType element{duckdb::LogicalTypeId::INTEGER};
  const auto type = duckdb::LogicalType::LIST(element);
  const ValueAt row = [&](uint64_t g) {
    duckdb::vector<duckdb::Value> items;
    const uint64_t count = g >= kFull || g < 1000 ? 2 : 1;
    for (uint64_t j = 0; j < count; ++j) {
      items.push_back(g == kFull + 13 && j == 1
                        ? duckdb::Value{element}
                        : duckdb::Value::INTEGER(static_cast<int32_t>(
                            g * 2 + j + Mix(g) % 2 * 100000)));
    }
    return duckdb::Value::LIST(element, items);
  };
  for (const auto codec :
       {irs::codecs::CodecType::Bitpacking, irs::codecs::CodecType::Auto}) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    ASSERT_NO_FATAL_FAILURE(WriteValues(Db(), dir, type, irs::WriteTier::Flush,
                                        kRows, kRows, codec, row,
                                        irs::DistinctStats::Exact));
    ExpectValues(dir, type, kRows, row);
  }
}
