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
#include <absl/container/flat_hash_set.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <duckdb.hpp>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/vector/dictionary_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/immutable_strings.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/object_cache.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iresearch/formats/column/codecs/compression_function.hpp>
#include <iresearch/formats/column/codecs/dictionary_cache.hpp>
#include <iresearch/formats/column/codecs/fsst.hpp>
#include <iresearch/formats/column/codecs/onpair.hpp>
#include <iresearch/formats/column/codecs/registry.hpp>
#include <iresearch/formats/column/codecs/string_layout.hpp>
#include <iresearch/formats/column/codecs/string_scan.hpp>
#include <iresearch/formats/column/codecs/string_seal.hpp>
#include <iresearch/formats/column/codecs/zstd.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/immutable_payloads.hpp>
#include <iresearch/formats/column/internal/gather_arms.hpp>
#include <iresearch/formats/column/internal/write_context.hpp>
#include <iresearch/formats/column/list_ingest.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "gtest/gtest.h"
#include "tests_shared.hpp"

namespace {

using Value = std::function<std::optional<std::string>(uint64_t)>;

constexpr irs::field_id kField = 7;
constexpr std::string_view kSeg = "seg";

struct Rows2 {
  const uint64_t* p;
  size_t n;
  size_t size() const noexcept { return n; }
  uint64_t operator[](size_t i) const noexcept { return p[i]; }
};

duckdb::CompressionType Written(duckdb::CompressionType codec) {
  if (const auto choice = irs::codecs::ChoiceOf(codec)) {
    return irs::codecs::TypeOf(*choice);
  }
  return codec;
}

class ColCodecsTest : public TestBase {
 protected:
  duckdb::DatabaseInstance& Db() { return *_db.instance; }

  std::vector<std::string> CacheKeys(irs::ColReader& reader,
                                     const irs::ColumnReader& col) {
    const auto file_id = irs::tests::ColFileId(reader, col);
    std::vector<std::string> keys;
    for (const auto& block : col.DataBlocks()) {
      keys.push_back(
        irs::codecs::DictionaryCacheKey(file_id, block.file_offset));
    }
    return keys;
  }

  void Write(irs::Directory& dir, duckdb::CompressionType codec,
             irs::ColCodecParams params, uint64_t rows, uint32_t rg_size,
             const Value& value) {
    irs::ColWriter w{dir, kSeg, Db(), params.tier};
    auto& cw = w.OpenColumn(kField, duckdb::LogicalType::VARCHAR,
                            {.row_group_size = rg_size,
                             .compression = codec,
                             .codec_params = params});
    AppendValues(cw, rows, value);
    ASSERT_TRUE(w.Commit(0));
  }

  static void AppendValues(irs::ColumnWriter& cw, uint64_t rows,
                           const Value& value) {
    uint64_t pos = 0;
    while (pos < rows) {
      const auto take = std::min<uint64_t>(rows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector vec{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      auto* d = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec);
      auto& validity = duckdb::FlatVector::ValidityMutable(vec);
      validity.Reset(STANDARD_VECTOR_SIZE);
      for (uint64_t k = 0; k < take; ++k) {
        if (const auto v = value(pos + k)) {
          d[k] = duckdb::StringVector::AddString(vec, *v);
        } else {
          validity.SetInvalid(k);
        }
      }
      duckdb::FlatVector::SetSize(vec, take);
      cw.Append(vec, take);
      pos += take;
    }
  }

  static void ExpectRow(duckdb::Vector& out, duckdb::idx_t k, uint64_t g,
                        const Value& value) {
    const auto expected = value(g);
    const auto& validity = duckdb::FlatVector::Validity(out);
    if (!expected) {
      EXPECT_FALSE(validity.RowIsValid(k)) << "row " << g;
      return;
    }
    ASSERT_TRUE(validity.RowIsValid(k)) << "row " << g;
    const auto& s = duckdb::FlatVector::GetData<duckdb::string_t>(out)[k];
    EXPECT_EQ(s.GetString(), *expected) << "row " << g;
  }

  void Verify(irs::Directory& dir, duckdb::CompressionType codec, uint64_t rows,
              const Value& value) {
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    ASSERT_EQ(col->RowCount(), rows);
    if (codec != duckdb::CompressionType::COMPRESSION_AUTO) {
      for (const auto& block : col->DataBlocks()) {
        EXPECT_EQ(block.codec->type, Written(codec));
      }
    }

    {
      auto state = col->InitScan(r.Ctx());
      uint64_t pos = 0;
      while (pos < rows) {
        const auto take = std::min<uint64_t>(rows - pos, STANDARD_VECTOR_SIZE);
        duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
        col->Scan(state, out, take);
        out.Flatten(take);
        for (duckdb::idx_t k = 0; k < take; ++k) {
          ExpectRow(out, k, pos + k, value);
        }
        pos += take;
      }
    }
    {
      auto state = col->InitScan(r.Ctx());
      constexpr uint64_t kSkip = 5;
      if (rows > kSkip) {
        col->Skip(state, kSkip);
      }
      uint64_t pos = rows > kSkip ? kSkip : 0;
      while (pos < rows) {
        const auto take = std::min<uint64_t>(rows - pos, 700);
        duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
        duckdb::FlatVector::ValidityMutable(out).Reset(STANDARD_VECTOR_SIZE);
        col->ScanCount(state, out, take, 0);
        for (duckdb::idx_t k = 0; k < take; ++k) {
          ExpectRow(out, k, pos + k, value);
        }
        pos += take;
      }
    }
    {
      std::vector<uint64_t> picked;
      for (uint64_t g = 3; g < rows; g += 41) {
        picked.push_back(g);
      }
      auto state = col->InitScan(r.Ctx());
      size_t i = 0;
      while (i < picked.size()) {
        const auto take =
          std::min<size_t>(picked.size() - i, STANDARD_VECTOR_SIZE);
        duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
        duckdb::FlatVector::ValidityMutable(out).Reset(STANDARD_VECTOR_SIZE);
        const Rows2 sub{&picked[i], take};
        irs::column_internal::GatherRows(*col, state, sub, out, 0,
                                         /*whole_output=*/true);
        out.Flatten(take);
        for (size_t k = 0; k < take; ++k) {
          ExpectRow(out, k, picked[i + k], value);
        }
        i += take;
      }
    }
    {
      irs::ColumnReader::PointReader cursor{r, *col};
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, 1};
      std::vector<uint64_t> points;
      for (uint64_t g = 0; g < rows; g += 97) {
        points.push_back(g);
      }
      if (rows > 0) {
        points.push_back(rows - 1);
        points.push_back(0);
      }
      for (const auto g : points) {
        duckdb::FlatVector::ValidityMutable(out).Reset();
        const bool valid = cursor.FetchRow(g, out, 0);
        EXPECT_EQ(valid, value(g).has_value()) << "row " << g;
        ExpectRow(out, 0, g, value);
      }
    }
    {
      irs::ColumnReader::PointReader cursor{r, *col};
      constexpr duckdb::idx_t kOffset = 5;
      for (const uint64_t step : {uint64_t{1}, uint64_t{7}, uint64_t{97}}) {
        std::vector<uint64_t> batch;
        for (uint64_t g = step / 2; g < rows + 3; g += step) {
          batch.push_back(g);
          if (batch.size() + kOffset < STANDARD_VECTOR_SIZE &&
              g + step < rows) {
            continue;
          }
          duckdb::Vector out{duckdb::LogicalType::VARCHAR,
                             STANDARD_VECTOR_SIZE};
          cursor.FetchRows(batch, out, kOffset);
          for (size_t k = 0; k < batch.size(); ++k) {
            if (batch[k] >= rows) {
              EXPECT_FALSE(
                duckdb::FlatVector::Validity(out).RowIsValid(kOffset + k));
              continue;
            }
            ExpectRow(out, kOffset + k, batch[k], value);
          }
          batch.clear();
        }
      }
    }
  }

  void RoundTrip(duckdb::CompressionType codec, irs::ColCodecParams params,
                 uint64_t rows, uint32_t rg_size, const Value& value) {
    irs::MemoryDirectory dir{};
    Write(dir, codec, params, rows, rg_size, value);
    Verify(dir, codec, rows, value);
  }

  uint64_t ColumnBytes(irs::Directory& dir) {
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    uint64_t bytes = 0;
    for (const auto& block : col->DataBlocks()) {
      bytes += block.byte_size;
    }
    if (const auto* validity = col->Validity()) {
      for (const auto& block : validity->DataBlocks()) {
        bytes += block.byte_size;
      }
    }
    return bytes;
  }

  std::vector<std::pair<duckdb::CompressionType, std::string>> BlockLevels(
    irs::Directory& dir) {
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    std::vector<std::pair<duckdb::CompressionType, std::string>> levels;
    irs::ReadContext ctx{r};
    irs::BlockWindow window{};
    uint64_t row = 0;
    for (const auto& block : col->DataBlocks()) {
      std::string level;
      if (duckdb::IsSereneDBCompressionType(block.codec->type)) {
        window = col->Locate(row, window);
        auto seg = col->OpenSegment(window.block, ctx);
        auto info = seg->function.get_segment_info(*seg);
        level = info["level"];
      }
      levels.emplace_back(block.codec->type, std::move(level));
      row += block.tuple_count;
    }
    return levels;
  }

  std::vector<std::string> SegmentInfo(irs::Directory& dir,
                                       const std::string& key) {
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    std::vector<std::string> values;
    irs::ReadContext ctx{r};
    irs::BlockWindow window{};
    uint64_t row = 0;
    for (const auto& block : col->DataBlocks()) {
      window = col->Locate(row, window);
      auto seg = col->OpenSegment(window.block, ctx);
      auto info = seg->function.get_segment_info(*seg);
      values.emplace_back(info[key]);
      row += block.tuple_count;
    }
    return values;
  }

  duckdb::DuckDB _db;
};

std::string Pseudo(uint64_t g, size_t len) {
  std::mt19937_64 rng{g * 0x9E3779B97F4A7C15ULL + 1};
  std::string s;
  s.reserve(len);
  static constexpr char kAlphabet[] =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 -_/";
  for (size_t i = 0; i < len; ++i) {
    s.push_back(kAlphabet[rng() % (sizeof(kAlphabet) - 1)]);
  }
  return s;
}

const Value kLowCardinalityWithNulls =
  [](uint64_t g) -> std::optional<std::string> {
  if (g % 9 == 0) {
    return std::nullopt;
  }
  return "value-" + std::to_string(g % 37);
};

const Value kUniqueShort = [](uint64_t g) -> std::optional<std::string> {
  return "row-" + std::to_string(g) + "-" + std::to_string(g * 7919 % 1000003);
};

const Value kLongTextWithNulls = [](uint64_t g) -> std::optional<std::string> {
  if (g % 13 == 0) {
    return std::nullopt;
  }
  return Pseudo(g, 1400 + g % 300);
};

const Value kAllNull = [](uint64_t) -> std::optional<std::string> {
  return std::nullopt;
};

const Value kEmptyAndShort = [](uint64_t g) -> std::optional<std::string> {
  if (g % 3 == 0) {
    return std::string{};
  }
  return std::string(1, static_cast<char>('a' + g % 26));
};

const Value kOneHugeString = [](uint64_t g) -> std::optional<std::string> {
  if (g == 100) {
    return Pseudo(g, 3 * 1024 * 1024 + 17);
  }
  return "row-" + std::to_string(g % 500);
};

struct LevelArm {
  duckdb::CompressionType codec;
  std::vector<uint8_t> levels;
};

const std::vector<LevelArm>& EveryCodec() {
  static const std::vector<LevelArm> arms = {
    {duckdb::CompressionType::COMPRESSION_DICT_LZ4, {0, 1, 12}},
    {duckdb::CompressionType::COMPRESSION_LZ4, {0, 12}},
    {duckdb::CompressionType::COMPRESSION_DICT_ZSTD, {0, 1, 22}},
    {duckdb::CompressionType::COMPRESSION_DICT_ZXC, {0, 1, 7}},
    {duckdb::CompressionType::COMPRESSION_ZXC, {0, 7}},
    {duckdb::CompressionType::COMPRESSION_DICT_FSST, {0}},
    {duckdb::CompressionType::COMPRESSION_FSST, {0}},
    {duckdb::CompressionType::COMPRESSION_COL_DICT_FSST_PLUS, {0}},
    {duckdb::CompressionType::COMPRESSION_COL_FSST_PLUS, {0}},
    {duckdb::CompressionType::COMPRESSION_COL_DICT_ONPAIR, {0}},
    {duckdb::CompressionType::COMPRESSION_COL_ONPAIR, {0}},
    {duckdb::CompressionType::COMPRESSION_COL_DICT_FSST12, {0}},
    {duckdb::CompressionType::COMPRESSION_COL_FSST12, {0}},
    {duckdb::CompressionType::COMPRESSION_UNCOMPRESSED, {0}},
    {duckdb::CompressionType::COMPRESSION_ZSTD, {0, 1, 22}},
    {duckdb::CompressionType::COMPRESSION_AUTO, {0}},
  };
  return arms;
}

}  // namespace

TEST_F(ColCodecsTest, DictLz4LowCardinality) {
  RoundTrip(duckdb::CompressionType::COMPRESSION_DICT_LZ4, {}, 10000, 4096,
            kLowCardinalityWithNulls);
}

TEST_F(ColCodecsTest, DictZstdLowCardinalityLevel5) {
  RoundTrip(duckdb::CompressionType::COMPRESSION_DICT_ZSTD,
            {.compression_level = 5}, 10000, 4096, kLowCardinalityWithNulls);
}

TEST_F(ColCodecsTest, DictLz4UniqueValues) {
  RoundTrip(duckdb::CompressionType::COMPRESSION_DICT_LZ4, {}, 30000, 8192,
            kUniqueShort);
}

TEST_F(ColCodecsTest, PlainLz4LongText) {
  RoundTrip(duckdb::CompressionType::COMPRESSION_LZ4, {}, 3000, 2048,
            kLongTextWithNulls);
}

TEST_F(ColCodecsTest, DictFsstSortedKeys) {
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 17 == 0) {
      return std::nullopt;
    }
    char buf[64];
    std::snprintf(buf, sizeof buf, "https://example.org/users/%06llu/profile",
                  static_cast<unsigned long long>((g * 7919) % 1500));
    return std::string{buf};
  };
  RoundTrip(duckdb::CompressionType::COMPRESSION_DICT_FSST, {}, 30000, 8192,
            value);
  RoundTrip(duckdb::CompressionType::COMPRESSION_COL_DICT_FSST_PLUS, {}, 30000,
            8192, value);
  RoundTrip(duckdb::CompressionType::COMPRESSION_COL_FSST_PLUS, {}, 30000, 8192,
            value);
  RoundTrip(duckdb::CompressionType::COMPRESSION_COL_DICT_ONPAIR, {}, 30000,
            8192, value);
  RoundTrip(duckdb::CompressionType::COMPRESSION_COL_ONPAIR, {}, 30000, 8192,
            value);
  RoundTrip(duckdb::CompressionType::COMPRESSION_COL_DICT_FSST12, {}, 30000,
            8192, value);
  RoundTrip(duckdb::CompressionType::COMPRESSION_COL_FSST12, {}, 30000, 8192,
            value);
}

TEST_F(ColCodecsTest, PlainFsstRowOrder) {
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 11 == 0) {
      return std::nullopt;
    }
    char buf[32];
    std::snprintf(buf, sizeof buf, "key-%08llu",
                  static_cast<unsigned long long>(g));
    return std::string{buf};
  };
  RoundTrip(duckdb::CompressionType::COMPRESSION_FSST, {}, 30000, 8192, value);
  RoundTrip(duckdb::CompressionType::COMPRESSION_FSST, {}, 3000, 2048,
            kLongTextWithNulls);
}

TEST(FsstEncoderTest, BatchesEncodeLikeSingleStrings) {
  constexpr std::array<std::string_view, 8> kWords{
    "https://", "example.org/", "users/", "profile?",
    "id=",      "&lang=en",     "search", "/page/"};
  constexpr std::array<size_t, 13> kEdges{0,   1,   7,   8,   12,  13,  64,
                                          510, 511, 512, 513, 900, 4000};
  std::mt19937_64 rng{11};
  std::vector<std::string> strings;
  for (size_t i = 0; i < 6000; ++i) {
    const size_t length = i < 260 ? kEdges[i % kEdges.size()] : rng() % 900;
    std::string s;
    while (s.size() < length) {
      if (rng() % 4 == 0) {
        s.push_back(static_cast<char>(rng() % 256));
      } else {
        s.append(kWords[rng() % kWords.size()]);
      }
    }
    s.resize(length);
    strings.emplace_back(std::move(s));
  }
  std::vector<duckdb::string_t> values;
  for (const auto& s : strings) {
    values.emplace_back(s.data(), static_cast<uint32_t>(s.size()));
  }
  const std::vector<uint32_t> skip(values.size(), 0);

  size_t total = 0;
  for (const auto& s : strings) {
    total += s.size();
  }

  irs::codecs::FsstEncoder encoder;
  std::string batch(2 * total + 8, '\0');
  std::vector<uint32_t> lengths(values.size());
  const auto all =
    encoder.Encode(values, skip, batch.data(), batch.size(), lengths);
  ASSERT_EQ(values.size(), all.strings);
  batch.resize(all.bytes);

  irs::codecs::FsstDecoder decoder;
  ASSERT_TRUE(decoder.Import(encoder.SymbolTable()));
  std::string single;
  uint32_t single_length = 0;
  std::string decoded;
  size_t offset = 0;
  for (size_t i = 0; i < values.size(); ++i) {
    const std::string_view encoded{batch.data() + offset, lengths[i]};
    offset += lengths[i];
    single.resize(2 * strings[i].size() + 8);
    const auto one = encoder.Encode(
      std::span{&values[i], 1}, std::span{&skip[i], 1}, single.data(),
      single.size(), std::span{&single_length, 1});
    ASSERT_EQ(1u, one.strings);
    ASSERT_EQ(encoded, std::string_view(single.data(), one.bytes))
      << "string " << i << ", " << strings[i].size() << " bytes";
    decoded.resize(strings[i].size() + 8);
    const auto size = decoder.Decode(encoded.data(), encoded.size(),
                                     decoded.data(), decoded.size());
    ASSERT_EQ(strings[i], std::string_view(decoded.data(), size));
  }
  EXPECT_EQ(batch.size(), offset);
}

TEST(FsstEncoderTest, ResumesWhereTheOutputRanOut) {
  std::mt19937_64 rng{5};
  std::vector<std::string> strings;
  size_t total = 0;
  for (size_t i = 0; i < 2000; ++i) {
    std::string s(8 + rng() % 64, '\0');
    for (auto& c : s) {
      c = static_cast<char>(rng() % 256);
    }
    total += s.size();
    strings.emplace_back(std::move(s));
  }
  std::vector<duckdb::string_t> values;
  for (const auto& s : strings) {
    values.emplace_back(s.data(), static_cast<uint32_t>(s.size()));
  }
  const std::vector<uint32_t> skip(values.size(), 1);

  irs::codecs::FsstEncoder whole;
  std::string expected(2 * total + 8, '\0');
  std::vector<uint32_t> expected_lengths(values.size());
  const auto all = whole.Encode(values, skip, expected.data(), expected.size(),
                                expected_lengths);
  ASSERT_EQ(values.size(), all.strings);
  expected.resize(all.bytes);

  irs::codecs::FsstEncoder resumed;
  std::string out(total / 2, '\0');
  std::vector<uint32_t> lengths(values.size());
  const auto first =
    resumed.Encode(values, skip, out.data(), out.size(), lengths);
  ASSERT_LT(first.strings, values.size());
  out.resize(first.bytes);
  std::string rest(2 * total + 8, '\0');
  const auto second =
    resumed.Encode(std::span{values}.subspan(first.strings),
                   std::span{skip}.subspan(first.strings), rest.data(),
                   rest.size(), std::span{lengths}.subspan(first.strings));
  ASSERT_EQ(values.size(), first.strings + second.strings);
  out.append(rest.data(), second.bytes);
  EXPECT_EQ(expected, out);
  EXPECT_EQ(expected_lengths, lengths);
}

TEST(PackedReaderTest, ReadsEveryWidth) {
  constexpr size_t kValues = 5 * irs::codecs::kGroup;
  std::mt19937_64 rng{7};
  for (uint8_t width = 0; width <= 32; ++width) {
    const uint64_t mask = (uint64_t{1} << width) - 1;
    std::vector<uint32_t> values(kValues);
    for (auto& v : values) {
      v = static_cast<uint32_t>(rng() & mask);
    }
    if (width != 0) {
      values[3] = static_cast<uint32_t>(mask);
    }
    std::vector<duckdb::data_t> packed(
      duckdb::BitpackingPrimitives::GetRequiredSize(kValues, width));
    duckdb::BitpackingPrimitives::PackBuffer<uint32_t, true>(
      packed.data(), values.data(), kValues, width);
    irs::codecs::text::PackedReader reader{packed.data(), width};
    for (size_t i = 0; i < kValues; ++i) {
      ASSERT_EQ(values[i], reader.At(i))
        << "width " << int{width} << ", value " << i;
    }
  }
}

TEST(ZstdTest, ShortPeriodRepeatsRoundTrip) {
  constexpr std::array<size_t, 7> kRepeatBytes{9, 15, 16, 17, 31, 33, 4000};
  constexpr std::array<uint8_t, 4> kLevels{1, 3, 9, 19};
  std::mt19937_64 rng{3};
  std::string raw;
  for (size_t period = 1; period < 16; ++period) {
    for (const size_t repeat : kRepeatBytes) {
      std::string pattern(period, '\0');
      for (auto& c : pattern) {
        c = static_cast<char>(rng());
      }
      for (size_t i = 0; i < period + repeat; ++i) {
        raw.push_back(pattern[i % period]);
      }
    }
  }
  for (const uint8_t level : kLevels) {
    irs::codecs::Zstd::Compressor compressor{level};
    std::string compressed(irs::codecs::Zstd::Bound(raw.size()), '\0');
    compressed.resize(compressor.Compress(
      raw.data(), raw.size(), compressed.data(), compressed.size()));
    irs::codecs::Zstd::Decompressor decompressor;
    std::string decoded(raw.size(), '\0');
    ASSERT_TRUE(decompressor.Decompress(compressed.data(), compressed.size(),
                                        decoded.data(), decoded.size()))
      << "level " << int{level};
    EXPECT_EQ(raw, decoded) << "level " << int{level};
  }
}

TEST_F(ColCodecsTest, RunLengthCodes) {
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g / 500 % 9 == 4) {
      return std::nullopt;
    }
    return "bucket-" + std::to_string(g / 500);
  };
  RoundTrip(duckdb::CompressionType::COMPRESSION_DICT_LZ4, {}, 40000, 16384,
            value);
  RoundTrip(duckdb::CompressionType::COMPRESSION_DICT_FSST, {}, 40000, 16384,
            value);
}

TEST_F(ColCodecsTest, PlainSparseGatherThenScan) {
  for (const auto codec : {duckdb::CompressionType::COMPRESSION_LZ4,
                           duckdb::CompressionType::COMPRESSION_FSST,
                           duckdb::CompressionType::COMPRESSION_COL_FSST_PLUS,
                           duckdb::CompressionType::COMPRESSION_COL_ONPAIR,
                           duckdb::CompressionType::COMPRESSION_COL_FSST12,
                           duckdb::CompressionType::COMPRESSION_ZXC}) {
    irs::MemoryDirectory dir{};
    Write(dir, codec, {.segment_target = 64 * 1024}, 6000, 2048,
          kLongTextWithNulls);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    auto state = col->InitScan(r.Ctx());
    const uint64_t picked[] = {10, 2500, 5990};
    duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    duckdb::FlatVector::ValidityMutable(out).Reset(STANDARD_VECTOR_SIZE);
    irs::column_internal::GatherRows(*col, state, Rows2{picked, 3}, out, 0,
                                     /*whole_output=*/true);
    out.Flatten(3);
    for (size_t k = 0; k < 3; ++k) {
      ExpectRow(out, k, picked[k], kLongTextWithNulls);
    }
    duckdb::Vector rest{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    duckdb::FlatVector::ValidityMutable(rest).Reset(STANDARD_VECTOR_SIZE);
    col->ScanCount(state, rest, 9, 0);
    for (duckdb::idx_t k = 0; k < 9; ++k) {
      ExpectRow(rest, k, 5991 + k, kLongTextWithNulls);
    }
    Verify(dir, codec, 6000, kLongTextWithNulls);
  }
}

TEST_F(ColCodecsTest, DictZxcLevel5) {
  RoundTrip(duckdb::CompressionType::COMPRESSION_DICT_ZXC,
            {.compression_level = 5}, 30000, 8192, kUniqueShort);
}

TEST_F(ColCodecsTest, PlainZxcLongText) {
  RoundTrip(duckdb::CompressionType::COMPRESSION_ZXC, {}, 3000, 2048,
            kLongTextWithNulls);
}

TEST_F(ColCodecsTest, EveryCodecEveryShapeEveryLevel) {
  const std::pair<const char*, const Value*> datasets[] = {
    {"low-cardinality with nulls", &kLowCardinalityWithNulls},
    {"unique short", &kUniqueShort},
    {"long text with nulls", &kLongTextWithNulls},
    {"all null", &kAllNull},
    {"empty and short", &kEmptyAndShort},
    {"one huge string", &kOneHugeString},
  };
  for (const auto& arm : EveryCodec()) {
    for (const auto level : arm.levels) {
      for (const auto& [name, value] : datasets) {
        for (const uint32_t target : {uint32_t{16 * 1024}, uint32_t{0}}) {
          if (target != 0 && level != 0) {
            continue;
          }
          SCOPED_TRACE(std::string{duckdb::CompressionTypeToString(arm.codec)} +
                       " level " + std::to_string(level) + " on " + name +
                       " target " + std::to_string(target));
          irs::ColCodecParams params{.compression_level = level};
          if (target != 0) {
            params.segment_target = target;
          }
          const uint64_t rows = value == &kOneHugeString ? 400 : 2500;
          irs::MemoryDirectory dir{};
          Write(dir, arm.codec, params, rows, 1024, *value);
          irs::ColReader r{dir, std::string{kSeg}, Db(),
                           irs::IOAdvice::SEQUENTIAL};
          const auto* col = r.Column(kField);
          ASSERT_NE(col, nullptr);
          ASSERT_EQ(col->RowCount(), rows);
          auto state = col->InitScan(r.Ctx());
          uint64_t pos = 0;
          while (pos < rows) {
            const auto take =
              std::min<uint64_t>(rows - pos, STANDARD_VECTOR_SIZE);
            duckdb::Vector out{duckdb::LogicalType::VARCHAR,
                               STANDARD_VECTOR_SIZE};
            col->Scan(state, out, take);
            out.Flatten(take);
            for (duckdb::idx_t k = 0; k < take; ++k) {
              ExpectRow(out, k, pos + k, *value);
            }
            pos += take;
          }
          irs::ColumnReader::PointReader cursor{r, *col};
          duckdb::Vector one{duckdb::LogicalType::VARCHAR, 1};
          for (const uint64_t g : {uint64_t{0}, uint64_t{100}, rows - 1}) {
            duckdb::FlatVector::ValidityMutable(one).Reset();
            const bool valid = cursor.FetchRow(g, one, 0);
            EXPECT_EQ(valid, (*value)(g).has_value()) << "row " << g;
            ExpectRow(one, 0, g, *value);
          }
        }
      }
    }
  }
}

TEST_F(ColCodecsTest, ColumnLevelOverridesTableLevel) {
  const auto level_of = [&](irs::Directory& dir) {
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    irs::ReadContext ctx{r};
    irs::BlockWindow window{};
    window = col->Locate(0, window);
    auto seg = col->OpenSegment(window.block, ctx);
    auto info = seg->function.get_segment_info(*seg);
    return info["level"];
  };
  for (const uint8_t column_level : {uint8_t{0}, uint8_t{9}}) {
    irs::FunctionFieldOptions options{
      [column_level](irs::field_id) {
        return irs::ColumnOptions{
          .compression = duckdb::CompressionType::COMPRESSION_DICT_ZSTD,
          .compression_level = column_level};
      },
      [](irs::field_id id) { return id; },
      1024,
      {}};
    options.codec_params = irs::ColCodecParams{.compression_level = 2};
    irs::MemoryDirectory dir{};
    {
      irs::ColWriter w{dir, kSeg, Db()};
      w.SetFieldOptions(&options);
      auto& cw = w.OpenColumn(kField, duckdb::LogicalType::VARCHAR);
      duckdb::Vector vec{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      auto* d = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec);
      for (uint64_t k = 0; k < 500; ++k) {
        d[k] = duckdb::StringVector::AddString(vec, *kUniqueShort(k));
      }
      duckdb::FlatVector::SetSize(vec, 500);
      cw.Append(vec, 500);
      ASSERT_TRUE(w.Commit(0));
    }
    EXPECT_EQ(level_of(dir), column_level == 0 ? "2" : "9");
  }
}

TEST_F(ColCodecsTest, PlainLz4Hc) {
  RoundTrip(duckdb::CompressionType::COMPRESSION_LZ4, {.compression_level = 9},
            3000, 2048, kLongTextWithNulls);
}

TEST_F(ColCodecsTest, SegmentsExceedTheBlock) {
  irs::MemoryDirectory dir{};
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    return Pseudo(g, 1500);
  };
  Write(dir, duckdb::CompressionType::COMPRESSION_LZ4, {}, 4096,
        DEFAULT_ROW_GROUP_SIZE, value);
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  uint64_t largest = 0;
  for (const auto& block : col->DataBlocks()) {
    largest = std::max(largest, block.byte_size);
  }
  EXPECT_GT(largest, 262144U);
  Verify(dir, duckdb::CompressionType::COMPRESSION_LZ4, 4096, value);
}

TEST_F(ColCodecsTest, SegmentTargetCutsRowGroups) {
  irs::MemoryDirectory dir{};
  Write(dir, duckdb::CompressionType::COMPRESSION_DICT_LZ4,
        {.segment_target = 16 * 1024}, 30000, DEFAULT_ROW_GROUP_SIZE,
        kUniqueShort);
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  EXPECT_GT(col->DataBlocks().size(), 4U);
  Verify(dir, duckdb::CompressionType::COMPRESSION_DICT_LZ4, 30000,
         kUniqueShort);
}

TEST_F(ColCodecsTest, DictionaryLargerThanAFrame) {
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    return Pseudo(g, 200);
  };
  RoundTrip(duckdb::CompressionType::COMPRESSION_DICT_ZSTD, {}, 20000,
            DEFAULT_ROW_GROUP_SIZE, value);
}

TEST_F(ColCodecsTest, LaterFramesUseTheFirstFrameAsDictionary) {
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 11 == 0) {
      return std::nullopt;
    }
    return "request /api/v1/items/" + std::to_string(g % 700) + " took " +
           std::to_string(g % 97) + "ms " + Pseudo(g % 50, 40);
  };
  constexpr uint64_t kRows = 20000;
  for (const auto codec : {duckdb::CompressionType::COMPRESSION_DICT_LZ4,
                           duckdb::CompressionType::COMPRESSION_LZ4,
                           duckdb::CompressionType::COMPRESSION_DICT_ZSTD,
                           duckdb::CompressionType::COMPRESSION_DICT_ZXC,
                           duckdb::CompressionType::COMPRESSION_ZXC}) {
    for (const uint8_t level : {uint8_t{0}, uint8_t{9}}) {
      SCOPED_TRACE(testing::Message() << static_cast<int>(codec) << " level "
                                      << static_cast<int>(level));
      irs::MemoryDirectory dir{};
      Write(dir, codec,
            {.compression_level = level, .tier = irs::WriteTier::Flush}, kRows,
            DEFAULT_ROW_GROUP_SIZE, value);
      const auto dictionaries = SegmentInfo(dir, "dictionary");
      EXPECT_NE(
        std::find(dictionaries.begin(), dictionaries.end(), "first_frame"),
        dictionaries.end());
      Verify(dir, codec, kRows, value);

      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      irs::ColumnReader::PointReader cursor{r, *col};
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, 1};
      for (uint64_t g = kRows; g-- > 0;) {
        if (g % 53 != 0 && g + 1 != kRows) {
          continue;
        }
        duckdb::FlatVector::ValidityMutable(out).Reset();
        EXPECT_EQ(cursor.FetchRow(g, out, 0), value(g).has_value())
          << "row " << g;
        ExpectRow(out, 0, g, value);
      }
    }
  }
}

TEST_F(ColCodecsTest, CompactionsTrainADictionaryPerSegment) {
  using duckdb::CompressionType;
  constexpr uint64_t kRows = 120000;
  constexpr uint32_t kRowGroup = 16384;
  std::vector<std::string> templates;
  for (uint64_t t = 0; t < 200; ++t) {
    templates.emplace_back(Pseudo(t, 100));
  }
  const Value value = [&](uint64_t g) -> std::optional<std::string> {
    if (g % 211 == 0) {
      return std::nullopt;
    }
    return templates[(g * 0x9E3779B97F4A7C15ULL >> 40) % templates.size()] +
           std::to_string(g);
  };
  for (const auto codec :
       {CompressionType::COMPRESSION_DICT_LZ4, CompressionType::COMPRESSION_LZ4,
        CompressionType::COMPRESSION_DICT_ZSTD,
        CompressionType::COMPRESSION_COL_ZSTD,
        CompressionType::COMPRESSION_DICT_ZXC,
        CompressionType::COMPRESSION_ZXC}) {
    for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
      SCOPED_TRACE(
        absl::StrCat(duckdb::CompressionTypeToString(codec),
                     tier == irs::WriteTier::Merge ? " merge" : " flush"));
      irs::MemoryDirectory dir{};
      Write(dir, codec, {.compression_level = 3, .tier = tier}, kRows,
            kRowGroup, value);
      const auto dictionaries = SegmentInfo(dir, "dictionary");
      const auto local = absl::c_count(dictionaries, "local");
      if (tier == irs::WriteTier::Merge) {
        EXPECT_GT(local * 2, std::ssize(dictionaries))
          << ::testing::PrintToString(dictionaries);
      } else {
        EXPECT_EQ(local, 0) << ::testing::PrintToString(dictionaries);
      }
      Verify(dir, codec, kRows, value);
    }
  }
}

TEST_F(ColCodecsTest, CompactionsDropADictionaryThatDoesNotPay) {
  using duckdb::CompressionType;
  constexpr uint64_t kRows = 20000;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    return Pseudo(g, 24);
  };
  for (const auto codec :
       {CompressionType::COMPRESSION_LZ4, CompressionType::COMPRESSION_COL_ZSTD,
        CompressionType::COMPRESSION_ZXC}) {
    SCOPED_TRACE(duckdb::CompressionTypeToString(codec));
    irs::MemoryDirectory dir{};
    Write(dir, codec, {}, kRows, DEFAULT_ROW_GROUP_SIZE, value);
    const auto dictionaries = SegmentInfo(dir, "dictionary");
    EXPECT_EQ(absl::c_count(dictionaries, "local"), 0)
      << ::testing::PrintToString(dictionaries);
    Verify(dir, codec, kRows, value);
  }
}

TEST_F(ColCodecsTest, SegmentsTooSmallToTrainUseTheFirstFrame) {
  using duckdb::CompressionType;
  constexpr uint64_t kRows = 1500;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    return "https://shop" + std::to_string(g % 97) +
           ".example.org/item?id=" + std::to_string(g);
  };
  for (const auto codec :
       {CompressionType::COMPRESSION_LZ4, CompressionType::COMPRESSION_COL_ZSTD,
        CompressionType::COMPRESSION_ZXC}) {
    SCOPED_TRACE(duckdb::CompressionTypeToString(codec));
    irs::MemoryDirectory dir{};
    Write(dir, codec, {}, kRows, DEFAULT_ROW_GROUP_SIZE, value);
    EXPECT_EQ(SegmentInfo(dir, "dictionary"),
              std::vector<std::string>{"first_frame"});
    Verify(dir, codec, kRows, value);
  }
}

TEST_F(ColCodecsTest, AnEmptyFirstFrameIsNoDictionary) {
  constexpr uint64_t kRows = 105;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g < 100) {
      return std::string{};
    }
    return Pseudo(g % 4, 40000) + std::to_string(g);
  };
  for (const auto codec : {duckdb::CompressionType::COMPRESSION_LZ4,
                           duckdb::CompressionType::COMPRESSION_COL_ZSTD,
                           duckdb::CompressionType::COMPRESSION_ZXC}) {
    SCOPED_TRACE(duckdb::CompressionTypeToString(codec));
    irs::MemoryDirectory dir{};
    Write(dir, codec, {}, kRows, DEFAULT_ROW_GROUP_SIZE, value);
    EXPECT_EQ(SegmentInfo(dir, "dictionary"), std::vector<std::string>{""});
    Verify(dir, codec, kRows, value);
  }
}

TEST_F(ColCodecsTest, AutoTrainsADictionaryWhereItPays) {
  constexpr uint64_t kRows = 60000;
  std::vector<std::string> templates;
  for (uint64_t t = 0; t < 200; ++t) {
    templates.emplace_back(Pseudo(t, 100));
  }
  const Value value = [&](uint64_t g) -> std::optional<std::string> {
    return templates[(g * 0x9E3779B97F4A7C15ULL >> 40) % templates.size()] +
           std::to_string(g);
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Merge ? "merge" : "flush");
    irs::MemoryDirectory dir{};
    Write(dir, duckdb::CompressionType::COMPRESSION_AUTO, {.tier = tier}, kRows,
          DEFAULT_ROW_GROUP_SIZE, value);
    const auto dictionaries = SegmentInfo(dir, "dictionary");
    EXPECT_EQ(absl::c_count(dictionaries, "local") != 0,
              tier == irs::WriteTier::Merge)
      << ::testing::PrintToString(dictionaries)
      << ::testing::PrintToString(SegmentInfo(dir, "codec"));
    Verify(dir, duckdb::CompressionType::COMPRESSION_AUTO, kRows, value);
  }
}

TEST_F(ColCodecsTest, ConstantAndEmptyStrings) {
  const Value constant = [](uint64_t) -> std::optional<std::string> {
    return "the same";
  };
  RoundTrip(duckdb::CompressionType::COMPRESSION_DICT_LZ4, {}, 5000, 2048,
            constant);
  const Value empties = [](uint64_t g) -> std::optional<std::string> {
    if (g % 3 == 0) {
      return "";
    }
    if (g % 3 == 1) {
      return std::nullopt;
    }
    return "x";
  };
  RoundTrip(duckdb::CompressionType::COMPRESSION_DICT_LZ4, {}, 5000, 2048,
            empties);
  RoundTrip(duckdb::CompressionType::COMPRESSION_LZ4, {}, 5000, 2048, empties);
}

TEST_F(ColCodecsTest, ChainGroupsOfEmptyStrings) {
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 128 < 64) {
      return "";
    }
    return "row-" + std::to_string(g);
  };
  for (const auto codec :
       {duckdb::CompressionType::COMPRESSION_FSST,
        duckdb::CompressionType::COMPRESSION_COL_FSST12,
        duckdb::CompressionType::COMPRESSION_COL_FSST_PLUS}) {
    SCOPED_TRACE(static_cast<int>(codec));
    RoundTrip(codec, {}, 5000, 2048, value);
  }
}

TEST_F(ColCodecsTest, AllNull) {
  const Value none = [](uint64_t) -> std::optional<std::string> {
    return std::nullopt;
  };
  RoundTrip(duckdb::CompressionType::COMPRESSION_DICT_LZ4, {}, 3000, 2048,
            none);
  RoundTrip(duckdb::CompressionType::COMPRESSION_LZ4, {}, 3000, 2048, none);
}

TEST_F(ColCodecsTest, SingleRow) {
  RoundTrip(duckdb::CompressionType::COMPRESSION_DICT_LZ4, {}, 1, 2048,
            kUniqueShort);
  RoundTrip(duckdb::CompressionType::COMPRESSION_LZ4, {}, 1, 2048,
            kUniqueShort);
}

namespace {

std::vector<std::unique_ptr<duckdb::Vector>> Chunks(uint64_t rows,
                                                    const Value& value) {
  std::vector<std::unique_ptr<duckdb::Vector>> chunks;
  for (uint64_t pos = 0; pos < rows; pos += STANDARD_VECTOR_SIZE) {
    const auto take = std::min<uint64_t>(rows - pos, STANDARD_VECTOR_SIZE);
    auto vec = std::make_unique<duckdb::Vector>(duckdb::LogicalType::VARCHAR,
                                                STANDARD_VECTOR_SIZE);
    auto* d = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(*vec);
    auto& validity = duckdb::FlatVector::ValidityMutable(*vec);
    validity.Reset(STANDARD_VECTOR_SIZE);
    for (uint64_t k = 0; k < take; ++k) {
      if (const auto v = value(pos + k)) {
        d[k] = duckdb::StringVector::AddString(*vec, *v);
      } else {
        validity.SetInvalid(k);
      }
    }
    duckdb::FlatVector::SetSize(*vec, take);
    chunks.push_back(std::move(vec));
  }
  return chunks;
}

std::vector<irs::WriteChunk> WriteChunks(
  const std::vector<std::unique_ptr<duckdb::Vector>>& chunks) {
  std::vector<irs::WriteChunk> out;
  for (const auto& c : chunks) {
    out.push_back(irs::RefChunk(*c, c->size()));
  }
  return out;
}

void Accumulate(irs::codecs::StringAccumulator& acc,
                const std::vector<std::unique_ptr<duckdb::Vector>>& chunks) {
  for (const auto& c : chunks) {
    acc.Add(*c);
  }
}

const Value kWordSoup = [](uint64_t g) -> std::optional<std::string> {
  static constexpr std::string_view kWords[] = {
    "event",  "completed", "for",   "user",    "on",      "host",
    "status", "ok",        "retry", "queue",   "latency", "ms",
    "region", "eu-west",   "trace", "sampled", "payload", "bytes"};
  std::string s;
  for (uint64_t k = 0; k < 24; ++k) {
    s += kWords[(g * 7 + k * 13) % std::size(kWords)];
    s += ' ';
  }
  s += std::to_string(g);
  return s;
};

const Value kVocabulary = [](uint64_t g) -> std::optional<std::string> {
  std::mt19937_64 rng{g * 0x9E3779B97F4A7C15ULL + 7};
  std::string s;
  for (int k = 0; k < 40; ++k) {
    const auto w = rng() % 3000;
    const auto word = w * w / 3000;
    std::mt19937_64 letters{word + 1};
    for (uint64_t c = 0; c < 3 + word % 8; ++c) {
      s.push_back(static_cast<char>('a' + letters() % 26));
    }
    s.push_back(' ');
  }
  return s;
};

}  // namespace

TEST_F(ColCodecsTest, NamedCodecSealsEverySegment) {
  using irs::codecs::ByteCodec;
  using irs::codecs::Shape;
  const irs::codecs::StringChoice choices[] = {
    {Shape::Dedup, ByteCodec::Fsst},     {Shape::Dedup, ByteCodec::Lz4},
    {Shape::Dedup, ByteCodec::Zstd},     {Shape::Dedup, ByteCodec::Zxc},
    {Shape::Dedup, ByteCodec::FsstPlus}, {Shape::Dedup, ByteCodec::OnPair},
    {Shape::Dedup, ByteCodec::Fsst12},   {Shape::Plain, ByteCodec::Fsst},
    {Shape::Plain, ByteCodec::Lz4},      {Shape::Plain, ByteCodec::Zstd},
    {Shape::Plain, ByteCodec::Zxc},      {Shape::Plain, ByteCodec::FsstPlus},
    {Shape::Plain, ByteCodec::OnPair},   {Shape::Plain, ByteCodec::Fsst12},
  };
  const auto chunks = Chunks(20000, kWordSoup);
  irs::codecs::StringAccumulator acc;
  Accumulate(acc, chunks);
  const auto write_chunks = WriteChunks(chunks);
  for (const auto choice : choices) {
    irs::codecs::StringTuning tuning;
    uint64_t segments = 0;
    uint64_t rows = 0;
    const irs::ColCodecParams params{.segment_target = 16 * 1024};
    const auto sink = [&](irs::codecs::StringChoice sealed,
                          duckdb::BaseStatistics, uint64_t count,
                          std::string_view) {
      EXPECT_EQ(sealed.shape, choice.shape);
      EXPECT_EQ(sealed.leaf, choice.leaf);
      ++segments;
      rows += count;
    };
    auto& buffers = Db().GetBufferManager();
    const auto outcome =
      choice.shape == Shape::Dedup
        ? irs::codecs::SealSegments(acc, &choice, params,
                                    duckdb::LogicalType::VARCHAR, tuning,
                                    buffers, sink)
        : irs::codecs::SealSegments(write_chunks, choice, params,
                                    duckdb::LogicalType::VARCHAR, tuning,
                                    buffers, sink);
    EXPECT_TRUE(outcome.sealed);
    EXPECT_EQ(outcome.all_dedup, choice.shape == Shape::Dedup);
    EXPECT_GT(segments, 1u);
    EXPECT_EQ(rows, 20000u);
    EXPECT_FALSE(tuning.reuse.pick);
  }
}

TEST_F(ColCodecsTest, AutoChoosesPerRowGroup) {
  using irs::codecs::Shape;
  constexpr uint64_t kSwitch = 10 * STANDARD_VECTOR_SIZE;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 11 == 0) {
      return std::nullopt;
    }
    if (g < kSwitch) {
      return Pseudo(g, 24);
    }
    return "status-" + std::to_string(g % 4);
  };
  const auto chunks = Chunks(3 * kSwitch, value);
  irs::codecs::StringTuning tuning;
  const auto seal = [&](size_t first, size_t end) {
    irs::codecs::StringAccumulator acc;
    for (auto c = first; c < end; ++c) {
      acc.Add(*chunks[c]);
    }
    std::vector<Shape> shapes;
    const auto outcome = irs::codecs::SealSegments(
      acc, nullptr, irs::ColCodecParams{.segment_target = 16 * 1024},
      duckdb::LogicalType::VARCHAR, tuning, Db().GetBufferManager(),
      [&](irs::codecs::StringChoice choice, duckdb::BaseStatistics, uint64_t,
          std::string_view) { shapes.push_back(choice.shape); });
    EXPECT_TRUE(outcome.sealed);
    return shapes;
  };
  const auto all = [](const std::vector<Shape>& shapes, Shape shape) {
    return !shapes.empty() &&
           absl::c_all_of(shapes, [&](Shape s) { return s == shape; });
  };
  const auto distinct = seal(0, 10);
  EXPECT_GT(distinct.size(), 1u);
  EXPECT_TRUE(all(distinct, Shape::Plain))
    << ::testing::PrintToString(distinct);
  const auto repeated = seal(10, 30);
  EXPECT_TRUE(all(repeated, Shape::Dedup))
    << ::testing::PrintToString(repeated);
  const auto mixed = seal(5, 15);
  EXPECT_GT(mixed.size(), 1u);
  EXPECT_TRUE(all(mixed, Shape::Plain)) << ::testing::PrintToString(mixed);
  ASSERT_TRUE(tuning.reuse.pick);
  EXPECT_EQ(tuning.reuse.pick->choice.shape, Shape::Plain);

  RoundTrip(duckdb::CompressionType::COMPRESSION_AUTO,
            {.segment_target = 16 * 1024}, 3 * kSwitch, DEFAULT_ROW_GROUP_SIZE,
            value);
}

TEST_F(ColCodecsTest, PointReadsAlternateBlocksOfDifferentCodecs) {
  constexpr uint64_t kRows = 6 * 1024;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if ((g / 1024) % 2 == 0) {
      return absl::StrCat("v", g % 3);
    }
    return absl::StrCat("unique-", g * 7919, "-", g);
  };
  irs::MemoryDirectory dir;
  Write(dir, duckdb::CompressionType::COMPRESSION_AUTO, {}, kRows, 1024, value);
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  absl::flat_hash_set<duckdb::CompressionType> codecs;
  for (const auto& block : col->DataBlocks()) {
    codecs.insert(block.codec->type);
  }
  ASSERT_GT(codecs.size(), 1u);

  irs::ColumnReader::PointReader cursor{r, *col};
  duckdb::Vector out{duckdb::LogicalType::VARCHAR, 1};
  for (uint64_t i = 0; i < kRows; i += 37) {
    for (const auto g : {i, kRows - 1 - i}) {
      duckdb::FlatVector::ValidityMutable(out).Reset();
      EXPECT_TRUE(cursor.FetchRow(g, out, 0)) << "row " << g;
      ExpectRow(out, 0, g, value);
    }
  }
}

TEST_F(ColCodecsTest, BatchedPointReadsKeepTheStringsOfEveryBlock) {
  constexpr uint64_t kRows = 8 * 1024;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    return absl::StrCat("uncompressed-row-", g * 7919, "-", g);
  };
  for (const auto codec : {duckdb::CompressionType::COMPRESSION_UNCOMPRESSED,
                           duckdb::CompressionType::COMPRESSION_LZ4}) {
    irs::MemoryDirectory dir;
    Write(dir, codec, {}, kRows, 1024, value);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    ASSERT_GT(col->DataBlocks().size(), 4u);
    for (const auto& block : col->DataBlocks()) {
      ASSERT_EQ(block.codec->type, Written(codec));
    }

    irs::ColumnReader::PointReader cursor{r, *col};
    std::vector<uint64_t> rows;
    for (uint64_t g = 5; g < kRows; g += 13) {
      rows.push_back(g);
    }
    duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    cursor.FetchRows(rows, out, 0);
    for (size_t k = 0; k < rows.size(); ++k) {
      ExpectRow(out, k, rows[k], value);
    }
  }
}

TEST_F(ColCodecsTest, PointReadsUseTheCachedDictionary) {
  constexpr uint64_t kRows = 20000;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 11 == 0) {
      return std::nullopt;
    }
    return absl::StrCat("dictionary-entry-", g % 97);
  };
  irs::MemoryDirectory dir;
  Write(dir, duckdb::CompressionType::COMPRESSION_DICT_LZ4, {}, kRows, 8192,
        value);
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  ASSERT_GT(col->DataBlocks().size(), 1u);
  for (int pass = 0; pass < 2; ++pass) {
    auto state = col->InitScan(r.Ctx());
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      const auto take = std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, take);
      out.Flatten(take);
    }
  }
  auto& cache = Db().GetObjectCache();
  const auto keys = CacheKeys(r, *col);
  for (size_t b = 0; b < keys.size(); ++b) {
    const auto cached = cache.Get<irs::codecs::DecodedDictionary>(keys[b]);
    ASSERT_NE(cached, nullptr) << "block " << b;
    auto& data = cached->Dictionary()->data;
    auto* values = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(data);
    for (duckdb::idx_t i = 1; i < data.size(); ++i) {
      values[i] = duckdb::StringVector::AddString(
        data, absl::StrCat("cached:", values[i].GetString()));
    }
  }
  const Value cached = [&](uint64_t g) -> std::optional<std::string> {
    if (const auto v = value(g)) {
      return absl::StrCat("cached:", *v);
    }
    return std::nullopt;
  };

  irs::ColumnReader::PointReader cursor{r, *col};
  std::vector<uint64_t> rows;
  for (uint64_t g = 3; g < kRows; g += 29) {
    rows.push_back(g);
  }
  duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
  cursor.FetchRows(rows, out, 0);
  for (size_t k = 0; k < rows.size(); ++k) {
    ExpectRow(out, k, rows[k], cached);
  }
  duckdb::Vector one{duckdb::LogicalType::VARCHAR, 1};
  for (const auto g : {uint64_t{1}, kRows / 2 + 1, kRows - 1}) {
    duckdb::FlatVector::ValidityMutable(one).Reset();
    cursor.FetchRow(g, one, 0);
    ExpectRow(one, 0, g, cached);
  }
}

TEST_F(ColCodecsTest, AutoIsNoLargerThanItsCandidates) {
  using duckdb::CompressionType;
  const CompressionType candidates[] = {
    CompressionType::COMPRESSION_DICT_FSST, CompressionType::COMPRESSION_FSST,
    CompressionType::COMPRESSION_DICT_LZ4,  CompressionType::COMPRESSION_LZ4,
    CompressionType::COMPRESSION_DICT_ZXC,  CompressionType::COMPRESSION_ZXC};
  struct Corpus {
    const char* name;
    const Value* value;
    uint64_t rows;
  };
  const Corpus corpora[] = {
    {"low-cardinality", &kLowCardinalityWithNulls, 30000},
    {"unique-short", &kUniqueShort, 30000},
    {"word-soup", &kWordSoup, 30000},
    {"long-text", &kLongTextWithNulls, 3000},
  };
  for (const auto& corpus : corpora) {
    irs::MemoryDirectory auto_dir{};
    Write(auto_dir, CompressionType::COMPRESSION_AUTO, {}, corpus.rows, 16384,
          *corpus.value);
    const auto auto_bytes = ColumnBytes(auto_dir);
    auto best = std::numeric_limits<uint64_t>::max();
    for (const auto codec : candidates) {
      irs::MemoryDirectory dir{};
      Write(dir, codec, {}, corpus.rows, 16384, *corpus.value);
      best = std::min(best, ColumnBytes(dir));
    }
    EXPECT_LE(auto_bytes, best + best / 8)
      << corpus.name << " auto " << auto_bytes << " best named " << best;
  }
}

TEST_F(ColCodecsTest, SampledPricesDoNotLoseToTheCandidatesTheyPrice) {
  using duckdb::CompressionType;
  const Value urls = [](uint64_t g) -> std::optional<std::string> {
    return "https://example.org/section/" + std::to_string(g % 5000) +
           "/page?id=" + std::to_string(g % 97);
  };
  struct Arm {
    const char* name;
    const Value* value;
    uint64_t rows;
    uint32_t segment_target;
    CompressionType named;
    uint8_t level;
  };
  const Arm arms[] = {
    {"periodic urls", &urls, 150000, irs::ColCodecParams{}.segment_target,
     CompressionType::COMPRESSION_LZ4, 6},
    {"word soup", &kWordSoup, 60000, 64 * 1024,
     CompressionType::COMPRESSION_FSST, 0},
  };
  for (const auto& arm : arms) {
    irs::MemoryDirectory auto_dir{};
    Write(auto_dir, CompressionType::COMPRESSION_AUTO,
          {.segment_target = arm.segment_target}, arm.rows,
          DEFAULT_ROW_GROUP_SIZE, *arm.value);
    irs::MemoryDirectory named_dir{};
    Write(
      named_dir, arm.named,
      {.compression_level = arm.level, .segment_target = arm.segment_target},
      arm.rows, DEFAULT_ROW_GROUP_SIZE, *arm.value);
    const auto auto_bytes = ColumnBytes(auto_dir);
    const auto named_bytes = ColumnBytes(named_dir);
    EXPECT_LE(auto_bytes, named_bytes + named_bytes / 100)
      << arm.name << " auto " << auto_bytes << " named " << named_bytes;
    Verify(auto_dir, CompressionType::COMPRESSION_AUTO, arm.rows, *arm.value);
  }
}

TEST_F(ColCodecsTest, AutoLevelsComeFromTheLadders) {
  using duckdb::CompressionType;
  const std::set<std::string> lz4{"1", "4", "6"};
  const std::pair<const char*, const Value*> corpora[] = {
    {"low-cardinality", &kLowCardinalityWithNulls},
    {"unique-short", &kUniqueShort},
    {"word-soup", &kWordSoup},
    {"long-text", &kLongTextWithNulls},
  };
  for (const auto& [name, value] : corpora) {
    irs::MemoryDirectory dir{};
    Write(dir, CompressionType::COMPRESSION_AUTO, {.segment_target = 16 * 1024},
          6000, 2048, *value);
    for (const auto& [type, level] : BlockLevels(dir)) {
      const auto label = std::string{name} + " " +
                         duckdb::CompressionTypeToString(type) + " level " +
                         level;
      switch (type) {
        case CompressionType::COMPRESSION_LZ4:
        case CompressionType::COMPRESSION_DICT_LZ4:
          EXPECT_TRUE(lz4.contains(level)) << label;
          break;
        case CompressionType::COMPRESSION_COL_FSST:
        case CompressionType::COMPRESSION_COL_DICT_FSST:
        case CompressionType::COMPRESSION_COL_FSST_PLUS:
        case CompressionType::COMPRESSION_COL_DICT_FSST_PLUS:
        case CompressionType::COMPRESSION_COL_ONPAIR:
        case CompressionType::COMPRESSION_COL_DICT_ONPAIR:
        case CompressionType::COMPRESSION_COL_FSST12:
        case CompressionType::COMPRESSION_COL_DICT_FSST12:
          EXPECT_EQ(level, "0") << label;
          break;
        default:
          EXPECT_TRUE(type == CompressionType::COMPRESSION_UNCOMPRESSED ||
                      type == CompressionType::COMPRESSION_CONSTANT)
            << label;
      }
    }
  }
}

TEST_F(ColCodecsTest, AutoLevelsFollowTheData) {
  using duckdb::CompressionType;
  const auto levels = [&](const Value& value, uint64_t rows) {
    irs::MemoryDirectory dir{};
    Write(dir, CompressionType::COMPRESSION_AUTO, {.segment_target = 64 * 1024},
          rows, DEFAULT_ROW_GROUP_SIZE, value);
    std::set<std::string> out;
    for (const auto& [type, level] : BlockLevels(dir)) {
      if (!level.empty() && level != "0") {
        out.insert(std::string{duckdb::CompressionTypeToString(type)} + ":" +
                   level);
      }
    }
    return out;
  };
  const auto top = [](const std::set<std::string>& found) {
    int level = 0;
    for (const auto& s : found) {
      level = std::max(level, std::stoi(s.substr(s.rfind(':') + 1)));
    }
    return level;
  };
  const auto text = levels(kVocabulary, 8000);
  ASSERT_FALSE(text.empty());
  EXPECT_GT(top(text), 1) << ::testing::PrintToString(text);
}

TEST_F(ColCodecsTest, AutoTunesLevelsOnTheFirstRowGroupWithData) {
  using Chunked = std::vector<std::unique_ptr<duckdb::Vector>>;
  constexpr uint64_t kRowGroup = 1024;
  const auto seal = [&](const Chunked& chunks,
                        irs::codecs::StringTuning& tuning) {
    irs::codecs::StringAccumulator acc;
    Accumulate(acc, chunks);
    const auto outcome = irs::codecs::SealSegments(
      acc, nullptr, irs::ColCodecParams{}, duckdb::LogicalType::VARCHAR, tuning,
      Db().GetBufferManager(),
      [](irs::codecs::StringChoice, duckdb::BaseStatistics, uint64_t,
         std::string_view) {});
    EXPECT_TRUE(outcome.sealed);
  };
  const auto levels = [](const irs::codecs::StringTuning& tuning) {
    std::vector<int> out;
    for (size_t leaf = 0; leaf < irs::codecs::kByteCodecCount; ++leaf) {
      out.push_back(tuning.tuned.level[leaf]);
      out.push_back(static_cast<int>(tuning.tuned.layout[leaf]));
    }
    return out;
  };
  const Value constant = [](uint64_t) -> std::optional<std::string> {
    return "n/a";
  };
  const auto text = Chunks(kRowGroup, kVocabulary);
  const auto nulls = Chunks(kRowGroup, kAllNull);
  const auto constants = Chunks(kRowGroup, constant);

  irs::codecs::StringTuning untuned;
  seal(nulls, untuned);
  irs::codecs::StringTuning tuned;
  seal(text, tuned);
  ASSERT_NE(levels(tuned), levels(untuned));

  const std::pair<const char*, const Chunked*> heads[] = {
    {"all null", &nulls},
    {"constant", &constants},
  };
  for (const auto& [name, head] : heads) {
    for (int groups = 1; groups <= 2; ++groups) {
      SCOPED_TRACE(absl::StrCat(groups, " ", name, " row groups"));
      irs::codecs::StringTuning tuning;
      for (int g = 0; g < groups; ++g) {
        seal(*head, tuning);
      }
      seal(text, tuning);
      EXPECT_EQ(levels(tuning), levels(tuned));
    }
  }
}

const std::set<duckdb::CompressionType>& ColumnstoreLeaves() {
  using duckdb::CompressionType;
  static const std::set<CompressionType> leaves{
    CompressionType::COMPRESSION_LZ4,
    CompressionType::COMPRESSION_DICT_LZ4,
    CompressionType::COMPRESSION_COL_FSST,
    CompressionType::COMPRESSION_COL_DICT_FSST,
    CompressionType::COMPRESSION_COL_ZSTD,
    CompressionType::COMPRESSION_DICT_ZSTD,
    CompressionType::COMPRESSION_ZXC,
    CompressionType::COMPRESSION_DICT_ZXC,
    CompressionType::COMPRESSION_UNCOMPRESSED};
  return leaves;
}

TEST_F(ColCodecsTest, AutoPicksColumnstoreLeaves) {
  using duckdb::CompressionType;
  const auto& leaves = ColumnstoreLeaves();
  struct Corpus {
    const char* name;
    const Value* value;
    uint64_t rows;
  };
  const Corpus corpora[] = {
    {"low-cardinality", &kLowCardinalityWithNulls, 30000},
    {"unique-short", &kUniqueShort, 30000},
    {"word-soup", &kWordSoup, 30000},
    {"long-text", &kLongTextWithNulls, 3000},
  };
  for (const auto& corpus : corpora) {
    irs::MemoryDirectory dir{};
    Write(dir, CompressionType::COMPRESSION_AUTO, {}, corpus.rows, 16384,
          *corpus.value);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    for (const auto& block : col->DataBlocks()) {
      EXPECT_TRUE(leaves.contains(block.codec->type))
        << corpus.name << " "
        << duckdb::CompressionTypeToString(block.codec->type);
    }
  }
}

TEST_F(ColCodecsTest, TransactionalCodecsLeaveTextToTheColumnstore) {
  using duckdb::CompressionType;
  struct Corpus {
    const char* name;
    const Value* value;
    uint64_t rows;
  };
  const Corpus corpora[] = {
    {"low-cardinality", &kLowCardinalityWithNulls, 30000},
    {"long-text", &kLongTextWithNulls, 3000},
  };
  for (const auto forced : {CompressionType::COMPRESSION_DICTIONARY,
                            CompressionType::COMPRESSION_RLE,
                            CompressionType::COMPRESSION_BITPACKING}) {
    for (const auto& corpus : corpora) {
      SCOPED_TRACE(absl::StrCat(duckdb::CompressionTypeToString(forced), " ",
                                corpus.name));
      irs::MemoryDirectory dir{};
      Write(dir, forced, {}, corpus.rows, 16384, *corpus.value);
      Verify(dir, CompressionType::COMPRESSION_AUTO, corpus.rows,
             *corpus.value);
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      for (const auto& block : col->DataBlocks()) {
        EXPECT_TRUE(ColumnstoreLeaves().contains(block.codec->type))
          << duckdb::CompressionTypeToString(block.codec->type);
      }
    }
  }
}

TEST_F(ColCodecsTest, CachedDictionaryKeepsNoScanContext) {
  const auto& value = kLowCardinalityWithNulls;
  constexpr uint32_t kRows = 10 * STANDARD_VECTOR_SIZE;
  irs::MemoryDirectory dir{};
  Write(dir, duckdb::CompressionType::COMPRESSION_DICT_LZ4, {}, kRows, kRows,
        value);
  auto& cache = Db().GetObjectCache();
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  const auto keys = CacheKeys(r, *col);
  for (const auto& key : keys) {
    cache.Delete(key);
  }
  const auto certified = [](const duckdb::Vector& out) {
    const auto& payload =
      out.GetVectorType() == duckdb::VectorType::DICTIONARY_VECTOR
        ? duckdb::DictionaryVector::Child(out)
        : out;
    const auto& aux = payload.GetBufferRef()->GetAuxiliaryData();
    return aux && duckdb::ImmutableStrings::Certified(*aux);
  };
  const auto scan_all = [&] {
    auto ctx = std::make_shared<irs::ReadContext>(r);
    std::weak_ptr<irs::ReadContext> scanned = ctx;
    auto state = col->InitScan(std::move(ctx));
    size_t dictionaries = 0;
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      const auto take = std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, take);
      dictionaries +=
        out.GetVectorType() == duckdb::VectorType::DICTIONARY_VECTOR;
      EXPECT_TRUE(certified(out));
    }
    EXPECT_GT(dictionaries, 0u);
    return scanned;
  };
  EXPECT_TRUE(scan_all().expired());
  EXPECT_TRUE(scan_all().expired());
  for (const auto& key : keys) {
    EXPECT_NE(cache.Get<irs::codecs::DecodedDictionary>(key), nullptr) << key;
  }
  EXPECT_TRUE(scan_all().expired());
}

TEST_F(ColCodecsTest, FetchedStringsOutliveTheReader) {
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 11 == 0) {
      return std::nullopt;
    }
    return Pseudo(g % 3001, 40 + g % 23);
  };
  constexpr uint32_t kRows = 30000;
  irs::MemoryDirectory dir{};
  Write(dir, duckdb::CompressionType::COMPRESSION_DICT_LZ4, {}, kRows, kRows,
        value);
  std::vector<uint64_t> rows;
  for (uint64_t g = 0; g < kRows; g += 3) {
    rows.push_back(g);
  }
  std::vector<std::unique_ptr<duckdb::Vector>> outs;
  {
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    irs::ColumnReader::PointReader cursor{r, *col};
    for (size_t i = 0; i < rows.size(); i += STANDARD_VECTOR_SIZE) {
      const auto take = std::min<size_t>(rows.size() - i, STANDARD_VECTOR_SIZE);
      outs.push_back(std::make_unique<duckdb::Vector>(
        duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE));
      cursor.FetchRows(std::span<const uint64_t>{&rows[i], take}, *outs.back(),
                       0);
    }
    for (const auto& key : CacheKeys(r, *col)) {
      Db().GetObjectCache().Delete(key);
    }
  }
  for (size_t i = 0; i < rows.size(); i += STANDARD_VECTOR_SIZE) {
    const auto take = std::min<size_t>(rows.size() - i, STANDARD_VECTOR_SIZE);
    for (size_t k = 0; k < take; ++k) {
      ExpectRow(*outs[i / STANDARD_VECTOR_SIZE], k, rows[i + k], value);
    }
  }
}

TEST_F(ColCodecsTest, DecodedDictionaryIsCachedAcrossScans) {
  const auto& value = kLowCardinalityWithNulls;
  constexpr uint64_t kRows = 20000;
  irs::MemoryDirectory dir{};
  Write(dir, duckdb::CompressionType::COMPRESSION_DICT_LZ4, {}, kRows, 8192,
        value);
  auto& cache = Db().GetObjectCache();
  EXPECT_EQ(cache.GetMaxMemory(),
            duckdb::ObjectCache::DefaultMaxMemory(
              duckdb::DBConfig::GetConfig(Db()).options.maximum_memory));
  std::vector<std::string> keys;
  const auto cached = [&](const std::string& key) {
    return cache.Get<irs::codecs::DecodedDictionary>(key) != nullptr;
  };
  const auto scan_all = [&](irs::ColReader& r, const irs::ColumnReader& col) {
    auto state = col.InitScan(r.Ctx());
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take = std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      col.Scan(state, out, take);
      out.Flatten(take);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        ExpectRow(out, k, pos + k, value);
      }
      pos += take;
    }
  };
  {
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    ASSERT_GT(col->DataBlocks().size(), 1u);
    keys = CacheKeys(r, *col);
    for (const auto& key : keys) {
      EXPECT_FALSE(cached(key));
    }
    scan_all(r, *col);
    for (const auto& key : keys) {
      EXPECT_FALSE(cached(key)) << key;
    }
    scan_all(r, *col);
    for (const auto& key : keys) {
      EXPECT_TRUE(cached(key)) << key;
    }
    scan_all(r, *col);
    {
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
      for (const auto& key : keys) {
        cache.Delete(key);
      }
      irs::ColReader other{dir, std::string{kSeg}, Db(),
                           irs::IOAdvice::SEQUENTIAL};
      scan_all(other, *other.Column(kField));
      out.Flatten(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
        ExpectRow(out, k, k, value);
      }
      uint64_t pos = STANDARD_VECTOR_SIZE;
      while (pos < kRows) {
        const auto take = std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE);
        duckdb::Vector next{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
        col->Scan(state, next, take);
        next.Flatten(take);
        for (duckdb::idx_t k = 0; k < take; ++k) {
          ExpectRow(next, k, pos + k, value);
        }
        pos += take;
      }
    }
    {
      std::vector<uint64_t> picked;
      for (uint64_t g = 3; g < kRows; g += 41) {
        picked.push_back(g);
      }
      auto state = col->InitScan(r.Ctx());
      size_t i = 0;
      while (i < picked.size()) {
        const auto take =
          std::min<size_t>(picked.size() - i, STANDARD_VECTOR_SIZE);
        duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
        duckdb::FlatVector::ValidityMutable(out).Reset(STANDARD_VECTOR_SIZE);
        const Rows2 sub{&picked[i], take};
        irs::column_internal::GatherRows(*col, state, sub, out, 0,
                                         /*whole_output=*/true);
        out.Flatten(take);
        for (size_t k = 0; k < take; ++k) {
          ExpectRow(out, k, picked[i + k], value);
        }
        i += take;
      }
    }
    scan_all(r, *col);
    for (const auto& key : keys) {
      EXPECT_TRUE(cached(key)) << key;
    }
  }
  irs::ColReader reopened{dir, std::string{kSeg}, Db(),
                          irs::IOAdvice::SEQUENTIAL};
  const auto* col = reopened.Column(kField);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->DataBlocks().size(), keys.size());
  scan_all(reopened, *col);
}

TEST_F(ColCodecsTest, DictionaryCompletedByPartialScansIsCached) {
  const auto& value = kLowCardinalityWithNulls;
  constexpr uint64_t kRows = 20000;
  irs::MemoryDirectory dir{};
  Write(dir, duckdb::CompressionType::COMPRESSION_DICT_FSST, {}, kRows, 8192,
        value);
  auto& cache = Db().GetObjectCache();
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  ASSERT_GT(col->DataBlocks().size(), 1u);
  const auto keys = CacheKeys(r, *col);
  const auto cached = [&](const std::string& key) {
    return cache.Get<irs::codecs::DecodedDictionary>(key) != nullptr;
  };
  const auto scan_partial = [&] {
    auto state = col->InitScan(r.Ctx());
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take = std::min<uint64_t>(kRows - pos, 700);
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      duckdb::FlatVector::ValidityMutable(out).Reset(STANDARD_VECTOR_SIZE);
      col->ScanCount(state, out, take, 0);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        ExpectRow(out, k, pos + k, value);
      }
      pos += take;
    }
  };
  scan_partial();
  for (const auto& key : keys) {
    EXPECT_FALSE(cached(key)) << key;
  }
  scan_partial();
  for (const auto& key : keys) {
    EXPECT_TRUE(cached(key)) << key;
  }
}

TEST_F(ColCodecsTest, GatherFilterReleasesPassedSegments) {
  const auto& value = kLowCardinalityWithNulls;
  constexpr uint64_t kRows = 40000;
  irs::MemoryDirectory dir{};
  Write(dir, duckdb::CompressionType::COMPRESSION_DICT_LZ4, {}, kRows, 4096,
        value);
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  ASSERT_GT(col->DataBlocks().size(), 4u);
  auto expr = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
    duckdb::ExpressionType::OPERATOR_IS_NOT_NULL, duckdb::LogicalType::BOOLEAN);
  expr->GetChildrenMutable().push_back(
    duckdb::make_uniq<duckdb::BoundReferenceExpression>(
      duckdb::LogicalType::VARCHAR, 0));
  const duckdb::ExpressionFilter filter{std::move(expr)};
  duckdb::Connection con{_db};
  auto filter_state =
    duckdb::TableFilterState::Initialize(*con.context, filter);
  auto state = col->InitScan(r.Ctx());
  uint64_t kept = 0;
  for (uint64_t anchor = 0; anchor < kRows; anchor += STANDARD_VECTOR_SIZE) {
    const auto span = std::min<uint64_t>(kRows - anchor, STANDARD_VECTOR_SIZE);
    duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
    for (duckdb::idx_t i = 0; i < span; ++i) {
      sel.set_index(i, i);
    }
    duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    kept += col->GatherFilter(state, anchor, span, sel, span, filter,
                              *filter_state, irs::NullCheckKind::None, out);
    EXPECT_LE(state.segments.size(), 2u) << "window at row " << anchor;
    EXPECT_LE(state.st.previous_states.size(), 1u)
      << "window at row " << anchor;
  }
  uint64_t expected = 0;
  for (uint64_t g = 0; g < kRows; ++g) {
    expected += value(g).has_value() ? 1 : 0;
  }
  EXPECT_EQ(kept, expected);
}

TEST_F(ColCodecsTest, SortedDictionaryFiltersMatchTheValues) {
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 13 == 0) {
      return std::nullopt;
    }
    char buf[48];
    std::snprintf(buf, sizeof buf, "tenant-%05llu/item",
                  static_cast<unsigned long long>((g * 7919) % 3000));
    return std::string{buf};
  };
  constexpr uint64_t kRows = 30000;
  for (const auto codec :
       {duckdb::CompressionType::COMPRESSION_DICT_FSST,
        duckdb::CompressionType::COMPRESSION_COL_DICT_FSST_PLUS,
        duckdb::CompressionType::COMPRESSION_COL_DICT_ONPAIR,
        duckdb::CompressionType::COMPRESSION_COL_DICT_FSST12}) {
    SCOPED_TRACE(duckdb::CompressionTypeToString(codec));
    irs::MemoryDirectory dir{};
    Write(dir, codec, {}, kRows, 8192, value);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    duckdb::Connection con{_db};
    using Cmp = duckdb::ExpressionType;
    const auto holds = [](Cmp cmp, std::string_view v, std::string_view key) {
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
    for (const std::string key :
         {"tenant-00000/item", "tenant-01499/item", "tenant-01499/itex",
          "tenant-02999/item", "a", "z"}) {
      for (const auto cmp :
           {Cmp::COMPARE_EQUAL, Cmp::COMPARE_NOTEQUAL, Cmp::COMPARE_LESSTHAN,
            Cmp::COMPARE_LESSTHANOREQUALTO, Cmp::COMPARE_GREATERTHAN,
            Cmp::COMPARE_GREATERTHANOREQUALTO}) {
        const duckdb::ExpressionFilter filter{
          duckdb::BoundComparisonExpression::Create(
            cmp,
            duckdb::make_uniq<duckdb::BoundReferenceExpression>(
              duckdb::LogicalType::VARCHAR, 0),
            duckdb::make_uniq<duckdb::BoundConstantExpression>(
              duckdb::Value{key}))};
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
          duckdb::Vector out{duckdb::LogicalType::VARCHAR,
                             STANDARD_VECTOR_SIZE};
          const auto kept =
            col->GatherFilter(state, anchor, span, sel, span, filter,
                              *filter_state, irs::NullCheckKind::None, out);
          duckdb::idx_t next = 0;
          for (duckdb::idx_t i = 0; i < span; ++i) {
            const auto v = value(anchor + i);
            const bool expected = v && holds(cmp, *v, key);
            const bool got = next < kept && sel.get_index(next) == i;
            next += got ? 1 : 0;
            ASSERT_EQ(got, expected)
              << "row " << anchor + i << " key " << key << " cmp "
              << duckdb::EnumUtil::ToString(cmp);
          }
        }
      }
    }
  }
}

TEST_F(ColCodecsTest, SparseSelectionsFilterOnlyTheirEntries) {
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 17 == 0) {
      return std::nullopt;
    }
    char buf[32];
    std::snprintf(buf, sizeof buf, "key-%06llu",
                  static_cast<unsigned long long>((g * 7919) % 20000));
    return std::string{buf};
  };
  constexpr uint64_t kRows = 60000;
  irs::MemoryDirectory dir{};
  Write(dir, duckdb::CompressionType::COMPRESSION_DICT_LZ4, {}, kRows, 65536,
        value);
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  duckdb::Connection con{_db};
  for (const auto cmp : {duckdb::ExpressionType::COMPARE_EQUAL,
                         duckdb::ExpressionType::COMPARE_NOTEQUAL}) {
    for (const uint64_t stride : {uint64_t{9}, uint64_t{1}}) {
      const std::string key = "key-012345";
      const duckdb::ExpressionFilter filter{
        duckdb::BoundComparisonExpression::Create(
          cmp,
          duckdb::make_uniq<duckdb::BoundReferenceExpression>(
            duckdb::LogicalType::VARCHAR, 0),
          duckdb::make_uniq<duckdb::BoundConstantExpression>(
            duckdb::Value{key}))};
      auto filter_state =
        duckdb::TableFilterState::Initialize(*con.context, filter);
      auto state = col->InitScan(r.Ctx());
      for (uint64_t anchor = 0; anchor < kRows;
           anchor += STANDARD_VECTOR_SIZE) {
        const auto span =
          std::min<uint64_t>(kRows - anchor, STANDARD_VECTOR_SIZE);
        duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
        duckdb::idx_t offered = 0;
        for (duckdb::idx_t i = 0; i < span; ++i) {
          if ((anchor + i) % stride == 0) {
            sel.set_index(offered++, i);
          }
        }
        duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
        const auto kept =
          col->GatherFilter(state, anchor, span, sel, offered, filter,
                            *filter_state, irs::NullCheckKind::None, out);
        duckdb::idx_t next = 0;
        for (duckdb::idx_t i = 0; i < span; ++i) {
          const auto g = anchor + i;
          const auto v = value(g);
          const bool expected =
            g % stride == 0 && v &&
            (cmp == duckdb::ExpressionType::COMPARE_EQUAL ? *v == key
                                                          : *v != key);
          const bool got = next < kept && sel.get_index(next) == i;
          if (got) {
            EXPECT_EQ(out.GetValue(i).ToString(), *v) << "row " << g;
          }
          next += got ? 1 : 0;
          ASSERT_EQ(got, expected) << "row " << g << " stride " << stride;
        }
      }
    }
  }
}

TEST_F(ColCodecsTest, ZxcFramesAfterAnOversizedEntryCanBeEmpty) {
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g == 0) {
      return std::string(40000, 'a') + "z";
    }
    if (g % 2 == 0) {
      return std::nullopt;
    }
    return std::string{};
  };
  for (const auto codec : {duckdb::CompressionType::COMPRESSION_ZXC,
                           duckdb::CompressionType::COMPRESSION_DICT_ZXC}) {
    irs::MemoryDirectory dir;
    Write(dir, codec, {}, 5000, 2048, value);
    Verify(dir, codec, 5000, value);
  }
}

TEST_F(ColCodecsTest, DictionarySegmentsCountEveryRowInTotalStringLength) {
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 11 == 0) {
      return std::nullopt;
    }
    return "status-" + std::to_string(g % 5);
  };
  constexpr uint64_t kRows = 20000;
  uint64_t expected = 0;
  for (uint64_t g = 0; g < kRows; ++g) {
    expected += value(g).value_or("").size();
  }
  for (const auto codec : {duckdb::CompressionType::COMPRESSION_DICT_LZ4,
                           duckdb::CompressionType::COMPRESSION_DICT_FSST}) {
    irs::MemoryDirectory dir;
    Write(dir, codec, {}, kRows, 4096, value);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    uint64_t total = 0;
    for (const auto& block : col->DataBlocks()) {
      const auto length =
        duckdb::StringStats::TotalStringLength(block.statistics);
      ASSERT_TRUE(length.IsValid());
      total += length.GetIndex();
    }
    EXPECT_EQ(total, expected) << duckdb::CompressionTypeToString(codec);
  }
}

TEST_F(ColCodecsTest, MappedFileAlignedBlocks) {
  const auto path = test_dir() / "col_codecs_mmap";
  std::filesystem::create_directories(path);
  irs::MMapDirectory dir{path};
  struct Arm {
    duckdb::CompressionType codec;
    const Value* value;
  };
  const Arm arms[] = {
    {duckdb::CompressionType::COMPRESSION_DICT_LZ4, &kLowCardinalityWithNulls},
    {duckdb::CompressionType::COMPRESSION_DICT_ZSTD, &kUniqueShort},
    {duckdb::CompressionType::COMPRESSION_LZ4, &kLongTextWithNulls},
    {duckdb::CompressionType::COMPRESSION_DICT_FSST, &kLowCardinalityWithNulls},
    {duckdb::CompressionType::COMPRESSION_FSST, &kLongTextWithNulls},
    {duckdb::CompressionType::COMPRESSION_DICT_ZXC, &kLowCardinalityWithNulls},
    {duckdb::CompressionType::COMPRESSION_ZXC, &kLongTextWithNulls},
    {duckdb::CompressionType::COMPRESSION_UNCOMPRESSED, &kUniqueShort},
  };
  for (const auto& arm : arms) {
    Write(dir, arm.codec, {}, 5000, 2048, *arm.value);
    {
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      for (const auto& block : col->DataBlocks()) {
        EXPECT_EQ(block.file_offset % 8, 0u)
          << duckdb::CompressionTypeToString(arm.codec);
      }
    }
    Verify(dir, arm.codec, 5000, *arm.value);
    dir.remove(irs::FileName(kSeg));
  }
}

TEST_F(ColCodecsTest, MappedFileNumericCodecs) {
  const auto path = test_dir() / "col_codecs_mmap_numeric";
  std::filesystem::create_directories(path);
  irs::MMapDirectory dir{path};
  constexpr uint64_t kRows = 20000;
  constexpr irs::field_id kRle = 1;
  constexpr irs::field_id kBitpacking = 2;
  constexpr irs::field_id kAlp = 3;
  constexpr irs::field_id kBool = 4;
  {
    irs::ColWriter w{dir, kSeg, Db()};
    auto& rle =
      w.OpenColumn(kRle, duckdb::LogicalType::BIGINT,
                   {.row_group_size = 4096,
                    .compression = duckdb::CompressionType::COMPRESSION_RLE});
    auto& bp = w.OpenColumn(
      kBitpacking, duckdb::LogicalType::INTEGER,
      {.row_group_size = 4096,
       .compression = duckdb::CompressionType::COMPRESSION_BITPACKING});
    auto& alp =
      w.OpenColumn(kAlp, duckdb::LogicalType::DOUBLE,
                   {.row_group_size = 4096,
                    .compression = duckdb::CompressionType::COMPRESSION_ALP});
    auto& bools = w.OpenColumn(
      kBool, duckdb::LogicalType::BOOLEAN,
      {.row_group_size = 4096,
       .compression = duckdb::CompressionType::COMPRESSION_ROARING});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take = std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v_rle{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      duckdb::Vector v_bp{duckdb::LogicalType::INTEGER, STANDARD_VECTOR_SIZE};
      duckdb::Vector v_alp{duckdb::LogicalType::DOUBLE, STANDARD_VECTOR_SIZE};
      duckdb::Vector v_bool{duckdb::LogicalType::BOOLEAN, STANDARD_VECTOR_SIZE};
      auto* d_rle = duckdb::FlatVector::GetDataMutable<int64_t>(v_rle);
      auto* d_bp = duckdb::FlatVector::GetDataMutable<int32_t>(v_bp);
      auto* d_alp = duckdb::FlatVector::GetDataMutable<double>(v_alp);
      auto* d_bool = duckdb::FlatVector::GetDataMutable<bool>(v_bool);
      auto& val_bp = duckdb::FlatVector::ValidityMutable(v_bp);
      val_bp.Reset(STANDARD_VECTOR_SIZE);
      for (uint64_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        d_rle[k] = static_cast<int64_t>(g / 50);
        d_bp[k] = static_cast<int32_t>(g % 1000);
        if (g % 6 == 0) {
          val_bp.SetInvalid(k);
        }
        d_alp[k] = static_cast<double>(g) / 100.0;
        d_bool[k] = g % 3 == 0;
      }
      for (auto* v : {&v_rle, &v_bp, &v_alp, &v_bool}) {
        duckdb::FlatVector::SetSize(*v, take);
      }
      rle.Append(v_rle, take);
      bp.Append(v_bp, take);
      alp.Append(v_alp, take);
      bools.Append(v_bool, take);
      pos += take;
    }
    ASSERT_TRUE(w.Commit(0));
  }
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* c_rle = r.Column(kRle);
  const auto* c_bp = r.Column(kBitpacking);
  const auto* c_alp = r.Column(kAlp);
  const auto* c_bool = r.Column(kBool);
  ASSERT_NE(c_rle, nullptr);
  ASSERT_NE(c_bp, nullptr);
  ASSERT_NE(c_alp, nullptr);
  ASSERT_NE(c_bool, nullptr);
  EXPECT_EQ(c_rle->DataBlocks().front().codec->type,
            duckdb::CompressionType::COMPRESSION_RLE);
  EXPECT_EQ(c_bp->DataBlocks().front().codec->type,
            duckdb::CompressionType::COMPRESSION_BITPACKING);
  EXPECT_EQ(c_alp->DataBlocks().front().codec->type,
            duckdb::CompressionType::COMPRESSION_ALP);
  EXPECT_EQ(c_bool->DataBlocks().front().codec->type,
            duckdb::CompressionType::COMPRESSION_ROARING);

  auto s_rle = c_rle->InitScan(r.Ctx());
  auto s_bp = c_bp->InitScan(r.Ctx());
  auto s_alp = c_alp->InitScan(r.Ctx());
  auto s_bool = c_bool->InitScan(r.Ctx());
  uint64_t pos = 0;
  while (pos < kRows) {
    const auto take = std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector v_rle{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    duckdb::Vector v_bp{duckdb::LogicalType::INTEGER, STANDARD_VECTOR_SIZE};
    duckdb::Vector v_alp{duckdb::LogicalType::DOUBLE, STANDARD_VECTOR_SIZE};
    duckdb::Vector v_bool{duckdb::LogicalType::BOOLEAN, STANDARD_VECTOR_SIZE};
    c_rle->Scan(s_rle, v_rle, take);
    c_bp->Scan(s_bp, v_bp, take);
    c_alp->Scan(s_alp, v_alp, take);
    c_bool->Scan(s_bool, v_bool, take);
    for (auto* v : {&v_rle, &v_bp, &v_alp, &v_bool}) {
      v->Flatten(take);
    }
    const auto* d_rle = duckdb::FlatVector::GetData<int64_t>(v_rle);
    const auto* d_bp = duckdb::FlatVector::GetData<int32_t>(v_bp);
    const auto* d_alp = duckdb::FlatVector::GetData<double>(v_alp);
    const auto* d_bool = duckdb::FlatVector::GetData<bool>(v_bool);
    const auto& val_bp = duckdb::FlatVector::Validity(v_bp);
    for (uint64_t k = 0; k < take; ++k) {
      const auto g = pos + k;
      EXPECT_EQ(d_rle[k], static_cast<int64_t>(g / 50)) << g;
      if (g % 6 == 0) {
        EXPECT_FALSE(val_bp.RowIsValid(k)) << g;
      } else {
        ASSERT_TRUE(val_bp.RowIsValid(k)) << g;
        EXPECT_EQ(d_bp[k], static_cast<int32_t>(g % 1000)) << g;
      }
      EXPECT_DOUBLE_EQ(d_alp[k], static_cast<double>(g) / 100.0) << g;
      EXPECT_EQ(d_bool[k], g % 3 == 0) << g;
    }
    pos += take;
  }
  irs::ColumnReader::PointReader p_rle{r, *c_rle};
  irs::ColumnReader::PointReader p_bp{r, *c_bp};
  duckdb::Vector o_rle{duckdb::LogicalType::BIGINT, 1};
  duckdb::Vector o_bp{duckdb::LogicalType::INTEGER, 1};
  for (uint64_t g = 1; g < kRows; g += 333) {
    duckdb::FlatVector::ValidityMutable(o_rle).Reset();
    duckdb::FlatVector::ValidityMutable(o_bp).Reset();
    ASSERT_TRUE(p_rle.FetchRow(g, o_rle, 0));
    EXPECT_EQ(duckdb::FlatVector::GetData<int64_t>(o_rle)[0],
              static_cast<int64_t>(g / 50));
    EXPECT_EQ(p_bp.FetchRow(g, o_bp, 0), g % 6 != 0);
  }
}

TEST_F(ColCodecsTest, RleLookupsInEveryOrder) {
  constexpr uint64_t kRows = 40000;
  constexpr irs::field_id kRle = 1;
  constexpr irs::field_id kNumeric = 2;
  const auto rle_value = [](uint64_t g) {
    return static_cast<int64_t>(g * g / 1000003);
  };
  const auto numeric_value = [](uint64_t g) {
    return static_cast<int64_t>(1'700'000'000'000'000LL + (g / 4) * 1000 +
                                (g / 4) * 2654435761ULL % 3 * 100);
  };
  irs::MemoryDirectory dir;
  {
    irs::ColWriter w{dir, kSeg, Db()};
    auto& rle =
      w.OpenColumn(kRle, duckdb::LogicalType::BIGINT,
                   {.row_group_size = 4096,
                    .compression = duckdb::CompressionType::COMPRESSION_RLE});
    auto& numeric = w.OpenColumn(kNumeric, duckdb::LogicalType::BIGINT,
                                 {.row_group_size = 16384});
    for (uint64_t pos = 0; pos < kRows;) {
      const auto take = std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v_rle{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      duckdb::Vector v_num{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      auto* d_rle = duckdb::FlatVector::GetDataMutable<int64_t>(v_rle);
      auto* d_num = duckdb::FlatVector::GetDataMutable<int64_t>(v_num);
      for (uint64_t k = 0; k < take; ++k) {
        d_rle[k] = rle_value(pos + k);
        d_num[k] = numeric_value(pos + k);
      }
      duckdb::FlatVector::SetSize(v_rle, take);
      duckdb::FlatVector::SetSize(v_num, take);
      rle.Append(v_rle, take);
      numeric.Append(v_num, take);
      pos += take;
    }
    ASSERT_TRUE(w.Commit(0));
  }
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* c_rle = r.Column(kRle);
  const auto* c_num = r.Column(kNumeric);
  ASSERT_NE(c_rle, nullptr);
  ASSERT_NE(c_num, nullptr);
  ASSERT_GT(c_rle->DataBlocks().size(), 5);
  for (const auto& block : c_rle->DataBlocks()) {
    ASSERT_EQ(block.codec->type, duckdb::CompressionType::COMPRESSION_RLE);
  }
  ASSERT_EQ(c_num->DataBlocks().front().codec->type,
            duckdb::CompressionType::COMPRESSION_COL_NUMERIC);

  std::vector<std::vector<uint64_t>> orders(5);
  for (uint64_t g = 0; g < kRows; g += 7) {
    orders[0].push_back(g);
  }
  for (uint64_t g = kRows; g-- > 0;) {
    if (g % 13 == 0) {
      orders[1].push_back(g);
    }
  }
  std::mt19937_64 rng{5};
  for (int i = 0; i < 3000; ++i) {
    orders[2].push_back(rng() % kRows);
  }
  for (uint64_t g = 100; g < kRows; g += 977) {
    orders[3].insert(orders[3].end(), {g, g, g + 1, g - 1, g + 2});
  }
  for (uint64_t g = 0; g < 4096; g += 61) {
    orders[4].insert(orders[4].end(), {g, kRows - 1 - g});
  }
  duckdb::Vector out{duckdb::LogicalType::BIGINT, 1};
  for (const auto& order : orders) {
    irs::ColumnReader::PointReader cursor{r, *c_rle};
    for (const auto g : order) {
      duckdb::FlatVector::ValidityMutable(out).Reset();
      ASSERT_TRUE(cursor.FetchRow(g, out, 0)) << g;
      ASSERT_EQ(duckdb::FlatVector::GetData<int64_t>(out)[0], rle_value(g))
        << g;
    }
  }

  irs::ReadContext ctx{r};
  auto rle_segment = c_rle->OpenSegment(0, ctx);
  auto numeric_segment = c_num->OpenSegment(0, ctx);
  irs::codecs::ColumnFetchState rle_state;
  irs::codecs::ColumnFetchState numeric_state;
  const auto fetch = [&](const irs::codecs::ColumnSegment& segment,
                         irs::codecs::ColumnFetchState& state, uint64_t g) {
    const duckdb::idx_t row = g;
    segment.function.fetch_rows(
      segment, state, &row, 1, out,
      *duckdb::FlatVector::IncrementalSelectionVector(), 0);
    return duckdb::FlatVector::GetData<int64_t>(out)[0];
  };
  for (uint64_t g = 0; g < 4096; g += 37) {
    ASSERT_EQ(fetch(*rle_segment, rle_state, g), rle_value(g)) << g;
    ASSERT_EQ(fetch(*numeric_segment, numeric_state, g), numeric_value(g)) << g;
  }
}

TEST_F(ColCodecsTest, CachedDictionariesUnderConcurrentReadersAndEviction) {
  constexpr uint64_t kRows = 120000;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 97 == 0) {
      return std::nullopt;
    }
    return absl::StrCat("https://example.org/item/", (g * 7919) % 3001,
                        "/page");
  };
  irs::MemoryDirectory dir;
  Write(dir, duckdb::CompressionType::COMPRESSION_DICT_LZ4, {}, kRows, 8192,
        value);
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  const auto keys = CacheKeys(r, *col);
  ASSERT_GT(keys.size(), 8);
  auto& cache = Db().GetObjectCache();
  const auto matches = [&](duckdb::Vector& out, duckdb::idx_t k, uint64_t g) {
    const auto expected = value(g);
    const auto& validity = duckdb::FlatVector::Validity(out);
    if (!expected) {
      return !validity.RowIsValid(k);
    }
    return validity.RowIsValid(k) &&
           duckdb::FlatVector::GetData<duckdb::string_t>(out)[k].GetString() ==
             *expected;
  };
  std::atomic<bool> stop{false};
  std::atomic<uint64_t> mismatches{0};
  std::vector<std::thread> readers;
  for (uint64_t t = 0; t < 8; ++t) {
    readers.emplace_back([&, t] {
      std::mt19937_64 rng{t};
      for (int round = 0; round < 6; ++round) {
        irs::ReadContext ctx{r};
        auto state = col->InitScan(ctx);
        for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
          const auto take =
            std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE);
          duckdb::Vector out{duckdb::LogicalType::VARCHAR,
                             STANDARD_VECTOR_SIZE};
          col->Scan(state, out, take);
          out.Flatten(take);
          for (duckdb::idx_t k = 0; k < take; ++k) {
            mismatches += !matches(out, k, pos + k);
          }
        }
        irs::ColumnReader::PointReader cursor{r, *col};
        duckdb::Vector one{duckdb::LogicalType::VARCHAR, 1};
        for (int i = 0; i < 2000; ++i) {
          const auto g = rng() % kRows;
          duckdb::FlatVector::ValidityMutable(one).Reset();
          cursor.FetchRow(g, one, 0);
          mismatches += !matches(one, 0, g);
        }
      }
    });
  }
  std::thread evictor{[&] {
    while (!stop.load()) {
      for (const auto& key : keys) {
        cache.Delete(key);
      }
      std::this_thread::yield();
    }
  }};
  for (auto& reader : readers) {
    reader.join();
  }
  stop.store(true);
  evictor.join();
  EXPECT_EQ(0, mismatches.load());
}

TEST(ColCodecNames, ColumnstoreCompressionTypesRoundTrip) {
  const std::pair<duckdb::CompressionType, std::string_view> names[] = {
    {duckdb::CompressionType::COMPRESSION_DICT_LZ4, "DICT_LZ4"},
    {duckdb::CompressionType::COMPRESSION_DICT_ZSTD, "DICT_ZSTD"},
    {duckdb::CompressionType::COMPRESSION_LZ4, "LZ4"},
    {duckdb::CompressionType::COMPRESSION_COL_DICT_FSST, "DICT_FSST"},
    {duckdb::CompressionType::COMPRESSION_COL_FSST, "FSST"},
    {duckdb::CompressionType::COMPRESSION_DICT_ZXC, "DICT_ZXC"},
    {duckdb::CompressionType::COMPRESSION_ZXC, "ZXC"},
    {duckdb::CompressionType::COMPRESSION_COL_ZSTD, "ZSTD"},
    {duckdb::CompressionType::COMPRESSION_COL_DICT_FSST_PLUS, "DICT_FSST_PLUS"},
    {duckdb::CompressionType::COMPRESSION_COL_FSST_PLUS, "FSST_PLUS"},
    {duckdb::CompressionType::COMPRESSION_COL_DICT_ONPAIR, "DICT_ONPAIR"},
    {duckdb::CompressionType::COMPRESSION_COL_ONPAIR, "ONPAIR"},
    {duckdb::CompressionType::COMPRESSION_COL_DICT_FSST12, "DICT_FSST12"},
    {duckdb::CompressionType::COMPRESSION_COL_FSST12, "FSST12"},
  };
  EXPECT_EQ(
    static_cast<size_t>(duckdb::CompressionType::COMPRESSION_SERENEDB_COUNT) -
      static_cast<size_t>(duckdb::CompressionType::COMPRESSION_DICT_LZ4),
    std::size(names) + 2);
  EXPECT_TRUE(duckdb::IsSereneDBCompressionType(
    duckdb::CompressionType::COMPRESSION_COL_SEQUENCE));
  EXPECT_EQ(duckdb::CompressionTypeToString(
              duckdb::CompressionType::COMPRESSION_COL_SEQUENCE),
            "Sequence");
  EXPECT_ANY_THROW(
    duckdb::EnumUtil::FromString<duckdb::CompressionType>("sequence"));
  EXPECT_TRUE(duckdb::IsSereneDBCompressionType(
    duckdb::CompressionType::COMPRESSION_COL_NUMERIC));
  EXPECT_EQ(duckdb::CompressionTypeToString(
              duckdb::CompressionType::COMPRESSION_COL_NUMERIC),
            "Numeric");
  EXPECT_ANY_THROW(
    duckdb::EnumUtil::FromString<duckdb::CompressionType>("numeric"));
  for (const auto& [type, name] : names) {
    SCOPED_TRACE(name);
    EXPECT_TRUE(duckdb::IsSereneDBCompressionType(type));
    EXPECT_EQ(duckdb::EnumUtil::ToString(type), name);
    EXPECT_EQ(duckdb::CompressionTypeToString(type), name);
  }
  const std::pair<std::string_view, duckdb::CompressionType> parsed[] = {
    {"dict_lz4", duckdb::CompressionType::COMPRESSION_DICT_LZ4},
    {"dict_zstd", duckdb::CompressionType::COMPRESSION_DICT_ZSTD},
    {"lz4", duckdb::CompressionType::COMPRESSION_LZ4},
    {"dict_zxc", duckdb::CompressionType::COMPRESSION_DICT_ZXC},
    {"zxc", duckdb::CompressionType::COMPRESSION_ZXC},
    {"dict_fsst", duckdb::CompressionType::COMPRESSION_DICT_FSST},
    {"fsst", duckdb::CompressionType::COMPRESSION_FSST},
    {"zstd", duckdb::CompressionType::COMPRESSION_ZSTD},
    {"dict_fsst_plus", duckdb::CompressionType::COMPRESSION_COL_DICT_FSST_PLUS},
    {"fsst_plus", duckdb::CompressionType::COMPRESSION_COL_FSST_PLUS},
    {"dict_onpair", duckdb::CompressionType::COMPRESSION_COL_DICT_ONPAIR},
    {"onpair", duckdb::CompressionType::COMPRESSION_COL_ONPAIR},
    {"dict_fsst12", duckdb::CompressionType::COMPRESSION_COL_DICT_FSST12},
    {"fsst12", duckdb::CompressionType::COMPRESSION_COL_FSST12},
  };
  for (const auto& [name, type] : parsed) {
    SCOPED_TRACE(name);
    EXPECT_EQ(
      duckdb::EnumUtil::FromString<duckdb::CompressionType>(std::string{name}),
      type);
  }
}

TEST_F(ColCodecsTest, CorruptedDictionaryCodesAreRejected) {
  const auto path = test_dir() / "col_codecs_corrupt_codes";
  std::filesystem::create_directories(path);
  const Value random_codes = [](uint64_t g) -> std::optional<std::string> {
    if (g % 9 == 0) {
      return std::nullopt;
    }
    return "value-" + std::to_string((g * 0x9E3779B97F4A7C15ULL >> 40) % 1000);
  };
  uint64_t offset = 0;
  uint64_t size = 0;
  {
    irs::MMapDirectory dir{path};
    Write(dir, duckdb::CompressionType::COMPRESSION_DICT_LZ4, {}, 5000, 2048,
          random_codes);
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    ASSERT_FALSE(col->DataBlocks().empty());
    offset = col->DataBlocks().front().file_offset;
    size = col->DataBlocks().front().byte_size;
  }
  const auto file = path / irs::FileName(kSeg);
  std::string bytes;
  {
    std::ifstream in{file, std::ios::binary};
    bytes.assign(std::istreambuf_iterator<char>{in}, {});
  }
  ASSERT_LE(offset + size, bytes.size());
  auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
  const auto h = irs::codecs::Header::Parse(block, size);
  ASSERT_EQ(h.shape, irs::codecs::Shape::Dedup);
  auto codes = irs::codecs::NumericHeader::Parse(block + h.off_codes,
                                                 h.off_symtab - h.off_codes);
  codes.scale = h.entry_count;
  duckdb::Store(codes, block + h.off_codes);
  {
    std::ofstream out{file, std::ios::binary | std::ios::trunc};
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  }
  irs::MMapDirectory dir{path};
  const auto scan = [&] {
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    auto state = col->InitScan(r.Ctx());
    duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    col->Scan(state, out, STANDARD_VECTOR_SIZE);
  };
#ifdef SDB_DEV
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(scan(), "corrupted row codes");
#else
  EXPECT_ANY_THROW(scan());
#endif
}

TEST_F(ColCodecsTest, CorruptedSegmentSizesAreRejected) {
  struct Case {
    std::string_view name;
    void (*corrupt)(irs::codecs::Header&);
    const char* error;
  };
  const Case cases[] = {
    {"frames", [](irs::codecs::Header& h) { h.off_frames = 0; },
     "corrupted segment header"},
    {"entries",
     [](irs::codecs::Header& h) {
       h.length_width = 0;
       h.entry_count = h.row_count + 2;
     },
     "corrupted segment header"},
    {"raw_bytes",
     [](irs::codecs::Header& h) { h.raw_bytes = uint64_t{1} << 46; },
     "corrupted frame table"},
  };
  const Value values = [](uint64_t g) -> std::optional<std::string> {
    return "value-" + std::to_string((g * 0x9E3779B97F4A7C15ULL >> 40) % 1000);
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    const auto path =
      test_dir() / ("col_codecs_corrupt_sizes_" + std::string{c.name});
    std::filesystem::create_directories(path);
    uint64_t offset = 0;
    uint64_t size = 0;
    {
      irs::MMapDirectory dir{path};
      Write(dir, duckdb::CompressionType::COMPRESSION_DICT_LZ4, {}, 5000, 2048,
            values);
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      offset = col->DataBlocks().front().file_offset;
      size = col->DataBlocks().front().byte_size;
    }
    const auto file = path / irs::FileName(kSeg);
    std::string bytes;
    {
      std::ifstream in{file, std::ios::binary};
      bytes.assign(std::istreambuf_iterator<char>{in}, {});
    }
    ASSERT_LE(offset + size, bytes.size());
    auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
    auto h = irs::codecs::Header::Parse(block, size);
    c.corrupt(h);
    duckdb::Store(h, block);
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    irs::MMapDirectory dir{path};
    const auto scan = [&] {
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
    };
#ifdef SDB_DEV
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(scan(), c.error);
#else
    EXPECT_ANY_THROW(scan());
#endif
  }
}

TEST_F(ColCodecsTest, CorruptedSegmentDictionariesAreRejected) {
  struct Case {
    std::string_view name;
    void (*corrupt)(irs::codecs::Header&, duckdb::data_ptr_t);
    const char* error;
  };
  const Case cases[] = {
    {"size",
     [](irs::codecs::Header& h, duckdb::data_ptr_t) {
       h.dictionary_size = irs::codecs::kMaxDictionaryBytes + 1;
     },
     "corrupted segment header"},
    {"bytes",
     [](irs::codecs::Header& h, duckdb::data_ptr_t block) {
       std::memset(block + h.off_symtab, 0xFF, h.symtab_size);
     },
     "corrupted dictionary"},
  };
  std::vector<std::string> templates;
  for (uint64_t t = 0; t < 200; ++t) {
    templates.emplace_back(Pseudo(t, 100));
  }
  const Value values = [&](uint64_t g) -> std::optional<std::string> {
    return templates[(g * 0x9E3779B97F4A7C15ULL >> 40) % templates.size()] +
           std::to_string(g);
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    const auto path =
      test_dir() / ("col_codecs_corrupt_dictionary_" + std::string{c.name});
    std::filesystem::create_directories(path);
    uint64_t offset = 0;
    uint64_t size = 0;
    {
      irs::MMapDirectory dir{path};
      Write(dir, duckdb::CompressionType::COMPRESSION_LZ4, {}, 20000,
            DEFAULT_ROW_GROUP_SIZE, values);
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      offset = col->DataBlocks().front().file_offset;
      size = col->DataBlocks().front().byte_size;
    }
    const auto file = path / irs::FileName(kSeg);
    std::string bytes;
    {
      std::ifstream in{file, std::ios::binary};
      bytes.assign(std::istreambuf_iterator<char>{in}, {});
    }
    ASSERT_LE(offset + size, bytes.size());
    auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
    auto h = irs::codecs::Header::Parse(block, size);
    ASSERT_EQ(h.flags, irs::codecs::FrameDictionary::Local);
    c.corrupt(h, block);
    duckdb::Store(h, block);
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    irs::MMapDirectory dir{path};
    const auto scan = [&] {
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
    };
    const auto fetch = [&] {
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::RANDOM};
      const auto* col = r.Column(kField);
      irs::ColumnReader::PointReader cursor{r, *col};
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, 1};
      cursor.FetchRow(7, out, 0);
    };
#ifdef SDB_DEV
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(scan(), c.error);
    EXPECT_DEATH(fetch(), c.error);
#else
    EXPECT_ANY_THROW(scan());
    EXPECT_ANY_THROW(fetch());
#endif
  }
}

TEST_F(ColCodecsTest, CorruptedFsstPlusPrefixesAreRejected) {
  struct Case {
    std::string_view name;
    void (*corrupt)(irs::codecs::Header&, duckdb::data_ptr_t);
    const char* error;
  };
  static constexpr auto kPrefixes =
    +[](irs::codecs::Header& h, duckdb::data_ptr_t block) {
      return block + h.off_symtab + h.dictionary_size;
    };
  const Case cases[] = {
    {"table size",
     [](irs::codecs::Header& h, duckdb::data_ptr_t) {
       h.dictionary_size = h.symtab_size;
     },
     "corrupted segment header"},
    {"prefix count",
     [](irs::codecs::Header& h, duckdb::data_ptr_t block) {
       duckdb::Store<uint32_t>(h.symtab_size, kPrefixes(h, block));
     },
     "corrupted symbol table"},
    {"prefix end",
     [](irs::codecs::Header& h, duckdb::data_ptr_t block) {
       duckdb::Store<uint32_t>(0, kPrefixes(h, block) + sizeof(uint32_t));
     },
     "corrupted symbol table"},
  };
  const Value values = [](uint64_t g) -> std::optional<std::string> {
    return "https://example.org/users/" + std::to_string(g % 5000) + "/profile";
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    const auto path =
      test_dir() / ("col_codecs_corrupt_fsst_plus_" + std::string{c.name});
    std::filesystem::create_directories(path);
    uint64_t offset = 0;
    uint64_t size = 0;
    {
      irs::MMapDirectory dir{path};
      Write(dir, duckdb::CompressionType::COMPRESSION_COL_FSST_PLUS, {}, 20000,
            DEFAULT_ROW_GROUP_SIZE, values);
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      offset = col->DataBlocks().front().file_offset;
      size = col->DataBlocks().front().byte_size;
    }
    const auto file = path / irs::FileName(kSeg);
    std::string bytes;
    {
      std::ifstream in{file, std::ios::binary};
      bytes.assign(std::istreambuf_iterator<char>{in}, {});
    }
    ASSERT_LE(offset + size, bytes.size());
    auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
    auto h = irs::codecs::Header::Parse(block, size);
    ASSERT_EQ(h.codec, irs::codecs::ByteCodec::FsstPlus);
    ASSERT_GT(duckdb::Load<uint32_t>(kPrefixes(h, block)), 0u);
    c.corrupt(h, block);
    duckdb::Store(h, block);
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    irs::MMapDirectory dir{path};
    const auto scan = [&] {
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
    };
    const auto fetch = [&] {
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::RANDOM};
      const auto* col = r.Column(kField);
      irs::ColumnReader::PointReader cursor{r, *col};
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, 1};
      cursor.FetchRow(7, out, 0);
    };
#ifdef SDB_DEV
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(scan(), c.error);
    EXPECT_DEATH(fetch(), c.error);
#else
    EXPECT_ANY_THROW(scan());
    EXPECT_ANY_THROW(fetch());
#endif
  }
}

TEST_F(ColCodecsTest, CorruptedOnPairDictionariesAreRejected) {
  struct Case {
    std::string_view name;
    void (*corrupt)(irs::codecs::Header&, duckdb::data_ptr_t);
    const char* error;
  };
  const Case cases[] = {
    {"code width",
     [](irs::codecs::Header& h, duckdb::data_ptr_t block) {
       block[h.off_symtab] = 17;
     },
     "corrupted symbol table"},
    {"token count",
     [](irs::codecs::Header& h, duckdb::data_ptr_t block) {
       duckdb::Store<uint32_t>(0, block + h.off_symtab + sizeof(uint32_t));
     },
     "corrupted symbol table"},
    {"token lengths",
     [](irs::codecs::Header& h, duckdb::data_ptr_t block) {
       block[h.off_symtab + irs::codecs::kOnPairSideHeader] ^= 0x0F;
     },
     "corrupted symbol table"},
  };
  const Value values = [](uint64_t g) -> std::optional<std::string> {
    return "https://example.org/users/" + std::to_string(g % 5000) + "/profile";
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    const auto path =
      test_dir() / ("col_codecs_corrupt_onpair_" + std::string{c.name});
    std::filesystem::create_directories(path);
    uint64_t offset = 0;
    uint64_t size = 0;
    {
      irs::MMapDirectory dir{path};
      Write(dir, duckdb::CompressionType::COMPRESSION_COL_ONPAIR, {}, 20000,
            DEFAULT_ROW_GROUP_SIZE, values);
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      offset = col->DataBlocks().front().file_offset;
      size = col->DataBlocks().front().byte_size;
    }
    const auto file = path / irs::FileName(kSeg);
    std::string bytes;
    {
      std::ifstream in{file, std::ios::binary};
      bytes.assign(std::istreambuf_iterator<char>{in}, {});
    }
    ASSERT_LE(offset + size, bytes.size());
    auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
    auto h = irs::codecs::Header::Parse(block, size);
    ASSERT_EQ(h.codec, irs::codecs::ByteCodec::OnPair);
    c.corrupt(h, block);
    duckdb::Store(h, block);
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    irs::MMapDirectory dir{path};
    const auto scan = [&] {
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
    };
    const auto fetch = [&] {
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::RANDOM};
      const auto* col = r.Column(kField);
      irs::ColumnReader::PointReader cursor{r, *col};
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, 1};
      cursor.FetchRow(7, out, 0);
    };
#ifdef SDB_DEV
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(scan(), c.error);
    EXPECT_DEATH(fetch(), c.error);
#else
    EXPECT_ANY_THROW(scan());
    EXPECT_ANY_THROW(fetch());
#endif
  }
}

TEST_F(ColCodecsTest, CorruptedFsst12TablesAreRejected) {
  struct Case {
    std::string_view name;
    void (*corrupt)(irs::codecs::Header&, duckdb::data_ptr_t);
  };
  const Case cases[] = {
    {"version",
     [](irs::codecs::Header& h, duckdb::data_ptr_t block) {
       duckdb::Store<uint64_t>(0, block + h.off_symtab);
     }},
    {"symbol count",
     [](irs::codecs::Header& h, duckdb::data_ptr_t block) {
       duckdb::Store<uint16_t>(0xFFFF, block + h.off_symtab + 8);
     }},
    {"symbol length",
     [](irs::codecs::Header& h, duckdb::data_ptr_t block) {
       block[h.off_symtab + 24] = 9;
     }},
  };
  const Value values = [](uint64_t g) -> std::optional<std::string> {
    return "https://example.org/users/" + std::to_string(g % 5000) + "/profile";
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    const auto path =
      test_dir() / ("col_codecs_corrupt_fsst12_" + std::string{c.name});
    std::filesystem::create_directories(path);
    uint64_t offset = 0;
    uint64_t size = 0;
    {
      irs::MMapDirectory dir{path};
      Write(dir, duckdb::CompressionType::COMPRESSION_COL_FSST12, {}, 20000,
            DEFAULT_ROW_GROUP_SIZE, values);
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      offset = col->DataBlocks().front().file_offset;
      size = col->DataBlocks().front().byte_size;
    }
    const auto file = path / irs::FileName(kSeg);
    std::string bytes;
    {
      std::ifstream in{file, std::ios::binary};
      bytes.assign(std::istreambuf_iterator<char>{in}, {});
    }
    ASSERT_LE(offset + size, bytes.size());
    auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
    auto h = irs::codecs::Header::Parse(block, size);
    ASSERT_EQ(h.codec, irs::codecs::ByteCodec::Fsst12);
    ASSERT_GT(h.symtab_size, 24u);
    c.corrupt(h, block);
    duckdb::Store(h, block);
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    irs::MMapDirectory dir{path};
    const auto scan = [&] {
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
    };
    const auto fetch = [&] {
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::RANDOM};
      const auto* col = r.Column(kField);
      irs::ColumnReader::PointReader cursor{r, *col};
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, 1};
      cursor.FetchRow(7, out, 0);
    };
#ifdef SDB_DEV
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(scan(), "corrupted symbol table");
    EXPECT_DEATH(fetch(), "corrupted symbol table");
#else
    EXPECT_ANY_THROW(scan());
    EXPECT_ANY_THROW(fetch());
#endif
  }
}

TEST_F(ColCodecsTest, CorruptedSegmentCountsAreRejectedByPointReads) {
  struct Case {
    std::string_view name;
    duckdb::CompressionType codec;
    void (*corrupt)(irs::codecs::Header&);
  };
  const Case cases[] = {
    {"dedup_entries", duckdb::CompressionType::COMPRESSION_DICT_LZ4,
     [](irs::codecs::Header& h) {
       h.length_width = 0;
       h.entry_count = h.row_count + 2;
     }},
    {"plain_entries", duckdb::CompressionType::COMPRESSION_LZ4,
     [](irs::codecs::Header& h) { h.entry_count = 0; }},
    {"rows", duckdb::CompressionType::COMPRESSION_DICT_LZ4,
     [](irs::codecs::Header& h) { ++h.row_count; }},
  };
  const Value values = [](uint64_t g) -> std::optional<std::string> {
    return "value-" + std::to_string(g * 0x9E3779B97F4A7C15ULL >> 40);
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    const auto path =
      test_dir() / ("col_codecs_corrupt_counts_" + std::string{c.name});
    std::filesystem::create_directories(path);
    uint64_t offset = 0;
    uint64_t size = 0;
    {
      irs::MMapDirectory dir{path};
      Write(dir, c.codec, {}, 5000, 2048, values);
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      offset = col->DataBlocks().front().file_offset;
      size = col->DataBlocks().front().byte_size;
    }
    const auto file = path / irs::FileName(kSeg);
    std::string bytes;
    {
      std::ifstream in{file, std::ios::binary};
      bytes.assign(std::istreambuf_iterator<char>{in}, {});
    }
    ASSERT_LE(offset + size, bytes.size());
    auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
    auto h = irs::codecs::Header::Parse(block, size);
    c.corrupt(h);
    duckdb::Store(h, block);
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    irs::MMapDirectory dir{path};
    const auto fetch = [&] {
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      irs::ColumnReader::PointReader cursor{r, *col};
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, 1};
      cursor.FetchRow(0, out, 0);
    };
#ifdef SDB_DEV
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(fetch(), "corrupted segment header");
#else
    EXPECT_ANY_THROW(fetch());
#endif
  }
}

const Value kTripledValues = [](uint64_t g) -> std::optional<std::string> {
  return "v" + std::to_string(g / 3);
};

TEST_F(ColCodecsTest, DictionaryCodesUseTheNumericCodec) {
  for (const auto codec : {duckdb::CompressionType::COMPRESSION_AUTO,
                           duckdb::CompressionType::COMPRESSION_DICT_FSST,
                           duckdb::CompressionType::COMPRESSION_DICT_LZ4}) {
    SCOPED_TRACE(duckdb::EnumUtil::ToString(codec));
    irs::MemoryDirectory dir{};
    Write(dir, codec, {}, 60000, 16384, kTripledValues);
    Verify(dir, codec, 60000, kTripledValues);
    const auto transforms = SegmentInfo(dir, "codes_transform");
    EXPECT_TRUE(std::ranges::any_of(
      transforms, [](const auto& transform) { return !transform.empty(); }))
      << ::testing::PrintToString(transforms);
  }
}

const Value kShortCodeRuns = [](uint64_t g) -> std::optional<std::string> {
  return "s" + std::to_string(std::mt19937_64{g / 2}() % 16);
};

TEST_F(ColCodecsTest, CodesInShortRunsAreNotRunPacked) {
  constexpr uint64_t kRows = 60000;
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(static_cast<int>(tier));
    irs::MemoryDirectory dir{};
    Write(dir, duckdb::CompressionType::COMPRESSION_DICT_LZ4, {.tier = tier},
          kRows, 16384, kShortCodeRuns);
    Verify(dir, duckdb::CompressionType::COMPRESSION_DICT_LZ4, kRows,
           kShortCodeRuns);
    for (const auto& transform : SegmentInfo(dir, "codes_transform")) {
      EXPECT_NE(transform, "rle_ffor");
    }
  }
}

TEST_F(ColCodecsTest, CodesThatTurnIntoRunsAreRunPacked) {
  constexpr uint64_t kSwitch = 400000;
  constexpr uint64_t kRows = 600000;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    const auto key = g < kSwitch ? g : g / 128;
    return "v" + std::to_string(std::mt19937_64{key}() % 1000);
  };
  irs::MemoryDirectory dir{};
  Write(dir, duckdb::CompressionType::COMPRESSION_DICT_LZ4,
        {.segment_target = 16 * 1024}, kRows, 65536, value);
  Verify(dir, duckdb::CompressionType::COMPRESSION_DICT_LZ4, kRows, value);
  const auto transforms = SegmentInfo(dir, "codes_transform");
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->DataBlocks().size(), transforms.size());
  uint64_t row = 0;
  size_t runs = 0;
  for (size_t b = 0; b < transforms.size(); ++b) {
    if (row >= kSwitch) {
      ++runs;
      EXPECT_TRUE(transforms[b].starts_with("rle"))
        << "block at row " << row << ": " << transforms[b];
    }
    row += col->DataBlocks()[b].tuple_count;
  }
  EXPECT_GT(runs, 0u);
}

TEST_F(ColCodecsTest, NumericCodesFilterTheValues) {
  constexpr uint64_t kRows = 30000;
  irs::MemoryDirectory dir{};
  Write(dir, duckdb::CompressionType::COMPRESSION_DICT_LZ4, {}, kRows, 8192,
        kTripledValues);
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  duckdb::Connection con{_db};
  for (const std::string key : {"v0", "v7", "v4000", "v9999", "zz"}) {
    for (const auto cmp : {duckdb::ExpressionType::COMPARE_EQUAL,
                           duckdb::ExpressionType::COMPARE_NOTEQUAL}) {
      const duckdb::ExpressionFilter filter{
        duckdb::BoundComparisonExpression::Create(
          cmp,
          duckdb::make_uniq<duckdb::BoundReferenceExpression>(
            duckdb::LogicalType::VARCHAR, 0),
          duckdb::make_uniq<duckdb::BoundConstantExpression>(
            duckdb::Value{key}))};
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
        duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
        const auto kept =
          col->GatherFilter(state, anchor, span, sel, span, filter,
                            *filter_state, irs::NullCheckKind::None, out);
        duckdb::idx_t next = 0;
        for (duckdb::idx_t i = 0; i < span; ++i) {
          const auto v = kTripledValues(anchor + i);
          const bool expected =
            v && (cmp == duckdb::ExpressionType::COMPARE_EQUAL ? *v == key
                                                               : *v != key);
          const bool got = next < kept && sel.get_index(next) == i;
          next += got ? 1 : 0;
          ASSERT_EQ(got, expected) << "row " << anchor + i << " key " << key;
        }
      }
    }
  }
}

TEST_F(ColCodecsTest, CorruptedNumericCodesAreRejected) {
  for (const bool leaf : {false, true}) {
    SCOPED_TRACE(leaf ? "leaf" : "row count");
    const auto path = test_dir() / (leaf ? "col_codecs_corrupt_codes_leaf"
                                         : "col_codecs_corrupt_codes_rows");
    std::filesystem::create_directories(path);
    uint64_t offset = 0;
    uint64_t size = 0;
    {
      irs::MMapDirectory dir{path};
      Write(dir, duckdb::CompressionType::COMPRESSION_DICT_LZ4, {}, 30000,
            30000, kTripledValues);
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      offset = col->DataBlocks().front().file_offset;
      size = col->DataBlocks().front().byte_size;
    }
    const auto file = path / irs::FileName(kSeg);
    std::string bytes;
    {
      std::ifstream in{file, std::ios::binary};
      bytes.assign(std::istreambuf_iterator<char>{in}, {});
    }
    ASSERT_LE(offset + size, bytes.size());
    auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
    const auto h = irs::codecs::Header::Parse(block, size);
    ASSERT_EQ(h.shape, irs::codecs::Shape::Dedup);
    if (leaf) {
      auto codes = irs::codecs::NumericHeader::Parse(
        block + h.off_codes, h.off_symtab - h.off_codes);
      codes.leaf = irs::codecs::NumericLeaf::Zstd;
      duckdb::Store(codes, block + h.off_codes);
    } else {
      duckdb::Store<uint32_t>(h.row_count + 1, block + h.off_codes + 8);
    }
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    irs::MMapDirectory dir{path};
    const auto scan = [&] {
      irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
      const auto* col = r.Column(kField);
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
    };
#ifdef SDB_DEV
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(scan(), "corrupted");
#else
    EXPECT_ANY_THROW(scan());
#endif
  }
}

TEST_F(ColCodecsTest, CorruptedSegmentHeadersAreIndexCorruption) {
  struct Case {
    std::string_view name;
    duckdb::LogicalType type;
    duckdb::CompressionType codec;
  };
  const Case cases[] = {
    {"rle", duckdb::LogicalType::BIGINT,
     duckdb::CompressionType::COMPRESSION_RLE},
    {"bitpacking", duckdb::LogicalType::INTEGER,
     duckdb::CompressionType::COMPRESSION_BITPACKING},
    {"alp", duckdb::LogicalType::DOUBLE,
     duckdb::CompressionType::COMPRESSION_ALP},
    {"alprd", duckdb::LogicalType::DOUBLE,
     duckdb::CompressionType::COMPRESSION_ALPRD},
    {"roaring", duckdb::LogicalType::BOOLEAN,
     duckdb::CompressionType::COMPRESSION_ROARING},
    {"uncompressed", duckdb::LogicalType::VARCHAR,
     duckdb::CompressionType::COMPRESSION_UNCOMPRESSED},
  };
  constexpr uint64_t kRows = 5000;
  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    const auto path =
      test_dir() / absl::StrCat("col_codecs_corrupt_header_", c.name);
    std::filesystem::create_directories(path);
    uint64_t offset = 0;
    uint64_t size = 0;
    {
      irs::MMapDirectory dir{path};
      irs::ColWriter w{dir, kSeg, Db()};
      auto& cw = w.OpenColumn(kField, c.type,
                              {.row_group_size = 4096, .compression = c.codec});
      for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
        const auto take = std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE);
        duckdb::Vector v{c.type, STANDARD_VECTOR_SIZE};
        for (uint64_t k = 0; k < take; ++k) {
          const auto g = static_cast<int64_t>(pos + k);
          v.SetValue(k, c.type.id() == duckdb::LogicalTypeId::VARCHAR
                          ? duckdb::Value{absl::StrCat("row-", g % 97)}
                          : duckdb::Value::BIGINT(g / 7).DefaultCastAs(c.type));
        }
        duckdb::FlatVector::SetSize(v, take);
        cw.Append(v, take);
      }
      ASSERT_TRUE(w.Commit(0));
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
    ASSERT_GE(size, sizeof(uint64_t));
    ASSERT_LE(offset + size, bytes.size());
    std::memset(bytes.data() + offset, 0xFF, sizeof(uint64_t));
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    irs::MMapDirectory dir{path};
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    try {
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
      ADD_FAILURE() << "the scan accepted a corrupted segment";
    } catch (const irs::SqlException& e) {
      EXPECT_EQ(e.error().errcode, ERRCODE_INDEX_CORRUPTED) << e.what();
    }
  }
}

TEST_F(ColCodecsTest, CorruptedSegmentBodiesAreIndexCorruption) {
  struct Case {
    std::string_view name;
    duckdb::LogicalType type;
    duckdb::CompressionType codec;
    std::function<duckdb::Value(int64_t)> value;
    std::function<void(std::string&, uint64_t, uint64_t)> corrupt;
    std::string_view error;
  };
  const auto store32 = [](std::string& bytes, uint64_t at, uint32_t v) {
    absl::little_endian::Store32(bytes.data() + at, v);
  };
  const auto store64 = [](std::string& bytes, uint64_t at, uint64_t v) {
    absl::little_endian::Store64(bytes.data() + at, v);
  };
  const auto runs = [](int64_t g) { return duckdb::Value::BIGINT(g / 7); };
  const auto sparse = [](int64_t g) {
    return duckdb::Value::BOOLEAN(g % 100 == 0);
  };
  const std::string long_text(5000, 'q');
  const auto overflowing = [&](int64_t g) {
    return duckdb::Value{absl::StrCat("row-", g, "-", long_text)};
  };
  const Case cases[] = {
    {"rle entry count", duckdb::LogicalType::BIGINT,
     duckdb::CompressionType::COMPRESSION_RLE, runs,
     [&](std::string& b, uint64_t at, uint64_t) {
       store32(b, at + 8, std::numeric_limits<uint32_t>::max());
       b[at + 14] = 0;
       b[at + 15] = 0;
     },
     "Corrupted RLE segment: entry count exceeds the segment capacity"},
    {"rle misaligned counts", duckdb::LogicalType::BIGINT,
     duckdb::CompressionType::COMPRESSION_RLE, runs,
     [&](std::string& b, uint64_t at, uint64_t) {
       store64(b, at, (uint64_t{1} << 63) | 25);
       b[at + 14] = 0;
     },
     "Corrupted RLE segment: rle_count_offset is corrupted"},
    {"roaring metadata offset", duckdb::LogicalType::BOOLEAN,
     duckdb::CompressionType::COMPRESSION_ROARING, sparse,
     [&](std::string& b, uint64_t at, uint64_t size) {
       store64(b, at, size - sizeof(uint64_t) + 1);
     },
     "Corrupted roaring segment: metadata offset exceeds the segment"},
    {"roaring short metadata", duckdb::LogicalType::BOOLEAN,
     duckdb::CompressionType::COMPRESSION_ROARING, sparse,
     [&](std::string& b, uint64_t at, uint64_t size) {
       store64(b, at, size - sizeof(uint64_t) - 2);
     },
     "Corrupted roaring segment: container metadata extends past the segment"},
    {"roaring container data", duckdb::LogicalType::BOOLEAN,
     duckdb::CompressionType::COMPRESSION_ROARING, sparse,
     [&](std::string& b, uint64_t at, uint64_t size) {
       b[at + size - 1] = static_cast<char>(250);
     },
     "Corrupted roaring segment: container data extends into the metadata"},
    {"overflow string length", duckdb::LogicalType::VARCHAR,
     duckdb::CompressionType::COMPRESSION_UNCOMPRESSED, overflowing,
     [&](std::string& b, uint64_t, uint64_t) {
       const auto at = b.find(absl::StrCat("row-0-", long_text));
       ASSERT_NE(at, std::string::npos);
       store32(b, at - sizeof(uint32_t), 0xFFFFFFF0);
     },
     "Corrupted uncompressed string segment: overflow string runs past the end "
     "of the file"},
  };
  constexpr uint64_t kRows = 4096;
  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    const auto path =
      test_dir() / absl::StrCat("col_codecs_corrupt_body_", &c - cases);
    std::filesystem::create_directories(path);
    uint64_t offset = 0;
    uint64_t size = 0;
    {
      irs::MMapDirectory dir{path};
      irs::ColWriter w{dir, kSeg, Db()};
      auto& cw = w.OpenColumn(kField, c.type,
                              {.row_group_size = 4096, .compression = c.codec});
      for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
        duckdb::Vector v{c.type, STANDARD_VECTOR_SIZE};
        for (uint64_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
          v.SetValue(k, c.value(static_cast<int64_t>(pos + k)));
        }
        duckdb::FlatVector::SetSize(v, STANDARD_VECTOR_SIZE);
        cw.Append(v, STANDARD_VECTOR_SIZE);
      }
      ASSERT_TRUE(w.Commit(0));
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
    c.corrupt(bytes, offset, size);
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    irs::MMapDirectory dir{path};
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    try {
      auto state = col->InitScan(r.Ctx());
      for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
        duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
        col->Scan(state, out, STANDARD_VECTOR_SIZE);
      }
      ADD_FAILURE() << "the scan accepted a corrupted segment";
    } catch (const irs::SqlException& e) {
      EXPECT_EQ(e.error().errcode, ERRCODE_INDEX_CORRUPTED) << e.what();
      EXPECT_EQ(e.message(), c.error);
    }
  }
}

TEST_F(ColCodecsTest, RoaringBooleanSegmentsCarryTheirOwnStats) {
  constexpr uint64_t kChunk = STANDARD_VECTOR_SIZE;
  const auto& fn = *irs::codecs::GetCodec(
    duckdb::CompressionType::COMPRESSION_ROARING, duckdb::PhysicalType::BOOL);
  struct Flushed {
    duckdb::BaseStatistics stats;
    uint64_t rows;
  };
  const auto value = [](uint64_t g) -> bool {
    return ((g * 0x9E3779B97F4A7C15ULL) >> 61) & 1;
  };
  const auto seal = [&](std::span<const uint64_t> chunk_ends) {
    irs::MemoryDirectory dir;
    auto out = dir.create("blocks");
    irs::WriteContext ctx{Db(), *out};
    std::vector<duckdb::Vector> chunks;
    uint64_t pos = 0;
    for (const auto end : chunk_ends) {
      auto& v = chunks.emplace_back(duckdb::LogicalType::BOOLEAN, end - pos);
      auto* data = duckdb::FlatVector::GetDataMutable<bool>(v);
      for (uint64_t g = pos; g < end; ++g) {
        data[g - pos] = value(g);
      }
      duckdb::FlatVector::SetSize(v, end - pos);
      pos = end;
    }
    irs::codecs::CompressionAnalyzeContext actx{ctx};
    auto analyze = fn.init_analyze(actx, duckdb::PhysicalType::BOOL);
    for (const auto& v : chunks) {
      EXPECT_TRUE(fn.analyze(*analyze, v));
    }
    EXPECT_NE(fn.final_analyze(*analyze), duckdb::DConstants::INVALID_INDEX);
    std::vector<Flushed> flushed;
    const auto flush = [&](duckdb::BaseStatistics stats, uint64_t n,
                           std::string_view) {
      flushed.push_back({std::move(stats), n});
    };
    irs::codecs::ColumnDataCheckpointData checkpoint{
      .type = duckdb::LogicalType::BOOLEAN,
      .ctx = ctx,
      .flush = flush,
    };
    auto state = fn.init_compression(checkpoint, std::move(analyze));
    for (const auto& v : chunks) {
      fn.compress(*state, v);
    }
    fn.compress_finalize(*state);
    return flushed;
  };
  std::vector<uint64_t> ends;
  for (uint64_t end = kChunk; end <= 3'000'000; end += kChunk) {
    ends.push_back(end);
  }
  const auto probe = seal(ends);
  ASSERT_GE(probe.size(), 2u);
  const uint64_t first = probe.front().rows;
  constexpr uint64_t kBefore = 5;
  ends.clear();
  for (uint64_t end = kChunk; end < first - kBefore; end += kChunk) {
    ends.push_back(end);
  }
  ends.push_back(first - kBefore);
  ends.push_back(first - kBefore + kChunk);
  const auto flushed = seal(ends);
  ASSERT_EQ(flushed.size(), 2u);
  ASSERT_EQ(flushed[0].rows, first);
  ASSERT_EQ(flushed[1].rows, kChunk - kBefore);
  uint64_t begin = 0;
  for (const auto& [stats, rows] : flushed) {
    bool any_true = false;
    bool any_false = false;
    for (uint64_t g = begin; g < begin + rows; ++g) {
      any_true |= value(g);
      any_false |= !value(g);
    }
    SCOPED_TRACE(begin);
    EXPECT_EQ(duckdb::NumericStats::Max(stats).GetValue<bool>(), any_true);
    EXPECT_EQ(duckdb::NumericStats::Min(stats).GetValue<bool>(), !any_false);
    EXPECT_EQ(stats.IsConstant(), !(any_true && any_false));
    begin += rows;
  }
}

TEST_F(ColCodecsTest, WideRowsStayNearTheSegmentTarget) {
  constexpr uint32_t kTarget = 16384;
  irs::MemoryDirectory dir{};
  Write(dir, duckdb::CompressionType::COMPRESSION_LZ4,
        {.segment_target = kTarget}, 3000, 4096, kLongTextWithNulls);
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  ASSERT_GT(col->DataBlocks().size(), 10u);
  for (const auto& block : col->DataBlocks()) {
    EXPECT_LE(block.byte_size, kTarget * 3 / 2);
  }
  Verify(dir, duckdb::CompressionType::COMPRESSION_LZ4, 3000,
         kLongTextWithNulls);
}

TEST_F(ColCodecsTest, AutoMeasuresLessOftenWhileItsCodecHolds) {
  using irs::codecs::Shape;
  const auto seal = [&](const std::vector<std::unique_ptr<duckdb::Vector>>& in,
                        irs::codecs::StringTuning& tuning) {
    irs::codecs::StringAccumulator acc;
    Accumulate(acc, in);
    const auto outcome = irs::codecs::SealSegments(
      acc, nullptr, irs::ColCodecParams{}, duckdb::LogicalType::VARCHAR, tuning,
      Db().GetBufferManager(),
      [](irs::codecs::StringChoice, duckdb::BaseStatistics, uint64_t,
         std::string_view) {});
    EXPECT_TRUE(outcome.sealed);
  };
  const Value urls = [](uint64_t g) -> std::optional<std::string> {
    return "https://example.org/section/" + std::to_string(g % 5000) +
           "/page?id=" + std::to_string(g % 97);
  };
  const auto steady = Chunks(4096, urls);
  irs::codecs::StringTuning tuning;
  std::vector<uint32_t> gaps;
  for (int rg = 0; rg < 32; ++rg) {
    seal(steady, tuning);
    gaps.push_back(tuning.reuse.clock.gap);
  }
  std::vector<uint32_t> expected{1};
  expected.resize(3, 2);
  expected.resize(7, 4);
  expected.resize(15, 8);
  expected.resize(32, 16);
  EXPECT_EQ(gaps, expected);
  ASSERT_TRUE(tuning.reuse.pick);
  EXPECT_EQ(tuning.reuse.pick->choice.shape, Shape::Plain);

  const Value repeated = [](uint64_t g) -> std::optional<std::string> {
    std::mt19937_64 rng{g + 1};
    return "status-" + std::to_string(rng() % 37);
  };
  const auto changed = Chunks(4096, repeated);
  seal(changed, tuning);
  EXPECT_EQ(tuning.reuse.clock.gap, 1u);
  ASSERT_TRUE(tuning.reuse.pick);
  EXPECT_EQ(tuning.reuse.pick->choice.shape, Shape::Dedup);
  gaps.clear();
  for (int rg = 0; rg < 4; ++rg) {
    seal(changed, tuning);
    gaps.push_back(tuning.reuse.clock.gap);
  }
  EXPECT_EQ(gaps, (std::vector<uint32_t>{1, 2, 2, 4}));

  seal(steady, tuning);
  EXPECT_EQ(tuning.reuse.clock.gap, 1u);
  ASSERT_TRUE(tuning.reuse.pick);
  EXPECT_EQ(tuning.reuse.pick->choice.shape, Shape::Plain);
}

TEST_F(ColCodecsTest, AutoFollowsAChangeBetweenRowGroups) {
  constexpr uint32_t kRowGroup = 4096;
  constexpr uint64_t kSteady = 8 * kRowGroup;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g < kSteady) {
      return "status-" + std::to_string(g % 5);
    }
    return Pseudo(g, 300);
  };
  const uint64_t rows = kSteady + 3 * kRowGroup;
  irs::MemoryDirectory dir{};
  Write(dir, duckdb::CompressionType::COMPRESSION_AUTO, {}, rows, kRowGroup,
        value);
  irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  uint64_t row = 0;
  for (const auto& block : col->DataBlocks()) {
    const auto* choice = irs::codecs::ChoiceOf(block.codec->type);
    ASSERT_NE(choice, nullptr)
      << duckdb::CompressionTypeToString(block.codec->type);
    EXPECT_EQ(choice->shape, row < kSteady ? irs::codecs::Shape::Dedup
                                           : irs::codecs::Shape::Plain)
      << "block at row " << row;
    row += block.tuple_count;
  }
  Verify(dir, duckdb::CompressionType::COMPRESSION_AUTO, rows, value);
}

TEST_F(ColCodecsTest, FsstSegmentsFollowAChangeInCompressibility) {
  constexpr uint32_t kTarget = 64 * 1024;
  constexpr uint64_t kRows = 6000;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g < kRows / 2) {
      std::string s;
      for (int k = 0; k < 20; ++k) {
        s += "event ok host-" + std::to_string(g % 7) + " ";
      }
      return s;
    }
    return Pseudo(g, 400);
  };
  const auto vectors = Chunks(kRows, value);
  const auto chunks = WriteChunks(vectors);
  irs::codecs::StringTuning tuning;
  std::vector<uint64_t> sizes;
  const irs::codecs::StringChoice fsst{irs::codecs::Shape::Plain,
                                       irs::codecs::ByteCodec::Fsst};
  const auto outcome = irs::codecs::SealSegments(
    chunks, fsst, irs::ColCodecParams{.segment_target = kTarget},
    duckdb::LogicalType::VARCHAR, tuning, Db().GetBufferManager(),
    [&](irs::codecs::StringChoice, duckdb::BaseStatistics, uint64_t,
        std::string_view bytes) { sizes.push_back(bytes.size()); });
  ASSERT_TRUE(outcome.sealed);
  ASSERT_GT(sizes.size(), 8u);
  const auto oversized = std::ranges::count_if(
    sizes, [](uint64_t bytes) { return bytes > kTarget * 3 / 2; });
  EXPECT_LE(oversized, 2) << ::testing::PrintToString(sizes);
  RoundTrip(duckdb::CompressionType::COMPRESSION_FSST,
            {.segment_target = kTarget}, kRows, DEFAULT_ROW_GROUP_SIZE, value);
}

TEST_F(ColCodecsTest, TheWritersTierDecidesOverTheColumnsParams) {
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    return "https://shop" + std::to_string(g % 997) + ".example.org/catalog/" +
           std::to_string((g * 7919) % 100003) +
           "/item?id=" + std::to_string(g) + "&ref=" + std::to_string(g % 13);
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Flush ? "flush" : "merge");
    irs::MemoryDirectory dir{};
    {
      irs::ColWriter w{dir, kSeg, Db(), tier};
      auto& cw =
        w.OpenColumn(kField, duckdb::LogicalType::VARCHAR,
                     {.row_group_size = 16384,
                      .compression = duckdb::CompressionType::COMPRESSION_AUTO,
                      .codec_params = {.tier = irs::WriteTier::Merge}});
      AppendValues(cw, 120000, value);
      ASSERT_TRUE(cw.Meta().write_string_tuning);
      const auto lz4 =
        cw.Meta().write_string_tuning->tuned.level[static_cast<size_t>(
          irs::codecs::ByteCodec::Lz4)];
      EXPECT_EQ(tier == irs::WriteTier::Merge, lz4 > 1);
      ASSERT_TRUE(w.Commit(0));
    }
    Verify(dir, duckdb::CompressionType::COMPRESSION_AUTO, 120000, value);
  }
}

TEST_F(ColCodecsTest, RefreshMeasuresOnlyTheCheapLeaves) {
  constexpr uint64_t kRows = 60000;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    return "2026-10-05T12:" + std::to_string(g % 60) + " INFO service-" +
           std::to_string(g % 17) + " handled request " + Pseudo(g, 12) +
           " /api/v1/orders/" + std::to_string(g) + " for customer " +
           std::to_string((g * 31) % 5000) + " in " + std::to_string(g % 997) +
           "ms with status 200 and payload size " +
           std::to_string((g * 13) % 65536) + " bytes";
  };
  irs::MemoryDirectory dir{};
  Write(dir, duckdb::CompressionType::COMPRESSION_AUTO,
        {.tier = irs::WriteTier::Flush}, kRows, 8192, value);
  const auto codecs = SegmentInfo(dir, "codec");
  const auto levels = SegmentInfo(dir, "level");
  ASSERT_EQ(codecs.size(), levels.size());
  for (size_t i = 0; i < codecs.size(); ++i) {
    EXPECT_TRUE(codecs[i] == "lz4" || codecs[i] == "fsst") << codecs[i];
    if (codecs[i] == "lz4") {
      EXPECT_EQ(levels[i], "1");
    }
  }
  Verify(dir, duckdb::CompressionType::COMPRESSION_AUTO, kRows, value);
}

TEST_F(ColCodecsTest, SealedRowGroupsReleaseTheirInputs) {
  constexpr duckdb::idx_t kHalf = 512;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    return Pseudo(g % kHalf, 40);
  };
  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, kSeg, Db()};
    auto& cw =
      w.OpenColumn(kField, duckdb::LogicalType::VARCHAR,
                   {.row_group_size = static_cast<uint32_t>(2 * kHalf),
                    .compression = duckdb::CompressionType::COMPRESSION_LZ4});
    duckdb::Vector source{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    auto* d = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(source);
    for (duckdb::idx_t k = 0; k < kHalf; ++k) {
      d[k] = duckdb::StringVector::AddString(source, *value(k));
    }
    duckdb::StringVector::AddAuxiliaryData(
      source, duckdb::make_uniq<irs::ImmutablePayloads>());
    duckdb::FlatVector::SetSize(source, kHalf);
    const auto& aux = source.GetBufferRef()->GetAuxiliaryData();
    ASSERT_TRUE(aux);
    EXPECT_EQ(1, aux.use_count());
    cw.Append(source, kHalf);
    EXPECT_LT(1, aux.use_count());
    cw.Append(source, kHalf);
    EXPECT_EQ(1, aux.use_count());
    ASSERT_TRUE(w.Commit(0));
  }
  Verify(dir, duckdb::CompressionType::COMPRESSION_LZ4, 2 * kHalf, value);
}
