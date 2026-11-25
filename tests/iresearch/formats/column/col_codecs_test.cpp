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
#include <absl/container/flat_hash_set.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <duckdb.hpp>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/vector/dictionary_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/immutable_strings.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/function/scalar/string_functions.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/filter/table_filter_functions.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/object_cache.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iresearch/formats/column/codecs/bytes/zstd.hpp>
#include <iresearch/formats/column/codecs/compression_function.hpp>
#include <iresearch/formats/column/codecs/dictionary_cache.hpp>
#include <iresearch/formats/column/codecs/registry.hpp>
#include <iresearch/formats/column/codecs/text/fsst.hpp>
#include <iresearch/formats/column/codecs/text/onpair.hpp>
#include <iresearch/formats/column/codecs/text/segment_buffer.hpp>
#include <iresearch/formats/column/codecs/text/string_layout.hpp>
#include <iresearch/formats/column/codecs/text/string_scan.hpp>
#include <iresearch/formats/column/codecs/text/values.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/immutable_payloads.hpp>
#include <iresearch/formats/column/internal/gather_arms.hpp>
#include <iresearch/formats/column/internal/write_context.hpp>
#include <iresearch/formats/column/list_ingest.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <limits>
#include <magic_enum/magic_enum.hpp>
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
#include "index_dir.hpp"
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

std::string Written(irs::codecs::CodecType codec) {
  return std::string{magic_enum::enum_name(codec)};
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

  void Write(const tests::IndexDir& dir, irs::codecs::CodecType codec,
             irs::ColCodecParams params, uint64_t rows, uint32_t rg_size,
             const Value& value) {
    irs::ColWriter w{Db(), dir.Tracker(kSeg), params.tier};
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

  void Verify(const tests::IndexDir& dir, irs::codecs::CodecType codec,
              uint64_t rows, const Value& value) {
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    ASSERT_EQ(col->RowCount(), rows);
    if (codec != irs::codecs::CodecType::Auto) {
      for (const auto& block : col->DataBlocks()) {
        if (block.codec->type != irs::codecs::CodecType::Constant) {
          EXPECT_EQ(magic_enum::enum_name(block.codec->type), Written(codec));
        }
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
        std::vector<uint32_t> batch;
        for (uint64_t g = step / 2; g < rows + 3; g += step) {
          batch.push_back(g);
          if (batch.size() + kOffset < STANDARD_VECTOR_SIZE &&
              g + step < rows) {
            continue;
          }
          duckdb::Vector out{duckdb::LogicalType::VARCHAR,
                             STANDARD_VECTOR_SIZE};
          cursor.FetchRows(batch, 0, out, kOffset);
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

  void RoundTrip(irs::codecs::CodecType codec, irs::ColCodecParams params,
                 uint64_t rows, uint32_t rg_size, const Value& value) {
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, codec, params, rows, rg_size, value);
    Verify(dir, codec, rows, value);
  }

  uint64_t ColumnBytes(const tests::IndexDir& dir) {
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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

  std::vector<std::pair<irs::codecs::CodecType, std::string>> BlockLevels(
    const tests::IndexDir& dir) {
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    std::vector<std::pair<irs::codecs::CodecType, std::string>> levels;
    irs::ReadContext ctx{r};
    irs::BlockWindow window{};
    uint64_t row = 0;
    for (const auto& block : col->DataBlocks()) {
      std::string level;
      if (irs::codecs::IsText(block.codec->type) ||
          irs::codecs::IsNumeric(block.codec->type)) {
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

  std::vector<std::string> SegmentInfo(const tests::IndexDir& dir,
                                       const std::string& key) {
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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

std::string Clustered(uint64_t k) {
  return absl::StrCat(Pseudo(k / 4, 200), "/", k % 4);
}

uint64_t Scattered(uint64_t g, uint64_t distinct) {
  uint64_t z = g + 0x9E3779B97F4A7C15ULL;
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
  return (z ^ (z >> 31)) % distinct;
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
  irs::codecs::CodecType codec;
  std::vector<uint8_t> levels;
};

const std::vector<LevelArm>& EveryCodec() {
  static const std::vector<LevelArm> arms = {
    {irs::codecs::CodecType::DictLz4, {0, 1, 12}},
    {irs::codecs::CodecType::Lz4, {0, 12}},
    {irs::codecs::CodecType::DictZstd, {0, 1, 22}},
    {irs::codecs::CodecType::DictZxc, {0, 1, 7}},
    {irs::codecs::CodecType::Zxc, {0, 7}},
    {irs::codecs::CodecType::DictFsst, {0}},
    {irs::codecs::CodecType::Fsst, {0}},
    {irs::codecs::CodecType::DictFsstPlus, {0}},
    {irs::codecs::CodecType::FsstPlus, {0}},
    {irs::codecs::CodecType::DictOnpair, {0}},
    {irs::codecs::CodecType::Onpair, {0}},
    {irs::codecs::CodecType::DictFsst12, {0}},
    {irs::codecs::CodecType::Fsst12, {0}},
    {irs::codecs::CodecType::Uncompressed, {0}},
    {irs::codecs::CodecType::Zstd, {0, 1, 22}},
    {irs::codecs::CodecType::Auto, {0}},
  };
  return arms;
}

}  // namespace

TEST_F(ColCodecsTest, DictLz4LowCardinality) {
  RoundTrip(irs::codecs::CodecType::DictLz4, {}, 10000, 4096,
            kLowCardinalityWithNulls);
}

TEST_F(ColCodecsTest, DictZstdLowCardinalityLevel5) {
  RoundTrip(irs::codecs::CodecType::DictZstd, {.compression_level = 5}, 10000,
            4096, kLowCardinalityWithNulls);
}

TEST_F(ColCodecsTest, DictLz4UniqueValues) {
  RoundTrip(irs::codecs::CodecType::DictLz4, {}, 30000, 8192, kUniqueShort);
}

TEST_F(ColCodecsTest, PlainLz4LongText) {
  RoundTrip(irs::codecs::CodecType::Lz4, {}, 3000, 2048, kLongTextWithNulls);
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
  RoundTrip(irs::codecs::CodecType::DictFsst, {}, 30000, 8192, value);
  RoundTrip(irs::codecs::CodecType::DictFsstPlus, {}, 30000, 8192, value);
  RoundTrip(irs::codecs::CodecType::FsstPlus, {}, 30000, 8192, value);
  RoundTrip(irs::codecs::CodecType::DictOnpair, {}, 30000, 8192, value);
  RoundTrip(irs::codecs::CodecType::Onpair, {}, 30000, 8192, value);
  RoundTrip(irs::codecs::CodecType::DictFsst12, {}, 30000, 8192, value);
  RoundTrip(irs::codecs::CodecType::Fsst12, {}, 30000, 8192, value);
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
  RoundTrip(irs::codecs::CodecType::Fsst, {}, 30000, 8192, value);
  RoundTrip(irs::codecs::CodecType::Fsst, {}, 3000, 2048, kLongTextWithNulls);
}

struct CountedSymbols {
  static constexpr std::string_view kCacheType = "test-symbols";

  bool Open(std::string_view bytes) {
    ++opens;
    value = bytes;
    return !bytes.empty();
  }

  static inline size_t opens = 0;
  std::string value;
};

TEST_F(ColCodecsTest, EqualSymbolTableBytesShareOneOpenedTable) {
  auto& cache = Db().GetObjectCache();
  CountedSymbols::opens = 0;
  const auto open = [&](std::string_view bytes) {
    return irs::codecs::OpenSymbolTable<CountedSymbols>(&cache, bytes);
  };
  const auto first = open("abcdefghij");
  ASSERT_TRUE(first);
  EXPECT_EQ("abcdefghij", first->Table().value);
  EXPECT_EQ(first, open("abcdefghij"));
  EXPECT_EQ(1, CountedSymbols::opens);
  const auto second = open("Xbcdefghij");
  ASSERT_TRUE(second);
  EXPECT_NE(first, second);
  EXPECT_EQ("Xbcdefghij", second->Table().value);
  EXPECT_EQ(first, open("abcdefghij"));
  EXPECT_EQ(second, open("Xbcdefghij"));
  EXPECT_EQ(2, CountedSymbols::opens);
  ASSERT_EQ(irs::codecs::SymbolTableCacheKey("t", "abcdefghij"),
            irs::codecs::SymbolTableCacheKey("t", "abcdefghiX"));
  const auto collided = open("abcdefghiX");
  ASSERT_TRUE(collided);
  EXPECT_EQ("abcdefghiX", collided->Table().value);
  EXPECT_EQ("abcdefghij", first->Table().value);
  EXPECT_EQ(3, CountedSymbols::opens);
  EXPECT_FALSE(open(""));
  EXPECT_EQ(4, CountedSymbols::opens);
  const auto uncached =
    irs::codecs::OpenSymbolTable<CountedSymbols>(nullptr, "Xbcdefghij");
  ASSERT_TRUE(uncached);
  EXPECT_NE(second, uncached);
  EXPECT_EQ(5, CountedSymbols::opens);
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

  irs::codecs::text::fsst::Encoder<irs::codecs::text::fsst::Fsst8> encoder;
  std::string batch(2 * total + 8, '\0');
  std::vector<uint32_t> lengths(values.size());
  const auto all =
    encoder.Encode(values, skip, batch.data(), batch.size(), lengths);
  ASSERT_EQ(values.size(), all.strings);
  batch.resize(all.bytes);

  irs::codecs::text::fsst::Decoder<irs::codecs::text::fsst::Fsst8> decoder;
  ASSERT_TRUE(decoder.Import(nullptr, encoder.SymbolTable()));
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

  irs::codecs::text::fsst::Encoder<irs::codecs::text::fsst::Fsst8> whole;
  std::string expected(2 * total + 8, '\0');
  std::vector<uint32_t> expected_lengths(values.size());
  const auto all = whole.Encode(values, skip, expected.data(), expected.size(),
                                expected_lengths);
  ASSERT_EQ(values.size(), all.strings);
  expected.resize(all.bytes);

  irs::codecs::text::fsst::Encoder<irs::codecs::text::fsst::Fsst8> resumed;
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
  RoundTrip(irs::codecs::CodecType::DictLz4, {}, 40000, 16384, value);
  RoundTrip(irs::codecs::CodecType::DictFsst, {}, 40000, 16384, value);
}

TEST_F(ColCodecsTest, PlainSparseGatherThenScan) {
  for (const auto codec :
       {irs::codecs::CodecType::Lz4, irs::codecs::CodecType::Fsst,
        irs::codecs::CodecType::FsstPlus, irs::codecs::CodecType::Onpair,
        irs::codecs::CodecType::Fsst12, irs::codecs::CodecType::Zxc}) {
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, codec, {.segment_target = 64 * 1024}, 6000, 2048,
          kLongTextWithNulls);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
  RoundTrip(irs::codecs::CodecType::DictZxc, {.compression_level = 5}, 30000,
            8192, kUniqueShort);
}

TEST_F(ColCodecsTest, PlainZxcLongText) {
  RoundTrip(irs::codecs::CodecType::Zxc, {}, 3000, 2048, kLongTextWithNulls);
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
          SCOPED_TRACE(std::string{magic_enum::enum_name(arm.codec)} +
                       " level " + std::to_string(level) + " on " + name +
                       " target " + std::to_string(target));
          irs::ColCodecParams params{.compression_level = level};
          if (target != 0) {
            params.segment_target = target;
          }
          const uint64_t rows = value == &kOneHugeString ? 400 : 2500;
          tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
          Write(dir, arm.codec, params, rows, 1024, *value);
          irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
  const auto level_of = [&](const tests::IndexDir& dir) {
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
          .compression = irs::codecs::CodecType::DictZstd,
          .compression_level = column_level};
      },
      [](irs::field_id id) { return id; },
      1024,
      {}};
    options.codec_params = irs::ColCodecParams{.compression_level = 2};
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    {
      irs::ColWriter w{Db(), dir.Tracker(kSeg)};
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
  RoundTrip(irs::codecs::CodecType::Lz4, {.compression_level = 9}, 3000, 2048,
            kLongTextWithNulls);
}

TEST_F(ColCodecsTest, SegmentsExceedTheBlock) {
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    return Pseudo(g, 1500);
  };
  Write(dir, irs::codecs::CodecType::Lz4, {}, 4096, DEFAULT_ROW_GROUP_SIZE,
        value);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  uint64_t largest = 0;
  for (const auto& block : col->DataBlocks()) {
    largest = std::max(largest, block.byte_size);
  }
  EXPECT_GT(largest, 262144U);
  Verify(dir, irs::codecs::CodecType::Lz4, 4096, value);
}

TEST_F(ColCodecsTest, SegmentTargetCutsRowGroups) {
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::DictLz4, {.segment_target = 16 * 1024},
        30000, DEFAULT_ROW_GROUP_SIZE, kUniqueShort);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  EXPECT_GT(col->DataBlocks().size(), 4U);
  Verify(dir, irs::codecs::CodecType::DictLz4, 30000, kUniqueShort);
}

TEST_F(ColCodecsTest, DictionaryLargerThanAFrame) {
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    return Pseudo(g, 200);
  };
  RoundTrip(irs::codecs::CodecType::DictZstd, {}, 20000, DEFAULT_ROW_GROUP_SIZE,
            value);
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
  for (const auto codec :
       {irs::codecs::CodecType::DictLz4, irs::codecs::CodecType::Lz4,
        irs::codecs::CodecType::DictZstd, irs::codecs::CodecType::DictZxc,
        irs::codecs::CodecType::Zxc}) {
    for (const uint8_t level : {uint8_t{0}, uint8_t{9}}) {
      SCOPED_TRACE(testing::Message() << static_cast<int>(codec) << " level "
                                      << static_cast<int>(level));
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, codec,
            {.compression_level = level, .tier = irs::WriteTier::Flush}, kRows,
            DEFAULT_ROW_GROUP_SIZE, value);
      const auto dictionaries = SegmentInfo(dir, "dictionary");
      EXPECT_NE(
        std::find(dictionaries.begin(), dictionaries.end(), "first_frame"),
        dictionaries.end());
      Verify(dir, codec, kRows, value);

      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
  using irs::codecs::CodecType;
  constexpr uint64_t kRows = 120000;
  constexpr uint32_t kRowGroup = 16384;
  std::vector<std::string> templates;
  for (uint64_t t = 0; t < 200; ++t) {
    templates.emplace_back(Pseudo(t, 100));
  }
  const Value spread = [&](uint64_t g) -> std::optional<std::string> {
    if (g % 211 == 0) {
      return std::nullopt;
    }
    return templates[(g * 0x9E3779B97F4A7C15ULL >> 40) % templates.size()] +
           std::to_string(g);
  };
  const Value apart = [&](uint64_t g) -> std::optional<std::string> {
    if (g % 211 == 0) {
      return std::nullopt;
    }
    return std::to_string(g) +
           templates[(g * 0x9E3779B97F4A7C15ULL >> 40) % templates.size()];
  };
  for (const auto codec :
       {irs::codecs::CodecType::DictLz4, irs::codecs::CodecType::Lz4,
        irs::codecs::CodecType::DictZstd, irs::codecs::CodecType::DictZxc,
        irs::codecs::CodecType::Zxc}) {
    const auto& value =
      irs::codecs::ShapeOf(codec) == irs::codecs::Shape::Dictionary ? apart
                                                                    : spread;
    for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
      SCOPED_TRACE(
        absl::StrCat(magic_enum::enum_name(codec),
                     tier == irs::WriteTier::Merge ? " merge" : " flush"));
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
  using irs::codecs::CodecType;
  constexpr uint64_t kRows = 20000;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    return Pseudo(g, 24);
  };
  for (const auto codec :
       {irs::codecs::CodecType::Lz4, irs::codecs::CodecType::Zxc}) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, codec, {}, kRows, DEFAULT_ROW_GROUP_SIZE, value);
    const auto dictionaries = SegmentInfo(dir, "dictionary");
    EXPECT_EQ(absl::c_count(dictionaries, "local"), 0)
      << ::testing::PrintToString(dictionaries);
    Verify(dir, codec, kRows, value);
  }
}

TEST_F(ColCodecsTest, SegmentsTooSmallToTrainUseTheFirstFrame) {
  using irs::codecs::CodecType;
  constexpr uint64_t kRows = 1500;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    return "https://shop" + std::to_string(g % 97) +
           ".example.org/item?id=" + std::to_string(g);
  };
  for (const auto codec :
       {irs::codecs::CodecType::Lz4, irs::codecs::CodecType::Zxc}) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
  for (const auto codec :
       {irs::codecs::CodecType::Lz4, irs::codecs::CodecType::Zxc}) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, codec, {}, kRows, DEFAULT_ROW_GROUP_SIZE, value);
    EXPECT_EQ(SegmentInfo(dir, "dictionary"), std::vector<std::string>{""});
    Verify(dir, codec, kRows, value);
  }
}

TEST_F(ColCodecsTest, AutoStoresEmptyStringsAheadOfValuesLargerThanAFrame) {
  constexpr uint64_t kRows = 32;
  const std::string large[] = {
    std::string(40 * 1024, 'a') + "/first",
    std::string(40 * 1024, 'b') + "/second",
  };
  const Value value = [&](uint64_t g) -> std::optional<std::string> {
    if (g % 4 < 2) {
      return std::string{};
    }
    return large[g % 2];
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Merge ? "merge" : "flush");
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, irs::codecs::CodecType::Auto, {.tier = tier}, kRows, kRows,
          value);
    {
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      for (const auto& block : col->DataBlocks()) {
        EXPECT_TRUE(irs::codecs::IsText(block.codec->type))
          << magic_enum::enum_name(block.codec->type);
      }
    }
    Verify(dir, irs::codecs::CodecType::Auto, kRows, value);
  }
}

TEST_F(ColCodecsTest, AutoStoresRepeatingTextAfterASegmentOfNulls) {
  constexpr uint64_t kNulls = 1024;
  constexpr uint64_t kRows = kNulls + 512;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g < kNulls) {
      return std::nullopt;
    }
    return absl::StrCat("tenant-", g % 16, "/region-with-a-long-suffix");
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::Auto,
        {.segment_target = 64, .tier = irs::WriteTier::Merge}, kRows, kRows,
        value);
  {
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    for (const auto& block : col->DataBlocks()) {
      EXPECT_TRUE(irs::codecs::IsText(block.codec->type))
        << magic_enum::enum_name(block.codec->type);
    }
  }
  Verify(dir, irs::codecs::CodecType::Auto, kRows, value);
}

TEST_F(ColCodecsTest, AutoTrainsADictionaryWhereItPays) {
  constexpr uint64_t kRows = 60000;
  std::vector<std::string> templates;
  for (uint64_t t = 0; t < 200; ++t) {
    templates.emplace_back(Pseudo(t, 100));
  }
  const Value value = [&](uint64_t g) -> std::optional<std::string> {
    const auto v = g % (kRows / 4);
    return absl::StrCat(
      v, " ", templates[(v * 0x9E3779B97F4A7C15ULL >> 40) % templates.size()]);
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Merge ? "merge" : "flush");
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, irs::codecs::CodecType::Auto, {.tier = tier}, kRows,
          DEFAULT_ROW_GROUP_SIZE, value);
    const auto dictionaries = SegmentInfo(dir, "dictionary");
    EXPECT_EQ(absl::c_count(dictionaries, "local") != 0,
              tier == irs::WriteTier::Merge)
      << ::testing::PrintToString(dictionaries)
      << ::testing::PrintToString(SegmentInfo(dir, "codec"));
    Verify(dir, irs::codecs::CodecType::Auto, kRows, value);
  }
}

TEST_F(ColCodecsTest, ConstantAndEmptyStrings) {
  const Value constant = [](uint64_t) -> std::optional<std::string> {
    return "the same";
  };
  RoundTrip(irs::codecs::CodecType::DictLz4, {}, 5000, 2048, constant);
  const Value empties = [](uint64_t g) -> std::optional<std::string> {
    if (g % 3 == 0) {
      return "";
    }
    if (g % 3 == 1) {
      return std::nullopt;
    }
    return "x";
  };
  RoundTrip(irs::codecs::CodecType::DictLz4, {}, 5000, 2048, empties);
  RoundTrip(irs::codecs::CodecType::Lz4, {}, 5000, 2048, empties);
}

TEST_F(ColCodecsTest, ChainGroupsOfEmptyStrings) {
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 128 < 64) {
      return "";
    }
    return "row-" + std::to_string(g);
  };
  for (const auto codec :
       {irs::codecs::CodecType::Fsst, irs::codecs::CodecType::Fsst12,
        irs::codecs::CodecType::FsstPlus}) {
    SCOPED_TRACE(static_cast<int>(codec));
    RoundTrip(codec, {}, 5000, 2048, value);
  }
}

TEST_F(ColCodecsTest, AllNull) {
  const Value none = [](uint64_t) -> std::optional<std::string> {
    return std::nullopt;
  };
  RoundTrip(irs::codecs::CodecType::DictLz4, {}, 3000, 2048, none);
  RoundTrip(irs::codecs::CodecType::Lz4, {}, 3000, 2048, none);
}

TEST_F(ColCodecsTest, SingleRow) {
  RoundTrip(irs::codecs::CodecType::DictLz4, {}, 1, 2048, kUniqueShort);
  RoundTrip(irs::codecs::CodecType::Lz4, {}, 1, 2048, kUniqueShort);
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

uint64_t RowsOf(std::span<const irs::WriteChunk> chunks) {
  return absl::c_accumulate(chunks, uint64_t{0},
                            [](uint64_t sum, const irs::WriteChunk& chunk) {
                              return sum + chunk.data.size();
                            });
}

std::vector<irs::codecs::CodecType> SealAuto(
  duckdb::DatabaseInstance& db, std::span<const irs::WriteChunk> chunks,
  const irs::ColCodecParams& params, irs::codecs::ColumnTuning& tuning,
  const irs::codecs::CodecSet& disabled = {}) {
  duckdb::MemoryStream out;
  irs::WriteContext ctx{db.GetBufferManager(), out};
  std::vector<irs::codecs::CodecType> types;
  const auto flush = [&](const irs::codecs::Codec& codec,
                         duckdb::BaseStatistics, uint64_t, std::string_view,
                         std::string_view) { types.push_back(codec.type); };
  irs::codecs::ColumnDataCheckpointData checkpoint{
    .type = duckdb::LogicalType::VARCHAR,
    .ctx = &ctx,
    .flush = flush,
    .params = params,
    .tuning = tuning,
    .disabled = disabled,
    .rows = {chunks, RowsOf(chunks)},
  };
  irs::codecs::Compress(irs::codecs::CodecType::Auto, checkpoint);
  return types;
}

bool AllText(const std::vector<irs::codecs::CodecType>& types) {
  return absl::c_all_of(types, [](irs::codecs::CodecType type) {
    return irs::codecs::IsText(type);
  });
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
  struct Choice {
    Shape shape;
    ByteCodec leaf;
  };
  const Choice choices[] = {
    {Shape::Dictionary, ByteCodec::Fsst},
    {Shape::Dictionary, ByteCodec::Lz4},
    {Shape::Dictionary, ByteCodec::Zstd},
    {Shape::Dictionary, ByteCodec::Zxc},
    {Shape::Dictionary, ByteCodec::FsstPlus},
    {Shape::Dictionary, ByteCodec::OnPair},
    {Shape::Dictionary, ByteCodec::Fsst12},
    {Shape::Plain, ByteCodec::Fsst},
    {Shape::Plain, ByteCodec::Lz4},
    {Shape::Plain, ByteCodec::Zstd},
    {Shape::Plain, ByteCodec::Zxc},
    {Shape::Plain, ByteCodec::FsstPlus},
    {Shape::Plain, ByteCodec::OnPair},
    {Shape::Plain, ByteCodec::Fsst12},
  };
  const auto chunks = Chunks(20000, kWordSoup);
  const auto write_chunks = WriteChunks(chunks);
  duckdb::MemoryStream out;
  irs::WriteContext ctx{Db().GetBufferManager(), out};
  for (const auto choice : choices) {
    irs::codecs::ColumnTuning tuning;
    uint64_t segments = 0;
    uint64_t rows = 0;
    const auto flush = [&](const irs::codecs::Codec&, duckdb::BaseStatistics,
                           uint64_t count, std::string_view, std::string_view) {
      ++segments;
      rows += count;
    };
    irs::codecs::ColumnDataCheckpointData checkpoint{
      .type = duckdb::LogicalType::VARCHAR,
      .ctx = &ctx,
      .flush = flush,
      .params = {.segment_target = 16 * 1024},
      .tuning = tuning,
      .rows = {write_chunks, RowsOf(write_chunks)},
    };
    irs::codecs::Compress(irs::codecs::CodecOf(choice.shape, choice.leaf),
                          checkpoint);
    EXPECT_GT(segments, 1u);
    EXPECT_EQ(rows, 20000u);
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
  const auto write_chunks = WriteChunks(chunks);
  irs::codecs::ColumnTuning tuning;
  const auto seal = [&](size_t first, size_t end) {
    const auto types =
      SealAuto(Db(), std::span{write_chunks}.subspan(first, end - first),
               {.segment_target = 16 * 1024}, tuning);
    EXPECT_TRUE(AllText(types));
    std::vector<Shape> shapes;
    for (const auto type : types) {
      if (irs::codecs::IsText(type)) {
        shapes.push_back(irs::codecs::ShapeOf(type));
      }
    }
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
  EXPECT_TRUE(all(repeated, Shape::Dictionary))
    << ::testing::PrintToString(repeated);
  const auto mixed = seal(2, 12);
  EXPECT_GT(mixed.size(), 1u);
  EXPECT_TRUE(all(mixed, Shape::Plain)) << ::testing::PrintToString(mixed);

  RoundTrip(irs::codecs::CodecType::Auto, {.segment_target = 16 * 1024},
            3 * kSwitch, DEFAULT_ROW_GROUP_SIZE, value);
}

TEST_F(ColCodecsTest, AutoLeavesDisabledTextCodecsOut) {
  using irs::codecs::CodecType;
  using irs::codecs::Shape;
  const Value urls = [](uint64_t g) -> std::optional<std::string> {
    const auto page = g % 1500;
    return "https://example.org/section/" + std::to_string(page % 50) +
           "/page?id=" + std::to_string((page * 7919) % 100003);
  };
  const auto chunks = Chunks(4 * STANDARD_VECTOR_SIZE, urls);
  const auto write_chunks = WriteChunks(chunks);
  const auto without = [](std::initializer_list<CodecType> types) {
    irs::codecs::CodecSet disabled;
    for (const auto type : types) {
      disabled.set(std::to_underlying(type));
    }
    return disabled;
  };
  const auto seal = [&](irs::codecs::ColumnTuning& tuning,
                        const irs::codecs::CodecSet& disabled) {
    const auto types = SealAuto(Db(), write_chunks, {}, tuning, disabled);
    return std::set<CodecType>{types.begin(), types.end()};
  };

  irs::codecs::ColumnTuning kept;
  const auto all = seal(kept, {});
  ASSERT_EQ(all.size(), 1u);
  const auto first = *all.begin();
  ASSERT_TRUE(irs::codecs::IsText(first)) << magic_enum::enum_name(first);
  const auto shape = irs::codecs::ShapeOf(first);

  const auto second = seal(kept, without({first}));
  ASSERT_EQ(second.size(), 1u);
  EXPECT_NE(*second.begin(), first);
  EXPECT_TRUE(irs::codecs::IsText(*second.begin()));
  EXPECT_EQ(irs::codecs::ShapeOf(*second.begin()), shape);

  const auto other = shape == Shape::Plain ? Shape::Dictionary : Shape::Plain;
  irs::codecs::ColumnTuning fresh;
  const auto shaped = seal(
    fresh, without({irs::codecs::CodecOf(shape, irs::codecs::ByteCodec::Fsst),
                    irs::codecs::CodecOf(shape, irs::codecs::ByteCodec::Lz4)}));
  ASSERT_EQ(shaped.size(), 1u);
  ASSERT_TRUE(irs::codecs::IsText(*shaped.begin()));
  EXPECT_EQ(irs::codecs::ShapeOf(*shaped.begin()), other);

  irs::codecs::ColumnTuning none;
  EXPECT_EQ(seal(none, without({CodecType::Fsst, CodecType::Lz4,
                                CodecType::DictFsst, CodecType::DictLz4})),
            std::set<CodecType>{CodecType::Uncompressed});
}

TEST_F(ColCodecsTest,
       AutoStoresRepeatingTextWithNullsPlainWhenDictionaryCodecsAreDisabled) {
  constexpr uint64_t kRows = 4096;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 7 == 0) {
      return std::nullopt;
    }
    return absl::StrCat("tenant-", g % 40, "/region-with-a-long-suffix");
  };
  duckdb::DBConfig::GetConfig(Db()).SetDisabledCompressionMethods(
    {duckdb::CompressionType::COMPRESSION_DICT_FSST,
     duckdb::CompressionType::COMPRESSION_DICT_LZ4});
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Merge ? "merge" : "flush");
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, irs::codecs::CodecType::Auto, {.tier = tier}, kRows, kRows,
          value);
    {
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      for (const auto& block : col->DataBlocks()) {
        ASSERT_TRUE(irs::codecs::IsText(block.codec->type))
          << magic_enum::enum_name(block.codec->type);
        EXPECT_EQ(irs::codecs::ShapeOf(block.codec->type),
                  irs::codecs::Shape::Plain)
          << magic_enum::enum_name(block.codec->type);
      }
    }
    Verify(dir, irs::codecs::CodecType::Auto, kRows, value);
  }
}

TEST_F(ColCodecsTest, PointReadsAlternateBlocksOfDifferentCodecs) {
  constexpr uint64_t kRows = 6 * 1024;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if ((g / 1024) % 2 == 0) {
      return absl::StrCat("v", g % 3);
    }
    return absl::StrCat("unique-", g * 7919, "-", g);
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::Auto, {}, kRows, 1024, value);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  absl::flat_hash_set<irs::codecs::CodecType> codecs;
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
  for (const auto codec :
       {irs::codecs::CodecType::Uncompressed, irs::codecs::CodecType::Lz4}) {
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, codec, {}, kRows, 1024, value);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    ASSERT_GT(col->DataBlocks().size(), 4u);
    for (const auto& block : col->DataBlocks()) {
      ASSERT_EQ(magic_enum::enum_name(block.codec->type), Written(codec));
    }

    irs::ColumnReader::PointReader cursor{r, *col};
    std::vector<uint32_t> rows;
    for (uint64_t g = 5; g < kRows; g += 13) {
      rows.push_back(g);
    }
    duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    cursor.FetchRows(rows, 0, out, 0);
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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::DictLz4, {}, kRows, 8192, value);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
  std::vector<uint32_t> rows;
  for (uint64_t g = 3; g < kRows; g += 29) {
    rows.push_back(g);
  }
  duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
  cursor.FetchRows(rows, 0, out, 0);
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

TEST_F(ColCodecsTest, BatchedPointReadsCopyTheirValuesWhenTheCacheHasNoRoom) {
  constexpr uint64_t kRows = 8000;
  const Value distinct = [](uint64_t g) -> std::optional<std::string> {
    return absl::StrCat("https://example.org/catalog/", 100000000 + g,
                        "/reviews?page=", g % 7);
  };
  const Value repeating = [&](uint64_t g) -> std::optional<std::string> {
    return distinct(g % 200);
  };
  auto& cache = Db().GetObjectCache();
  const auto room = cache.GetMaxMemory();
  const absl::Cleanup restore = [&] { cache.SetMaxMemory(room); };
  std::vector<uint32_t> rows;
  for (uint64_t g = 7; g < kRows * 6 / 10; g += 79) {
    rows.push_back(g);
  }
  ASSERT_LT(rows.size(), 64u);
  for (const auto codec :
       {irs::codecs::CodecType::DictFsst, irs::codecs::CodecType::DictLz4}) {
    for (const auto* value : {&distinct, &repeating}) {
      SCOPED_TRACE(absl::StrCat(Written(codec), value == &distinct
                                                  ? ", distinct values"
                                                  : ", repeating values"));
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, codec, {}, kRows, 8192, *value);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      ASSERT_EQ(col->DataBlocks().size(), 1u);
      ASSERT_EQ(magic_enum::enum_name(col->DataBlocks().front().codec->type),
                Written(codec));
      const auto copied_bytes = [&] {
        irs::ColumnReader::PointReader cursor{r, *col};
        duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
        cursor.FetchRows(rows, 0, out, 0);
        for (size_t k = 0; k < rows.size(); ++k) {
          ExpectRow(out, k, rows[k], *value);
        }
        return duckdb::StringVector::GetStringAllocator(out).SizeInBytes();
      };
      cache.SetMaxMemory(cache.GetCurrentMemory());
      if (value == &distinct) {
        EXPECT_GT(copied_bytes(), 0u);
      } else {
        EXPECT_EQ(copied_bytes(), 0u);
      }
      cache.SetMaxMemory(room);
      EXPECT_EQ(copied_bytes(), 0u);
    }
  }
}

TEST_F(ColCodecsTest,
       SelectionsReadADictionaryOfDistinctValuesWithoutCacheRoom) {
  constexpr uint64_t kRows = 40000;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 97 == 0) {
      return std::nullopt;
    }
    return absl::StrCat("https://example.org/catalog/", (g * 7919) % 100003,
                        "/item/", g, "/reviews");
  };
  auto& cache = Db().GetObjectCache();
  const auto room = cache.GetMaxMemory();
  const absl::Cleanup restore = [&] { cache.SetMaxMemory(room); };
  for (const auto codec :
       {irs::codecs::CodecType::DictFsst, irs::codecs::CodecType::DictFsst12,
        irs::codecs::CodecType::DictLz4}) {
    SCOPED_TRACE(Written(codec));
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, codec, {}, kRows, 8192, value);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    cache.SetMaxMemory(cache.GetCurrentMemory());
    const auto gather = [&](const std::vector<uint64_t>& picked) {
      auto state = col->InitScan(r.Ctx());
      size_t i = 0;
      while (i < picked.size()) {
        const auto take =
          std::min<size_t>(picked.size() - i, STANDARD_VECTOR_SIZE);
        duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
        duckdb::FlatVector::ValidityMutable(out).Reset(STANDARD_VECTOR_SIZE);
        irs::column_internal::GatherRows(*col, state, Rows2{&picked[i], take},
                                         out, 0, /*whole_output=*/true);
        out.Flatten(take);
        for (size_t k = 0; k < take; ++k) {
          ExpectRow(out, k, picked[i + k], value);
        }
        i += take;
      }
    };
    std::vector<uint64_t> every_tenth;
    for (uint64_t g = 1; g < kRows; g += 10) {
      every_tenth.push_back(g);
    }
    gather(every_tenth);
    std::vector<uint64_t> sparse_then_dense;
    for (uint64_t g = 1; g < kRows; g += g < 3000 ? 10 : 1) {
      sparse_then_dense.push_back(g);
    }
    gather(sparse_then_dense);
    std::vector<uint64_t> sparse_then_sparser;
    for (uint64_t g = 1; g < kRows; g += g % 20000 < 5000 ? 10 : 53) {
      sparse_then_sparser.push_back(g);
    }
    gather(sparse_then_sparser);
    cache.SetMaxMemory(room);
  }
}

TEST_F(ColCodecsTest, RowReadsDecodeTheSharedBytesOfEarlierEntries) {
  constexpr uint32_t kRows = 12000;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 97 == 0) {
      return std::nullopt;
    }
    std::string v = absl::StrCat(
      "https://example.org/",
      absl::Hex((g / 5) * 2654435761 % 4294967291, absl::kZeroPad8), "/");
    for (uint64_t k = 0; k < 3 + g % 7 * 11; ++k) {
      v.push_back(static_cast<char>(0xC2 + (g * 31 + k * 17) % 30));
      v.push_back(static_cast<char>(0x80 + (g * 7 + k * 13) % 64));
    }
    absl::StrAppend(&v, "/item/", g);
    v.append(g % 13 * 40, static_cast<char>('a' + g % 5));
    return v;
  };
  auto& cache = Db().GetObjectCache();
  const auto room = cache.GetMaxMemory();
  const absl::Cleanup restore = [&] { cache.SetMaxMemory(room); };
  for (const auto codec :
       {irs::codecs::CodecType::Fsst, irs::codecs::CodecType::Fsst12,
        irs::codecs::CodecType::DictFsst, irs::codecs::CodecType::DictFsst12}) {
    SCOPED_TRACE(Written(codec));
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, codec, {}, kRows, 8192, value);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    cache.SetMaxMemory(cache.GetCurrentMemory());
    {
      irs::ColumnReader::PointReader cursor{r, *col};
      for (uint32_t g = 3; g < kRows; g += 41) {
        duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
        cursor.FetchRows(std::span<const uint32_t>{&g, 1}, 0, out, 0);
        ExpectRow(out, 0, g, value);
      }
    }
    {
      irs::ColumnReader::PointReader cursor{r, *col};
      std::vector<uint32_t> rows;
      const auto fetch = [&] {
        duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
        cursor.FetchRows(rows, 0, out, 0);
        for (size_t k = 0; k < rows.size(); ++k) {
          ExpectRow(out, k, rows[k], value);
        }
        rows.clear();
      };
      for (uint32_t g = 1; g < kRows; g += g % 16 < 4 ? 1 : 23) {
        rows.push_back(g);
        if (rows.size() == 48) {
          fetch();
        }
      }
      fetch();
    }
    const auto gather = [&](const std::vector<uint64_t>& picked) {
      auto state = col->InitScan(r.Ctx());
      size_t i = 0;
      while (i < picked.size()) {
        const auto take =
          std::min<size_t>(picked.size() - i, STANDARD_VECTOR_SIZE);
        duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
        duckdb::FlatVector::ValidityMutable(out).Reset(STANDARD_VECTOR_SIZE);
        irs::column_internal::GatherRows(*col, state, Rows2{&picked[i], take},
                                         out, 0, /*whole_output=*/true);
        out.Flatten(take);
        for (size_t k = 0; k < take; ++k) {
          ExpectRow(out, k, picked[i + k], value);
        }
        i += take;
      }
    };
    std::vector<uint64_t> three_of_sixteen;
    for (uint64_t g = 0; g < kRows; ++g) {
      if (g % 16 == 5 || g % 16 == 6 || g % 16 == 11) {
        three_of_sixteen.push_back(g);
      }
    }
    gather(three_of_sixteen);
    std::vector<uint64_t> every_tenth;
    for (uint64_t g = 4; g < kRows; g += 10) {
      every_tenth.push_back(g);
    }
    gather(every_tenth);
    std::vector<uint64_t> few;
    for (uint64_t g = 9; g < kRows; g += 211) {
      few.push_back(g);
    }
    gather(few);
    cache.SetMaxMemory(room);
  }
}

TEST_F(ColCodecsTest, TwelveBitSymbolTablesAreStoredOncePerRowGroup) {
  constexpr uint64_t kRows = 24000;
  constexpr uint32_t kRowGroup = 8192;
  constexpr uint64_t kFirst = 1000;
  constexpr uint64_t kCount = 20000;
  constexpr size_t kGroups = (kRows + kRowGroup - 1) / kRowGroup;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 53 == 0) {
      return std::nullopt;
    }
    return absl::StrCat("https://example.org/", g % 977, "/catalog/",
                        (g * 7919) % 100003, "/item/", g, "?ref=", g % 31);
  };
  const irs::ColCodecParams params{.segment_target = 32 * 1024};
  const auto tables = [](const irs::ColumnReader& col) {
    std::set<uint64_t> offsets;
    for (const auto& block : col.DataBlocks()) {
      if (block.shared_size != 0) {
        offsets.insert(block.shared_offset);
      }
    }
    return offsets.size();
  };
  const auto check = [&](const tests::IndexDir& dir,
                         irs::codecs::CodecType codec) {
    const bool shares = codec == irs::codecs::CodecType::Fsst12 ||
                        codec == irs::codecs::CodecType::DictFsst12;
    Write(dir, codec, params, kRows, kRowGroup, value);
    Verify(dir, codec, kRows, value);
    irs::ColReader src = irs::tests::OpenColReader(dir, kSeg);
    const auto* source = src.Column(kField);
    ASSERT_NE(source, nullptr);
    ASSERT_GT(source->DataBlocks().size(), 2 * kGroups);
    for (const auto& block : source->DataBlocks()) {
      EXPECT_EQ(block.shared_size != 0, shares);
    }
    EXPECT_EQ(tables(*source), shares ? kGroups : 0);
    {
      irs::ColWriter w{Db(), dir.Tracker("copy"), params.tier};
      auto& cw = w.OpenColumn(kField, duckdb::LogicalType::VARCHAR,
                              {.row_group_size = kRowGroup,
                               .compression = codec,
                               .codec_params = params});
      ASSERT_TRUE(cw.CopyableFrom(*source));
      ASSERT_TRUE(cw.CopyBlocks(*source, src.Ctx(), [] { return true; }));
      cw.CopyRows(kRows, *source, src.Ctx(), kFirst, kCount);
      ASSERT_TRUE(w.Commit(0));
    }
    irs::ColReader dst = irs::tests::OpenColReader(dir, "copy");
    const auto* copied = dst.Column(kField);
    ASSERT_NE(copied, nullptr);
    ASSERT_EQ(copied->RowCount(), kRows + kCount);
    for (const auto& block : copied->DataBlocks()) {
      EXPECT_EQ(block.shared_size != 0, shares);
    }
    if (shares) {
      EXPECT_GE(tables(*copied), 2 * kGroups);
    }
    auto state = copied->InitScan(dst.Ctx());
    for (uint64_t pos = 0; pos < kRows + kCount;) {
      const auto take =
        std::min<uint64_t>(kRows + kCount - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      copied->Scan(state, out, take);
      out.Flatten(take);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto row = pos + k;
        ExpectRow(out, k, row < kRows ? row : kFirst + (row - kRows), value);
      }
      pos += take;
    }
    irs::ColumnReader::PointReader cursor{dst, *copied};
    for (uint32_t row = 5; row < kRows + kCount; row += 997) {
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      cursor.FetchRows(std::span<const uint32_t>{&row, 1}, 0, out, 0);
      ExpectRow(out, 0, row < kRows ? row : kFirst + (row - kRows), value);
    }
  };
  for (const auto codec :
       {irs::codecs::CodecType::Fsst12, irs::codecs::CodecType::DictFsst12,
        irs::codecs::CodecType::Fsst, irs::codecs::CodecType::DictFsst}) {
    SCOPED_TRACE(Written(codec));
    {
      SCOPED_TRACE("memory");
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      check(dir, codec);
    }
    {
      SCOPED_TRACE("files");
      const auto path =
        test_dir() / absl::StrCat("col_codecs_shared_table_", Written(codec));
      std::filesystem::create_directories(path);
      tests::IndexDir dir{Db(), path.string()};
      check(dir, codec);
    }
  }
}

TEST_F(ColCodecsTest, AutoIsNoLargerThanItsCandidates) {
  using irs::codecs::CodecType;
  const irs::codecs::CodecType candidates[] = {
    irs::codecs::CodecType::DictFsst, irs::codecs::CodecType::Fsst,
    irs::codecs::CodecType::DictLz4,  irs::codecs::CodecType::Lz4,
    irs::codecs::CodecType::DictZxc,  irs::codecs::CodecType::Zxc};
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
    tests::IndexDir auto_dir{Db(), TestEnv::MemoryRoot()};
    Write(auto_dir, irs::codecs::CodecType::Auto, {}, corpus.rows, 16384,
          *corpus.value);
    const auto auto_bytes = ColumnBytes(auto_dir);
    auto best = std::numeric_limits<uint64_t>::max();
    for (const auto codec : candidates) {
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, codec, {}, corpus.rows, 16384, *corpus.value);
      best = std::min(best, ColumnBytes(dir));
    }
    EXPECT_LE(auto_bytes, best + best / 8)
      << corpus.name << " auto " << auto_bytes << " best named " << best;
  }
}

TEST_F(ColCodecsTest, SampledPricesDoNotLoseToTheCandidatesTheyPrice) {
  using irs::codecs::CodecType;
  const Value urls = [](uint64_t g) -> std::optional<std::string> {
    return "https://example.org/section/" + std::to_string(g % 5000) +
           "/page?id=" + std::to_string(g % 97);
  };
  struct Arm {
    const char* name;
    const Value* value;
    uint64_t rows;
    uint32_t segment_target;
    irs::codecs::CodecType named;
    uint8_t level;
  };
  const Arm arms[] = {
    {"periodic urls", &urls, 150000, irs::ColCodecParams{}.segment_target,
     irs::codecs::CodecType::Lz4, 6},
    {"word soup", &kWordSoup, 60000, 64 * 1024, irs::codecs::CodecType::Fsst,
     0},
  };
  for (const auto& arm : arms) {
    tests::IndexDir auto_dir{Db(), TestEnv::MemoryRoot()};
    Write(auto_dir, irs::codecs::CodecType::Auto,
          {.segment_target = arm.segment_target}, arm.rows,
          DEFAULT_ROW_GROUP_SIZE, *arm.value);
    tests::IndexDir named_dir{Db(), TestEnv::MemoryRoot()};
    Write(
      named_dir, arm.named,
      {.compression_level = arm.level, .segment_target = arm.segment_target},
      arm.rows, DEFAULT_ROW_GROUP_SIZE, *arm.value);
    const auto auto_bytes = ColumnBytes(auto_dir);
    const auto named_bytes = ColumnBytes(named_dir);
    EXPECT_LE(auto_bytes, named_bytes + named_bytes / 100)
      << arm.name << " auto " << auto_bytes << " named " << named_bytes;
    Verify(auto_dir, irs::codecs::CodecType::Auto, arm.rows, *arm.value);
  }
}

TEST_F(ColCodecsTest, AutoLevelsComeFromTheLadders) {
  using irs::codecs::CodecType;
  const std::set<std::string> lz4{"1", "4", "6"};
  const std::pair<const char*, const Value*> corpora[] = {
    {"low-cardinality", &kLowCardinalityWithNulls},
    {"unique-short", &kUniqueShort},
    {"word-soup", &kWordSoup},
    {"long-text", &kLongTextWithNulls},
  };
  for (const auto& [name, value] : corpora) {
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, irs::codecs::CodecType::Auto, {.segment_target = 16 * 1024},
          6000, 2048, *value);
    for (const auto& [type, level] : BlockLevels(dir)) {
      const auto label =
        absl::StrCat(name, " ", magic_enum::enum_name(type), " level ", level);
      switch (type) {
        case irs::codecs::CodecType::Lz4:
        case irs::codecs::CodecType::DictLz4:
          EXPECT_TRUE(lz4.contains(level)) << label;
          break;
        case irs::codecs::CodecType::Fsst:
        case irs::codecs::CodecType::DictFsst:
        case irs::codecs::CodecType::FsstPlus:
        case irs::codecs::CodecType::DictFsstPlus:
        case irs::codecs::CodecType::Onpair:
        case irs::codecs::CodecType::DictOnpair:
        case irs::codecs::CodecType::Fsst12:
        case irs::codecs::CodecType::DictFsst12:
          EXPECT_EQ(level, "0") << label;
          break;
        default:
          EXPECT_TRUE(type == irs::codecs::CodecType::Uncompressed ||
                      type == irs::codecs::CodecType::Constant)
            << label;
      }
    }
  }
}

TEST_F(ColCodecsTest, AutoLevelsFollowTheData) {
  using irs::codecs::CodecType;
  const auto levels = [&](const Value& value, uint64_t rows) {
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, irs::codecs::CodecType::Auto, {.segment_target = 64 * 1024},
          rows, DEFAULT_ROW_GROUP_SIZE, value);
    std::set<std::string> out;
    for (const auto& [type, level] : BlockLevels(dir)) {
      if (!level.empty() && level != "0") {
        out.insert(absl::StrCat(magic_enum::enum_name(type), ":", level));
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
  const Value vocabulary = [](uint64_t g) -> std::optional<std::string> {
    return kVocabulary(g % 2000);
  };
  const auto text = levels(vocabulary, 8000);
  ASSERT_FALSE(text.empty());
  EXPECT_GT(top(text), 1) << ::testing::PrintToString(text);
}

TEST_F(ColCodecsTest, AutoTunesLevelsOnTheFirstRowGroupWithData) {
  using Chunked = std::vector<std::unique_ptr<duckdb::Vector>>;
  constexpr uint64_t kRowGroup = 1024;
  const auto seal = [&](const Chunked& chunks,
                        irs::codecs::ColumnTuning& tuning) {
    EXPECT_TRUE(AllText(SealAuto(Db(), WriteChunks(chunks), {}, tuning)));
  };
  const auto seal_constant = [&](const Chunked& chunks,
                                 irs::codecs::ColumnTuning& tuning) {
    EXPECT_TRUE(absl::c_all_of(
      SealAuto(Db(), WriteChunks(chunks), {}, tuning),
      [](auto type) { return type == irs::codecs::CodecType::Constant; }));
  };
  const auto levels = [](const irs::codecs::ColumnTuning& tuning) {
    std::vector<int> out;
    for (const auto type :
         {irs::codecs::CodecType::DictLz4, irs::codecs::CodecType::Lz4}) {
      const auto& hint = tuning.hints[std::to_underlying(type)];
      out.push_back(hint.refreshed != 0 ? 1 : 0);
      out.push_back(hint.variant);
    }
    return out;
  };
  const Value constant = [](uint64_t) -> std::optional<std::string> {
    return "n/a";
  };
  const Value vocabulary = [](uint64_t g) -> std::optional<std::string> {
    return kVocabulary(g % (kRowGroup / 4));
  };
  const auto text = Chunks(kRowGroup, vocabulary);
  const auto nulls = Chunks(kRowGroup, kAllNull);
  const auto constants = Chunks(kRowGroup, constant);

  irs::codecs::ColumnTuning untuned;
  seal_constant(nulls, untuned);
  irs::codecs::ColumnTuning tuned;
  seal(text, tuned);
  ASSERT_NE(levels(tuned), levels(untuned));

  const std::pair<const char*, const Chunked*> heads[] = {
    {"all null", &nulls},
    {"constant", &constants},
  };
  for (const auto& [name, head] : heads) {
    for (int groups = 1; groups <= 2; ++groups) {
      SCOPED_TRACE(absl::StrCat(groups, " ", name, " row groups"));
      irs::codecs::ColumnTuning tuning;
      for (int g = 0; g < groups; ++g) {
        seal_constant(*head, tuning);
      }
      seal(text, tuning);
      EXPECT_EQ(levels(tuning), levels(tuned));
    }
  }
}

const std::set<irs::codecs::CodecType>& ColumnstoreLeaves() {
  using irs::codecs::CodecType;
  static const std::set<CodecType> leaves{
    CodecType::Lz4,      CodecType::DictLz4, CodecType::Fsst,
    CodecType::DictFsst, CodecType::Zstd,    CodecType::DictZstd,
    CodecType::Zxc,      CodecType::DictZxc, CodecType::Uncompressed};
  return leaves;
}

TEST_F(ColCodecsTest, AutoPicksColumnstoreLeaves) {
  using irs::codecs::CodecType;
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, irs::codecs::CodecType::Auto, {}, corpus.rows, 16384,
          *corpus.value);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    for (const auto& block : col->DataBlocks()) {
      EXPECT_TRUE(leaves.contains(block.codec->type))
        << corpus.name << " " << magic_enum::enum_name(block.codec->type);
    }
  }
}

TEST_F(ColCodecsTest, TransactionalCodecsLeaveTextToTheColumnstore) {
  using irs::codecs::CodecType;
  struct Corpus {
    const char* name;
    const Value* value;
    uint64_t rows;
  };
  const Corpus corpora[] = {
    {"low-cardinality", &kLowCardinalityWithNulls, 30000},
    {"long-text", &kLongTextWithNulls, 3000},
  };
  for (const auto forced :
       {irs::codecs::CodecType::Rle, irs::codecs::CodecType::Bitpacking}) {
    for (const auto& corpus : corpora) {
      SCOPED_TRACE(
        absl::StrCat(magic_enum::enum_name(forced), " ", corpus.name));
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, forced, {}, corpus.rows, 16384, *corpus.value);
      Verify(dir, irs::codecs::CodecType::Auto, corpus.rows, *corpus.value);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      for (const auto& block : col->DataBlocks()) {
        EXPECT_TRUE(ColumnstoreLeaves().contains(block.codec->type))
          << magic_enum::enum_name(block.codec->type);
      }
    }
  }
}

TEST_F(ColCodecsTest, AScanStartsEveryRowGroupWithADictionaryVector) {
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    return "status-" + std::to_string(g % 37);
  };
  constexpr uint32_t kRowGroup = 2 * STANDARD_VECTOR_SIZE;
  constexpr uint32_t kRows = 3 * kRowGroup;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::DictLz4, {}, kRows, kRowGroup, value);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->DataBlocks().size(), 3u);
  auto state = col->InitScan(r.Ctx());
  for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
    duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    ASSERT_EQ(col->Scan(state, out, STANDARD_VECTOR_SIZE),
              duckdb::idx_t{STANDARD_VECTOR_SIZE});
    EXPECT_EQ(out.GetVectorType(), duckdb::VectorType::DICTIONARY_VECTOR)
      << "row " << pos;
    for (duckdb::idx_t i = 0; i < STANDARD_VECTOR_SIZE; i += 97) {
      EXPECT_EQ(out.GetValue(i).ToString(), *value(pos + i))
        << "row " << pos + i;
    }
  }
}

TEST_F(ColCodecsTest, ADictionaryVectorHoldsANullOnlyWhenItsSegmentHasNulls) {
  constexpr uint32_t kRows = 6 * STANDARD_VECTOR_SIZE;
  constexpr uint64_t kDistinct = 300;
  const auto text = [](uint64_t k) {
    return absl::StrCat("tenant-", k, "/item");
  };
  absl::flat_hash_set<std::string> held;
  for (uint64_t k = 0; k < kDistinct; ++k) {
    held.emplace(text(k));
  }
  for (const bool nulls : {false, true}) {
    const Value value = [&](uint64_t g) -> std::optional<std::string> {
      if (nulls && g % 13 == 0) {
        return std::nullopt;
      }
      return text((g * 7919) % kDistinct);
    };
    for (const auto codec :
         {irs::codecs::CodecType::DictFsst,
          irs::codecs::CodecType::DictFsstPlus,
          irs::codecs::CodecType::DictOnpair,
          irs::codecs::CodecType::DictFsst12, irs::codecs::CodecType::DictLz4,
          irs::codecs::CodecType::DictZstd, irs::codecs::CodecType::DictZxc}) {
      SCOPED_TRACE(absl::StrCat(magic_enum::enum_name(codec),
                                nulls ? " with NULLs" : " without NULLs"));
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, codec, {}, kRows, kRows, value);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      for (int pass = 0; pass < 3; ++pass) {
        auto state = col->InitScan(r.Ctx());
        for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
          duckdb::Vector out{duckdb::LogicalType::VARCHAR,
                             STANDARD_VECTOR_SIZE};
          ASSERT_EQ(col->Scan(state, out, STANDARD_VECTOR_SIZE),
                    duckdb::idx_t{STANDARD_VECTOR_SIZE});
          ASSERT_EQ(out.GetVectorType(), duckdb::VectorType::DICTIONARY_VECTOR);
          const auto& entries = duckdb::DictionaryVector::Child(out);
          EXPECT_EQ(duckdb::FlatVector::Validity(entries).CanHaveNull(), nulls)
            << "pass " << pass << " row " << pos;
          const auto entry_count =
            duckdb::DictionaryVector::DictionarySize(out).GetIndex();
          ASSERT_EQ(entry_count, kDistinct + 1);
          for (duckdb::idx_t e = 0; e < entry_count; ++e) {
            const auto entry = entries.GetValue(e);
            ASSERT_EQ(entry.IsNull(), nulls && e == 0) << "entry " << e;
            if (!entry.IsNull()) {
              EXPECT_TRUE(held.contains(entry.ToString())) << "entry " << e;
            }
          }
          for (duckdb::idx_t i = 0; i < STANDARD_VECTOR_SIZE; i += 97) {
            const auto expected = value(pos + i);
            const auto got = out.GetValue(i);
            ASSERT_EQ(got.IsNull(), !expected) << "row " << pos + i;
            if (expected) {
              EXPECT_EQ(got.ToString(), *expected) << "row " << pos + i;
            }
          }
        }
      }
    }
  }
}

TEST_F(ColCodecsTest, CachedDictionaryKeepsNoScanContext) {
  const auto& value = kLowCardinalityWithNulls;
  constexpr uint32_t kRows = 10 * STANDARD_VECTOR_SIZE;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::DictLz4, {}, kRows, kRows, value);
  auto& cache = Db().GetObjectCache();
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::DictLz4, {}, kRows, kRows, value);
  std::vector<uint32_t> rows;
  for (uint64_t g = 0; g < kRows; g += 3) {
    rows.push_back(g);
  }
  std::vector<std::unique_ptr<duckdb::Vector>> outs;
  {
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    irs::ColumnReader::PointReader cursor{r, *col};
    for (size_t i = 0; i < rows.size(); i += STANDARD_VECTOR_SIZE) {
      const auto take = std::min<size_t>(rows.size() - i, STANDARD_VECTOR_SIZE);
      outs.push_back(std::make_unique<duckdb::Vector>(
        duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE));
      cursor.FetchRows(std::span<const uint32_t>{&rows[i], take}, 0,
                       *outs.back(), 0);
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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::DictLz4, {}, kRows, 8192, value);
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
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
      irs::ColReader other = irs::tests::OpenColReader(dir, kSeg);
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
  irs::ColReader reopened = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = reopened.Column(kField);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->DataBlocks().size(), keys.size());
  scan_all(reopened, *col);
}

TEST_F(ColCodecsTest, ScanAdoptsTheDictionaryCachedAfterItOpenedTheBlock) {
  const auto& value = kLowCardinalityWithNulls;
  constexpr uint64_t kRows = 20000;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::DictLz4, {}, kRows, 8192, value);
  auto& cache = Db().GetObjectCache();
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  const auto key = CacheKeys(r, *col).front();
  const auto scan = [&](irs::ColumnReader::ScanState& state, uint64_t pos,
                        duckdb::idx_t count, duckdb::Vector& out) {
    col->Scan(state, out, count);
    duckdb::Vector flat{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    flat.Reference(out);
    flat.Flatten(count);
    for (duckdb::idx_t k = 0; k < count; ++k) {
      ExpectRow(flat, k, pos + k, value);
    }
  };
  {
    auto first = col->InitScan(r.Ctx());
    duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    scan(first, 0, STANDARD_VECTOR_SIZE, out);
  }
  auto late = col->InitScan(r.Ctx());
  {
    duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    scan(late, 0, 1, out);
  }
  ASSERT_EQ(cache.Get<irs::codecs::DecodedDictionary>(key), nullptr);
  {
    auto second = col->InitScan(r.Ctx());
    duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    scan(second, 0, STANDARD_VECTOR_SIZE, out);
  }
  const auto cached = cache.Get<irs::codecs::DecodedDictionary>(key);
  ASSERT_NE(cached, nullptr);
  duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
  scan(late, 1, STANDARD_VECTOR_SIZE, out);
  ASSERT_EQ(out.GetVectorType(), duckdb::VectorType::DICTIONARY_VECTOR);
  EXPECT_EQ(
    duckdb::FlatVector::GetData<duckdb::string_t>(
      duckdb::DictionaryVector::Child(out)),
    duckdb::FlatVector::GetData<duckdb::string_t>(cached->Dictionary()->data));
}

TEST_F(ColCodecsTest, DictionaryCompletedByPartialScansIsCached) {
  const auto& value = kLowCardinalityWithNulls;
  constexpr uint64_t kRows = 20000;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::DictFsst, {}, kRows, 8192, value);
  auto& cache = Db().GetObjectCache();
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::DictLz4, {}, kRows, 4096, value);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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

TEST_F(ColCodecsTest, AFrameDictionaryIsSortedWhenThatMakesItSmaller) {
  const Value clustered = [](uint64_t g) -> std::optional<std::string> {
    return Clustered(Scattered(g, 4000));
  };
  const Value repeating = [](uint64_t g) -> std::optional<std::string> {
    return absl::StrCat("tenant-", (g * 7919) % 3000, "/item");
  };
  const Value few = [](uint64_t g) -> std::optional<std::string> {
    return absl::StrCat("status-", (g * 7919) % 40);
  };
  const Value ids = [](uint64_t g) -> std::optional<std::string> {
    return Pseudo(Scattered(g, 3000), 32);
  };
  constexpr uint64_t kRows = 30000;
  for (const auto codec :
       {irs::codecs::CodecType::DictLz4, irs::codecs::CodecType::DictZstd,
        irs::codecs::CodecType::DictZxc}) {
    for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
      SCOPED_TRACE(
        absl::StrCat(magic_enum::enum_name(codec),
                     tier == irs::WriteTier::Merge ? " merge" : " flush"));
      for (const auto& [value, sorted] :
           {std::pair<const Value&, std::string_view>{clustered, "true"},
            std::pair<const Value&, std::string_view>{repeating, "false"},
            std::pair<const Value&, std::string_view>{few, "false"},
            std::pair<const Value&, std::string_view>{ids, "false"}}) {
        tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
        Write(dir, codec, {.tier = tier}, kRows, kRows, value);
        Verify(dir, codec, kRows, value);
        const auto flags = SegmentInfo(dir, "sorted");
        ASSERT_FALSE(flags.empty());
        EXPECT_TRUE(absl::c_all_of(
          flags, [&](const std::string& flag) { return flag == sorted; }))
          << ::testing::PrintToString(flags);
      }
    }
  }
}

TEST_F(ColCodecsTest, ARowGroupOfOneTextValueIsStoredConstantUnderAnyCodec) {
  using irs::codecs::CodecType;
  constexpr uint32_t kRowGroup = 4096;
  constexpr uint64_t kRows = 3 * kRowGroup;
  const std::string texts[] = {"", "ok", std::string(40, 'x') + "/tail"};
  for (const auto& text : texts) {
    SCOPED_TRACE(text.size());
    const Value value = [&](uint64_t) -> std::optional<std::string> {
      return text;
    };
    for (const auto codec : {CodecType::Auto, CodecType::DictFsst,
                             CodecType::DictLz4, CodecType::Lz4}) {
      SCOPED_TRACE(magic_enum::enum_name(codec));
      for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
        tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
        Write(dir, codec, {.tier = tier}, kRows, kRowGroup, value);
        {
          irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
          const auto* col = r.Column(kField);
          ASSERT_NE(col, nullptr);
          ASSERT_EQ(col->DataBlocks().size(), kRows / kRowGroup);
          for (const auto& block : col->DataBlocks()) {
            EXPECT_EQ(block.codec->type, CodecType::Constant);
            EXPECT_EQ(block.byte_size, text.size());
          }
        }
        Verify(dir, codec, kRows, value);
      }
    }
  }
}

TEST_F(ColCodecsTest, ARowGroupOfTextNullsIsStoredConstantWithoutValidity) {
  using irs::codecs::CodecType;
  constexpr uint32_t kRowGroup = 4096;
  constexpr uint64_t kRows = 4 * kRowGroup;
  const Value every_null = [](uint64_t) -> std::optional<std::string> {
    return std::nullopt;
  };
  const Value between_text = [](uint64_t g) -> std::optional<std::string> {
    if (g / kRowGroup % 2 == 0) {
      return std::nullopt;
    }
    return absl::StrCat("value-", g % 50);
  };
  for (const auto codec : {CodecType::Auto, CodecType::DictFsst,
                           CodecType::DictLz4, CodecType::Lz4}) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
      for (const auto* value : {&every_null, &between_text}) {
        tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
        Write(dir, codec, {.tier = tier}, kRows, kRowGroup, *value);
        {
          irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
          const auto* col = r.Column(kField);
          ASSERT_NE(col, nullptr);
          EXPECT_FALSE(col->HasValidity());
          uint64_t row = 0;
          for (const auto& block : col->DataBlocks()) {
            if ((*value)(row)) {
              EXPECT_NE(block.codec->type, CodecType::Constant);
            } else {
              EXPECT_EQ(block.codec->type, CodecType::Constant);
              EXPECT_EQ(block.byte_size, 0U);
            }
            row += block.tuple_count;
          }
          EXPECT_EQ(row, kRows);
        }
        Verify(dir, codec, kRows, *value);
      }
    }
  }
}

TEST_F(ColCodecsTest, ACompactionCopiesConstantRowGroupsUnderANamedCodec) {
  using irs::codecs::CodecType;
  constexpr uint32_t kRowGroup = 4096;
  constexpr uint64_t kRows = 3 * kRowGroup;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    return g / kRowGroup == 1 ? absl::StrCat("value-", g % 50)
                              : "the row groups around hold this value";
  };
  for (const auto codec : {CodecType::DictLz4, CodecType::Fsst}) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, codec, {}, kRows, kRowGroup, value);
    irs::ColReader src = irs::tests::OpenColReader(dir, kSeg);
    const auto* source = src.Column(kField);
    ASSERT_NE(source, nullptr);
    const auto blocks = source->DataBlocks();
    ASSERT_GE(blocks.size(), 3U);
    EXPECT_EQ(blocks.front().codec->type, CodecType::Constant);
    EXPECT_EQ(blocks[1].codec->type, codec);
    EXPECT_EQ(blocks.back().codec->type, CodecType::Constant);
    {
      irs::ColWriter w{Db(), dir.Tracker("copy")};
      auto& cw =
        w.OpenColumn(kField, duckdb::LogicalType::VARCHAR,
                     {.row_group_size = kRowGroup, .compression = codec});
      ASSERT_TRUE(cw.CopyableFrom(*source));
      ASSERT_TRUE(cw.CopyBlocks(*source, src.Ctx(), [] { return true; }));
      ASSERT_TRUE(w.Commit(0));
    }
    irs::ColReader dst = irs::tests::OpenColReader(dir, "copy");
    const auto* copied = dst.Column(kField);
    ASSERT_NE(copied, nullptr);
    ASSERT_EQ(copied->RowCount(), kRows);
    const auto copied_blocks = copied->DataBlocks();
    ASSERT_EQ(copied_blocks.size(), blocks.size());
    for (size_t i = 0; i < blocks.size(); ++i) {
      EXPECT_EQ(copied_blocks[i].codec->type, blocks[i].codec->type);
      EXPECT_EQ(copied_blocks[i].byte_size, blocks[i].byte_size);
    }
    auto state = copied->InitScan(dst.Ctx());
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      copied->Scan(state, out, STANDARD_VECTOR_SIZE);
      out.Flatten(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
        ExpectRow(out, k, pos + k, value);
      }
    }
  }
}

TEST_F(ColCodecsTest, ARowGroupWithANullOrASecondTextValueIsNotConstant) {
  using irs::codecs::CodecType;
  constexpr uint32_t kRowGroup = 4096;
  const Value with_null = [](uint64_t g) -> std::optional<std::string> {
    if (g == 100) {
      return std::nullopt;
    }
    return "same";
  };
  const Value with_other = [](uint64_t g) -> std::optional<std::string> {
    return g == 4000 ? "other" : "same";
  };
  const Value null_first = [](uint64_t g) -> std::optional<std::string> {
    if (g % kRowGroup == 0) {
      return std::nullopt;
    }
    return "same";
  };
  for (const auto& value : {with_null, with_other, null_first}) {
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, CodecType::Auto, {}, kRowGroup, kRowGroup, value);
    {
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      for (const auto& block : col->DataBlocks()) {
        EXPECT_NE(block.codec->type, CodecType::Constant);
      }
    }
    Verify(dir, CodecType::Auto, kRowGroup, value);
  }
  const Value then_varied = [](uint64_t g) -> std::optional<std::string> {
    return g < 2 * kRowGroup ? "the first two row groups hold this value"
                             : absl::StrCat("value-", g % 50);
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, CodecType::Auto, {}, 4 * kRowGroup, kRowGroup, then_varied);
  {
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    const auto blocks = col->DataBlocks();
    ASSERT_GE(blocks.size(), 4U);
    EXPECT_EQ(blocks[0].codec->type, CodecType::Constant);
    EXPECT_EQ(blocks[1].codec->type, CodecType::Constant);
    EXPECT_NE(blocks[2].codec->type, CodecType::Constant);
  }
  Verify(dir, CodecType::Auto, 4 * kRowGroup, then_varied);
}

TEST_F(ColCodecsTest, ADictionarySegmentEndsOnAVectorBoundary) {
  using irs::codecs::CodecType;
  constexpr uint32_t kRowGroup = 75 * STANDARD_VECTOR_SIZE;
  constexpr uint64_t kRows = 2 * kRowGroup;
  const Value repeating = [](uint64_t g) -> std::optional<std::string> {
    return Pseudo(Scattered(g, 500), 24);
  };
  const Value long_pairs = [](uint64_t g) -> std::optional<std::string> {
    return Pseudo(g / 2, 600);
  };
  const irs::ColCodecParams params{.segment_target = 64 * 1024};
  for (const auto codec :
       {CodecType::DictLz4, CodecType::DictZstd, CodecType::DictZxc,
        CodecType::DictFsst, CodecType::DictFsstPlus, CodecType::DictOnpair,
        CodecType::DictFsst12}) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    {
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, codec, params, kRows, kRowGroup, repeating);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      ASSERT_GT(col->DataBlocks().size(), 2 * kRows / kRowGroup);
      uint64_t row = 0;
      for (const auto& block : col->DataBlocks()) {
        row += block.tuple_count;
        EXPECT_EQ(row % STANDARD_VECTOR_SIZE, 0U) << row;
      }
      Verify(dir, codec, kRows, repeating);
    }
    {
      constexpr uint64_t kLongRows = 6 * STANDARD_VECTOR_SIZE;
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, codec, params, kLongRows, kLongRows, long_pairs);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      for (const auto& block : col->DataBlocks()) {
        EXPECT_LT(block.tuple_count, STANDARD_VECTOR_SIZE);
      }
      Verify(dir, codec, kLongRows, long_pairs);
    }
  }
}

TEST_F(ColCodecsTest, AutoMeasuresTheSortedOrderOfAnLz4Dictionary) {
  using irs::codecs::CodecType;
  constexpr uint64_t kRows = 30000;
  duckdb::DBConfig::GetConfig(Db()).SetDisabledCompressionMethods(
    {duckdb::CompressionType::COMPRESSION_FSST,
     duckdb::CompressionType::COMPRESSION_DICT_FSST});
  const auto sorted = [&](const Value& value, irs::WriteTier tier) {
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, CodecType::Auto, {.tier = tier}, kRows, kRows, value);
    {
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      EXPECT_NE(col, nullptr);
      if (!col) {
        return false;
      }
      for (const auto& block : col->DataBlocks()) {
        EXPECT_EQ(block.codec->type, CodecType::DictLz4)
          << magic_enum::enum_name(block.codec->type);
      }
    }
    const auto orders = SegmentInfo(dir, "sorted");
    return !orders.empty() && absl::c_all_of(orders, [](const std::string& s) {
      return s == "true";
    });
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Merge ? "merge" : "flush");
    EXPECT_TRUE(sorted(
      [](uint64_t g) -> std::optional<std::string> {
        return Clustered(Scattered(g, 4000));
      },
      tier));
    EXPECT_FALSE(sorted(
      [](uint64_t g) -> std::optional<std::string> {
        return absl::StrCat("tenant-", (g * 7919) % 3000, "/item");
      },
      tier));
  }
}

TEST_F(ColCodecsTest, DictionaryFiltersMatchTheValues) {
  struct Case {
    Value value;
    std::vector<std::string> keys;
  };
  const Case cases[] = {
    {[](uint64_t g) -> std::optional<std::string> {
       if (g % 13 == 0) {
         return std::nullopt;
       }
       char buf[48];
       std::snprintf(buf, sizeof buf, "tenant-%05llu/item",
                     static_cast<unsigned long long>((g * 7919) % 3000));
       return std::string{buf};
     },
     {"tenant-00000/item", "tenant-01499/item", "tenant-01499/itex",
      "tenant-02999/item", "a", "z"}},
    {[](uint64_t g) -> std::optional<std::string> {
       if (g % 13 == 0) {
         return std::nullopt;
       }
       const auto k = (g * 7919) % 600;
       std::string nested(k % 40, 'a');
       nested.append(k % 3, '\0');
       if (k % 5 == 0) {
         nested.append("tail-").append(std::to_string(k % 7));
       }
       return nested;
     },
     {"", "a", std::string(8, 'a'), std::string(9, 'a'),
      std::string(16, 'a') + std::string(1, '\0'),
      std::string(8, 'a') + std::string(2, '\0'), "aaaaaaaatail-3", "b"}},
    {[](uint64_t g) -> std::optional<std::string> {
       if (g % 13 == 0) {
         return std::nullopt;
       }
       return absl::StrCat("status-", (g * 7919) % 40);
     },
     {"status-0", "status-17", "status-39", "status-4", "status-40", "", "z"}},
    {[](uint64_t g) -> std::optional<std::string> {
       if (g % 13 == 0) {
         return std::nullopt;
       }
       return Clustered(Scattered(g, 4000));
     },
     {Clustered(0), Clustered(1), Clustered(2001), Clustered(3999),
      Clustered(0) + "x", Pseudo(7, 200), "", "~"}},
  };
  constexpr uint64_t kRows = 30000;
  for (const auto& [value, keys] : cases) {
    for (const auto codec :
         {irs::codecs::CodecType::DictFsst,
          irs::codecs::CodecType::DictFsstPlus,
          irs::codecs::CodecType::DictOnpair,
          irs::codecs::CodecType::DictFsst12, irs::codecs::CodecType::DictLz4,
          irs::codecs::CodecType::DictZstd, irs::codecs::CodecType::DictZxc}) {
      SCOPED_TRACE(magic_enum::enum_name(codec));
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, codec, {}, kRows, 8192, value);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
      for (const std::string& key : keys) {
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
}

TEST_F(ColCodecsTest, SortedDictionariesFollowAMovingTopNBound) {
  constexpr uint64_t kRows = 9 * STANDARD_VECTOR_SIZE + 100;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 11 == 0) {
      return std::nullopt;
    }
    if (g % 7 == 0) {
      return std::string{};
    }
    return absl::StrCat("phrase-", (g * 7919) % 5000);
  };
  using Cmp = duckdb::ExpressionType;
  const auto column = [] {
    return duckdb::make_uniq<duckdb::BoundReferenceExpression>(
      duckdb::LogicalType::VARCHAR, 0);
  };
  const auto not_empty = [&] {
    return duckdb::BoundComparisonExpression::Create(
      Cmp::COMPARE_NOTEQUAL, column(),
      duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value{""}));
  };
  const auto optional = [](duckdb::unique_ptr<duckdb::Expression> dynamic) {
    return duckdb::CreateSelectivityOptionalFilterExpression(
      std::move(dynamic), duckdb::LogicalType::VARCHAR, 1, 1);
  };
  enum class Shape : uint8_t {
    Bound,
    OverAConstant,
    IntegerBound,
    BoundFirst,
  };
  for (const auto shape : {Shape::Bound, Shape::OverAConstant,
                           Shape::IntegerBound, Shape::BoundFirst}) {
    for (const auto cmp :
         {Cmp::COMPARE_LESSTHANOREQUALTO, Cmp::COMPARE_LESSTHAN,
          Cmp::COMPARE_GREATERTHAN, Cmp::COMPARE_GREATERTHANOREQUALTO}) {
      std::vector<std::string> bounds = {
        "phrase-4", "phrase-3",  "phrase-3", "phrase-2999",
        "phrase-1", "phrase-00", "phrase-0", "a",
      };
      const bool ascending = cmp == Cmp::COMPARE_GREATERTHAN ||
                             cmp == Cmp::COMPARE_GREATERTHANOREQUALTO;
      if (ascending) {
        absl::c_reverse(bounds);
      }
      for (const auto codec : {irs::codecs::CodecType::DictFsst,
                               irs::codecs::CodecType::DictFsst12,
                               irs::codecs::CodecType::DictLz4}) {
        SCOPED_TRACE(absl::StrCat(magic_enum::enum_name(codec), " ",
                                  duckdb::EnumUtil::ToString(cmp), " shape ",
                                  static_cast<int>(shape)));
        tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
        Write(dir, codec, {}, kRows, 4 * STANDARD_VECTOR_SIZE, value);
        irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
        const auto* col = r.Column(kField);
        ASSERT_NE(col, nullptr);
        auto bound = duckdb::make_shared_ptr<duckdb::DynamicFilterData>(
          cmp, shape == Shape::IntegerBound ? duckdb::Value::INTEGER(0)
                                            : duckdb::Value{""});
        auto dynamic = shape == Shape::OverAConstant
                         ? duckdb::CreateDynamicFilterExpression(
                             bound, duckdb::LogicalType::VARCHAR,
                             duckdb::make_uniq<duckdb::BoundConstantExpression>(
                               duckdb::Value{"phrase-2"}))
                         : duckdb::CreateDynamicFilterExpression(
                             bound, duckdb::LogicalType::VARCHAR);
        auto expr = shape == Shape::BoundFirst
                      ? duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
                          Cmp::CONJUNCTION_AND, optional(std::move(dynamic)),
                          duckdb::BoundComparisonExpression::Create(
                            Cmp::COMPARE_EQUAL, column(), column()))
                      : duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
                          Cmp::CONJUNCTION_AND, not_empty(),
                          optional(std::move(dynamic)));
        const duckdb::ExpressionFilter filter{std::move(expr)};
        duckdb::Connection con{_db};
        auto filter_state =
          duckdb::TableFilterState::Initialize(*con.context, filter);
        auto state = col->InitScan(r.Ctx());
        const bool exact = shape == Shape::Bound &&
                           irs::codecs::Sorted(irs::codecs::LeafOf(codec));
        for (uint64_t anchor = 0; anchor < kRows;
             anchor += STANDARD_VECTOR_SIZE) {
          const auto vector = anchor / STANDARD_VECTOR_SIZE;
          if (vector != 0) {
            const auto& next = bounds[(vector - 1) % bounds.size()];
            bound->SetValue(
              shape == Shape::IntegerBound
                ? duckdb::Value::INTEGER(static_cast<int32_t>(next.size()))
                : duckdb::Value{next});
          }
          const auto key = shape == Shape::IntegerBound
                             ? std::string{}
                             : duckdb::StringValue::Get(bound->constant);
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
            const bool candidate =
              v && (shape == Shape::BoundFirst || !v->empty());
            bool within = candidate && vector != 0;
            switch (cmp) {
              case Cmp::COMPARE_LESSTHANOREQUALTO:
                within = within && *v <= key;
                break;
              case Cmp::COMPARE_LESSTHAN:
                within = within && *v < key;
                break;
              case Cmp::COMPARE_GREATERTHAN:
                within = within && *v > key;
                break;
              default:
                within = within && *v >= key;
                break;
            }
            const bool got = next < kept && sel.get_index(next) == i;
            next += got ? 1 : 0;
            ASSERT_TRUE(!got || candidate) << "row " << anchor + i;
            if (exact) {
              ASSERT_EQ(got, vector == 0 ? candidate : within)
                << "row " << anchor + i << " bound " << key;
            } else if (shape != Shape::OverAConstant &&
                       shape != Shape::IntegerBound) {
              ASSERT_TRUE(!within || got) << "row " << anchor + i;
            }
          }
          ASSERT_EQ(next, kept);
        }
      }
    }
  }
}

TEST_F(ColCodecsTest, FiltersOnAConstantTextRowGroupMatchItsValue) {
  constexpr uint64_t kRows = 3 * STANDARD_VECTOR_SIZE + 100;
  const std::string text = "a value longer than twelve bytes";
  const Value value = [&](uint64_t) -> std::optional<std::string> {
    return text;
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::Auto, {}, kRows, 4096, value);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  for (const auto& block : col->DataBlocks()) {
    ASSERT_EQ(block.codec->type, irs::codecs::CodecType::Constant);
  }
  duckdb::Connection con{_db};
  using Cmp = duckdb::ExpressionType;
  for (const std::string& key :
       {text, std::string{"a value"}, text + "!", std::string{}}) {
    for (const auto cmp :
         {Cmp::COMPARE_EQUAL, Cmp::COMPARE_NOTEQUAL, Cmp::COMPARE_LESSTHAN,
          Cmp::COMPARE_GREATERTHANOREQUALTO}) {
      const bool expected = cmp == Cmp::COMPARE_EQUAL      ? text == key
                            : cmp == Cmp::COMPARE_NOTEQUAL ? text != key
                            : cmp == Cmp::COMPARE_LESSTHAN ? text < key
                                                           : text >= key;
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
        for (duckdb::idx_t i = 0; i < span; i += 3) {
          sel.set_index(offered++, i);
        }
        duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
        const auto kept =
          col->GatherFilter(state, anchor, span, sel, offered, filter,
                            *filter_state, irs::NullCheckKind::None, out);
        ASSERT_EQ(kept, expected ? offered : 0)
          << key << " " << duckdb::EnumUtil::ToString(cmp);
        for (duckdb::idx_t k = 0; k < kept; ++k) {
          ASSERT_EQ(sel.get_index(k), 3 * k);
          ASSERT_EQ(out.GetValue(sel.get_index(k)).ToString(), text);
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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::DictLz4, {}, kRows, 65536, value);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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

TEST_F(ColCodecsTest,
       FiltersThatKeepMostEntriesDecodeTheDictionaryOnlyForRowsThatNeedIt) {
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 17 == 0) {
      return std::nullopt;
    }
    return absl::StrCat("https://example.org/catalog/",
                        absl::Dec((g * 7919) % 20000, absl::kZeroPad6),
                        "/reviews");
  };
  constexpr uint64_t kRows = 60000;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::DictFsst, {}, kRows, 65536, value);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  std::vector<uint64_t> segment_ends;
  for (uint64_t end = 0; const auto& block : col->DataBlocks()) {
    end += block.tuple_count;
    segment_ends.push_back(end);
  }
  duckdb::Connection con{_db};
  static constexpr std::string_view kKey =
    "https://example.org/catalog/012345/reviews";
  static constexpr std::string_view kPiece = "catalog/01";
  const auto column = [] {
    return duckdb::make_uniq<duckdb::BoundReferenceExpression>(
      duckdb::LogicalType::VARCHAR, 0);
  };
  const duckdb::ExpressionFilter differs{
    duckdb::BoundComparisonExpression::Create(
      duckdb::ExpressionType::COMPARE_NOTEQUAL, column(),
      duckdb::make_uniq<duckdb::BoundConstantExpression>(
        duckdb::Value{std::string{kKey}}))};
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> args;
  args.push_back(column());
  args.push_back(duckdb::make_uniq<duckdb::BoundConstantExpression>(
    duckdb::Value{std::string{kPiece}}));
  const duckdb::ExpressionFilter contains{
    duckdb::make_uniq<duckdb::BoundFunctionExpression>(
      duckdb::BoundScalarFunction(
        duckdb::ContainsFun::GetFunctions().GetFunctionByOffset(0)),
      std::move(args), nullptr)};
  auto& cache = Db().GetObjectCache();
  const auto room = cache.GetMaxMemory();
  const absl::Cleanup restore = [&] { cache.SetMaxMemory(room); };
  struct Kind {
    const char* name;
    const duckdb::ExpressionFilter* filter;
    bool (*passes)(std::string_view);
    duckdb::VectorType thinned_with_room;
  };
  struct Pass {
    bool room;
    uint64_t stride;
    duckdb::VectorType type;
  };
  const Kind kinds[] = {
    {"answered by ranges", &differs,
     [](std::string_view v) { return v != kKey; },
     duckdb::VectorType::DICTIONARY_VECTOR},
    {"evaluated per entry", &contains,
     [](std::string_view v) { return v.find(kPiece) != v.npos; },
     duckdb::VectorType::FLAT_VECTOR},
  };
  for (const auto& kind : kinds) {
    const auto& filter = *kind.filter;
    for (const auto& pass :
         {Pass{false, 97, duckdb::VectorType::FLAT_VECTOR},
          Pass{false, 1, duckdb::VectorType::DICTIONARY_VECTOR},
          Pass{true, 97, kind.thinned_with_room}}) {
      SCOPED_TRACE(absl::StrCat(kind.name, ", ", pass.room ? "room" : "no room",
                                ", every ", pass.stride, " rows"));
      for (const auto& cached : CacheKeys(r, *col)) {
        cache.Delete(cached);
      }
      cache.SetMaxMemory(pass.room ? room : cache.GetCurrentMemory());
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
          if ((anchor + i) % pass.stride == 0) {
            sel.set_index(offered++, i);
          }
        }
        duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
        const auto kept =
          col->GatherFilter(state, anchor, span, sel, offered, filter,
                            *filter_state, irs::NullCheckKind::None, out);
        if (span == STANDARD_VECTOR_SIZE &&
            anchor + span <= *absl::c_upper_bound(segment_ends, anchor)) {
          EXPECT_EQ(out.GetVectorType(), pass.type) << "anchor " << anchor;
        }
        duckdb::idx_t next = 0;
        for (duckdb::idx_t i = 0; i < span; ++i) {
          const auto g = anchor + i;
          const auto v = value(g);
          const bool expected = g % pass.stride == 0 && v && kind.passes(*v);
          const bool got = next < kept && sel.get_index(next) == i;
          if (got) {
            EXPECT_EQ(out.GetValue(i).ToString(), *v) << "row " << g;
          }
          next += got ? 1 : 0;
          ASSERT_EQ(got, expected) << "row " << g;
        }
      }
    }
  }
}

TEST_F(ColCodecsTest, DenseSelectionsAfterSparseOnesFilterOnTheDictionary) {
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    char buf[32];
    std::snprintf(buf, sizeof buf, "key-%06llu",
                  static_cast<unsigned long long>((g * 7919) % 20000));
    return std::string{buf};
  };
  constexpr uint64_t kRows = 60000;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::DictLz4, {}, kRows, 65536, value);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  duckdb::Connection con{_db};
  const std::string key = "key-012345";
  const duckdb::ExpressionFilter filter{
    duckdb::BoundComparisonExpression::Create(
      duckdb::ExpressionType::COMPARE_NOTEQUAL,
      duckdb::make_uniq<duckdb::BoundReferenceExpression>(
        duckdb::LogicalType::VARCHAR, 0),
      duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value{key}))};
  auto filter_state =
    duckdb::TableFilterState::Initialize(*con.context, filter);
  auto state = col->InitScan(r.Ctx());
  for (uint64_t anchor = 0; anchor < 4 * STANDARD_VECTOR_SIZE;
       anchor += STANDARD_VECTOR_SIZE) {
    const uint64_t stride = anchor == 0 ? 9 : 1;
    duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
    duckdb::idx_t offered = 0;
    for (duckdb::idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
      if ((anchor + i) % stride == 0) {
        sel.set_index(offered++, i);
      }
    }
    duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    const auto kept =
      col->GatherFilter(state, anchor, STANDARD_VECTOR_SIZE, sel, offered,
                        filter, *filter_state, irs::NullCheckKind::None, out);
    if (stride == 1) {
      EXPECT_EQ(out.GetVectorType(), duckdb::VectorType::DICTIONARY_VECTOR)
        << "anchor " << anchor;
    }
    duckdb::idx_t next = 0;
    for (duckdb::idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
      const auto g = anchor + i;
      const auto v = value(g);
      const bool expected = g % stride == 0 && *v != key;
      const bool got = next < kept && sel.get_index(next) == i;
      if (got) {
        EXPECT_EQ(out.GetValue(i).ToString(), *v) << "row " << g;
      }
      next += got ? 1 : 0;
      ASSERT_EQ(got, expected) << "row " << g;
    }
  }
}

TEST_F(ColCodecsTest, SelectionsOfLz4FramesMatchTheValues) {
  struct Shape {
    const char* name;
    uint64_t rows;
    Value value;
  };
  const std::vector<Shape> shapes{
    {"short", 40000,
     [](uint64_t g) -> std::optional<std::string> {
       if (g % 13 == 0) {
         return std::nullopt;
       }
       return g % 5 == 0 ? std::string{} : Pseudo(g, 1 + g % 12);
     }},
    {"medium", 40000,
     [](uint64_t g) -> std::optional<std::string> {
       return "row " + std::to_string(g) + " " + Pseudo(g % 977, 30 + g % 60);
     }},
    {"long", 6000,
     [](uint64_t g) -> std::optional<std::string> {
       if (g % 17 == 0) {
         return std::nullopt;
       }
       return Pseudo(g % 211, 300 + g % 900) + std::to_string(g);
     }},
  };
  size_t trained = 0;
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    for (const auto& shape : shapes) {
      SCOPED_TRACE(testing::Message()
                   << shape.name << " tier " << static_cast<int>(tier));
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, irs::codecs::CodecType::Lz4, {.tier = tier}, shape.rows, 16384,
            shape.value);
      trained += absl::c_count(SegmentInfo(dir, "dictionary"), "local");
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      for (const uint64_t stride : {2, 3, 7, 41, 250, 1000}) {
        for (const bool appending : {false, true}) {
          SCOPED_TRACE(testing::Message()
                       << "stride " << stride << " appending " << appending);
          std::vector<uint64_t> picked;
          for (uint64_t g = stride / 2; g < shape.rows; g += stride) {
            picked.push_back(g);
          }
          if (picked.back() != shape.rows - 1) {
            picked.push_back(shape.rows - 1);
          }
          auto state = col->InitScan(r.Ctx());
          constexpr size_t kBatch = STANDARD_VECTOR_SIZE / 2;
          std::unique_ptr<duckdb::Vector> out;
          for (size_t i = 0; i < picked.size(); i += kBatch) {
            const auto take = std::min(picked.size() - i, kBatch);
            const duckdb::idx_t at = appending ? (i / kBatch % 2) * kBatch : 0;
            if (at == 0) {
              out = std::make_unique<duckdb::Vector>(
                duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE);
              duckdb::FlatVector::ValidityMutable(*out).Reset(
                STANDARD_VECTOR_SIZE);
            }
            irs::column_internal::GatherRows(
              *col, state, Rows2{&picked[i], take}, *out, at, !appending);
            if (!appending) {
              out->Flatten(take);
            }
            for (size_t k = 0; k < take; ++k) {
              ExpectRow(*out, at + k, picked[i + k], shape.value);
            }
            if (HasFatalFailure()) {
              return;
            }
          }
        }
      }
    }
  }
  EXPECT_GT(trained, 0u);
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
  for (const auto codec :
       {irs::codecs::CodecType::Zxc, irs::codecs::CodecType::DictZxc}) {
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
  for (const auto codec :
       {irs::codecs::CodecType::DictLz4, irs::codecs::CodecType::DictFsst}) {
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, codec, {}, kRows, 4096, value);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    uint64_t total = 0;
    for (const auto& block : col->DataBlocks()) {
      const auto length =
        duckdb::StringStats::TotalStringLength(block.statistics);
      ASSERT_TRUE(length.IsValid());
      total += length.GetIndex();
    }
    EXPECT_EQ(total, expected) << magic_enum::enum_name(codec);
  }
}

TEST_F(ColCodecsTest, MappedFileAlignedBlocks) {
  const auto path = test_dir() / "col_codecs_mmap";
  std::filesystem::create_directories(path);
  tests::IndexDir dir{Db(), path.string()};
  struct Arm {
    irs::codecs::CodecType codec;
    const Value* value;
  };
  const Arm arms[] = {
    {irs::codecs::CodecType::DictLz4, &kLowCardinalityWithNulls},
    {irs::codecs::CodecType::DictZstd, &kUniqueShort},
    {irs::codecs::CodecType::Lz4, &kLongTextWithNulls},
    {irs::codecs::CodecType::DictFsst, &kLowCardinalityWithNulls},
    {irs::codecs::CodecType::Fsst, &kLongTextWithNulls},
    {irs::codecs::CodecType::DictZxc, &kLowCardinalityWithNulls},
    {irs::codecs::CodecType::Zxc, &kLongTextWithNulls},
    {irs::codecs::CodecType::Uncompressed, &kUniqueShort},
  };
  for (const auto& arm : arms) {
    Write(dir, arm.codec, {}, 5000, 2048, *arm.value);
    {
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      for (const auto& block : col->DataBlocks()) {
        EXPECT_EQ(block.file_offset % 8, 0u)
          << magic_enum::enum_name(arm.codec);
      }
    }
    Verify(dir, arm.codec, 5000, *arm.value);
    dir.Remove(irs::FileName(kSeg));
  }
}

TEST_F(ColCodecsTest, MappedFileNumericCodecs) {
  const auto path = test_dir() / "col_codecs_mmap_numeric";
  std::filesystem::create_directories(path);
  tests::IndexDir dir{Db(), path.string()};
  constexpr uint64_t kRows = 20000;
  constexpr irs::field_id kRle = 1;
  constexpr irs::field_id kBitpacking = 2;
  constexpr irs::field_id kAlp = 3;
  constexpr irs::field_id kBool = 4;
  {
    irs::ColWriter w{Db(), dir.Tracker(kSeg)};
    auto& rle = w.OpenColumn(
      kRle, duckdb::LogicalType::BIGINT,
      {.row_group_size = 4096, .compression = irs::codecs::CodecType::Rle});
    auto& bp =
      w.OpenColumn(kBitpacking, duckdb::LogicalType::INTEGER,
                   {.row_group_size = 4096,
                    .compression = irs::codecs::CodecType::Bitpacking});
    auto& alp = w.OpenColumn(
      kAlp, duckdb::LogicalType::DOUBLE,
      {.row_group_size = 4096, .compression = irs::codecs::CodecType::Alp});
    auto& bools = w.OpenColumn(
      kBool, duckdb::LogicalType::BOOLEAN,
      {.row_group_size = 4096, .compression = irs::codecs::CodecType::Roaring});
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
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* c_rle = r.Column(kRle);
  const auto* c_bp = r.Column(kBitpacking);
  const auto* c_alp = r.Column(kAlp);
  const auto* c_bool = r.Column(kBool);
  ASSERT_NE(c_rle, nullptr);
  ASSERT_NE(c_bp, nullptr);
  ASSERT_NE(c_alp, nullptr);
  ASSERT_NE(c_bool, nullptr);
  EXPECT_EQ(c_rle->DataBlocks().front().codec->type,
            irs::codecs::CodecType::Rle);
  EXPECT_EQ(c_bp->DataBlocks().front().codec->type,
            irs::codecs::CodecType::Bitpacking);
  EXPECT_EQ(c_alp->DataBlocks().front().codec->type,
            irs::codecs::CodecType::Alp);
  EXPECT_EQ(c_bool->DataBlocks().front().codec->type,
            irs::codecs::CodecType::Roaring);

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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker(kSeg)};
    auto& rle = w.OpenColumn(
      kRle, duckdb::LogicalType::BIGINT,
      {.row_group_size = 4096, .compression = irs::codecs::CodecType::Rle});
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
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* c_rle = r.Column(kRle);
  const auto* c_num = r.Column(kNumeric);
  ASSERT_NE(c_rle, nullptr);
  ASSERT_NE(c_num, nullptr);
  ASSERT_GT(c_rle->DataBlocks().size(), 5);
  for (const auto& block : c_rle->DataBlocks()) {
    ASSERT_EQ(block.codec->type, irs::codecs::CodecType::Rle);
  }
  ASSERT_TRUE(irs::codecs::IsNumeric(c_num->DataBlocks().front().codec->type));

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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::DictLz4, {}, kRows, 8192, value);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    {duckdb::CompressionType::COMPRESSION_DICT_ZXC, "DICT_ZXC"},
    {duckdb::CompressionType::COMPRESSION_ZXC, "ZXC"},
    {duckdb::CompressionType::COMPRESSION_COL_DICT_FSST_PLUS, "DICT_FSST_PLUS"},
    {duckdb::CompressionType::COMPRESSION_COL_FSST_PLUS, "FSST_PLUS"},
    {duckdb::CompressionType::COMPRESSION_COL_DICT_ONPAIR, "DICT_ONPAIR"},
    {duckdb::CompressionType::COMPRESSION_COL_ONPAIR, "ONPAIR"},
    {duckdb::CompressionType::COMPRESSION_COL_DICT_FSST12, "DICT_FSST12"},
    {duckdb::CompressionType::COMPRESSION_COL_FSST12, "FSST12"},
    {duckdb::CompressionType::COMPRESSION_COL_DELTA, "DELTA"},
    {duckdb::CompressionType::COMPRESSION_COL_DICT, "DICT"},
    {duckdb::CompressionType::COMPRESSION_COL_DELTA_LZ4, "DELTA_LZ4"},
    {duckdb::CompressionType::COMPRESSION_COL_RLE_LZ4, "RLE_LZ4"},
    {duckdb::CompressionType::COMPRESSION_COL_DELTA_ZSTD, "DELTA_ZSTD"},
    {duckdb::CompressionType::COMPRESSION_COL_RLE_ZSTD, "RLE_ZSTD"},
  };
  constexpr size_t kRetired = 2;
  EXPECT_EQ(
    static_cast<size_t>(duckdb::CompressionType::COMPRESSION_SERENEDB_COUNT) -
      static_cast<size_t>(duckdb::CompressionType::COMPRESSION_DICT_LZ4),
    std::size(names) + kRetired);
  for (const auto& [type, name] : names) {
    SCOPED_TRACE(name);
    EXPECT_TRUE(duckdb::IsSereneDBCompressionType(type));
    EXPECT_EQ(duckdb::EnumUtil::ToString(type), name);
    EXPECT_EQ(duckdb::CompressionTypeToString(type), name);
    const auto codec = irs::codecs::FindCodec(name);
    ASSERT_NE(codec, irs::codecs::kNoCodec);
    EXPECT_EQ(magic_enum::enum_name(codec), name);
    EXPECT_EQ(irs::codecs::FindCodec(type), codec);
  }
  for (const auto type : {duckdb::CompressionType::COMPRESSION_UNCOMPRESSED,
                          duckdb::CompressionType::COMPRESSION_RLE,
                          duckdb::CompressionType::COMPRESSION_BITPACKING,
                          duckdb::CompressionType::COMPRESSION_ALP,
                          duckdb::CompressionType::COMPRESSION_ALPRD,
                          duckdb::CompressionType::COMPRESSION_ROARING,
                          duckdb::CompressionType::COMPRESSION_CONSTANT,
                          duckdb::CompressionType::COMPRESSION_EMPTY,
                          duckdb::CompressionType::COMPRESSION_FSST,
                          duckdb::CompressionType::COMPRESSION_DICT_FSST,
                          duckdb::CompressionType::COMPRESSION_ZSTD}) {
    SCOPED_TRACE(duckdb::CompressionTypeToString(type));
    const auto codec = irs::codecs::FindCodec(type);
    ASSERT_NE(codec, irs::codecs::kNoCodec);
    EXPECT_EQ(irs::codecs::FindCodec(magic_enum::enum_name(codec)), codec);
  }
  const auto automatic = irs::codecs::FindCodec(
    duckdb::CompressionTypeToString(duckdb::CompressionType::COMPRESSION_AUTO));
  EXPECT_EQ(automatic, irs::codecs::CodecType::Auto);
  EXPECT_EQ(irs::codecs::FindCodec(duckdb::CompressionType::COMPRESSION_AUTO),
            automatic);
  for (const auto type : {duckdb::CompressionType::COMPRESSION_DICTIONARY,
                          duckdb::CompressionType::COMPRESSION_PFOR_DELTA,
                          duckdb::CompressionType::COMPRESSION_CHIMP,
                          duckdb::CompressionType::COMPRESSION_PATAS}) {
    const auto name = duckdb::CompressionTypeToString(type);
    SCOPED_TRACE(name);
    EXPECT_EQ(irs::codecs::FindCodec(name), irs::codecs::kNoCodec);
    EXPECT_EQ(irs::codecs::FindCodec(type), irs::codecs::kNoCodec);
  }
  for (const auto count :
       {duckdb::CompressionType::ENUM_SIZE,
        duckdb::CompressionType::COMPRESSION_SERENEDB_COUNT}) {
    EXPECT_EQ(irs::codecs::FindCodec(count), irs::codecs::kNoCodec);
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
    {"bitpacking", duckdb::CompressionType::COMPRESSION_BITPACKING},
    {"delta", duckdb::CompressionType::COMPRESSION_COL_DELTA},
    {"rle", duckdb::CompressionType::COMPRESSION_RLE},
    {"dict", duckdb::CompressionType::COMPRESSION_COL_DICT},
    {"delta_lz4", duckdb::CompressionType::COMPRESSION_COL_DELTA_LZ4},
    {"rle_lz4", duckdb::CompressionType::COMPRESSION_COL_RLE_LZ4},
    {"delta_zstd", duckdb::CompressionType::COMPRESSION_COL_DELTA_ZSTD},
    {"rle_zstd", duckdb::CompressionType::COMPRESSION_COL_RLE_ZSTD},
  };
  for (const auto& [name, type] : parsed) {
    SCOPED_TRACE(name);
    EXPECT_EQ(
      duckdb::EnumUtil::FromString<duckdb::CompressionType>(std::string{name}),
      type);
    const auto codec = irs::codecs::FindCodec(name);
    ASSERT_NE(codec, irs::codecs::kNoCodec);
    EXPECT_TRUE(absl::EqualsIgnoreCase(magic_enum::enum_name(codec), name));
    EXPECT_EQ(irs::codecs::FindCodec(type), codec);
  }
}

TEST(ColCodecNames, SetColumnCompressionAlterTypeRoundTrips) {
  constexpr auto kType = duckdb::AlterTableType::SET_COLUMN_COMPRESSION;
  const std::string_view name = duckdb::EnumUtil::ToChars(kType);
  EXPECT_EQ(name, "SET_COLUMN_COMPRESSION");
  EXPECT_EQ(duckdb::EnumUtil::FromString<duckdb::AlterTableType>(name), kType);
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
  irs::codecs::CodecType block_codec = irs::codecs::CodecType::Auto;
  {
    tests::IndexDir dir{Db(), path.string()};
    Write(dir, irs::codecs::CodecType::DictLz4, {}, 5000, 2048, random_codes);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    ASSERT_FALSE(col->DataBlocks().empty());
    offset = col->DataBlocks().front().file_offset;
    size = col->DataBlocks().front().byte_size;
    block_codec = col->DataBlocks().front().codec->type;
  }
  const auto file = path / irs::FileName(kSeg);
  std::string bytes;
  {
    std::ifstream in{file, std::ios::binary};
    bytes.assign(std::istreambuf_iterator<char>{in}, {});
  }
  ASSERT_LE(offset + size, bytes.size());
  auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
  const auto h = irs::codecs::Header::Parse(block, size, block_codec);
  ASSERT_EQ(irs::codecs::ShapeOf(block_codec), irs::codecs::Shape::Dictionary);
  auto* codes = block + h.off_codes;
  const auto frames = irs::codecs::ParseFrames(
    codes, h.off_symtab - h.off_codes, h.row_count, h.codes);
  ASSERT_EQ(irs::codecs::TransformName(irs::codecs::ShapeOf(h.codes), frames),
            "bitpacking");
  auto* bounds = codes + frames.off_frames + sizeof(uint32_t) + frames.width;
  for (auto* bound : {bounds, bounds + frames.width}) {
    uint64_t value = 0;
    std::memcpy(&value, bound, frames.width);
    value += h.entry_count;
    std::memcpy(bound, &value, frames.width);
  }
  {
    std::ofstream out{file, std::ios::binary | std::ios::trunc};
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  }
  tests::IndexDir dir{Db(), path.string()};
  const auto scan = [&] {
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    auto state = col->InitScan(r.Ctx());
    duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    col->Scan(state, out, STANDARD_VECTOR_SIZE);
  };
  ExpectCorrupted(scan,
                  "Corrupted text segment: dictionary code is out of range");
}

TEST_F(ColCodecsTest, CorruptedSegmentSizesAreRejected) {
  struct Case {
    std::string_view name;
    void (*corrupt)(irs::codecs::Header&);
    const char* error;
  };
  const Case cases[] = {
    {"frames", [](irs::codecs::Header& h) { h.off_frames = 0; },
     "Corrupted text segment: header is inconsistent"},
    {"entries",
     [](irs::codecs::Header& h) {
       h.length_width = 0;
       h.entry_count = h.row_count + 2;
     },
     "Corrupted text segment: header is inconsistent"},
    {"raw_bytes",
     [](irs::codecs::Header& h) { h.raw_bytes = uint64_t{1} << 46; },
     "Corrupted text segment: frame table is inconsistent"},
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
    irs::codecs::CodecType block_codec = irs::codecs::CodecType::Auto;
    {
      tests::IndexDir dir{Db(), path.string()};
      Write(dir, irs::codecs::CodecType::DictLz4, {}, 5000, 2048, values);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      offset = col->DataBlocks().front().file_offset;
      size = col->DataBlocks().front().byte_size;
      block_codec = col->DataBlocks().front().codec->type;
    }
    const auto file = path / irs::FileName(kSeg);
    std::string bytes;
    {
      std::ifstream in{file, std::ios::binary};
      bytes.assign(std::istreambuf_iterator<char>{in}, {});
    }
    ASSERT_LE(offset + size, bytes.size());
    auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
    auto h = irs::codecs::Header::Parse(block, size, block_codec);
    c.corrupt(h);
    duckdb::Store(h, block);
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    tests::IndexDir dir{Db(), path.string()};
    const auto scan = [&] {
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
    };
    ExpectCorrupted(scan, c.error);
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
     "Corrupted text segment: header is inconsistent"},
    {"bytes",
     [](irs::codecs::Header& h, duckdb::data_ptr_t block) {
       std::memset(block + h.off_symtab, 0xFF, h.symtab_size);
     },
     "Corrupted text segment: dictionary is inconsistent"},
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
    irs::codecs::CodecType block_codec = irs::codecs::CodecType::Auto;
    {
      tests::IndexDir dir{Db(), path.string()};
      Write(dir, irs::codecs::CodecType::Lz4, {}, 20000, DEFAULT_ROW_GROUP_SIZE,
            values);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      offset = col->DataBlocks().front().file_offset;
      size = col->DataBlocks().front().byte_size;
      block_codec = col->DataBlocks().front().codec->type;
    }
    const auto file = path / irs::FileName(kSeg);
    std::string bytes;
    {
      std::ifstream in{file, std::ios::binary};
      bytes.assign(std::istreambuf_iterator<char>{in}, {});
    }
    ASSERT_LE(offset + size, bytes.size());
    auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
    auto h = irs::codecs::Header::Parse(block, size, block_codec);
    ASSERT_EQ(h.flags, irs::codecs::FrameDictionary::Local);
    c.corrupt(h, block);
    duckdb::Store(h, block);
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    tests::IndexDir dir{Db(), path.string()};
    const auto scan = [&] {
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
    };
    const auto fetch = [&] {
      irs::ColReader r{Db(), dir.Fs(), dir.Root(),
                       kSeg, true,     duckdb::MappedFileAccess::RANDOM};
      const auto* col = r.Column(kField);
      irs::ColumnReader::PointReader cursor{r, *col};
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, 1};
      cursor.FetchRow(7, out, 0);
    };
    ExpectCorrupted(scan, c.error);
    ExpectCorrupted(fetch, c.error);
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
     "Corrupted text segment: header is inconsistent"},
    {"prefix count",
     [](irs::codecs::Header& h, duckdb::data_ptr_t block) {
       duckdb::Store<uint32_t>(h.symtab_size, kPrefixes(h, block));
     },
     "Corrupted text segment: symbol table is inconsistent"},
    {"prefix end",
     [](irs::codecs::Header& h, duckdb::data_ptr_t block) {
       duckdb::Store<uint32_t>(0, kPrefixes(h, block) + sizeof(uint32_t));
     },
     "Corrupted text segment: symbol table is inconsistent"},
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
    irs::codecs::CodecType block_codec = irs::codecs::CodecType::Auto;
    {
      tests::IndexDir dir{Db(), path.string()};
      Write(dir, irs::codecs::CodecType::FsstPlus, {}, 20000,
            DEFAULT_ROW_GROUP_SIZE, values);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      offset = col->DataBlocks().front().file_offset;
      size = col->DataBlocks().front().byte_size;
      block_codec = col->DataBlocks().front().codec->type;
    }
    const auto file = path / irs::FileName(kSeg);
    std::string bytes;
    {
      std::ifstream in{file, std::ios::binary};
      bytes.assign(std::istreambuf_iterator<char>{in}, {});
    }
    ASSERT_LE(offset + size, bytes.size());
    auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
    auto h = irs::codecs::Header::Parse(block, size, block_codec);
    ASSERT_EQ(irs::codecs::LeafOf(block_codec),
              irs::codecs::ByteCodec::FsstPlus);
    ASSERT_GT(duckdb::Load<uint32_t>(kPrefixes(h, block)), 0u);
    c.corrupt(h, block);
    duckdb::Store(h, block);
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    tests::IndexDir dir{Db(), path.string()};
    const auto scan = [&] {
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
    };
    const auto fetch = [&] {
      irs::ColReader r{Db(), dir.Fs(), dir.Root(),
                       kSeg, true,     duckdb::MappedFileAccess::RANDOM};
      const auto* col = r.Column(kField);
      irs::ColumnReader::PointReader cursor{r, *col};
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, 1};
      cursor.FetchRow(7, out, 0);
    };
    ExpectCorrupted(scan, c.error);
    ExpectCorrupted(fetch, c.error);
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
     "Corrupted text segment: symbol table is inconsistent"},
    {"token count",
     [](irs::codecs::Header& h, duckdb::data_ptr_t block) {
       duckdb::Store<uint32_t>(0, block + h.off_symtab + sizeof(uint32_t));
     },
     "Corrupted text segment: symbol table is inconsistent"},
    {"token count beyond the code width",
     [](irs::codecs::Header& h, duckdb::data_ptr_t block) {
       block[h.off_symtab] = 2;
       duckdb::Store<uint32_t>(5, block + h.off_symtab + sizeof(uint32_t));
     },
     "Corrupted text segment: symbol table is inconsistent"},
    {"table shorter than its header",
     [](irs::codecs::Header& h, duckdb::data_ptr_t) {
       h.symtab_size = irs::codecs::text::onpair::kSymbolTableHeader - 1;
     },
     "Corrupted text segment: symbol table is inconsistent"},
    {"token lengths",
     [](irs::codecs::Header& h, duckdb::data_ptr_t block) {
       block[h.off_symtab + irs::codecs::text::onpair::kSymbolTableHeader] ^=
         0x0F;
     },
     "Corrupted text segment: symbol table is inconsistent"},
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
    irs::codecs::CodecType block_codec = irs::codecs::CodecType::Auto;
    {
      tests::IndexDir dir{Db(), path.string()};
      Write(dir, irs::codecs::CodecType::Onpair, {}, 20000,
            DEFAULT_ROW_GROUP_SIZE, values);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      offset = col->DataBlocks().front().file_offset;
      size = col->DataBlocks().front().byte_size;
      block_codec = col->DataBlocks().front().codec->type;
    }
    const auto file = path / irs::FileName(kSeg);
    std::string bytes;
    {
      std::ifstream in{file, std::ios::binary};
      bytes.assign(std::istreambuf_iterator<char>{in}, {});
    }
    ASSERT_LE(offset + size, bytes.size());
    auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
    auto h = irs::codecs::Header::Parse(block, size, block_codec);
    ASSERT_EQ(irs::codecs::LeafOf(block_codec), irs::codecs::ByteCodec::OnPair);
    c.corrupt(h, block);
    duckdb::Store(h, block);
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    tests::IndexDir dir{Db(), path.string()};
    const auto scan = [&] {
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
    };
    const auto fetch = [&] {
      irs::ColReader r{Db(), dir.Fs(), dir.Root(),
                       kSeg, true,     duckdb::MappedFileAccess::RANDOM};
      const auto* col = r.Column(kField);
      irs::ColumnReader::PointReader cursor{r, *col};
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, 1};
      cursor.FetchRow(7, out, 0);
    };
    ExpectCorrupted(scan, c.error);
    ExpectCorrupted(fetch, c.error);
  }
}

TEST_F(ColCodecsTest, OnPairCodesPastTheTokenTableAreRejected) {
  namespace onpair = irs::codecs::text::onpair;
  std::string table(onpair::kSymbolTableHeader, '\0');
  table[0] = 2;
  duckdb::Store<uint32_t>(
    3, duckdb::data_ptr_cast(table.data() + sizeof(uint32_t)));
  table.append(2, '\0');
  table.append("abc");
  onpair::Table tokens;
  ASSERT_TRUE(tokens.Open(table));
  constexpr uint64_t kCodes = 64;
  std::vector<uint8_t> stream(
    kCodes * 2 / 8 + onpair::kPadding + sizeof(uint64_t), 0);
  std::string expected;
  for (uint64_t k = 0; k < kCodes; ++k) {
    stream[k * 2 / 8] |= static_cast<uint8_t>((k % 3) << (k * 2 % 8));
    expected += "abc"[k % 3];
  }
  std::string out(kCodes * onpair::kMaxToken, '\0');
  ASSERT_EQ(tokens.Decode(stream.data(), 0, kCodes, out.data(), out.size()),
            kCodes);
  EXPECT_EQ(out.substr(0, kCodes), expected);
  stream[3] = 0xFF;
  ExpectCorrupted(
    [&] { tokens.Decode(stream.data(), 0, kCodes, out.data(), out.size()); },
    "Corrupted text segment: entry is inconsistent");
}

TEST_F(ColCodecsTest, OnPairTrainingStopsOnceItsTokenTableIsFull) {
  namespace onpair = irs::codecs::text::onpair;
  constexpr uint64_t kMotifs = 3851;
  constexpr uint64_t kRows = 1'500'000;
  std::vector<std::string> motifs;
  for (uint64_t m = 0; m < kMotifs; ++m) {
    motifs.push_back(
      {static_cast<char>('!' + m % 90), static_cast<char>('!' + m / 90 % 90)});
  }
  std::vector<duckdb::string_t> entries;
  uint64_t raw = 0;
  for (uint64_t r = 0; r < kRows; ++r) {
    const auto& motif = motifs[r % kMotifs];
    entries.emplace_back(motif.data(), static_cast<uint32_t>(motif.size()));
    raw += motif.size();
  }
  onpair::Codec codec;
  codec.Train(entries, raw);
  std::string exported;
  codec.Export(exported);
  ASSERT_GE(exported.size(), onpair::kSymbolTableHeader);
  EXPECT_EQ(duckdb::Load<uint32_t>(
              duckdb::const_data_ptr_cast(exported.data() + sizeof(uint32_t))),
            uint32_t{1} << onpair::kDictionaryBits);
  onpair::Table table;
  ASSERT_TRUE(table.Open(exported));
  ASSERT_EQ(table.Bits(), codec.Bits());
  const std::span<const duckdb::string_t> probe{entries.data(), kMotifs};
  uint64_t probe_raw = 0;
  for (const auto& entry : probe) {
    probe_raw += entry.GetSize();
  }
  std::vector<uint16_t> codes(probe_raw);
  std::vector<uint32_t> counts(probe.size());
  codec.Encode(probe, codes, counts);
  uint64_t at = 0;
  for (size_t e = 0; e < probe.size(); ++e) {
    const auto bits = table.Bits();
    std::vector<uint8_t> stream(
      (counts[e] * bits + 7) / 8 + onpair::kPadding + sizeof(uint64_t), 0);
    for (uint32_t k = 0; k < counts[e]; ++k) {
      for (uint32_t b = 0; b < bits; ++b) {
        if (codes[at + k] >> b & 1) {
          const auto bit = uint64_t{k} * bits + b;
          stream[bit / 8] |= static_cast<uint8_t>(1U << bit % 8);
        }
      }
    }
    std::string out(uint64_t{counts[e]} * onpair::kMaxToken, '\0');
    const auto n =
      table.Decode(stream.data(), 0, counts[e], out.data(), out.size());
    ASSERT_EQ(out.substr(0, n), probe[e].GetString()) << "entry " << e;
    at += probe[e].GetSize();
  }
}

TEST_F(ColCodecsTest, CorruptedFsst12TablesAreRejected) {
  struct Case {
    std::string_view name;
    void (*corrupt)(duckdb::data_ptr_t);
  };
  const Case cases[] = {
    {"version",
     [](duckdb::data_ptr_t table) { duckdb::Store<uint64_t>(0, table); }},
    {"symbol count",
     [](duckdb::data_ptr_t table) {
       duckdb::Store<uint16_t>(0xFFFF, table + 8);
     }},
    {"symbol length", [](duckdb::data_ptr_t table) { table[24] = 9; }},
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
    irs::codecs::CodecType block_codec = irs::codecs::CodecType::Auto;
    {
      tests::IndexDir dir{Db(), path.string()};
      Write(dir, irs::codecs::CodecType::Fsst12, {}, 20000,
            DEFAULT_ROW_GROUP_SIZE, values);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      offset = col->DataBlocks().front().shared_offset;
      size = col->DataBlocks().front().shared_size;
      block_codec = col->DataBlocks().front().codec->type;
    }
    const auto file = path / irs::FileName(kSeg);
    std::string bytes;
    {
      std::ifstream in{file, std::ios::binary};
      bytes.assign(std::istreambuf_iterator<char>{in}, {});
    }
    ASSERT_LE(offset + size, bytes.size());
    ASSERT_EQ(irs::codecs::LeafOf(block_codec), irs::codecs::ByteCodec::Fsst12);
    ASSERT_GT(size, 24u);
    c.corrupt(reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset));
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    tests::IndexDir dir{Db(), path.string()};
    const auto scan = [&] {
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
    };
    const auto fetch = [&] {
      irs::ColReader r{Db(), dir.Fs(), dir.Root(),
                       kSeg, true,     duckdb::MappedFileAccess::RANDOM};
      const auto* col = r.Column(kField);
      irs::ColumnReader::PointReader cursor{r, *col};
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, 1};
      cursor.FetchRow(7, out, 0);
    };
    ExpectCorrupted(scan,
                    "Corrupted text segment: symbol table is inconsistent");
    ExpectCorrupted(fetch,
                    "Corrupted text segment: symbol table is inconsistent");
  }
}

TEST_F(ColCodecsTest, CorruptedFrameRangesAreRejectedByPointReads) {
  using irs::codecs::CodecType;
  const Value values = [](uint64_t g) -> std::optional<std::string> {
    return "https://example.org/users/" + std::to_string(g % 5000) + "/profile";
  };
  for (const auto codec :
       {CodecType::FsstPlus, CodecType::DictFsstPlus, CodecType::Onpair,
        CodecType::DictOnpair, CodecType::Fsst, CodecType::DictFsst,
        CodecType::Fsst12, CodecType::Lz4, CodecType::DictLz4}) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    const auto path = test_dir() / absl::StrCat("col_codecs_corrupt_frame_",
                                                magic_enum::enum_name(codec));
    std::filesystem::create_directories(path);
    uint64_t offset = 0;
    uint64_t size = 0;
    irs::codecs::CodecType block_codec = irs::codecs::CodecType::Auto;
    {
      tests::IndexDir dir{Db(), path.string()};
      Write(dir, codec, {}, 20000, DEFAULT_ROW_GROUP_SIZE, values);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      const auto& block = col->DataBlocks().front();
      ASSERT_EQ(magic_enum::enum_name(block.codec->type),
                magic_enum::enum_name(codec));
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
    auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
    const auto h = irs::codecs::Header::Parse(block, size, block_codec);
    ASSERT_NE(h.frame_count, 0u);
    for (uint32_t f = 0; f < h.frame_count; ++f) {
      auto* at = block + h.off_frames + f * irs::codecs::kFrameMetaSize;
      auto frame = duckdb::Load<irs::codecs::FrameMeta>(at);
      ASSERT_NE(frame.comp_len, 0u);
      frame.comp_off = h.data_size;
      duckdb::Store(frame, at);
    }
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    tests::IndexDir dir{Db(), path.string()};
    const auto fetch = [&] {
      irs::ColReader r{Db(), dir.Fs(), dir.Root(),
                       kSeg, true,     duckdb::MappedFileAccess::RANDOM};
      const auto* col = r.Column(kField);
      irs::ColumnReader::PointReader cursor{r, *col};
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, 1};
      cursor.FetchRow(7, out, 0);
    };
    ExpectCorrupted(fetch,
                    "Corrupted text segment: frame table is inconsistent");
  }
}

TEST_F(ColCodecsTest, CorruptedSegmentCountsAreRejectedByPointReads) {
  struct Case {
    std::string_view name;
    irs::codecs::CodecType codec;
    void (*corrupt)(irs::codecs::Header&);
  };
  const Case cases[] = {
    {"dedup_entries", irs::codecs::CodecType::DictLz4,
     [](irs::codecs::Header& h) {
       h.length_width = 0;
       h.entry_count = h.row_count + 2;
     }},
    {"plain_entries", irs::codecs::CodecType::Lz4,
     [](irs::codecs::Header& h) { h.entry_count = 0; }},
    {"rows", irs::codecs::CodecType::DictLz4,
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
    irs::codecs::CodecType block_codec = irs::codecs::CodecType::Auto;
    {
      tests::IndexDir dir{Db(), path.string()};
      Write(dir, c.codec, {}, 5000, 2048, values);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      offset = col->DataBlocks().front().file_offset;
      size = col->DataBlocks().front().byte_size;
      block_codec = col->DataBlocks().front().codec->type;
    }
    const auto file = path / irs::FileName(kSeg);
    std::string bytes;
    {
      std::ifstream in{file, std::ios::binary};
      bytes.assign(std::istreambuf_iterator<char>{in}, {});
    }
    ASSERT_LE(offset + size, bytes.size());
    auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
    auto h = irs::codecs::Header::Parse(block, size, block_codec);
    c.corrupt(h);
    duckdb::Store(h, block);
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    tests::IndexDir dir{Db(), path.string()};
    const auto fetch = [&] {
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      irs::ColumnReader::PointReader cursor{r, *col};
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, 1};
      cursor.FetchRow(0, out, 0);
    };
    ExpectCorrupted(fetch, "Corrupted text segment: header is inconsistent");
  }
}

const Value kTripledValues = [](uint64_t g) -> std::optional<std::string> {
  return "v" + std::to_string(g / 3);
};

TEST_F(ColCodecsTest, DictionaryCodesUseTheNumericCodec) {
  for (const auto codec :
       {irs::codecs::CodecType::Auto, irs::codecs::CodecType::DictFsst,
        irs::codecs::CodecType::DictLz4}) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
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
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, irs::codecs::CodecType::DictLz4, {.tier = tier}, kRows, 16384,
          kShortCodeRuns);
    Verify(dir, irs::codecs::CodecType::DictLz4, kRows, kShortCodeRuns);
    for (const auto& transform : SegmentInfo(dir, "codes_transform")) {
      EXPECT_NE(transform, "rle_bitpacking");
    }
  }
}

TEST_F(ColCodecsTest, CodesAreRunLengthCodedOnlyWhenThatIsClearlySmaller) {
  constexpr uint64_t kRows = 60000;
  struct Runs {
    uint64_t rows;
    uint64_t runs;
    bool coded;
  };
  for (const auto codec :
       {irs::codecs::CodecType::DictFsst, irs::codecs::CodecType::DictLz4}) {
    for (const auto& [rows, runs, coded] :
         {Runs{2, 1, false}, Runs{7, 3, false}, Runs{8, 1, true}}) {
      SCOPED_TRACE(absl::StrCat(magic_enum::enum_name(codec), " ", runs,
                                " runs in ", rows, " rows"));
      const Value value = [rows,
                           runs](uint64_t g) -> std::optional<std::string> {
        return "v" + std::to_string(std::mt19937_64{g * runs / rows}() % 10000);
      };
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, codec, {}, kRows, 16384, value);
      Verify(dir, codec, kRows, value);
      const auto transforms = SegmentInfo(dir, "codes_transform");
      ASSERT_FALSE(transforms.empty());
      for (const auto& transform : transforms) {
        EXPECT_EQ(transform.starts_with("rle"), coded) << transform;
      }
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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::DictLz4, {.segment_target = 16 * 1024},
        kRows, 65536, value);
  Verify(dir, irs::codecs::CodecType::DictLz4, kRows, value);
  const auto transforms = SegmentInfo(dir, "codes_transform");
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::DictLz4, {}, kRows, 8192, kTripledValues);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
  for (const bool codes_codec : {false, true}) {
    SCOPED_TRACE(codes_codec ? "codes codec" : "row count");
    const auto path =
      test_dir() / (codes_codec ? "col_codecs_corrupt_codes_codec"
                                : "col_codecs_corrupt_codes_rows");
    std::filesystem::create_directories(path);
    uint64_t offset = 0;
    uint64_t size = 0;
    irs::codecs::CodecType block_codec = irs::codecs::CodecType::Auto;
    {
      tests::IndexDir dir{Db(), path.string()};
      Write(dir, irs::codecs::CodecType::DictLz4, {}, 30000, 30000,
            kTripledValues);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      offset = col->DataBlocks().front().file_offset;
      size = col->DataBlocks().front().byte_size;
      block_codec = col->DataBlocks().front().codec->type;
    }
    const auto file = path / irs::FileName(kSeg);
    std::string bytes;
    {
      std::ifstream in{file, std::ios::binary};
      bytes.assign(std::istreambuf_iterator<char>{in}, {});
    }
    ASSERT_LE(offset + size, bytes.size());
    auto* block = reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + offset);
    const auto h = irs::codecs::Header::Parse(block, size, block_codec);
    ASSERT_EQ(irs::codecs::ShapeOf(block_codec),
              irs::codecs::Shape::Dictionary);
    if (codes_codec) {
      auto corrupted = h;
      corrupted.codes = irs::codecs::CodecType::DictLz4;
      duckdb::Store(corrupted, block);
    } else {
      duckdb::Store<uint32_t>(h.row_count + 1, block + h.off_codes + 8);
    }
    {
      std::ofstream out{file, std::ios::binary | std::ios::trunc};
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
    tests::IndexDir dir{Db(), path.string()};
    const auto scan = [&] {
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
    };
    if (!codes_codec) {
      ExpectCorrupted(scan,
                      "Corrupted numeric segment: header is inconsistent");
      continue;
    }
    ExpectCorrupted(scan, "Corrupted text segment: header is inconsistent");
  }
}

TEST_F(ColCodecsTest, AlpRdSplitsDoublesWhereTheFewestBitsRemain) {
  struct Case {
    std::string_view name;
    std::span<const uint64_t> leading;
    uint64_t frequent;
    uint64_t rare_percent;
    uint64_t random_bits;
  };
  constexpr uint64_t kRows = 8 * STANDARD_VECTOR_SIZE;
  constexpr uint64_t kLeadingBits = 16;
  constexpr uint64_t kDictionaryBits = 3;
  constexpr uint64_t kExceptionBytes = 4;
  constexpr uint64_t kLeading[] = {0x0123, 0x1234, 0x2345, 0x3456,
                                   0x4567, 0x5678, 0x6789, 0x789A,
                                   0x0127, 0x1230, 0x2341, 0x3452};
  constexpr uint64_t kPrefixes[] = {0b000'0000001, 0b001'0101010, 0b010'0110011,
                                    0b100'0001111, 0b110'1100110};
  const Case cases[] = {
    {"sixteen leading bits", std::span{kLeading}.first(5), 5, 0, 0},
    {"ten leading bits", kPrefixes, 5, 0, 6},
    {"frequent leading bits", kLeading, 8, 4, 0},
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    std::mt19937_64 random{42};
    std::vector<double> expected;
    expected.reserve(kRows);
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    {
      irs::ColWriter w{Db(), dir.Tracker(kSeg)};
      auto& cw = w.OpenColumn(kField, duckdb::LogicalType::DOUBLE,
                              {.compression = irs::codecs::CodecType::Alprd});
      for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
        duckdb::Vector v{duckdb::LogicalType::DOUBLE, STANDARD_VECTOR_SIZE};
        auto* values = duckdb::FlatVector::GetDataMutable<double>(v);
        for (uint64_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
          const auto index =
            random() % 100 < c.rare_percent
              ? c.frequent + random() % (c.leading.size() - c.frequent)
              : random() % c.frequent;
          const auto leading = c.leading[index] << c.random_bits |
                               random() % (uint64_t{1} << c.random_bits);
          values[k] = std::bit_cast<double>(leading << (64 - kLeadingBits) |
                                            random() >> kLeadingBits);
          expected.push_back(values[k]);
        }
        duckdb::FlatVector::SetSize(v, STANDARD_VECTOR_SIZE);
        cw.Append(v, STANDARD_VECTOR_SIZE);
      }
      ASSERT_TRUE(w.Commit(0));
    }
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    uint64_t stored = 0;
    for (const auto& block : col->DataBlocks()) {
      ASSERT_EQ(block.codec->type, irs::codecs::CodecType::Alprd);
      stored += block.byte_size;
    }
    const auto packed =
      kRows * (64 - kLeadingBits + c.random_bits + kDictionaryBits) / 8;
    const auto exceptions =
      c.rare_percent ? kRows * (c.rare_percent + 1) / 100 * kExceptionBytes : 0;
    EXPECT_GE(stored, packed);
    EXPECT_LT(stored, packed + exceptions + kRows / 16);
    auto state = col->InitScan(r.Ctx());
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector out{duckdb::LogicalType::DOUBLE, STANDARD_VECTOR_SIZE};
      ASSERT_EQ(col->Scan(state, out, STANDARD_VECTOR_SIZE),
                STANDARD_VECTOR_SIZE);
      out.Flatten(STANDARD_VECTOR_SIZE);
      const auto* values = duckdb::FlatVector::GetData<double>(out);
      for (uint64_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
        ASSERT_EQ(std::bit_cast<uint64_t>(values[k]),
                  std::bit_cast<uint64_t>(expected[pos + k]))
          << "row " << pos + k;
      }
    }
  }
}

TEST_F(ColCodecsTest, CorruptedSegmentHeadersAreIndexCorruption) {
  struct Case {
    std::string_view name;
    duckdb::LogicalType type;
    irs::codecs::CodecType codec;
  };
  const Case cases[] = {
    {"rle", duckdb::LogicalType::BIGINT, irs::codecs::CodecType::Rle},
    {"bitpacking", duckdb::LogicalType::INTEGER,
     irs::codecs::CodecType::Bitpacking},
    {"alp", duckdb::LogicalType::DOUBLE, irs::codecs::CodecType::Alp},
    {"alprd", duckdb::LogicalType::DOUBLE, irs::codecs::CodecType::Alprd},
    {"roaring", duckdb::LogicalType::BOOLEAN, irs::codecs::CodecType::Roaring},
    {"uncompressed", duckdb::LogicalType::VARCHAR,
     irs::codecs::CodecType::Uncompressed},
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
      tests::IndexDir dir{Db(), path.string()};
      irs::ColWriter w{Db(), dir.Tracker(kSeg)};
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
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      const auto& block = col->DataBlocks().front();
      ASSERT_EQ(magic_enum::enum_name(block.codec->type),
                magic_enum::enum_name(c.codec));
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
    tests::IndexDir dir{Db(), path.string()};
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    irs::codecs::CodecType codec;
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
  const auto sparse = [](int64_t g) {
    return duckdb::Value::BOOLEAN(g % 100 == 0);
  };
  const std::string long_text(5000, 'q');
  const auto overflowing = [&](int64_t g) {
    return duckdb::Value{absl::StrCat("row-", g, "-", long_text)};
  };
  const Case cases[] = {
    {"roaring metadata offset", duckdb::LogicalType::BOOLEAN,
     irs::codecs::CodecType::Roaring, sparse,
     [&](std::string& b, uint64_t at, uint64_t size) {
       store64(b, at, size - sizeof(uint64_t) + 1);
     },
     "Corrupted roaring segment: metadata offset exceeds the segment"},
    {"roaring short metadata", duckdb::LogicalType::BOOLEAN,
     irs::codecs::CodecType::Roaring, sparse,
     [&](std::string& b, uint64_t at, uint64_t size) {
       store64(b, at, size - sizeof(uint64_t) - 2);
     },
     "Corrupted roaring segment: container metadata extends past the segment"},
    {"roaring container data", duckdb::LogicalType::BOOLEAN,
     irs::codecs::CodecType::Roaring, sparse,
     [&](std::string& b, uint64_t at, uint64_t size) {
       b[at + size - 1] = static_cast<char>(250);
     },
     "Corrupted roaring segment: container data extends into the metadata"},
    {"overflow string length", duckdb::LogicalType::VARCHAR,
     irs::codecs::CodecType::Uncompressed, overflowing,
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
      tests::IndexDir dir{Db(), path.string()};
      irs::ColWriter w{Db(), dir.Tracker(kSeg)};
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
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      const auto& block = col->DataBlocks().front();
      ASSERT_EQ(magic_enum::enum_name(block.codec->type),
                magic_enum::enum_name(c.codec));
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
    tests::IndexDir dir{Db(), path.string()};
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
  const auto& fn = *irs::codecs::GetCodec(irs::codecs::CodecType::Roaring,
                                          duckdb::PhysicalType::BOOL);
  struct Flushed {
    duckdb::BaseStatistics stats;
    uint64_t rows;
  };
  const auto value = [](uint64_t g) -> bool {
    return ((g * 0x9E3779B97F4A7C15ULL) >> 61) & 1;
  };
  const auto seal = [&](std::span<const uint64_t> chunk_ends,
                        const auto& value) {
    duckdb::MemoryStream out;
    irs::WriteContext ctx{Db().GetBufferManager(), out};
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
    std::vector<irs::WriteChunk> rows;
    for (const auto& v : chunks) {
      rows.push_back(irs::RefChunk(v, v.size()));
    }
    std::vector<Flushed> flushed;
    const auto flush = [&](const irs::codecs::Codec&,
                           duckdb::BaseStatistics stats, uint64_t n,
                           std::string_view, std::string_view) {
      flushed.push_back({std::move(stats), n});
    };
    irs::codecs::ColumnTuning tuning;
    irs::codecs::ColumnDataCheckpointData checkpoint{
      .type = duckdb::LogicalType::BOOLEAN,
      .ctx = &ctx,
      .flush = flush,
      .tuning = tuning,
      .rows = {rows, RowsOf(rows)},
    };
    irs::codecs::Compress(fn.type, checkpoint);
    return flushed;
  };
  std::vector<uint64_t> ends;
  for (uint64_t end = kChunk; end <= 3'000'000; end += kChunk) {
    ends.push_back(end);
  }
  const auto probe = seal(ends, value);
  ASSERT_GE(probe.size(), 2u);
  const uint64_t first = probe.front().rows;
  constexpr uint64_t kBefore = 5;
  ends.clear();
  for (uint64_t end = kChunk; end < first - kBefore; end += kChunk) {
    ends.push_back(end);
  }
  ends.push_back(first - kBefore);
  ends.push_back(first - kBefore + kChunk);
  const auto flushed = seal(ends, value);
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
  const auto runs = [](uint64_t g) -> bool { return g / 300 % 2 == 0; };
  constexpr uint64_t kSizes[] = {1, 37, 64, 100, 5, kChunk, 1000, 63, 300, 2};
  ends.clear();
  for (uint64_t end = 0, i = 0; end < 40'000; ++i) {
    end += kSizes[i % std::size(kSizes)];
    ends.push_back(end);
  }
  begin = 0;
  for (const auto& [stats, rows] : seal(ends, runs)) {
    bool any_true = false;
    bool any_false = false;
    for (uint64_t g = begin; g < begin + rows; ++g) {
      any_true |= runs(g);
      any_false |= !runs(g);
    }
    SCOPED_TRACE(begin);
    EXPECT_EQ(duckdb::NumericStats::Max(stats).GetValue<bool>(), any_true);
    EXPECT_EQ(duckdb::NumericStats::Min(stats).GetValue<bool>(), !any_false);
    begin += rows;
  }
  EXPECT_EQ(begin, ends.back());
}

TEST_F(ColCodecsTest, WideRowsStayNearTheSegmentTarget) {
  constexpr uint32_t kTarget = 16384;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::Lz4, {.segment_target = kTarget}, 3000,
        4096, kLongTextWithNulls);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  ASSERT_GT(col->DataBlocks().size(), 10u);
  for (const auto& block : col->DataBlocks()) {
    EXPECT_LE(block.byte_size, kTarget * 3 / 2);
  }
  Verify(dir, irs::codecs::CodecType::Lz4, 3000, kLongTextWithNulls);
}

TEST_F(ColCodecsTest, CodesHeavyDictionarySegmentsStayNearTheSegmentTarget) {
  constexpr uint32_t kTarget = 16384;
  constexpr uint64_t kRows = 200000;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    return "v" + std::to_string(std::mt19937_64{g}() % 200);
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::DictLz4, {.segment_target = kTarget},
        kRows, DEFAULT_ROW_GROUP_SIZE, value);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  for (const auto& block : col->DataBlocks()) {
    EXPECT_LE(block.byte_size, kTarget * 3 / 2);
  }
  EXPECT_GT(col->DataBlocks().size(), 6u);
  Verify(dir, irs::codecs::CodecType::DictLz4, kRows, value);
}

TEST_F(ColCodecsTest,
       AutoAsksEveryVariantOfLz4AgainWhenARowGroupBringsMoreText) {
  const auto seal = [&](const std::vector<std::unique_ptr<duckdb::Vector>>& in,
                        irs::codecs::ColumnTuning& tuning) {
    EXPECT_TRUE(AllText(SealAuto(Db(), WriteChunks(in), {}, tuning)));
  };
  const Value brief = [](uint64_t g) -> std::optional<std::string> {
    return "id-" + std::to_string(g % 50);
  };
  const Value similar = [](uint64_t g) -> std::optional<std::string> {
    return "key-" + std::to_string(g % 60);
  };
  const Value wordy = [](uint64_t g) -> std::optional<std::string> {
    return kVocabulary(g % 60);
  };
  const auto small = Chunks(4096, brief);
  const auto alike = Chunks(4096, similar);
  const auto large = Chunks(4096, wordy);
  irs::codecs::ColumnTuning tuning;
  const auto& hint =
    tuning.hints[std::to_underlying(irs::codecs::CodecType::DictLz4)];
  seal(small, tuning);
  ASSERT_NE(hint.forced, 0u);
  const auto first = hint.refreshed;
  EXPECT_NE(first, 0u);
  const auto brief_bytes = hint.input;
  EXPECT_NE(brief_bytes, 0u);
  for (int rg = 0; rg < 4; ++rg) {
    seal(alike, tuning);
    EXPECT_EQ(hint.refreshed, first);
    EXPECT_EQ(hint.input, brief_bytes);
  }
  seal(large, tuning);
  const auto grown = hint.refreshed;
  EXPECT_GT(grown, first);
  EXPECT_GE(hint.input, irs::codecs::kMoreInput * brief_bytes);
  const auto wordy_bytes = hint.input;
  seal(small, tuning);
  EXPECT_EQ(hint.refreshed, grown);
  EXPECT_EQ(hint.input, wordy_bytes);
}

TEST_F(ColCodecsTest, AutoMeasuresATextCandidateAgainOnlyWhenItMayWin) {
  using irs::codecs::CodecType;
  using irs::codecs::Shape;
  const auto seal = [&](const std::vector<std::unique_ptr<duckdb::Vector>>& in,
                        irs::codecs::ColumnTuning& tuning) {
    const auto types = SealAuto(Db(), WriteChunks(in), {}, tuning);
    EXPECT_TRUE(AllText(types));
    return types;
  };
  const auto all = [](const std::vector<CodecType>& types, Shape shape) {
    return !types.empty() && absl::c_all_of(types, [&](CodecType type) {
      return irs::codecs::ShapeOf(type) == shape;
    });
  };
  const Value urls = [](uint64_t g) -> std::optional<std::string> {
    return "https://example.org/section/" + std::to_string(g % 5000) +
           "/page?id=" + std::to_string(g % 97);
  };
  const auto steady = Chunks(4096, urls);
  irs::codecs::ColumnTuning tuning;
  auto& dictionary = tuning.hints[std::to_underlying(CodecType::DictFsst)];
  auto types = seal(steady, tuning);
  ASSERT_NE(dictionary.forced, 0u);
  const auto far = 2 * dictionary.score;
  dictionary.score = far;
  for (unsigned rg = 1; rg < 8; ++rg) {
    types = seal(steady, tuning);
    EXPECT_EQ(dictionary.score, far);
  }
  EXPECT_TRUE(all(types, Shape::Plain)) << ::testing::PrintToString(types);

  const Value repeated = [](uint64_t g) -> std::optional<std::string> {
    std::mt19937_64 rng{g + 1};
    return "status-" + std::to_string(rng() % 37);
  };
  const auto changed = Chunks(4096, repeated);
  types = seal(changed, tuning);
  EXPECT_TRUE(all(types, Shape::Dictionary)) << ::testing::PrintToString(types);
  EXPECT_NE(dictionary.score, far);

  types = seal(steady, tuning);
  EXPECT_TRUE(all(types, Shape::Plain)) << ::testing::PrintToString(types);
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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::Auto, {}, rows, kRowGroup, value);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  uint64_t row = 0;
  for (const auto& block : col->DataBlocks()) {
    ASSERT_TRUE(irs::codecs::IsText(block.codec->type))
      << magic_enum::enum_name(block.codec->type);
    EXPECT_EQ(irs::codecs::ShapeOf(block.codec->type),
              row < kSteady ? irs::codecs::Shape::Dictionary
                            : irs::codecs::Shape::Plain)
      << "block at row " << row;
    row += block.tuple_count;
  }
  Verify(dir, irs::codecs::CodecType::Auto, rows, value);
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
  irs::codecs::ColumnTuning tuning;
  std::vector<uint64_t> sizes;
  duckdb::MemoryStream out;
  irs::WriteContext ctx{Db().GetBufferManager(), out};
  const auto flush = [&](const irs::codecs::Codec&, duckdb::BaseStatistics,
                         uint64_t, std::string_view bytes,
                         std::string_view) { sizes.push_back(bytes.size()); };
  irs::codecs::ColumnDataCheckpointData checkpoint{
    .type = duckdb::LogicalType::VARCHAR,
    .ctx = &ctx,
    .flush = flush,
    .params = {.segment_target = kTarget},
    .tuning = tuning,
    .rows = {chunks, kRows},
  };
  irs::codecs::Compress(irs::codecs::CodecType::Fsst, checkpoint);
  ASSERT_GT(sizes.size(), 8u);
  const auto oversized = std::ranges::count_if(
    sizes, [](uint64_t bytes) { return bytes > kTarget * 3 / 2; });
  EXPECT_LE(oversized, 2) << ::testing::PrintToString(sizes);
  RoundTrip(irs::codecs::CodecType::Fsst, {.segment_target = kTarget}, kRows,
            DEFAULT_ROW_GROUP_SIZE, value);
}

TEST_F(ColCodecsTest, TheWritersTierDecidesOverTheColumnsParams) {
  constexpr uint64_t kRows = 8000;
  const Value vocabulary = [](uint64_t g) -> std::optional<std::string> {
    return kVocabulary(g % (kRows / 4));
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Flush ? "flush" : "merge");
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    {
      irs::ColWriter w{Db(), dir.Tracker(kSeg), tier};
      auto& cw =
        w.OpenColumn(kField, duckdb::LogicalType::VARCHAR,
                     {.compression = irs::codecs::CodecType::Auto,
                      .codec_params = {.segment_target = 64 * 1024,
                                       .tier = irs::WriteTier::Merge}});
      AppendValues(cw, kRows, vocabulary);
      ASSERT_TRUE(w.Commit(0));
    }
    bool above_fast = false;
    for (const auto& [type, level] : BlockLevels(dir)) {
      above_fast |= !level.empty() && level != "0" && level != "1";
    }
    EXPECT_EQ(tier == irs::WriteTier::Merge, above_fast);
    Verify(dir, irs::codecs::CodecType::Auto, kRows, vocabulary);
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
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::Auto, {.tier = irs::WriteTier::Flush},
        kRows, 8192, value);
  const auto codecs = SegmentInfo(dir, "codec");
  const auto levels = SegmentInfo(dir, "level");
  ASSERT_EQ(codecs.size(), levels.size());
  for (size_t i = 0; i < codecs.size(); ++i) {
    EXPECT_TRUE(codecs[i] == "lz4" || codecs[i] == "fsst") << codecs[i];
    if (codecs[i] == "lz4") {
      EXPECT_EQ(levels[i], "1");
    }
  }
  Verify(dir, irs::codecs::CodecType::Auto, kRows, value);
}

TEST_F(ColCodecsTest, AutoKeepsTheDictionaryForTextThatIsMostlyNull) {
  constexpr uint64_t kRows = 4 * 4096;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 211 != 0) {
      return std::nullopt;
    }
    return absl::StrCat("https://example.org/orders/", g * 7919, "/receipt");
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    SCOPED_TRACE(tier == irs::WriteTier::Merge ? "merge" : "flush");
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, irs::codecs::CodecType::Auto, {.tier = tier}, kRows, 4096,
          value);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    EXPECT_FALSE(col->HasValidity());
    for (const auto& shape : SegmentInfo(dir, "shape")) {
      EXPECT_EQ(shape, "dictionary");
    }
    Verify(dir, irs::codecs::CodecType::Auto, kRows, value);
  }
}

TEST_F(ColCodecsTest, CompactionsPreferFsstForTextThatSeldomRepeats) {
  constexpr uint64_t kRows = 600000;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    auto h = g;
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return "document title " + std::to_string(h % 1000) + " about topic " +
           std::to_string((h >> 20) % 5000) + " in section " +
           std::to_string((h >> 40) % 300);
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::Auto, {.tier = irs::WriteTier::Merge},
        kRows, DEFAULT_ROW_GROUP_SIZE, value);
  const auto codecs = SegmentInfo(dir, "codec");
  ASSERT_GT(codecs.size(), 4u);
  for (size_t i = 0; i < codecs.size(); ++i) {
    EXPECT_EQ(codecs[i], "fsst") << i;
  }
  Verify(dir, irs::codecs::CodecType::Auto, kRows, value);
}

TEST_F(ColCodecsTest, SealedRowGroupsReleaseTheirInputs) {
  constexpr duckdb::idx_t kHalf = 512;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    return Pseudo(g % kHalf, 40);
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker(kSeg)};
    auto& cw = w.OpenColumn(kField, duckdb::LogicalType::VARCHAR,
                            {.row_group_size = static_cast<uint32_t>(2 * kHalf),
                             .compression = irs::codecs::CodecType::Lz4});
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
  Verify(dir, irs::codecs::CodecType::Lz4, 2 * kHalf, value);
}

TEST_F(ColCodecsTest, AutoCutsLz4FramesIntoSegmentsWhenItIsTheOnlyTextCodec) {
  constexpr uint64_t kRows = 20000;
  const std::string suffix(120, 'q');
  const Value value = [&](uint64_t g) -> std::optional<std::string> {
    return absl::StrCat("row-", (g % 2000) * 7919, "-", suffix);
  };
  duckdb::DBConfig::GetConfig(Db()).SetDisabledCompressionMethods(
    {duckdb::CompressionType::COMPRESSION_DICT_FSST,
     duckdb::CompressionType::COMPRESSION_DICT_LZ4,
     duckdb::CompressionType::COMPRESSION_FSST});
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::Auto,
        {.segment_target = 64, .tier = irs::WriteTier::Merge}, kRows, kRows,
        value);
  {
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    EXPECT_GT(col->DataBlocks().size(), 1u);
    for (const auto& block : col->DataBlocks()) {
      EXPECT_EQ(block.codec->type, irs::codecs::CodecType::Lz4)
        << magic_enum::enum_name(block.codec->type);
    }
  }
  Verify(dir, irs::codecs::CodecType::Auto, kRows, value);
}

TEST_F(ColCodecsTest, SparseAndDenseSelectionsOfEveryTextCodecMatchTheValues) {
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 17 == 0) {
      return std::nullopt;
    }
    return absl::StrCat("key-", absl::Dec((g * 7919) % 20000, absl::kZeroPad6));
  };
  constexpr uint64_t kRows = 30000;
  const std::string key = "key-012345";
  duckdb::Connection con{_db};
  for (const auto codec :
       {irs::codecs::CodecType::Lz4, irs::codecs::CodecType::Zstd,
        irs::codecs::CodecType::Zxc, irs::codecs::CodecType::Fsst,
        irs::codecs::CodecType::Fsst12, irs::codecs::CodecType::FsstPlus,
        irs::codecs::CodecType::Onpair, irs::codecs::CodecType::DictLz4,
        irs::codecs::CodecType::DictZstd, irs::codecs::CodecType::DictZxc,
        irs::codecs::CodecType::DictFsst, irs::codecs::CodecType::DictFsst12,
        irs::codecs::CodecType::DictFsstPlus,
        irs::codecs::CodecType::DictOnpair}) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, codec, {}, kRows, 32768, value);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
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
        const uint64_t stride =
          (anchor / STANDARD_VECTOR_SIZE) % 2 == 0 ? 9 : 1;
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

TEST_F(ColCodecsTest, DictionaryInListsMatchTheValues) {
  constexpr uint64_t kRows = 30000;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 13 == 0) {
      return std::nullopt;
    }
    return absl::StrCat("tenant-", (g * 7919) % 3000, "/item");
  };
  std::vector<std::vector<std::optional<std::string>>> lists = {
    {"tenant-0/item", "tenant-1499/item", "tenant-2999/item"},
    {"tenant-17/item", "missing", std::nullopt},
    {"tenant-42/item", "tenant-42/item"},
    {"tenant-100/item", "tenant-1000/item", "tenant-10/item", "tenant-1/item"},
    {"a", "z"},
    {std::nullopt},
    {},
  };
  for (uint64_t i = 0; i < 300; ++i) {
    lists.back().emplace_back(absl::StrCat("tenant-", 3 * i, "/item"));
    lists.back().emplace_back(absl::StrCat("absent-", i));
  }
  for (const auto codec :
       {irs::codecs::CodecType::DictFsst, irs::codecs::CodecType::DictFsstPlus,
        irs::codecs::CodecType::DictOnpair, irs::codecs::CodecType::DictFsst12,
        irs::codecs::CodecType::DictLz4, irs::codecs::CodecType::DictZstd,
        irs::codecs::CodecType::DictZxc}) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, codec, {}, kRows, 8192, value);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    duckdb::Connection con{_db};
    const bool sorted = irs::codecs::Sorted(irs::codecs::LeafOf(codec));
    for (const auto& list : lists) {
      for (const bool optional : {false, true}) {
        if (optional && !sorted) {
          continue;
        }
        SCOPED_TRACE(optional ? "optional" : "required");
        duckdb::vector<duckdb::Value> constants;
        for (const auto& key : list) {
          constants.push_back(key
                                ? duckdb::Value{*key}
                                : duckdb::Value{duckdb::LogicalType::VARCHAR});
        }
        auto in = duckdb::ExpressionFilter::CreateInExpression(
          duckdb::make_uniq<duckdb::BoundReferenceExpression>(
            duckdb::LogicalType::VARCHAR, 0),
          std::move(constants));
        const duckdb::ExpressionFilter filter{
          optional ? duckdb::CreateOptionalFilterExpression(
                       std::move(in), duckdb::LogicalType::VARCHAR)
                   : std::move(in)};
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
            const bool expected = v && absl::c_linear_search(list, v);
            const bool got = next < kept && sel.get_index(next) == i;
            next += got ? 1 : 0;
            if (optional && list.size() > 4) {
              ASSERT_TRUE(!expected || got) << "row " << anchor + i;
            } else {
              ASSERT_EQ(got, expected) << "row " << anchor + i;
            }
          }
        }
      }
    }
    duckdb::vector<duckdb::Value> constants{duckdb::Value{"tenant-0/item"},
                                            duckdb::Value{"missing"}};
    const duckdb::ExpressionFilter over_a_constant{
      duckdb::ExpressionFilter::CreateInExpression(
        duckdb::make_uniq<duckdb::BoundConstantExpression>(
          duckdb::Value{"tenant-0/item"}),
        std::move(constants))};
    auto filter_state =
      duckdb::TableFilterState::Initialize(*con.context, over_a_constant);
    auto state = col->InitScan(r.Ctx());
    for (uint64_t anchor = 0; anchor < kRows; anchor += STANDARD_VECTOR_SIZE) {
      const auto span =
        std::min<uint64_t>(kRows - anchor, STANDARD_VECTOR_SIZE);
      duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
      for (duckdb::idx_t i = 0; i < span; ++i) {
        sel.set_index(i, i);
      }
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      ASSERT_EQ(
        col->GatherFilter(state, anchor, span, sel, span, over_a_constant,
                          *filter_state, irs::NullCheckKind::None, out),
        span);
    }
  }
}

namespace {

using Offered = std::function<bool(uint64_t)>;
using Passes = std::function<bool(const std::optional<std::string>&)>;

constexpr irs::codecs::CodecType kDictionaryCodecs[] = {
  irs::codecs::CodecType::DictLz4,    irs::codecs::CodecType::DictZstd,
  irs::codecs::CodecType::DictZxc,    irs::codecs::CodecType::DictFsst,
  irs::codecs::CodecType::DictFsst12, irs::codecs::CodecType::DictFsstPlus,
  irs::codecs::CodecType::DictOnpair,
};

constexpr irs::codecs::CodecType kSortedDictionaryCodecs[] = {
  irs::codecs::CodecType::DictFsst,
  irs::codecs::CodecType::DictFsst12,
  irs::codecs::CodecType::DictFsstPlus,
  irs::codecs::CodecType::DictOnpair,
};

bool EveryRow(uint64_t) { return true; }

duckdb::unique_ptr<duckdb::Expression> Compared(duckdb::ExpressionType cmp,
                                                const duckdb::Value& key) {
  return duckdb::BoundComparisonExpression::Create(
    cmp, duckdb::make_uniq<duckdb::BoundReferenceExpression>(key.type(), 0),
    duckdb::make_uniq<duckdb::BoundConstantExpression>(key));
}

duckdb::unique_ptr<duckdb::Expression> NullChecked(
  duckdb::ExpressionType check, const duckdb::LogicalType& type) {
  auto expr = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
    check, duckdb::LogicalType::BOOLEAN);
  expr->GetChildrenMutable().push_back(
    duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0));
  return expr;
}

bool Satisfies(duckdb::ExpressionType cmp, std::string_view v,
               std::string_view key) {
  switch (cmp) {
    case duckdb::ExpressionType::COMPARE_EQUAL:
      return v == key;
    case duckdb::ExpressionType::COMPARE_NOTEQUAL:
      return v != key;
    case duckdb::ExpressionType::COMPARE_LESSTHAN:
      return v < key;
    case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
      return v <= key;
    case duckdb::ExpressionType::COMPARE_GREATERTHAN:
      return v > key;
    default:
      return v >= key;
  }
}

void ExpectFilteredSpan(const irs::ColumnReader& col,
                        irs::ColumnReader::ScanState& state, uint64_t anchor,
                        uint64_t span, const duckdb::TableFilter& filter,
                        duckdb::TableFilterState& filter_state,
                        const Offered& offered, const Value& value,
                        const Passes& passes) {
  duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
  duckdb::idx_t count = 0;
  for (duckdb::idx_t i = 0; i < span; ++i) {
    if (offered(anchor + i)) {
      sel.set_index(count++, i);
    }
  }
  duckdb::Vector out{col.Type(), STANDARD_VECTOR_SIZE};
  const auto kept =
    col.GatherFilter(state, anchor, span, sel, count, filter, filter_state,
                     irs::NullCheckKind::None, out);
  duckdb::idx_t next = 0;
  for (duckdb::idx_t i = 0; i < span; ++i) {
    const auto g = anchor + i;
    const auto v = value(g);
    const bool expected = offered(g) && passes(v);
    const bool got = next < kept && sel.get_index(next) == i;
    ASSERT_EQ(got, expected) << "row " << g;
    if (got) {
      const auto held = out.GetValue(i);
      ASSERT_EQ(held.IsNull(), !v) << "row " << g;
      if (v) {
        EXPECT_EQ(duckdb::StringValue::Get(held), *v) << "row " << g;
      }
      ++next;
    }
  }
  EXPECT_EQ(next, kept);
}

void ExpectFilterKeeps(irs::ColReader& r, const irs::ColumnReader& col,
                       duckdb::ClientContext& context,
                       const duckdb::TableFilter& filter, uint64_t rows,
                       const Offered& offered, const Value& value,
                       const Passes& passes) {
  auto filter_state = duckdb::TableFilterState::Initialize(context, filter);
  auto state = col.InitScan(r.Ctx());
  for (uint64_t anchor = 0; anchor < rows; anchor += STANDARD_VECTOR_SIZE) {
    ExpectFilteredSpan(col, state, anchor,
                       std::min<uint64_t>(rows - anchor, STANDARD_VECTOR_SIZE),
                       filter, *filter_state, offered, value, passes);
    if (::testing::Test::HasFatalFailure()) {
      return;
    }
  }
}

TEST_F(ColCodecsTest, ARoaringScanStateMovedBackReloadsItsContainer) {
  constexpr uint64_t kRows = 3 * STANDARD_VECTOR_SIZE;
  const auto truth = [](uint64_t g) { return g / 7 % 3 == 0 || g % 11 == 0; };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker(kSeg)};
    auto& cw = w.OpenColumn(kField, duckdb::LogicalType::BOOLEAN,
                            {.row_group_size = kRows,
                             .compression = irs::codecs::CodecType::Roaring});
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector v{duckdb::LogicalType::BOOLEAN, STANDARD_VECTOR_SIZE};
      auto* data = duckdb::FlatVector::GetDataMutable<bool>(v);
      for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
        data[k] = truth(pos + k);
      }
      duckdb::FlatVector::SetSize(v, STANDARD_VECTOR_SIZE);
      cw.Append(v, STANDARD_VECTOR_SIZE);
    }
    ASSERT_TRUE(w.Commit(0));
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->DataBlocks().size(), 1);
  ASSERT_EQ(col->DataBlocks().front().codec->type,
            irs::codecs::CodecType::Roaring);
  irs::ReadContext ctx{r};
  const auto segment = col->OpenSegment(0, ctx);
  irs::codecs::ColumnScanState state;
  state.scan_state = segment->function.init_scan(*segment);
  const auto scan = [&](uint64_t row, duckdb::idx_t count) {
    state.offset_in_column = row;
    duckdb::Vector out{duckdb::LogicalType::BOOLEAN, STANDARD_VECTOR_SIZE};
    segment->function.scan(*segment, state, count, out, 0,
                           duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    const auto* got = duckdb::FlatVector::GetData<bool>(out);
    for (duckdb::idx_t k = 0; k < count; ++k) {
      ASSERT_EQ(got[k], truth(row + k)) << "row " << row + k;
    }
  };
  ASSERT_NO_FATAL_FAILURE(scan(600, 100));
  ASSERT_NO_FATAL_FAILURE(scan(100, 100));
  ASSERT_NO_FATAL_FAILURE(scan(150, 1000));
}

TEST_F(ColCodecsTest, ASegmentBufferThatCannotAllocateGivesBackItsReservation) {
  duckdb::Connection con{Db()};
  auto set = con.Query("SET memory_limit = '8192TB'");
  ASSERT_FALSE(set->HasError()) << set->GetError();
  auto& buffers = Db().GetBufferManager();
  const auto used = buffers.GetUsedMemory();
  irs::codecs::text::SegmentBuffer segment{buffers};
  EXPECT_THROW(segment.Extend(uint64_t{1} << 52), std::bad_alloc);
  EXPECT_EQ(segment.Size(), 0);
  EXPECT_EQ(buffers.GetUsedMemory(), used);
  auto* at = segment.Extend(16);
  std::memset(at, 'x', 16);
  EXPECT_EQ(segment.View(), std::string(16, 'x'));
}

TEST_F(ColCodecsTest, AHeapReferenceWhoseTargetExpiredIsMadeAgain) {
  duckdb::Vector source{duckdb::LogicalType::VARCHAR, 4};
  source.SetValue(0, duckdb::Value{"a value longer than an inlined string"});
  duckdb::StringVector::AddAuxiliaryData(
    source, duckdb::make_uniq<irs::ImmutablePayloads>());
  duckdb::Vector result{duckdb::LogicalType::VARCHAR, 4};
  irs::codecs::text::HeapReference reference;
  irs::codecs::text::ReferenceOnce(reference, source, result);
  const auto* held = result.GetBufferRef()->GetAuxiliaryData().get();
  ASSERT_NE(held, nullptr);
  EXPECT_EQ(reference.target, held);
  EXPECT_FALSE(reference.target_weak.expired());
  reference.target_weak = {};
  irs::codecs::text::ReferenceOnce(reference, source, result);
  EXPECT_EQ(reference.target, result.GetBufferRef()->GetAuxiliaryData().get());
  EXPECT_FALSE(reference.target_weak.expired());
  EXPECT_EQ(reference.source, source.GetBufferRef()->GetAuxiliaryData().get());
}

}  // namespace

TEST_F(ColCodecsTest, DictionarySegmentsOfOnlyNullsReadAndFilter) {
  using Cmp = duckdb::ExpressionType;
  constexpr uint32_t kRowGroup = 320;
  constexpr uint64_t kRows = 2 * kRowGroup;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g < 256 || g >= 384) {
      return std::nullopt;
    }
    return absl::StrCat("tenant-", (g * 7919) % 41, "/item");
  };
  duckdb::Connection con{_db};
  for (const auto codec : kDictionaryCodecs) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, codec, {.segment_target = 64}, kRows, kRowGroup, value);
    const auto entries = SegmentInfo(dir, "entries");
    EXPECT_GE(absl::c_count(entries, "1"), 2)
      << ::testing::PrintToString(entries);
    Verify(dir, codec, kRows, value);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    for (const std::string_view key : {"tenant-20/item", "", "zzz"}) {
      for (const auto cmp :
           {Cmp::COMPARE_EQUAL, Cmp::COMPARE_NOTEQUAL, Cmp::COMPARE_LESSTHAN,
            Cmp::COMPARE_GREATERTHANOREQUALTO}) {
        SCOPED_TRACE(
          absl::StrCat(duckdb::EnumUtil::ToString(cmp), " '", key, "'"));
        const duckdb::ExpressionFilter filter{
          Compared(cmp, duckdb::Value{std::string{key}})};
        ExpectFilterKeeps(r, *col, *con.context, filter, kRows, EveryRow, value,
                          [&](const std::optional<std::string>& v) {
                            return v && Satisfies(cmp, *v, key);
                          });
      }
    }
    for (const auto check :
         {Cmp::OPERATOR_IS_NULL, Cmp::OPERATOR_IS_NOT_NULL}) {
      SCOPED_TRACE(duckdb::EnumUtil::ToString(check));
      const duckdb::ExpressionFilter filter{
        NullChecked(check, duckdb::LogicalType::VARCHAR)};
      ExpectFilterKeeps(r, *col, *con.context, filter, kRows, EveryRow, value,
                        [&](const std::optional<std::string>& v) {
                          return (check == Cmp::OPERATOR_IS_NULL) == !v;
                        });
    }
  }
}

TEST_F(ColCodecsTest, LazyFiltersOfEveryDictionaryCodecMatchTheValues) {
  constexpr uint64_t kDistinct = 6144;
  constexpr uint64_t kRows = 4 * STANDARD_VECTOR_SIZE;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    return absl::StrCat("https://example.org/catalog/",
                        absl::Dec(g % kDistinct, absl::kZeroPad4), "/reviews");
  };
  const Offered offered = [](uint64_t g) {
    const auto key = g % kDistinct;
    return key % (key < STANDARD_VECTOR_SIZE ? 5 : 7) == 0;
  };
  static constexpr std::string_view kPiece = "catalog/1";
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> args;
  args.push_back(duckdb::make_uniq<duckdb::BoundReferenceExpression>(
    duckdb::LogicalType::VARCHAR, 0));
  args.push_back(duckdb::make_uniq<duckdb::BoundConstantExpression>(
    duckdb::Value{std::string{kPiece}}));
  const duckdb::ExpressionFilter contains{
    duckdb::make_uniq<duckdb::BoundFunctionExpression>(
      duckdb::BoundScalarFunction(
        duckdb::ContainsFun::GetFunctions().GetFunctionByOffset(0)),
      std::move(args), nullptr)};
  auto& cache = Db().GetObjectCache();
  const auto room = cache.GetMaxMemory();
  const absl::Cleanup restore = [&] { cache.SetMaxMemory(room); };
  duckdb::Connection con{_db};
  for (const auto codec : kDictionaryCodecs) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, codec, {}, kRows, kRows, value);
    ASSERT_EQ(SegmentInfo(dir, "entries"),
              std::vector<std::string>{absl::StrCat(kDistinct + 1)});
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    for (const bool with_room : {true, false}) {
      SCOPED_TRACE(with_room ? "cache with room" : "cache without room");
      for (const auto& key : CacheKeys(r, *col)) {
        cache.Delete(key);
      }
      cache.SetMaxMemory(with_room ? room : cache.GetCurrentMemory());
      ExpectFilterKeeps(r, *col, *con.context, contains, kRows, offered, value,
                        [](const std::optional<std::string>& v) {
                          return v && v->find(kPiece) != std::string::npos;
                        });
      cache.SetMaxMemory(room);
    }
  }
}

TEST_F(ColCodecsTest, RangeFiltersAfterAPartialScanMatchTheValues) {
  using Cmp = duckdb::ExpressionType;
  constexpr uint64_t kRows = 8 * STANDARD_VECTOR_SIZE;
  std::vector<std::string> rows;
  for (uint64_t g = 0; g < kRows; ++g) {
    rows.push_back(
      absl::StrCat("key-", absl::Dec(3 * g, absl::kZeroPad7), "/x"));
  }
  const Value value = [&](uint64_t g) -> std::optional<std::string> {
    return rows[g];
  };
  const auto after = [&](uint64_t g) { return rows[g] + "~"; };
  auto& cache = Db().GetObjectCache();
  duckdb::Connection con{_db};
  for (const auto codec : kSortedDictionaryCodecs) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, codec, {}, kRows, kRows, value);
    for (const auto& sorted : SegmentInfo(dir, "sorted")) {
      ASSERT_EQ(sorted, "true");
    }
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    for (const uint64_t scanned :
         {uint64_t{40}, uint64_t{700}, uint64_t{STANDARD_VECTOR_SIZE}}) {
      const std::string keys[] = {
        rows[scanned / 4], after(15),       after(31),
        rows[scanned - 1], rows[kRows - 5], std::string{"a"},
        std::string{"z"},
      };
      for (const auto& key : keys) {
        for (const auto cmp :
             {Cmp::COMPARE_EQUAL, Cmp::COMPARE_NOTEQUAL, Cmp::COMPARE_LESSTHAN,
              Cmp::COMPARE_LESSTHANOREQUALTO, Cmp::COMPARE_GREATERTHAN,
              Cmp::COMPARE_GREATERTHANOREQUALTO}) {
          SCOPED_TRACE(absl::StrCat(scanned, " rows scanned, ",
                                    duckdb::EnumUtil::ToString(cmp), " '", key,
                                    "'"));
          for (const auto& cached : CacheKeys(r, *col)) {
            cache.Delete(cached);
          }
          auto state = col->InitScan(r.Ctx());
          duckdb::Vector lead{duckdb::LogicalType::VARCHAR,
                              STANDARD_VECTOR_SIZE};
          duckdb::FlatVector::ValidityMutable(lead).Reset(STANDARD_VECTOR_SIZE);
          if (scanned == STANDARD_VECTOR_SIZE) {
            ASSERT_EQ(col->Scan(state, lead, scanned), scanned);
            lead.Flatten(scanned);
          } else {
            ASSERT_EQ(col->ScanCount(state, lead, scanned, 0), scanned);
          }
          for (duckdb::idx_t k = 0; k < scanned; ++k) {
            ExpectRow(lead, k, k, value);
          }
          const duckdb::ExpressionFilter filter{
            Compared(cmp, duckdb::Value{key})};
          auto filter_state =
            duckdb::TableFilterState::Initialize(*con.context, filter);
          for (uint64_t anchor = STANDARD_VECTOR_SIZE; anchor < kRows;
               anchor += STANDARD_VECTOR_SIZE) {
            ExpectFilteredSpan(*col, state, anchor, STANDARD_VECTOR_SIZE,
                               filter, *filter_state, EveryRow, value,
                               [&](const std::optional<std::string>& v) {
                                 return v && Satisfies(cmp, *v, key);
                               });
            if (HasFatalFailure()) {
              return;
            }
          }
        }
      }
    }
  }
}

TEST_F(ColCodecsTest,
       BatchedPointReadsOfEveryDictionaryCodecCopyOnlyWithoutCacheRoom) {
  using irs::codecs::CodecType;
  constexpr uint64_t kRows = 8000;
  const Value distinct = [](uint64_t g) -> std::optional<std::string> {
    return absl::StrCat("https://example.org/catalog/", 100000000 + g,
                        "/reviews?page=", g % 7);
  };
  const Value repeating = [&](uint64_t g) -> std::optional<std::string> {
    return distinct(g % 200);
  };
  auto& cache = Db().GetObjectCache();
  const auto room = cache.GetMaxMemory();
  const absl::Cleanup restore = [&] { cache.SetMaxMemory(room); };
  std::vector<uint32_t> spread;
  for (uint32_t g = 7; g < kRows * 6 / 10; g += 79) {
    spread.push_back(g);
  }
  ASSERT_LT(spread.size(), 64u);
  std::vector<uint32_t> paired;
  for (uint32_t g = 3; g < kRows; g += 100) {
    paired.push_back(g);
  }
  for (const auto codec :
       {CodecType::DictZstd, CodecType::DictZxc, CodecType::DictFsst12,
        CodecType::DictFsstPlus, CodecType::DictOnpair}) {
    for (const auto* value : {&distinct, &repeating}) {
      SCOPED_TRACE(absl::StrCat(
        magic_enum::enum_name(codec),
        value == &distinct ? ", distinct values" : ", repeating values"));
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      Write(dir, codec, {}, kRows, 8192, *value);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      ASSERT_EQ(col->DataBlocks().size(), 1u);
      ASSERT_EQ(col->DataBlocks().front().codec->type, codec);
      const auto copied_bytes = [&](const std::vector<uint32_t>& rows) {
        irs::ColumnReader::PointReader cursor{r, *col};
        duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
        cursor.FetchRows(rows, 0, out, 0);
        for (size_t k = 0; k < rows.size(); ++k) {
          ExpectRow(out, k, rows[k], *value);
        }
        return duckdb::StringVector::GetStringAllocator(out).SizeInBytes();
      };
      cache.SetMaxMemory(cache.GetCurrentMemory());
      if (value == &distinct) {
        EXPECT_GT(copied_bytes(spread), 0u);
      } else {
        EXPECT_EQ(copied_bytes(spread), 0u);
      }
      copied_bytes(paired);
      cache.SetMaxMemory(room);
      EXPECT_EQ(copied_bytes(spread), 0u);
      copied_bytes(paired);
    }
  }
}

TEST_F(ColCodecsTest, DictionaryFiltersOnABlobColumnMatchTheBytes) {
  using Cmp = duckdb::ExpressionType;
  constexpr uint64_t kRows = 3 * STANDARD_VECTOR_SIZE + 77;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 17 == 0) {
      return std::nullopt;
    }
    const auto k = (g * 7919) % 300;
    std::string v(13 + k % 4, '\0');
    for (size_t i = 0; i < v.size(); ++i) {
      v[i] = static_cast<char>((k * 37 + i * 101) % 256);
    }
    return v;
  };
  duckdb::Connection con{_db};
  for (const auto codec :
       {irs::codecs::CodecType::DictFsst, irs::codecs::CodecType::DictLz4}) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    {
      irs::ColWriter w{Db(), dir.Tracker(kSeg)};
      auto& cw = w.OpenColumn(kField, duckdb::LogicalType::BLOB,
                              {.row_group_size = 4096, .compression = codec});
      for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
        const auto take = std::min<uint64_t>(kRows - pos, STANDARD_VECTOR_SIZE);
        duckdb::Vector vec{duckdb::LogicalType::BLOB, STANDARD_VECTOR_SIZE};
        auto* d = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec);
        auto& validity = duckdb::FlatVector::ValidityMutable(vec);
        validity.Reset(STANDARD_VECTOR_SIZE);
        for (uint64_t k = 0; k < take; ++k) {
          if (const auto v = value(pos + k)) {
            d[k] =
              duckdb::StringVector::AddStringOrBlob(vec, v->data(), v->size());
          } else {
            validity.SetInvalid(k);
          }
        }
        duckdb::FlatVector::SetSize(vec, take);
        cw.Append(vec, take);
      }
      ASSERT_TRUE(w.Commit(0));
    }
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    for (const auto& block : col->DataBlocks()) {
      EXPECT_EQ(block.codec->type, codec);
    }
    auto bumped = *value(1);
    bumped.back() = static_cast<char>(bumped.back() + 1);
    for (const auto& key :
         {*value(1), bumped, std::string(1, '\x80'), std::string{}}) {
      for (const auto cmp :
           {Cmp::COMPARE_EQUAL, Cmp::COMPARE_NOTEQUAL, Cmp::COMPARE_LESSTHAN,
            Cmp::COMPARE_GREATERTHANOREQUALTO}) {
        SCOPED_TRACE(absl::StrCat(duckdb::EnumUtil::ToString(cmp), " ",
                                  key.size(), " bytes"));
        const duckdb::ExpressionFilter filter{
          Compared(cmp, duckdb::Value::BLOB_RAW(key))};
        ExpectFilterKeeps(r, *col, *con.context, filter, kRows, EveryRow, value,
                          [&](const std::optional<std::string>& v) {
                            return v && Satisfies(cmp, *v, key);
                          });
      }
    }
  }
}

TEST_F(ColCodecsTest, ConjunctionsOfRangesOnASortedDictionaryKeepTheirOverlap) {
  using Cmp = duckdb::ExpressionType;
  constexpr uint64_t kRows = 30000;
  const auto tenant = [](uint64_t k) {
    return absl::StrCat("tenant-", absl::Dec(k, absl::kZeroPad5), "/item");
  };
  const Value value = [&](uint64_t g) -> std::optional<std::string> {
    if (g % 13 == 0) {
      return std::nullopt;
    }
    return tenant((g * 7919) % 3000);
  };
  const auto lo = tenant(1000);
  const auto hi = tenant(2000);
  const auto both = [](duckdb::unique_ptr<duckdb::Expression> a,
                       duckdb::unique_ptr<duckdb::Expression> b) {
    return duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
      Cmp::CONJUNCTION_AND, std::move(a), std::move(b));
  };
  const duckdb::ExpressionFilter from_lo{
    both(Compared(Cmp::COMPARE_GREATERTHANOREQUALTO, duckdb::Value{lo}),
         Compared(Cmp::COMPARE_LESSTHAN, duckdb::Value{hi}))};
  const duckdb::ExpressionFilter below_hi{
    both(Compared(Cmp::COMPARE_LESSTHAN, duckdb::Value{hi}),
         Compared(Cmp::COMPARE_GREATERTHANOREQUALTO, duckdb::Value{lo}))};
  const duckdb::ExpressionFilter apart{
    both(Compared(Cmp::COMPARE_LESSTHAN, duckdb::Value{lo}),
         Compared(Cmp::COMPARE_GREATERTHAN, duckdb::Value{hi}))};
  const Passes between = [&](const std::optional<std::string>& v) {
    return v && *v >= lo && *v < hi;
  };
  duckdb::Connection con{_db};
  for (const auto codec : kSortedDictionaryCodecs) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    Write(dir, codec, {}, kRows, 8192, value);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    {
      SCOPED_TRACE("from the lower bound");
      ExpectFilterKeeps(r, *col, *con.context, from_lo, kRows, EveryRow, value,
                        between);
    }
    {
      SCOPED_TRACE("from the upper bound");
      ExpectFilterKeeps(r, *col, *con.context, below_hi, kRows, EveryRow, value,
                        between);
    }
    {
      SCOPED_TRACE("disjoint ranges");
      ExpectFilterKeeps(
        r, *col, *con.context, apart, kRows, EveryRow, value,
        [](const std::optional<std::string>&) { return false; });
    }
  }
}

TEST_F(ColCodecsTest, UncompressedStringsKeepANullAfterAnOverflowString) {
  constexpr uint64_t kRows = 3000;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    switch (g % 7) {
      case 0:
        return Pseudo(g, 5000 + g % 13);
      case 1:
        return std::nullopt;
      default:
        return Pseudo(g % 97, 20);
    }
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, irs::codecs::CodecType::Uncompressed, {}, kRows, 1024, value);
  Verify(dir, irs::codecs::CodecType::Uncompressed, kRows, value);
}

TEST_F(ColCodecsTest, ACompactionUnderANumericCodecNameCopiesTheTextBlocks) {
  using irs::codecs::CodecType;
  constexpr uint32_t kRowGroup = 4096;
  constexpr uint64_t kRows = 3 * kRowGroup;
  const Value value = [](uint64_t g) -> std::optional<std::string> {
    if (g % 11 == 0) {
      return std::nullopt;
    }
    return absl::StrCat("tenant-", (g * 7919) % 300, "/item");
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  Write(dir, CodecType::DictLz4, {}, kRows, kRowGroup, value);
  irs::ColReader src = irs::tests::OpenColReader(dir, kSeg);
  const auto* source = src.Column(kField);
  ASSERT_NE(source, nullptr);
  const auto blocks = source->DataBlocks();
  ASSERT_EQ(blocks.size(), kRows / kRowGroup);
  struct Named {
    CodecType codec;
    bool copies;
  };
  for (const auto [codec, copies] :
       {Named{CodecType::Rle, true}, Named{CodecType::Bitpacking, true},
        Named{CodecType::Lz4, false}}) {
    SCOPED_TRACE(magic_enum::enum_name(codec));
    const auto name = absl::StrCat("copy_", magic_enum::enum_name(codec));
    {
      irs::ColWriter w{Db(), dir.Tracker(name)};
      auto& cw =
        w.OpenColumn(kField, duckdb::LogicalType::VARCHAR,
                     {.row_group_size = kRowGroup, .compression = codec});
      ASSERT_EQ(cw.CopyableFrom(*source), copies);
      if (copies) {
        ASSERT_TRUE(cw.CopyBlocks(*source, src.Ctx(), [] { return true; }));
      } else {
        AppendValues(cw, kRows, value);
      }
      ASSERT_TRUE(w.Commit(0));
    }
    irs::ColReader dst = irs::tests::OpenColReader(dir, name);
    const auto* copied = dst.Column(kField);
    ASSERT_NE(copied, nullptr);
    ASSERT_EQ(copied->RowCount(), kRows);
    const auto copied_blocks = copied->DataBlocks();
    if (copies) {
      ASSERT_EQ(copied_blocks.size(), blocks.size());
      for (size_t i = 0; i < blocks.size(); ++i) {
        EXPECT_EQ(copied_blocks[i].codec->type, blocks[i].codec->type);
        EXPECT_EQ(copied_blocks[i].byte_size, blocks[i].byte_size);
      }
    } else {
      for (const auto& block : copied_blocks) {
        EXPECT_EQ(block.codec->type, codec);
      }
    }
    auto state = copied->InitScan(dst.Ctx());
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      ASSERT_EQ(copied->Scan(state, out, STANDARD_VECTOR_SIZE),
                duckdb::idx_t{STANDARD_VECTOR_SIZE});
      out.Flatten(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
        ExpectRow(out, k, pos + k, value);
      }
    }
  }
}

TEST_F(ColCodecsTest, ListElementsFillAnUncompressedBlockAtTheEndOfAVector) {
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::INTERVAL);
  constexpr uint64_t kBlockRows =
    irs::codecs::kBlockSize / sizeof(duckdb::interval_t);
  constexpr uint64_t kRows = 2 * STANDARD_VECTOR_SIZE;
  const auto length = [](uint64_t g) -> uint64_t {
    if (g >= STANDARD_VECTOR_SIZE) {
      return 2;
    }
    return kBlockRows / STANDARD_VECTOR_SIZE +
           (g < kBlockRows % STANDARD_VECTOR_SIZE ? 1 : 0);
  };
  const auto element = [](uint64_t g, uint64_t j) {
    return duckdb::interval_t{static_cast<int32_t>((g + j) % 12),
                              static_cast<int32_t>((g * 7 + j) % 28),
                              static_cast<int64_t>(g * 1000 + j)};
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker(kSeg)};
    auto& cw = w.OpenColumn(kField, type,
                            {.row_group_size = static_cast<uint32_t>(kRows),
                             .distinct = irs::DistinctStats::Exact});
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector vec{type, STANDARD_VECTOR_SIZE};
      for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
        duckdb::vector<duckdb::Value> items;
        for (uint64_t j = 0; j < length(pos + k); ++j) {
          items.push_back(duckdb::Value::INTERVAL(element(pos + k, j)));
        }
        vec.SetValue(k, duckdb::Value::LIST(duckdb::LogicalType::INTERVAL,
                                            std::move(items)));
      }
      duckdb::FlatVector::SetSize(vec, STANDARD_VECTOR_SIZE);
      cw.Append(vec, STANDARD_VECTOR_SIZE);
    }
    ASSERT_TRUE(w.Commit(0));
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), kRows);
  const auto* elements = col->Child();
  ASSERT_NE(elements, nullptr);
  const auto blocks = elements->DataBlocks();
  ASSERT_EQ(blocks.size(), 2u);
  EXPECT_EQ(blocks[0].codec->type, irs::codecs::CodecType::Uncompressed);
  EXPECT_EQ(blocks[0].tuple_count, kBlockRows);
  EXPECT_EQ(blocks[1].tuple_count, 2 * STANDARD_VECTOR_SIZE);
  auto state = col->InitScan(r.Ctx());
  for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
    duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
    ASSERT_EQ(col->Scan(state, out, STANDARD_VECTOR_SIZE),
              duckdb::idx_t{STANDARD_VECTOR_SIZE});
    for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
      const auto g = pos + k;
      const auto list = out.GetValue(k);
      ASSERT_FALSE(list.IsNull()) << g;
      const auto& items = duckdb::ListValue::GetChildren(list);
      ASSERT_EQ(items.size(), length(g)) << g;
      for (uint64_t j = 0; j < items.size(); ++j) {
        const auto got = items[j].GetValue<duckdb::interval_t>();
        const auto expected = element(g, j);
        EXPECT_EQ(got.months, expected.months) << g << ":" << j;
        EXPECT_EQ(got.days, expected.days) << g << ":" << j;
        EXPECT_EQ(got.micros, expected.micros) << g << ":" << j;
      }
    }
  }
}

TEST_F(ColCodecsTest, ZstdTextFramesNeverCrossABatchOfTheirRowGroup) {
  constexpr uint64_t kRows = 30000;
  constexpr uint32_t kRowGroup = 10000;
  const Value values = [](uint64_t g) -> std::optional<std::string> {
    if (g % 97 == 0) {
      return std::nullopt;
    }
    if (g >= 20000) {
      return absl::StrCat("s", g % 13);
    }
    return Pseudo(g, 100 + g % 120);
  };
  const auto path = test_dir() / "col_codecs_zstd_batch_frames";
  std::filesystem::create_directories(path);
  tests::IndexDir dir{Db(), path.string()};
  Write(dir, irs::codecs::CodecType::Zstd, {}, kRows, kRowGroup, values);
  Verify(dir, irs::codecs::CodecType::Zstd, kRows, values);

  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  {
    auto state = col->InitScan(r.Ctx());
    duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    col->Scan(state, out, 100);
    out.Flatten(100);
    for (duckdb::idx_t k = 0; k < 100; ++k) {
      ExpectRow(out, k, k, values);
    }
  }
  std::string bytes;
  {
    std::ifstream in{path / irs::FileName(kSeg), std::ios::binary};
    bytes.assign(std::istreambuf_iterator<char>{in}, {});
  }
  uint64_t row_in_group = 0;
  uint64_t whole_batches = 0;
  uint64_t split_batches = 0;
  for (const auto& block : col->DataBlocks()) {
    ASSERT_EQ(block.codec->type, irs::codecs::CodecType::Zstd);
    ASSERT_LE(block.file_offset + block.byte_size, bytes.size());
    auto* base =
      reinterpret_cast<duckdb::data_ptr_t>(bytes.data() + block.file_offset);
    const auto h =
      irs::codecs::Header::Parse(base, block.byte_size, block.codec->type);
    EXPECT_EQ(h.flags, irs::codecs::FrameDictionary::None);
    ASSERT_NE(h.frame_count, 0u);
    for (uint32_t f = 0; f < h.frame_count; ++f) {
      const auto frame = duckdb::Load<irs::codecs::FrameMeta>(
        base + h.off_frames + f * irs::codecs::kFrameMetaSize);
      const uint64_t end =
        f + 1 < h.frame_count
          ? duckdb::Load<irs::codecs::FrameMeta>(
              base + h.off_frames + (f + 1) * irs::codecs::kFrameMetaSize)
              .first_entry
          : h.entry_count;
      const auto first = row_in_group + frame.first_entry;
      const auto last = row_in_group + end - 1;
      EXPECT_EQ(first / STANDARD_VECTOR_SIZE, last / STANDARD_VECTOR_SIZE)
        << "rows " << first << ".." << last;
      EXPECT_LE(frame.raw_len, irs::codecs::kZstdFrameBytes);
      whole_batches += end - frame.first_entry == STANDARD_VECTOR_SIZE;
      split_batches +=
        first % STANDARD_VECTOR_SIZE != 0 && f != 0 && first % kRowGroup != 0;
    }
    row_in_group = (row_in_group + block.tuple_count) % kRowGroup;
  }
  EXPECT_NE(whole_batches, 0u);
  EXPECT_NE(split_batches, 0u);
}

TEST_F(ColCodecsTest, ZstdTextWithoutALevelCompressesAtLevelThree) {
  const Value values = [](uint64_t g) -> std::optional<std::string> {
    return absl::StrCat("https://shop", g % 97, ".example.org/item?id=", g,
                        "&ref=", g % 13 * 7919);
  };
  const auto blocks = [&](uint8_t level) {
    const auto path =
      test_dir() / absl::StrCat("col_codecs_zstd_level_", level);
    std::filesystem::create_directories(path);
    tests::IndexDir dir{Db(), path.string()};
    Write(dir, irs::codecs::CodecType::Zstd, {.compression_level = level},
          20000, DEFAULT_ROW_GROUP_SIZE, values);
    std::string file;
    {
      std::ifstream in{path / irs::FileName(kSeg), std::ios::binary};
      file.assign(std::istreambuf_iterator<char>{in}, {});
    }
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    std::string out;
    for (const auto& block : col->DataBlocks()) {
      const auto h =
        irs::codecs::Header::Parse(reinterpret_cast<duckdb::const_data_ptr_t>(
                                     file.data() + block.file_offset),
                                   block.byte_size, block.codec->type);
      out.append(file, block.file_offset + h.off_data, h.data_size);
    }
    return out;
  };
  const auto unset = blocks(0);
  EXPECT_EQ(unset, blocks(3));
  EXPECT_NE(unset, blocks(1));
}
