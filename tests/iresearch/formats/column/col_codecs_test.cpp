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

#include <cstdint>
#include <cstdio>
#include <duckdb.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/function/compression_function.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <filesystem>
#include <functional>
#include <iresearch/formats/column/codecs/dictionary_cache.hpp>
#include <iresearch/formats/column/codecs/registry.hpp>
#include <iresearch/formats/column/codecs/string_writer.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/internal/gather_arms.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <optional>
#include <random>
#include <string>
#include <vector>

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
  if (const auto choice = irs::codecs::ColCodecs::Choice(codec)) {
    return irs::codecs::ColCodecs::TypeOf(*choice);
  }
  return codec;
}

class ColCodecsTest : public TestBase {
 protected:
  duckdb::DatabaseInstance& Db() { return *_db.instance; }

  void Write(irs::Directory& dir, duckdb::CompressionType codec,
             irs::ColCodecParams params, uint64_t rows, uint32_t rg_size,
             const Value& value) {
    irs::ColWriter w{dir, kSeg, Db()};
    auto& cw = w.OpenColumn(kField, duckdb::LogicalType::VARCHAR,
                            /*skip_validity=*/false, rg_size, codec,
                            /*hyperloglog=*/false, params);
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
    ASSERT_TRUE(w.Commit(0));
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
    irs::ColReader r{dir, std::string{kSeg}, Db()};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    ASSERT_EQ(col->RowCount(), rows);
    for (const auto& block : col->DataBlocks()) {
      EXPECT_EQ(block.codec->type, Written(codec));
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
  }

  void RoundTrip(duckdb::CompressionType codec, irs::ColCodecParams params,
                 uint64_t rows, uint32_t rg_size, const Value& value) {
    irs::MemoryDirectory dir{};
    Write(dir, codec, params, rows, rg_size, value);
    Verify(dir, codec, rows, value);
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
                           duckdb::CompressionType::COMPRESSION_ZXC}) {
    irs::MemoryDirectory dir{};
    Write(dir, codec, {.segment_target = 64 * 1024}, 6000, 2048,
          kLongTextWithNulls);
    irs::ColReader r{dir, std::string{kSeg}, Db()};
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
          irs::ColReader r{dir, std::string{kSeg}, Db()};
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
    irs::ColReader r{dir, std::string{kSeg}, Db()};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    irs::ReadContext ctx{r};
    irs::BlockWindow window{};
    window = col->Locate(0, window);
    auto seg = col->OpenSegment(window.block, ctx);
    auto info = seg->GetCompressionFunction().get_segment_info(
      duckdb::QueryContext{}, *seg);
    return info["level"];
  };
  for (const uint8_t column_level : {uint8_t{0}, uint8_t{9}}) {
    irs::FunctionFieldOptions options{
      [column_level](irs::field_id) {
        return irs::ColumnOptions{
          .compression = duckdb::CompressionType::COMPRESSION_DICT_ZSTD,
          .compression_level = column_level};
      },
      [](irs::field_id id) { return id; }, 1024};
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
  irs::ColReader r{dir, std::string{kSeg}, Db()};
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
  irs::ColReader r{dir, std::string{kSeg}, Db()};
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

}  // namespace

TEST_F(ColCodecsTest, EstimateTracksSealedSize) {
  using irs::codecs::ByteCodec;
  using irs::codecs::Shape;
  struct Corpus {
    const char* name;
    const Value* value;
    uint64_t rows;
  };
  const Corpus corpora[] = {
    {"low-cardinality", &kLowCardinalityWithNulls, 40000},
    {"unique-short", &kUniqueShort, 40000},
    {"word-soup", &kWordSoup, 20000},
    {"long-text", &kLongTextWithNulls, 3000},
  };
  const irs::codecs::StringChoice choices[] = {
    {Shape::Dedup, ByteCodec::Fsst}, {Shape::Dedup, ByteCodec::Lz4},
    {Shape::Dedup, ByteCodec::Zstd}, {Shape::Dedup, ByteCodec::Zxc},
    {Shape::Plain, ByteCodec::Fsst}, {Shape::Plain, ByteCodec::Lz4},
    {Shape::Plain, ByteCodec::Zstd}, {Shape::Plain, ByteCodec::Zxc},
  };
  for (const auto& corpus : corpora) {
    const auto chunks = Chunks(corpus.rows, *corpus.value);
    for (const auto choice : choices) {
      irs::codecs::StringAccumulator acc{choice.shape == Shape::Dedup};
      for (const auto& c : chunks) {
        acc.Add(*c);
      }
      const irs::ColCodecParams params{};
      const auto priced = irs::codecs::Price(acc, choice, params);
      uint64_t sealed = 0;
      irs::codecs::SealSegments(acc, priced, params,
                                duckdb::LogicalType::VARCHAR,
                                [&](duckdb::BaseStatistics, uint64_t,
                                    std::span<const std::string_view> parts) {
                                  for (const auto part : parts) {
                                    sealed += part.size();
                                  }
                                });
      ASSERT_GT(sealed, 0u);
      const double error = std::fabs(static_cast<double>(priced.bytes) -
                                     static_cast<double>(sealed)) /
                           static_cast<double>(sealed);
      EXPECT_LE(error, 0.25)
        << corpus.name << " shape " << static_cast<int>(choice.shape)
        << " leaf " << static_cast<int>(choice.leaf) << " estimate "
        << priced.bytes << " sealed " << sealed;
    }
  }
}

TEST_F(ColCodecsTest, AutoObjectivePicksTheLeaf) {
  using duckdb::CompressionType;
  const auto written = [&](irs::AutoObjective objective, const Value& value,
                           uint64_t rows, std::set<CompressionType>& types) {
    irs::MemoryDirectory dir{};
    Write(dir, CompressionType::COMPRESSION_AUTO, {.objective = objective},
          rows, 16384, value);
    irs::ColReader r{dir, std::string{kSeg}, Db()};
    const auto* col = r.Column(kField);
    EXPECT_NE(col, nullptr);
    uint64_t bytes = 0;
    for (const auto& block : col->DataBlocks()) {
      types.insert(block.codec->type);
      bytes += block.byte_size;
    }
    return bytes;
  };
  const std::set<CompressionType> lz4{
    CompressionType::COMPRESSION_LZ4, CompressionType::COMPRESSION_DICT_LZ4,
    CompressionType::COMPRESSION_UNCOMPRESSED};
  const std::set<CompressionType> balanced{
    CompressionType::COMPRESSION_LZ4, CompressionType::COMPRESSION_DICT_LZ4,
    CompressionType::COMPRESSION_COL_FSST,
    CompressionType::COMPRESSION_COL_DICT_FSST,
    CompressionType::COMPRESSION_UNCOMPRESSED};
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
    std::set<CompressionType> speed_types;
    std::set<CompressionType> balanced_types;
    std::set<CompressionType> size_types;
    const auto speed_bytes = written(irs::AutoObjective::Speed, *corpus.value,
                                     corpus.rows, speed_types);
    const auto balanced_bytes = written(
      irs::AutoObjective::Balanced, *corpus.value, corpus.rows, balanced_types);
    const auto size_bytes =
      written(irs::AutoObjective::Size, *corpus.value, corpus.rows, size_types);
    for (const auto t : speed_types) {
      EXPECT_TRUE(lz4.contains(t))
        << corpus.name << " " << duckdb::CompressionTypeToString(t);
    }
    for (const auto t : balanced_types) {
      EXPECT_TRUE(balanced.contains(t))
        << corpus.name << " " << duckdb::CompressionTypeToString(t);
    }
    EXPECT_LE(size_bytes, balanced_bytes + balanced_bytes / 20)
      << corpus.name << " size " << size_bytes << " balanced "
      << balanced_bytes;
    EXPECT_LE(balanced_bytes, speed_bytes + speed_bytes / 20)
      << corpus.name << " balanced " << balanced_bytes << " speed "
      << speed_bytes;
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
    irs::ColReader r{dir, std::string{kSeg}, Db()};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    ASSERT_GT(col->DataBlocks().size(), 1u);
    for (size_t b = 0; b < col->DataBlocks().size(); ++b) {
      keys.push_back(irs::codecs::DictionaryCacheKey(col->CacheScope(), b));
      EXPECT_FALSE(cached(keys.back()));
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
      irs::ColReader other{dir, std::string{kSeg}, Db()};
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
  }
  for (const auto& key : keys) {
    EXPECT_FALSE(cached(key)) << key;
  }
}

TEST_F(ColCodecsTest, DictionaryCompletedByPartialScansIsCached) {
  const auto& value = kLowCardinalityWithNulls;
  constexpr uint64_t kRows = 20000;
  irs::MemoryDirectory dir{};
  Write(dir, duckdb::CompressionType::COMPRESSION_DICT_FSST, {}, kRows, 8192,
        value);
  auto& cache = Db().GetObjectCache();
  irs::ColReader r{dir, std::string{kSeg}, Db()};
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  ASSERT_GT(col->DataBlocks().size(), 1u);
  std::vector<std::string> keys;
  for (size_t b = 0; b < col->DataBlocks().size(); ++b) {
    keys.push_back(irs::codecs::DictionaryCacheKey(col->CacheScope(), b));
  }
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
  irs::ColReader r{dir, std::string{kSeg}, Db()};
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
  auto filter_state = duckdb::TableFilterState::Initialize(*con.context, filter);
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

TEST_F(ColCodecsTest, MappedFileUnalignedBlocks) {
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
      irs::ColReader r{dir, std::string{kSeg}, Db()};
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      bool unaligned = false;
      for (const auto& block : col->DataBlocks()) {
        unaligned |= block.file_offset % 8 != 0;
      }
      EXPECT_TRUE(unaligned) << duckdb::CompressionTypeToString(arm.codec);
    }
    Verify(dir, arm.codec, 5000, *arm.value);
    dir.remove(irs::FileName(kSeg));
  }
}

TEST_F(ColCodecsTest, MappedFileNumericCodecsUnaligned) {
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
    auto& rle = w.OpenColumn(kRle, duckdb::LogicalType::BIGINT, false, 4096,
                             duckdb::CompressionType::COMPRESSION_RLE);
    auto& bp =
      w.OpenColumn(kBitpacking, duckdb::LogicalType::INTEGER, false, 4096,
                   duckdb::CompressionType::COMPRESSION_BITPACKING);
    auto& alp = w.OpenColumn(kAlp, duckdb::LogicalType::DOUBLE, false, 4096,
                             duckdb::CompressionType::COMPRESSION_ALP);
    auto& bools = w.OpenColumn(kBool, duckdb::LogicalType::BOOLEAN, false, 4096,
                               duckdb::CompressionType::COMPRESSION_ROARING);
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
  irs::ColReader r{dir, std::string{kSeg}, Db()};
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
