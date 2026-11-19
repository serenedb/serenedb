////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <absl/base/internal/endian.h>
#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <bit>
#include <cstdint>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <functional>
#include <limits>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "pg/serialize.h"
#include "server/utils/message_buffer.h"

namespace {

using sdb::pg::VarFormat;

constexpr duckdb::idx_t kRows = 300;
constexpr duckdb::idx_t kLongStringRow = 5;
constexpr duckdb::idx_t kLongBlobRow = 7;

constexpr std::string_view kStrings[] = {
  "",
  "plain",
  "with\ttab",
  "new\nline",
  "carriage\rreturn",
  "quote\"inside",
  "back\\slash",
  "\\.",
  "\\N",
  "NULL",
  "a,b",
  "-1",
  "pipe|pipe",
  "dot.dot",
  "a string longer than twelve bytes",
  "\xc3\xbcnic\xc3\xb6\x64\x65",
  "\b\f\v",
};

constexpr std::string_view kJson[] = {
  R"({"a": 1, "b": [true, null]})",
  "[1, 2.5, \"x\"]",
  "\"str\\\"ing\"",
  "null",
  "{}",
};

constexpr std::string_view kPgCopyHeader{
  "PGCOPY\n\xFF\r\n\0"
  "\0\0\0\0"
  "\0\0\0\0",
  19};

std::vector<duckdb::LogicalType> Types() {
  using duckdb::LogicalType;
  return {
    LogicalType::BOOLEAN,        LogicalType::TINYINT,
    LogicalType::SMALLINT,       LogicalType::INTEGER,
    LogicalType::BIGINT,         LogicalType::UTINYINT,
    LogicalType::USMALLINT,      LogicalType::UINTEGER,
    LogicalType::UBIGINT,        LogicalType::HUGEINT,
    LogicalType::FLOAT,          LogicalType::DOUBLE,
    LogicalType::DECIMAL(4, 1),  LogicalType::DECIMAL(9, 2),
    LogicalType::DECIMAL(18, 3), LogicalType::DECIMAL(38, 5),
    LogicalType::DATE,           LogicalType::UUID,
    LogicalType::VARCHAR,        LogicalType::BLOB,
    LogicalType::TIMESTAMP,      LogicalType::TIMESTAMP_TZ,
    LogicalType::INTERVAL,       LogicalType::LIST(LogicalType::INTEGER),
    LogicalType::JSON(),
  };
}

template<typename T>
T Mod(uint64_t bits, int64_t modulus) {
  return static_cast<T>(static_cast<int64_t>(bits) % modulus);
}

duckdb::Value RandomValue(const duckdb::LogicalType& type,
                          std::mt19937_64& rng) {
  if (rng() % 8 == 0) {
    return duckdb::Value(type);
  }
  const auto bits = rng();
  switch (type.id()) {
    using enum duckdb::LogicalTypeId;
    case BOOLEAN:
      return duckdb::Value::BOOLEAN((bits & 1) != 0);
    case TINYINT:
      return duckdb::Value::TINYINT(static_cast<int8_t>(bits));
    case SMALLINT:
      return duckdb::Value::SMALLINT(static_cast<int16_t>(bits));
    case INTEGER:
      return duckdb::Value::INTEGER(static_cast<int32_t>(bits));
    case BIGINT:
      return duckdb::Value::BIGINT(static_cast<int64_t>(bits));
    case UTINYINT:
      return duckdb::Value::UTINYINT(static_cast<uint8_t>(bits));
    case USMALLINT:
      return duckdb::Value::USMALLINT(static_cast<uint16_t>(bits));
    case UINTEGER:
      return duckdb::Value::UINTEGER(static_cast<uint32_t>(bits));
    case UBIGINT:
      return duckdb::Value::UBIGINT(bits);
    case HUGEINT:
      return duckdb::Value::HUGEINT(
        duckdb::hugeint_t{static_cast<int64_t>(rng()), bits});
    case FLOAT: {
      constexpr float kSpecial[] = {
        0.0f,
        -0.0f,
        std::numeric_limits<float>::infinity(),
        -std::numeric_limits<float>::infinity(),
        std::numeric_limits<float>::quiet_NaN(),
        std::numeric_limits<float>::denorm_min(),
        std::numeric_limits<float>::max(),
      };
      if (bits % 4 == 0) {
        return duckdb::Value::FLOAT(
          kSpecial[(bits >> 8) % std::size(kSpecial)]);
      }
      return duckdb::Value::FLOAT(
        static_cast<float>(static_cast<int32_t>(bits)) /
        static_cast<float>((bits >> 40) | 1));
    }
    case DOUBLE: {
      constexpr double kSpecial[] = {
        0.0,
        -0.0,
        std::numeric_limits<double>::infinity(),
        -std::numeric_limits<double>::infinity(),
        std::numeric_limits<double>::quiet_NaN(),
        std::numeric_limits<double>::denorm_min(),
        std::numeric_limits<double>::max(),
      };
      if (bits % 4 == 0) {
        return duckdb::Value::DOUBLE(
          kSpecial[(bits >> 8) % std::size(kSpecial)]);
      }
      return duckdb::Value::DOUBLE(
        static_cast<double>(static_cast<int64_t>(bits)) /
        static_cast<double>((bits >> 30) | 1));
    }
    case DECIMAL: {
      const auto width = duckdb::DecimalType::GetWidth(type);
      const auto scale = duckdb::DecimalType::GetScale(type);
      switch (type.InternalType()) {
        using enum duckdb::PhysicalType;
        case INT16:
          return duckdb::Value::DECIMAL(Mod<int16_t>(bits, 10'000), width,
                                        scale);
        case INT32:
          return duckdb::Value::DECIMAL(Mod<int32_t>(bits, 1'000'000'000),
                                        width, scale);
        case INT64:
          return duckdb::Value::DECIMAL(
            Mod<int64_t>(bits, 1'000'000'000'000'000'000), width, scale);
        default:
          return duckdb::Value::DECIMAL(
            duckdb::hugeint_t{static_cast<int64_t>(rng()) >> 2, bits}, width,
            scale);
      }
    }
    case DATE:
      if (bits % 16 == 0) {
        return duckdb::Value::DATE(bits % 32 == 0
                                     ? duckdb::date_t::infinity()
                                     : duckdb::date_t::ninfinity());
      }
      return duckdb::Value::DATE(
        duckdb::date_t{Mod<int32_t>(bits, 4'000'000) - 1'000'000});
    case UUID:
      return duckdb::Value::UUID(
        duckdb::hugeint_t{static_cast<int64_t>(rng()), bits});
    case VARCHAR:
      if (type.IsJSONType()) {
        return duckdb::Value{std::string{kJson[bits % std::size(kJson)]}}
          .DefaultCastAs(type);
      }
      return duckdb::Value{std::string{kStrings[bits % std::size(kStrings)]}};
    case BLOB: {
      std::string bytes(bits % 40, '\0');
      for (auto& byte : bytes) {
        byte = static_cast<char>(rng());
      }
      return duckdb::Value::BLOB_RAW(bytes);
    }
    case TIMESTAMP:
      if (bits % 16 == 0) {
        return duckdb::Value::TIMESTAMP(duckdb::timestamp_t::infinity());
      }
      return duckdb::Value::TIMESTAMP(duckdb::timestamp_t{
        Mod<int64_t>(bits, 400'000'000'000'000'000) - 60'000'000'000'000'000});
    case TIMESTAMP_TZ:
      if (bits % 16 == 0) {
        return duckdb::Value::TIMESTAMPTZ(
          duckdb::timestamp_tz_t{duckdb::timestamp_t::ninfinity()});
      }
      return duckdb::Value::TIMESTAMPTZ(duckdb::timestamp_tz_t{
        Mod<int64_t>(bits, 4'000'000'000'000'000) - 600'000'000'000'000});
    case INTERVAL:
      return duckdb::Value::INTERVAL(Mod<int32_t>(bits, 1000),
                                     Mod<int32_t>(bits >> 16, 100),
                                     Mod<int64_t>(bits >> 8, 100'000'000'000));
    case LIST: {
      duckdb::vector<duckdb::Value> items;
      for (auto i = bits % 4; i > 0; --i) {
        items.push_back(
          rng() % 4 == 0 ? duckdb::Value{duckdb::LogicalType::INTEGER}
                         : duckdb::Value::INTEGER(static_cast<int32_t>(rng())));
      }
      return duckdb::Value::LIST(duckdb::LogicalType::INTEGER,
                                 std::move(items));
    }
    default:
      ADD_FAILURE() << "no generator for " << type.ToString();
      return duckdb::Value(type);
  }
}

std::string Collect(const sdb::message::Buffer& buffer) {
  std::string bytes;
  for (const auto piece : buffer.Written()) {
    bytes.append(static_cast<const char*>(piece.data()), piece.size());
  }
  return bytes;
}

std::string Difference(std::string_view expected, std::string_view actual) {
  size_t at = 0;
  while (at < expected.size() && at < actual.size() &&
         expected[at] == actual[at]) {
    ++at;
  }
  if (at == expected.size() && at == actual.size()) {
    return {};
  }
  const auto around = [&](std::string_view s) {
    const auto from = at < 16 ? 0 : at - 16;
    return absl::CEscape(s.substr(from, 48));
  };
  return absl::StrCat("sizes ", expected.size(), " vs ", actual.size(),
                      ", first difference at ", at, ": expected '",
                      around(expected), "', actual '", around(actual), "'");
}

std::string RowByRow(sdb::pg::SerializationContext& ctx,
                     std::span<const duckdb::LogicalType> types,
                     std::span<const VarFormat> formats,
                     const duckdb::DataChunk& chunk,
                     const sdb::pg::RowShape& shape, duckdb::idx_t begin,
                     duckdb::idx_t end) {
  std::vector<sdb::pg::SerializationFunction> fields;
  std::vector<duckdb::RecursiveUnifiedVectorFormat> decoded(types.size());
  for (size_t column = 0; column < types.size(); ++column) {
    fields.push_back(
      sdb::pg::GetSerialization(types[column], formats[column], ctx));
    duckdb::Vector::RecursiveToUnifiedFormat(chunk.data[column], chunk.size(),
                                             decoded[column]);
  }
  sdb::message::Buffer out{64, 4096};
  for (auto row = begin; row < end; ++row) {
    sdb::message::Writer writer{out};
    ctx.writer = &writer;
    uint8_t* header = shape.tag != '\0' ? writer.Alloc(5) : nullptr;
    if (row == begin && !shape.first.empty()) {
      writer.Write(shape.first);
    }
    if (shape.field_count) {
      absl::big_endian::Store16(writer.Alloc(2),
                                static_cast<uint16_t>(types.size()));
    }
    if (!shape.before.empty()) {
      writer.Write(shape.before);
    }
    for (size_t column = 0; column < types.size(); ++column) {
      if (column > 0 && shape.delimited) {
        writer.Write({&ctx.copy_delim, 1});
      }
      fields[column](ctx, decoded[column], row);
    }
    if (!shape.after.empty()) {
      writer.Write(shape.after);
    }
    if (header) {
      header[0] = static_cast<uint8_t>(shape.tag);
      absl::big_endian::Store32(header + 1,
                                static_cast<uint32_t>(writer.Written() - 1));
    }
    writer.Commit(false);
  }
  ctx.writer = nullptr;
  return Collect(out);
}

std::string ByChunk(sdb::pg::SerializationContext& ctx,
                    std::span<const duckdb::LogicalType> types,
                    std::span<const VarFormat> formats,
                    const duckdb::DataChunk& chunk,
                    const sdb::pg::RowShape& shape, duckdb::idx_t begin,
                    duckdb::idx_t end) {
  sdb::message::Buffer out{64, 4096};
  sdb::pg::ChunkSerializer serializer{types, formats, ctx};
  serializer.Write(out, ctx, chunk, begin, end, shape);
  return Collect(out);
}

sdb::pg::SerializationContext Context() {
  sdb::pg::SerializationContext ctx;
  ctx.bytea_output = sdb::ByteaOutput::Hex;
  ctx.caches = std::make_unique<sdb::pg::SerializationCaches>();
  return ctx;
}

struct Mode {
  std::string name;
  std::function<void(sdb::pg::SerializationContext&)> setup;
  std::function<VarFormat(size_t)> format;
  sdb::pg::RowShape shape;
};

std::vector<Mode> Modes() {
  const auto all = [](VarFormat format) {
    return [format](size_t) { return format; };
  };
  const auto none = [](sdb::pg::SerializationContext&) {};
  const auto text = [](char delim, std::string null) {
    return [delim, null](sdb::pg::SerializationContext& ctx) {
      sdb::pg::SetCopyText(ctx, delim, null);
    };
  };
  const auto csv = [](char delim, char quote, char escape, std::string null,
                      std::string new_line) {
    return [=](sdb::pg::SerializationContext& ctx) {
      sdb::pg::SetCopyCsv(ctx, delim, quote, escape, null, new_line, false);
    };
  };
  return {
    {"data row text",
     none,
     all(VarFormat::Text),
     {.tag = 'D', .field_count = true}},
    {"data row binary",
     none,
     all(VarFormat::Binary),
     {.tag = 'D', .field_count = true}},
    {"data row mixed",
     none,
     [](size_t column) {
       return column % 2 == 0 ? VarFormat::Text : VarFormat::Binary;
     },
     {.tag = 'D', .field_count = true}},
    {"copy binary stdout",
     none,
     all(VarFormat::Binary),
     {.tag = 'd', .field_count = true, .first = kPgCopyHeader}},
    {"copy binary file", none, all(VarFormat::Binary), {.field_count = true}},
    {"copy text tab",
     text('\t', "\\N"),
     all(VarFormat::CopyText),
     {.tag = 'd', .delimited = true, .after = "\n"}},
    {"copy text dash",
     text('-', "\\N"),
     all(VarFormat::CopyText),
     {.delimited = true, .after = "\n"}},
    {"copy text quote",
     text('"', ""),
     all(VarFormat::CopyText),
     {.delimited = true, .after = "\n"}},
    {"copy text separated",
     text(',', "NULL"),
     all(VarFormat::CopyText),
     {.delimited = true, .before = "\n"}},
    {"csv",
     csv(',', '"', '"', "", "\n"),
     all(VarFormat::CopyCsv),
     {.delimited = true, .after = "\n"}},
    {"csv backslash escape",
     csv('|', '"', '\\', "NULL", "\r\n"),
     all(VarFormat::CopyCsv),
     {.tag = 'd', .delimited = true, .after = "\r\n"}},
    {"csv dot delimiter",
     csv('.', '\'', '\'', "\\N", "\n"),
     all(VarFormat::CopyCsv),
     {.delimited = true, .after = "\n"}},
    {"csv force quote",
     csv(',', '"', '"', "", "\n"),
     [](size_t column) {
       return column % 2 == 0 ? VarFormat::CopyCsvQuoted : VarFormat::CopyCsv;
     },
     {.delimited = true, .after = "\n"}},
    {"csv without quote",
     csv('\t', '\0', '\0', "", "\n"),
     all(VarFormat::CopyCsv),
     {.delimited = true, .after = "\n"}},
  };
}

void FillRandom(duckdb::DataChunk& chunk,
                std::span<const duckdb::LogicalType> types, uint64_t seed) {
  std::mt19937_64 rng{seed};
  chunk.Initialize(
    duckdb::Allocator::DefaultAllocator(),
    duckdb::vector<duckdb::LogicalType>{types.begin(), types.end()}, kRows);
  for (size_t column = 0; column < types.size(); ++column) {
    for (duckdb::idx_t row = 0; row < kRows; ++row) {
      chunk.SetValue(column, row, RandomValue(types[column], rng));
    }
    if (types[column] == duckdb::LogicalType::VARCHAR) {
      chunk.SetValue(column, kLongStringRow,
                     duckdb::Value{std::string(70'000, 'x') + "\t\"\\,|."});
    }
    if (types[column] == duckdb::LogicalType::BLOB) {
      chunk.SetValue(column, kLongBlobRow,
                     duckdb::Value::BLOB_RAW(std::string(40'000, '\\')));
    }
  }
  chunk.SetCardinality(kRows);
}

void Compare(const Mode& mode, std::span<const duckdb::LogicalType> types,
             const duckdb::DataChunk& chunk, duckdb::idx_t begin,
             duckdb::idx_t end) {
  std::vector<VarFormat> formats;
  for (size_t column = 0; column < types.size(); ++column) {
    formats.push_back(mode.format(column));
  }
  auto row_ctx = Context();
  mode.setup(row_ctx);
  auto chunk_ctx = Context();
  mode.setup(chunk_ctx);
  const auto expected =
    RowByRow(row_ctx, types, formats, chunk, mode.shape, begin, end);
  const auto actual =
    ByChunk(chunk_ctx, types, formats, chunk, mode.shape, begin, end);
  EXPECT_EQ(Difference(expected, actual), "")
    << mode.name << ", rows [" << begin << ", " << end << ")";
}

TEST(PgSerializeChunkTest, MatchesRowByRowOnFlatVectors) {
  const auto types = Types();
  for (uint64_t seed = 1; seed <= 3; ++seed) {
    duckdb::DataChunk chunk;
    FillRandom(chunk, types, seed);
    for (const auto& mode : Modes()) {
      Compare(mode, types, chunk, 0, kRows);
      Compare(mode, types, chunk, 3, kRows - 2);
      Compare(mode, types, chunk, kLongStringRow, kLongStringRow + 1);
    }
  }
}

TEST(PgSerializeChunkTest, MatchesRowByRowOnDictionaryAndConstantVectors) {
  const auto types = Types();
  duckdb::DataChunk chunk;
  FillRandom(chunk, types, 7);
  duckdb::SelectionVector sel{kRows};
  for (duckdb::idx_t row = 0; row < kRows; ++row) {
    sel.set_index(row, (row * 7) % kRows);
  }
  chunk.Slice(sel, kRows);
  std::mt19937_64 rng{11};
  for (size_t column = 0; column < types.size(); column += 3) {
    chunk.data[column].Reference(RandomValue(types[column], rng),
                                 duckdb::count_t{kRows});
  }
  for (const auto& mode : Modes()) {
    Compare(mode, types, chunk, 0, kRows);
  }
}

TEST(PgSerializeChunkTest, MatchesRowByRowForSingleColumnCsv) {
  const std::vector<duckdb::LogicalType> types{duckdb::LogicalType::VARCHAR};
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(), {types[0]}, kRows);
  for (duckdb::idx_t row = 0; row < kRows; ++row) {
    chunk.SetValue(
      0, row,
      row % 5 == 0
        ? duckdb::Value{duckdb::LogicalType::VARCHAR}
        : duckdb::Value{std::string{kStrings[row % std::size(kStrings)]}});
  }
  chunk.SetCardinality(kRows);
  const Mode mode{
    "csv single column",
    [](sdb::pg::SerializationContext& ctx) {
      sdb::pg::SetCopyCsv(ctx, ',', '"', '"', "", "\n", true);
    },
    [](size_t) { return VarFormat::CopyCsv; },
    {.delimited = true, .after = "\n"},
  };
  Compare(mode, types, chunk, 0, kRows);
}

TEST(PgSerializeChunkTest, WritesRowsWithoutColumns) {
  duckdb::DataChunk chunk;
  chunk.SetCardinality(4);
  auto ctx = Context();
  sdb::message::Buffer out{64, 4096};
  sdb::pg::ChunkSerializer serializer{{}, {}, ctx};
  serializer.Write(out, ctx, chunk, 0, 4, {.tag = 'D', .field_count = true});
  EXPECT_EQ(Collect(out),
            std::string(std::string_view{
              "D\0\0\0\6\0\0D\0\0\0\6\0\0D\0\0\0\6\0\0D\0\0\0\6\0\0", 28}));
}

}  // namespace
