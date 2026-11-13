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

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <duckdb.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iresearch/formats/column/codecs/registry.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/internal/gather_arms.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <set>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "tests_shared.hpp"

namespace {

using duckdb::LogicalType;
using duckdb::Value;
using irs::codecs::CodecType;

constexpr irs::field_id kField = 5;
constexpr std::string_view kSeg = "seg";
constexpr uint32_t kRgSize = 4096;
constexpr uint64_t kRows = 2600;

struct Rows {
  const uint64_t* p;
  size_t n;
  size_t size() const noexcept { return n; }
  uint64_t operator[](size_t i) const noexcept { return p[i]; }
};

struct Case {
  std::string name;
  LogicalType type;
  CodecType codec;
  std::function<Value(uint64_t)> value;
  std::set<std::string> rejected;
  bool validity = false;
  uint64_t rows = kRows;
};

uint64_t Mix(uint64_t g) noexcept {
  g += 0x9E3779B97F4A7C15ULL;
  g = (g ^ (g >> 30)) * 0xBF58476D1CE4E5B9ULL;
  g = (g ^ (g >> 27)) * 0x94D049BB133111EBULL;
  return g ^ (g >> 31);
}

std::string Digitless(std::string_view message) {
  std::string out;
  bool digits = false;
  for (const char c : message) {
    if (c >= '0' && c <= '9') {
      if (!digits) {
        out += '#';
      }
      digits = true;
      continue;
    }
    digits = false;
    out += c;
  }
  return out;
}

class ColCodecCorruptionTest : public TestBase {
 protected:
  duckdb::DatabaseInstance& Db() { return *_db.instance; }

  void Write(const std::filesystem::path& path, const Case& c, uint64_t& offset,
             uint64_t& size) {
    irs::MMapDirectory dir{path};
    {
      irs::ColWriter w{dir, kSeg, Db(), irs::WriteTier::Flush};
      auto& cw = w.OpenColumn(
        kField, c.type, {.row_group_size = kRgSize, .compression = c.codec});
      for (uint64_t pos = 0; pos < c.rows;) {
        const auto take =
          std::min<duckdb::idx_t>(c.rows - pos, STANDARD_VECTOR_SIZE);
        duckdb::Vector v{c.type, STANDARD_VECTOR_SIZE};
        for (duckdb::idx_t k = 0; k < take; ++k) {
          v.SetValue(k, c.value(pos + k));
        }
        duckdb::FlatVector::SetSize(v, take);
        cw.Append(v, take);
        pos += take;
      }
      ASSERT_TRUE(w.Commit(0));
    }
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    const irs::ColumnReader* target = col;
    if (c.validity) {
      target = col->Validity();
      ASSERT_NE(target, nullptr);
    }
    ASSERT_FALSE(target->DataBlocks().empty());
    const auto& block = target->DataBlocks().front();
    if (c.codec != CodecType::Auto && !c.validity) {
      ASSERT_EQ(irs::codecs::NameOf(block.codec->type),
                irs::codecs::NameOf(c.codec));
    }
    offset = block.file_offset;
    size = block.byte_size;
  }

  template<typename Stage>
  void Guarded(std::set<std::string>& messages, Stage&& stage) {
    try {
      stage();
    } catch (const irs::SqlException& e) {
      EXPECT_EQ(e.error().errcode, ERRCODE_INDEX_CORRUPTED) << e.what();
      messages.insert(Digitless(e.message()));
    }
  }

  void Read(const std::filesystem::path& path, const Case& c,
            const Value& probe, std::set<std::string>& messages) {
    irs::MMapDirectory dir{path};
    irs::ColReader r{dir, std::string{kSeg}, Db(), irs::IOAdvice::SEQUENTIAL};
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    Guarded(messages, [&] {
      auto state = col->InitScan(r.Ctx());
      for (uint64_t pos = 0; pos < c.rows;) {
        const auto take =
          std::min<duckdb::idx_t>(c.rows - pos, STANDARD_VECTOR_SIZE);
        duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
        col->Scan(state, out, take);
        pos += take;
      }
    });
    Guarded(messages, [&] {
      auto state = col->InitScan(r.Ctx());
      col->Skip(state, 700);
      duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
      duckdb::FlatVector::ValidityMutable(out).Reset(STANDARD_VECTOR_SIZE);
      col->ScanCount(state, out, 900, 3);
    });
    Guarded(messages, [&] {
      std::vector<uint64_t> picked;
      for (uint64_t g = 3; g < c.rows; g += 41) {
        picked.push_back(g);
      }
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
      duckdb::FlatVector::ValidityMutable(out).Reset(STANDARD_VECTOR_SIZE);
      const Rows sub{picked.data(), picked.size()};
      irs::column_internal::GatherRows(*col, state, sub, out, 0,
                                       /*whole_output=*/true);
    });
    Guarded(messages, [&] {
      irs::ColumnReader::PointReader reader{r, *col};
      duckdb::Vector out{c.type, 1};
      for (const uint64_t g : {uint64_t{0}, uint64_t{1}, uint64_t{777},
                               uint64_t{2050}, c.rows - 1, uint64_t{5}}) {
        duckdb::FlatVector::ValidityMutable(out).Reset();
        reader.FetchRow(g % c.rows, out, 0);
      }
    });
    Guarded(messages, [&] {
      irs::ColumnReader::PointReader reader{r, *col};
      std::vector<uint32_t> batch;
      for (uint32_t g = 2; g < c.rows; g += 97) {
        batch.push_back(g);
      }
      duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
      reader.FetchRows(batch, 0, out, 0);
    });
    Guarded(messages, [&] {
      duckdb::Connection con{_db};
      const duckdb::ExpressionFilter filter{
        duckdb::BoundComparisonExpression::Create(
          duckdb::ExpressionType::COMPARE_EQUAL,
          duckdb::make_uniq<duckdb::BoundReferenceExpression>(c.type, 0),
          duckdb::make_uniq<duckdb::BoundConstantExpression>(probe))};
      auto filter_state =
        duckdb::TableFilterState::Initialize(*con.context, filter);
      auto state = col->InitScan(r.Ctx());
      for (uint64_t anchor = 0; anchor < c.rows;
           anchor += STANDARD_VECTOR_SIZE) {
        const auto span =
          std::min<uint64_t>(c.rows - anchor, STANDARD_VECTOR_SIZE);
        duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
        for (duckdb::idx_t i = 0; i < span; ++i) {
          sel.set_index(i, i);
        }
        duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
        col->GatherFilter(state, anchor, span, sel, span, filter, *filter_state,
                          irs::NullCheckKind::None, out);
      }
    });
    Guarded(messages, [&] {
      const auto& described = c.validity ? *col->Validity() : *col;
      irs::ReadContext ctx{r};
      const auto segment = described.OpenSegment(0, ctx);
      if (segment->function.get_segment_info) {
        segment->function.get_segment_info(*segment);
      }
    });
  }

  void Sweep(const Case& c) {
    SCOPED_TRACE(c.name);
    const auto path =
      test_dir() / absl::StrCat("col_codec_corruption_", _next++);
    std::filesystem::create_directories(path);
    uint64_t offset = 0;
    uint64_t size = 0;
    ASSERT_NO_FATAL_FAILURE(Write(path, c, offset, size));
    ASSERT_NE(size, 0);
    Value probe{c.type};
    for (uint64_t g = 0; g < c.rows && probe.IsNull(); ++g) {
      probe = c.value(g);
    }
    const auto file = path / irs::FileName(kSeg);
    std::fstream io{file, std::ios::in | std::ios::out | std::ios::binary};
    ASSERT_TRUE(io.good());
    std::vector<uint64_t> spots;
    for (uint64_t at = 0; at < size; ++at) {
      if (at < 96 || at + 160 >= size || at % 61 == 0) {
        spots.push_back(at);
      }
    }
    std::set<std::string> messages;
    for (const auto at : spots) {
      io.seekg(static_cast<std::streamoff>(offset + at));
      char original = 0;
      io.read(&original, 1);
      for (const char damaged :
           {static_cast<char>(0xFF), static_cast<char>(0x00),
            static_cast<char>(original ^ 0x01)}) {
        if (damaged == original) {
          continue;
        }
        io.seekp(static_cast<std::streamoff>(offset + at));
        io.write(&damaged, 1);
        io.flush();
        ASSERT_NO_FATAL_FAILURE(Read(path, c, probe, messages));
      }
      io.seekp(static_cast<std::streamoff>(offset + at));
      io.write(&original, 1);
      io.flush();
    }
    std::set<std::string> clean;
    ASSERT_NO_FATAL_FAILURE(Read(path, c, probe, clean));
    EXPECT_TRUE(clean.empty()) << absl::StrJoin(clean, "; ");
    EXPECT_EQ(absl::StrJoin(messages, "\n"), absl::StrJoin(c.rejected, "\n"));
  }

  duckdb::DuckDB _db;
  size_t _next = 0;
};

Value Null(const LogicalType& type) { return Value{type}; }

TEST_F(ColCodecCorruptionTest, BitpackingRejectsDamagedSegments) {
  Sweep(
    {"for",
     LogicalType::BIGINT,
     CodecType::Bitpacking,
     [](uint64_t g) {
       return Value::BIGINT(static_cast<int64_t>((g * 7919) % 1000) - 500);
     },
     {"Corrupted bitpacking group data: attempted to read past the end of the "
      "segment",
      "Corrupted bitpacking segment: array offset is misaligned",
      "Corrupted bitpacking segment: bit width exceeds the physical type width",
      "Corrupted bitpacking segment: group offsets do not describe a range in "
      "the data region",
      "Corrupted bitpacking segment: offset is outside the segment",
      "Corrupted bitpacking segment: unknown bitpacking mode"}});
  Sweep(
    {"delta for",
     LogicalType::BIGINT,
     CodecType::Bitpacking,
     [](uint64_t g) {
       return Value::BIGINT(static_cast<int64_t>(g * 3 + (g * 7919) % 5));
     },
     {"Corrupted bitpacking group data: attempted to read past the end of the "
      "segment",
      "Corrupted bitpacking segment: array offset is misaligned",
      "Corrupted bitpacking segment: bit width exceeds the physical type width",
      "Corrupted bitpacking segment: group offsets do not describe a range in "
      "the data region",
      "Corrupted bitpacking segment: offset is outside the segment",
      "Corrupted bitpacking segment: unknown bitpacking mode"}});
  Sweep({"constant delta",
         LogicalType::INTEGER,
         CodecType::Bitpacking,
         [](uint64_t g) {
           return Value::INTEGER(1000 + static_cast<int32_t>(g) * 5);
         },
         {"Corrupted bitpacking group data: attempted to read past the end of "
          "the segment",
          "Corrupted bitpacking segment: array offset is misaligned",
          "Corrupted bitpacking segment: group offsets do not describe a range "
          "in the data region",
          "Corrupted bitpacking segment: offset is outside the segment",
          "Corrupted bitpacking segment: unknown bitpacking mode"}});
  Sweep(
    {"constant",
     LogicalType::SMALLINT,
     CodecType::Bitpacking,
     [](uint64_t g) {
       return g < 2048 ? Value::SMALLINT(7)
                       : Value::SMALLINT(static_cast<int16_t>(g % 50));
     },
     {"Corrupted bitpacking group data: attempted to read past the end of the "
      "segment",
      "Corrupted bitpacking segment: array offset is misaligned",
      "Corrupted bitpacking segment: bit width exceeds the physical type width",
      "Corrupted bitpacking segment: group offsets do not describe a range in "
      "the data region",
      "Corrupted bitpacking segment: offset is outside the segment",
      "Corrupted bitpacking segment: unknown bitpacking mode"}});
  Sweep({"hugeint",
         LogicalType::HUGEINT,
         CodecType::Bitpacking,
         [](uint64_t g) {
           return Value::HUGEINT(
             duckdb::hugeint_t{static_cast<int64_t>((g * 31) % 100000)});
         },
         {"Corrupted bitpacking group data: attempted to read past the end of "
          "the segment",
          "Corrupted bitpacking segment: array offset is misaligned",
          "Corrupted bitpacking segment: group offsets do not describe a range "
          "in the data region",
          "Corrupted bitpacking segment: offset is outside the segment",
          "Corrupted bitpacking segment: unknown bitpacking mode"}});
}

TEST_F(ColCodecCorruptionTest, SmallSegmentsRejectDamagedHeaders) {
  Sweep(
    {"bitpacking",
     LogicalType::BIGINT,
     CodecType::Bitpacking,
     [](uint64_t g) {
       return Value::BIGINT(static_cast<int64_t>((g * 7919) % 1000) - 500);
     },
     {"Corrupted bitpacking group data: attempted to read past the end of the "
      "segment",
      "Corrupted bitpacking segment: array offset is misaligned",
      "Corrupted bitpacking segment: bit width exceeds the physical type width",
      "Corrupted bitpacking segment: group offsets do not describe a range in "
      "the data region",
      "Corrupted bitpacking segment: offset is outside the segment",
      "Corrupted bitpacking segment: unknown bitpacking mode"},
     /*validity=*/false,
     /*rows=*/300});
  Sweep(
    {"rle",
     LogicalType::BIGINT,
     CodecType::Rle,
     [](uint64_t g) { return Value::BIGINT(static_cast<int64_t>(g / 7)); },
     {"Corrupted RLE segment: bitpacking width exceeds the value width",
      "Corrupted RLE segment: entry count exceeds the segment capacity",
      "Corrupted RLE segment: rle_count_offset is corrupted",
      "Corrupted RLE segment: run count # exceeds the remaining row count #",
      "Corrupted RLE segment: run counts do not cover the segment row count"},
     /*validity=*/false,
     /*rows=*/300});
  Sweep({"alp",
         LogicalType::DOUBLE,
         CodecType::Alp,
         [](uint64_t g) {
           return Value::DOUBLE(static_cast<double>((g * 7919) % 100000) / 100);
         },
         {"Corrupted ALP segment: bit width # exceeds #",
          "Corrupted ALP segment: exponent # exceeds the maximum #",
          "Corrupted ALP segment: requested range extends past the end of the "
          "segment",
          "Corrupted ALP segment: vector offset is outside the data region",
          "Corrupted ALP segment: vector offsets do not describe a data range"},
         /*validity=*/false,
         /*rows=*/300});
  Sweep(
    {"alprd",
     LogicalType::DOUBLE,
     CodecType::AlpRd,
     [](uint64_t g) {
       return Value::DOUBLE(std::sqrt(static_cast<double>(g + 2)));
     },
     {"Corrupted ALPRD segment: dictionary size exceeds maximum",
      "Corrupted ALPRD segment: exception count # exceeds vector size #",
      "Corrupted ALPRD segment: exception position # is outside vector size #",
      "Corrupted ALPRD segment: left bit width # exceeds #",
      "Corrupted ALPRD segment: requested range extends past the end of the "
      "segment",
      "Corrupted ALPRD segment: right bit width # exceeds #",
      "Corrupted ALPRD segment: vector offset is outside the data region",
      "Corrupted ALPRD segment: vector offsets do not describe a data range",
      "Corrupted ALPRD vector: attempted to read past the end of the segment"},
     /*validity=*/false,
     /*rows=*/300});
}

TEST_F(ColCodecCorruptionTest, RleRejectsDamagedSegments) {
  Sweep(
    {"bigint",
     LogicalType::BIGINT,
     CodecType::Rle,
     [](uint64_t g) {
       return Value::BIGINT(static_cast<int64_t>((g / 37) % 11));
     },
     {"Corrupted RLE segment: bitpacking width exceeds the value width",
      "Corrupted RLE segment: entry count exceeds the segment capacity",
      "Corrupted RLE segment: rle_count_offset is corrupted",
      "Corrupted RLE segment: run count # exceeds the remaining row count #",
      "Corrupted RLE segment: run counts do not cover the segment row count"}});
  Sweep(
    {"double",
     LogicalType::DOUBLE,
     CodecType::Rle,
     [](uint64_t g) {
       return Value::DOUBLE(static_cast<double>((g / 50) % 7) + 0.5);
     },
     {"Corrupted RLE segment: bitpacking width exceeds the value width",
      "Corrupted RLE segment: entry count exceeds the segment capacity",
      "Corrupted RLE segment: rle_count_offset is corrupted",
      "Corrupted RLE segment: run count # exceeds the remaining row count #",
      "Corrupted RLE segment: run counts do not cover the segment row count"}});
  Sweep(
    {"single runs",
     LogicalType::INTEGER,
     CodecType::Rle,
     [](uint64_t g) {
       return Value::INTEGER(g < 2000 ? static_cast<int32_t>(g / 500)
                                      : static_cast<int32_t>(g % 90));
     },
     {"Corrupted RLE segment: bitpacking width exceeds the value width",
      "Corrupted RLE segment: entry count exceeds the segment capacity",
      "Corrupted RLE segment: rle_count_offset is corrupted",
      "Corrupted RLE segment: run count # exceeds the remaining row count #",
      "Corrupted RLE segment: run counts do not cover the segment row count"}});
}

TEST_F(ColCodecCorruptionTest, AlpCodecsRejectDamagedSegments) {
  Sweep(
    {"alp double",
     LogicalType::DOUBLE,
     CodecType::Alp,
     [](uint64_t g) {
       return g % 101 == 0
                ? Value::DOUBLE(std::sqrt(static_cast<double>(g)) * 1.000001)
                : Value::DOUBLE(static_cast<double>(g % 10000) / 100.0);
     },
     {"Corrupted ALP segment: bit width # exceeds #",
      "Corrupted ALP segment: exception count # exceeds vector size #",
      "Corrupted ALP segment: exception position # is outside vector size #",
      "Corrupted ALP segment: exponent # exceeds the maximum #",
      "Corrupted ALP segment: factor # exceeds exponent #",
      "Corrupted ALP segment: metadata offset table exceeds the segment",
      "Corrupted ALP segment: requested range extends past the end of the "
      "segment",
      "Corrupted ALP segment: vector offset is outside the data region",
      "Corrupted ALP segment: vector offsets do not describe a data range",
      "Corrupted ALP vector: attempted to read past the end of the segment",
      "Corrupted ALP vector: offset is outside the segment"}});
  Sweep(
    {"alp float",
     LogicalType::FLOAT,
     CodecType::Alp,
     [](uint64_t g) {
       return g % 67 == 0
                ? Value::FLOAT(std::sqrt(static_cast<float>(g)) * 1.0001F)
                : Value::FLOAT(static_cast<float>(g % 1000) / 10.0F);
     },
     {"Corrupted ALP segment: bit width # exceeds #",
      "Corrupted ALP segment: exception count # exceeds vector size #",
      "Corrupted ALP segment: exception position # is outside vector size #",
      "Corrupted ALP segment: exponent # exceeds the maximum #",
      "Corrupted ALP segment: factor # exceeds exponent #",
      "Corrupted ALP segment: requested range extends past the end of the "
      "segment",
      "Corrupted ALP segment: vector offset is outside the data region",
      "Corrupted ALP segment: vector offsets do not describe a data range",
      "Corrupted ALP vector: attempted to read past the end of the segment",
      "Corrupted ALP vector: offset is outside the segment"}});
  Sweep(
    {"alprd double",
     LogicalType::DOUBLE,
     CodecType::AlpRd,
     [](uint64_t g) {
       return g % 97 == 0 ? Value::DOUBLE(-1e300 * static_cast<double>(g + 1))
                          : Value::DOUBLE(std::sin(static_cast<double>(g)));
     },
     {"Corrupted ALPRD segment: dictionary size exceeds maximum",
      "Corrupted ALPRD segment: exception count # exceeds vector size #",
      "Corrupted ALPRD segment: exception position # is outside vector size #",
      "Corrupted ALPRD segment: left bit width # exceeds #",
      "Corrupted ALPRD segment: requested range extends past the end of the "
      "segment",
      "Corrupted ALPRD segment: right bit width # exceeds #",
      "Corrupted ALPRD segment: vector offset is outside the data region",
      "Corrupted ALPRD segment: vector offsets do not describe a data range",
      "Corrupted ALPRD vector: attempted to read past the end of the "
      "segment"}});
  Sweep(
    {"alprd float",
     LogicalType::FLOAT,
     CodecType::AlpRd,
     [](uint64_t g) {
       return g % 89 == 0
                ? Value::FLOAT(-1e30F * static_cast<float>(g + 1))
                : Value::FLOAT(std::sin(static_cast<float>(g)) * 123.456F);
     },
     {"Corrupted ALPRD segment: dictionary size exceeds maximum",
      "Corrupted ALPRD segment: exception count # exceeds vector size #",
      "Corrupted ALPRD segment: exception position # is outside vector size #",
      "Corrupted ALPRD segment: left bit width # exceeds #",
      "Corrupted ALPRD segment: requested range extends past the end of the "
      "segment",
      "Corrupted ALPRD segment: right bit width # exceeds #",
      "Corrupted ALPRD segment: vector offset is outside the data region",
      "Corrupted ALPRD segment: vector offsets do not describe a data range",
      "Corrupted ALPRD vector: attempted to read past the end of the "
      "segment"}});
}

TEST_F(ColCodecCorruptionTest, ValidityCodecsRejectDamagedSegments) {
  const auto ints = [](uint64_t g) {
    return Value::BIGINT(static_cast<int64_t>(g % 777));
  };
  Sweep(
    {"sparse nulls",
     LogicalType::BIGINT,
     CodecType::Bitpacking,
     [&](uint64_t g) {
       return g % 97 == 0 ? Null(LogicalType::BIGINT) : ints(g);
     },
     {"Corrupted roaring segment: container data extends into the metadata",
      "Corrupted roaring segment: container metadata extends past the segment",
      "Corrupted roaring segment: metadata offset exceeds the segment"},
     /*validity=*/true});
  Sweep({"null runs",
         LogicalType::BIGINT,
         CodecType::Bitpacking,
         [&](uint64_t g) {
           return (g / 50) % 4 == 0 ? Null(LogicalType::BIGINT) : ints(g);
         },
         {"Corrupted roaring segment: container data extends into the metadata",
          "Corrupted roaring segment: metadata offset exceeds the segment",
          "Corrupted roaring segment: run ends before it starts",
          "Corrupted roaring segment: runs overlap or are out of order"},
         /*validity=*/true});
  Sweep({"dense nulls",
         LogicalType::BIGINT,
         CodecType::Bitpacking,
         [&](uint64_t g) {
           return Mix(g) % 3 == 0 ? Null(LogicalType::BIGINT) : ints(g);
         },
         {},
         /*validity=*/true});
  Sweep(
    {"mostly null",
     LogicalType::BIGINT,
     CodecType::Bitpacking,
     [&](uint64_t g) {
       return g % 50 == 0 ? ints(g) : Null(LogicalType::BIGINT);
     },
     {"Corrupted roaring segment: container data extends into the metadata",
      "Corrupted roaring segment: container metadata extends past the segment",
      "Corrupted roaring segment: metadata offset exceeds the segment"},
     /*validity=*/true});
  Sweep(
    {"roaring booleans",
     LogicalType::BOOLEAN,
     CodecType::Roaring,
     [](uint64_t g) { return Value::BOOLEAN(g % 97 == 0); },
     {"Corrupted roaring segment: container data extends into the metadata",
      "Corrupted roaring segment: container metadata extends past the segment",
      "Corrupted roaring segment: metadata offset exceeds the segment"}});
  Sweep({"roaring boolean runs",
         LogicalType::BOOLEAN,
         CodecType::Roaring,
         [](uint64_t g) { return Value::BOOLEAN((g / 100) % 2 == 0); },
         {"Corrupted roaring segment: container data extends into the metadata",
          "Corrupted roaring segment: metadata offset exceeds the segment",
          "Corrupted roaring segment: run ends before it starts",
          "Corrupted roaring segment: runs overlap or are out of order"}});
}

TEST_F(ColCodecCorruptionTest, UncompressedRejectsDamagedSegments) {
  Sweep({"bigint",
         LogicalType::BIGINT,
         CodecType::Uncompressed,
         [](uint64_t g) { return Value::BIGINT(static_cast<int64_t>(Mix(g))); },
         {}});
  Sweep({"short strings",
         LogicalType::VARCHAR,
         CodecType::Uncompressed,
         [](uint64_t g) { return Value{absl::StrCat("row-", g % 97)}; },
         {"Corrupted uncompressed string segment: dictionary is outside the "
          "segment or overlaps offsets",
          "Corrupted uncompressed string segment: dictionary offsets are "
          "invalid"}});
  const std::string long_text(5000, 'q');
  Sweep(
    {"overflow strings",
     LogicalType::VARCHAR,
     CodecType::Uncompressed,
     [&](uint64_t g) {
       return g % 400 == 7 ? Value{absl::StrCat("row-", g, "-", long_text)}
                           : Value{absl::StrCat("row-", g)};
     },
     {"Corrupted uncompressed string segment: dictionary is outside the "
      "segment or overlaps offsets",
      "Corrupted uncompressed string segment: dictionary offsets are invalid",
      "Corrupted uncompressed string segment: invalid overflow string marker",
      "Corrupted uncompressed string segment: overflow string is outside the "
      "file",
      "Corrupted uncompressed string segment: overflow string runs past the "
      "end of the file"}});
}

}  // namespace
