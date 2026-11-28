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
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <duckdb.hpp>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iresearch/formats/column/codecs/frame_meta.hpp>
#include <iresearch/formats/column/codecs/registry.hpp>
#include <iresearch/formats/column/codecs/text/string_layout.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/internal/gather_arms.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <limits>
#include <magic_enum/magic_enum.hpp>
#include <set>
#include <string>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "gtest/gtest.h"
#include "index_dir.hpp"
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
    tests::IndexDir dir{Db(), path.string()};
    {
      irs::ColWriter w{Db(), dir.Tracker(kSeg), irs::WriteTier::Flush};
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
    _sizes = dir.Sizes(kSeg, irs::kColFile);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
      ASSERT_EQ(magic_enum::enum_name(block.codec->type),
                magic_enum::enum_name(c.codec));
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
    tests::IndexDir dir{Db(), path.string()};
    dir.Adopt(kSeg, irs::kColFile, _sizes);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
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
    if (c.type.InternalType() != duckdb::PhysicalType::VARCHAR) {
      return;
    }
    Guarded(messages, [&] {
      irs::ColumnReader::PointReader reader{r, *col};
      duckdb::Vector out{c.type, 1};
      for (uint64_t g = 0; g < c.rows; ++g) {
        if (g >= 128 && g % 16 != 15 && g + 1 != c.rows) {
          continue;
        }
        duckdb::FlatVector::ValidityMutable(out).Reset();
        reader.FetchRow(g, out, 0);
      }
    });
    Guarded(messages, [&] {
      irs::ColumnReader::PointReader reader{r, *col};
      duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
      for (uint32_t g = 0; g + 3 < c.rows; g += 37) {
        const std::vector<uint32_t> batch{g, g + 1, g + 3};
        reader.FetchRows(batch, 0, out, 0);
      }
    });
    Guarded(messages, [&] {
      auto state = col->InitScan(r.Ctx());
      for (uint64_t anchor = 0; anchor < c.rows;
           anchor += STANDARD_VECTOR_SIZE) {
        const auto span =
          std::min<uint64_t>(c.rows - anchor, STANDARD_VECTOR_SIZE);
        const auto hits = std::min<duckdb::idx_t>(span, 70);
        duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
        for (duckdb::idx_t i = 0; i < hits; ++i) {
          sel.set_index(i, i);
        }
        duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
        col->GatherDense(state, anchor, sel, hits, span, out);
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
  irs::FileSizes _sizes;
};

Value Null(const LogicalType& type) { return Value{type}; }

TEST_F(ColCodecCorruptionTest, BitpackingRejectsDamagedSegments) {
  Sweep({"for",
         LogicalType::BIGINT,
         CodecType::Bitpacking,
         [](uint64_t g) {
           return Value::BIGINT(static_cast<int64_t>((g * 7919) % 1000) - 500);
         },
         {"Corrupted numeric segment: blocks are inconsistent",
          "Corrupted numeric segment: frame table is inconsistent",
          "Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #"}});
  Sweep({"delta for",
         LogicalType::BIGINT,
         CodecType::Bitpacking,
         [](uint64_t g) {
           return Value::BIGINT(static_cast<int64_t>(g * 3 + (g * 7919) % 5));
         },
         {"Corrupted numeric segment: blocks are inconsistent",
          "Corrupted numeric segment: frame table is inconsistent",
          "Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #"}});
  Sweep({"constant delta",
         LogicalType::INTEGER,
         CodecType::Bitpacking,
         [](uint64_t g) {
           return Value::INTEGER(1000 + static_cast<int32_t>(g) * 5);
         },
         {"Corrupted numeric segment: frame table is inconsistent",
          "Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #"}});
  Sweep({"constant",
         LogicalType::SMALLINT,
         CodecType::Bitpacking,
         [](uint64_t g) {
           return g < 2048 ? Value::SMALLINT(7)
                           : Value::SMALLINT(static_cast<int16_t>(g % 50));
         },
         {"Corrupted numeric segment: blocks are inconsistent",
          "Corrupted numeric segment: frame table is inconsistent",
          "Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #"}});
  Sweep({"hugeint",
         LogicalType::HUGEINT,
         CodecType::Bitpacking,
         [](uint64_t g) {
           return Value::HUGEINT(
             duckdb::hugeint_t{static_cast<int64_t>((g * 31) % 100000)});
         },
         {"Corrupted numeric segment: frame table is inconsistent",
          "Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #"}});
}

TEST_F(ColCodecCorruptionTest, SmallSegmentsRejectDamagedHeaders) {
  Sweep({"bitpacking",
         LogicalType::BIGINT,
         CodecType::Bitpacking,
         [](uint64_t g) {
           return Value::BIGINT(static_cast<int64_t>((g * 7919) % 1000) - 500);
         },
         {"Corrupted numeric segment: blocks are inconsistent",
          "Corrupted numeric segment: frame table is inconsistent",
          "Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #"},
         /*validity=*/false,
         /*rows=*/300});
  Sweep({"rle",
         LogicalType::BIGINT,
         CodecType::Rle,
         [](uint64_t g) { return Value::BIGINT(static_cast<int64_t>(g / 7)); },
         {"Corrupted numeric segment: blocks are inconsistent",
          "Corrupted numeric segment: frame table is inconsistent",
          "Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: runs are inconsistent",
          "Corrupted numeric segment: unsupported version #"},
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
          "Corrupted ALP segment: metadata offset table exceeds the segment",
          "Corrupted ALP segment: vector offsets do not describe a data range"},
         /*validity=*/false,
         /*rows=*/300});
  Sweep(
    {"alprd",
     LogicalType::DOUBLE,
     CodecType::Alprd,
     [](uint64_t g) {
       return Value::DOUBLE(std::sqrt(static_cast<double>(g + 2)));
     },
     {"Corrupted ALPRD segment: dictionary size exceeds maximum",
      "Corrupted ALPRD segment: exception count # exceeds vector size #",
      "Corrupted ALPRD segment: exception position # is outside vector size #",
      "Corrupted ALPRD segment: left bit width # exceeds #",
      "Corrupted ALPRD segment: metadata offset table exceeds the segment",
      "Corrupted ALPRD segment: right bit width # exceeds #",
      "Corrupted ALPRD segment: vector exceeds its data range",
      "Corrupted ALPRD segment: vector offsets do not describe a data range"},
     /*validity=*/false,
     /*rows=*/300});
  Sweep({"alp few values",
         LogicalType::DOUBLE,
         CodecType::Alp,
         [](uint64_t g) { return Value::DOUBLE(static_cast<double>(g) / 10); },
         {"Corrupted ALP segment: bit width # exceeds #",
          "Corrupted ALP segment: exception count # exceeds vector size #",
          "Corrupted ALP segment: factor # exceeds exponent #",
          "Corrupted ALP segment: metadata ends before the segment header",
          "Corrupted ALP segment: metadata offset table exceeds the segment",
          "Corrupted ALP segment: vector exceeds its data range",
          "Corrupted ALP segment: vector offsets do not describe a data range"},
         /*validity=*/false,
         /*rows=*/8});
  Sweep(
    {"alprd few values",
     LogicalType::DOUBLE,
     CodecType::Alprd,
     [](uint64_t g) {
       return Value::DOUBLE(std::sqrt(static_cast<double>(g + 2)));
     },
     {"Corrupted ALPRD segment: dictionary size exceeds maximum",
      "Corrupted ALPRD segment: exception count # exceeds vector size #",
      "Corrupted ALPRD segment: left bit width # exceeds #",
      "Corrupted ALPRD segment: metadata ends before the segment header",
      "Corrupted ALPRD segment: metadata offset table exceeds the segment",
      "Corrupted ALPRD segment: right bit width # exceeds #",
      "Corrupted ALPRD segment: vector offsets do not describe a data range"},
     /*validity=*/false,
     /*rows=*/8});
  Sweep(
    {"alp last vector of one value",
     LogicalType::DOUBLE,
     CodecType::Alp,
     [](uint64_t g) {
       return g == 100 || g == 200 || g == 300
                ? Value::DOUBLE(std::sqrt(static_cast<double>(g)) * 1.000001)
                : Value::DOUBLE(static_cast<double>(g % 1000) / 100.0);
     },
     {"Corrupted ALP segment: bit width # exceeds #",
      "Corrupted ALP segment: exception count # exceeds vector size #",
      "Corrupted ALP segment: exception position # is outside vector size #",
      "Corrupted ALP segment: exponent # exceeds the maximum #",
      "Corrupted ALP segment: factor # exceeds exponent #",
      "Corrupted ALP segment: metadata offset table exceeds the segment",
      "Corrupted ALP segment: vector exceeds its data range",
      "Corrupted ALP segment: vector offsets do not describe a data range"},
     /*validity=*/false,
     /*rows=*/1025});
  Sweep(
    {"alprd vectors stored as is",
     LogicalType::DOUBLE,
     CodecType::Alprd,
     [](uint64_t g) {
       auto bits = Mix(g);
       if ((bits >> 52 & 0x7FF) == 0x7FF) {
         bits ^= uint64_t{1} << 52;
       }
       return Value::DOUBLE(std::bit_cast<double>(bits));
     },
     {"Corrupted ALPRD segment: dictionary size exceeds maximum",
      "Corrupted ALPRD segment: exception count # exceeds vector size #",
      "Corrupted ALPRD segment: left bit width # exceeds #",
      "Corrupted ALPRD segment: metadata offset table exceeds the segment",
      "Corrupted ALPRD segment: right bit width # exceeds #",
      "Corrupted ALPRD segment: vector exceeds its data range",
      "Corrupted ALPRD segment: vector offsets do not describe a data range"}});
}

TEST_F(ColCodecCorruptionTest, RleRejectsDamagedSegments) {
  Sweep({"bigint",
         LogicalType::BIGINT,
         CodecType::Rle,
         [](uint64_t g) {
           return Value::BIGINT(static_cast<int64_t>((g / 37) % 11));
         },
         {"Corrupted numeric segment: blocks are inconsistent",
          "Corrupted numeric segment: frame table is inconsistent",
          "Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: runs are inconsistent",
          "Corrupted numeric segment: unsupported version #"}});
  Sweep({"double",
         LogicalType::DOUBLE,
         CodecType::Rle,
         [](uint64_t g) {
           return Value::DOUBLE(static_cast<double>((g / 50) % 7) + 0.5);
         },
         {"Corrupted numeric segment: blocks are inconsistent",
          "Corrupted numeric segment: frame table is inconsistent",
          "Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: runs are inconsistent",
          "Corrupted numeric segment: unsupported version #"}});
  Sweep({"single runs",
         LogicalType::INTEGER,
         CodecType::Rle,
         [](uint64_t g) {
           return Value::INTEGER(g < 2000 ? static_cast<int32_t>(g / 500)
                                          : static_cast<int32_t>(g % 90));
         },
         {"Corrupted numeric segment: blocks are inconsistent",
          "Corrupted numeric segment: frame table is inconsistent",
          "Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: runs are inconsistent",
          "Corrupted numeric segment: unsupported version #"}});
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
      "Corrupted ALP segment: vector exceeds its data range",
      "Corrupted ALP segment: vector offsets do not describe a data range"}});
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
      "Corrupted ALP segment: metadata offset table exceeds the segment",
      "Corrupted ALP segment: vector exceeds its data range",
      "Corrupted ALP segment: vector offsets do not describe a data range"}});
  Sweep(
    {"alprd double",
     LogicalType::DOUBLE,
     CodecType::Alprd,
     [](uint64_t g) {
       return g % 97 == 0 ? Value::DOUBLE(-1e300 * static_cast<double>(g + 1))
                          : Value::DOUBLE(std::sin(static_cast<double>(g)));
     },
     {"Corrupted ALPRD segment: dictionary size exceeds maximum",
      "Corrupted ALPRD segment: exception count # exceeds vector size #",
      "Corrupted ALPRD segment: exception position # is outside vector size #",
      "Corrupted ALPRD segment: left bit width # exceeds #",
      "Corrupted ALPRD segment: metadata offset table exceeds the segment",
      "Corrupted ALPRD segment: right bit width # exceeds #",
      "Corrupted ALPRD segment: vector exceeds its data range",
      "Corrupted ALPRD segment: vector offsets do not describe a data range"}});
  Sweep(
    {"alprd float",
     LogicalType::FLOAT,
     CodecType::Alprd,
     [](uint64_t g) {
       return g % 89 == 0
                ? Value::FLOAT(-1e30F * static_cast<float>(g + 1))
                : Value::FLOAT(std::sin(static_cast<float>(g)) * 123.456F);
     },
     {"Corrupted ALPRD segment: dictionary size exceeds maximum",
      "Corrupted ALPRD segment: exception count # exceeds vector size #",
      "Corrupted ALPRD segment: exception position # is outside vector size #",
      "Corrupted ALPRD segment: left bit width # exceeds #",
      "Corrupted ALPRD segment: metadata offset table exceeds the segment",
      "Corrupted ALPRD segment: right bit width # exceeds #",
      "Corrupted ALPRD segment: vector exceeds its data range",
      "Corrupted ALPRD segment: vector offsets do not describe a data range"}});
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

TEST_F(ColCodecCorruptionTest,
       RoaringRejectsMoreContainersThanItsMetadataHolds) {
  const Case c{"roaring booleans",
               LogicalType::BOOLEAN,
               CodecType::Roaring,
               [](uint64_t g) { return Value::BOOLEAN(g % 97 == 0); },
               {}};
  const auto path = test_dir() / "col_codec_corruption_roaring_metadata";
  std::filesystem::create_directories(path);
  uint64_t offset = 0;
  uint64_t size = 0;
  ASSERT_NO_FATAL_FAILURE(Write(path, c, offset, size));
  ASSERT_GT(c.rows, uint64_t{2048});
  ASSERT_GT(size, uint64_t{16});
  const uint64_t metadata_offset = size - sizeof(uint64_t) - 1;
  {
    std::fstream io{path / irs::FileName(kSeg),
                    std::ios::in | std::ios::out | std::ios::binary};
    ASSERT_TRUE(io.good());
    io.seekp(static_cast<std::streamoff>(offset));
    io.write(reinterpret_cast<const char*>(&metadata_offset),
             sizeof(metadata_offset));
  }
  std::set<std::string> messages;
  ASSERT_NO_FATAL_FAILURE(Read(path, c, Value::BOOLEAN(true), messages));
  EXPECT_EQ(absl::StrJoin(messages, "\n"),
            "Corrupted roaring segment: container count exceeds available "
            "metadata space");
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

TEST_F(ColCodecCorruptionTest,
       NumericCodecsWithByteLeavesRejectDamagedSegments) {
  Sweep({"lz4",
         LogicalType::BIGINT,
         CodecType::Lz4,
         [](uint64_t g) {
           return Value::BIGINT(static_cast<int64_t>(g % 16) * 1000);
         },
         {"Corrupted numeric segment: frame is inconsistent",
          "Corrupted numeric segment: frame table is inconsistent",
          "Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #"}});
  Sweep({"zstd",
         LogicalType::INTEGER,
         CodecType::Zstd,
         [](uint64_t g) {
           return Value::INTEGER(static_cast<int32_t>(g % 64) * 3);
         },
         {"Corrupted numeric segment: frame is inconsistent",
          "Corrupted numeric segment: frame table is inconsistent",
          "Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #"}});
  Sweep({"delta lz4",
         LogicalType::BIGINT,
         CodecType::DeltaLz4,
         [](uint64_t g) {
           return Value::BIGINT(static_cast<int64_t>(g * 5 + g % 3));
         },
         {"Corrupted numeric segment: frame is inconsistent",
          "Corrupted numeric segment: frame table is inconsistent",
          "Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #"}});
  Sweep({"dict zstd",
         LogicalType::BIGINT,
         CodecType::DictZstd,
         [](uint64_t g) {
           return Value::BIGINT(static_cast<int64_t>(Mix(g % 40) % 100000));
         },
         {"Corrupted numeric segment: dictionary code is out of range",
          "Corrupted numeric segment: frame is inconsistent",
          "Corrupted numeric segment: frame table is inconsistent",
          "Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #"}});
  Sweep({"rle lz4",
         LogicalType::INTEGER,
         CodecType::RleLz4,
         [](uint64_t g) {
           return Value::INTEGER(static_cast<int32_t>((g / 9) % 13));
         },
         {"Corrupted numeric segment: frame is inconsistent",
          "Corrupted numeric segment: frame table is inconsistent",
          "Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: runs are inconsistent",
          "Corrupted numeric segment: unsupported version #"}});
  Sweep({"rle zstd",
         LogicalType::SMALLINT,
         CodecType::RleZstd,
         [](uint64_t g) {
           return Value::SMALLINT(static_cast<int16_t>((g / 5) % 7));
         },
         {"Corrupted numeric segment: frame is inconsistent",
          "Corrupted numeric segment: frame table is inconsistent",
          "Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: runs are inconsistent",
          "Corrupted numeric segment: unsupported version #"}});
  Sweep({"hugeint halves",
         LogicalType::HUGEINT,
         CodecType::Bitpacking,
         [](uint64_t g) {
           return Value::HUGEINT(
             duckdb::hugeint_t{static_cast<int64_t>(g % 4) << 20, Mix(g)});
         },
         {"Corrupted numeric segment: blocks are inconsistent",
          "Corrupted numeric segment: frame table is inconsistent",
          "Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #"}});
}

Value TextRow(uint64_t g) {
  return g % 13 == 0
           ? Null(LogicalType::VARCHAR)
           : Value{absl::StrCat("tenant-", Mix(g) % 100000, "/item-", g % 7)};
}

Value RepeatedText(uint64_t g) {
  return g % 13 == 0 ? Null(LogicalType::VARCHAR)
                     : Value{absl::StrCat("status-", (g * 7919) % 300,
                                          "/a longer suffix")};
}

Value TextWithoutNulls(uint64_t g) {
  return Value{absl::StrCat("status-", (g * 7919) % 200, "/a longer suffix")};
}

TEST_F(ColCodecCorruptionTest,
       TextCodecsWithSymbolTablesRejectDamagedSegments) {
  Sweep({"fsst",
         LogicalType::VARCHAR,
         CodecType::Fsst,
         TextRow,
         {"Corrupted text segment: entry is inconsistent",
          "Corrupted text segment: frame is inconsistent",
          "Corrupted text segment: frame table is inconsistent",
          "Corrupted text segment: front coding is inconsistent",
          "Corrupted text segment: header is inconsistent",
          "Corrupted text segment: symbol table is inconsistent",
          "Corrupted text segment: unsupported version #"}});
  Sweep({"fsst12",
         LogicalType::VARCHAR,
         CodecType::Fsst12,
         TextRow,
         {"Corrupted text segment: entry is inconsistent",
          "Corrupted text segment: frame is inconsistent",
          "Corrupted text segment: frame table is inconsistent",
          "Corrupted text segment: front coding is inconsistent",
          "Corrupted text segment: header is inconsistent",
          "Corrupted text segment: unsupported version #"}});
  Sweep({"fsst plus",
         LogicalType::VARCHAR,
         CodecType::FsstPlus,
         TextRow,
         {"Corrupted text segment: entry is inconsistent",
          "Corrupted text segment: frame is inconsistent",
          "Corrupted text segment: frame table is inconsistent",
          "Corrupted text segment: front coding is inconsistent",
          "Corrupted text segment: header is inconsistent",
          "Corrupted text segment: prefix is inconsistent",
          "Corrupted text segment: symbol table is inconsistent",
          "Corrupted text segment: unsupported version #"}});
  Sweep({"onpair",
         LogicalType::VARCHAR,
         CodecType::Onpair,
         TextRow,
         {"Corrupted text segment: entry is inconsistent",
          "Corrupted text segment: frame is inconsistent",
          "Corrupted text segment: frame table is inconsistent",
          "Corrupted text segment: front coding is inconsistent",
          "Corrupted text segment: header is inconsistent",
          "Corrupted text segment: symbol table is inconsistent",
          "Corrupted text segment: unsupported version #"}});
}

TEST_F(ColCodecCorruptionTest, TextCodecsWithByteLeavesRejectDamagedSegments) {
  Sweep({"lz4",
         LogicalType::VARCHAR,
         CodecType::Lz4,
         TextRow,
         {"Corrupted text segment: entry lengths are inconsistent",
          "Corrupted text segment: frame is inconsistent",
          "Corrupted text segment: frame table is inconsistent",
          "Corrupted text segment: header is inconsistent",
          "Corrupted text segment: unsupported version #"}});
  Sweep({"zstd",
         LogicalType::VARCHAR,
         CodecType::Zstd,
         TextRow,
         {"Corrupted text segment: entry lengths are inconsistent",
          "Corrupted text segment: frame is inconsistent",
          "Corrupted text segment: frame table is inconsistent",
          "Corrupted text segment: header is inconsistent",
          "Corrupted text segment: unsupported version #"}});
  Sweep({"zxc",
         LogicalType::VARCHAR,
         CodecType::Zxc,
         TextRow,
         {"Corrupted text segment: entry lengths are inconsistent",
          "Corrupted text segment: frame is inconsistent",
          "Corrupted text segment: frame table is inconsistent",
          "Corrupted text segment: header is inconsistent",
          "Corrupted text segment: unsupported version #"}});
}

void RewritePacked(const std::filesystem::path& file, uint64_t offset,
                   uint64_t size, bool lengths,
                   const std::function<void(std::vector<uint32_t>&)>& change) {
  std::fstream io{file, std::ios::in | std::ios::out | std::ios::binary};
  ASSERT_TRUE(io.good());
  std::string bytes(size, '\0');
  io.seekg(static_cast<std::streamoff>(offset));
  io.read(bytes.data(), static_cast<std::streamsize>(size));
  irs::codecs::Header h;
  ASSERT_GE(size, sizeof(h));
  std::memcpy(&h, bytes.data(), sizeof(h));
  const uint64_t at = lengths ? h.off_lengths : h.off_lcps;
  const auto width = lengths ? h.length_width : h.lcp_width;
  ASSERT_NE(width, 0);
  const uint64_t groups =
    (uint64_t{h.entry_count} + irs::codecs::kGroup - 1) / irs::codecs::kGroup;
  ASSERT_LE(at + groups * irs::codecs::kGroup * width / 8, size);
  std::vector<uint32_t> values(groups * irs::codecs::kGroup);
  auto* packed = reinterpret_cast<duckdb::data_ptr_t>(bytes.data()) + at;
  for (uint64_t g = 0; g < groups; ++g) {
    duckdb::BitpackingPrimitives::UnPackBlock<uint32_t>(
      reinterpret_cast<duckdb::data_ptr_t>(values.data() +
                                           g * irs::codecs::kGroup),
      packed + g * irs::codecs::kGroup * width / 8, width);
  }
  values.resize(h.entry_count);
  change(values);
  for (const auto v : values) {
    ASSERT_LT(uint64_t{v}, uint64_t{1} << width);
  }
  values.resize(groups * irs::codecs::kGroup);
  for (uint64_t g = 0; g < groups; ++g) {
    duckdb::BitpackingPrimitives::PackBlock<uint32_t>(
      packed + g * irs::codecs::kGroup * width / 8,
      values.data() + g * irs::codecs::kGroup, width);
  }
  io.seekp(static_cast<std::streamoff>(offset));
  io.write(bytes.data(), static_cast<std::streamsize>(size));
}

TEST_F(ColCodecCorruptionTest, TextSegmentsRejectALargerFieldAtTheirLastEntry) {
  const auto damaged =
    [&](const Case& c, bool lengths,
        const std::function<void(std::vector<uint32_t>&)>& change,
        const std::function<void(irs::ColReader&, const irs::ColumnReader&)>&
          read,
        std::string_view rejected) {
      SCOPED_TRACE(c.name);
      const auto path =
        test_dir() / absl::StrCat("col_codec_corruption_field_", _next++);
      std::filesystem::create_directories(path);
      uint64_t offset = 0;
      uint64_t size = 0;
      ASSERT_NO_FATAL_FAILURE(Write(path, c, offset, size));
      ASSERT_NO_FATAL_FAILURE(RewritePacked(path / irs::FileName(kSeg), offset,
                                            size, lengths, change));
      tests::IndexDir dir{Db(), path.string()};
      dir.Adopt(kSeg, irs::kColFile, _sizes);
      irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
      const auto* col = r.Column(kField);
      ASSERT_NE(col, nullptr);
      std::set<std::string> messages;
      Guarded(messages, [&] { read(r, *col); });
      EXPECT_EQ(absl::StrJoin(messages, "\n"), rejected);
    };
  damaged(
    {"an fsst length that runs past the data, read by rows of its group",
     LogicalType::VARCHAR,
     CodecType::Fsst,
     [](uint64_t g) {
       return Value{absl::StrCat("tenant-", Mix(g) % 100000, "/item-", g % 7)};
     },
     {}},
    /*lengths=*/true, [](std::vector<uint32_t>& lengths) { ++lengths.back(); },
    [](irs::ColReader& r, const irs::ColumnReader& col) {
      constexpr duckdb::idx_t kHits = 65;
      const uint64_t anchor =
        col.RowCount() / STANDARD_VECTOR_SIZE * uint64_t{STANDARD_VECTOR_SIZE};
      const auto span = static_cast<duckdb::idx_t>(col.RowCount() - anchor);
      ASSERT_GT(span, kHits);
      duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
      for (duckdb::idx_t i = 0; i < kHits; ++i) {
        sel.set_index(i, span - kHits + i);
      }
      auto state = col.InitScan(r.Ctx());
      duckdb::Vector out{LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      col.GatherDense(state, anchor, sel, kHits, span, out);
    },
    "Corrupted text segment: front coding is inconsistent");
  damaged(
    {"an fsst_plus prefix id longer than the rest of its frame",
     LogicalType::VARCHAR,
     CodecType::FsstPlus,
     [](uint64_t g) {
       return g + 1 == kRows
                ? Value{"x"}
                : Value{absl::StrCat(
                    "a prefix that every row but the last starts with/", g)};
     },
     {}},
    /*lengths=*/false,
    [](std::vector<uint32_t>& ids) {
      const auto max_id = *absl::c_max_element(ids);
      ASSERT_NE(max_id, 0);
      ids.back() = max_id;
    },
    [](irs::ColReader& r, const irs::ColumnReader& col) {
      auto state = col.InitScan(r.Ctx());
      for (uint64_t pos = 0; pos < col.RowCount();) {
        const auto take =
          std::min<duckdb::idx_t>(col.RowCount() - pos, STANDARD_VECTOR_SIZE);
        duckdb::Vector out{LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
        col.Scan(state, out, take);
        pos += take;
      }
    },
    "Corrupted text segment: frame is inconsistent");
}

TEST_F(ColCodecCorruptionTest, TextDictionariesRejectDamagedSegments) {
  Sweep({"dict lz4",
         LogicalType::VARCHAR,
         CodecType::DictLz4,
         RepeatedText,
         {"Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #",
          "Corrupted text segment: dictionary code is out of range",
          "Corrupted text segment: entry lengths are inconsistent",
          "Corrupted text segment: frame is inconsistent",
          "Corrupted text segment: frame table is inconsistent",
          "Corrupted text segment: header is inconsistent",
          "Corrupted text segment: unsupported version #"}});
  Sweep({"dict lz4 without nulls",
         LogicalType::VARCHAR,
         CodecType::DictLz4,
         TextWithoutNulls,
         {"Corrupted numeric segment: frame table is inconsistent",
          "Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #",
          "Corrupted text segment: dictionary code is out of range",
          "Corrupted text segment: entry lengths are inconsistent",
          "Corrupted text segment: frame is inconsistent",
          "Corrupted text segment: frame table is inconsistent",
          "Corrupted text segment: header is inconsistent",
          "Corrupted text segment: unsupported version #"}});
  Sweep({"dict zstd",
         LogicalType::VARCHAR,
         CodecType::DictZstd,
         RepeatedText,
         {"Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #",
          "Corrupted text segment: dictionary code is out of range",
          "Corrupted text segment: entry lengths are inconsistent",
          "Corrupted text segment: frame is inconsistent",
          "Corrupted text segment: frame table is inconsistent",
          "Corrupted text segment: header is inconsistent",
          "Corrupted text segment: unsupported version #"}});
  Sweep({"dict zxc",
         LogicalType::VARCHAR,
         CodecType::DictZxc,
         RepeatedText,
         {"Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #",
          "Corrupted text segment: dictionary code is out of range",
          "Corrupted text segment: entry lengths are inconsistent",
          "Corrupted text segment: frame is inconsistent",
          "Corrupted text segment: frame table is inconsistent",
          "Corrupted text segment: header is inconsistent",
          "Corrupted text segment: unsupported version #"}});
  Sweep({"dict fsst",
         LogicalType::VARCHAR,
         CodecType::DictFsst,
         RepeatedText,
         {"Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #",
          "Corrupted text segment: dictionary code is out of range",
          "Corrupted text segment: entry is inconsistent",
          "Corrupted text segment: frame is inconsistent",
          "Corrupted text segment: frame table is inconsistent",
          "Corrupted text segment: front coding is inconsistent",
          "Corrupted text segment: header is inconsistent",
          "Corrupted text segment: symbol table is inconsistent",
          "Corrupted text segment: unsupported version #"}});
  Sweep({"dict fsst12",
         LogicalType::VARCHAR,
         CodecType::DictFsst12,
         RepeatedText,
         {"Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #",
          "Corrupted text segment: dictionary code is out of range",
          "Corrupted text segment: entry is inconsistent",
          "Corrupted text segment: frame is inconsistent",
          "Corrupted text segment: frame table is inconsistent",
          "Corrupted text segment: front coding is inconsistent",
          "Corrupted text segment: header is inconsistent",
          "Corrupted text segment: unsupported version #"}});
  Sweep({"dict fsst plus",
         LogicalType::VARCHAR,
         CodecType::DictFsstPlus,
         RepeatedText,
         {"Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #",
          "Corrupted text segment: dictionary code is out of range",
          "Corrupted text segment: entry is inconsistent",
          "Corrupted text segment: frame is inconsistent",
          "Corrupted text segment: frame table is inconsistent",
          "Corrupted text segment: front coding is inconsistent",
          "Corrupted text segment: header is inconsistent",
          "Corrupted text segment: symbol table is inconsistent",
          "Corrupted text segment: unsupported version #"}});
  Sweep({"dict onpair",
         LogicalType::VARCHAR,
         CodecType::DictOnpair,
         RepeatedText,
         {"Corrupted numeric segment: header is inconsistent",
          "Corrupted numeric segment: unsupported version #",
          "Corrupted text segment: dictionary code is out of range",
          "Corrupted text segment: entry is inconsistent",
          "Corrupted text segment: frame is inconsistent",
          "Corrupted text segment: frame table is inconsistent",
          "Corrupted text segment: front coding is inconsistent",
          "Corrupted text segment: header is inconsistent",
          "Corrupted text segment: symbol table is inconsistent",
          "Corrupted text segment: unsupported version #"}});
}

TEST_F(ColCodecCorruptionTest,
       ADictionaryWithoutValuesIsRefusedUnlessItsBlockHasNulls) {
  const auto valueless = [&](const Case& c, std::string_view rejected) {
    SCOPED_TRACE(c.name);
    const auto path =
      test_dir() / absl::StrCat("col_codec_corruption_valueless_", _next++);
    std::filesystem::create_directories(path);
    uint64_t offset = 0;
    uint64_t size = 0;
    ASSERT_NO_FATAL_FAILURE(Write(path, c, offset, size));
    {
      std::fstream io{path / irs::FileName(kSeg),
                      std::ios::in | std::ios::out | std::ios::binary};
      ASSERT_TRUE(io.good());
      irs::codecs::Header h;
      ASSERT_GE(size, sizeof(h));
      io.seekg(static_cast<std::streamoff>(offset));
      io.read(reinterpret_cast<char*>(&h), sizeof(h));
      ASSERT_GT(h.entry_count, 1);
      h.entry_count = 1;
      h.frame_count = 0;
      h.raw_bytes = 0;
      io.seekp(static_cast<std::streamoff>(offset));
      io.write(reinterpret_cast<const char*>(&h), sizeof(h));
    }
    tests::IndexDir dir{Db(), path.string()};
    dir.Adopt(kSeg, irs::kColFile, _sizes);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    std::set<std::string> messages;
    Guarded(messages, [&] {
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
      col->Scan(state, out, STANDARD_VECTOR_SIZE);
    });
    Guarded(messages, [&] {
      irs::ColumnReader::PointReader reader{r, *col};
      duckdb::Vector out{c.type, 1};
      reader.FetchRow(1, out, 0);
    });
    EXPECT_EQ(absl::StrJoin(messages, "\n"), rejected);
  };
  valueless({"no nulls",
             LogicalType::VARCHAR,
             CodecType::DictLz4,
             TextWithoutNulls,
             {}},
            "Corrupted text segment: header is inconsistent");
  valueless(
    {"nulls", LogicalType::VARCHAR, CodecType::DictLz4, RepeatedText, {}},
    "Corrupted text segment: dictionary code is out of range");
}

TEST_F(ColCodecCorruptionTest,
       UncompressedStringsRejectAHeaderOrOffsetOutOfRange) {
  const Case c{"short strings",
               LogicalType::VARCHAR,
               CodecType::Uncompressed,
               [](uint64_t g) { return Value{absl::StrCat("row-", g % 97)}; },
               {}};
  const auto damaged = [&](std::string_view name, uint64_t at, uint32_t value,
                           uint64_t row, std::string_view rejected) {
    SCOPED_TRACE(name);
    const auto path =
      test_dir() / absl::StrCat("col_codec_corruption_strings_", _next++);
    std::filesystem::create_directories(path);
    uint64_t offset = 0;
    uint64_t size = 0;
    ASSERT_NO_FATAL_FAILURE(Write(path, c, offset, size));
    ASSERT_LT(at + sizeof(value), size);
    {
      std::fstream io{path / irs::FileName(kSeg),
                      std::ios::in | std::ios::out | std::ios::binary};
      ASSERT_TRUE(io.good());
      io.seekp(static_cast<std::streamoff>(offset + at));
      io.write(reinterpret_cast<const char*>(&value), sizeof(value));
    }
    tests::IndexDir dir{Db(), path.string()};
    dir.Adopt(kSeg, irs::kColFile, _sizes);
    irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    std::set<std::string> messages;
    Guarded(messages, [&] {
      irs::ColumnReader::PointReader reader{r, *col};
      duckdb::Vector out{c.type, 1};
      reader.FetchRow(row, out, 0);
    });
    EXPECT_EQ(absl::StrJoin(messages, "\n"), rejected);
  };
  damaged("a dictionary end inside the header", sizeof(uint32_t), 4, 5,
          "Corrupted uncompressed string segment: dictionary is outside the "
          "segment or overlaps offsets");
  damaged("an offset of INT32_MIN just before the fetched row",
          2 * sizeof(uint32_t) + 99 * sizeof(int32_t),
          static_cast<uint32_t>(std::numeric_limits<int32_t>::min()), 100,
          "Corrupted uncompressed string segment: dictionary offsets are "
          "invalid");
}

TEST_F(ColCodecCorruptionTest, OnpairFramesShorterThanTheirPaddingAreRefused) {
  const Case c{"onpair", LogicalType::VARCHAR, CodecType::Onpair, TextRow, {}};
  const auto path = test_dir() / "col_codec_corruption_onpair_padding";
  std::filesystem::create_directories(path);
  uint64_t offset = 0;
  uint64_t size = 0;
  ASSERT_NO_FATAL_FAILURE(Write(path, c, offset, size));
  {
    std::fstream io{path / irs::FileName(kSeg),
                    std::ios::in | std::ios::out | std::ios::binary};
    ASSERT_TRUE(io.good());
    irs::codecs::Header h;
    ASSERT_GE(size, sizeof(h));
    io.seekg(static_cast<std::streamoff>(offset));
    io.read(reinterpret_cast<char*>(&h), sizeof(h));
    ASSERT_NE(h.frame_count, 0);
    irs::codecs::FrameMeta frame;
    io.seekg(static_cast<std::streamoff>(offset + h.off_frames));
    io.read(reinterpret_cast<char*>(&frame), sizeof(frame));
    ASSERT_GT(frame.comp_len, 1);
    frame.comp_len = 1;
    io.seekp(static_cast<std::streamoff>(offset + h.off_frames));
    io.write(reinterpret_cast<const char*>(&frame), sizeof(frame));
  }
  tests::IndexDir dir{Db(), path.string()};
  dir.Adopt(kSeg, irs::kColFile, _sizes);
  irs::ColReader r = irs::tests::OpenColReader(dir, kSeg);
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  std::set<std::string> messages;
  Guarded(messages, [&] {
    auto state = col->InitScan(r.Ctx());
    duckdb::Vector out{c.type, STANDARD_VECTOR_SIZE};
    col->Scan(state, out, STANDARD_VECTOR_SIZE);
  });
  Guarded(messages, [&] {
    irs::ColumnReader::PointReader reader{r, *col};
    duckdb::Vector out{c.type, 1};
    reader.FetchRow(1, out, 0);
  });
  EXPECT_EQ(absl::StrJoin(messages, "\n"),
            "Corrupted text segment: entry is inconsistent");
}

TEST_F(ColCodecCorruptionTest, TextHeadersRejectAnImpossibleLocalDictionary) {
  const auto damaged =
    [&](const Case& c,
        const std::function<void(irs::codecs::Header&)>& change) {
      SCOPED_TRACE(c.name);
      const auto path =
        test_dir() / absl::StrCat("col_codec_corruption_header_", _next++);
      std::filesystem::create_directories(path);
      uint64_t offset = 0;
      uint64_t size = 0;
      ASSERT_NO_FATAL_FAILURE(Write(path, c, offset, size));
      {
        std::fstream io{path / irs::FileName(kSeg),
                        std::ios::in | std::ios::out | std::ios::binary};
        ASSERT_TRUE(io.good());
        irs::codecs::Header h;
        ASSERT_GE(size, sizeof(h));
        io.seekg(static_cast<std::streamoff>(offset));
        io.read(reinterpret_cast<char*>(&h), sizeof(h));
        change(h);
        io.seekp(static_cast<std::streamoff>(offset));
        io.write(reinterpret_cast<const char*>(&h), sizeof(h));
      }
      std::set<std::string> messages;
      ASSERT_NO_FATAL_FAILURE(Read(path, c, Value{"x"}, messages));
      EXPECT_EQ(absl::StrJoin(messages, "\n"),
                "Corrupted text segment: header is inconsistent");
    };
  const Case zstd{"zstd", LogicalType::VARCHAR, CodecType::Zstd, TextRow, {}};
  const Case fsst{"fsst", LogicalType::VARCHAR, CodecType::Fsst, TextRow, {}};
  using irs::codecs::FrameDictionary;
  damaged(zstd, [](irs::codecs::Header& h) {
    h.flags = FrameDictionary::Local;
    h.symtab_size = 0;
  });
  damaged(zstd, [](irs::codecs::Header& h) {
    h.flags = FrameDictionary::Local;
    h.symtab_size = 16;
    h.dictionary_size = 0;
  });
  damaged(zstd, [](irs::codecs::Header& h) {
    h.flags = FrameDictionary::Local;
    h.symtab_size = 16;
    h.dictionary_size = irs::codecs::kMaxDictionaryBytes + 1;
  });
  damaged(fsst, [](irs::codecs::Header& h) {
    ASSERT_NE(h.symtab_size, 0);
    h.flags = FrameDictionary::Local;
    h.dictionary_size = 1;
  });
}

TEST_F(ColCodecCorruptionTest, FsstPlusPrefixesWithoutASymbolTableAreRefused) {
  const Case c{
    "fsst_plus", LogicalType::VARCHAR, CodecType::FsstPlus, TextRow, {}};
  const auto path = test_dir() / "col_codec_corruption_fsst_plus_prefixes";
  std::filesystem::create_directories(path);
  uint64_t offset = 0;
  uint64_t size = 0;
  ASSERT_NO_FATAL_FAILURE(Write(path, c, offset, size));
  {
    std::fstream io{path / irs::FileName(kSeg),
                    std::ios::in | std::ios::out | std::ios::binary};
    ASSERT_TRUE(io.good());
    irs::codecs::Header h;
    ASSERT_GE(size, sizeof(h));
    io.seekg(static_cast<std::streamoff>(offset));
    io.read(reinterpret_cast<char*>(&h), sizeof(h));
    ASSERT_NE(h.dictionary_size, 0);
    ASSERT_GE(h.symtab_size, 2 * sizeof(uint32_t));
    h.dictionary_size = 0;
    io.seekp(static_cast<std::streamoff>(offset));
    io.write(reinterpret_cast<const char*>(&h), sizeof(h));
    const uint32_t prefixes = 1;
    io.seekp(static_cast<std::streamoff>(offset + h.off_symtab));
    io.write(reinterpret_cast<const char*>(&prefixes), sizeof(prefixes));
  }
  std::set<std::string> messages;
  ASSERT_NO_FATAL_FAILURE(Read(path, c, Value{"x"}, messages));
  EXPECT_EQ(absl::StrJoin(messages, "\n"),
            "Corrupted text segment: symbol table is inconsistent");
}

TEST_F(ColCodecCorruptionTest, TextSegmentsOfEntriesWithoutFramesAreRefused) {
  const auto empty = [](uint64_t g) {
    return g % 3 == 0 ? Null(LogicalType::VARCHAR) : Value{""};
  };
  for (const auto codec : {CodecType::Lz4, CodecType::DictLz4}) {
    const Case c{std::string{magic_enum::enum_name(codec)},
                 LogicalType::VARCHAR,
                 codec,
                 empty,
                 {}};
    SCOPED_TRACE(c.name);
    const auto path =
      test_dir() / absl::StrCat("col_codec_corruption_frameless_", _next++);
    std::filesystem::create_directories(path);
    uint64_t offset = 0;
    uint64_t size = 0;
    ASSERT_NO_FATAL_FAILURE(Write(path, c, offset, size));
    {
      std::fstream io{path / irs::FileName(kSeg),
                      std::ios::in | std::ios::out | std::ios::binary};
      ASSERT_TRUE(io.good());
      irs::codecs::Header h;
      ASSERT_GE(size, sizeof(h));
      io.seekg(static_cast<std::streamoff>(offset));
      io.read(reinterpret_cast<char*>(&h), sizeof(h));
      ASSERT_EQ(h.raw_bytes, 0);
      ASSERT_NE(h.frame_count, 0);
      h.frame_count = 0;
      io.seekp(static_cast<std::streamoff>(offset));
      io.write(reinterpret_cast<const char*>(&h), sizeof(h));
    }
    std::set<std::string> messages;
    ASSERT_NO_FATAL_FAILURE(Read(path, c, Value{""}, messages));
    EXPECT_EQ(absl::StrJoin(messages, "\n"),
              "Corrupted text segment: frame table is inconsistent");
  }
}

TEST_F(ColCodecCorruptionTest,
       OverflowStringsPastTheStartOfATinyFileAreRefused) {
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    auto out = dir.Create("tiny");
    const irs::byte_type bytes[] = {1, 2};
    out->WriteData(bytes, sizeof(bytes));
    out->Close();
  }
  const auto file = dir.Open("tiny", duckdb::MappedFileAccess::SEQUENTIAL);
  irs::ReadContext ctx{Db(), file.get()};
  duckdb::Vector result{LogicalType::VARCHAR, 1};
  try {
    ctx.ReadString(result, 0, 0);
    ADD_FAILURE() << "an overflow string was read from a file of two bytes";
  } catch (const irs::SqlException& e) {
    EXPECT_EQ(e.error().errcode, ERRCODE_INDEX_CORRUPTED) << e.what();
    EXPECT_EQ(e.message(),
              "Corrupted uncompressed string segment: overflow string is "
              "outside the file");
  }
}

}  // namespace
