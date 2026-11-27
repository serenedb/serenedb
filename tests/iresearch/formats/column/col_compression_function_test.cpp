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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <duckdb.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <iresearch/formats/column/codecs/codec_type.hpp>
#include <iresearch/formats/column/codecs/compression_function.hpp>
#include <iresearch/formats/column/codecs/registry.hpp>
#include <iresearch/formats/column/codecs/text/string_tuning.hpp>
#include <iresearch/formats/column/codecs/tuning.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/internal/write_context.hpp>
#include <iresearch/formats/column/list_ingest.hpp>
#include <iresearch/index/column_info.hpp>
#include <limits>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "index_dir.hpp"
#include "tests_shared.hpp"

namespace {

using irs::codecs::CodecType;
using irs::codecs::ColumnDataCheckpointData;

constexpr uint64_t kRows = 100;
constexpr uint64_t kManyRows = 16384;
constexpr irs::field_id kField = 1;

uint64_t Mix(uint64_t g) noexcept {
  g += 0x9E3779B97F4A7C15ULL;
  g = (g ^ (g >> 30)) * 0xBF58476D1CE4E5B9ULL;
  g = (g ^ (g >> 27)) * 0x94D049BB133111EBULL;
  return g ^ (g >> 31);
}

class FlushedBlocks {
 public:
  explicit FlushedBlocks(const duckdb::LogicalType& type) {
    _meta.id = kField;
    _meta.type = type;
  }

  void Add(const irs::codecs::Codec& codec, duckdb::BaseStatistics stats,
           uint64_t rows, std::string_view bytes, std::string_view shared) {
    if (rows == 0) {
      return;
    }
    if (!shared.empty() && shared != _shared) {
      _shared_offset = Append(shared);
      _shared = shared;
    }
    auto& block = _meta.data.emplace_back(irs::ColumnBlockMeta{
      .statistics = std::move(stats),
      .tuple_count = rows,
      .codec = &codec,
    });
    if (!bytes.empty()) {
      block.file_offset = Append(bytes);
      block.byte_size = bytes.size();
      if (!shared.empty()) {
        block.shared_offset = _shared_offset;
        block.shared_size = shared.size();
      }
    }
  }

  template<typename Self>
  auto& Blocks(this Self& self) noexcept {
    return self._meta.data;
  }

  irs::ColReader Open(const tests::IndexDir& dir) {
    constexpr std::string_view kFile = "blocks";
    {
      auto out = dir.Create(kFile);
      out->WriteData(reinterpret_cast<const irs::byte_type*>(_file.data()),
                     _file.size());
      out->Close();
    }
    irs::ColFooter footer;
    footer.columns.push_back(std::move(_meta));
    return irs::ColReader{dir.Db(),
                          dir.Open(kFile, duckdb::MappedFileAccess::SEQUENTIAL),
                          std::move(footer)};
  }

 private:
  uint64_t Append(std::string_view bytes) {
    _file.resize(duckdb::AlignValue<size_t, 8>(_file.size()), '\0');
    const auto at = _file.size();
    _file.append(bytes);
    return at;
  }

  irs::ColumnMeta _meta;
  std::string _file;
  std::string _shared;
  uint64_t _shared_offset = 0;
};

class Rows {
 public:
  Rows(const duckdb::LogicalType& type, std::vector<duckdb::Value> values)
    : _type{type}, _values{std::move(values)} {
    for (uint64_t pos = 0; pos < _values.size();) {
      const auto take =
        std::min<duckdb::idx_t>(_values.size() - pos, STANDARD_VECTOR_SIZE);
      auto& v = _vectors.emplace_back(type, STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        v.SetValue(k, _values[pos + k]);
      }
      duckdb::FlatVector::SetSize(v, take);
      pos += take;
    }
    for (const auto& v : _vectors) {
      _chunks.push_back(irs::RefChunk(v, v.size()));
    }
  }

  const duckdb::LogicalType& Type() const noexcept { return _type; }
  std::span<const irs::WriteChunk> Chunks() const noexcept { return _chunks; }
  uint64_t Count() const noexcept { return _values.size(); }

  void ExpectReadBack(irs::ColReader& r) const {
    const auto* col = r.Column(kField);
    ASSERT_NE(col, nullptr);
    ASSERT_EQ(col->RowCount(), _values.size());
    auto state = col->InitScan(r.Ctx());
    for (uint64_t pos = 0; pos < _values.size();) {
      const auto take =
        std::min<duckdb::idx_t>(_values.size() - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector out{col->Type(), STANDARD_VECTOR_SIZE};
      col->Scan(state, out, take);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        ASSERT_TRUE(
          duckdb::Value::NotDistinctFrom(out.GetValue(k), _values[pos + k]))
          << "row " << pos + k << ": " << out.GetValue(k).ToString()
          << " != " << _values[pos + k].ToString();
      }
      pos += take;
    }
    irs::ColumnReader::PointReader reader{r, *col};
    duckdb::Vector one{col->Type(), 1};
    for (uint64_t g = _values.size(); g-- > 0;) {
      if (g % 37 != 0 && g + 1 != _values.size()) {
        continue;
      }
      duckdb::FlatVector::ValidityMutable(one).Reset();
      ASSERT_EQ(reader.FetchRow(g, one, 0), !_values[g].IsNull()) << g;
      if (!_values[g].IsNull()) {
        ASSERT_TRUE(duckdb::Value::NotDistinctFrom(one.GetValue(0), _values[g]))
          << "row " << g;
      }
    }
  }

 private:
  duckdb::LogicalType _type;
  std::vector<duckdb::Value> _values;
  std::vector<duckdb::Vector> _vectors;
  std::vector<irs::WriteChunk> _chunks;
};

class ColCompressionFunctionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    Fill(kRows, [](uint64_t i) { return static_cast<int64_t>(i); });
  }

  template<typename Value>
  void Fill(uint64_t rows, Value&& value) {
    _values =
      std::make_unique<duckdb::Vector>(duckdb::LogicalType::BIGINT, rows);
    _values->SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(*_values);
    for (uint64_t i = 0; i < rows; ++i) {
      data[i] = value(i);
    }
    duckdb::FlatVector::SetSize(*_values, rows);
    _chunks.clear();
    _chunks.push_back(irs::RefChunk(*_values, rows));
  }

  template<typename Valid>
  void FillValidity(uint64_t rows, Valid&& valid) {
    _values =
      std::make_unique<duckdb::Vector>(duckdb::LogicalType::TINYINT, rows);
    _values->SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    auto& validity = duckdb::FlatVector::ValidityMutable(*_values);
    validity.Reset(rows);
    for (uint64_t i = 0; i < rows; ++i) {
      if (!valid(i)) {
        validity.SetInvalid(i);
      }
    }
    duckdb::FlatVector::SetSize(*_values, rows);
    _chunks.clear();
    _chunks.push_back(irs::RefChunk(*_values, rows));
  }

  std::vector<CodecType> Blocks(const duckdb::LogicalType& type,
                                CodecType method, irs::WriteTier tier,
                                irs::codecs::CodecSet disabled = {}) {
    duckdb::MemoryStream out;
    irs::WriteContext ctx{_db.instance->GetBufferManager(), out};
    std::vector<CodecType> blocks;
    const auto flush = [&](const irs::codecs::Codec& codec,
                           duckdb::BaseStatistics, uint64_t, std::string_view,
                           std::string_view) { blocks.push_back(codec.type); };
    ColumnDataCheckpointData data{
      .type = type,
      .ctx = &ctx,
      .flush = flush,
      .params = {.tier = tier},
      .tuning = _tuning,
      .disabled = disabled,
      .rows = {_chunks, _values->size()},
    };
    irs::codecs::Compress(method, data);
    return blocks;
  }

  CodecType CompressAs(CodecType method, irs::WriteTier tier,
                       irs::codecs::CodecSet disabled = {}) {
    const auto blocks =
      Blocks(duckdb::LogicalType::BIGINT, method, tier, disabled);
    return blocks.empty() ? CodecType::Auto : blocks.back();
  }

  const irs::codecs::Hint& HintOf(CodecType type) const noexcept {
    return _tuning.hints[std::to_underlying(type)];
  }

  template<typename Run>
  void Checkpoint(const duckdb::LogicalType& type, const Rows& rows,
                  irs::ColCodecParams params, irs::codecs::CodecSet disabled,
                  bool contested, FlushedBlocks& blocks, Run&& run) {
    duckdb::MemoryStream out;
    irs::WriteContext ctx{_db.instance->GetBufferManager(), out};
    const auto flush = [&](const irs::codecs::Codec& codec,
                           duckdb::BaseStatistics stats, uint64_t count,
                           std::string_view bytes, std::string_view shared) {
      blocks.Add(codec, std::move(stats), count, bytes, shared);
    };
    irs::codecs::ColumnTuning tuning;
    ColumnDataCheckpointData data{
      .type = type,
      .ctx = &ctx,
      .flush = flush,
      .params = params,
      .tuning = tuning,
      .disabled = disabled,
      .contested = contested,
      .rows = {rows.Chunks(), rows.Count()},
    };
    run(data);
  }

  void ExpectReadBack(FlushedBlocks& blocks, const Rows& rows) {
    tests::IndexDir dir{*_db.instance, TestEnv::MemoryRoot()};
    auto r = blocks.Open(dir);
    rows.ExpectReadBack(r);
  }

  duckdb::DuckDB _db;
  std::unique_ptr<duckdb::Vector> _values;
  std::vector<irs::WriteChunk> _chunks;
  irs::codecs::ColumnTuning _tuning;
};

std::vector<duckdb::Value> RepeatedUrls(uint64_t rows, uint64_t distinct) {
  std::vector<duckdb::Value> values;
  values.reserve(rows);
  for (uint64_t g = 0; g < rows; ++g) {
    const auto k = Mix(g) % distinct;
    values.emplace_back(absl::StrCat("https://example.org/catalog/",
                                     Mix(k) % 100003, "/item/", k));
  }
  return values;
}

TEST_F(ColCompressionFunctionTest,
       NamedOnlyTextCodecsContestedAsAutosCandidatesReadBack) {
  const duckdb::LogicalType type = duckdb::LogicalType::VARCHAR;
  const Rows rows{type, RepeatedUrls(30000, 2000)};
  irs::codecs::CodecSet disabled;
  disabled.set(std::to_underlying(CodecType::DictFsst));
  disabled.set(std::to_underlying(CodecType::DictLz4));
  const auto& window =
    *irs::codecs::GetCodec(CodecType::Fsst, type.InternalType());
  struct Variant {
    uint8_t level;
    uint8_t option;
  };
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    for (const auto named :
         {CodecType::Zstd, CodecType::Zxc, CodecType::Fsst12,
          CodecType::FsstPlus, CodecType::Onpair, CodecType::DictZstd,
          CodecType::DictZxc, CodecType::DictFsst12, CodecType::DictFsstPlus,
          CodecType::DictOnpair}) {
      const auto& codec = *irs::codecs::GetCodec(named, type.InternalType());
      ASSERT_EQ(codec.compression.size(), 1);
      const auto& fn = codec.compression.front();
      std::vector<Variant> variants{{0, 0}};
      if (fn.estimate) {
        const uint8_t high = irs::codecs::MaxLevel(named) / 2 + 2;
        for (const uint8_t level : {uint8_t{1}, high}) {
          for (const uint8_t option :
               {uint8_t{0}, irs::codecs::text::kSorted,
                irs::codecs::text::kWide,
                static_cast<uint8_t>(irs::codecs::text::kSorted |
                                     irs::codecs::text::kWide)}) {
            variants.push_back({level, option});
          }
        }
      }
      for (const bool after_another : {false, true}) {
        SCOPED_TRACE(testing::Message()
                     << magic_enum::enum_name(named) << " tier "
                     << magic_enum::enum_name(tier) << " after another "
                     << after_another);
        FlushedBlocks blocks{type};
        Checkpoint(
          type, rows, {.tier = tier}, disabled, true, blocks,
          [&](ColumnDataCheckpointData& data) {
            if (after_another) {
              EXPECT_LT(
                window.compression.front().analyze(data, irs::codecs::kNoScore),
                irs::codecs::kNoScore);
            }
            const auto hand = [&](const Variant& variant) {
              data.params.compression_level = variant.level;
              data.option = variant.option;
            };
            for (const auto& variant : variants) {
              hand(variant);
              if (fn.estimate) {
                const auto estimate = fn.estimate(data);
                EXPECT_GT(estimate, 0.0);
                EXPECT_LT(estimate, 1.0);
              }
            }
            for (const auto& variant : variants) {
              hand(variant);
              const auto score = fn.analyze(data, irs::codecs::kNoScore);
              EXPECT_GT(score, 0.0);
              EXPECT_LT(score, 1.0);
            }
            hand(variants.back());
            EXPECT_TRUE(fn.compress(codec, data));
          });
        ASSERT_FALSE(blocks.Blocks().empty());
        for (const auto& block : blocks.Blocks()) {
          EXPECT_EQ(block.codec->type, named);
        }
        ASSERT_NO_FATAL_FAILURE(ExpectReadBack(blocks, rows));
      }
    }
  }
}

std::vector<duckdb::Value> Steps(const duckdb::LogicalType& type, uint64_t rows,
                                 bool scattered = false) {
  std::vector<duckdb::Value> values;
  values.reserve(rows);
  for (uint64_t g = 0; g < rows; ++g) {
    const auto step =
      static_cast<int64_t>((scattered ? Mix(g / 5) : g / 5) % 40) - 20;
    switch (type.id()) {
      case duckdb::LogicalTypeId::FLOAT:
        values.push_back(duckdb::Value::FLOAT(static_cast<float>(step) / 4));
        break;
      case duckdb::LogicalTypeId::DOUBLE:
        values.push_back(duckdb::Value::DOUBLE(static_cast<double>(step) / 4));
        break;
      default:
        values.push_back(
          duckdb::Value::BIGINT(type.IsUnsigned() ? step + 20 : step)
            .DefaultCastAs(type));
    }
  }
  return values;
}

std::vector<duckdb::Value> WideHalves(const duckdb::LogicalType& type,
                                      uint64_t rows) {
  std::vector<duckdb::Value> values;
  values.reserve(rows);
  for (uint64_t g = 0; g < rows; ++g) {
    const uint64_t lower = Mix(g) & ((uint64_t{1} << 40) - 1);
    const auto upper = static_cast<int64_t>(g / 1000 % 3);
    values.push_back(type.id() == duckdb::LogicalTypeId::HUGEINT
                       ? duckdb::Value::HUGEINT(duckdb::hugeint_t{upper, lower})
                       : duckdb::Value::UHUGEINT(duckdb::uhugeint_t{
                           static_cast<uint64_t>(upper), lower}));
  }
  return values;
}

TEST_F(ColCompressionFunctionTest,
       EveryVariantOfANumericCodecRefusesItsRowsOrReadsThemBack) {
  constexpr uint64_t kVariantRows = 5000;
  const auto sweep = [&](std::string_view name, const Rows& rows) {
    const auto& type = rows.Type();
    for (const auto named : magic_enum::enum_values<CodecType>()) {
      if (named == CodecType::Auto || named == CodecType::Constant ||
          named == CodecType::Empty) {
        continue;
      }
      const auto* codec = irs::codecs::GetCodec(named, type.InternalType());
      if (!codec) {
        continue;
      }
      for (size_t i = 0; i < codec->compression.size(); ++i) {
        SCOPED_TRACE(testing::Message()
                     << name << " " << magic_enum::enum_name(named)
                     << " variant " << i);
        const auto& fn = codec->compression[i];
        FlushedBlocks blocks{type};
        bool written = false;
        Checkpoint(type, rows, {}, {}, false, blocks,
                   [&](ColumnDataCheckpointData& data) {
                     if (fn.analyze) {
                       EXPECT_GE(fn.analyze(data, irs::codecs::kNoScore), 0.0);
                     }
                     written = fn.compress(*codec, data);
                   });
        if (!written) {
          EXPECT_TRUE(blocks.Blocks().empty());
          continue;
        }
        ASSERT_FALSE(blocks.Blocks().empty());
        ASSERT_NO_FATAL_FAILURE(ExpectReadBack(blocks, rows));
      }
    }
  };
  for (const duckdb::LogicalType type :
       {duckdb::LogicalType::TINYINT, duckdb::LogicalType::UTINYINT,
        duckdb::LogicalType::SMALLINT, duckdb::LogicalType::USMALLINT,
        duckdb::LogicalType::INTEGER, duckdb::LogicalType::UINTEGER,
        duckdb::LogicalType::BIGINT, duckdb::LogicalType::UBIGINT,
        duckdb::LogicalType::FLOAT, duckdb::LogicalType::DOUBLE,
        duckdb::LogicalType::HUGEINT, duckdb::LogicalType::UHUGEINT}) {
    sweep(absl::StrCat(type.ToString(), " steps"),
          Rows{type, Steps(type, kVariantRows)});
    sweep(absl::StrCat(type.ToString(), " scattered steps"),
          Rows{type, Steps(type, kVariantRows, true)});
    if (type.InternalType() == duckdb::PhysicalType::INT128 ||
        type.InternalType() == duckdb::PhysicalType::UINT128) {
      sweep(absl::StrCat(type.ToString(), " halves"),
            Rows{type, WideHalves(type, kVariantRows)});
    }
  }
}

TEST_F(ColCompressionFunctionTest,
       ADictionaryWhoseBoundAffordsLessThanOneValueOffersNothing) {
  const duckdb::LogicalType type = duckdb::LogicalType::BIGINT;
  const Rows rows{type, Steps(type, 5000)};
  const auto& codec =
    *irs::codecs::GetCodec(CodecType::Dict, type.InternalType());
  for (size_t i = 0; i < codec.compression.size(); ++i) {
    SCOPED_TRACE(i);
    FlushedBlocks blocks{type};
    Checkpoint(type, rows, {}, {}, false, blocks,
               [&](ColumnDataCheckpointData& data) {
                 EXPECT_EQ(codec.compression[i].analyze(data, 1e-9),
                           irs::codecs::kNoScore);
               });
    EXPECT_TRUE(blocks.Blocks().empty());
  }
}

TEST_F(ColCompressionFunctionTest, AlpCodecsOfferNothingForARowGroupOfNoRows) {
  for (const auto named : {CodecType::Alp, CodecType::Alprd}) {
    for (const duckdb::LogicalType type :
         {duckdb::LogicalType::FLOAT, duckdb::LogicalType::DOUBLE}) {
      SCOPED_TRACE(
        absl::StrCat(magic_enum::enum_name(named), " ", type.ToString()));
      const Rows rows{type, {}};
      const auto& codec = *irs::codecs::GetCodec(named, type.InternalType());
      const auto& fn = codec.compression.front();
      FlushedBlocks blocks{type};
      Checkpoint(type, rows, {}, {}, false, blocks,
                 [&](ColumnDataCheckpointData& data) {
                   EXPECT_EQ(fn.analyze(data, irs::codecs::kNoScore),
                             irs::codecs::kNoScore);
                   EXPECT_FALSE(fn.compress(codec, data));
                 });
      EXPECT_TRUE(blocks.Blocks().empty());
    }
  }
}

TEST_F(ColCompressionFunctionTest,
       AContestedWideDictionaryOfOneByteHighHalvesOffersAndWritesNothing) {
  const duckdb::LogicalType type = duckdb::LogicalType::HUGEINT;
  std::vector<duckdb::Value> values;
  values.reserve(5000);
  for (uint64_t g = 0; g < 5000; ++g) {
    values.push_back(duckdb::Value::HUGEINT(duckdb::hugeint_t{
      static_cast<int64_t>(g % 2), uint64_t{0x10000000} + g % 8 * 0x01000001}));
  }
  const Rows rows{type, std::move(values)};
  const auto& codec =
    *irs::codecs::GetCodec(CodecType::Dict, type.InternalType());
  const auto& fn = codec.compression.front();
  FlushedBlocks blocks{type};
  Checkpoint(
    type, rows, {}, {}, true, blocks, [&](ColumnDataCheckpointData& data) {
      EXPECT_EQ(fn.analyze(data, irs::codecs::kNoScore), irs::codecs::kNoScore);
      EXPECT_FALSE(fn.compress(codec, data));
    });
  EXPECT_TRUE(blocks.Blocks().empty());
}

TEST_F(ColCompressionFunctionTest,
       PlainZstdAndZxcContestedOnMoreThanASegmentCutTheirTrialAndReadBack) {
  const duckdb::LogicalType type = duckdb::LogicalType::VARCHAR;
  constexpr uint64_t kDistinct = 40000;
  std::vector<duckdb::Value> values;
  values.reserve(2 * kDistinct);
  for (uint64_t g = 0; g < 2 * kDistinct; ++g) {
    std::string s;
    for (uint64_t i = 0; i < 4; ++i) {
      absl::StrAppend(&s,
                      absl::Hex(Mix(g % kDistinct * 4 + i), absl::kZeroPad16));
    }
    values.emplace_back(std::move(s));
  }
  const Rows rows{type, std::move(values)};
  irs::codecs::CodecSet disabled;
  disabled.set(std::to_underlying(CodecType::DictFsst));
  disabled.set(std::to_underlying(CodecType::DictLz4));
  for (const auto named : {CodecType::Zstd, CodecType::Zxc}) {
    SCOPED_TRACE(magic_enum::enum_name(named));
    const auto& codec = *irs::codecs::GetCodec(named, type.InternalType());
    const auto& fn = codec.compression.front();
    FlushedBlocks blocks{type};
    Checkpoint(type, rows, {}, disabled, true, blocks,
               [&](ColumnDataCheckpointData& data) {
                 EXPECT_LT(fn.analyze(data, irs::codecs::kNoScore),
                           irs::codecs::kNoScore);
                 EXPECT_TRUE(fn.compress(codec, data));
               });
    ASSERT_GT(blocks.Blocks().size(), 1);
    for (const auto& block : blocks.Blocks()) {
      EXPECT_EQ(block.codec->type, named);
    }
    ASSERT_NO_FATAL_FAILURE(ExpectReadBack(blocks, rows));
  }
}

TEST_F(ColCompressionFunctionTest,
       RunsOfFloatsEstimatedOverSeveralLeafFramesReadBack) {
  constexpr uint64_t kRunRows = 122880;
  for (const duckdb::LogicalType type :
       {duckdb::LogicalType::FLOAT, duckdb::LogicalType::DOUBLE}) {
    std::vector<duckdb::Value> values;
    values.reserve(kRunRows);
    for (uint64_t g = 0; g < kRunRows; ++g) {
      const auto run = static_cast<double>(g / 3);
      values.emplace_back(type.id() == duckdb::LogicalTypeId::FLOAT
                            ? duckdb::Value::FLOAT(static_cast<float>(run))
                            : duckdb::Value::DOUBLE(run));
    }
    const Rows rows{type, std::move(values)};
    for (const auto named : {CodecType::RleLz4, CodecType::RleZstd}) {
      SCOPED_TRACE(
        absl::StrCat(magic_enum::enum_name(named), " ", type.ToString()));
      const auto& codec = *irs::codecs::GetCodec(named, type.InternalType());
      ASSERT_EQ(codec.compression.size(), 1);
      const auto& fn = codec.compression.front();
      FlushedBlocks blocks{type};
      Checkpoint(type, rows, {}, {}, false, blocks,
                 [&](ColumnDataCheckpointData& data) {
                   EXPECT_LT(fn.analyze(data, irs::codecs::kNoScore),
                             irs::codecs::kNoScore);
                   EXPECT_TRUE(fn.compress(codec, data));
                 });
      ASSERT_FALSE(blocks.Blocks().empty());
      ASSERT_NO_FATAL_FAILURE(ExpectReadBack(blocks, rows));
    }
  }
}

TEST_F(ColCompressionFunctionTest,
       AnAlpVectorPackedAtSixtyFourBitsAnswersFiltersByItsValues) {
  constexpr uint32_t kCount = 32;
  constexpr uint32_t kVectorHeader = 13;
  constexpr uint32_t kPacked = kCount * sizeof(uint64_t);
  const duckdb::LogicalType type = duckdb::LogicalType::DOUBLE;
  std::vector<duckdb::Value> values;
  std::string vector(kVectorHeader, '\0');
  vector[12] = 64;
  for (uint32_t i = 0; i < kCount; ++i) {
    const uint64_t digit = i * 3 % kCount;
    values.push_back(duckdb::Value::DOUBLE(static_cast<double>(digit)));
    vector.append(reinterpret_cast<const char*>(&digit), sizeof(digit));
  }
  ASSERT_EQ(vector.size(), kVectorHeader + kPacked);
  std::string segment(sizeof(uint32_t), '\0');
  segment.append(vector);
  const uint32_t start = sizeof(uint32_t);
  segment.append(reinterpret_cast<const char*>(&start), sizeof(start));
  const auto metadata_end = static_cast<uint32_t>(segment.size());
  std::memcpy(segment.data(), &metadata_end, sizeof(metadata_end));
  auto stats = duckdb::NumericStats::CreateEmpty(type);
  duckdb::NumericStats::SetMin<double>(stats, 0);
  duckdb::NumericStats::SetMax<double>(stats, kCount - 1);
  stats.SetHasNoNullFast();
  const Rows rows{type, values};
  FlushedBlocks blocks{type};
  blocks.Add(*irs::codecs::GetCodec(CodecType::Alp, type.InternalType()),
             std::move(stats), kCount, segment, {});
  tests::IndexDir dir{*_db.instance, TestEnv::MemoryRoot()};
  auto r = blocks.Open(dir);
  ASSERT_NO_FATAL_FAILURE(rows.ExpectReadBack(r));
  const auto* col = r.Column(kField);
  duckdb::Connection con{_db};
  const duckdb::ExpressionFilter filter{
    duckdb::BoundComparisonExpression::Create(
      duckdb::ExpressionType::COMPARE_LESSTHAN,
      duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0),
      duckdb::make_uniq<duckdb::BoundConstantExpression>(
        duckdb::Value::DOUBLE(10)))};
  auto filter_state =
    duckdb::TableFilterState::Initialize(*con.context, filter);
  auto state = col->InitScan(r.Ctx());
  duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
  for (duckdb::idx_t i = 0; i < kCount; ++i) {
    sel.set_index(i, i);
  }
  duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
  const auto kept =
    col->GatherFilter(state, 0, kCount, sel, kCount, filter, *filter_state,
                      irs::NullCheckKind::None, out);
  std::vector<duckdb::idx_t> want;
  for (uint32_t i = 0; i < kCount; ++i) {
    if (values[i].GetValue<double>() < 10) {
      want.push_back(i);
    }
  }
  ASSERT_EQ(kept, want.size());
  for (duckdb::idx_t k = 0; k < kept; ++k) {
    EXPECT_EQ(sel.get_index(k), want[k]);
    EXPECT_EQ(out.GetValue(want[k]).GetValue<double>(),
              values[want[k]].GetValue<double>());
  }
}

TEST_F(ColCompressionFunctionTest, UncompressedTextIsNeverScoredByAnalysis) {
  const duckdb::LogicalType type = duckdb::LogicalType::VARCHAR;
  const Rows rows{type, RepeatedUrls(1000, 10)};
  const auto& codec =
    *irs::codecs::GetCodec(CodecType::Uncompressed, type.InternalType());
  FlushedBlocks blocks{type};
  Checkpoint(
    type, rows, {}, {}, true, blocks, [&](ColumnDataCheckpointData& data) {
      EXPECT_EQ(codec.compression.front().analyze(data, irs::codecs::kNoScore),
                irs::codecs::kNoScore);
      EXPECT_TRUE(codec.compression.front().compress(codec, data));
    });
  ASSERT_NO_FATAL_FAILURE(ExpectReadBack(blocks, rows));
}

TEST(ColCodecTypeTest, EveryFormIsNamedByItsShapeAndLeaf) {
  size_t forms = 0;
  for (const auto type : magic_enum::enum_values<CodecType>()) {
    if (!irs::codecs::IsForm(type)) {
      continue;
    }
    ++forms;
    EXPECT_EQ(irs::codecs::CodecOf(irs::codecs::ShapeOf(type),
                                   irs::codecs::LeafOf(type)),
              type)
      << magic_enum::enum_name(type);
  }
  EXPECT_EQ(forms, irs::codecs::kTextShapeForms +
                     (irs::codecs::kShapeCount - irs::codecs::kTextShapeCount) *
                       irs::codecs::kNumericLeafCount);
}

TEST_F(ColCompressionFunctionTest,
       ANamedCodecStoresARowGroupOfOneValueConstant) {
  Fill(kManyRows, [](uint64_t) { return int64_t{42}; });
  for (const auto named : {CodecType::Rle, CodecType::Uncompressed}) {
    EXPECT_EQ(CompressAs(named, irs::WriteTier::Flush), CodecType::Constant);
  }
  irs::codecs::CodecSet disabled;
  disabled.set(std::to_underlying(CodecType::Constant));
  EXPECT_EQ(CompressAs(CodecType::Rle, irs::WriteTier::Flush, disabled),
            CodecType::Rle);
}

TEST_F(ColCompressionFunctionTest,
       ABlockOfOneValueIsStoredConstantInARowGroupThatIsNot) {
  constexpr uint64_t kBlockRows = irs::codecs::kBlockSize / sizeof(int64_t);
  Fill(2 * kBlockRows, [](uint64_t i) {
    return i < kBlockRows ? int64_t{7} : static_cast<int64_t>(i);
  });
  EXPECT_EQ(Blocks(duckdb::LogicalType::BIGINT, CodecType::Uncompressed,
                   irs::WriteTier::Flush),
            (std::vector{CodecType::Constant, CodecType::Uncompressed}));
}

TEST_F(ColCompressionFunctionTest, AValidityRowGroupWithoutNullsIsStoredEmpty) {
  const duckdb::LogicalType type{duckdb::LogicalTypeId::VALIDITY};
  FillValidity(kManyRows, [](uint64_t) { return true; });
  for (const auto method :
       {CodecType::Auto, CodecType::Roaring, CodecType::Uncompressed}) {
    EXPECT_EQ(Blocks(type, method, irs::WriteTier::Flush),
              std::vector{CodecType::Empty});
  }
  EXPECT_FALSE(irs::codecs::Writes(CodecType::Empty, type.InternalType()));
}

TEST_F(ColCompressionFunctionTest, AValidityRowGroupOfNullsIsStoredConstant) {
  const duckdb::LogicalType type{duckdb::LogicalTypeId::VALIDITY};
  FillValidity(kManyRows, [](uint64_t) { return false; });
  for (const auto method :
       {CodecType::Auto, CodecType::Roaring, CodecType::Uncompressed}) {
    EXPECT_EQ(Blocks(type, method, irs::WriteTier::Flush),
              std::vector{CodecType::Constant});
  }
  FillValidity(kManyRows, [](uint64_t i) { return i == kManyRows / 2; });
  EXPECT_EQ(Blocks(type, CodecType::Roaring, irs::WriteTier::Flush),
            std::vector{CodecType::Roaring});
}

TEST_F(ColCompressionFunctionTest,
       AValidityBlockWithoutNullsIsStoredEmptyBesideOneWithNulls) {
  constexpr uint64_t kBlockRows = irs::codecs::kBlockSize /
                                  duckdb::ValidityMask::STANDARD_MASK_SIZE *
                                  STANDARD_VECTOR_SIZE;
  const duckdb::LogicalType type{duckdb::LogicalTypeId::VALIDITY};
  FillValidity(2 * kBlockRows, [](uint64_t i) { return i != kBlockRows + 5; });
  EXPECT_EQ(Blocks(type, CodecType::Uncompressed, irs::WriteTier::Flush),
            (std::vector{CodecType::Empty, CodecType::Uncompressed}));
  FillValidity(2 * kBlockRows, [](uint64_t i) { return i < kBlockRows; });
  EXPECT_EQ(Blocks(type, CodecType::Uncompressed, irs::WriteTier::Flush),
            (std::vector{CodecType::Empty, CodecType::Constant}));
}

TEST_F(ColCompressionFunctionTest, AutoStoresAConstantRowGroupConstant) {
  Fill(kManyRows, [](uint64_t) { return int64_t{42}; });
  EXPECT_EQ(CompressAs(CodecType::Auto, irs::WriteTier::Merge),
            CodecType::Constant);
  EXPECT_EQ(HintOf(CodecType::Zstd).forced, 0u);
  irs::codecs::CodecSet disabled;
  disabled.set(std::to_underlying(CodecType::Constant));
  EXPECT_NE(CompressAs(CodecType::Auto, irs::WriteTier::Merge, disabled),
            CodecType::Constant);
}

TEST_F(ColCompressionFunctionTest, ADisabledCodecIsNeitherPickedNorReused) {
  const auto held = CompressAs(CodecType::Auto, irs::WriteTier::Merge);
  ASSERT_EQ(CompressAs(CodecType::Auto, irs::WriteTier::Merge), held);
  irs::codecs::CodecSet disabled;
  disabled.set(std::to_underlying(held));
  const auto next =
    CompressAs(CodecType::Auto, irs::WriteTier::Merge, disabled);
  EXPECT_NE(next, held);
  disabled.set(std::to_underlying(next));
  for (const auto tier : {irs::WriteTier::Flush, irs::WriteTier::Merge}) {
    const auto again = CompressAs(CodecType::Auto, tier, disabled);
    EXPECT_FALSE(disabled.test(std::to_underlying(again)))
      << magic_enum::enum_name(again);
  }
}

TEST_F(ColCompressionFunctionTest, ANamedCodecWritesItsOwnLeafAfterAnother) {
  EXPECT_EQ(CompressAs(CodecType::Lz4, irs::WriteTier::Merge), CodecType::Lz4);
  EXPECT_EQ(CompressAs(CodecType::Lz4, irs::WriteTier::Merge), CodecType::Lz4);
  EXPECT_EQ(CompressAs(CodecType::Zstd, irs::WriteTier::Merge),
            CodecType::Zstd);
}

TEST_F(ColCompressionFunctionTest, AutoDoesNotOfferALeafItsTierDoesNotList) {
  EXPECT_EQ(CompressAs(CodecType::Zstd, irs::WriteTier::Flush),
            CodecType::Zstd);
  EXPECT_EQ(CompressAs(CodecType::Zstd, irs::WriteTier::Flush),
            CodecType::Zstd);
  EXPECT_NE(CompressAs(CodecType::Auto, irs::WriteTier::Flush),
            CodecType::Zstd);
}

TEST_F(ColCompressionFunctionTest,
       ACandidateFarFromWinningStaysOutUntilItsHintIsOld) {
  Fill(kManyRows, [](uint64_t i) { return static_cast<int64_t>(i); });
  const auto& zstd = HintOf(CodecType::Zstd);
  const auto& dict = HintOf(CodecType::Dict);
  const auto first = CompressAs(CodecType::Auto, irs::WriteTier::Merge);
  EXPECT_EQ(first, CodecType::Bitpacking);
  ASSERT_EQ(zstd.forced, 1u);
  ASSERT_EQ(dict.forced, 1u);
  const auto remembered = zstd.score;
  for (unsigned age = 1; age <= irs::codecs::kOldHint; ++age) {
    EXPECT_EQ(CompressAs(CodecType::Auto, irs::WriteTier::Merge), first);
    EXPECT_EQ(zstd.forced, 1u);
    EXPECT_EQ(dict.forced, 1u);
    EXPECT_EQ(zstd.score, remembered);
  }
  EXPECT_EQ(CompressAs(CodecType::Auto, irs::WriteTier::Merge), first);
  EXPECT_EQ(zstd.forced, irs::codecs::kOldHint + 2);
  EXPECT_EQ(dict.forced, irs::codecs::kOldHint + 2);
}

TEST_F(ColCompressionFunctionTest,
       ACandidateMeasuresAgainWhenTheRowGroupMoves) {
  Fill(kManyRows, [](uint64_t i) { return static_cast<int64_t>(i % 1000); });
  CompressAs(CodecType::Auto, irs::WriteTier::Merge);
  CompressAs(CodecType::Auto, irs::WriteTier::Merge);
  const auto& hint = HintOf(CodecType::RleZstd);
  ASSERT_NE(hint.forced, 0u);
  const auto features = hint.features;
  Fill(kManyRows,
       [](uint64_t i) { return static_cast<int64_t>(i / 64 % 1000); });
  CompressAs(CodecType::Auto, irs::WriteTier::Merge);
  EXPECT_NE(hint.features, features);
}

TEST_F(ColCompressionFunctionTest, TheWinnerIsMeasuredInItsOwnRowGroup) {
  Fill(kManyRows, [](uint64_t i) {
    return static_cast<int64_t>((i * 2654435761ULL) % 7919 * 1000003);
  });
  const auto winner = CompressAs(CodecType::Auto, irs::WriteTier::Merge);
  ASSERT_NE(HintOf(winner).forced, 0u) << magic_enum::enum_name(winner);
  for (int rg = 0; rg < 4; ++rg) {
    EXPECT_EQ(CompressAs(CodecType::Auto, irs::WriteTier::Merge), winner);
    EXPECT_LT(HintOf(winner).score, irs::codecs::kNoScore);
  }
}

TEST_F(ColCompressionFunctionTest, ACandidateThatWasNotMeasuredIsNotStored) {
  Fill(kManyRows, [](uint64_t i) { return static_cast<int64_t>(i); });
  ASSERT_EQ(CompressAs(CodecType::Auto, irs::WriteTier::Merge),
            CodecType::Bitpacking);
  auto& zstd = _tuning.hints[std::to_underlying(CodecType::Zstd)];
  ASSERT_NE(zstd.forced, 0u);
  const auto close = HintOf(CodecType::Bitpacking).score * 0.99;
  zstd.score = close;
  EXPECT_EQ(CompressAs(CodecType::Auto, irs::WriteTier::Merge),
            CodecType::Bitpacking);
  EXPECT_EQ(zstd.score, close);
}

TEST(ColHintTest, TheShiftIsTheLargestRatioBetweenThenAndNow) {
  using irs::codecs::Features;
  using irs::codecs::Shift;
  EXPECT_EQ(Shift(Features{8, 0.5F, 0}, Features{8, 0.5F, 0}), 1.0);
  EXPECT_DOUBLE_EQ(Shift(Features{8, 0.5F, 0}, Features{16, 0.5F, 0}), 2.0);
  EXPECT_DOUBLE_EQ(Shift(Features{8, 0.25F, 0}, Features{6, 0.5F, 0}), 2.0);
  EXPECT_EQ(Shift(Features{8, 0, 0}, Features{8, 0.5F, 0}),
            irs::codecs::kNoScore);
}

TEST(ColHintTest, AShiftBelowTheLeastOneDoesNotCount) {
  using irs::codecs::Features;
  using irs::codecs::Hint;
  using irs::codecs::Measures;
  const Hint hint{.features = {10, 0.5F, 0}, .score = 1.0, .forced = 1};
  EXPECT_FALSE(Measures(hint, 0.95, Features{14, 0.5F, 0}, 1.5));
  EXPECT_TRUE(Measures(hint, 0.95, Features{16, 0.5F, 0}, 1.5));
  EXPECT_TRUE(Measures(hint, 0.95, Features{14, 0.5F, 0}, 1.0));

  const Hint nothing{.features = {10, 0.5F, 0}, .forced = 1};
  EXPECT_FALSE(Measures(nothing, 0.95, Features{12, 0.5F, 0}, 1.5));
  EXPECT_TRUE(Measures(nothing, 0.95, Features{15, 0.5F, 0}, 1.5));
  EXPECT_TRUE(Measures(nothing, 0.95, Features{12, 0.5F, 0}, 1.1));
}

TEST(ColHintTest, ACandidateIsAskedAgainOnceItsLastForcedAskIsOld) {
  using irs::codecs::Forgotten;
  using irs::codecs::Hint;
  using irs::codecs::kOldHint;
  EXPECT_TRUE(Forgotten(Hint{}, 1));
  const Hint hint{.forced = 3};
  EXPECT_FALSE(Forgotten(hint, 3));
  EXPECT_FALSE(Forgotten(hint, 3 + kOldHint));
  EXPECT_TRUE(Forgotten(hint, 3 + kOldHint + 1));
}

TEST(ColHintTest, AFormMeasuresWhenItsCreditedAnswerWouldWin) {
  using irs::codecs::kNoScore;
  using irs::codecs::Measures;
  EXPECT_TRUE(Measures(0.9, 1.0, 1.0, 1.0));
  EXPECT_FALSE(Measures(1.0, 1.0, 1.0, 1.0));
  EXPECT_FALSE(Measures(0.99, 1.0, 1.0, 1.0));
  EXPECT_TRUE(Measures(1.5, 1.0, 1.6, 1.0));
  EXPECT_FALSE(Measures(1.5, 1.0, 1.4, 1.0));
  EXPECT_FALSE(Measures(1.5, 1.0, 1.6, 2.0));
  EXPECT_TRUE(Measures(100.0, kNoScore, 1.0, 1.0));
  EXPECT_TRUE(Measures(100.0, 1.0, kNoScore, 1.0));
}

TEST(ColHintTest, AFormWithNothingToOfferLooksAgainOnceTheRowGroupMoves) {
  using irs::codecs::kNoScore;
  using irs::codecs::Measures;
  EXPECT_FALSE(Measures(kNoScore, 1.0, 1.0, 1.1));
  EXPECT_FALSE(Measures(kNoScore, 1.0, 1.05, 1.1));
  EXPECT_TRUE(Measures(kNoScore, 1.0, 1.1, 1.1));
  EXPECT_FALSE(Measures(kNoScore, 1.0, 1.2, 1.5));
  EXPECT_TRUE(Measures(kNoScore, kNoScore, 1.0, 1.1));
}

TEST_F(ColCompressionFunctionTest, ACodecIsWritableWhenItMayBeNamedForAType) {
  const auto bigint = duckdb::PhysicalType::INT64;
  EXPECT_TRUE(irs::codecs::Writes(CodecType::Rle, bigint));
  EXPECT_FALSE(irs::codecs::Writes(CodecType::Auto, bigint));
  EXPECT_FALSE(irs::codecs::Writes(CodecType::Constant, bigint));
  EXPECT_FALSE(irs::codecs::Writes(CodecType::Fsst, bigint));
}

}  // namespace
