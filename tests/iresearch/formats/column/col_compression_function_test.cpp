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

#include <cstdint>
#include <duckdb.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <iresearch/formats/column/codecs/compression_function.hpp>
#include <iresearch/formats/column/codecs/registry.hpp>
#include <iresearch/formats/column/codecs/tuning.hpp>
#include <iresearch/formats/column/internal/write_context.hpp>
#include <iresearch/formats/column/list_ingest.hpp>
#include <iresearch/index/column_info.hpp>
#include <limits>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "gtest/gtest.h"

namespace {

using irs::codecs::CodecType;
using irs::codecs::ColumnDataCheckpointData;

constexpr uint64_t kRows = 100;
constexpr uint64_t kManyRows = 16384;

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
      .chunks = _chunks,
      .ctx = &ctx,
      .flush = flush,
      .params = {.tier = tier},
      .tuning = _tuning,
      .disabled = disabled,
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

  duckdb::DuckDB _db;
  std::unique_ptr<duckdb::Vector> _values;
  std::vector<irs::WriteChunk> _chunks;
  irs::codecs::ColumnTuning _tuning;
};

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
  constexpr uint64_t kBlockRows = irs::codecs::kBlockSize * 8;
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
  EXPECT_FALSE(HintOf(CodecType::Zstd).known);
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
  ASSERT_TRUE(zstd.known);
  ASSERT_TRUE(dict.known);
  const auto remembered = zstd.score;
  for (unsigned age = 1; age < irs::codecs::kOldHint; ++age) {
    EXPECT_EQ(CompressAs(CodecType::Auto, irs::WriteTier::Merge), first);
    EXPECT_EQ(zstd.age, age);
    EXPECT_EQ(dict.age, age);
    EXPECT_EQ(zstd.score, remembered);
  }
  EXPECT_EQ(CompressAs(CodecType::Auto, irs::WriteTier::Merge), first);
  EXPECT_EQ(zstd.age, irs::codecs::kOldHint);
  EXPECT_EQ(CompressAs(CodecType::Auto, irs::WriteTier::Merge), first);
  EXPECT_EQ(zstd.age, 0);
  EXPECT_EQ(dict.age, 0);
}

TEST_F(ColCompressionFunctionTest,
       ACandidateMeasuresAgainWhenTheRowGroupMoves) {
  Fill(kManyRows, [](uint64_t i) { return static_cast<int64_t>(i % 1000); });
  CompressAs(CodecType::Auto, irs::WriteTier::Merge);
  CompressAs(CodecType::Auto, irs::WriteTier::Merge);
  const auto& hint = HintOf(CodecType::RleZstd);
  ASSERT_TRUE(hint.known);
  const auto facts = hint.facts;
  Fill(kManyRows,
       [](uint64_t i) { return static_cast<int64_t>(i / 64 % 1000); });
  CompressAs(CodecType::Auto, irs::WriteTier::Merge);
  EXPECT_NE(hint.facts, facts);
}

TEST_F(ColCompressionFunctionTest, TheWinnerIsMeasuredInItsOwnRowGroup) {
  Fill(kManyRows, [](uint64_t i) {
    return static_cast<int64_t>((i * 2654435761ULL) % 7919 * 1000003);
  });
  const auto winner = CompressAs(CodecType::Auto, irs::WriteTier::Merge);
  ASSERT_TRUE(HintOf(winner).known) << magic_enum::enum_name(winner);
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
  ASSERT_TRUE(zstd.known);
  const auto close = HintOf(CodecType::Bitpacking).score * 0.99;
  zstd.score = close;
  EXPECT_EQ(CompressAs(CodecType::Auto, irs::WriteTier::Merge),
            CodecType::Bitpacking);
  EXPECT_EQ(zstd.score, close);
}

TEST(ColHintTest, TheShiftIsTheLargestRatioBetweenThenAndNow) {
  using irs::codecs::Facts;
  using irs::codecs::Shift;
  EXPECT_EQ(Shift(Facts{8, 0.5F, 0}, Facts{8, 0.5F, 0}), 1.0);
  EXPECT_DOUBLE_EQ(Shift(Facts{8, 0.5F, 0}, Facts{16, 0.5F, 0}), 2.0);
  EXPECT_DOUBLE_EQ(Shift(Facts{8, 0.25F, 0}, Facts{6, 0.5F, 0}), 2.0);
  EXPECT_EQ(Shift(Facts{8, 0, 0}, Facts{8, 0.5F, 0}), irs::codecs::kNoScore);
}

TEST(ColHintTest, AShiftBelowTheLeastOneDoesNotCount) {
  using irs::codecs::Facts;
  using irs::codecs::Hint;
  using irs::codecs::Hinted;
  using irs::codecs::kNoScore;
  unsigned measured = 0;
  const auto measure = [&](bool) {
    ++measured;
    return 0.9;
  };
  Hint hint{.facts = {10, 0.5F, 0}, .score = 1.0, .known = true};
  EXPECT_EQ(Hinted(hint, 0.95, Facts{14, 0.5F, 0}, 1.5, measure), kNoScore);
  EXPECT_EQ(measured, 0);
  EXPECT_EQ(hint.age, 1);
  EXPECT_EQ(Hinted(hint, 0.95, Facts{16, 0.5F, 0}, 1.5, measure), 0.9);
  EXPECT_EQ(measured, 1);
  EXPECT_EQ(hint.age, 0);
  EXPECT_EQ(hint.facts, (Facts{16, 0.5F, 0}));

  Hint nothing{.facts = {10, 0.5F, 0}, .known = true};
  EXPECT_EQ(Hinted(nothing, 0.95, Facts{10.5F, 0.5F, 0}, 1.5, measure),
            kNoScore);
  EXPECT_EQ(measured, 1);
  EXPECT_EQ(Hinted(nothing, 0.95, Facts{12, 0.5F, 0}, 1.5, measure), 0.9);
  EXPECT_EQ(measured, 2);
}

TEST(ColHintTest, AFormMeasuresWhenItsCreditedAnswerWouldWin) {
  using irs::codecs::kNoScore;
  using irs::codecs::Measures;
  EXPECT_TRUE(Measures(0.9, 1.0, 1.0));
  EXPECT_FALSE(Measures(1.0, 1.0, 1.0));
  EXPECT_FALSE(Measures(0.99, 1.0, 1.0));
  EXPECT_TRUE(Measures(1.5, 1.0, 1.6));
  EXPECT_FALSE(Measures(1.5, 1.0, 1.4));
  EXPECT_TRUE(Measures(100.0, kNoScore, 1.0));
  EXPECT_TRUE(Measures(100.0, 1.0, kNoScore));
}

TEST(ColHintTest, AFormWithNothingToOfferLooksAgainOnceTheRowGroupMoves) {
  using irs::codecs::kNoScore;
  using irs::codecs::Measures;
  EXPECT_FALSE(Measures(kNoScore, 1.0, 1.0));
  EXPECT_FALSE(Measures(kNoScore, 1.0, 1.05));
  EXPECT_TRUE(Measures(kNoScore, 1.0, 1.1));
  EXPECT_TRUE(Measures(kNoScore, kNoScore, 1.0));
}

TEST_F(ColCompressionFunctionTest, ACodecIsWritableWhenItMayBeNamedForAType) {
  const auto bigint = duckdb::PhysicalType::INT64;
  EXPECT_TRUE(irs::codecs::Writes(CodecType::Rle, bigint));
  EXPECT_FALSE(irs::codecs::Writes(CodecType::Auto, bigint));
  EXPECT_FALSE(irs::codecs::Writes(CodecType::Constant, bigint));
  EXPECT_FALSE(irs::codecs::Writes(CodecType::Fsst, bigint));
}

}  // namespace
