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
#include <iresearch/store/memory_directory.hpp>
#include <limits>
#include <magic_enum/magic_enum.hpp>
#include <span>
#include <string_view>
#include <vector>

#include "gtest/gtest.h"

namespace {

using irs::codecs::CodecType;
using irs::codecs::ColumnDataCheckpointData;
using irs::codecs::CompressionFunction;
using irs::codecs::Contender;

constexpr uint64_t kRows = 100;

struct Probe {
  double estimate = std::numeric_limits<double>::infinity();
  double bound = 0;
  uint64_t bytes = kRows;
  bool writes = true;
  int analyzed = 0;
  int compressed = 0;
};

duckdb::BaseStatistics StatsOf(const ColumnDataCheckpointData& data,
                               int64_t min, int64_t max) {
  auto stats = duckdb::BaseStatistics::CreateEmpty(data.type);
  stats.SetHasNoNullFast();
  stats.UpdateNumericStats<int64_t>(min);
  stats.UpdateNumericStats<int64_t>(max);
  return stats;
}

duckdb::BaseStatistics RowStats(const ColumnDataCheckpointData& data) {
  return StatsOf(data, 0, static_cast<int64_t>(kRows) - 1);
}

template<int kId>
struct Fake {
  static inline Probe probe;

  static double Analyze(ColumnDataCheckpointData&, double bound) {
    ++probe.analyzed;
    probe.bound = bound;
    return probe.estimate;
  }

  static bool Compress(ColumnDataCheckpointData& data) {
    ++probe.compressed;
    if (!probe.writes) {
      return false;
    }
    data.FlushSegment(kCodec, RowStats(data), data.rows,
                      std::string(probe.bytes, 'x'));
    return true;
  }

  static constexpr CodecType kCodec =
    kId == 0 ? CodecType::Rle
             : (kId == 1 ? CodecType::Bitpacking : CodecType::Alp);
};

template<int kId>
constexpr CompressionFunction kFake{
  .analyze = Fake<kId>::Analyze,
  .compress = Fake<kId>::Compress,
};

template<int kId>
Contender FakeOf() {
  return {&kFake<kId>, Fake<kId>::kCodec};
}

uint64_t StreamedAnalyze(std::span<const irs::WriteChunk>) { return kRows; }

void StreamedCompress(ColumnDataCheckpointData& data,
                      std::span<const irs::WriteChunk>) {
  data.FlushSegment(CodecType::Rle, RowStats(data), data.rows, {});
}

void ConstantCompress(ColumnDataCheckpointData& data,
                      std::span<const irs::WriteChunk>) {
  data.FlushSegment(CodecType::Rle, StatsOf(data, 7, 7), data.rows, {});
}

constexpr const CompressionFunction& kConstantRle =
  irs::codecs::kStreamed<CodecType::Rle, StreamedAnalyze, ConstantCompress>;

constexpr const CompressionFunction& kStreamedRle =
  irs::codecs::kStreamed<CodecType::Rle, StreamedAnalyze, StreamedCompress>;

class ColCompressionFunctionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    Fake<0>::probe = {};
    Fake<1>::probe = {};
    Fake<2>::probe = {};
    _values.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(_values);
    for (uint64_t i = 0; i < kRows; ++i) {
      data[i] = static_cast<int64_t>(i);
    }
    duckdb::FlatVector::SetSize(_values, kRows);
    _chunks.push_back(irs::RefChunk(_values, kRows));
  }

  CodecType Compress(std::span<const Contender> contenders, irs::WriteTier tier,
                     irs::codecs::CodecSet disabled = {}) {
    auto out = _dir.create("blocks");
    irs::WriteContext ctx{_db.instance->GetBufferManager(), *out};
    CodecType flushed = CodecType::Auto;
    const auto flush = [&](const irs::codecs::Codec& codec,
                           duckdb::BaseStatistics, uint64_t,
                           std::string_view) { flushed = codec.type; };
    ColumnDataCheckpointData data{
      .type = duckdb::LogicalType::BIGINT,
      .chunks = _chunks,
      .ctx = &ctx,
      .flush = flush,
      .params = {.tier = tier},
      .tuning = _tuning,
      .disabled = disabled,
      .rows = kRows,
    };
    irs::codecs::Contest(contenders, data);
    return flushed;
  }

  CodecType CompressAs(CodecType method, irs::WriteTier tier,
                       irs::codecs::CodecSet disabled = {}) {
    auto out = _dir.create("blocks");
    irs::WriteContext ctx{_db.instance->GetBufferManager(), *out};
    CodecType flushed = CodecType::Auto;
    const auto flush = [&](const irs::codecs::Codec& codec,
                           duckdb::BaseStatistics, uint64_t,
                           std::string_view) { flushed = codec.type; };
    ColumnDataCheckpointData data{
      .type = duckdb::LogicalType::BIGINT,
      .chunks = _chunks,
      .ctx = &ctx,
      .flush = flush,
      .params = {.tier = tier},
      .tuning = _tuning,
      .disabled = disabled,
    };
    irs::codecs::Compress(method, data);
    return flushed;
  }

  duckdb::DuckDB _db;
  irs::MemoryDirectory _dir;
  duckdb::Vector _values{duckdb::LogicalType::BIGINT, kRows};
  std::vector<irs::WriteChunk> _chunks;
  irs::codecs::ColumnTuning _tuning;
};

TEST_F(ColCompressionFunctionTest, TheSmallestEstimateWins) {
  Fake<1>::probe.estimate = 0.5;
  const Contender functions[] = {{&kStreamedRle, CodecType::Rle}, FakeOf<1>()};
  EXPECT_EQ(Compress(functions, irs::WriteTier::Merge), CodecType::Bitpacking);
  EXPECT_EQ(Fake<1>::probe.compressed, 1);
}

TEST_F(ColCompressionFunctionTest, ASegmentWithConstantStatsIsStoredConstant) {
  const Contender functions[] = {{&kConstantRle, CodecType::Rle}};
  EXPECT_EQ(Compress(functions, irs::WriteTier::Flush), CodecType::Constant);
}

TEST_F(ColCompressionFunctionTest,
       ASegmentWithConstantStatsKeepsItsCodecWhenConstantIsDisabled) {
  const Contender functions[] = {{&kConstantRle, CodecType::Rle}};
  irs::codecs::CodecSet disabled;
  disabled.set(std::to_underlying(CodecType::Constant));
  EXPECT_EQ(Compress(functions, irs::WriteTier::Flush, disabled),
            CodecType::Rle);
}

TEST_F(ColCompressionFunctionTest, AWinnerThatCannotWriteLeavesItUncompressed) {
  Fake<1>::probe.estimate = 0.5;
  Fake<1>::probe.writes = false;
  const Contender functions[] = {FakeOf<1>()};
  EXPECT_EQ(Compress(functions, irs::WriteTier::Flush),
            CodecType::Uncompressed);
  EXPECT_EQ(Fake<1>::probe.compressed, 1);
}

TEST_F(ColCompressionFunctionTest, AHeldCodecThatGrowsIsContestedAgain) {
  Fake<0>::probe.estimate = 0.5;
  Fake<0>::probe.bytes = kRows / 2;
  const Contender functions[] = {FakeOf<0>(), FakeOf<1>()};
  for (int i = 0; i < 4; ++i) {
    EXPECT_EQ(Compress(functions, irs::WriteTier::Flush), CodecType::Rle);
  }
  EXPECT_EQ(Fake<0>::probe.analyzed, 3);
  EXPECT_EQ(Compress(functions, irs::WriteTier::Flush), CodecType::Rle);
  EXPECT_EQ(Fake<0>::probe.analyzed, 3);
  Fake<0>::probe.bytes = 2 * kRows;
  EXPECT_EQ(Compress(functions, irs::WriteTier::Flush), CodecType::Rle);
  EXPECT_EQ(Fake<0>::probe.analyzed, 3);
  EXPECT_EQ(Compress(functions, irs::WriteTier::Flush), CodecType::Rle);
  EXPECT_EQ(Fake<0>::probe.analyzed, 4);
}

TEST_F(ColCompressionFunctionTest, AHeldCodecThatCannotWriteIsContestedAgain) {
  Fake<0>::probe.estimate = 0.5;
  Fake<0>::probe.bytes = kRows / 2;
  const Contender functions[] = {FakeOf<0>(), FakeOf<1>()};
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(Compress(functions, irs::WriteTier::Flush), CodecType::Rle);
  }
  EXPECT_EQ(Fake<0>::probe.analyzed, 3);
  Fake<0>::probe.writes = false;
  EXPECT_EQ(Compress(functions, irs::WriteTier::Flush),
            CodecType::Uncompressed);
  EXPECT_EQ(Fake<0>::probe.analyzed, 4);
}

TEST_F(ColCompressionFunctionTest,
       EveryFunctionIsAnalyzedOnceUnderTheBestScoreBeforeIt) {
  Fake<0>::probe.estimate = 0.6;
  Fake<1>::probe.estimate = 0.3;
  Fake<1>::probe.bytes = kRows / 4;
  Fake<2>::probe.estimate = 0.4;
  const Contender functions[] = {FakeOf<0>(), FakeOf<1>(), FakeOf<2>()};
  EXPECT_EQ(Compress(functions, irs::WriteTier::Flush), CodecType::Bitpacking);
  EXPECT_EQ(Fake<0>::probe.analyzed, 1);
  EXPECT_EQ(Fake<1>::probe.analyzed, 1);
  EXPECT_EQ(Fake<2>::probe.analyzed, 1);
  EXPECT_EQ(Fake<0>::probe.bound, std::numeric_limits<double>::infinity());
  EXPECT_EQ(Fake<1>::probe.bound, 0.6);
  EXPECT_EQ(Fake<2>::probe.bound, 0.3);
}

TEST_F(ColCompressionFunctionTest,
       AHeldFunctionTheRowGroupDoesNotOfferIsNotReused) {
  Fake<0>::probe.estimate = 0.5;
  Fake<1>::probe.estimate = 0.5;
  const Contender first[] = {FakeOf<0>(), FakeOf<2>()};
  const Contender second[] = {FakeOf<1>(), FakeOf<2>()};
  EXPECT_EQ(Compress(first, irs::WriteTier::Flush), CodecType::Rle);
  EXPECT_EQ(Compress(first, irs::WriteTier::Flush), CodecType::Rle);
  EXPECT_EQ(Fake<0>::probe.analyzed, 2);
  EXPECT_EQ(Compress(second, irs::WriteTier::Flush), CodecType::Bitpacking);
  EXPECT_EQ(Fake<0>::probe.analyzed, 2);
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

TEST_F(ColCompressionFunctionTest,
       AHeldLeafTheNamedCodecDoesNotUseIsNotReused) {
  EXPECT_EQ(CompressAs(CodecType::Lz4, irs::WriteTier::Merge), CodecType::Lz4);
  EXPECT_EQ(CompressAs(CodecType::Lz4, irs::WriteTier::Merge), CodecType::Lz4);
  EXPECT_EQ(CompressAs(CodecType::Zstd, irs::WriteTier::Merge),
            CodecType::Zstd);
}

TEST_F(ColCompressionFunctionTest,
       AHeldLeafTheAutoPlanDoesNotOfferIsNotReused) {
  EXPECT_EQ(CompressAs(CodecType::Zstd, irs::WriteTier::Flush),
            CodecType::Zstd);
  EXPECT_EQ(CompressAs(CodecType::Zstd, irs::WriteTier::Flush),
            CodecType::Zstd);
  EXPECT_NE(CompressAs(CodecType::Auto, irs::WriteTier::Flush),
            CodecType::Zstd);
}

TEST_F(ColCompressionFunctionTest, ALayoutFunctionWritesItsOwnLayout) {
  const auto layouts =
    irs::codecs::Contenders(CodecType::Lz4, duckdb::PhysicalType::INT64);
  ASSERT_GE(layouts.size(), 3U);
  const auto& raw = layouts[0];
  const auto& narrowed = layouts[2];
  auto out = _dir.create("blocks");
  irs::WriteContext ctx{_db.instance->GetBufferManager(), *out};
  std::string written;
  const auto flush = [&](const irs::codecs::Codec&, duckdb::BaseStatistics,
                         uint64_t, std::string_view bytes) { written = bytes; };
  ColumnDataCheckpointData data{
    .type = duckdb::LogicalType::BIGINT,
    .chunks = _chunks,
    .ctx = &ctx,
    .flush = flush,
    .params = {.tier = irs::WriteTier::Merge},
    .tuning = _tuning,
    .rows = kRows,
  };
  const auto stored = [&] {
    const auto* block =
      reinterpret_cast<duckdb::const_data_ptr_t>(written.data());
    return irs::codecs::ParseFrames(block, written.size(), CodecType::Lz4)
      .stored;
  };
  const auto first =
    raw.function->analyze(data, std::numeric_limits<double>::infinity());
  ASSERT_LT(first, irs::codecs::kNoScore);
  const auto second = narrowed.function->analyze(data, first);
  ASSERT_LT(second, first);
  ASSERT_TRUE(raw.function->compress(data));
  EXPECT_EQ(stored(), sizeof(int64_t));
  ASSERT_TRUE(narrowed.function->compress(data));
  EXPECT_EQ(stored(), 1);
}

TEST_F(ColCompressionFunctionTest,
       AHeldFunctionIsNotContestedForBitPackingTheRowGroupDoesNotOffer) {
  Fake<0>::probe.estimate = 0.5;
  Fake<0>::probe.bytes = 4 * kRows;
  const Contender functions[] = {FakeOf<0>(), FakeOf<1>()};
  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(Compress(functions, irs::WriteTier::Flush), CodecType::Rle);
  }
  EXPECT_EQ(Fake<0>::probe.analyzed, 2);
}

TEST_F(ColCompressionFunctionTest,
       AHeldFunctionIsContestedWhenBitPackingOnOfferUndercutsIt) {
  Fake<0>::probe.estimate = 0.5;
  Fake<0>::probe.bytes = 4 * kRows;
  const Contender functions[] = {{&kFake<0>, CodecType::Ffor}, FakeOf<1>()};
  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(Compress(functions, irs::WriteTier::Flush), CodecType::Rle);
  }
  EXPECT_EQ(Fake<0>::probe.analyzed, 3);
}

TEST_F(ColCompressionFunctionTest,
       FunctionsOfOneCodecAreContestedAgainOnlyWhenTheHeldOneGrows) {
  Fake<0>::probe.estimate = 0.5;
  Fake<0>::probe.bytes = kRows / 2;
  Fake<1>::probe.estimate = 0.6;
  const Contender functions[] = {{&kFake<0>, CodecType::Rle},
                                 {&kFake<1>, CodecType::Rle}};
  for (int i = 0; i < 20; ++i) {
    EXPECT_EQ(Compress(functions, irs::WriteTier::Flush), CodecType::Rle);
  }
  EXPECT_EQ(Fake<0>::probe.analyzed, 1);
  EXPECT_EQ(Fake<1>::probe.analyzed, 1);
  Fake<0>::probe.bytes = 2 * kRows;
  EXPECT_EQ(Compress(functions, irs::WriteTier::Flush), CodecType::Rle);
  EXPECT_EQ(Fake<0>::probe.analyzed, 1);
  EXPECT_EQ(Compress(functions, irs::WriteTier::Flush), CodecType::Rle);
  EXPECT_EQ(Fake<0>::probe.analyzed, 2);
  EXPECT_EQ(Fake<1>::probe.analyzed, 2);
}

TEST_F(ColCompressionFunctionTest, OnlyTheFrameOptionThatBidWritesText) {
  constexpr uint64_t kStrings = 8192;
  duckdb::Vector strings{duckdb::LogicalType::VARCHAR, kStrings};
  strings.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* values = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(strings);
  uint64_t state = 1;
  for (uint64_t i = 0; i < kStrings; ++i) {
    state = state * 6364136223846793005ULL + 1442695040888963407ULL;
    values[i] = duckdb::StringVector::AddString(
      strings, absl::StrCat("https://example.org/items/", state >> 40));
  }
  duckdb::FlatVector::SetSize(strings, kStrings);
  std::vector<irs::WriteChunk> chunks;
  chunks.push_back(irs::RefChunk(strings, kStrings));
  auto out = _dir.create("blocks");
  irs::WriteContext ctx{_db.instance->GetBufferManager(), *out};
  uint64_t segments = 0;
  const auto flush = [&](const irs::codecs::Codec& codec,
                         duckdb::BaseStatistics, uint64_t, std::string_view) {
    EXPECT_EQ(codec.type, CodecType::Lz4);
    ++segments;
  };
  ColumnDataCheckpointData data{
    .type = duckdb::LogicalType::VARCHAR,
    .chunks = chunks,
    .ctx = &ctx,
    .flush = flush,
    .params = {.tier = irs::WriteTier::Merge},
    .tuning = _tuning,
    .rows = kStrings,
  };
  const auto contenders =
    irs::codecs::Contenders(CodecType::Lz4, duckdb::PhysicalType::VARCHAR);
  ASSERT_EQ(contenders.size(), 3U);
  const Contender* bidder = nullptr;
  for (const auto& contender : contenders) {
    if (contender.function->analyze(data, irs::codecs::kNoScore) <
        irs::codecs::kNoScore) {
      ASSERT_EQ(bidder, nullptr);
      bidder = &contender;
    }
  }
  ASSERT_NE(bidder, nullptr);
  for (const auto& contender : contenders) {
    if (&contender != bidder) {
      EXPECT_FALSE(contender.function->compress(data));
    }
  }
  EXPECT_EQ(segments, 0U);
  EXPECT_TRUE(bidder->function->compress(data));
  EXPECT_GT(segments, 0U);
}

TEST_F(ColCompressionFunctionTest, OnlyCodecsWithAFunctionAreWritable) {
  const auto bigint = duckdb::PhysicalType::INT64;
  EXPECT_TRUE(irs::codecs::Writes(CodecType::Rle, bigint, {}));
  EXPECT_FALSE(irs::codecs::Writes(CodecType::Auto, bigint, {}));
  EXPECT_FALSE(irs::codecs::Writes(CodecType::Constant, bigint, {}));
  EXPECT_FALSE(irs::codecs::Writes(CodecType::Fsst, bigint, {}));
  irs::codecs::CodecSet disabled;
  disabled.set(std::to_underlying(CodecType::Rle));
  EXPECT_FALSE(irs::codecs::Writes(CodecType::Rle, bigint, disabled));
}

}  // namespace
