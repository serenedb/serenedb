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

#include <duckdb.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <iresearch/formats/column/codecs/compression_function.hpp>
#include <iresearch/formats/column/codecs/registry.hpp>
#include <iresearch/formats/column/codecs/tuning.hpp>
#include <iresearch/formats/column/internal/write_context.hpp>
#include <iresearch/formats/column/list_ingest.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <span>
#include <vector>

#include "gtest/gtest.h"

namespace {

using irs::codecs::CodecType;
using irs::codecs::ColumnDataCheckpointData;
using irs::codecs::CompressionFunction;
using irs::codecs::Encoded;
using irs::codecs::Estimate;

constexpr uint64_t kRows = 100;

struct Probe {
  double estimate[2] = {std::numeric_limits<double>::infinity(),
                        std::numeric_limits<double>::infinity()};
  uint64_t encoded_bytes = kRows;
  int analyzed[2] = {};
  int encodes = 0;
};

template<int kId>
struct Fake {
  static inline Probe probe;

  static Estimate Analyze(ColumnDataCheckpointData&, double, uint8_t round) {
    ++probe.analyzed[round];
    return {.codec = kCodec, .ratio = probe.estimate[round]};
  }

  static Encoded Encode(ColumnDataCheckpointData& data, const Estimate*) {
    ++probe.encodes;
    return {
      .bytes = probe.encoded_bytes, .input = data.input, .buffered = true};
  }

  static void Flush(ColumnDataCheckpointData& data) {
    data.codec = irs::codecs::GetCodec(kCodec, data.type.InternalType());
    data.FlushSegment(duckdb::BaseStatistics::CreateEmpty(data.type), data.rows,
                      {});
  }

  static constexpr CodecType kCodec =
    kId == 0 ? CodecType::Rle
             : (kId == 1 ? CodecType::Bitpacking : CodecType::Alp);
};

template<int kId, uint8_t kRounds = 1>
constexpr CompressionFunction kFake{
  .analyze = Fake<kId>::Analyze,
  .encode = Fake<kId>::Encode,
  .flush = Fake<kId>::Flush,
  .rounds = kRounds,
};

uint64_t StreamedAnalyze(std::span<const irs::WriteChunk>) { return kRows; }

void StreamedCompress(ColumnDataCheckpointData& data,
                      std::span<const irs::WriteChunk>) {
  data.FlushSegment(duckdb::BaseStatistics::CreateEmpty(data.type), data.rows,
                    {});
}

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

  CodecType Compress(std::span<const CompressionFunction* const> functions,
                     irs::WriteTier tier, irs::codecs::CodecSet disabled = {}) {
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
      .tuning = &_tuning,
      .disabled = disabled,
      .rows = kRows,
      .input = kRows,
    };
    irs::codecs::Compress(functions, data);
    return flushed;
  }

  CodecType CompressAs(CodecType method, irs::WriteTier tier) {
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
      .tuning = &_tuning,
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

TEST_F(ColCompressionFunctionTest,
       AnExactCodecTakesARowGroupAnEstimateOversold) {
  Fake<1>::probe.estimate[0] = 0.5;
  Fake<1>::probe.encoded_bytes = 2 * kRows;
  const CompressionFunction* const functions[] = {&kStreamedRle, &kFake<1>};
  EXPECT_EQ(Compress(functions, irs::WriteTier::Merge), CodecType::Rle);
  EXPECT_EQ(Fake<1>::probe.encodes, 1);
}

TEST_F(ColCompressionFunctionTest, AnEstimateThatHoldsBeatsTheExactCodec) {
  Fake<1>::probe.estimate[0] = 0.5;
  Fake<1>::probe.encoded_bytes = kRows / 2;
  const CompressionFunction* const functions[] = {&kStreamedRle, &kFake<1>};
  EXPECT_EQ(Compress(functions, irs::WriteTier::Merge), CodecType::Bitpacking);
}

TEST_F(ColCompressionFunctionTest, OnlyFunctionsWithMoreRoundsAreAskedAgain) {
  Fake<0>::probe.estimate[0] = 0.6;
  Fake<1>::probe.estimate[1] = 0.3;
  Fake<1>::probe.encoded_bytes = kRows / 4;
  const CompressionFunction* const functions[] = {&kFake<0>, &kFake<1, 2>};
  EXPECT_EQ(Compress(functions, irs::WriteTier::Flush), CodecType::Bitpacking);
  EXPECT_EQ(Fake<0>::probe.analyzed[0], 1);
  EXPECT_EQ(Fake<0>::probe.analyzed[1], 0);
  EXPECT_EQ(Fake<1>::probe.analyzed[0], 1);
  EXPECT_EQ(Fake<1>::probe.analyzed[1], 1);
}

TEST_F(ColCompressionFunctionTest,
       AHeldFunctionTheRowGroupDoesNotOfferIsNotReused) {
  Fake<0>::probe.estimate[0] = 0.5;
  Fake<1>::probe.estimate[0] = 0.5;
  const CompressionFunction* const first[] = {&kFake<0>};
  const CompressionFunction* const second[] = {&kFake<1>};
  EXPECT_EQ(Compress(first, irs::WriteTier::Flush), CodecType::Rle);
  EXPECT_EQ(Compress(first, irs::WriteTier::Flush), CodecType::Rle);
  EXPECT_EQ(Fake<0>::probe.analyzed[0], 2);
  EXPECT_EQ(Compress(second, irs::WriteTier::Flush), CodecType::Bitpacking);
  EXPECT_EQ(Fake<0>::probe.analyzed[0], 2);
}

TEST_F(ColCompressionFunctionTest,
       AHeldStreamedCodecThatGotDisabledIsNotReused) {
  const CompressionFunction* const functions[] = {&kStreamedRle};
  EXPECT_EQ(Compress(functions, irs::WriteTier::Merge), CodecType::Rle);
  EXPECT_EQ(Compress(functions, irs::WriteTier::Merge), CodecType::Rle);
  irs::codecs::CodecSet disabled;
  disabled.set(std::to_underlying(CodecType::Rle));
  EXPECT_EQ(Compress(functions, irs::WriteTier::Flush, disabled),
            CodecType::Uncompressed);
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
