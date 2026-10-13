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
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <iresearch/formats/column/col_copy_check.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "gtest/gtest.h"

namespace {

constexpr irs::field_id kText = 1;
constexpr irs::field_id kNumber = 2;
constexpr irs::field_id kList = 3;
constexpr std::string_view kSegment = "segment";
constexpr uint32_t kRowGroup = 4096;

duckdb::DatabaseInstance& Db() {
  return irs::DuckDBEngine::Instance().instance();
}

std::string Text(uint64_t row) {
  static constexpr std::string_view kServices[] = {"checkout", "frontend",
                                                   "cart", "shipping"};
  return absl::StrCat("2026-10-12T23:", row % 60, " INFO ", kServices[row % 4],
                      " handled request id=", row * 7919 % 100003, " took ",
                      row % 997, "ms");
}

struct Written {
  duckdb::CompressionType text = duckdb::CompressionType::COMPRESSION_AUTO;
  irs::WriteTier tier = irs::WriteTier::Merge;
  uint64_t rows = 6 * kRowGroup;
  bool nulls = false;
  bool list = false;
};

irs::FunctionFieldOptions AutoOptions(uint32_t row_group = kRowGroup) {
  return irs::FunctionFieldOptions{
    [](irs::field_id) { return irs::ColumnOptions{}; }, nullptr, row_group};
}

irs::FunctionFieldOptions UncompressedText() {
  return irs::FunctionFieldOptions{
    [](irs::field_id id) {
      if (id != kText) {
        return irs::ColumnOptions{};
      }
      return irs::ColumnOptions{
        .compression = duckdb::CompressionType::COMPRESSION_UNCOMPRESSED};
    },
    nullptr, kRowGroup};
}

class ColCopyCheckTest : public ::testing::Test {
 protected:
  void Write(const Written& spec) {
    irs::ColWriter writer{_dir, kSegment, Db(), spec.tier};
    const irs::ColCodecParams params{.tier = spec.tier};
    auto& text = writer.OpenColumn(kText, duckdb::LogicalType::VARCHAR,
                                   /*skip_validity=*/false, kRowGroup,
                                   spec.text, /*hyperloglog=*/false, params);
    auto& number = writer.OpenColumn(
      kNumber, duckdb::LogicalType::BIGINT, /*skip_validity=*/false, kRowGroup,
      duckdb::CompressionType::COMPRESSION_AUTO, /*hyperloglog=*/false, params);
    const auto list_type =
      duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER);
    auto* list =
      spec.list ? &writer.OpenColumn(kList, list_type, /*skip_validity=*/false,
                                     kRowGroup,
                                     duckdb::CompressionType::COMPRESSION_AUTO,
                                     /*hyperloglog=*/false, params)
                : nullptr;
    for (uint64_t pos = 0; pos < spec.rows; pos += STANDARD_VECTOR_SIZE) {
      const auto take =
        std::min<uint64_t>(spec.rows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector strings{duckdb::LogicalType::VARCHAR,
                             STANDARD_VECTOR_SIZE};
      duckdb::Vector numbers{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      auto* s = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(strings);
      auto* n = duckdb::FlatVector::GetDataMutable<int64_t>(numbers);
      auto& validity = duckdb::FlatVector::ValidityMutable(strings);
      validity.Reset(STANDARD_VECTOR_SIZE);
      for (uint64_t k = 0; k < take; ++k) {
        const auto row = pos + k;
        if (spec.nulls && row % 7 == 0) {
          validity.SetInvalid(k);
        } else {
          s[k] = duckdb::StringVector::AddString(strings, Text(row));
        }
        n[k] = static_cast<int64_t>(row * row % 1000003);
      }
      duckdb::FlatVector::SetSize(strings, take);
      duckdb::FlatVector::SetSize(numbers, take);
      text.Append(strings, take);
      number.Append(numbers, take);
      if (list) {
        duckdb::Vector lists{list_type, STANDARD_VECTOR_SIZE};
        auto* entries =
          duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(lists);
        duckdb::ListVector::Reserve(lists, take);
        auto& child = duckdb::ListVector::GetChildMutable(lists);
        auto* values = duckdb::FlatVector::GetDataMutable<int32_t>(child);
        for (uint64_t k = 0; k < take; ++k) {
          entries[k] = duckdb::list_entry_t{k, 1};
          values[k] = static_cast<int32_t>(pos + k);
        }
        duckdb::ListVector::SetListSize(lists, take);
        duckdb::FlatVector::SetSize(lists, take);
        list->Append(lists, take);
      }
    }
    ASSERT_TRUE(writer.Commit(spec.rows));
    _reader = std::make_unique<irs::ColReader>(_dir, kSegment, Db());
  }

  const irs::ColumnReader& Column(irs::field_id id) const {
    const auto* column = _reader->Column(id);
    EXPECT_NE(nullptr, column);
    return *column;
  }

  std::optional<irs::ReencodeEstimate> Estimate(
    irs::field_id id, const irs::IndexFieldOptions& options,
    size_t samples = irs::kReencodeSampleRowGroups) const {
    return irs::EstimateReencode(*_reader, Column(id), options, samples);
  }

  irs::MemoryDirectory _dir;
  std::unique_ptr<irs::ColReader> _reader;
};

double Ratio(const irs::ReencodeEstimate& estimate) {
  return static_cast<double>(estimate.reencoded_bytes) /
         static_cast<double>(estimate.stored_bytes);
}

TEST_F(ColCopyCheckTest, AColumnStoredUncompressedPaysToReencode) {
  Write({.text = duckdb::CompressionType::COMPRESSION_UNCOMPRESSED});
  const auto options = AutoOptions();
  const auto estimate = Estimate(kText, options);
  ASSERT_TRUE(estimate);
  EXPECT_LT(Ratio(*estimate), 0.5);
}

TEST_F(ColCopyCheckTest, AColumnAMergeWroteDoesNotPayToReencode) {
  Write({.nulls = true});
  const auto options = AutoOptions();
  for (const auto id : {kText, kNumber}) {
    SCOPED_TRACE(id);
    const auto estimate = Estimate(id, options);
    ASSERT_TRUE(estimate);
    EXPECT_GT(Ratio(*estimate), 0.9);
    EXPECT_FALSE(estimate->codec_changed);
  }
}

TEST_F(ColCopyCheckTest, ADeclaredCodecTheBlocksDoNotUseIsAChange) {
  Write({});
  const auto options = UncompressedText();
  const auto text = Estimate(kText, options);
  ASSERT_TRUE(text);
  EXPECT_TRUE(text->codec_changed);
  EXPECT_GT(Ratio(*text), 1.0);
  const auto number = Estimate(kNumber, options);
  ASSERT_TRUE(number);
  EXPECT_FALSE(number->codec_changed);
}

TEST_F(ColCopyCheckTest, ADeclaredCodecTheBlocksUseIsNoChange) {
  Write({.text = duckdb::CompressionType::COMPRESSION_UNCOMPRESSED});
  const auto options = UncompressedText();
  const auto estimate = Estimate(kText, options);
  ASSERT_TRUE(estimate);
  EXPECT_FALSE(estimate->codec_changed);
  EXPECT_GT(Ratio(*estimate), 0.9);
}

TEST_F(ColCopyCheckTest, SamplesCoverEveryRowGroupOfASmallColumn) {
  Write({.rows = kRowGroup + kRowGroup / 2});
  const auto options = AutoOptions();
  const auto estimate = Estimate(kNumber, options);
  ASSERT_TRUE(estimate);
  EXPECT_EQ(irs::StoredBytes(Column(kNumber)), estimate->stored_bytes);
}

TEST_F(ColCopyCheckTest, SampledRowsNeedNotLineUpWithStoredBlocks) {
  Write({.rows = 10 * kRowGroup});
  const auto options = AutoOptions(3 * kRowGroup / 2);
  const auto estimate = Estimate(kText, options);
  ASSERT_TRUE(estimate);
  EXPECT_GT(estimate->stored_bytes, 0U);
  EXPECT_LT(estimate->stored_bytes, irs::StoredBytes(Column(kText)));
}

TEST_F(ColCopyCheckTest, NestedColumnsAndNoSamplesAreNotChecked) {
  Write({.list = true});
  const auto options = AutoOptions();
  EXPECT_FALSE(Estimate(kList, options));
  EXPECT_GT(irs::StoredBytes(Column(kList)), 0U);
  EXPECT_FALSE(Estimate(kText, options, /*samples=*/0));
}

TEST_F(ColCopyCheckTest, EstimatesAreCachedPerOptions) {
  Write({});
  const auto options = AutoOptions();
  const auto first = Estimate(kNumber, options);
  ASSERT_TRUE(first);
  irs::ReencodeKey key{
    .codec_params = irs::ColCodecParams{.tier = irs::WriteTier::Merge},
    .row_group_size = kRowGroup,
    .samples = irs::kReencodeSampleRowGroups,
  };
  EXPECT_EQ(first, _reader->Reencodes().Find(kNumber, key));
  EXPECT_FALSE(_reader->Reencodes().Find(kText, key));

  const irs::ReencodeEstimate planted{.stored_bytes = 1, .reencoded_bytes = 2};
  _reader->Reencodes().Store(kNumber, key, planted);
  EXPECT_EQ(planted, Estimate(kNumber, options));

  key.samples = 1;
  EXPECT_FALSE(_reader->Reencodes().Find(kNumber, key));
  const auto single = Estimate(kNumber, options, /*samples=*/1);
  ASSERT_TRUE(single);
  EXPECT_NE(planted, *single);
  EXPECT_EQ(single, _reader->Reencodes().Find(kNumber, key));
}

}  // namespace
