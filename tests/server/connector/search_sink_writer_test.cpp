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

#include <velox/vector/tests/utils/VectorTestBase.h>

#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/search/scorers.hpp>
#include <iresearch/store/memory_directory.hpp>

#include "catalog/table_options.h"
#include "connector/common.h"
#include "connector/primary_key.hpp"
#include "connector/search_remove_filter.hpp"
#include "connector/search_sink_writer.hpp"
#include "gtest/gtest.h"
#include "iresearch/utils/bytes_utils.hpp"

namespace sdb::connector::search {

class SearchSinkWriterTest : public ::testing::Test,
                             public velox::test::VectorTestBase {
 public:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
    // TODO(Dronplane): make it to the main function of tests
    // while running this many times makes no harm but is redundant
    irs::analysis::analyzers::Init();
    irs::formats::Init();
    irs::scorers::Init();
    irs::compression::Init();
  }

  void SetUp() final {
    auto column_info_provider = [](const std::string_view&) {
      return irs::ColumnInfo{
        .compression = irs::Type<irs::compression::None>::get(),
        .options = {},
        .encryption = false,
        .track_prev_doc = false};
    };

    auto feature_provider = [](irs::IndexFeatures) {
      return std::make_pair(
        irs::ColumnInfo{.compression = irs::Type<irs::compression::None>::get(),
                        .options = {},
                        .encryption = false,
                        .track_prev_doc = false},
        irs::FeatureWriterFactory{});
    };
    irs::IndexWriterOptions options;
    options.column_info = column_info_provider;
    options.features = feature_provider;
    _codec = irs::formats::Get("1_5avx");
    _data_writer =
      irs::IndexWriter::Make(_dir, _codec, irs::kOmCreate, options);
  }

  void TearDown() final { _data_writer.reset(); }

 protected:
  irs::Format::ptr _codec;
  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _data_writer;
};

TEST_F(SearchSinkWriterTest, InsertDeleteMultipleColumns) {
  auto trx = _data_writer->GetBatch();

  SearchSinkInsertWriter sink{trx};
  sink.Init(4);

  const std::vector<sdb::catalog::Column::Id> col_id{1, 2, 3, 4, 5};
  const std::vector<std::string_view> pk{
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk1", 19},
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x2pk2", 19},
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x3pk3", 19},
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x4pk4", 19}};
  const std::vector<std::string_view> string_data{
    std::string_view{"\x0rrr", 4}, std::string_view{"\x0", 1},
    std::string_view{"abcdef", 6}, std::string_view{"\x0\x0", 2}};
  const std::vector<std::string_view> integer_data{
    std::string_view{"\x0\x0\x0\x0", 4}, std::string_view{"\x1\x0\x0\x0", 4},
    std::string_view{"\x2\x0\x0\x0", 4}, std::string_view{"\x3\x0\x0\x0", 4}};
  const std::vector<std::string_view> boolean_data{
    std::string_view{"\x0", 1}, std::string_view{"\x1", 1},
    std::string_view{"\x1", 1}, std::string_view{"\x0", 1}};
  const std::vector<std::string_view> huge_data{
    std::string_view{"\x5\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0", 16},
    std::string_view{"\x6\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0", 16},
    std::string_view{"\x7\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0", 16},
    std::string_view{"\x8\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0", 16}};
  const std::vector<std::string_view> big_data{
    std::string_view{"\x9\x0\x0\x0\x0\x0\x0\x0", 8},
    std::string_view{"\xa\x0\x0\x0\x0\x0\x0\x0", 8},
    std::string_view{"\xb\x0\x0\x0\x0\x0\x0\x0", 8},
    std::string_view{"\xc\x0\x0\x0\x0\x0\x0\x0", 8}};

  sink.SwitchColumn(velox::TypeKind::INTEGER, false, col_id[0]);
  sink.Write({rocksdb::Slice(integer_data[0])}, pk[0]);
  sink.Write({rocksdb::Slice(integer_data[1])}, pk[1]);
  sink.Write({rocksdb::Slice(integer_data[2])}, pk[2]);
  sink.Write({rocksdb::Slice(integer_data[3])}, pk[3]);
  sink.SwitchColumn(velox::TypeKind::VARCHAR, false, col_id[1]);
  sink.Write({rocksdb::Slice(string_data[0])}, pk[0]);
  sink.Write({rocksdb::Slice(string_data[1])}, pk[1]);
  sink.Write({rocksdb::Slice(string_data[2])}, pk[2]);
  sink.Write({rocksdb::Slice(string_data[3])}, pk[3]);
  sink.SwitchColumn(velox::TypeKind::BOOLEAN, false, col_id[2]);
  sink.Write({rocksdb::Slice(boolean_data[0])}, pk[0]);
  sink.Write({rocksdb::Slice(boolean_data[1])}, pk[1]);
  sink.Write({rocksdb::Slice(boolean_data[2])}, pk[2]);
  sink.Write({rocksdb::Slice(boolean_data[3])}, pk[3]);
  sink.SwitchColumn(velox::TypeKind::HUGEINT, false, col_id[3]);
  sink.Write({rocksdb::Slice(huge_data[0])}, pk[0]);
  sink.Write({rocksdb::Slice(huge_data[1])}, pk[1]);
  sink.Write({rocksdb::Slice(huge_data[2])}, pk[2]);
  sink.Write({rocksdb::Slice(huge_data[3])}, pk[3]);
  sink.SwitchColumn(velox::TypeKind::BIGINT, false, col_id[4]);
  sink.Write({rocksdb::Slice(big_data[0])}, pk[0]);
  sink.Write({rocksdb::Slice(big_data[1])}, pk[1]);
  sink.Write({rocksdb::Slice(big_data[2])}, pk[2]);
  sink.Write({rocksdb::Slice(big_data[3])}, pk[3]);
  sink.Finish();
  ASSERT_TRUE(trx.Commit());
  _data_writer->Commit();

  auto validate_row = [](const irs::SubReader& segment, std::string_view pk,
                         int32_t col1, std::string_view col2, bool col3,
                         __int128_t col4, int64_t col5) {
    const auto* pk_column =
      segment.column(sdb::connector::search::kPkFieldName);
    ASSERT_NE(nullptr, pk_column);
    auto pk_values_itr = pk_column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, pk_values_itr);
    auto* actual_pk_value = irs::get<irs::PayAttr>(*pk_values_itr);
    ASSERT_NE(nullptr, actual_pk_value);
    auto int32_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x02", 9});
    ASSERT_NE(nullptr, int32_terms);
    auto varchar_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x02\x03", 9});
    ASSERT_NE(nullptr, varchar_terms);
    auto bool_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x03\x01", 9});
    ASSERT_NE(nullptr, bool_terms);
    auto huge_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x04\x02", 9});
    ASSERT_NE(nullptr, huge_terms);
    auto big_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x05\x02", 9});
    ASSERT_NE(nullptr, big_terms);

    irs::NumericTokenizer num_stream;
    const auto* num_token = irs::get<irs::TermAttr>(num_stream);
    ASSERT_TRUE(num_token);
    irs::BooleanTokenizer bool_stream;
    const auto* bool_token = irs::get<irs::TermAttr>(bool_stream);
    ASSERT_TRUE(bool_token);
    SCOPED_TRACE(absl::StrCat("validating pk=", pk));
    auto varchar_term_itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(varchar_term_itr->seek(irs::ViewCast<irs::byte_type>(col2)));
    auto varchar_postings =
      segment.mask(varchar_term_itr->postings(irs::IndexFeatures::None));
    num_stream.reset(col1);
    ASSERT_TRUE(num_stream.next());
    auto int32_term_itr = int32_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(int32_term_itr->seek(num_token->value));
    auto int32_postings =
      segment.mask(int32_term_itr->postings(irs::IndexFeatures::None));
    num_stream.reset(static_cast<double>(col4));
    ASSERT_TRUE(num_stream.next());
    auto huge_term_itr = huge_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(huge_term_itr->seek(num_token->value));
    auto huge_postings =
      segment.mask(huge_term_itr->postings(irs::IndexFeatures::None));
    num_stream.reset(col5);
    ASSERT_TRUE(num_stream.next());
    auto big_term_itr = big_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(big_term_itr->seek(num_token->value));
    auto big_postings =
      segment.mask(big_term_itr->postings(irs::IndexFeatures::None));
    bool_stream.reset(col3);
    ASSERT_TRUE(bool_stream.next());
    auto bool_term_itr = bool_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(bool_term_itr->seek(bool_token->value));
    auto bool_postings =
      segment.mask(bool_term_itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(int32_postings->next());
    ASSERT_TRUE(varchar_postings->next());
    ASSERT_TRUE(huge_postings->next());
    ASSERT_TRUE(big_postings->next());
    ASSERT_EQ(big_postings->value(), varchar_postings->value());
    ASSERT_EQ(huge_postings->value(), varchar_postings->value());
    ASSERT_EQ(int32_postings->value(), varchar_postings->value());
    // Bools are not unique in each row so checking with seek that our row has
    // expected value
    ASSERT_TRUE(bool_postings->seek(int32_postings->value()));
    ASSERT_EQ(int32_postings->value(), bool_postings->value());
    ASSERT_EQ(varchar_postings->value(),
              pk_values_itr->seek(varchar_postings->value()));
    ASSERT_EQ(pk, irs::ViewCast<char>(actual_pk_value->value));
    ASSERT_FALSE(varchar_postings->next());
    ASSERT_FALSE(int32_postings->next());
    ASSERT_FALSE(huge_postings->next());
    ASSERT_FALSE(big_postings->next());
  };
  {
    auto reader = irs::DirectoryReader(_dir, _codec);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(4, reader.docs_count());
    ASSERT_EQ(4, reader.live_docs_count());

    validate_row(reader[0], "pk1", 0, string_data[0].substr(1), false, 5, 9);
    validate_row(reader[0], "pk2", 1, string_data[1].substr(1), true, 6, 10);
    validate_row(reader[0], "pk3", 2, string_data[2], true, 7, 11);
    validate_row(reader[0], "pk4", 3, string_data[3].substr(1), false, 8, 12);
  }

  // Delete rows
  auto delete_trx = _data_writer->GetBatch();

  {
    // in local block to make sure remove filters ownership is properly
    // transferred
    SearchSinkDeleteWriter delete_sink{delete_trx};
    delete_sink.Init(2);
    delete_sink.DeleteRow("pk2");
    delete_sink.DeleteRow("pk4");
    delete_sink.Finish();
    ASSERT_TRUE(delete_trx.Commit());
  }
  _data_writer->Commit();

  {
    auto reader = irs::DirectoryReader(_dir, _codec);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(4, reader.docs_count());
    ASSERT_EQ(2, reader.live_docs_count());

    validate_row(reader[0], "pk1", 0, string_data[0].substr(1), false, 5, 9);
    validate_row(reader[0], "pk3", 2, string_data[2], true, 7, 11);
  }
}

TEST_F(SearchSinkWriterTest, InsertNullsColumns) {
  auto trx = _data_writer->GetBatch();

  const std::vector<sdb::catalog::Column::Id> col_id{1, 2};
  const std::vector<std::string_view> pk{
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk1", 19},
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x2pk2", 19},
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x3pk3", 19},
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x4pk4", 19}};

  const std::vector<std::string_view> string_data{std::string_view{"foo", 3},
                                                  std::string_view{"bar", 3}};

  SearchSinkInsertWriter sink{trx};
  sink.Init(4);

  sink.SwitchColumn(velox::TypeKind::VARCHAR, true, col_id[0]);
  sink.Write({rocksdb::Slice(string_data[0])}, pk[0]);
  sink.Write({{}}, pk[1]);  // null
  sink.Write({rocksdb::Slice(string_data[1])}, pk[2]);
  sink.Write({{}}, pk[3]);  // null
  sink.SwitchColumn(velox::TypeKind::UNKNOWN, true, col_id[1]);
  sink.Write({{}}, pk[0]);  // null
  sink.Write({{}}, pk[1]);  // null
  sink.Write({{}}, pk[2]);  // null
  sink.Write({{}}, pk[3]);  // null
  sink.Finish();
  ASSERT_TRUE(trx.Commit());
  _data_writer->Commit();

  auto reader = irs::DirectoryReader(_dir, _codec);
  ASSERT_EQ(1, reader.size());
  ASSERT_EQ(4, reader.docs_count());
  ASSERT_EQ(4, reader.live_docs_count());
  auto& segment = reader[0];
  const auto* pk_column = segment.column(sdb::connector::search::kPkFieldName);
  ASSERT_NE(nullptr, pk_column);
  auto varchar_terms =
    segment.field(std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9});
  ASSERT_NE(nullptr, varchar_terms);
  auto varchar_nulls =
    segment.field(std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x00", 9});
  ASSERT_NE(nullptr, varchar_nulls);
  auto unknown_terms =
    segment.field(std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x02\x00", 9});
  ASSERT_NE(nullptr, unknown_terms);

  // Row 1   foo, NULL
  {
    auto pk_values_itr = pk_column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, pk_values_itr);
    auto* actual_pk_value = irs::get<irs::PayAttr>(*pk_values_itr);
    ASSERT_NE(nullptr, actual_pk_value);
    auto varchar_terms_itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_NE(nullptr, varchar_terms_itr);
    ASSERT_TRUE(
      varchar_terms_itr->seek(irs::ViewCast<irs::byte_type>(string_data[0])));
    // We have some nulls so term should be present
    auto varchar_nulls_itr = varchar_nulls->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(varchar_nulls_itr->next());
    auto varchar_postings =
      varchar_terms_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(varchar_postings->next());
    ASSERT_EQ(varchar_postings->value(),
              pk_values_itr->seek(varchar_postings->value()));
    ASSERT_EQ("pk1", irs::ViewCast<char>(actual_pk_value->value));
    auto varchar_nulls_postings =
      varchar_nulls_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(varchar_nulls_postings->seek(varchar_postings->value()));
    // NULL is not in this row
    ASSERT_NE(varchar_nulls_postings->value(), varchar_postings->value());
    auto unknown_terms_itr = unknown_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(unknown_terms_itr->next());
    auto unknown_postings =
      unknown_terms_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(unknown_postings->seek(varchar_postings->value()));
    ASSERT_EQ(varchar_postings->value(), unknown_postings->value());
    ASSERT_FALSE(varchar_postings->next());
  }
  // Row 2  NULL, NULL
  {
    auto pk_values_itr = pk_column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, pk_values_itr);
    auto* actual_pk_value = irs::get<irs::PayAttr>(*pk_values_itr);
    ASSERT_NE(nullptr, actual_pk_value);
    // Find expected PK so we know document id for this row
    irs::doc_id_t row_doc_id = irs::doc_limits::invalid();
    while (pk_values_itr->next()) {
      if (irs::ViewCast<char>(actual_pk_value->value) == "pk2") {
        row_doc_id = pk_values_itr->value();
        break;
      }
    }
    ASSERT_TRUE(irs::doc_limits::valid(row_doc_id));
    // We have some nulls so term should be present
    auto varchar_nulls_itr = varchar_nulls->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(varchar_nulls_itr->next());
    auto varchar_nulls_postings =
      varchar_nulls_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(varchar_nulls_postings->seek(row_doc_id));
    ASSERT_EQ(varchar_nulls_postings->value(), row_doc_id);
    auto unknown_terms_itr = unknown_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(unknown_terms_itr->next());
    auto unknown_postings =
      unknown_terms_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(unknown_postings->seek(row_doc_id));
    ASSERT_EQ(unknown_postings->value(), row_doc_id);
  }
  // Row 3 bar, null
  {
    auto pk_values_itr = pk_column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, pk_values_itr);
    auto* actual_pk_value = irs::get<irs::PayAttr>(*pk_values_itr);
    ASSERT_NE(nullptr, actual_pk_value);
    auto varchar_terms_itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_NE(nullptr, varchar_terms_itr);
    ASSERT_TRUE(
      varchar_terms_itr->seek(irs::ViewCast<irs::byte_type>(string_data[1])));
    // We have some nulls so term should be present
    auto varchar_nulls_itr = varchar_nulls->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(varchar_nulls_itr->next());
    auto varchar_postings =
      varchar_terms_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(varchar_postings->next());
    ASSERT_EQ(varchar_postings->value(),
              pk_values_itr->seek(varchar_postings->value()));
    ASSERT_EQ("pk3", irs::ViewCast<char>(actual_pk_value->value));
    auto varchar_nulls_postings =
      varchar_nulls_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(varchar_nulls_postings->seek(varchar_postings->value()));
    // NULL is not in this row
    ASSERT_NE(varchar_nulls_postings->value(), varchar_postings->value());

    auto unknown_terms_itr = unknown_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(unknown_terms_itr->next());
    auto unknown_postings =
      unknown_terms_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(unknown_postings->seek(varchar_postings->value()));
    ASSERT_EQ(varchar_postings->value(), unknown_postings->value());
    ASSERT_FALSE(varchar_postings->next());
  }
  // Row 4  NULL, NULL
  {
    auto pk_values_itr = pk_column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, pk_values_itr);
    auto* actual_pk_value = irs::get<irs::PayAttr>(*pk_values_itr);
    ASSERT_NE(nullptr, actual_pk_value);
    // Find expected PK so we know document id for this row
    irs::doc_id_t row_doc_id = irs::doc_limits::invalid();
    while (pk_values_itr->next()) {
      if (irs::ViewCast<char>(actual_pk_value->value) == "pk4") {
        row_doc_id = pk_values_itr->value();
        break;
      }
    }
    ASSERT_TRUE(irs::doc_limits::valid(row_doc_id));
    // We have some nulls so term should be present
    auto varchar_nulls_itr = varchar_nulls->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(varchar_nulls_itr->next());
    auto varchar_nulls_postings =
      varchar_nulls_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(varchar_nulls_postings->seek(row_doc_id));
    ASSERT_EQ(varchar_nulls_postings->value(), row_doc_id);
    auto unknown_terms_itr = unknown_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(unknown_terms_itr->next());
    auto unknown_postings =
      unknown_terms_itr->postings(irs::IndexFeatures::None);
    ASSERT_TRUE(unknown_postings->seek(row_doc_id));
    ASSERT_EQ(unknown_postings->value(), row_doc_id);
  }
}

// corner case for string encoding in values
TEST_F(SearchSinkWriterTest, InsertStringPrefix) {
  auto trx = _data_writer->GetBatch();

  SearchSinkInsertWriter sink{trx};
  sink.Init(1);
  const sdb::catalog::Column::Id col_id = 1;
  const std::vector<std::string_view> pk{
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk1", 19},
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x2pk2", 19},
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x3pk3", 19},
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x4pk4", 19}};

  sink.SwitchColumn(velox::TypeKind::VARCHAR, false, col_id);
  sink.Write({rocksdb::Slice("\x0", 1), rocksdb::Slice("\x0foo", 4)}, pk[0]);

  sink.Finish();
  ASSERT_TRUE(trx.Commit());
  _data_writer->Commit();
  auto reader = irs::DirectoryReader(_dir, _codec);
  ASSERT_EQ(1, reader.size());
  ASSERT_EQ(1, reader.docs_count());
  ASSERT_EQ(1, reader.live_docs_count());
  auto& segment = reader[0];
  const auto* pk_column = segment.column(sdb::connector::search::kPkFieldName);
  ASSERT_NE(nullptr, pk_column);
  auto pk_values_itr = pk_column->iterator(irs::ColumnHint::Normal);
  ASSERT_NE(nullptr, pk_values_itr);
  auto* actual_pk_value = irs::get<irs::PayAttr>(*pk_values_itr);
  ASSERT_NE(nullptr, actual_pk_value);

  auto varchar_terms =
    segment.field(std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9});
  ASSERT_NE(nullptr, varchar_terms);
  auto varchar_terms_itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
  ASSERT_NE(nullptr, varchar_terms_itr);
  ASSERT_TRUE(varchar_terms_itr->seek(
    irs::ViewCast<irs::byte_type>(std::string_view{"\x0foo", 4})));

  auto varchar_postings = varchar_terms_itr->postings(irs::IndexFeatures::None);
  ASSERT_TRUE(varchar_postings->next());
  ASSERT_EQ(varchar_postings->value(),
            pk_values_itr->seek(varchar_postings->value()));
  ASSERT_EQ("pk1", irs::ViewCast<char>(actual_pk_value->value));
}

TEST_F(SearchSinkWriterTest, InsertDeleteInsertWithExisting) {
  constexpr std::string_view kPk = {
    "\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk1", 19};
  constexpr std::string_view kPk2 = {
    "\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk2", 19};
  {
    auto trx = _data_writer->GetBatch();
    SearchSinkInsertWriter sink{trx};
    sink.Init(2);
    sink.SwitchColumn(velox::TypeKind::VARCHAR, false, 1);
    sink.Write({rocksdb::Slice("value1", 6)}, kPk);
    // second document to keep segment around
    sink.Write({rocksdb::Slice("value9", 6)}, kPk2);
    sink.Finish();
    trx.Commit();
    // That would be our "existing" segment
    _data_writer->Commit();
  }
  {
    auto delete_trx = _data_writer->GetBatch();
    SearchSinkDeleteWriter delete_sink{delete_trx};
    delete_sink.Init(1);
    delete_sink.DeleteRow("pk1");
    delete_sink.Finish();
    ASSERT_TRUE(delete_trx.Commit());
  }
  {
    auto trx = _data_writer->GetBatch();
    SearchSinkInsertWriter sink{trx};
    sink.Init(1);
    sink.SwitchColumn(velox::TypeKind::VARCHAR, false, 1);
    sink.Write({rocksdb::Slice("value2", 6)}, kPk);
    sink.Finish();
    trx.Commit();
    // Intentionally do not commit data writer to force several same PKs in one
    // writer commit
  }
  {
    auto delete_trx = _data_writer->GetBatch();
    SearchSinkDeleteWriter delete_sink{delete_trx};
    delete_sink.Init(1);
    delete_sink.DeleteRow("pk1");
    delete_sink.Finish();
    ASSERT_TRUE(delete_trx.Commit());
    // still no data writer commit
  }
  {
    auto trx = _data_writer->GetBatch();
    SearchSinkInsertWriter sink{trx};
    sink.Init(1);
    sink.SwitchColumn(velox::TypeKind::VARCHAR, false, 1);
    sink.Write({rocksdb::Slice("value3", 6)}, kPk);
    sink.Finish();
    trx.Commit();
    // eventually commit. value3 would be visible
    _data_writer->Commit();
  }
  auto reader = irs::DirectoryReader(_dir, _codec);
  ASSERT_EQ(2, reader.size());
  ASSERT_EQ(4, reader.docs_count());
  ASSERT_EQ(2, reader.live_docs_count());
  {
    auto& segment = reader[1];
    const auto* pk_column =
      segment.column(sdb::connector::search::kPkFieldName);
    ASSERT_NE(nullptr, pk_column);
    auto varchar_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9});
    ASSERT_NE(nullptr, varchar_terms);
    auto itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(
      itr->seek(irs::ViewCast<irs::byte_type>(std::string_view{"value3", 6})));
    auto postings = segment.mask(itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(postings->next());
    auto pk_itr = pk_column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, pk_itr);
    auto* actual_pk_value = irs::get<irs::PayAttr>(*pk_itr);
    ASSERT_NE(nullptr, actual_pk_value);
    ASSERT_EQ(postings->value(), pk_itr->seek(postings->value()));
    ASSERT_EQ("pk1", irs::ViewCast<char>(actual_pk_value->value));
    ASSERT_FALSE(postings->next());
  }
  // check deleted
  {
    auto& segment = reader[1];
    auto varchar_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9});
    ASSERT_NE(nullptr, varchar_terms);

    auto itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(
      itr->seek(irs::ViewCast<irs::byte_type>(std::string_view{"value2", 6})));
    auto postings = segment.mask(itr->postings(irs::IndexFeatures::None));
    ASSERT_FALSE(postings->next());
  }
  {
    auto& segment = reader[0];
    auto varchar_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9});
    ASSERT_NE(nullptr, varchar_terms);

    auto itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(
      itr->seek(irs::ViewCast<irs::byte_type>(std::string_view{"value1", 6})));
    auto postings = segment.mask(itr->postings(irs::IndexFeatures::None));
    ASSERT_FALSE(postings->next());
  }
}

TEST_F(SearchSinkWriterTest, InsertDeleteInsertOnePending) {
  constexpr std::string_view kPk = {
    "\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk1", 19};
  {
    auto trx = _data_writer->GetBatch();
    SearchSinkInsertWriter sink{trx};
    sink.Init(1);
    sink.SwitchColumn(velox::TypeKind::VARCHAR, false, 1);
    sink.Write({rocksdb::Slice("value1", 6)}, kPk);
    sink.Finish();
    trx.Commit();
    // Intentionally do not commit data writer to force several same PKs in one
    // writer commit
  }
  {
    auto delete_trx = _data_writer->GetBatch();
    SearchSinkDeleteWriter delete_sink{delete_trx};
    delete_sink.Init(1);
    delete_sink.DeleteRow("pk1");
    delete_sink.Finish();
    ASSERT_TRUE(delete_trx.Commit());
    // still no data writer commit
  }
  {
    auto trx = _data_writer->GetBatch();
    SearchSinkInsertWriter sink{trx};
    sink.Init(1);
    sink.SwitchColumn(velox::TypeKind::VARCHAR, false, 1);
    sink.Write({rocksdb::Slice("value2", 6)}, kPk);
    sink.Finish();
    trx.Commit();
    // Intentionally do not commit data writer to force several same PKs in one
    // writer commit
  }
  {
    auto delete_trx = _data_writer->GetBatch();
    SearchSinkDeleteWriter delete_sink{delete_trx};
    delete_sink.Init(1);
    delete_sink.DeleteRow("pk1");
    delete_sink.Finish();
    ASSERT_TRUE(delete_trx.Commit());
    // still no data writer commit
  }
  {
    auto trx = _data_writer->GetBatch();
    SearchSinkInsertWriter sink{trx};
    sink.Init(1);
    sink.SwitchColumn(velox::TypeKind::VARCHAR, false, 1);
    sink.Write({rocksdb::Slice("value3", 6)}, kPk);
    sink.Finish();
    trx.Commit();
    // eventually commit. value3 would be visible
    _data_writer->Commit();
  }
  auto reader = irs::DirectoryReader(_dir, _codec);
  ASSERT_EQ(1, reader.size());
  ASSERT_EQ(3, reader.docs_count());
  ASSERT_EQ(1, reader.live_docs_count());
  {
    auto& segment = reader[0];
    const auto* pk_column =
      segment.column(sdb::connector::search::kPkFieldName);
    ASSERT_NE(nullptr, pk_column);
    auto varchar_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9});
    ASSERT_NE(nullptr, varchar_terms);
    auto itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(
      itr->seek(irs::ViewCast<irs::byte_type>(std::string_view{"value3", 6})));
    auto postings = segment.mask(itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(postings->next());
    auto pk_itr = pk_column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, pk_itr);
    auto* actual_pk_value = irs::get<irs::PayAttr>(*pk_itr);
    ASSERT_NE(nullptr, actual_pk_value);
    ASSERT_EQ(postings->value(), pk_itr->seek(postings->value()));
    ASSERT_EQ("pk1", irs::ViewCast<char>(actual_pk_value->value));
    ASSERT_FALSE(postings->next());
  }
  // check deleted
  {
    auto& segment = reader[0];
    auto varchar_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9});
    ASSERT_NE(nullptr, varchar_terms);
    {
      auto itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(itr->seek(
        irs::ViewCast<irs::byte_type>(std::string_view{"value2", 6})));
      auto postings = segment.mask(itr->postings(irs::IndexFeatures::None));
      ASSERT_FALSE(postings->next());
    }
    {
      auto itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(itr->seek(
        irs::ViewCast<irs::byte_type>(std::string_view{"value1", 6})));
      auto postings = segment.mask(itr->postings(irs::IndexFeatures::None));
      ASSERT_FALSE(postings->next());
    }
  }
}

TEST_F(SearchSinkWriterTest, InsertDeleteInsertOnePendingWithFlush) {
  irs::MemoryDirectory dir;
  auto column_info_provider = [](const std::string_view&) {
    return irs::ColumnInfo{
      .compression = irs::Type<irs::compression::None>::get(),
      .options = {},
      .encryption = false,
      .track_prev_doc = false};
  };

  auto feature_provider = [](irs::IndexFeatures) {
    return std::make_pair(
      irs::ColumnInfo{.compression = irs::Type<irs::compression::None>::get(),
                      .options = {},
                      .encryption = false,
                      .track_prev_doc = false},
      irs::FeatureWriterFactory{});
  };
  irs::IndexWriterOptions options;
  options.column_info = column_info_provider;
  options.features = feature_provider;
  // force writer to make flushes every 2 documents
  options.segment_docs_max = 2;
  // local block is needed as reader/writer should not outlive directory
  {
    auto limited_data_writer =
      irs::IndexWriter::Make(dir, _codec, irs::kOmCreate, options);
    constexpr std::string_view kPk = {
      "\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk1", 19};
    constexpr std::string_view kPk2 = {
      "\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk2", 19};
    constexpr std::string_view kPk3 = {
      "\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk3", 19};
    {
      auto trx = limited_data_writer->GetBatch();
      SearchSinkInsertWriter sink{trx};
      sink.Init(2);
      sink.SwitchColumn(velox::TypeKind::VARCHAR, false, 1);
      sink.Write({rocksdb::Slice("value1", 6)}, kPk);
      sink.Write({rocksdb::Slice("value8", 6)}, kPk3);
      sink.Finish();
      trx.Commit();
      // Intentionally do not commit data writer to force several same PKs in
      // one writer commit
    }
    {
      auto delete_trx = limited_data_writer->GetBatch();
      SearchSinkDeleteWriter delete_sink{delete_trx};
      delete_sink.Init(1);
      delete_sink.DeleteRow("pk1");
      delete_sink.Finish();
      ASSERT_TRUE(delete_trx.Commit());
      // still no data writer commit
    }
    {
      auto trx = limited_data_writer->GetBatch();
      SearchSinkInsertWriter sink{trx};
      sink.Init(2);
      sink.SwitchColumn(velox::TypeKind::VARCHAR, false, 1);
      sink.Write({rocksdb::Slice("value2", 6)}, kPk);
      // we need this doc to keep flushed segment from discarding as empty
      sink.Write({rocksdb::Slice("value22", 7)}, kPk2);
      sink.Finish();
      trx.Commit();
      // Intentionally do not commit data writer to force several same PKs in
      // one writer commit
    }
    {
      auto delete_trx = limited_data_writer->GetBatch();
      SearchSinkDeleteWriter delete_sink{delete_trx};
      delete_sink.Init(1);
      delete_sink.DeleteRow("pk1");
      delete_sink.Finish();
      ASSERT_TRUE(delete_trx.Commit());
      // still no data writer commit
    }
    {
      auto trx = limited_data_writer->GetBatch();
      SearchSinkInsertWriter sink{trx};
      sink.Init(1);
      sink.SwitchColumn(velox::TypeKind::VARCHAR, false, 1);
      sink.Write({rocksdb::Slice("value3", 6)}, kPk);
      sink.Finish();
      trx.Commit();
      // eventually commit. value3 would be visible
      limited_data_writer->Commit();
    }
    auto reader = irs::DirectoryReader(dir, _codec);
    ASSERT_EQ(3, reader.size());
    ASSERT_EQ(5, reader.docs_count());
    ASSERT_EQ(3, reader.live_docs_count());

    {
      auto& segment = reader[2];
      const auto* pk_column =
        segment.column(sdb::connector::search::kPkFieldName);
      ASSERT_NE(nullptr, pk_column);
      auto varchar_terms = segment.field(
        std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9});
      ASSERT_NE(nullptr, varchar_terms);
      auto itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(itr->seek(
        irs::ViewCast<irs::byte_type>(std::string_view{"value3", 6})));
      auto postings = segment.mask(itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(postings->next());
      auto pk_itr = pk_column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, pk_itr);
      auto* actual_pk_value = irs::get<irs::PayAttr>(*pk_itr);
      ASSERT_NE(nullptr, actual_pk_value);
      ASSERT_EQ(postings->value(), pk_itr->seek(postings->value()));
      ASSERT_EQ("pk1", irs::ViewCast<char>(actual_pk_value->value));
      ASSERT_FALSE(postings->next());
    }
    // check deleted
    {
      auto& segment = reader[0];
      auto varchar_terms = segment.field(
        std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9});
      ASSERT_NE(nullptr, varchar_terms);
      {
        auto itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
        ASSERT_TRUE(itr->seek(
          irs::ViewCast<irs::byte_type>(std::string_view{"value1", 6})));
        auto postings = segment.mask(itr->postings(irs::IndexFeatures::None));
        ASSERT_FALSE(postings->next());
      }
    }
    {
      auto& segment = reader[1];
      auto varchar_terms = segment.field(
        std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9});
      ASSERT_NE(nullptr, varchar_terms);
      {
        auto itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
        ASSERT_TRUE(itr->seek(
          irs::ViewCast<irs::byte_type>(std::string_view{"value2", 6})));
        auto postings = segment.mask(itr->postings(irs::IndexFeatures::None));
        ASSERT_FALSE(postings->next());
      }
    }
  }
}

TEST_F(SearchSinkWriterTest, DeleteNotMissedWithExisting) {
  constexpr std::string_view kPk = {
    "\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk1", 19};
  constexpr std::string_view kPk2 = {
    "\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk2", 19};
  {
    auto trx = _data_writer->GetBatch();
    SearchSinkInsertWriter sink{trx};
    sink.Init(2);
    sink.SwitchColumn(velox::TypeKind::VARCHAR, false, 1);
    sink.Write({rocksdb::Slice("value1", 6)}, kPk);
    // second document to keep segment around
    sink.Write({rocksdb::Slice("value9", 6)}, kPk2);
    sink.Finish();
    trx.Commit();
    // That would be our "existing" segment
    _data_writer->Commit();
  }
  {
    // this delete should not fire at value2 during new segment processing
    // and successfully delete value1.
    auto delete_trx = _data_writer->GetBatch();
    SearchSinkDeleteWriter delete_sink{delete_trx};
    delete_sink.Init(1);
    delete_sink.DeleteRow("pk1");
    delete_sink.Finish();
    ASSERT_TRUE(delete_trx.Commit());
  }
  {
    auto trx = _data_writer->GetBatch();
    SearchSinkInsertWriter sink{trx};
    sink.Init(1);
    sink.SwitchColumn(velox::TypeKind::VARCHAR, false, 1);
    sink.Write({rocksdb::Slice("value2", 6)}, kPk);
    sink.Finish();
    trx.Commit();
    _data_writer->Commit();
    // Intentionally do not commit data writer to force several same PKs in one
    // writer commit
  }
  auto reader = irs::DirectoryReader(_dir, _codec);
  ASSERT_EQ(2, reader.size());
  ASSERT_EQ(3, reader.docs_count());
  ASSERT_EQ(2, reader.live_docs_count());
  {
    auto& segment = reader[1];
    const auto* pk_column =
      segment.column(sdb::connector::search::kPkFieldName);
    ASSERT_NE(nullptr, pk_column);
    auto varchar_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9});
    ASSERT_NE(nullptr, varchar_terms);
    auto itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(
      itr->seek(irs::ViewCast<irs::byte_type>(std::string_view{"value2", 6})));
    auto postings = segment.mask(itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(postings->next());
    auto pk_itr = pk_column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, pk_itr);
    auto* actual_pk_value = irs::get<irs::PayAttr>(*pk_itr);
    ASSERT_NE(nullptr, actual_pk_value);
    ASSERT_EQ(postings->value(), pk_itr->seek(postings->value()));
    ASSERT_EQ("pk1", irs::ViewCast<char>(actual_pk_value->value));
    ASSERT_FALSE(postings->next());
  }
  // check deleted
  {
    auto& segment = reader[0];
    auto varchar_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9});
    ASSERT_NE(nullptr, varchar_terms);

    auto itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(
      itr->seek(irs::ViewCast<irs::byte_type>(std::string_view{"value1", 6})));
    auto postings = segment.mask(itr->postings(irs::IndexFeatures::None));
    ASSERT_FALSE(postings->next());
  }
}

}  // namespace sdb::connector::search
