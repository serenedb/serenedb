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
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/columnstore/column_reader.hpp>
#include <iresearch/columnstore/format.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/search/scorers.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/bytes_utils.hpp>

#include "basics/endian.h"
#include "catalog/table_options.h"
#include "connector/common.h"
#include "connector/duckdb_search_sink_writer.h"
#include "connector/search_remove_filter.hpp"
#include "connector/search_sink_writer.hpp"
#include "gtest/gtest.h"
#include "basics/duckdb_engine.h"

namespace {

using namespace sdb;
using namespace connector;

// Process-wide DuckDB instance, owned by sdb::DuckDBEngine. tests_main
// brings it up before RUN_ALL_TESTS and tears it down before main returns,
// so the lifetime envelope strictly covers every test body.
duckdb::DatabaseInstance& TestDb() {
  return ::sdb::DuckDBEngine::Instance().instance();
}

class DuckDBSearchSinkWriterTest : public ::testing::Test {
 public:
  static catalog::ColumnTokenizer AnalyzerProvider(catalog::Column::Id) {
    auto make_identity = [] {
      return std::string(vpack::Slice::emptyObjectSlice().startAs<char>(),
                         vpack::Slice::emptyObjectSlice().byteSize());
    };
    static catalog::Tokenizer gStringTokenizer(
      ObjectId{0}, ObjectId{12345}, "test_string_verbartim", {},
      DEFAULT_ROW_GROUP_SIZE, make_identity());
    auto tokenizer = gStringTokenizer.GetTokenizer();
    EXPECT_TRUE(tokenizer);
    return {.analyzer = *std::move(tokenizer),
            .features = irs::IndexFeatures::None};
  }

  static void SetUpTestCase() {
    // Running these multiple times does no harm but is redundant.
    irs::analysis::analyzers::Init();
    irs::formats::Init();
    irs::scorers::Init();
    irs::compression::Init();
  }

  void SetUp() final {
    irs::IndexWriterOptions options;
    options.db = &TestDb();
    options.reader_options.db = &TestDb();
    _codec = irs::formats::Get("1_5simd");
    _data_writer =
      irs::IndexWriter::Make(_dir, _codec, irs::kOmCreate, options);
  }

  void TearDown() final { _data_writer.reset(); }

 protected:
  irs::Format::ptr _codec;
  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _data_writer;
  // DuckDBSearchSink{Insert,Delete}Writer::Init takes a const DataChunk& but
  // the current implementations ignore it; a default-constructed chunk is
  // sufficient.
  duckdb::DataChunk _dummy_chunk;
};

TEST_F(DuckDBSearchSinkWriterTest, InsertDeleteMultipleColumns) {
  auto trx = _data_writer->GetBatch();
  const std::vector<catalog::Column::Id> col_id{
    catalog::Column::Id{1}, catalog::Column::Id{2}, catalog::Column::Id{3},
    catalog::Column::Id{4}, catalog::Column::Id{5}};
  DuckDBSearchSinkInsertWriter sink{trx, AnalyzerProvider, col_id};

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
  std::vector<std::string> real_data;
  std::string data;
  basics::StrResize(data, sizeof(float));
  absl::little_endian::Store<float>(data.data(), 5.);
  real_data.push_back(data);
  absl::little_endian::Store<float>(data.data(), 6.);
  real_data.push_back(data);
  absl::little_endian::Store<float>(data.data(), 7.);
  real_data.push_back(data);
  absl::little_endian::Store<float>(data.data(), 8.);
  real_data.push_back(data);

  const std::vector<std::string_view> big_data{
    std::string_view{"\x9\x0\x0\x0\x0\x0\x0\x0", 8},
    std::string_view{"\xa\x0\x0\x0\x0\x0\x0\x0", 8},
    std::string_view{"\xb\x0\x0\x0\x0\x0\x0\x0", 8},
    std::string_view{"\xc\x0\x0\x0\x0\x0\x0\x0", 8}};

  duckdb::Vector dummy_int{duckdb::LogicalType::INTEGER, 0};
  duckdb::Vector dummy_varchar{duckdb::LogicalType::VARCHAR, 0};
  duckdb::Vector dummy_bool{duckdb::LogicalType::BOOLEAN, 0};
  duckdb::Vector dummy_float{duckdb::LogicalType::FLOAT, 0};
  duckdb::Vector dummy_bigint{duckdb::LogicalType::BIGINT, 0};

  // First batch: rows 0 and 1
  sink.Init(2, _dummy_chunk);
  sink.SwitchColumn(
    ColumnDescriptor{col_id[0], catalog::ColumnStoreMode::kNormal,
                     duckdb::LogicalType::INTEGER, false},
    dummy_int, 0);
  sink.Write({rocksdb::Slice(integer_data[0])}, pk[0]);
  sink.Write({rocksdb::Slice(integer_data[1])}, pk[1]);
  sink.SwitchColumn(
    ColumnDescriptor{col_id[1], catalog::ColumnStoreMode::kNormal,
                     duckdb::LogicalType::VARCHAR, false},
    dummy_varchar, 0);
  sink.Write({rocksdb::Slice(string_data[0])}, pk[0]);
  sink.Write({rocksdb::Slice(string_data[1])}, pk[1]);
  sink.SwitchColumn(
    ColumnDescriptor{col_id[2], catalog::ColumnStoreMode::kNormal,
                     duckdb::LogicalType::BOOLEAN, false},
    dummy_bool, 0);
  sink.Write({rocksdb::Slice(boolean_data[0])}, pk[0]);
  sink.Write({rocksdb::Slice(boolean_data[1])}, pk[1]);
  sink.SwitchColumn(
    ColumnDescriptor{col_id[3], catalog::ColumnStoreMode::kNormal,
                     duckdb::LogicalType::FLOAT, false},
    dummy_float, 0);
  sink.Write({rocksdb::Slice(real_data[0])}, pk[0]);
  sink.Write({rocksdb::Slice(real_data[1])}, pk[1]);
  sink.SwitchColumn(
    ColumnDescriptor{col_id[4], catalog::ColumnStoreMode::kNormal,
                     duckdb::LogicalType::BIGINT, false},
    dummy_bigint, 0);
  sink.Write({rocksdb::Slice(big_data[0])}, pk[0]);
  sink.Write({rocksdb::Slice(big_data[1])}, pk[1]);
  sink.Finish();

  // Second batch: rows 2 and 3 - reusing the same sink (tests document reset)
  sink.Init(2, _dummy_chunk);
  sink.SwitchColumn(
    ColumnDescriptor{col_id[0], catalog::ColumnStoreMode::kNormal,
                     duckdb::LogicalType::INTEGER, false},
    dummy_int, 0);
  sink.Write({rocksdb::Slice(integer_data[2])}, pk[2]);
  sink.Write({rocksdb::Slice(integer_data[3])}, pk[3]);
  sink.SwitchColumn(
    ColumnDescriptor{col_id[1], catalog::ColumnStoreMode::kNormal,
                     duckdb::LogicalType::VARCHAR, false},
    dummy_varchar, 0);
  sink.Write({rocksdb::Slice(string_data[2])}, pk[2]);
  sink.Write({rocksdb::Slice(string_data[3])}, pk[3]);
  sink.SwitchColumn(
    ColumnDescriptor{col_id[2], catalog::ColumnStoreMode::kNormal,
                     duckdb::LogicalType::BOOLEAN, false},
    dummy_bool, 0);
  sink.Write({rocksdb::Slice(boolean_data[2])}, pk[2]);
  sink.Write({rocksdb::Slice(boolean_data[3])}, pk[3]);
  sink.SwitchColumn(
    ColumnDescriptor{col_id[3], catalog::ColumnStoreMode::kNormal,
                     duckdb::LogicalType::FLOAT, false},
    dummy_float, 0);
  sink.Write({rocksdb::Slice(real_data[2])}, pk[2]);
  sink.Write({rocksdb::Slice(real_data[3])}, pk[3]);
  sink.SwitchColumn(
    ColumnDescriptor{col_id[4], catalog::ColumnStoreMode::kNormal,
                     duckdb::LogicalType::BIGINT, false},
    dummy_bigint, 0);
  sink.Write({rocksdb::Slice(big_data[2])}, pk[2]);
  sink.Write({rocksdb::Slice(big_data[3])}, pk[3]);
  sink.Finish();
  ASSERT_TRUE(trx.Commit());
  _data_writer->RefreshCommit();

  auto validate_row = [](const irs::SubReader& segment, std::string_view pk,
                         int32_t col1, std::string_view col2, bool col3,
                         float col4, int64_t col5) {
    const auto* cs_reader = segment.CsReader();
    ASSERT_NE(nullptr, cs_reader);
    const auto* pk_column = cs_reader->Column(
      static_cast<irs::field_id>(catalog::Column::kGeneratedPKId));
    ASSERT_NE(nullptr, pk_column);
    irs::columnstore::ColumnReader::BlobPointReader pk_cursor{*cs_reader,
                                                              *pk_column};
    auto read_pk_at = [&](irs::doc_id_t doc_id) -> std::string_view {
      const auto bytes = pk_cursor.FetchDoc(doc_id);
      return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
    };
    auto int32_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x02", 9});
    ASSERT_NE(nullptr, int32_terms);
    auto varchar_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x02\x03", 9});
    ASSERT_NE(nullptr, varchar_terms);
    auto bool_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x03\x01", 9});
    ASSERT_NE(nullptr, bool_terms);
    auto real_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x04\x02", 9});
    ASSERT_NE(nullptr, real_terms);
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
    num_stream.reset(col4);
    ASSERT_TRUE(num_stream.next());
    auto real_term_itr = real_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(real_term_itr->seek(num_token->value));
    auto real_postings =
      segment.mask(real_term_itr->postings(irs::IndexFeatures::None));
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
    ASSERT_TRUE(real_postings->next());
    ASSERT_TRUE(big_postings->next());
    ASSERT_EQ(big_postings->value(), varchar_postings->value());
    ASSERT_EQ(real_postings->value(), varchar_postings->value());
    ASSERT_EQ(int32_postings->value(), varchar_postings->value());
    // Bools are not unique in each row so checking with seek that our row has
    // expected value
    ASSERT_TRUE(bool_postings->seek(int32_postings->value()));
    ASSERT_EQ(int32_postings->value(), bool_postings->value());
    ASSERT_EQ(pk, read_pk_at(varchar_postings->value()));
    ASSERT_FALSE(varchar_postings->next());
    ASSERT_FALSE(int32_postings->next());
    ASSERT_FALSE(real_postings->next());
    ASSERT_FALSE(big_postings->next());
  };
  {
    auto reader = irs::DirectoryReader(_dir, _codec, {.db = &TestDb()});
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
    DuckDBSearchSinkDeleteWriter delete_sink{delete_trx};
    delete_sink.Init(2, _dummy_chunk);
    delete_sink.DeleteRow("pk2");
    delete_sink.DeleteRow("pk4");
    delete_sink.Finish();
    ASSERT_TRUE(delete_trx.Commit());
  }
  _data_writer->RefreshCommit();

  {
    auto reader = irs::DirectoryReader(_dir, _codec, {.db = &TestDb()});
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(4, reader.docs_count());
    ASSERT_EQ(2, reader.live_docs_count());

    validate_row(reader[0], "pk1", 0, string_data[0].substr(1), false, 5, 9);
    validate_row(reader[0], "pk3", 2, string_data[2], true, 7, 11);
  }
}

TEST_F(DuckDBSearchSinkWriterTest, InsertNullsColumns) {
  auto trx = _data_writer->GetBatch();

  const std::vector<catalog::Column::Id> col_id{catalog::Column::Id{1},
                                                catalog::Column::Id{2}};
  const std::vector<std::string_view> pk{
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk1", 19},
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x2pk2", 19},
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x3pk3", 19},
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x4pk4", 19}};

  const std::vector<std::string_view> string_data{std::string_view{"foo", 3},
                                                  std::string_view{"bar", 3}};

  DuckDBSearchSinkInsertWriter sink{trx, AnalyzerProvider, col_id};
  sink.Init(4, _dummy_chunk);
  duckdb::Vector dummy_varchar{duckdb::LogicalType::VARCHAR, 0};
  duckdb::Vector dummy_sqlnull{duckdb::LogicalType::SQLNULL, 0};

  sink.SwitchColumn(
    ColumnDescriptor{col_id[0], catalog::ColumnStoreMode::kNormal,
                     duckdb::LogicalType::VARCHAR, true},
    dummy_varchar, 0);
  sink.Write({rocksdb::Slice(string_data[0])}, pk[0]);
  sink.Write({{}}, pk[1]);  // null
  sink.Write({rocksdb::Slice(string_data[1])}, pk[2]);
  sink.Write({{}}, pk[3]);  // null
  sink.SwitchColumn(
    ColumnDescriptor{col_id[1], catalog::ColumnStoreMode::kNormal,
                     duckdb::LogicalType::SQLNULL, true},
    dummy_sqlnull, 0);
  sink.Write({{}}, pk[0]);  // null
  sink.Write({{}}, pk[1]);  // null
  sink.Write({{}}, pk[2]);  // null
  sink.Write({{}}, pk[3]);  // null
  sink.Finish();
  ASSERT_TRUE(trx.Commit());
  _data_writer->RefreshCommit();

  auto reader = irs::DirectoryReader(_dir, _codec, {.db = &TestDb()});
  ASSERT_EQ(1, reader.size());
  ASSERT_EQ(4, reader.docs_count());
  ASSERT_EQ(4, reader.live_docs_count());
  auto& segment = reader[0];
  const auto* cs_reader = segment.CsReader();
  ASSERT_NE(nullptr, cs_reader);
  const auto* pk_column = cs_reader->Column(
    static_cast<irs::field_id>(catalog::Column::kGeneratedPKId));
  ASSERT_NE(nullptr, pk_column);
  irs::columnstore::ColumnReader::BlobPointReader pk_cursor{*cs_reader,
                                                            *pk_column};
  auto read_pk_at = [&](irs::doc_id_t doc_id) -> std::string_view {
    const auto bytes = pk_cursor.FetchDoc(doc_id);
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
  };
  auto find_doc_for_pk = [&](std::string_view target) -> irs::doc_id_t {
    const uint64_t rows = pk_column->RowCount();
    for (uint64_t row = 0; row < rows; ++row) {
      const auto doc = static_cast<irs::doc_id_t>(row + irs::doc_limits::min());
      if (read_pk_at(doc) == target) {
        return doc;
      }
    }
    return irs::doc_limits::invalid();
  };
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
    ASSERT_EQ("pk1", read_pk_at(varchar_postings->value()));
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
    // Find expected PK so we know document id for this row
    irs::doc_id_t row_doc_id = find_doc_for_pk("pk2");
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
    ASSERT_EQ("pk3", read_pk_at(varchar_postings->value()));
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
  // Row 4 NULL, NULL
  {
    // Find expected PK so we know document id for this row
    irs::doc_id_t row_doc_id = find_doc_for_pk("pk4");
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
TEST_F(DuckDBSearchSinkWriterTest, InsertStringPrefix) {
  auto trx = _data_writer->GetBatch();
  const catalog::Column::Id col_id{1};
  DuckDBSearchSinkInsertWriter sink{trx, AnalyzerProvider, {col_id}};
  sink.Init(1, _dummy_chunk);
  duckdb::Vector dummy_varchar{duckdb::LogicalType::VARCHAR, 0};

  const std::vector<std::string_view> pk{
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk1", 19},
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x2pk2", 19},
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x3pk3", 19},
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x4pk4", 19}};

  sink.SwitchColumn(ColumnDescriptor{col_id, catalog::ColumnStoreMode::kNormal,
                                     duckdb::LogicalType::VARCHAR, false},
                    dummy_varchar, 0);
  sink.Write({rocksdb::Slice("\x0", 1), rocksdb::Slice("\x0foo", 4)}, pk[0]);

  sink.Finish();
  ASSERT_TRUE(trx.Commit());
  _data_writer->RefreshCommit();
  auto reader = irs::DirectoryReader(_dir, _codec, {.db = &TestDb()});
  ASSERT_EQ(1, reader.size());
  ASSERT_EQ(1, reader.docs_count());
  ASSERT_EQ(1, reader.live_docs_count());
  auto& segment = reader[0];
  const auto* cs_reader = segment.CsReader();
  ASSERT_NE(nullptr, cs_reader);
  const auto* pk_column = cs_reader->Column(
    static_cast<irs::field_id>(catalog::Column::kGeneratedPKId));
  ASSERT_NE(nullptr, pk_column);
  irs::columnstore::ColumnReader::BlobPointReader pk_cursor{*cs_reader,
                                                            *pk_column};
  auto read_pk_at = [&](irs::doc_id_t doc_id) -> std::string_view {
    const auto bytes = pk_cursor.FetchDoc(doc_id);
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
  };

  auto varchar_terms =
    segment.field(std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9});
  ASSERT_NE(nullptr, varchar_terms);
  auto varchar_terms_itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
  ASSERT_NE(nullptr, varchar_terms_itr);
  ASSERT_TRUE(varchar_terms_itr->seek(
    irs::ViewCast<irs::byte_type>(std::string_view{"\x0foo", 4})));

  auto varchar_postings = varchar_terms_itr->postings(irs::IndexFeatures::None);
  ASSERT_TRUE(varchar_postings->next());
  ASSERT_EQ("pk1", read_pk_at(varchar_postings->value()));
}

TEST_F(DuckDBSearchSinkWriterTest, InsertDeleteInsertWithExisting) {
  constexpr std::string_view kPk = {
    "\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk1", 19};
  constexpr std::string_view kPk2 = {
    "\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk2", 19};
  {
    auto trx = _data_writer->GetBatch();
    DuckDBSearchSinkInsertWriter sink{
      trx, AnalyzerProvider,
      std::array<catalog::Column::Id, 1>{catalog::Column::Id{1}}};
    sink.Init(2, _dummy_chunk);
    duckdb::Vector dummy_varchar{duckdb::LogicalType::VARCHAR, 0};
    sink.SwitchColumn(ColumnDescriptor{catalog::Column::Id{1},
                                       catalog::ColumnStoreMode::kNormal,
                                       duckdb::LogicalType::VARCHAR, false},
                      dummy_varchar, 0);
    sink.Write({rocksdb::Slice("value1", 6)}, kPk);
    // second document to keep segment around
    sink.Write({rocksdb::Slice("value9", 6)}, kPk2);
    sink.Finish();
    trx.Commit();
    // That would be our "existing" segment
    _data_writer->RefreshCommit();
  }
  {
    auto delete_trx = _data_writer->GetBatch();
    DuckDBSearchSinkDeleteWriter delete_sink{delete_trx};
    delete_sink.Init(1, _dummy_chunk);
    delete_sink.DeleteRow("pk1");
    delete_sink.Finish();
    ASSERT_TRUE(delete_trx.Commit());
  }
  {
    auto trx = _data_writer->GetBatch();
    DuckDBSearchSinkInsertWriter sink{
      trx, AnalyzerProvider,
      std::array<catalog::Column::Id, 1>{catalog::Column::Id{1}}};
    sink.Init(1, _dummy_chunk);
    duckdb::Vector dummy_varchar{duckdb::LogicalType::VARCHAR, 0};
    sink.SwitchColumn(ColumnDescriptor{catalog::Column::Id{1},
                                       catalog::ColumnStoreMode::kNormal,
                                       duckdb::LogicalType::VARCHAR, false},
                      dummy_varchar, 0);
    sink.Write({rocksdb::Slice("value2", 6)}, kPk);
    sink.Finish();
    trx.Commit();
    // Intentionally do not commit data writer to force several same PKs in one
    // writer commit
  }
  {
    auto delete_trx = _data_writer->GetBatch();
    DuckDBSearchSinkDeleteWriter delete_sink{delete_trx};
    delete_sink.Init(1, _dummy_chunk);
    delete_sink.DeleteRow("pk1");
    delete_sink.Finish();
    ASSERT_TRUE(delete_trx.Commit());
    // still no data writer commit
  }
  {
    auto trx = _data_writer->GetBatch();
    DuckDBSearchSinkInsertWriter sink{
      trx, AnalyzerProvider,
      std::array<catalog::Column::Id, 1>{catalog::Column::Id{1}}};
    sink.Init(1, _dummy_chunk);
    duckdb::Vector dummy_varchar{duckdb::LogicalType::VARCHAR, 0};
    sink.SwitchColumn(ColumnDescriptor{catalog::Column::Id{1},
                                       catalog::ColumnStoreMode::kNormal,
                                       duckdb::LogicalType::VARCHAR, false},
                      dummy_varchar, 0);
    sink.Write({rocksdb::Slice("value3", 6)}, kPk);
    sink.Finish();
    trx.Commit();
    // eventually commit. value3 would be visible
    _data_writer->RefreshCommit();
  }
  auto reader = irs::DirectoryReader(_dir, _codec, {.db = &TestDb()});
  ASSERT_EQ(2, reader.size());
  ASSERT_EQ(4, reader.docs_count());
  ASSERT_EQ(2, reader.live_docs_count());
  {
    auto& segment = reader[1];
    const auto* cs_reader = segment.CsReader();
    ASSERT_NE(nullptr, cs_reader);
    const auto* pk_column = cs_reader->Column(
      static_cast<irs::field_id>(catalog::Column::kGeneratedPKId));
    ASSERT_NE(nullptr, pk_column);
    irs::columnstore::ColumnReader::BlobPointReader pk_cursor{*cs_reader,
                                                              *pk_column};
    auto read_pk_at = [&](irs::doc_id_t doc_id) -> std::string_view {
      const auto bytes = pk_cursor.FetchDoc(doc_id);
      return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
    };
    auto varchar_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9});
    ASSERT_NE(nullptr, varchar_terms);
    auto itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(
      itr->seek(irs::ViewCast<irs::byte_type>(std::string_view{"value3", 6})));
    auto postings = segment.mask(itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(postings->next());
    ASSERT_EQ("pk1", read_pk_at(postings->value()));
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

TEST_F(DuckDBSearchSinkWriterTest, InsertDeleteInsertOnePending) {
  constexpr std::string_view kPk = {
    "\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk1", 19};
  {
    auto trx = _data_writer->GetBatch();
    DuckDBSearchSinkInsertWriter sink{
      trx, AnalyzerProvider,
      std::array<catalog::Column::Id, 1>{catalog::Column::Id{1}}};
    sink.Init(1, _dummy_chunk);
    duckdb::Vector dummy_varchar{duckdb::LogicalType::VARCHAR, 0};
    sink.SwitchColumn(ColumnDescriptor{catalog::Column::Id{1},
                                       catalog::ColumnStoreMode::kNormal,
                                       duckdb::LogicalType::VARCHAR, false},
                      dummy_varchar, 0);
    sink.Write({rocksdb::Slice("value1", 6)}, kPk);
    sink.Finish();
    trx.Commit();
    // Intentionally do not commit data writer to force several same PKs in one
    // writer commit
  }
  {
    auto delete_trx = _data_writer->GetBatch();
    DuckDBSearchSinkDeleteWriter delete_sink{delete_trx};
    delete_sink.Init(1, _dummy_chunk);
    delete_sink.DeleteRow("pk1");
    delete_sink.Finish();
    ASSERT_TRUE(delete_trx.Commit());
    // still no data writer commit
  }
  {
    auto trx = _data_writer->GetBatch();
    DuckDBSearchSinkInsertWriter sink{
      trx, AnalyzerProvider,
      std::array<catalog::Column::Id, 1>{catalog::Column::Id{1}}};
    sink.Init(1, _dummy_chunk);
    duckdb::Vector dummy_varchar{duckdb::LogicalType::VARCHAR, 0};
    sink.SwitchColumn(ColumnDescriptor{catalog::Column::Id{1},
                                       catalog::ColumnStoreMode::kNormal,
                                       duckdb::LogicalType::VARCHAR, false},
                      dummy_varchar, 0);
    sink.Write({rocksdb::Slice("value2", 6)}, kPk);
    sink.Finish();
    trx.Commit();
    // Intentionally do not commit data writer to force several same PKs in one
    // writer commit
  }
  {
    auto delete_trx = _data_writer->GetBatch();
    DuckDBSearchSinkDeleteWriter delete_sink{delete_trx};
    delete_sink.Init(1, _dummy_chunk);
    delete_sink.DeleteRow("pk1");
    delete_sink.Finish();
    ASSERT_TRUE(delete_trx.Commit());
    // still no data writer commit
  }
  {
    auto trx = _data_writer->GetBatch();
    DuckDBSearchSinkInsertWriter sink{
      trx, AnalyzerProvider,
      std::array<catalog::Column::Id, 1>{catalog::Column::Id{1}}};
    sink.Init(1, _dummy_chunk);
    duckdb::Vector dummy_varchar{duckdb::LogicalType::VARCHAR, 0};
    sink.SwitchColumn(ColumnDescriptor{catalog::Column::Id{1},
                                       catalog::ColumnStoreMode::kNormal,
                                       duckdb::LogicalType::VARCHAR, false},
                      dummy_varchar, 0);
    sink.Write({rocksdb::Slice("value3", 6)}, kPk);
    sink.Finish();
    trx.Commit();
    // eventually commit. value3 would be visible
    _data_writer->RefreshCommit();
  }
  auto reader = irs::DirectoryReader(_dir, _codec, {.db = &TestDb()});
  ASSERT_EQ(1, reader.size());
  ASSERT_EQ(3, reader.docs_count());
  ASSERT_EQ(1, reader.live_docs_count());
  {
    auto& segment = reader[0];
    const auto* cs_reader = segment.CsReader();
    ASSERT_NE(nullptr, cs_reader);
    const auto* pk_column = cs_reader->Column(
      static_cast<irs::field_id>(catalog::Column::kGeneratedPKId));
    ASSERT_NE(nullptr, pk_column);
    irs::columnstore::ColumnReader::BlobPointReader pk_cursor{*cs_reader,
                                                              *pk_column};
    auto read_pk_at = [&](irs::doc_id_t doc_id) -> std::string_view {
      const auto bytes = pk_cursor.FetchDoc(doc_id);
      return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
    };
    auto varchar_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9});
    ASSERT_NE(nullptr, varchar_terms);
    auto itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(
      itr->seek(irs::ViewCast<irs::byte_type>(std::string_view{"value3", 6})));
    auto postings = segment.mask(itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(postings->next());
    ASSERT_EQ("pk1", read_pk_at(postings->value()));
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

TEST_F(DuckDBSearchSinkWriterTest, InsertDeleteInsertOnePendingWithFlush) {
  irs::MemoryDirectory dir;
  irs::IndexWriterOptions options;
  options.db = &TestDb();
  options.reader_options.db = &TestDb();
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
      DuckDBSearchSinkInsertWriter sink{
        trx, AnalyzerProvider,
        std::array<catalog::Column::Id, 1>{catalog::Column::Id{1}}};
      sink.Init(2, _dummy_chunk);
      duckdb::Vector dummy_varchar{duckdb::LogicalType::VARCHAR, 0};
      sink.SwitchColumn(ColumnDescriptor{catalog::Column::Id{1},
                                         catalog::ColumnStoreMode::kNormal,
                                         duckdb::LogicalType::VARCHAR, false},
                        dummy_varchar, 0);
      sink.Write({rocksdb::Slice("value1", 6)}, kPk);
      sink.Write({rocksdb::Slice("value8", 6)}, kPk3);
      sink.Finish();
      trx.Commit();
      // Intentionally do not commit data writer to force several same PKs in
      // one writer commit
    }
    {
      auto delete_trx = limited_data_writer->GetBatch();
      DuckDBSearchSinkDeleteWriter delete_sink{delete_trx};
      delete_sink.Init(1, _dummy_chunk);
      delete_sink.DeleteRow("pk1");
      delete_sink.Finish();
      ASSERT_TRUE(delete_trx.Commit());
      // still no data writer commit
    }
    {
      auto trx = limited_data_writer->GetBatch();
      DuckDBSearchSinkInsertWriter sink{
        trx, AnalyzerProvider,
        std::array<catalog::Column::Id, 1>{catalog::Column::Id{1}}};
      sink.Init(2, _dummy_chunk);
      duckdb::Vector dummy_varchar{duckdb::LogicalType::VARCHAR, 0};
      sink.SwitchColumn(ColumnDescriptor{catalog::Column::Id{1},
                                         catalog::ColumnStoreMode::kNormal,
                                         duckdb::LogicalType::VARCHAR, false},
                        dummy_varchar, 0);
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
      DuckDBSearchSinkDeleteWriter delete_sink{delete_trx};
      delete_sink.Init(1, _dummy_chunk);
      delete_sink.DeleteRow("pk1");
      delete_sink.Finish();
      ASSERT_TRUE(delete_trx.Commit());
      // still no data writer commit
    }
    {
      auto trx = limited_data_writer->GetBatch();
      DuckDBSearchSinkInsertWriter sink{
        trx, AnalyzerProvider,
        std::array<catalog::Column::Id, 1>{catalog::Column::Id{1}}};
      sink.Init(1, _dummy_chunk);
      duckdb::Vector dummy_varchar{duckdb::LogicalType::VARCHAR, 0};
      sink.SwitchColumn(ColumnDescriptor{catalog::Column::Id{1},
                                         catalog::ColumnStoreMode::kNormal,
                                         duckdb::LogicalType::VARCHAR, false},
                        dummy_varchar, 0);
      sink.Write({rocksdb::Slice("value3", 6)}, kPk);
      sink.Finish();
      trx.Commit();
      // eventually commit. value3 would be visible
      limited_data_writer->RefreshCommit();
    }
    auto reader = irs::DirectoryReader(dir, _codec, {.db = &TestDb()});
    ASSERT_EQ(3, reader.size());
    ASSERT_EQ(5, reader.docs_count());
    ASSERT_EQ(3, reader.live_docs_count());

    {
      auto& segment = reader[2];
      const auto* cs_reader = segment.CsReader();
      ASSERT_NE(nullptr, cs_reader);
      const auto* pk_column = cs_reader->Column(
        static_cast<irs::field_id>(catalog::Column::kGeneratedPKId));
      ASSERT_NE(nullptr, pk_column);
      irs::columnstore::ColumnReader::BlobPointReader pk_cursor{*cs_reader,
                                                                *pk_column};
      auto read_pk_at = [&](irs::doc_id_t doc_id) -> std::string_view {
        const auto bytes = pk_cursor.FetchDoc(doc_id);
        return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
      };
      auto varchar_terms = segment.field(
        std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9});
      ASSERT_NE(nullptr, varchar_terms);
      auto itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(itr->seek(
        irs::ViewCast<irs::byte_type>(std::string_view{"value3", 6})));
      auto postings = segment.mask(itr->postings(irs::IndexFeatures::None));
      ASSERT_TRUE(postings->next());
      ASSERT_EQ("pk1", read_pk_at(postings->value()));
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

TEST_F(DuckDBSearchSinkWriterTest, DeleteNotMissedWithExisting) {
  constexpr std::string_view kPk = {
    "\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk1", 19};
  constexpr std::string_view kPk2 = {
    "\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk2", 19};
  {
    auto trx = _data_writer->GetBatch();
    DuckDBSearchSinkInsertWriter sink{
      trx, AnalyzerProvider,
      std::array<catalog::Column::Id, 1>{catalog::Column::Id{1}}};
    sink.Init(2, _dummy_chunk);
    duckdb::Vector dummy_varchar{duckdb::LogicalType::VARCHAR, 0};
    sink.SwitchColumn(ColumnDescriptor{catalog::Column::Id{1},
                                       catalog::ColumnStoreMode::kNormal,
                                       duckdb::LogicalType::VARCHAR, false},
                      dummy_varchar, 0);
    sink.Write({rocksdb::Slice("value1", 6)}, kPk);
    // second document to keep segment around
    sink.Write({rocksdb::Slice("value9", 6)}, kPk2);
    sink.Finish();
    trx.Commit();
    // That would be our "existing" segment
    _data_writer->RefreshCommit();
  }
  {
    // this delete should not fire at value2 during new segment processing
    // and successfully delete value1.
    auto delete_trx = _data_writer->GetBatch();
    DuckDBSearchSinkDeleteWriter delete_sink{delete_trx};
    delete_sink.Init(1, _dummy_chunk);
    delete_sink.DeleteRow("pk1");
    delete_sink.Finish();
    ASSERT_TRUE(delete_trx.Commit());
  }
  {
    auto trx = _data_writer->GetBatch();
    DuckDBSearchSinkInsertWriter sink{
      trx, AnalyzerProvider,
      std::array<catalog::Column::Id, 1>{catalog::Column::Id{1}}};
    sink.Init(1, _dummy_chunk);
    duckdb::Vector dummy_varchar{duckdb::LogicalType::VARCHAR, 0};
    sink.SwitchColumn(ColumnDescriptor{catalog::Column::Id{1},
                                       catalog::ColumnStoreMode::kNormal,
                                       duckdb::LogicalType::VARCHAR, false},
                      dummy_varchar, 0);
    sink.Write({rocksdb::Slice("value2", 6)}, kPk);
    sink.Finish();
    trx.Commit();
    _data_writer->RefreshCommit();
    // Intentionally do not commit data writer to force several same PKs in one
    // writer commit
  }
  auto reader = irs::DirectoryReader(_dir, _codec, {.db = &TestDb()});
  ASSERT_EQ(2, reader.size());
  ASSERT_EQ(3, reader.docs_count());
  ASSERT_EQ(2, reader.live_docs_count());
  {
    auto& segment = reader[1];
    const auto* cs_reader = segment.CsReader();
    ASSERT_NE(nullptr, cs_reader);
    const auto* pk_column = cs_reader->Column(
      static_cast<irs::field_id>(catalog::Column::kGeneratedPKId));
    ASSERT_NE(nullptr, pk_column);
    irs::columnstore::ColumnReader::BlobPointReader pk_cursor{*cs_reader,
                                                              *pk_column};
    auto read_pk_at = [&](irs::doc_id_t doc_id) -> std::string_view {
      const auto bytes = pk_cursor.FetchDoc(doc_id);
      return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
    };
    auto varchar_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9});
    ASSERT_NE(nullptr, varchar_terms);
    auto itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(
      itr->seek(irs::ViewCast<irs::byte_type>(std::string_view{"value2", 6})));
    auto postings = segment.mask(itr->postings(irs::IndexFeatures::None));
    ASSERT_TRUE(postings->next());
    ASSERT_EQ("pk1", read_pk_at(postings->value()));
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

TEST_F(DuckDBSearchSinkWriterTest, UpdateWithExisting) {
  constexpr std::string_view kPk1 = {
    "\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk1", 19};
  constexpr std::string_view kPk2 = {
    "\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk2", 19};
  // Phase 1: build an "existing" segment with two docs.
  {
    auto trx = _data_writer->GetBatch();
    DuckDBSearchSinkInsertWriter sink{
      trx, AnalyzerProvider,
      std::array<catalog::Column::Id, 1>{catalog::Column::Id{1}}};
    sink.Init(2, _dummy_chunk);
    duckdb::Vector dummy_varchar{duckdb::LogicalType::VARCHAR, 0};
    sink.SwitchColumn(ColumnDescriptor{catalog::Column::Id{1},
                                       catalog::ColumnStoreMode::kNormal,
                                       duckdb::LogicalType::VARCHAR, false},
                      dummy_varchar, 0);
    sink.Write({rocksdb::Slice("value1", 6)}, kPk1);
    sink.Write({rocksdb::Slice("value2", 6)}, kPk2);
    sink.Finish();
    ASSERT_TRUE(trx.Commit());
    _data_writer->RefreshCommit();
  }
  // Phase 2: update pk1's value using the combined update writer.
  // Update == delete old + insert new for the same PK.
  {
    auto trx = _data_writer->GetBatch();
    DuckDBSearchSinkUpdateWriter sink{
      trx, AnalyzerProvider,
      std::array<catalog::Column::Id, 1>{catalog::Column::Id{1}}};
    sink.Init(1, _dummy_chunk);
    duckdb::Vector dummy_varchar{duckdb::LogicalType::VARCHAR, 0};
    sink.DeleteRow("pk1");
    sink.SwitchColumn(ColumnDescriptor{catalog::Column::Id{1},
                                       catalog::ColumnStoreMode::kNormal,
                                       duckdb::LogicalType::VARCHAR, false},
                      dummy_varchar, 0);
    sink.Write({rocksdb::Slice("value1_new", 10)}, kPk1);
    sink.Finish();
    ASSERT_TRUE(trx.Commit());
    _data_writer->RefreshCommit();
  }

  auto reader = irs::DirectoryReader(_dir, _codec, {.db = &TestDb()});
  // Two segments: the original (pk1-old, pk2) plus the new insert (pk1-new).
  ASSERT_EQ(2, reader.size());
  // Three physical docs: pk1-old still contributes to docs_count even though
  // it was deleted - this is the same after-effect as a plain delete.
  ASSERT_EQ(3, reader.docs_count());
  // Two live docs: pk2 (untouched) and pk1-new.
  ASSERT_EQ(2, reader.live_docs_count());

  auto find = [](const irs::SubReader& segment, std::string_view value) {
    auto varchar_terms = segment.field(
      std::string_view{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9});
    EXPECT_NE(nullptr, varchar_terms);
    auto itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
    EXPECT_TRUE(itr->seek(irs::ViewCast<irs::byte_type>(value)));
    return segment.mask(itr->postings(irs::IndexFeatures::None));
  };
  auto pk_of = [](const irs::SubReader& segment,
                  irs::doc_id_t doc) -> std::string {
    const auto* cs_reader = segment.CsReader();
    EXPECT_NE(nullptr, cs_reader);
    const auto* pk_column = cs_reader->Column(
      static_cast<irs::field_id>(catalog::Column::kGeneratedPKId));
    EXPECT_NE(nullptr, pk_column);
    irs::columnstore::ColumnReader::BlobPointReader pk_cursor{*cs_reader,
                                                              *pk_column};
    const auto bytes = pk_cursor.FetchDoc(doc);
    return std::string{reinterpret_cast<const char*>(bytes.data()),
                       bytes.size()};
  };

  // Old segment: pk1's old value is masked, pk2 is still live.
  {
    auto& segment = reader[0];
    auto old = find(segment, "value1");
    ASSERT_FALSE(old->next());
    auto kept = find(segment, "value2");
    ASSERT_TRUE(kept->next());
    ASSERT_EQ("pk2", pk_of(segment, kept->value()));
    ASSERT_FALSE(kept->next());
  }
  // New segment: pk1's new value is visible.
  {
    auto& segment = reader[1];
    auto fresh = find(segment, "value1_new");
    ASSERT_TRUE(fresh->next());
    ASSERT_EQ("pk1", pk_of(segment, fresh->value()));
    ASSERT_FALSE(fresh->next());
  }
}

// JSON-path-on-column writer was removed when JSON paths were unified
// under indexed expressions; per-leaf-type field emission for JSON is now
// covered end-to-end by sqllogic `inverted_index_json.test`.

}  // namespace
