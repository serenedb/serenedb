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

#include <absl/base/internal/endian.h>

#include <duckdb.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/bytes_utils.hpp>

#include "basics/duckdb_engine.h"
#include "catalog/table_options.h"
#include "connector/common.h"
#include "connector/duckdb_search_sink_writer.h"
#include "connector/search_remove_filter.hpp"
#include "connector/search_sink_writer.hpp"
#include "gtest/gtest.h"

namespace {

using namespace sdb;
using namespace connector;

// `catalog::Column::Id` implicit-converts to `BaseType` (uint64_t), which is
// the same underlying type as `irs::field_id`, so no `static_cast` is needed
// at call sites that pass column ids to sink writers / `segment.field()`.
constexpr irs::field_id kPKFieldId = catalog::Column::kGeneratedPKId.id();

// Process-wide DuckDB instance, owned by sdb::DuckDBEngine. tests_main
// brings it up before RUN_ALL_TESTS and tears it down before main returns,
// so the lifetime envelope strictly covers every test body.
duckdb::DatabaseInstance& TestDb() {
  return ::sdb::DuckDBEngine::Instance().instance();
}

// Builds a flat-storage Vector of typed `T` from a list of values; all rows
// are valid (no nulls).
template<typename T>
duckdb::Vector MakeNumericVector(duckdb::LogicalType type,
                                 std::initializer_list<T> values) {
  duckdb::Vector vec{type, static_cast<duckdb::idx_t>(values.size())};
  duckdb::FlatVector::ValidityMutable(vec).SetAllValid(values.size());
  auto* data = duckdb::FlatVector::GetDataMutable<T>(vec);
  size_t i = 0;
  for (auto v : values) {
    data[i++] = v;
  }
  return vec;
}

// Flat VARCHAR Vector. Strings are heap-allocated inside the Vector's string
// auxiliary buffer; the returned Vector owns the storage.
duckdb::Vector MakeVarcharVector(
  std::initializer_list<std::string_view> values) {
  duckdb::Vector vec{duckdb::LogicalType::VARCHAR,
                     static_cast<duckdb::idx_t>(values.size())};
  duckdb::FlatVector::ValidityMutable(vec).SetAllValid(values.size());
  auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec);
  size_t i = 0;
  for (auto v : values) {
    data[i++] = duckdb::StringVector::AddStringOrBlob(vec, v.data(), v.size());
  }
  return vec;
}

// Flat VARCHAR Vector where some rows are NULL. `validity[i]==false` marks
// row i as NULL; `values[i]` is ignored for those rows.
duckdb::Vector MakeNullableVarcharVector(
  std::initializer_list<std::string_view> values,
  std::initializer_list<bool> validity) {
  EXPECT_EQ(values.size(), validity.size());
  duckdb::Vector vec{duckdb::LogicalType::VARCHAR,
                     static_cast<duckdb::idx_t>(values.size())};
  auto& v = duckdb::FlatVector::ValidityMutable(vec);
  v.SetAllValid(values.size());
  auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec);
  size_t i = 0;
  auto vit = validity.begin();
  for (auto val : values) {
    if (*vit) {
      data[i] =
        duckdb::StringVector::AddStringOrBlob(vec, val.data(), val.size());
    } else {
      v.SetInvalid(i);
    }
    ++i;
    ++vit;
  }
  return vec;
}

// SQLNULL-typed Vector: every row is null. Sink unconditionally treats
// SQLNULL columns as null, but we still mark the validity bits so the
// have_nulls path reads as expected.
duckdb::Vector MakeSqlNullVector(duckdb::idx_t count) {
  duckdb::Vector vec{duckdb::LogicalType::SQLNULL, count};
  auto& v = duckdb::FlatVector::ValidityMutable(vec);
  v.SetAllValid(count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    v.SetInvalid(i);
  }
  return vec;
}

class DuckDBSearchSinkWriterTest : public ::testing::Test {
 public:
  static catalog::ColumnTokenizer AnalyzerProvider(irs::field_id) {
    static catalog::Tokenizer gStringTokenizer(
      catalog::Permissions{}, ObjectId{0}, ObjectId{12345},
      "test_string_verbartim", {}, DEFAULT_ROW_GROUP_SIZE,
      irs::analysis::TokenizerConfig{.config =
                                       irs::StringTokenizer::Options{}});
    auto tokenizer = gStringTokenizer.GetTokenizer();
    EXPECT_TRUE(tokenizer);
    return {.analyzer = *std::move(tokenizer),
            .features = irs::IndexFeatures::None};
  }

  static void SetUpTestCase() {
    // Running these multiple times does no harm but is redundant.
    irs::formats::Init();
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

  // Indexed term values per row (per column).
  // VARCHAR: terms are exactly the string bytes -- no legacy kStringPrefix
  // dance under the Vector-native API.
  const std::vector<std::string_view> string_values{
    std::string_view{"rrr", 3}, std::string_view{"", 0},
    std::string_view{"abcdef", 6}, std::string_view{"\x0", 1}};
  const std::vector<int32_t> int_values{0, 1, 2, 3};
  const std::vector<bool> bool_values{false, true, true, false};
  const std::vector<float> float_values{5.f, 6.f, 7.f, 8.f};
  const std::vector<int64_t> bigint_values{9, 10, 11, 12};

  // First batch: rows 0 and 1
  sink.Init(2, _dummy_chunk);
  {
    auto vec = MakeNumericVector<int32_t>(duckdb::LogicalType::INTEGER,
                                          {int_values[0], int_values[1]});
    std::vector<std::string_view> rk{pk[0], pk[1]};
    sink.SwitchColumn(ColumnDescriptor{col_id[0], duckdb::LogicalType::INTEGER},
                      vec, rk, 2);
  }
  {
    auto vec = MakeVarcharVector({string_values[0], string_values[1]});
    std::vector<std::string_view> rk{pk[0], pk[1]};
    sink.SwitchColumn(ColumnDescriptor{col_id[1], duckdb::LogicalType::VARCHAR},
                      vec, rk, 2);
  }
  {
    auto vec = MakeNumericVector<bool>(duckdb::LogicalType::BOOLEAN,
                                       {bool_values[0], bool_values[1]});
    std::vector<std::string_view> rk{pk[0], pk[1]};
    sink.SwitchColumn(ColumnDescriptor{col_id[2], duckdb::LogicalType::BOOLEAN},
                      vec, rk, 2);
  }
  {
    auto vec = MakeNumericVector<float>(duckdb::LogicalType::FLOAT,
                                        {float_values[0], float_values[1]});
    std::vector<std::string_view> rk{pk[0], pk[1]};
    sink.SwitchColumn(ColumnDescriptor{col_id[3], duckdb::LogicalType::FLOAT},
                      vec, rk, 2);
  }
  {
    auto vec = MakeNumericVector<int64_t>(duckdb::LogicalType::BIGINT,
                                          {bigint_values[0], bigint_values[1]});
    std::vector<std::string_view> rk{pk[0], pk[1]};
    sink.SwitchColumn(ColumnDescriptor{col_id[4], duckdb::LogicalType::BIGINT},
                      vec, rk, 2);
  }
  sink.Finish();

  // Second batch: rows 2 and 3 - reusing the same sink (tests document reset)
  sink.Init(2, _dummy_chunk);
  {
    auto vec = MakeNumericVector<int32_t>(duckdb::LogicalType::INTEGER,
                                          {int_values[2], int_values[3]});
    std::vector<std::string_view> rk{pk[2], pk[3]};
    sink.SwitchColumn(ColumnDescriptor{col_id[0], duckdb::LogicalType::INTEGER},
                      vec, rk, 2);
  }
  {
    auto vec = MakeVarcharVector({string_values[2], string_values[3]});
    std::vector<std::string_view> rk{pk[2], pk[3]};
    sink.SwitchColumn(ColumnDescriptor{col_id[1], duckdb::LogicalType::VARCHAR},
                      vec, rk, 2);
  }
  {
    auto vec = MakeNumericVector<bool>(duckdb::LogicalType::BOOLEAN,
                                       {bool_values[2], bool_values[3]});
    std::vector<std::string_view> rk{pk[2], pk[3]};
    sink.SwitchColumn(ColumnDescriptor{col_id[2], duckdb::LogicalType::BOOLEAN},
                      vec, rk, 2);
  }
  {
    auto vec = MakeNumericVector<float>(duckdb::LogicalType::FLOAT,
                                        {float_values[2], float_values[3]});
    std::vector<std::string_view> rk{pk[2], pk[3]};
    sink.SwitchColumn(ColumnDescriptor{col_id[3], duckdb::LogicalType::FLOAT},
                      vec, rk, 2);
  }
  {
    auto vec = MakeNumericVector<int64_t>(duckdb::LogicalType::BIGINT,
                                          {bigint_values[2], bigint_values[3]});
    std::vector<std::string_view> rk{pk[2], pk[3]};
    sink.SwitchColumn(ColumnDescriptor{col_id[4], duckdb::LogicalType::BIGINT},
                      vec, rk, 2);
  }
  sink.Finish();
  ASSERT_TRUE(trx.Commit());
  _data_writer->RefreshCommit();

  auto validate_row = [](const irs::SubReader& segment, std::string_view pk,
                         int32_t col1, std::string_view col2, bool col3,
                         float col4, int64_t col5) {
    const auto* cs_reader = segment.GetColReader();
    ASSERT_NE(nullptr, cs_reader);
    const auto* pk_column = cs_reader->Column(kPKFieldId);
    ASSERT_NE(nullptr, pk_column);
    irs::ColumnReader::BlobPointReader pk_cursor{*cs_reader, *pk_column};
    auto read_pk_at = [&](irs::doc_id_t doc_id) -> std::string_view {
      const auto bytes = pk_cursor.FetchDoc(doc_id);
      return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
    };
    auto int32_terms = segment.field(catalog::Column::Id{1});
    ASSERT_NE(nullptr, int32_terms);
    auto varchar_terms = segment.field(catalog::Column::Id{2});
    ASSERT_NE(nullptr, varchar_terms);
    auto bool_terms = segment.field(catalog::Column::Id{3});
    ASSERT_NE(nullptr, bool_terms);
    auto real_terms = segment.field(catalog::Column::Id{4});
    ASSERT_NE(nullptr, real_terms);
    auto big_terms = segment.field(catalog::Column::Id{5});
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

    validate_row(reader[0], "pk1", int_values[0], string_values[0],
                 bool_values[0], float_values[0], bigint_values[0]);
    validate_row(reader[0], "pk2", int_values[1], string_values[1],
                 bool_values[1], float_values[1], bigint_values[1]);
    validate_row(reader[0], "pk3", int_values[2], string_values[2],
                 bool_values[2], float_values[2], bigint_values[2]);
    validate_row(reader[0], "pk4", int_values[3], string_values[3],
                 bool_values[3], float_values[3], bigint_values[3]);
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

    validate_row(reader[0], "pk1", int_values[0], string_values[0],
                 bool_values[0], float_values[0], bigint_values[0]);
    validate_row(reader[0], "pk3", int_values[2], string_values[2],
                 bool_values[2], float_values[2], bigint_values[2]);
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

  // Distinct IS-NULL marker field ids so the value and null fields land in
  // different term-dict slots (the production shape after per-kind id
  // allocation). Without this, the test would only exercise the legacy
  // fallback where null_field_id collapses onto the value field.
  constexpr irs::field_id kVarcharNullsFieldId = 100;
  constexpr irs::field_id kUnknownNullsFieldId = 101;
  catalog::InvertedIndexEntryInfo varchar_entry;
  varchar_entry.null_field_id = kVarcharNullsFieldId;
  catalog::InvertedIndexEntryInfo unknown_entry;
  unknown_entry.null_field_id = kUnknownNullsFieldId;
  EntryInfoProvider entry_provider =
    [varchar_field = col_id[0], unknown_field = col_id[1], &varchar_entry,
     &unknown_entry](
      irs::field_id id) -> const catalog::InvertedIndexEntryInfo* {
    if (id == varchar_field) {
      return &varchar_entry;
    }
    if (id == unknown_field) {
      return &unknown_entry;
    }
    return nullptr;
  };

  DuckDBSearchSinkInsertWriter sink{trx, AnalyzerProvider, col_id,
                                    std::move(entry_provider)};
  sink.Init(4, _dummy_chunk);
  std::vector<std::string_view> rk{pk[0], pk[1], pk[2], pk[3]};

  {
    // Row pattern: foo, NULL, bar, NULL.
    auto vec = MakeNullableVarcharVector(
      {string_data[0], std::string_view{}, string_data[1], std::string_view{}},
      {true, false, true, false});
    sink.SwitchColumn(ColumnDescriptor{col_id[0], duckdb::LogicalType::VARCHAR},
                      vec, rk, 4);
  }
  {
    // SQLNULL column: all 4 rows null.
    auto vec = MakeSqlNullVector(4);
    sink.SwitchColumn(ColumnDescriptor{col_id[1], duckdb::LogicalType::SQLNULL},
                      vec, rk, 4);
  }
  sink.Finish();
  ASSERT_TRUE(trx.Commit());
  _data_writer->RefreshCommit();

  auto reader = irs::DirectoryReader(_dir, _codec, {.db = &TestDb()});
  ASSERT_EQ(1, reader.size());
  ASSERT_EQ(4, reader.docs_count());
  ASSERT_EQ(4, reader.live_docs_count());
  auto& segment = reader[0];
  const auto* cs_reader = segment.GetColReader();
  ASSERT_NE(nullptr, cs_reader);
  const auto* pk_column = cs_reader->Column(kPKFieldId);
  ASSERT_NE(nullptr, pk_column);
  irs::ColumnReader::BlobPointReader pk_cursor{*cs_reader, *pk_column};
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
  auto varchar_terms = segment.field(catalog::Column::Id{1});
  ASSERT_NE(nullptr, varchar_terms);
  auto varchar_nulls = segment.field(kVarcharNullsFieldId);
  ASSERT_NE(nullptr, varchar_nulls);
  // VARCHAR-typed value field has its own slot; the IS-NULL marker lands in
  // a separate slot keyed by the entry's null_field_id.
  ASSERT_NE(varchar_terms, varchar_nulls);
  auto unknown_terms = segment.field(kUnknownNullsFieldId);
  ASSERT_NE(nullptr, unknown_terms);
  // SQLNULL kind: no value-side terms, only the null marker. The value-side
  // catalog::Column::Id{2} slot is never created.
  ASSERT_EQ(nullptr, segment.field(catalog::Column::Id{2}));

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

// Corner case: string with leading null byte is preserved verbatim in the
// term dict (the old test used the legacy 2-slice prefix encoding to assert
// the same thing; under the Vector-native API the bytes just go through).
TEST_F(DuckDBSearchSinkWriterTest, InsertStringPrefix) {
  auto trx = _data_writer->GetBatch();
  const catalog::Column::Id col_id{1};
  DuckDBSearchSinkInsertWriter sink{trx, AnalyzerProvider, {col_id}};
  sink.Init(1, _dummy_chunk);

  const std::vector<std::string_view> pk{
    {"\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk1", 19}};
  std::vector<std::string_view> rk{pk[0]};

  // Literal 4-byte term: \x0 'f' 'o' 'o'.
  auto vec = MakeVarcharVector({std::string_view{"\x0foo", 4}});
  sink.SwitchColumn(ColumnDescriptor{col_id, duckdb::LogicalType::VARCHAR}, vec,
                    rk, 1);
  sink.Finish();
  ASSERT_TRUE(trx.Commit());
  _data_writer->RefreshCommit();
  auto reader = irs::DirectoryReader(_dir, _codec, {.db = &TestDb()});
  ASSERT_EQ(1, reader.size());
  ASSERT_EQ(1, reader.docs_count());
  ASSERT_EQ(1, reader.live_docs_count());
  auto& segment = reader[0];
  const auto* cs_reader = segment.GetColReader();
  ASSERT_NE(nullptr, cs_reader);
  const auto* pk_column = cs_reader->Column(kPKFieldId);
  ASSERT_NE(nullptr, pk_column);
  irs::ColumnReader::BlobPointReader pk_cursor{*cs_reader, *pk_column};
  auto read_pk_at = [&](irs::doc_id_t doc_id) -> std::string_view {
    const auto bytes = pk_cursor.FetchDoc(doc_id);
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
  };

  auto varchar_terms = segment.field(catalog::Column::Id{1});
  ASSERT_NE(nullptr, varchar_terms);
  auto varchar_terms_itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
  ASSERT_NE(nullptr, varchar_terms_itr);
  ASSERT_TRUE(varchar_terms_itr->seek(
    irs::ViewCast<irs::byte_type>(std::string_view{"\x0foo", 4})));

  auto varchar_postings = varchar_terms_itr->postings(irs::IndexFeatures::None);
  ASSERT_TRUE(varchar_postings->next());
  ASSERT_EQ("pk1", read_pk_at(varchar_postings->value()));
}

// Helper for the multi-commit "insert / delete / insert" exercise tests
// below: each phase opens a transaction, writes a single (pk, value) row to
// column 1, finishes, commits.
namespace {

void InsertOneVarcharRow(irs::IndexWriter& writer, std::string_view pk,
                         std::string_view value,
                         duckdb::DataChunk& dummy_chunk) {
  auto trx = writer.GetBatch();
  DuckDBSearchSinkInsertWriter sink{
    trx, DuckDBSearchSinkWriterTest::AnalyzerProvider,
    std::array<catalog::Column::Id, 1>{catalog::Column::Id{1}}};
  sink.Init(1, dummy_chunk);
  auto vec = MakeVarcharVector({value});
  std::vector<std::string_view> rk{pk};
  sink.SwitchColumn(
    ColumnDescriptor{catalog::Column::Id{1}, duckdb::LogicalType::VARCHAR}, vec,
    rk, 1);
  sink.Finish();
  ASSERT_TRUE(trx.Commit());
}

void InsertTwoVarcharRows(irs::IndexWriter& writer, std::string_view pk_a,
                          std::string_view value_a, std::string_view pk_b,
                          std::string_view value_b,
                          duckdb::DataChunk& dummy_chunk) {
  auto trx = writer.GetBatch();
  DuckDBSearchSinkInsertWriter sink{
    trx, DuckDBSearchSinkWriterTest::AnalyzerProvider,
    std::array<catalog::Column::Id, 1>{catalog::Column::Id{1}}};
  sink.Init(2, dummy_chunk);
  auto vec = MakeVarcharVector({value_a, value_b});
  std::vector<std::string_view> rk{pk_a, pk_b};
  sink.SwitchColumn(
    ColumnDescriptor{catalog::Column::Id{1}, duckdb::LogicalType::VARCHAR}, vec,
    rk, 2);
  sink.Finish();
  ASSERT_TRUE(trx.Commit());
}

void DeleteOnePk(irs::IndexWriter& writer, std::string_view pk,
                 duckdb::DataChunk& dummy_chunk) {
  auto delete_trx = writer.GetBatch();
  DuckDBSearchSinkDeleteWriter delete_sink{delete_trx};
  delete_sink.Init(1, dummy_chunk);
  delete_sink.DeleteRow(pk);
  delete_sink.Finish();
  ASSERT_TRUE(delete_trx.Commit());
}

}  // namespace

TEST_F(DuckDBSearchSinkWriterTest, InsertDeleteInsertWithExisting) {
  constexpr std::string_view kPk = {
    "\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk1", 19};
  constexpr std::string_view kPk2 = {
    "\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x1pk2", 19};

  InsertTwoVarcharRows(*_data_writer, kPk, "value1", kPk2, "value9",
                       _dummy_chunk);
  _data_writer->RefreshCommit();  // "existing" segment.

  DeleteOnePk(*_data_writer, "pk1", _dummy_chunk);
  InsertOneVarcharRow(*_data_writer, kPk, "value2", _dummy_chunk);
  // Intentionally do not commit data writer between these steps to force
  // several same PKs in one writer commit.
  DeleteOnePk(*_data_writer, "pk1", _dummy_chunk);
  InsertOneVarcharRow(*_data_writer, kPk, "value3", _dummy_chunk);
  _data_writer->RefreshCommit();

  auto reader = irs::DirectoryReader(_dir, _codec, {.db = &TestDb()});
  ASSERT_EQ(2, reader.size());
  ASSERT_EQ(4, reader.docs_count());
  ASSERT_EQ(2, reader.live_docs_count());
  {
    auto& segment = reader[1];
    const auto* cs_reader = segment.GetColReader();
    ASSERT_NE(nullptr, cs_reader);
    const auto* pk_column = cs_reader->Column(kPKFieldId);
    ASSERT_NE(nullptr, pk_column);
    irs::ColumnReader::BlobPointReader pk_cursor{*cs_reader, *pk_column};
    auto read_pk_at = [&](irs::doc_id_t doc_id) -> std::string_view {
      const auto bytes = pk_cursor.FetchDoc(doc_id);
      return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
    };
    auto varchar_terms = segment.field(catalog::Column::Id{1});
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
    auto varchar_terms = segment.field(catalog::Column::Id{1});
    ASSERT_NE(nullptr, varchar_terms);

    auto itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(
      itr->seek(irs::ViewCast<irs::byte_type>(std::string_view{"value2", 6})));
    auto postings = segment.mask(itr->postings(irs::IndexFeatures::None));
    ASSERT_FALSE(postings->next());
  }
  {
    auto& segment = reader[0];
    auto varchar_terms = segment.field(catalog::Column::Id{1});
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

  InsertOneVarcharRow(*_data_writer, kPk, "value1", _dummy_chunk);
  // Intentionally do not commit data writer to force several same PKs in one
  // writer commit.
  DeleteOnePk(*_data_writer, "pk1", _dummy_chunk);
  InsertOneVarcharRow(*_data_writer, kPk, "value2", _dummy_chunk);
  DeleteOnePk(*_data_writer, "pk1", _dummy_chunk);
  InsertOneVarcharRow(*_data_writer, kPk, "value3", _dummy_chunk);
  _data_writer->RefreshCommit();

  auto reader = irs::DirectoryReader(_dir, _codec, {.db = &TestDb()});
  ASSERT_EQ(1, reader.size());
  ASSERT_EQ(3, reader.docs_count());
  ASSERT_EQ(1, reader.live_docs_count());
  {
    auto& segment = reader[0];
    const auto* cs_reader = segment.GetColReader();
    ASSERT_NE(nullptr, cs_reader);
    const auto* pk_column = cs_reader->Column(kPKFieldId);
    ASSERT_NE(nullptr, pk_column);
    irs::ColumnReader::BlobPointReader pk_cursor{*cs_reader, *pk_column};
    auto read_pk_at = [&](irs::doc_id_t doc_id) -> std::string_view {
      const auto bytes = pk_cursor.FetchDoc(doc_id);
      return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
    };
    auto varchar_terms = segment.field(catalog::Column::Id{1});
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
    auto varchar_terms = segment.field(catalog::Column::Id{1});
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

    InsertTwoVarcharRows(*limited_data_writer, kPk, "value1", kPk3, "value8",
                         _dummy_chunk);
    DeleteOnePk(*limited_data_writer, "pk1", _dummy_chunk);
    InsertTwoVarcharRows(*limited_data_writer, kPk, "value2", kPk2, "value22",
                         _dummy_chunk);
    DeleteOnePk(*limited_data_writer, "pk1", _dummy_chunk);
    InsertOneVarcharRow(*limited_data_writer, kPk, "value3", _dummy_chunk);
    limited_data_writer->RefreshCommit();

    auto reader = irs::DirectoryReader(dir, _codec, {.db = &TestDb()});
    ASSERT_EQ(3, reader.size());
    ASSERT_EQ(5, reader.docs_count());
    ASSERT_EQ(3, reader.live_docs_count());

    {
      auto& segment = reader[2];
      const auto* cs_reader = segment.GetColReader();
      ASSERT_NE(nullptr, cs_reader);
      const auto* pk_column = cs_reader->Column(kPKFieldId);
      ASSERT_NE(nullptr, pk_column);
      irs::ColumnReader::BlobPointReader pk_cursor{*cs_reader, *pk_column};
      auto read_pk_at = [&](irs::doc_id_t doc_id) -> std::string_view {
        const auto bytes = pk_cursor.FetchDoc(doc_id);
        return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
      };
      auto varchar_terms = segment.field(catalog::Column::Id{1});
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
      auto varchar_terms = segment.field(catalog::Column::Id{1});
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
      auto varchar_terms = segment.field(catalog::Column::Id{1});
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

  InsertTwoVarcharRows(*_data_writer, kPk, "value1", kPk2, "value9",
                       _dummy_chunk);
  _data_writer->RefreshCommit();

  // this delete should not fire at value2 during new segment processing
  // and successfully delete value1.
  DeleteOnePk(*_data_writer, "pk1", _dummy_chunk);
  InsertOneVarcharRow(*_data_writer, kPk, "value2", _dummy_chunk);
  _data_writer->RefreshCommit();

  auto reader = irs::DirectoryReader(_dir, _codec, {.db = &TestDb()});
  ASSERT_EQ(2, reader.size());
  ASSERT_EQ(3, reader.docs_count());
  ASSERT_EQ(2, reader.live_docs_count());
  {
    auto& segment = reader[1];
    const auto* cs_reader = segment.GetColReader();
    ASSERT_NE(nullptr, cs_reader);
    const auto* pk_column = cs_reader->Column(kPKFieldId);
    ASSERT_NE(nullptr, pk_column);
    irs::ColumnReader::BlobPointReader pk_cursor{*cs_reader, *pk_column};
    auto read_pk_at = [&](irs::doc_id_t doc_id) -> std::string_view {
      const auto bytes = pk_cursor.FetchDoc(doc_id);
      return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
    };
    auto varchar_terms = segment.field(catalog::Column::Id{1});
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
    auto varchar_terms = segment.field(catalog::Column::Id{1});
    ASSERT_NE(nullptr, varchar_terms);

    auto itr = varchar_terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(
      itr->seek(irs::ViewCast<irs::byte_type>(std::string_view{"value1", 6})));
    auto postings = segment.mask(itr->postings(irs::IndexFeatures::None));
    ASSERT_FALSE(postings->next());
  }
}

// JSON-path-on-column writer was removed when JSON paths were unified
// under indexed expressions; per-leaf-type field emission for JSON is now
// covered end-to-end by sqllogic `inverted_index_json.test`.

}  // namespace
