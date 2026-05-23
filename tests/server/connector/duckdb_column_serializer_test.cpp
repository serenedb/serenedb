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

#include <gtest/gtest.h>
#include <rocksdb/utilities/transaction_db.h>

#include <duckdb.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <filesystem>
#include <numeric>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

using namespace std::string_view_literals;

#include "catalog/table_options.h"
#include "connector/duckdb_primary_key.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_rocksdb_writer.h"
#include "connector/key_utils.hpp"
#include "duckdb_vector_builder.hpp"
#include "query/transaction.h"

namespace sdb::connector {

using namespace test;

constexpr ObjectId kSerializerTableId{654321};

class DuckDBColumnSerializerTest : public ::testing::Test {
 public:
  // In-memory DuckDB exists only to supply a ClientContext; no SQL is
  // executed through it.
  DuckDBColumnSerializerTest() : _duckdb{nullptr}, _conn{_duckdb} {}

  void SetUp() final {
    rocksdb::Options opts;
    opts.OptimizeForSmallDb();
    opts.create_if_missing = true;
    rocksdb::TransactionDBOptions txn_db_opts;
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_desc{
      {rocksdb::kDefaultColumnFamilyName, opts}};

    _path = testing::TempDir() + "/" +
            ::testing::UnitTest::GetInstance()->current_test_info()->name() +
            "_XXXXXX";
    ASSERT_NE(mkdtemp(_path.data()), nullptr);
    opts.wal_dir = _path + "/journals";

    ASSERT_TRUE(rocksdb::TransactionDB::Open(opts, txn_db_opts, _path, cf_desc,
                                             &_cf_handles, &_db)
                  .ok());
    _table_key = key_utils::PrepareTableKey(kSerializerTableId);
  }

  void TearDown() final {
    if (_db) {
      for (auto h : _cf_handles) {
        _db->DestroyColumnFamilyHandle(h);
      }
      _db->Close();
      delete _db;
      _db = nullptr;
    }
    std::filesystem::remove_all(_path);
  }

  // Build a sequential int32 DataChunk suitable for use as PK input.
  // Fills _pk_chunk (fixture member) and returns a reference to it.
  duckdb::DataChunk& MakePKChunk(duckdb::idx_t num_rows) {
    std::vector<int32_t> vals(num_rows);
    std::iota(vals.begin(), vals.end(), 0);
    MakeChunk(_pk_chunk, {duckdb::LogicalType::INTEGER},
              Vecs(MakeFlat<int32_t>(std::span(vals))), num_rows);
    return _pk_chunk;
  }

  // Build per-row key buffers. Layout after call:
  //   [ObjectId(8)][reserved ColumnId(8)][PK bytes]
  // Call key_utils::SetupColumnForKey on each key before writing a column.
  std::vector<std::string> BuildRowKeys(duckdb::DataChunk& pk_chunk) {
    const duckdb::idx_t num_rows = pk_chunk.size();
    duckdb_primary_key::PKColumn pk_col{0, duckdb::LogicalType::INTEGER};
    std::span<const duckdb_primary_key::PKColumn> pk_cols(&pk_col, 1);

    // Build the per-column UnifiedVectorFormats once, reuse across rows.
    std::vector<duckdb::UnifiedVectorFormat> pk_formats;
    duckdb_primary_key::PreparePKFormats(pk_chunk, pk_cols, pk_formats);

    std::vector<std::string> row_keys(num_rows);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      duckdb_primary_key::MakeColumnKey(
        pk_formats, pk_cols, row, _table_key,
        [](std::string_view) {},  // no locking in tests
        row_keys[row]);
    }
    return row_keys;
  }

  // Write `vec` as column `col_id` using an existing transaction.
  // Stamps col_id into copies of row_keys (original keys are preserved so
  // they can be reused for subsequent columns).
  void WriteColumn(query::Transaction& sdb_txn,
                   const std::vector<std::string>& base_row_keys,
                   catalog::Column::Id col_id, const duckdb::Vector& vec,
                   const duckdb::LogicalType& type, duckdb::idx_t num_rows) {
    std::vector<std::string> col_keys = base_row_keys;
    for (auto& key : col_keys) {
      key_utils::SetupColumnForKey(key, col_id);
    }
    DuckDBColumnSerializer serializer{duckdb::Allocator::DefaultAllocator()};
    DuckDBColumnSerializer::TxnWriter writer{sdb_txn, _cf_handles.front()};
    serializer.WriteColumn(
      writer, vec, num_rows, col_keys, {},
      ColumnDescriptor{col_id, catalog::ColumnStoreMode::kNormal, type,
                       /*have_nulls=*/false});
  }

  // Read back `num_rows` rows of column `col_id` into a fresh Vector.
  duckdb::Vector ReadColumn(const duckdb::LogicalType& type,
                            duckdb::idx_t num_rows,
                            catalog::Column::Id col_id) {
    auto [start_key, end_key] =
      key_utils::CreateTableColumnRange(kSerializerTableId, col_id);
    rocksdb::ReadOptions ro;
    std::unique_ptr<rocksdb::Iterator> it(
      _db->NewIterator(ro, _cf_handles.front()));
    it->Seek(start_key);

    duckdb::Vector output(type, num_rows);
    auto count = ReadColumnIntoDuckDB(*it, output, type, num_rows);
    EXPECT_EQ(count, num_rows);
    return output;
  }

  // Full write-read-compare for a single column.
  // Uses col_id=0. Each test uses a fresh DB (SetUp/TearDown), so col_ids
  // don't collide between tests.
  void CheckColumn(const duckdb::Vector& vec, const duckdb::LogicalType& type,
                   duckdb::idx_t num_rows,
                   catalog::Column::Id col_id = catalog::Column::Id{0}) {
    auto& pk_chunk = MakePKChunk(num_rows);
    auto row_keys = BuildRowKeys(pk_chunk);

    rocksdb::TransactionOptions txn_opts;
    txn_opts.skip_concurrency_control = true;
    rocksdb::WriteOptions wo;
    std::unique_ptr<rocksdb::Transaction> txn(
      _db->BeginTransaction(wo, txn_opts, nullptr));
    ASSERT_NE(txn, nullptr);
    query::Transaction sdb_txn{*_conn.context, std::move(txn)};

    WriteColumn(sdb_txn, row_keys, col_id, vec, type, num_rows);
    // Bypass the full sdb commit -- only the rocksdb write matters here.
    ASSERT_TRUE(sdb_txn.GetRocksDBTransaction().Commit().ok());
    sdb_txn.Destroy();

    auto output = ReadColumn(type, num_rows, col_id);

    for (duckdb::idx_t i = 0; i < num_rows; ++i) {
      SCOPED_TRACE(testing::Message("row ") << i);
      auto expected = vec.GetValue(i);
      auto actual = output.GetValue(i);
      // duckdb::Value::operator== throws on NULL operands (SQL semantics).
      // Check nullability first, then compare non-null values.
      EXPECT_EQ(expected.IsNull(), actual.IsNull());
      if (!expected.IsNull() && !actual.IsNull()) {
        EXPECT_EQ(expected, actual);
      }
    }
  }

 protected:
  std::string _path;
  rocksdb::TransactionDB* _db{nullptr};
  std::vector<rocksdb::ColumnFamilyHandle*> _cf_handles;
  duckdb::DuckDB _duckdb;
  duckdb::Connection _conn;
  std::string _table_key;
  duckdb::DataChunk _pk_chunk;
};

////////////////////////////////////////////////////////////////////////////////
/// Flat scalar columns
////////////////////////////////////////////////////////////////////////////////

TEST_F(DuckDBColumnSerializerTest, FlatInt) {
  auto vec = MakeFlat<int32_t>({1, 2, 3, 3, 2, 1, 5, 6, 7, 7, 6, 5, 9, 9, 9});
  CheckColumn(vec, duckdb::LogicalType::INTEGER, 15);
}

TEST_F(DuckDBColumnSerializerTest, FlatIntNulls) {
  auto vec = MakeNullableFlat<int32_t>(
    {1, 2, 3, 3, 2, 1, 5, std::nullopt, 7, 7, 6, 5, 9, 9, 9});
  CheckColumn(vec, duckdb::LogicalType::INTEGER, 15);
}

TEST_F(DuckDBColumnSerializerTest, FlatTinyInt) {
  auto vec = MakeFlat<int8_t>({-128, -1, 0, 1, 127});
  CheckColumn(vec, duckdb::LogicalType::TINYINT, 5);
}

TEST_F(DuckDBColumnSerializerTest, FlatTinyIntNulls) {
  auto vec =
    MakeNullableFlat<int8_t>({-128, std::nullopt, 0, std::nullopt, 127});
  CheckColumn(vec, duckdb::LogicalType::TINYINT, 5);
}

TEST_F(DuckDBColumnSerializerTest, FlatSmallInt) {
  auto vec = MakeFlat<int16_t>({-32768, -1, 0, 1, 32767});
  CheckColumn(vec, duckdb::LogicalType::SMALLINT, 5);
}

TEST_F(DuckDBColumnSerializerTest, FlatBigInt) {
  auto vec = MakeFlat<int64_t>({-1000000000000LL, 0, 1, 999999999999LL});
  CheckColumn(vec, duckdb::LogicalType::BIGINT, 4);
}

TEST_F(DuckDBColumnSerializerTest, FlatBigIntNulls) {
  auto vec = MakeNullableFlat<int64_t>(
    {-1000000000000LL, std::nullopt, 0, std::nullopt, 999999999999LL});
  CheckColumn(vec, duckdb::LogicalType::BIGINT, 5);
}

TEST_F(DuckDBColumnSerializerTest, FlatFloat) {
  auto vec = MakeFlat<float>({-1.5f, 0.0f, 1.0f, 3.14f});
  CheckColumn(vec, duckdb::LogicalType::FLOAT, 4);
}

TEST_F(DuckDBColumnSerializerTest, FlatDouble) {
  auto vec = MakeFlat<double>({-1.5, 0.0, 1.0, 3.141592653589793});
  CheckColumn(vec, duckdb::LogicalType::DOUBLE, 4);
}

TEST_F(DuckDBColumnSerializerTest, FlatBool) {
  auto vec = MakeFlat<bool>({true, false, true, false});
  CheckColumn(vec, duckdb::LogicalType::BOOLEAN, 4);
}

TEST_F(DuckDBColumnSerializerTest, FlatBoolNulls) {
  auto vec = MakeNullableFlat<bool>(
    {true, false, true, false, true, false, true, std::nullopt, false});
  CheckColumn(vec, duckdb::LogicalType::BOOLEAN, 9);
}

TEST_F(DuckDBColumnSerializerTest, FlatVarchar) {
  auto vec =
    MakeFlatVarchar({"ff", "long enough to not be inlined string yeaaaahhhh",
                     "", "a", "\0"sv, "\0\0"sv, "\0 123", "basr"});
  CheckColumn(vec, duckdb::LogicalType::VARCHAR, 8);
}

TEST_F(DuckDBColumnSerializerTest, FlatVarcharNulls) {
  auto vec = MakeNullableVarchar(
    {std::optional<std::string_view>{"ff"},
     std::optional<std::string_view>{
       "long enough to not be inlined string yeaaaahhhh"},
     std::optional<std::string_view>{""}, std::optional<std::string_view>{"a"},
     std::optional<std::string_view>{"\0"sv},
     std::optional<std::string_view>{"\0\0"sv},
     std::optional<std::string_view>{"\0 123"}, std::nullopt,
     std::optional<std::string_view>{"basr"}});
  CheckColumn(vec, duckdb::LogicalType::VARCHAR, 9);
}

TEST_F(DuckDBColumnSerializerTest, FlatTimestamp) {
  auto vec = MakeFlat<duckdb::timestamp_t>(
    {duckdb::timestamp_t{123000}, duckdb::timestamp_t{0}});
  CheckColumn(vec, duckdb::LogicalType::TIMESTAMP, 2);
}

TEST_F(DuckDBColumnSerializerTest, FlatTimestampNulls) {
  auto vec = MakeNullableFlat<duckdb::timestamp_t>(
    {duckdb::timestamp_t{123000}, std::nullopt, duckdb::timestamp_t{0}});
  CheckColumn(vec, duckdb::LogicalType::TIMESTAMP, 3);
}

////////////////////////////////////////////////////////////////////////////////
/// Constant vectors
////////////////////////////////////////////////////////////////////////////////

TEST_F(DuckDBColumnSerializerTest, ConstantInt) {
  // Constant vector: every row has the same value 42
  const duckdb::idx_t num_rows = 8;
  duckdb::Vector vec(duckdb::Value::INTEGER(42), duckdb::count_t(num_rows));
  CheckColumn(vec, duckdb::LogicalType::INTEGER, num_rows);
}

TEST_F(DuckDBColumnSerializerTest, NullConstant) {
  // Null constant: all rows are null (use named variable to avoid vexing parse)
  auto null_val = duckdb::Value(duckdb::LogicalType::INTEGER);
  const duckdb::idx_t num_rows = 8;
  duckdb::Vector vec(null_val, duckdb::count_t(num_rows));
  CheckColumn(vec, duckdb::LogicalType::INTEGER, num_rows);
}

TEST_F(DuckDBColumnSerializerTest, ConstantVarchar) {
  const duckdb::idx_t num_rows = 5;
  duckdb::Vector vec(duckdb::Value("hello constant"),
                     duckdb::count_t(num_rows));
  CheckColumn(vec, duckdb::LogicalType::VARCHAR, num_rows);
}

////////////////////////////////////////////////////////////////////////////////
/// Dictionary-encoded scalar columns
////////////////////////////////////////////////////////////////////////////////

TEST_F(DuckDBColumnSerializerTest, DictionaryInt) {
  auto child = MakeFlat<int32_t>({1, 2, 3, 5});
  std::vector<duckdb::sel_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  auto vec = MakeDict(std::move(child), std::span(indices));
  CheckColumn(vec, duckdb::LogicalType::INTEGER, 8);
}

TEST_F(DuckDBColumnSerializerTest, DictionaryNullsInt) {
  // Nulls carried in the child (dictionary value nulls)
  auto child = MakeNullableFlat<int32_t>({1, 2, 3, 5});
  std::vector<duckdb::sel_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  // Mark some child positions as null by setting null in child before MakeDict
  // Dict nulls: positions 3 and 5 in output are null (point to child[0])
  duckdb::FlatVector::ValidityMutable(child).SetInvalid(0);  // child[0]=null
  auto vec = MakeDict(std::move(child), std::span(indices));
  CheckColumn(vec, duckdb::LogicalType::INTEGER, 8);
}

TEST_F(DuckDBColumnSerializerTest, DictionaryAllNullsInt) {
  auto child = MakeFlat<int32_t>({1, 2, 3, 5});
  std::vector<duckdb::sel_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  // Make all child entries null
  auto& validity = duckdb::FlatVector::ValidityMutable(child);
  for (int i = 0; i < 4; ++i) {
    validity.SetInvalid(i);
  }
  auto vec = MakeDict(std::move(child), std::span(indices));
  CheckColumn(vec, duckdb::LogicalType::INTEGER, 8);
}

TEST_F(DuckDBColumnSerializerTest, DictionaryBool) {
  auto child = MakeFlat<bool>({true, false, false, false});
  std::vector<duckdb::sel_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  auto vec = MakeDict(std::move(child), std::span(indices));
  CheckColumn(vec, duckdb::LogicalType::BOOLEAN, 8);
}

TEST_F(DuckDBColumnSerializerTest, DictionaryVarchar) {
  auto child = MakeFlatVarchar(
    {"", "foo", "some long string that can not be inlined", " stringnullopt"});
  std::vector<duckdb::sel_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  auto vec = MakeDict(std::move(child), std::span(indices));
  CheckColumn(vec, duckdb::LogicalType::VARCHAR, 8);
}

TEST_F(DuckDBColumnSerializerTest, DictionaryNullsVarcharNulls) {
  // Nulls are carried in the dictionary child (child[3]=null).
  // Output rows 0 and 7 both index child[3], so they produce null values.
  auto child =
    MakeNullableVarchar({std::optional<std::string_view>{""},
                         std::optional<std::string_view>{"foo"},
                         std::optional<std::string_view>{
                           "some long string that can not be inlined"},
                         std::nullopt});
  std::vector<duckdb::sel_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  auto vec = MakeDict(std::move(child), std::span(indices));
  CheckColumn(vec, duckdb::LogicalType::VARCHAR, 8);
}

TEST_F(DuckDBColumnSerializerTest, DictionaryNullsMany) {
  // Many output rows, diverse null patterns in a dictionary
  auto child = MakeNullableFlat<int32_t>({10, std::nullopt, 30, std::nullopt});
  std::vector<duckdb::sel_t> indices{0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3};
  auto vec = MakeDict(std::move(child), std::span(indices));
  CheckColumn(vec, duckdb::LogicalType::INTEGER, 12);
}

////////////////////////////////////////////////////////////////////////////////
/// LIST columns
////////////////////////////////////////////////////////////////////////////////

TEST_F(DuckDBColumnSerializerTest, ListInt) {
  auto vec =
    MakeList<int32_t>({{1, 2, 3}, {3, 2, 1}, {}, {7, 6, 5}, {9, 9, 9}});
  auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER);
  CheckColumn(vec, type, 5);
}

TEST_F(DuckDBColumnSerializerTest, ListIntEmpty) {
  auto vec = MakeList<int32_t>({{}});
  auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER);
  CheckColumn(vec, type, 1);
}

TEST_F(DuckDBColumnSerializerTest, ListIntNullElements) {
  // List rows where some elements are null (null inside the list child)
  duckdb::LogicalType elem_type = duckdb::LogicalType::INTEGER;
  duckdb::LogicalType list_type = duckdb::LogicalType::LIST(elem_type);

  // Build child with nulls: [1, null, 3, null, 5, null, 7]
  std::vector<int32_t> child_vals = {1, 0, 3, 0, 5, 0, 7};
  duckdb::Vector child(elem_type, 7);
  auto* cdata = duckdb::FlatVector::GetDataMutable<int32_t>(child);
  auto& cvalid = duckdb::FlatVector::ValidityMutable(child);
  for (int i = 0; i < 7; ++i) {
    cdata[i] = child_vals[i];
  }
  cvalid.SetInvalid(1);
  cvalid.SetInvalid(3);
  cvalid.SetInvalid(5);

  // 3 list rows: [1,null,3], [null,5,null], [7]
  duckdb::Vector vec(list_type, 3);
  duckdb::ListVector::Reserve(vec, 7);
  auto& vec_child = duckdb::ListVector::GetEntry(vec);
  vec_child.Reference(child);
  auto* entries = duckdb::ListVector::GetData(vec);
  entries[0] = {0, 3};
  entries[1] = {3, 3};
  entries[2] = {6, 1};
  duckdb::ListVector::SetListSize(vec, 7);

  CheckColumn(vec, list_type, 3);
}

TEST_F(DuckDBColumnSerializerTest, NullableListInt) {
  // Some list rows are null (null list, not null elements)
  auto vec = MakeNullableList<int32_t>(
    {std::vector<int32_t>{1, 2, 3}, std::nullopt, std::vector<int32_t>{7, 6, 5},
     std::vector<int32_t>{}, std::nullopt});
  auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER);
  CheckColumn(vec, type, 5);
}

TEST_F(DuckDBColumnSerializerTest, ListBool) {
  auto vec = MakeList<bool>({{true, false, true},
                             {false, true, true},
                             {false, false, false},
                             {true, false}});
  auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::BOOLEAN);
  CheckColumn(vec, type, 4);
}

TEST_F(DuckDBColumnSerializerTest, NullableListBool) {
  auto vec =
    MakeNullableList<bool>({std::vector<bool>{true, false, true}, std::nullopt,
                            std::vector<bool>{false, true, true}});
  auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::BOOLEAN);
  CheckColumn(vec, type, 3);
}

TEST_F(DuckDBColumnSerializerTest, ListVarchar) {
  auto vec = MakeVarcharList(
    {{"foo", "bar", "very long string that does not fit inline"},
     {},
     {"hello"},
     {"a", "b", "c"}});
  auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  CheckColumn(vec, type, 4);
}

TEST_F(DuckDBColumnSerializerTest, NullableListVarchar) {
  std::vector<std::optional<std::vector<std::string_view>>> rows = {
    std::vector<std::string_view>{"foo", "bar"},
    std::nullopt,
    std::vector<std::string_view>{"hello", "world"},
  };
  auto vec = MakeNullableVarcharList(std::span(rows));
  auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  CheckColumn(vec, type, 3);
}

////////////////////////////////////////////////////////////////////////////////
/// Dictionary-encoded LIST (list whose child vector is dict-encoded)
////////////////////////////////////////////////////////////////////////////////

TEST_F(DuckDBColumnSerializerTest, DictionaryEncodedListInt) {
  // Dict child: {1,2,3,4} with indices [3,2,1,0,0,1,2,3]
  // List rows: entries at offsets [0,3,5] -> 3 rows of sizes [3,2,3]
  auto child_flat = MakeFlat<int32_t>({1, 2, 3, 4});
  std::vector<duckdb::sel_t> indices_v{3, 2, 1, 0, 0, 1, 2, 3};
  auto child_dict = MakeDict(std::move(child_flat), std::span(indices_v));

  duckdb::LogicalType elem_type = duckdb::LogicalType::INTEGER;
  duckdb::LogicalType list_type = duckdb::LogicalType::LIST(elem_type);
  duckdb::Vector vec(list_type, 3);
  duckdb::ListVector::Reserve(vec, 8);
  duckdb::ListVector::GetEntry(vec).Reference(child_dict);
  auto* entries = duckdb::ListVector::GetData(vec);
  entries[0] = {0, 3};
  entries[1] = {3, 2};
  entries[2] = {5, 3};
  duckdb::ListVector::SetListSize(vec, 8);

  CheckColumn(vec, list_type, 3);
}

TEST_F(DuckDBColumnSerializerTest, DictionaryEncodedListBool) {
  auto child_flat = MakeFlat<bool>({true, false, true, false});
  std::vector<duckdb::sel_t> indices_v{3, 2, 1, 0, 0, 1, 2, 3};
  auto child_dict = MakeDict(std::move(child_flat), std::span(indices_v));

  duckdb::LogicalType list_type =
    duckdb::LogicalType::LIST(duckdb::LogicalType::BOOLEAN);
  duckdb::Vector vec(list_type, 3);
  duckdb::ListVector::Reserve(vec, 8);
  duckdb::ListVector::GetEntry(vec).Reference(child_dict);
  auto* entries = duckdb::ListVector::GetData(vec);
  entries[0] = {0, 3};
  entries[1] = {3, 2};
  entries[2] = {5, 3};
  duckdb::ListVector::SetListSize(vec, 8);

  CheckColumn(vec, list_type, 3);
}

TEST_F(DuckDBColumnSerializerTest, DictionaryEncodedListVarchar) {
  auto child_flat =
    MakeFlatVarchar({"alpha", "beta", "gamma long enough string", ""});
  std::vector<duckdb::sel_t> indices_v{3, 2, 1, 0, 0, 1, 2, 3};
  auto child_dict = MakeDict(std::move(child_flat), std::span(indices_v));

  duckdb::LogicalType list_type =
    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  duckdb::Vector vec(list_type, 3);
  duckdb::ListVector::Reserve(vec, 8);
  duckdb::ListVector::GetEntry(vec).Reference(child_dict);
  auto* entries = duckdb::ListVector::GetData(vec);
  entries[0] = {0, 3};
  entries[1] = {3, 2};
  entries[2] = {5, 3};
  duckdb::ListVector::SetListSize(vec, 8);

  CheckColumn(vec, list_type, 3);
}

TEST_F(DuckDBColumnSerializerTest, DictionaryEncodedListIntWithNulls) {
  // Dict child has nulls; some list entries are also null
  auto child_flat = MakeNullableFlat<int32_t>({1, std::nullopt, 3, 4});
  std::vector<duckdb::sel_t> indices_v{3, 2, 1, 0, 0, 1, 2, 3};
  auto child_dict = MakeDict(std::move(child_flat), std::span(indices_v));

  duckdb::LogicalType list_type =
    duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER);
  duckdb::Vector vec(list_type, 3);
  duckdb::ListVector::Reserve(vec, 8);
  duckdb::ListVector::GetEntry(vec).Reference(child_dict);
  auto* entries = duckdb::ListVector::GetData(vec);
  entries[0] = {0, 3};
  entries[1] = {3, 2};
  entries[2] = {5, 3};
  duckdb::ListVector::SetListSize(vec, 8);
  // Mark row 1 as null list
  duckdb::FlatVector::ValidityMutable(vec).SetInvalid(1);

  CheckColumn(vec, list_type, 3);
}

////////////////////////////////////////////////////////////////////////////////
/// STRUCT columns
////////////////////////////////////////////////////////////////////////////////

TEST_F(DuckDBColumnSerializerTest, ScalarStruct) {
  duckdb::child_list_t<duckdb::LogicalType> fields{
    {"foo", duckdb::LogicalType::INTEGER},
    {"bar", duckdb::LogicalType::BOOLEAN},
    {"bas", duckdb::LogicalType::VARCHAR},
    {"baf", duckdb::LogicalType::TIMESTAMP}};
  auto struct_type = duckdb::LogicalType::STRUCT(fields);

  auto vec = MakeStruct(
    struct_type,
    Vecs(MakeFlat<int32_t>({1, 2, 3, 4}),
         MakeFlat<bool>({true, false, true, false}),
         MakeFlatVarchar(
           {"", "a", "\0"sv, "long lomng glgkfklgfklgf sdflkjsdlfk"}),
         MakeFlat<duckdb::timestamp_t>(
           {duckdb::timestamp_t{123000}, duckdb::timestamp_t{55555000},
            duckdb::timestamp_t{22222000}, duckdb::timestamp_t{99999999000}})),
    4);

  CheckColumn(vec, struct_type, 4);
}

TEST_F(DuckDBColumnSerializerTest, ScalarStructNulls) {
  // Nullable fields + one null struct row
  duckdb::child_list_t<duckdb::LogicalType> fields{
    {"foo", duckdb::LogicalType::INTEGER},
    {"bar", duckdb::LogicalType::BOOLEAN},
    {"bas", duckdb::LogicalType::VARCHAR}};
  auto struct_type = duckdb::LogicalType::STRUCT(fields);

  // row 5 is a null struct (false = null)
  constexpr bool kNulls6[] = {true, true, true, true, true, false};
  auto vec = MakeStruct(
    struct_type,
    Vecs(MakeNullableFlat<int32_t>({std::nullopt, 1, 2, 3, 4, 5}),
         MakeNullableFlat<bool>({true, std::nullopt, false, true, false, true}),
         MakeNullableVarchar(
           {std::optional<std::string_view>{""},
            std::optional<std::string_view>{"a"}, std::nullopt,
            std::optional<std::string_view>{"longer string value here"},
            std::optional<std::string_view>{"x"},
            std::optional<std::string_view>{""}})),
    6, std::span<const bool>(kNulls6));

  CheckColumn(vec, struct_type, 6);
}

TEST_F(DuckDBColumnSerializerTest, ScalarStructNullsDictionary) {
  // Struct where each field is a dictionary-encoded vector
  duckdb::child_list_t<duckdb::LogicalType> fields{
    {"foo", duckdb::LogicalType::INTEGER},
    {"bar", duckdb::LogicalType::BOOLEAN}};
  auto struct_type = duckdb::LogicalType::STRUCT(fields);

  auto int_flat = MakeNullableFlat<int32_t>({std::nullopt, 1, 2, 3, 4, 5});
  std::vector<duckdb::sel_t> int_idx{5, 4, 3, 2, 1, 0, 1, 2};
  auto int_dict = MakeDict(std::move(int_flat), std::span(int_idx));

  auto bool_flat =
    MakeNullableFlat<bool>({true, std::nullopt, false, true, false, true});
  std::vector<duckdb::sel_t> bool_idx{5, 4, 3, 2, 1, 0, 1, 2};
  auto bool_dict = MakeDict(std::move(bool_flat), std::span(bool_idx));

  constexpr bool kNulls8[] = {true, true, true, false, true, true, false, true};
  auto vec =
    MakeStruct(struct_type, Vecs(std::move(int_dict), std::move(bool_dict)), 8,
               std::span<const bool>(kNulls8));

  CheckColumn(vec, struct_type, 8);
}

TEST_F(DuckDBColumnSerializerTest, ArrayStruct) {
  // Struct whose fields are LIST columns
  duckdb::child_list_t<duckdb::LogicalType> fields{
    {"ints", duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER)},
    {"flags", duckdb::LogicalType::LIST(duckdb::LogicalType::BOOLEAN)}};
  auto struct_type = duckdb::LogicalType::STRUCT(fields);

  auto vec = MakeStruct(
    struct_type,
    Vecs(MakeList<int32_t>({{1, 2, 3}, {3, 2, 1}, {555, 4545445, 0}, {1}}),
         MakeList<bool>({{true, false}, {}, {false, true}, {true}})),
    4);

  CheckColumn(vec, struct_type, 4);
}

TEST_F(DuckDBColumnSerializerTest, NestedStruct) {
  // STRUCT<outer_int INT, inner STRUCT<a INT, b VARCHAR>>
  duckdb::child_list_t<duckdb::LogicalType> inner_fields{
    {"a", duckdb::LogicalType::INTEGER}, {"b", duckdb::LogicalType::VARCHAR}};
  auto inner_type = duckdb::LogicalType::STRUCT(inner_fields);

  duckdb::child_list_t<duckdb::LogicalType> outer_fields{
    {"outer_int", duckdb::LogicalType::INTEGER}, {"inner", inner_type}};
  auto outer_type = duckdb::LogicalType::STRUCT(outer_fields);

  auto inner_vec =
    MakeStruct(inner_type,
               Vecs(MakeFlat<int32_t>({10, 20, 30, 40}),
                    MakeFlatVarchar({"alpha", "beta", "gamma", "delta"})),
               4);

  auto vec = MakeStruct(
    outer_type, Vecs(MakeFlat<int32_t>({1, 2, 3, 4}), std::move(inner_vec)), 4);

  CheckColumn(vec, outer_type, 4);
}

TEST_F(DuckDBColumnSerializerTest, NestedStructNulls) {
  // STRUCT<outer_int INT, inner STRUCT<a INT, b VARCHAR>>
  // Some outer rows are null, some inner structs are null, some inner fields
  // are null.
  duckdb::child_list_t<duckdb::LogicalType> inner_fields{
    {"a", duckdb::LogicalType::INTEGER}, {"b", duckdb::LogicalType::VARCHAR}};
  auto inner_type = duckdb::LogicalType::STRUCT(inner_fields);

  duckdb::child_list_t<duckdb::LogicalType> outer_fields{
    {"outer_int", duckdb::LogicalType::INTEGER}, {"inner", inner_type}};
  auto outer_type = duckdb::LogicalType::STRUCT(outer_fields);

  // 6 rows; inner struct is null for rows 1 and 4; row 5 is a null outer struct
  constexpr bool kInnerNulls[] = {true, false, true, true, false, true};
  auto inner_vec = MakeStruct(
    inner_type,
    Vecs(
      MakeNullableFlat<int32_t>({10, 0, std::nullopt, 30, 0, 40}),
      MakeNullableVarchar({"alpha", {}, "gamma", std::nullopt, {}, "delta"})),
    6, std::span<const bool>(kInnerNulls));

  constexpr bool kOuterNulls[] = {true, true, true, true, true, false};
  auto vec =
    MakeStruct(outer_type,
               Vecs(MakeNullableFlat<int32_t>({1, 2, std::nullopt, 4, 5, 0}),
                    std::move(inner_vec)),
               6, std::span<const bool>(kOuterNulls));

  CheckColumn(vec, outer_type, 6);
}

////////////////////////////////////////////////////////////////////////////////
/// MAP columns
////////////////////////////////////////////////////////////////////////////////

TEST_F(DuckDBColumnSerializerTest, MapIntToInt) {
  // 4 rows: {1:10,2:20}, {3:5}, {4:40}, {}
  auto key_type = duckdb::LogicalType::INTEGER;
  auto val_type = duckdb::LogicalType::INTEGER;
  auto map_type = duckdb::LogicalType::MAP(key_type, val_type);

  auto keys = MakeFlat<int32_t>({1, 2, 3, 4});
  auto vals = MakeFlat<int32_t>({10, 20, 5, 40});
  std::vector<duckdb::list_entry_t> entries{{0, 2}, {2, 1}, {3, 1}, {4, 0}};

  auto vec =
    MakeMap(map_type, std::move(keys), std::move(vals), std::span(entries));
  CheckColumn(vec, map_type, 4);
}

TEST_F(DuckDBColumnSerializerTest, MapIntToIntNullable) {
  // Nullable values inside the map
  auto map_type = duckdb::LogicalType::MAP(duckdb::LogicalType::INTEGER,
                                           duckdb::LogicalType::INTEGER);

  auto keys = MakeFlat<int32_t>({1, 2, 3, 4});
  auto vals = MakeNullableFlat<int32_t>({10, std::nullopt, std::nullopt, 40});
  std::vector<duckdb::list_entry_t> entries{{0, 2}, {2, 1}, {3, 1}, {4, 0}};

  auto vec =
    MakeMap(map_type, std::move(keys), std::move(vals), std::span(entries));
  CheckColumn(vec, map_type, 4);
}

TEST_F(DuckDBColumnSerializerTest, MapIntToBoolNullable) {
  auto map_type = duckdb::LogicalType::MAP(duckdb::LogicalType::INTEGER,
                                           duckdb::LogicalType::BOOLEAN);

  auto keys = MakeFlat<int32_t>({1, 2, 3, 4});
  auto vals = MakeNullableFlat<bool>({true, false, std::nullopt, false});
  std::vector<duckdb::list_entry_t> entries{{0, 2}, {2, 1}, {3, 1}, {4, 0}};

  auto vec =
    MakeMap(map_type, std::move(keys), std::move(vals), std::span(entries));
  CheckColumn(vec, map_type, 4);
}

TEST_F(DuckDBColumnSerializerTest, MapIntToVarcharNullable) {
  auto map_type = duckdb::LogicalType::MAP(duckdb::LogicalType::INTEGER,
                                           duckdb::LogicalType::VARCHAR);

  auto keys = MakeFlat<int32_t>({1, 2, 3, 4});
  auto vals =
    MakeNullableVarchar({std::optional<std::string_view>{"red"}, std::nullopt,
                         std::optional<std::string_view>{"green"},
                         std::optional<std::string_view>{"yellow"}});
  std::vector<duckdb::list_entry_t> entries{{0, 2}, {2, 1}, {3, 1}, {4, 0}};

  auto vec =
    MakeMap(map_type, std::move(keys), std::move(vals), std::span(entries));
  CheckColumn(vec, map_type, 4);
}

TEST_F(DuckDBColumnSerializerTest, MapIntToListInt) {
  // MAP<INT, LIST<INT>>
  auto val_elem_type = duckdb::LogicalType::INTEGER;
  auto val_type = duckdb::LogicalType::LIST(val_elem_type);
  auto map_type =
    duckdb::LogicalType::MAP(duckdb::LogicalType::INTEGER, val_type);

  auto keys = MakeFlat<int32_t>({1, 2, 3, 4});
  auto vals = MakeList<int32_t>({{1, 2, 3}, {4, 5}, {}, {7, 8, 9}});
  std::vector<duckdb::list_entry_t> entries{{0, 2}, {2, 2}, {4, 0}};

  auto vec =
    MakeMap(map_type, std::move(keys), std::move(vals), std::span(entries));
  CheckColumn(vec, map_type, 3);
}

TEST_F(DuckDBColumnSerializerTest, NullableMapRows) {
  // Some map rows are null (null map, not null entries)
  auto map_type = duckdb::LogicalType::MAP(duckdb::LogicalType::INTEGER,
                                           duckdb::LogicalType::INTEGER);

  auto keys = MakeFlat<int32_t>({1, 2, 3, 4});
  auto vals = MakeFlat<int32_t>({10, 20, 5, 40});
  std::vector<duckdb::list_entry_t> entries{{0, 2}, {2, 1}, {3, 1}, {4, 0}};
  // rows 1 and 3 are null maps
  constexpr bool kValid4[] = {true, false, true, false};

  auto vec = MakeMap(map_type, std::move(keys), std::move(vals),
                     std::span(entries), std::span<const bool>(kValid4));
  CheckColumn(vec, map_type, 4);
}

TEST_F(DuckDBColumnSerializerTest, MapDictionaryEncoded) {
  // MAP column where the entire map vector is dictionary-encoded
  auto map_type = duckdb::LogicalType::MAP(duckdb::LogicalType::INTEGER,
                                           duckdb::LogicalType::INTEGER);

  // Build base maps (4 distinct maps)
  auto keys_base = MakeFlat<int32_t>({1, 2, 3, 4, 5, 6});
  auto vals_base = MakeFlat<int32_t>({10, 20, 30, 40, 50, 60});
  std::vector<duckdb::list_entry_t> base_entries{
    {0, 2}, {2, 2}, {4, 1}, {5, 1}};
  auto base_map = MakeMap(map_type, std::move(keys_base), std::move(vals_base),
                          std::span(base_entries));

  // Dictionary-encode: 6 output rows referencing the 4 base maps
  std::vector<duckdb::sel_t> indices{0, 1, 2, 3, 1, 0};
  auto vec = MakeDict(std::move(base_map), std::span(indices));

  CheckColumn(vec, map_type, 6);
}

////////////////////////////////////////////////////////////////////////////////
/// GEOMETRY columns
///
/// GEOMETRY rides on PhysicalType::VARCHAR but is stored as raw WKB bytes --
/// no kStringPrefix disambiguation. These tests exercise the writer/reader
/// round-trip at the column-serializer level; downstream WKB parsing is
/// covered by libs/geo/wkb_parser_test.cpp and geo_analyzer_test.cpp.
///
/// Uses valid 2D Point WKB (21 bytes: byte-order + type + lng + lat) so the
/// tests remain correct if DuckDB later adds per-row GEOMETRY validation at
/// the Vector level.
////////////////////////////////////////////////////////////////////////////////

namespace {

// Build a valid little-endian 2D Point WKB for (lng, lat). 21 bytes:
//   0x01                -- little-endian
//   0x01 0x00 0x00 0x00 -- WKB type 1 (Point)
//   <lng:double LE>
//   <lat:double LE>
std::string MakeWkbPoint(double lng, double lat) {
  std::string out;
  out.reserve(21);
  out.push_back(static_cast<char>(1));  // LE
  uint32_t type = 1;                    // Point
  if (std::endian::native != std::endian::little) {
    type = std::byteswap(type);
  }
  out.append(reinterpret_cast<const char*>(&type), sizeof(type));
  auto push_le_double = [&](double d) {
    uint64_t raw;
    std::memcpy(&raw, &d, sizeof(raw));
    if (std::endian::native != std::endian::little) {
      raw = std::byteswap(raw);
    }
    out.append(reinterpret_cast<const char*>(&raw), sizeof(raw));
  };
  push_le_double(lng);
  push_le_double(lat);
  return out;
}

duckdb::Value WkbPointValue(double lng, double lat) {
  auto bytes = MakeWkbPoint(lng, lat);
  return duckdb::Value::GEOMETRY(
    reinterpret_cast<const duckdb::const_data_ptr_t>(bytes.data()),
    bytes.size());
}

duckdb::Vector MakeFlatGeometry(std::span<const duckdb::Value> values) {
  duckdb::Vector result(duckdb::LogicalType::GEOMETRY(), values.size());
  for (duckdb::idx_t i = 0; i < values.size(); ++i) {
    result.SetValue(i, values[i]);
  }
  return result;
}

}  // namespace

TEST_F(DuckDBColumnSerializerTest, FlatGeometry) {
  duckdb::Value vals[] = {WkbPointValue(6.5, 50.3), WkbPointValue(0.0, 0.0),
                          WkbPointValue(-122.4, 37.7),
                          WkbPointValue(180.0, 90.0),
                          WkbPointValue(-180.0, -90.0)};
  auto vec = MakeFlatGeometry(std::span(vals));
  CheckColumn(vec, duckdb::LogicalType::GEOMETRY(), 5);
}

TEST_F(DuckDBColumnSerializerTest, FlatGeometryNulls) {
  duckdb::Vector vec(duckdb::LogicalType::GEOMETRY(), 5);
  vec.SetValue(0, WkbPointValue(6.5, 50.3));
  duckdb::FlatVector::ValidityMutable(vec).SetInvalid(1);
  vec.SetValue(2, WkbPointValue(0.0, 0.0));
  duckdb::FlatVector::ValidityMutable(vec).SetInvalid(3);
  vec.SetValue(4, WkbPointValue(-122.4, 37.7));
  CheckColumn(vec, duckdb::LogicalType::GEOMETRY(), 5);
}

TEST_F(DuckDBColumnSerializerTest, ConstantGeometry) {
  duckdb::Vector vec(WkbPointValue(6.5, 50.3), duckdb::count_t(6));
  CheckColumn(vec, duckdb::LogicalType::GEOMETRY(), 6);
}

TEST_F(DuckDBColumnSerializerTest, ListGeometry) {
  // LIST<GEOMETRY>: 4 rows of Point lists (including one empty).
  duckdb::LogicalType elem_type = duckdb::LogicalType::GEOMETRY();
  duckdb::LogicalType list_type = duckdb::LogicalType::LIST(elem_type);

  std::vector<std::vector<duckdb::Value>> rows = {
    {WkbPointValue(1.0, 2.0), WkbPointValue(3.0, 4.0)},
    {},
    {WkbPointValue(5.0, 6.0)},
    {WkbPointValue(7.0, 8.0), WkbPointValue(9.0, 10.0),
     WkbPointValue(11.0, 12.0)}};
  duckdb::idx_t total = 0;
  for (const auto& r : rows) {
    total += r.size();
  }

  duckdb::Vector vec(list_type, rows.size());
  duckdb::ListVector::Reserve(vec, total);
  auto& child = duckdb::ListVector::GetEntry(vec);
  auto* entries = duckdb::ListVector::GetData(vec);
  duckdb::idx_t offset = 0;
  for (duckdb::idx_t i = 0; i < rows.size(); ++i) {
    entries[i] = {offset, rows[i].size()};
    for (const auto& v : rows[i]) {
      child.SetValue(offset++, v);
    }
  }
  duckdb::ListVector::SetListSize(vec, total);

  CheckColumn(vec, list_type, rows.size());
}

TEST_F(DuckDBColumnSerializerTest, StructWithGeometry) {
  duckdb::child_list_t<duckdb::LogicalType> fields{
    {"id", duckdb::LogicalType::INTEGER},
    {"geom", duckdb::LogicalType::GEOMETRY()},
    {"flag", duckdb::LogicalType::BOOLEAN}};
  auto struct_type = duckdb::LogicalType::STRUCT(fields);

  duckdb::Value geoms[] = {WkbPointValue(6.5, 50.3), WkbPointValue(0.0, 0.0),
                           WkbPointValue(-122.4, 37.7)};
  auto vec = MakeStruct(
    struct_type,
    Vecs(MakeFlat<int32_t>({1, 2, 3}), MakeFlatGeometry(std::span(geoms)),
         MakeFlat<bool>({true, false, true})),
    3);
  CheckColumn(vec, struct_type, 3);
}

TEST_F(DuckDBColumnSerializerTest, MapIntToGeometry) {
  auto map_type = duckdb::LogicalType::MAP(duckdb::LogicalType::INTEGER,
                                           duckdb::LogicalType::GEOMETRY());

  auto keys = MakeFlat<int32_t>({1, 2, 3, 4});
  duckdb::Value geoms[] = {WkbPointValue(1.0, 1.0), WkbPointValue(2.0, 2.0),
                           WkbPointValue(3.0, 3.0), WkbPointValue(4.0, 4.0)};
  auto vals = MakeFlatGeometry(std::span(geoms));
  std::vector<duckdb::list_entry_t> entries{{0, 2}, {2, 1}, {3, 1}, {4, 0}};

  auto vec =
    MakeMap(map_type, std::move(keys), std::move(vals), std::span(entries));
  CheckColumn(vec, map_type, 4);
}

////////////////////////////////////////////////////////////////////////////////
/// Multi-column test
////////////////////////////////////////////////////////////////////////////////

TEST_F(DuckDBColumnSerializerTest, MulticolumnScalar) {
  constexpr duckdb::idx_t kRows = 10;

  // Build sequential int32 PK DataChunk
  auto& pk_chunk = MakePKChunk(kRows);
  auto row_keys = BuildRowKeys(pk_chunk);

  // Prepare three columns
  std::vector<int32_t> int_vals(kRows);
  std::iota(int_vals.begin(), int_vals.end(), 0);
  auto int_vec = MakeFlat<int32_t>(std::span(int_vals));

  auto bool_vec = MakeFlat<bool>(
    {true, false, true, false, true, false, true, false, true, false});

  auto varchar_vec = MakeFlatVarchar(
    {"a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9"});

  // Write all three in one transaction
  rocksdb::TransactionOptions txn_opts;
  txn_opts.skip_concurrency_control = true;
  rocksdb::WriteOptions wo;
  std::unique_ptr<rocksdb::Transaction> txn(
    _db->BeginTransaction(wo, txn_opts, nullptr));
  ASSERT_NE(txn, nullptr);
  query::Transaction sdb_txn{*_conn.context, std::move(txn)};

  WriteColumn(sdb_txn, row_keys, catalog::Column::Id{0}, int_vec,
              duckdb::LogicalType::INTEGER, kRows);
  WriteColumn(sdb_txn, row_keys, catalog::Column::Id{1}, bool_vec,
              duckdb::LogicalType::BOOLEAN, kRows);
  WriteColumn(sdb_txn, row_keys, catalog::Column::Id{2}, varchar_vec,
              duckdb::LogicalType::VARCHAR, kRows);
  ASSERT_TRUE(sdb_txn.GetRocksDBTransaction().Commit().ok());
  sdb_txn.Destroy();

  // Verify each column independently
  auto int_out =
    ReadColumn(duckdb::LogicalType::INTEGER, kRows, catalog::Column::Id{0});
  auto bool_out =
    ReadColumn(duckdb::LogicalType::BOOLEAN, kRows, catalog::Column::Id{1});
  auto varchar_out =
    ReadColumn(duckdb::LogicalType::VARCHAR, kRows, catalog::Column::Id{2});

  for (duckdb::idx_t i = 0; i < kRows; ++i) {
    SCOPED_TRACE(testing::Message("row ") << i);
    EXPECT_EQ(int_vec.GetValue(i), int_out.GetValue(i));
    EXPECT_EQ(bool_vec.GetValue(i), bool_out.GetValue(i));
    EXPECT_EQ(varchar_vec.GetValue(i), varchar_out.GetValue(i));
  }
}

}  // namespace sdb::connector
