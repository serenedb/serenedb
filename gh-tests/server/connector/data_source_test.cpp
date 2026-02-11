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

#include "connector/common.h"
#include "connector/data_sink.hpp"
#include "connector/data_source.hpp"
#include "connector/primary_key.hpp"
#include "connector/serenedb_connector.hpp"
#include "gtest/gtest.h"
#include "iresearch/utils/bytes_utils.hpp"
#include "rocksdb/utilities/transaction_db.h"

using namespace sdb::connector;

namespace {

constexpr sdb::ObjectId kObjectKey{1};
class DataSourceTest : public ::testing::Test,
                       public velox::test::VectorTestBase {
 public:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() final {
    rocksdb::TransactionDBOptions transaction_options;
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_families;
    rocksdb::Options db_options;
    db_options.OptimizeForSmallDb();
    db_options.create_if_missing = true;
    cf_families.emplace_back(rocksdb::kDefaultColumnFamilyName, db_options);
    _path = testing::TempDir() + "/" +
            ::testing::UnitTest::GetInstance()->current_test_info()->name() +
            "_XXXXXX";
    ASSERT_NE(mkdtemp(_path.data()), nullptr);
    db_options.wal_dir = _path + "/journals";
    rocksdb::Status status = rocksdb::TransactionDB::Open(
      db_options, transaction_options, _path, cf_families, &_cf_handles, &_db);
    ASSERT_TRUE(status.ok());
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

  void MakeRocksDBWrite(velox::RowVectorPtr data, sdb::ObjectId object_key) {
    std::vector<velox::column_index_t> pk;
    std::vector<sdb::catalog::Column::Id> column_ids;
    column_ids.reserve(data->type()->asRow().size());
    for (velox::vector_size_t i = 0; i < data->type()->asRow().size(); ++i) {
      if (!data->childAt(i)->mayHaveNulls()) {
        pk.push_back(i);
      }
      column_ids.emplace_back(i);
    }
    rocksdb::TransactionOptions trx_opts;
    rocksdb::WriteOptions wo;
    std::unique_ptr<rocksdb::Transaction> transaction{
      _db->BeginTransaction(wo, trx_opts, nullptr)};
    ASSERT_NE(transaction, nullptr);
    sdb::connector::RocksDBDataSink sink(*transaction, *_cf_handles.front(),
                                         *pool_.get(), object_key, pk,
                                         column_ids);
    sink.appendData(data);
    while (!sink.finish()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_TRUE(transaction->Commit().ok());
  }

  void MakeRocksDBWriteRead(velox::RowVectorPtr data,
                            sdb::ObjectId object_key) {
    MakeRocksDBWrite(data, object_key);
    std::vector<sdb::catalog::Column::Id> column_ids;
    column_ids.reserve(data->type()->asRow().size());
    for (velox::vector_size_t i = 0; i < data->type()->asRow().size(); ++i) {
      column_ids.emplace_back(i);
    }
    sdb::connector::RocksDBDataSource source(
      *pool_.get(), nullptr, *_db, *_cf_handles.front(),
      std::shared_ptr<const velox::RowType>(
        std::shared_ptr<const velox::RowType>{nullptr}, &data->type()->asRow()),
      column_ids, object_key);

    // read as single batch
    {
      source.addSplit(std::make_shared<sdb::connector::SereneDBConnectorSplit>(
        "test_connector"));
      auto future = velox::ContinueFuture::makeEmpty();

      auto read = source.next(data->size(), future);
      ASSERT_TRUE(read.has_value());
      ASSERT_TRUE(read.value() != nullptr);
      ASSERT_TRUE(future.isReady());
      facebook::velox::test::assertEqualVectors(data, read.value());
      // nulls should be inited only if necessary
      if (read.value()->mayHaveNulls()) {
        bool found_null = false;
        for (velox::vector_size_t i = 0; i < read.value()->size(); ++i) {
          if (read.value()->isNullAt(i)) {
            found_null = true;
            break;
          }
        }
        ASSERT_TRUE(found_null);
      }
      const auto final_res = source.next(data->size(), future);
      ASSERT_TRUE(final_res.has_value());
      ASSERT_EQ(final_res.value(), nullptr);
      ASSERT_TRUE(future.isReady());
    }

    // read by one row
    {
      source.addSplit(std::make_shared<sdb::connector::SereneDBConnectorSplit>(
        "test_connector"));
      auto future = velox::ContinueFuture::makeEmpty();
      for (velox::vector_size_t i = 0; i < data->size(); ++i) {
        auto read = source.next(1, future);
        ASSERT_TRUE(read.has_value());
        ASSERT_TRUE(future.isReady());
        ASSERT_TRUE(read.value() != nullptr);
        ASSERT_EQ(read.value()->size(), 1);
        ASSERT_TRUE(data->equalValueAt(read.value().get(), i, 0));
        // nulls should be inited only if necessary
        if (read.value()->mayHaveNulls()) {
          bool found_null = false;
          for (velox::vector_size_t i = 0; i < read.value()->size(); ++i) {
            if (read.value()->isNullAt(i)) {
              found_null = true;
              break;
            }
          }
          ASSERT_TRUE(found_null);
        }
      }
      const auto final_res = source.next(1, future);
      ASSERT_TRUE(final_res.has_value());
      ASSERT_EQ(final_res.value(), nullptr);
      ASSERT_TRUE(future.isReady());
    }
  }

 protected:
  std::string _path;
  std::vector<rocksdb::ColumnFamilyDescriptor> _cf_families;
  rocksdb::TransactionDB* _db{nullptr};
  std::vector<rocksdb::ColumnFamilyHandle*> _cf_handles;
};

TEST_F(DataSourceTest, test_tableReadFlatScalar) {
  std::vector<int32_t> data = {1, 2,  3,  4,  5,  6,  7, 8,
                               9, 10, 11, 12, 13, 14, 15};
  std::vector<int32_t> data_col2 = {11, 12,  13,  14,  15,  16,  17, 18,
                                    19, 110, 111, 112, 113, 114, 115};
  std::vector<bool> data_col3 = {true,  true, true,  false, true,
                                 true,  true, true,  true,  true,
                                 false, true, false, true,  true};
  std::vector<velox::Timestamp> data_col4 = {
    velox::Timestamp(1622548800, 0), velox::Timestamp(1622635200, 0),
    velox::Timestamp(1622721600, 0), velox::Timestamp(1622808000, 0),
    velox::Timestamp(1622894400, 0), velox::Timestamp(1622980800, 0),
    velox::Timestamp(1623067200, 0), velox::Timestamp(1623153600, 0),
    velox::Timestamp(1623240000, 0), velox::Timestamp(1623326400, 0),
    velox::Timestamp(1623412800, 0), velox::Timestamp(1623499200, 0),
    velox::Timestamp(1623585600, 0), velox::Timestamp(1623672000, 0),
    velox::Timestamp(1623758400, 0)};
  auto row_data = makeRowVector(
    {"col", "col2", "col3", "col4"},
    {makeFlatVector<int32_t>(data), makeFlatVector<int32_t>(data_col2),
     makeFlatVector<bool>(data_col3),
     makeFlatVector<velox::Timestamp>(data_col4)});
  MakeRocksDBWriteRead(row_data, kObjectKey);

  // same column name but different object
  std::vector<int32_t> data2 = {10, 20, 30, 40, 50};
  auto row_data2 = makeRowVector({"col"}, {makeFlatVector<int32_t>(data2)});
  sdb::ObjectId object_key2{2};
  MakeRocksDBWriteRead(row_data2, object_key2);
}

TEST_F(DataSourceTest, test_tableReadString) {
  std::vector<int32_t> key_data = {1, 2, 3, 4, 5, 6};
  std::vector<std::string> data = {
    "\0one",  "",     " three long foxes jumps over the brown lazy dogs ",
    "four\0", "five", "\0"};
  auto row_data = makeRowVector(
    {"id", "strcol"},
    {makeFlatVector<int32_t>(key_data), makeFlatVector<std::string>(data)});
  MakeRocksDBWriteRead(row_data, kObjectKey);
}

TEST_F(DataSourceTest, test_tableReadWithNulls) {
  std::vector<int32_t> data = {1, 2,  3,  4,  5,  6,  7, 8,
                               9, 10, 11, 12, 13, 14, 15};
  std::vector<std::optional<int32_t>> data_col2 = {
    11, 12,           13,  14,           15,  16,  17, 18,
    19, std::nullopt, 111, std::nullopt, 113, 114, 115};
  std::vector<std::optional<bool>> data_col3 = {
    true, true, true,  false,        true,  true, true, true,
    true, true, false, std::nullopt, false, true, true};
  std::vector<std::optional<velox::Timestamp>> data_col4 = {
    velox::Timestamp(1622548800, 0),
    velox::Timestamp(1622635200, 0),
    velox::Timestamp(1622721600, 0),
    velox::Timestamp(1622808000, 0),
    velox::Timestamp(1622894400, 0),
    velox::Timestamp(1622980800, 0),
    std::nullopt,
    velox::Timestamp(1623153600, 0),
    velox::Timestamp(1623240000, 0),
    velox::Timestamp(1623326400, 0),
    velox::Timestamp(1623412800, 0),
    velox::Timestamp(1623499200, 0),
    velox::Timestamp(1623585600, 0),
    velox::Timestamp(1623672000, 0),
    velox::Timestamp(1623758400, 0)};
  std::vector<std::optional<std::string>> data_col5 = {
    "\0one",
    "",
    " three long foxes jumps over the brown lazy dogs ",
    "four\0",
    std::nullopt,
    "\0",
    "seven",
    "eight",
    std::nullopt,
    std::nullopt,
    "11",
    "1232323",
    std::nullopt,
    "foor",
    "bar"};
  auto row_data = makeRowVector(
    {"col", "col2", "col3", "col4", "col5"},
    {makeFlatVector<int32_t>(data), makeNullableFlatVector<int32_t>(data_col2),
     makeNullableFlatVector<bool>(data_col3),
     makeNullableFlatVector<velox::Timestamp>(data_col4),
     makeNullableFlatVector<std::string>(data_col5)});
  MakeRocksDBWriteRead(row_data, kObjectKey);
}

TEST_F(DataSourceTest, test_tableReadUnknown) {
  std::vector<int32_t> key_data = {1, 2, 3, 4, 5, 6};
  auto flat_vector =
    velox::BaseVector::create(velox::UNKNOWN(), key_data.size(), pool_.get());
  auto row_data = makeRowVector(
    {"id", "unkcol"}, {makeFlatVector<int32_t>(key_data), flat_vector});
  MakeRocksDBWriteRead(row_data, kObjectKey);
}

TEST_F(DataSourceTest, test_tableReadEmptyOutput) {
  std::vector<int32_t> key_data = {1, 2, 3, 4, 5, 6};
  std::vector<std::string> data = {
    "\0one",  "",     " three long foxes jumps over the brown lazy dogs ",
    "four\0", "five", "\0"};
  auto row_data = makeRowVector(
    {"id", "strcol"},
    {makeFlatVector<int32_t>(key_data), makeFlatVector<std::string>(data)});
  auto row_type_empty = velox::ROW({});
  MakeRocksDBWrite(row_data, kObjectKey);
  sdb::connector::RocksDBDataSource source(*pool_.get(), nullptr, *_db,
                                           *_cf_handles.front(), row_type_empty,
                                           {0}, kObjectKey);

  // read as single batch
  {
    source.addSplit(std::make_shared<sdb::connector::SereneDBConnectorSplit>(
      "test_connector"));
    auto future = velox::ContinueFuture::makeEmpty();

    auto read = source.next(row_data->size(), future);
    ASSERT_TRUE(read.has_value());
    ASSERT_TRUE(read.value() != nullptr);
    ASSERT_TRUE(future.isReady());
    ASSERT_EQ(read.value()->size(), row_data->size());
    ASSERT_EQ(read.value()->type()->size(), 0);
    const auto final_res = source.next(row_data->size(), future);
    ASSERT_TRUE(final_res.has_value());
    ASSERT_EQ(final_res.value(), nullptr);
    ASSERT_TRUE(future.isReady());
  }

  // read by one row
  {
    source.addSplit(std::make_shared<sdb::connector::SereneDBConnectorSplit>(
      "test_connector"));
    auto future = velox::ContinueFuture::makeEmpty();
    for (velox::vector_size_t i = 0; i < row_data->size(); ++i) {
      auto read = source.next(1, future);
      ASSERT_TRUE(read.has_value());
      ASSERT_TRUE(future.isReady());
      ASSERT_TRUE(read.value() != nullptr);
      ASSERT_EQ(read.value()->size(), 1);
      ASSERT_EQ(read.value()->type()->size(), 0);
    }
    const auto final_res = source.next(1, future);
    ASSERT_TRUE(final_res.has_value());
    ASSERT_EQ(final_res.value(), nullptr);
    ASSERT_TRUE(future.isReady());
  }
}

}  // namespace
