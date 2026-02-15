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
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/scorers.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/store/memory_directory.hpp>

#include "connector/common.h"
#include "connector/data_sink.hpp"
#include "connector/key_utils.hpp"
#include "connector/primary_key.hpp"
#include "connector/search_data_source.hpp"
#include "connector/search_sink_writer.hpp"
#include "connector/serenedb_connector.hpp"
#include "gtest/gtest.h"
#include "iresearch/utils/bytes_utils.hpp"
#include "rocksdb/utilities/transaction_db.h"

using namespace sdb;
using namespace sdb::connector;
using namespace sdb::connector::search;

namespace {

constexpr ObjectId kObjectKey{123456};

class DataSourceWithSearchTest : public ::testing::Test,
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
    _codec = irs::formats::Get("1_5simd");
    _data_writer =
      irs::IndexWriter::Make(_dir, _codec, irs::kOmCreate, options);
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
    _data_writer.reset();
  }

  size_t GetTotalRocksDBKeys() {
    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> it{
      _db->NewIterator(read_options, _cf_handles.front())};
    size_t count = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      count++;
    }
    return count;
  }

  void PrepareRocksDBWrite(
    const velox::RowVectorPtr& data, std::vector<ColumnInfo> all_column_oids,
    ObjectId object_key, const std::vector<velox::column_index_t>& pk,
    std::unique_ptr<rocksdb::Transaction>& data_transaction,
    irs::IndexWriter::Transaction& index_transaction,
    primary_key::Keys& written_row_keys) {
    rocksdb::TransactionOptions trx_opts;
    trx_opts.skip_concurrency_control = true;
    trx_opts.lock_timeout = 100;
    rocksdb::WriteOptions wo;
    data_transaction.reset(_db->BeginTransaction(wo, trx_opts, nullptr));
    index_transaction = _data_writer->GetBatch();
    ASSERT_NE(data_transaction, nullptr);
    std::vector<std::unique_ptr<SinkInsertWriter>> index_writers;
    index_writers.emplace_back(
      std::make_unique<connector::search::SearchSinkInsertWriter>(
        index_transaction));
    primary_key::Create(*data, pk, written_row_keys);
    size_t rows_affected = 0;
    RocksDBInsertDataSink sink("", *data_transaction, *_cf_handles.front(),
                               *pool_.get(), object_key, pk, all_column_oids,
                               WriteConflictPolicy::Replace, rows_affected,
                               std::move(index_writers));
    sink.appendData(data);
    while (!sink.finish()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  void MakeRocksDBWrite(std::vector<std::string> names,
                        std::vector<velox::VectorPtr> data,
                        std::vector<ColumnInfo> all_column_oids,
                        ObjectId& object_key,
                        primary_key::Keys& written_row_keys) {
    object_key = kObjectKey;
    auto row_data = makeRowVector(names, data);
    std::unique_ptr<rocksdb::Transaction> transaction;
    irs::IndexWriter::Transaction index_transaction;
    std::vector<velox::column_index_t> pk = {0};
    PrepareRocksDBWrite(row_data, all_column_oids, object_key, pk, transaction,
                        index_transaction, written_row_keys);
    ASSERT_TRUE(index_transaction.Valid());
    ASSERT_TRUE(transaction->Commit().ok());
    ASSERT_TRUE(index_transaction.Commit());
    ASSERT_TRUE(_data_writer->Commit());
  }

 protected:
  std::string _path;
  std::vector<rocksdb::ColumnFamilyDescriptor> _cf_families;
  rocksdb::TransactionDB* _db{nullptr};
  std::vector<rocksdb::ColumnFamilyHandle*> _cf_handles;
  irs::Format::ptr _codec;
  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _data_writer;
};

TEST_F(DataSourceWithSearchTest, test_ReadSingleSegment) {
  std::vector<sdb::catalog::Column::Id> all_column_oids = {0, 1, 2};
  std::vector<std::string> names = {"id", "value", "description"};
  std::vector<ColumnInfo> all_columns = {{.id = 0, .name = names[0]},
                                         {.id = 1, .name = names[1]},
                                         {.id = 2, .name = names[2]}};

  std::vector<velox::TypePtr> types = {velox::INTEGER(), velox::VARCHAR(),
                                       velox::VARCHAR()};

  std::vector<velox::VectorPtr> data = {
    makeFlatVector<int32_t>({1, 42, 100, 9001}),
    makeFlatVector<velox::StringView>({"1", "42", "100", "9001"}),
    makeFlatVector<velox::StringView>(
      {"value1", "value2", "value3", "value4"})};
  ObjectId object_key;
  primary_key::Keys written_row_keys{*pool_.get()};
  MakeRocksDBWrite(names, data, all_columns, object_key, written_row_keys);
  auto or_filter = std::make_unique<irs::Or>();
  {
    auto& term_filter = or_filter->add<irs::ByTerm>();
    *term_filter.mutable_field() =
      std::string{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9};
    term_filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("100"));
  }
  {
    auto& term_filter = or_filter->add<irs::ByTerm>();
    *term_filter.mutable_field() =
      std::string{"\x00\x00\x00\x00\x00\x00\x00\x02\x03", 9};
    term_filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("value1"));
  }
  auto reader = irs::DirectoryReader(_dir, _codec);
  auto query = or_filter->prepare({.index = reader});
  SearchDataSource source(*pool(), nullptr, *_db, *_cf_handles.front(),
                          velox::ROW(names, types), all_column_oids,
                          all_column_oids[0], kObjectKey, reader, *query);
  auto expected =
    makeRowVector({makeFlatVector<int32_t>({1, 100}),
                   makeFlatVector<velox::StringView>({"1", "100"}),
                   makeFlatVector<velox::StringView>({"value1", "value3"})});
  {
    source.addSplit(std::make_shared<SereneDBConnectorSplit>("test_connector"));
    auto future = velox::ContinueFuture::makeEmpty();

    auto read = source.next(10, future);
    ASSERT_TRUE(read.has_value());
    ASSERT_TRUE(read.value() != nullptr);
    ASSERT_TRUE(future.isReady());
    facebook::velox::test::assertEqualVectors(expected, read.value());
    const auto final_res = source.next(10, future);
    ASSERT_TRUE(final_res.has_value());
    ASSERT_EQ(final_res.value(), nullptr);
    ASSERT_TRUE(future.isReady());
  }
}

TEST_F(DataSourceWithSearchTest, test_ReadManySegments) {
  std::vector<sdb::catalog::Column::Id> all_column_oids = {0, 1, 2};
  std::vector<std::string> names = {"id", "value", "description"};
  std::vector<ColumnInfo> all_columns = {{.id = 0, .name = names[0]},
                                         {.id = 1, .name = names[1]},
                                         {.id = 2, .name = names[2]}};

  std::vector<velox::TypePtr> types = {velox::INTEGER(), velox::VARCHAR(),
                                       velox::VARCHAR()};
  constexpr int kSegmentCount = 100;
  int32_t pk = 0;
  for (int segment = 0; segment < kSegmentCount; ++segment) {
    std::vector<velox::VectorPtr> data = {
      makeFlatVector<int32_t>({pk++, pk++, pk++, pk++}),
      makeFlatVector<velox::StringView>({"1", "42", "100", "9001"}),
      makeFlatVector<velox::StringView>(
        {"value1", "value2", "value3", "value4"})};
    ObjectId object_key;
    primary_key::Keys written_row_keys{*pool_.get()};
    MakeRocksDBWrite(names, data, all_columns, object_key, written_row_keys);
  }
  auto or_filter = std::make_unique<irs::Or>();
  {
    auto& term_filter = or_filter->add<irs::ByTerm>();
    *term_filter.mutable_field() =
      std::string{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9};
    term_filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("100"));
  }
  {
    auto& term_filter = or_filter->add<irs::ByTerm>();
    *term_filter.mutable_field() =
      std::string{"\x00\x00\x00\x00\x00\x00\x00\x02\x03", 9};
    term_filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("value1"));
  }
  auto reader = irs::DirectoryReader(_dir, _codec);
  auto query = or_filter->prepare({.index = reader});
  SearchDataSource source(*pool(), nullptr, *_db, *_cf_handles.front(),
                          velox::ROW(names, types), all_column_oids,
                          all_column_oids[0], kObjectKey, reader, *query);
  const auto expected =
    makeRowVector({makeFlatVector<velox::StringView>({"1", "100"}),
                   makeFlatVector<velox::StringView>({"value1", "value3"})});
  // not even batch size
  {
    size_t total = 0;
    source.addSplit(std::make_shared<SereneDBConnectorSplit>("test_connector"));
    do {
      auto future = velox::ContinueFuture::makeEmpty();

      auto read = source.next(11, future);
      ASSERT_TRUE(read.has_value());
      ASSERT_TRUE(future.isReady());
      if (read.value() == nullptr) {
        break;
      }
      total += read.value()->size();
      for (velox::vector_size_t i = 0; i < read.value()->size(); ++i) {
        // we do not care about order. Just validate all records actually match
        // filter
        ASSERT_TRUE((read.value()->childAt(1)->equalValueAt(
                       expected->childAt(0).get(), i, 0) &&
                     read.value()->childAt(2)->equalValueAt(
                       expected->childAt(1).get(), i, 0)) ||
                    (read.value()->childAt(1)->equalValueAt(
                       expected->childAt(0).get(), i, 1) &&
                     read.value()->childAt(2)->equalValueAt(
                       expected->childAt(1).get(), i, 1)));
      }
    } while (true);
    // two docs per segment
    ASSERT_EQ(total, kSegmentCount * 2);
  }

  // even batch size
  {
    size_t total = 0;
    source.addSplit(std::make_shared<SereneDBConnectorSplit>("test_connector"));
    do {
      auto future = velox::ContinueFuture::makeEmpty();

      auto read = source.next(10, future);
      ASSERT_TRUE(read.has_value());
      ASSERT_TRUE(future.isReady());
      if (read.value() == nullptr) {
        break;
      }
      total += read.value()->size();
      for (velox::vector_size_t i = 0; i < read.value()->size(); ++i) {
        // we do not care about order. Just validate all records actually match
        // filter
        ASSERT_TRUE((read.value()->childAt(1)->equalValueAt(
                       expected->childAt(0).get(), i, 0) &&
                     read.value()->childAt(2)->equalValueAt(
                       expected->childAt(1).get(), i, 0)) ||
                    (read.value()->childAt(1)->equalValueAt(
                       expected->childAt(0).get(), i, 1) &&
                     read.value()->childAt(2)->equalValueAt(
                       expected->childAt(1).get(), i, 1)));
      }
    } while (true);
    // two docs per segment
    ASSERT_EQ(total, kSegmentCount * 2);
  }

  // by one doc
  {
    size_t total = 0;
    source.addSplit(std::make_shared<SereneDBConnectorSplit>("test_connector"));
    do {
      auto future = velox::ContinueFuture::makeEmpty();

      auto read = source.next(1, future);
      ASSERT_TRUE(read.has_value());
      ASSERT_TRUE(future.isReady());
      if (read.value() == nullptr) {
        break;
      }
      total += read.value()->size();
      for (velox::vector_size_t i = 0; i < read.value()->size(); ++i) {
        // we do not care about order. Just validate all records actually match
        // filter
        ASSERT_TRUE((read.value()->childAt(1)->equalValueAt(
                       expected->childAt(0).get(), i, 0) &&
                     read.value()->childAt(2)->equalValueAt(
                       expected->childAt(1).get(), i, 0)) ||
                    (read.value()->childAt(1)->equalValueAt(
                       expected->childAt(0).get(), i, 1) &&
                     read.value()->childAt(2)->equalValueAt(
                       expected->childAt(1).get(), i, 1)));
      }
    } while (true);
    // two docs per segment
    ASSERT_EQ(total, kSegmentCount * 2);
  }
}

}  // namespace
