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
#include "connector/data_source.hpp"
#include "connector/key_utils.hpp"
#include "connector/primary_key.hpp"
#include "connector/search_remove_filter.hpp"
#include "connector/search_sink_writer.hpp"
#include "connector/serenedb_connector.hpp"
#include "gtest/gtest.h"
#include "iresearch/utils/bytes_utils.hpp"
#include "rocksdb/utilities/transaction_db.h"

using namespace sdb;
using namespace sdb::connector;

namespace {

constexpr ObjectId kObjectKey{123456};

class DataSinkWithSearchTest : public ::testing::Test,
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
    _codec = irs::formats::Get("1_5avx");
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

  void VerifyRocksDB(
    velox::BaseVector* left, velox::BaseVector* right,
    std::span<std::pair<velox::vector_size_t, velox::vector_size_t>> idxs) {
    for (const auto& idx : idxs) {
      ASSERT_TRUE(left->equalValueAt(right, idx.first, idx.second))
        << "at left index " << idx.first << " and right index " << idx.second
        << "\nLeft value: " << left->toString(idx.first)
        << "\nRight value: " << right->toString(idx.second);
    }
  }

  void VerifyRow(std::string_view pk_value,
                 std::string_view expected_description,
                 std::string_view expected_value, irs::IndexReader& reader,
                 bool must_exist = true) {
    SCOPED_TRACE(testing::Message("Failed SEARCH FOR  desciprtion ")
                 << expected_description << " AND value " << expected_value
                 << " must_exists " << must_exist);
    irs::And and_filter;
    {
      auto& term_filter = and_filter.add<irs::ByTerm>();
      *term_filter.mutable_field() =
        std::string{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9};
      term_filter.mutable_options()->term =
        irs::ViewCast<irs::byte_type>(expected_value);
    }
    {
      auto& term_filter = and_filter.add<irs::ByTerm>();
      *term_filter.mutable_field() =
        std::string{"\x00\x00\x00\x00\x00\x00\x00\x02\x03", 9};
      term_filter.mutable_options()->term =
        irs::ViewCast<irs::byte_type>(expected_description);
    }

    auto prepared = and_filter.prepare({.index = reader});
    size_t count = 0;
    for (auto& segment : reader) {
      auto docs =
        segment.mask(prepared->execute({.segment = segment, .scorers = {}}));
      while (docs->next()) {
        const auto* pk_column = segment.column(connector::search::kPkFieldName);
        ASSERT_NE(nullptr, pk_column);
        auto pk_values_itr = pk_column->iterator(irs::ColumnHint::Normal);
        ASSERT_NE(nullptr, pk_values_itr);
        auto* actual_pk_value = irs::get<irs::PayAttr>(*pk_values_itr);
        ASSERT_NE(nullptr, actual_pk_value);
        auto pk_seeked = pk_values_itr->seek(docs->value());
        ASSERT_EQ(docs->value(), pk_seeked);
        ASSERT_EQ(pk_value, irs::ViewCast<char>(actual_pk_value->value));
        ++count;
      }
    }
    if (must_exist) {
      ASSERT_EQ(count, 1);
    } else {
      ASSERT_EQ(count, 0);
    }
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

  void PrepareRocksDBUpdate(
    const velox::RowVectorPtr& data, std::vector<ColumnInfo> data_column_oids,
    velox::RowTypePtr table_row_type,
    std::vector<sdb::catalog::Column::Id> all_column_oids, ObjectId object_key,
    const std::vector<velox::column_index_t>& pk,
    std::unique_ptr<rocksdb::Transaction>& data_transaction,
    irs::IndexWriter::Transaction& index_transaction, bool update_pk) {
    rocksdb::TransactionOptions trx_opts;
    trx_opts.skip_concurrency_control = true;
    trx_opts.lock_timeout = 100;
    rocksdb::WriteOptions wo;
    data_transaction.reset(_db->BeginTransaction(wo, trx_opts, nullptr));
    data_transaction->SetSnapshot();
    ASSERT_NE(nullptr, data_transaction->GetSnapshot());
    index_transaction = _data_writer->GetBatch();
    ASSERT_NE(data_transaction, nullptr);
    std::vector<std::unique_ptr<SinkUpdateWriter>> index_writers;
    index_writers.emplace_back(
      std::make_unique<connector::search::SearchSinkUpdateWriter>(
        index_transaction));
    size_t rows_affected = 0;

    RocksDBUpdateDataSink sink("", *data_transaction, *_cf_handles.front(),
                               *pool_.get(), object_key, pk, data_column_oids,
                               all_column_oids, update_pk, table_row_type,
                               rows_affected, std::move(index_writers));
    sink.appendData(data);
    while (!sink.finish()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  void MakeRocksDBUpdate(std::vector<velox::VectorPtr> data,
                         std::vector<ColumnInfo> column_oids,
                         velox::RowTypePtr table_row_type,
                         std::vector<sdb::catalog::Column::Id> all_column_oids,
                         bool update_pk) {
    auto row_data = makeRowVector(data);
    std::unique_ptr<rocksdb::Transaction> transaction;
    irs::IndexWriter::Transaction index_transaction;
    std::vector<velox::column_index_t> pk = {0};
    PrepareRocksDBUpdate(row_data, column_oids, table_row_type, all_column_oids,
                         kObjectKey, pk, transaction, index_transaction,
                         update_pk);
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

TEST_F(DataSinkWithSearchTest, test_InsertDeleteFlatStrings) {
  std::vector<sdb::catalog::Column::Id> all_column_oids = {0, 1, 2};
  std::vector<ColumnInfo> all_columns = {
    {.id = 0, .name = ""}, {.id = 1, .name = ""}, {.id = 2, .name = ""}};
  std::vector<std::string> names = {"id", "value", "description"};
  std::vector<velox::TypePtr> types = {velox::INTEGER(), velox::VARCHAR(),
                                       velox::VARCHAR()};

  std::vector<velox::VectorPtr> data = {
    makeFlatVector<int32_t>({1, 42, 9001}),
    makeFlatVector<velox::StringView>({"1", "42", "9001"}),
    makeFlatVector<velox::StringView>({"value3", "value2", "value1"})};

  ObjectId object_key;
  primary_key::Keys written_row_keys{*pool_.get()};
  MakeRocksDBWrite(names, data, all_columns, object_key, written_row_keys);

  {
    auto reader = irs::DirectoryReader(_dir, _codec);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(3, reader.docs_count());
    ASSERT_EQ(3, reader.live_docs_count());
    VerifyRow(written_row_keys[0], std::string_view{"value3", 6},
              std::string_view{"1", 1}, reader);
    VerifyRow(written_row_keys[1], std::string_view{"value2", 6},
              std::string_view{"42", 2}, reader);
    VerifyRow(written_row_keys[2], std::string_view{"value1", 6},
              std::string_view{"9001", 4}, reader);
  }
  {
    RocksDBDataSource source(*pool_.get(), nullptr, *_db, *_cf_handles.front(),
                             velox::ROW(names, types), all_column_oids, 0,
                             kObjectKey);
    source.addSplit(std::make_shared<SereneDBConnectorSplit>("test_connector"));
    auto future = velox::ContinueFuture::makeEmpty();

    auto read = source.next(data[0]->size(), future);
    ASSERT_TRUE(read.has_value());
    ASSERT_TRUE(read.value() != nullptr);
    ASSERT_TRUE(future.isReady());
    ASSERT_EQ(read.value()->size(), 3);
    std::vector<std::pair<velox::vector_size_t, velox::vector_size_t>> idxs = {
      {0, 0}, {1, 1}, {2, 2}};
    VerifyRocksDB(read.value()->childAt(0).get(), data[0].get(), idxs);
    VerifyRocksDB(read.value()->childAt(1).get(), data[1].get(), idxs);
    VerifyRocksDB(read.value()->childAt(2).get(), data[2].get(), idxs);
    ASSERT_EQ(GetTotalRocksDBKeys(), 9);
  }
  {
    auto index_transaction = _data_writer->GetBatch();
    std::vector<std::unique_ptr<SinkDeleteWriter>> delete_writers;
    delete_writers.emplace_back(
      std::make_unique<connector::search::SearchSinkDeleteWriter>(
        index_transaction));

    rocksdb::TransactionOptions trx_opts;
    trx_opts.skip_concurrency_control = true;
    trx_opts.lock_timeout = 100;
    rocksdb::WriteOptions wo;
    std::unique_ptr<rocksdb::Transaction> transaction_delete{
      _db->BeginTransaction(wo, trx_opts, nullptr)};
    size_t rows_affected = 0;
    RocksDBDeleteDataSink delete_sink(
      *transaction_delete, *_cf_handles.front(), velox::ROW(names, types),
      kObjectKey, all_columns, rows_affected, std::move(delete_writers));
    auto delete_data = makeRowVector({makeFlatVector<int32_t>({9001, 1})});
    delete_sink.appendData(delete_data);
    ASSERT_TRUE(delete_sink.finish());
    ASSERT_TRUE(transaction_delete->Commit().ok());
    ASSERT_TRUE(index_transaction.Commit());
    ASSERT_TRUE(_data_writer->Commit());
  }
  {
    auto reader = irs::DirectoryReader(_dir, _codec);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(3, reader.docs_count());
    ASSERT_EQ(1, reader.live_docs_count());
    VerifyRow(written_row_keys[0], std::string_view{"value3", 6},
              std::string_view{"1", 1}, reader, false);
    VerifyRow(written_row_keys[1], std::string_view{"value2", 6},
              std::string_view{"42", 2}, reader, true);
    VerifyRow(written_row_keys[2], std::string_view{"value1", 6},
              std::string_view{"9001", 4}, reader, false);
  }

  {
    RocksDBDataSource source(*pool_.get(), nullptr, *_db, *_cf_handles.front(),
                             velox::ROW(names, types), all_column_oids, 0,
                             kObjectKey);
    source.addSplit(std::make_shared<SereneDBConnectorSplit>("test_connector"));
    auto future = velox::ContinueFuture::makeEmpty();

    auto read = source.next(data[0]->size(), future);
    ASSERT_TRUE(read.has_value());
    ASSERT_TRUE(read.value() != nullptr);
    ASSERT_TRUE(future.isReady());
    ASSERT_EQ(read.value()->size(), 1);
    std::vector<std::pair<velox::vector_size_t, velox::vector_size_t>> idxs = {
      {0, 1}};
    VerifyRocksDB(read.value()->childAt(0).get(), data[0].get(), idxs);
    VerifyRocksDB(read.value()->childAt(1).get(), data[1].get(), idxs);
    VerifyRocksDB(read.value()->childAt(2).get(), data[2].get(), idxs);
    ASSERT_EQ(GetTotalRocksDBKeys(), 3);
  }
}

TEST_F(DataSinkWithSearchTest, test_InsertOneUpdateFlatStrings) {
  std::vector<sdb::catalog::Column::Id> all_column_oids = {0, 1, 2};
  std::vector<ColumnInfo> all_columns = {
    {.id = 0, .name = ""}, {.id = 1, .name = ""}, {.id = 2, .name = ""}};
  std::vector<std::string> names = {"id", "value", "description"};
  std::vector<velox::TypePtr> types = {velox::INTEGER(), velox::VARCHAR(),
                                       velox::VARCHAR()};

  std::vector<velox::VectorPtr> data = {
    makeFlatVector<int32_t>({1, 42, 100, 9001}),
    makeFlatVector<velox::StringView>({"1", "42", "100", "9001"}),
    makeFlatVector<velox::StringView>(
      {"value1", "value2", "value3", "value4"})};
  std::vector<velox::VectorPtr> update_data = {
    makeFlatVector<int32_t>({1, 9001}),
    makeFlatVector<velox::StringView>({"1_updated", "9001_updated"})};
  ObjectId object_key;
  primary_key::Keys written_row_keys{*pool_.get()};
  MakeRocksDBWrite(names, data, all_columns, object_key, written_row_keys);
  {
    auto reader = irs::DirectoryReader(_dir, _codec);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(4, reader.docs_count());
    ASSERT_EQ(4, reader.live_docs_count());
    VerifyRow(written_row_keys[0], std::string_view{"value1", 6},
              std::string_view{"1", 1}, reader);
    VerifyRow(written_row_keys[1], std::string_view{"value2", 6},
              std::string_view{"42", 2}, reader);
    VerifyRow(written_row_keys[2], std::string_view{"value3", 6},
              std::string_view{"100", 3}, reader);
    VerifyRow(written_row_keys[3], std::string_view{"value4", 6},
              std::string_view{"9001", 4}, reader);
  }
  {
    RocksDBDataSource source(*pool_.get(), nullptr, *_db, *_cf_handles.front(),
                             velox::ROW(names, types), all_column_oids, 0,
                             kObjectKey);
    source.addSplit(std::make_shared<SereneDBConnectorSplit>("test_connector"));
    auto future = velox::ContinueFuture::makeEmpty();

    auto read = source.next(data[0]->size(), future);
    ASSERT_TRUE(read.has_value());
    ASSERT_TRUE(read.value() != nullptr);
    ASSERT_TRUE(future.isReady());
    ASSERT_EQ(read.value()->size(), 4);
    std::vector<std::pair<velox::vector_size_t, velox::vector_size_t>> idxs = {
      {0, 0}, {1, 1}, {2, 2}, {3, 3}};
    VerifyRocksDB(read.value()->childAt(0).get(), data[0].get(), idxs);
    VerifyRocksDB(read.value()->childAt(1).get(), data[1].get(), idxs);
    VerifyRocksDB(read.value()->childAt(2).get(), data[2].get(), idxs);
    ASSERT_EQ(GetTotalRocksDBKeys(), 12);
  }
  {
    MakeRocksDBUpdate(update_data,
                      {{.id = 0, .name = ""}, {.id = 1, .name = ""}},
                      velox::ROW(names, types), all_column_oids, false);
  }
  {
    auto reader = irs::DirectoryReader(_dir, _codec);
    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(6, reader.docs_count());
    ASSERT_EQ(4, reader.live_docs_count());
    VerifyRow(written_row_keys[0], std::string_view{"value1", 6},
              std::string_view{"1", 1}, reader, false);
    VerifyRow(written_row_keys[0], std::string_view{"value1", 6},
              std::string_view{"1_updated", 9}, reader, true);
    VerifyRow(written_row_keys[1], std::string_view{"value2", 6},
              std::string_view{"42", 2}, reader, true);
    VerifyRow(written_row_keys[2], std::string_view{"value3", 6},
              std::string_view{"100", 3}, reader, true);
    VerifyRow(written_row_keys[3], std::string_view{"value4", 6},
              std::string_view{"9001", 4}, reader, false);
    VerifyRow(written_row_keys[3], std::string_view{"value4", 6},
              std::string_view{"9001_updated", 12}, reader, true);
  }
  {
    RocksDBDataSource source(*pool_.get(), nullptr, *_db, *_cf_handles.front(),
                             velox::ROW(names, types), all_column_oids, 0,
                             kObjectKey);
    source.addSplit(std::make_shared<SereneDBConnectorSplit>("test_connector"));
    auto future = velox::ContinueFuture::makeEmpty();

    auto read = source.next(data[0]->size(), future);
    ASSERT_TRUE(read.has_value());
    ASSERT_TRUE(read.value() != nullptr);
    ASSERT_TRUE(future.isReady());
    ASSERT_EQ(read.value()->size(), 4);
    std::vector<std::pair<velox::vector_size_t, velox::vector_size_t>> idxs = {
      {1, 1}, {2, 2}};
    VerifyRocksDB(read.value()->childAt(0).get(), data[0].get(), idxs);
    VerifyRocksDB(read.value()->childAt(1).get(), data[1].get(), idxs);
    VerifyRocksDB(read.value()->childAt(2).get(), data[2].get(), idxs);
    std::vector<std::pair<velox::vector_size_t, velox::vector_size_t>> idxs2 = {
      {0, 0}, {3, 1}};
    VerifyRocksDB(read.value()->childAt(0).get(), update_data[0].get(), idxs2);
    VerifyRocksDB(read.value()->childAt(1).get(), update_data[1].get(), idxs2);
    std::vector<std::pair<velox::vector_size_t, velox::vector_size_t>> idxs3 = {
      {0, 0}, {3, 3}};
    VerifyRocksDB(read.value()->childAt(2).get(), data[2].get(), idxs3);
    ASSERT_EQ(GetTotalRocksDBKeys(), 12);
  }
}

TEST_F(DataSinkWithSearchTest, test_InsertAllExceptPKUpdateFlatStrings) {
  std::vector<sdb::catalog::Column::Id> all_column_oids = {0, 1, 2};
  std::vector<ColumnInfo> all_columns = {
    {.id = 0, .name = ""}, {.id = 1, .name = ""}, {.id = 2, .name = ""}};
  std::vector<std::string> names = {"id", "value", "description"};
  std::vector<velox::TypePtr> types = {velox::INTEGER(), velox::VARCHAR(),
                                       velox::VARCHAR()};

  std::vector<velox::VectorPtr> data = {
    makeFlatVector<int32_t>({9001, 42, 1, 100}),
    makeFlatVector<velox::StringView>({"9001", "42", "1", "3"}),
    makeFlatVector<velox::StringView>({"value1", "value2", "value3", "33"})};
  std::vector<velox::VectorPtr> update_data = {
    makeFlatVector<int32_t>({1, 100, 9001}),
    makeFlatVector<velox::StringView>({"1_updated", "4", "9001_updated"}),
    makeFlatVector<velox::StringView>({"value8", "32", "value9"})};

  ObjectId object_key;
  primary_key::Keys written_row_keys{*pool_.get()};
  MakeRocksDBWrite(names, data, all_columns, object_key, written_row_keys);
  {
    auto reader = irs::DirectoryReader(_dir, _codec);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(4, reader.docs_count());
    ASSERT_EQ(4, reader.live_docs_count());
    VerifyRow(written_row_keys[0], std::string_view{"value1", 6},
              std::string_view{"9001", 4}, reader);
    VerifyRow(written_row_keys[1], std::string_view{"value2", 6},
              std::string_view{"42", 2}, reader);
    VerifyRow(written_row_keys[2], std::string_view{"value3", 6},
              std::string_view{"1", 1}, reader);
    VerifyRow(written_row_keys[3], std::string_view{"33", 2},
              std::string_view{"3", 1}, reader);
  }
  {
    RocksDBDataSource source(*pool_.get(), nullptr, *_db, *_cf_handles.front(),
                             velox::ROW(names, types), all_column_oids, 0,
                             kObjectKey);
    source.addSplit(std::make_shared<SereneDBConnectorSplit>("test_connector"));
    auto future = velox::ContinueFuture::makeEmpty();

    auto read = source.next(data[0]->size(), future);
    ASSERT_TRUE(read.has_value());
    ASSERT_TRUE(read.value() != nullptr);
    ASSERT_TRUE(future.isReady());
    ASSERT_EQ(read.value()->size(), 4);
    std::vector<std::pair<velox::vector_size_t, velox::vector_size_t>> idxs = {
      {0, 2}, {1, 1}, {2, 3}, {3, 0}};
    VerifyRocksDB(read.value()->childAt(0).get(), data[0].get(), idxs);
    VerifyRocksDB(read.value()->childAt(1).get(), data[1].get(), idxs);
    VerifyRocksDB(read.value()->childAt(2).get(), data[2].get(), idxs);
  }
  MakeRocksDBUpdate(
    update_data,
    {{.id = 0, .name = ""}, {.id = 1, .name = ""}, {.id = 2, .name = ""}},
    velox::ROW(names, types), all_column_oids, false);
  ASSERT_EQ(GetTotalRocksDBKeys(), 12) << "Should have 12 keys after update";

  {
    auto reader = irs::DirectoryReader(_dir, _codec);
    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(7, reader.docs_count());
    ASSERT_EQ(4, reader.live_docs_count());
    VerifyRow(written_row_keys[0], std::string_view{"value1", 6},
              std::string_view{"9001", 4}, reader, false);
    VerifyRow(written_row_keys[0], std::string_view{"value9", 6},
              std::string_view{"9001_updated", 12}, reader, true);
    VerifyRow(written_row_keys[1], std::string_view{"value2", 6},
              std::string_view{"42", 2}, reader, true);
    VerifyRow(written_row_keys[2], std::string_view{"value3", 6},
              std::string_view{"1", 1}, reader, false);
    VerifyRow(written_row_keys[2], std::string_view{"value8", 6},
              std::string_view{"1_updated", 9}, reader, true);
    VerifyRow(written_row_keys[3], std::string_view{"33", 2},
              std::string_view{"3", 1}, reader, false);
    VerifyRow(written_row_keys[3], std::string_view{"32", 2},
              std::string_view{"4", 1}, reader, true);
  }
  {
    RocksDBDataSource source(*pool_.get(), nullptr, *_db, *_cf_handles.front(),
                             velox::ROW(names, types), all_column_oids, 0,
                             kObjectKey);
    source.addSplit(std::make_shared<SereneDBConnectorSplit>("test_connector"));
    auto future = velox::ContinueFuture::makeEmpty();

    auto read = source.next(data[0]->size(), future);
    ASSERT_TRUE(read.has_value());
    ASSERT_TRUE(read.value() != nullptr);
    ASSERT_TRUE(future.isReady());
    ASSERT_EQ(read.value()->size(), 4);
    // Not updated row
    std::vector<std::pair<velox::vector_size_t, velox::vector_size_t>> idxs = {
      {1, 1}};
    VerifyRocksDB(read.value()->childAt(0).get(), data[0].get(), idxs);
    VerifyRocksDB(read.value()->childAt(1).get(), data[1].get(), idxs);
    VerifyRocksDB(read.value()->childAt(2).get(), data[2].get(), idxs);
    // Updated rows
    std::vector<std::pair<velox::vector_size_t, velox::vector_size_t>> idxs2 = {
      {0, 0}, {2, 1}, {3, 2}};
    VerifyRocksDB(read.value()->childAt(0).get(), update_data[0].get(), idxs2);
    VerifyRocksDB(read.value()->childAt(1).get(), update_data[1].get(), idxs2);
    VerifyRocksDB(read.value()->childAt(2).get(), update_data[2].get(), idxs2);
  }
}

TEST_F(DataSinkWithSearchTest, test_InsertAllUpdateFlatStrings) {
  std::vector<sdb::catalog::Column::Id> all_column_oids = {0, 1, 2};
  std::vector<ColumnInfo> all_columns = {
    {.id = 0, .name = ""}, {.id = 1, .name = ""}, {.id = 2, .name = ""}};
  std::vector<std::string> names = {"id", "value", "description"};
  std::vector<velox::TypePtr> types = {velox::INTEGER(), velox::VARCHAR(),
                                       velox::VARCHAR()};

  std::vector<velox::VectorPtr> data = {
    makeFlatVector<int32_t>({9001, 42, 1, 100}),
    makeFlatVector<velox::StringView>({"9001", "42", "1", "3"}),
    makeFlatVector<velox::StringView>({"value1", "value2", "value3", "33"})};
  std::vector<velox::VectorPtr> update_data = {
    makeFlatVector<int32_t>({1, 100, 9001}),
    makeFlatVector<int32_t>({2, 101, 9002}),
    makeFlatVector<velox::StringView>({"1_updated", "4", "9001_updated"}),
    makeFlatVector<velox::StringView>({"value8", "32", "value9"})};

  ObjectId object_key;
  primary_key::Keys written_row_keys{*pool_.get()};
  MakeRocksDBWrite(names, data, all_columns, object_key, written_row_keys);
  ASSERT_EQ(GetTotalRocksDBKeys(), 12) << "Should have 12 keys after insert";
  {
    auto reader = irs::DirectoryReader(_dir, _codec);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(4, reader.docs_count());
    ASSERT_EQ(4, reader.live_docs_count());
    VerifyRow(written_row_keys[0], std::string_view{"value1", 6},
              std::string_view{"9001", 4}, reader);
    VerifyRow(written_row_keys[1], std::string_view{"value2", 6},
              std::string_view{"42", 2}, reader);
    VerifyRow(written_row_keys[2], std::string_view{"value3", 6},
              std::string_view{"1", 1}, reader);
    VerifyRow(written_row_keys[3], std::string_view{"33", 2},
              std::string_view{"3", 1}, reader);
  }
  {
    RocksDBDataSource source(*pool_.get(), nullptr, *_db, *_cf_handles.front(),
                             velox::ROW(names, types), all_column_oids, 0,
                             kObjectKey);
    source.addSplit(std::make_shared<SereneDBConnectorSplit>("test_connector"));
    auto future = velox::ContinueFuture::makeEmpty();

    auto read = source.next(data[0]->size(), future);
    ASSERT_TRUE(read.has_value());
    ASSERT_TRUE(read.value() != nullptr);
    ASSERT_TRUE(future.isReady());
    ASSERT_EQ(read.value()->size(), 4);
    std::vector<std::pair<velox::vector_size_t, velox::vector_size_t>> idxs = {
      {0, 2}, {1, 1}, {2, 3}, {3, 0}};
    VerifyRocksDB(read.value()->childAt(0).get(), data[0].get(), idxs);
    VerifyRocksDB(read.value()->childAt(1).get(), data[1].get(), idxs);
    VerifyRocksDB(read.value()->childAt(2).get(), data[2].get(), idxs);
  }
  MakeRocksDBUpdate(update_data,
                    {{.id = 0, .name = ""},
                     {.id = 0, .name = ""},
                     {.id = 1, .name = ""},
                     {.id = 2, .name = ""}},
                    velox::ROW(names, types), all_column_oids, true);
  primary_key::Keys updated_row_keys{*pool_.get()};
  primary_key::Create(*makeRowVector(update_data), {1}, updated_row_keys);
  ASSERT_EQ(GetTotalRocksDBKeys(), 12) << "Should have 12 keys after update";
  {
    auto reader = irs::DirectoryReader(_dir, _codec);
    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(7, reader.docs_count());
    ASSERT_EQ(4, reader.live_docs_count());
    VerifyRow(written_row_keys[0], std::string_view{"value1", 6},
              std::string_view{"9001", 4}, reader, false);
    VerifyRow(updated_row_keys[2], std::string_view{"value9", 6},
              std::string_view{"9001_updated", 12}, reader, true);
    VerifyRow(written_row_keys[1], std::string_view{"value2", 6},
              std::string_view{"42", 2}, reader, true);
    VerifyRow(written_row_keys[2], std::string_view{"value3", 6},
              std::string_view{"1", 1}, reader, false);
    VerifyRow(updated_row_keys[0], std::string_view{"value8", 6},
              std::string_view{"1_updated", 9}, reader, true);
    VerifyRow(written_row_keys[3], std::string_view{"33", 2},
              std::string_view{"3", 1}, reader, false);
    VerifyRow(updated_row_keys[1], std::string_view{"32", 2},
              std::string_view{"4", 1}, reader, true);
  }
  {
    RocksDBDataSource source(*pool_.get(), nullptr, *_db, *_cf_handles.front(),
                             velox::ROW(names, types), all_column_oids, 0,
                             kObjectKey);
    source.addSplit(std::make_shared<SereneDBConnectorSplit>("test_connector"));
    auto future = velox::ContinueFuture::makeEmpty();

    auto read = source.next(data[0]->size(), future);
    ASSERT_TRUE(read.has_value());
    ASSERT_TRUE(read.value() != nullptr);
    ASSERT_TRUE(future.isReady());
    ASSERT_EQ(read.value()->size(), 4);
    // Not updated row
    std::vector<std::pair<velox::vector_size_t, velox::vector_size_t>> idxs = {
      {1, 1}};
    VerifyRocksDB(read.value()->childAt(0).get(), data[0].get(), idxs);
    VerifyRocksDB(read.value()->childAt(1).get(), data[1].get(), idxs);
    VerifyRocksDB(read.value()->childAt(2).get(), data[2].get(), idxs);
    // Updated rows
    std::vector<std::pair<velox::vector_size_t, velox::vector_size_t>> idxs2 = {
      {0, 0}, {2, 1}, {3, 2}};
    VerifyRocksDB(read.value()->childAt(0).get(), update_data[1].get(), idxs2);
    VerifyRocksDB(read.value()->childAt(1).get(), update_data[2].get(), idxs2);
    VerifyRocksDB(read.value()->childAt(2).get(), update_data[3].get(), idxs2);
  }
}

TEST_F(DataSinkWithSearchTest,
       test_InsertAllUpdateFlatStringsUnsortedNewPKNotAll) {
  std::vector<sdb::catalog::Column::Id> all_column_oids = {0, 1, 2};
  std::vector<ColumnInfo> all_columns = {
    {.id = 0, .name = ""}, {.id = 1, .name = ""}, {.id = 2, .name = ""}};
  std::vector<std::string> names = {"id", "value", "description"};
  std::vector<velox::TypePtr> types = {velox::INTEGER(), velox::VARCHAR(),
                                       velox::VARCHAR()};

  std::vector<velox::VectorPtr> data = {
    makeFlatVector<int32_t>({9001, 42, 1, 100}),
    makeFlatVector<velox::StringView>({"9001", "42", "1", "3"}),
    makeFlatVector<velox::StringView>({"value1", "value2", "value3", "33"})};
  std::vector<velox::VectorPtr> update_data = {
    makeFlatVector<int32_t>({42}), makeFlatVector<int32_t>({101}),
    makeFlatVector<velox::StringView>({"32"})};

  ObjectId object_key;
  primary_key::Keys written_row_keys{*pool_.get()};
  MakeRocksDBWrite(names, data, all_columns, object_key, written_row_keys);
  ASSERT_EQ(GetTotalRocksDBKeys(), 12) << "Should have 12 keys after insert";
  {
    auto reader = irs::DirectoryReader(_dir, _codec);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(4, reader.docs_count());
    ASSERT_EQ(4, reader.live_docs_count());
    VerifyRow(written_row_keys[0], std::string_view{"value1", 6},
              std::string_view{"9001", 4}, reader);
    VerifyRow(written_row_keys[1], std::string_view{"value2", 6},
              std::string_view{"42", 2}, reader);
    VerifyRow(written_row_keys[2], std::string_view{"value3", 6},
              std::string_view{"1", 1}, reader);
    VerifyRow(written_row_keys[3], std::string_view{"33", 2},
              std::string_view{"3", 1}, reader);
  }
  {
    RocksDBDataSource source(*pool_.get(), nullptr, *_db, *_cf_handles.front(),
                             velox::ROW(names, types), all_column_oids, 0,
                             kObjectKey);
    source.addSplit(std::make_shared<SereneDBConnectorSplit>("test_connector"));
    auto future = velox::ContinueFuture::makeEmpty();

    auto read = source.next(data[0]->size(), future);
    ASSERT_TRUE(read.has_value());
    ASSERT_TRUE(read.value() != nullptr);
    ASSERT_TRUE(future.isReady());
    ASSERT_EQ(read.value()->size(), 4);
    std::vector<std::pair<velox::vector_size_t, velox::vector_size_t>> idxs = {
      {0, 2}, {1, 1}, {2, 3}, {3, 0}};
    VerifyRocksDB(read.value()->childAt(0).get(), data[0].get(), idxs);
    VerifyRocksDB(read.value()->childAt(1).get(), data[1].get(), idxs);
    VerifyRocksDB(read.value()->childAt(2).get(), data[2].get(), idxs);
  }
  MakeRocksDBUpdate(
    update_data,
    {{.id = 0, .name = ""}, {.id = 0, .name = ""}, {.id = 2, .name = ""}},
    velox::ROW(names, types), all_column_oids, true);
  primary_key::Keys updated_row_keys{*pool_.get()};
  primary_key::Create(*makeRowVector(update_data), {1}, updated_row_keys);
  ASSERT_EQ(GetTotalRocksDBKeys(), 12) << "Should have 12 keys after update";
  {
    auto reader = irs::DirectoryReader(_dir, _codec);
    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(5, reader.docs_count());
    ASSERT_EQ(4, reader.live_docs_count());
    VerifyRow(written_row_keys[0], std::string_view{"value1", 6},
              std::string_view{"9001", 4}, reader, true);
    VerifyRow(written_row_keys[1], std::string_view{"value2", 6},
              std::string_view{"42", 2}, reader, false);
    VerifyRow(written_row_keys[2], std::string_view{"value3", 6},
              std::string_view{"1", 1}, reader, true);
    VerifyRow(written_row_keys[3], std::string_view{"33", 2},
              std::string_view{"3", 1}, reader, true);
    VerifyRow(updated_row_keys[0], std::string_view{"32", 2},
              std::string_view{"42", 2}, reader, true);
  }
  {
    RocksDBDataSource source(*pool_.get(), nullptr, *_db, *_cf_handles.front(),
                             velox::ROW(names, types), all_column_oids, 0,
                             kObjectKey);
    source.addSplit(std::make_shared<SereneDBConnectorSplit>("test_connector"));
    auto future = velox::ContinueFuture::makeEmpty();

    auto read = source.next(data[0]->size(), future);
    ASSERT_TRUE(read.has_value());
    ASSERT_TRUE(read.value() != nullptr);
    ASSERT_TRUE(future.isReady());
    ASSERT_EQ(read.value()->size(), 4);
    // Not updated row
    std::vector<std::pair<velox::vector_size_t, velox::vector_size_t>> idxs = {
      {0, 2}, {1, 3}, {3, 0}};
    VerifyRocksDB(read.value()->childAt(0).get(), data[0].get(), idxs);
    VerifyRocksDB(read.value()->childAt(1).get(), data[1].get(), idxs);
    VerifyRocksDB(read.value()->childAt(2).get(), data[2].get(), idxs);
    std::vector<std::pair<velox::vector_size_t, velox::vector_size_t>> idxs1 = {
      {2, 1}};
    VerifyRocksDB(read.value()->childAt(1).get(), data[1].get(), idxs1);
    // Updated rows
    std::vector<std::pair<velox::vector_size_t, velox::vector_size_t>> idxs2 = {
      {2, 0}};
    VerifyRocksDB(read.value()->childAt(0).get(), update_data[1].get(), idxs2);
    VerifyRocksDB(read.value()->childAt(2).get(), update_data[2].get(), idxs2);
  }
}

}  // namespace
