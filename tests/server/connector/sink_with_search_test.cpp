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

#include "gtest/gtest.h"
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
#include "iresearch/utils/bytes_utils.hpp"
#include "rocksdb/utilities/transaction_db.h"
#include "connector/search_sink_writer.hpp"

using namespace sdb::connector;

namespace {

constexpr sdb::ObjectId kObjectKey{123456};
static constexpr std::string_view kPkColumn =
  std::string_view{"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 8};

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

    void PrepareRocksDBWrite(
    const velox::RowVectorPtr& data, sdb::ObjectId object_key,
    const std::vector<velox::column_index_t>& pk,
    std::unique_ptr<rocksdb::Transaction>& data_transaction,
    irs::IndexWriter::Transaction& index_transaction,
    sdb::connector::primary_key::Keys& written_row_keys) {
    rocksdb::TransactionOptions trx_opts;
    trx_opts.skip_concurrency_control = true;
    trx_opts.lock_timeout = 100;
    rocksdb::WriteOptions wo;
    data_transaction.reset(_db->BeginTransaction(wo, trx_opts, nullptr));
    index_transaction = _data_writer->GetBatch();
    ASSERT_NE(data_transaction, nullptr);
    std::vector<sdb::catalog::Column::Id> column_oids;
    column_oids.reserve(data->childrenSize());
    for (velox::column_index_t i = 0; i < data->childrenSize(); ++i) {
      column_oids.push_back(static_cast<sdb::catalog::Column::Id>(i));
    }
    std::vector<std::unique_ptr<sdb::connector::SinkInsertWriter>> index_writers;
    index_writers.emplace_back(std::make_unique<
                               sdb::connector::search::SearchSinkInsertWriter>(
      index_transaction));
    sdb::connector::primary_key::Create(*data, pk, written_row_keys);
    sdb::connector::RocksDBDataSink sink(*data_transaction, *_cf_handles.front(),
                                         *pool_.get(), object_key, pk,
                                         column_oids, std::move(index_writers), false);
    sink.appendData(data);
    while (!sink.finish()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  void MakeRocksDBWrite(std::vector<std::string> names,
                        std::vector<velox::VectorPtr> data,
                        sdb::ObjectId& object_key,
                        sdb::connector::primary_key::Keys& written_row_keys) {
    object_key = kObjectKey;
    // creating  PK column
    names.push_back("id");
    std::vector<int32_t> idx;
    idx.resize(data.front()->size());
    std::iota(idx.begin(), idx.end(), 0);
    data.push_back(makeFlatVector<int32_t>(idx));
    auto row_data = makeRowVector(names, {data});
    std::unique_ptr<rocksdb::Transaction> transaction;
    irs::IndexWriter::Transaction index_transaction;
    std::vector<velox::column_index_t> pk = {
      static_cast<velox::column_index_t>(names.size() - 1)};
    PrepareRocksDBWrite(row_data, object_key, pk, transaction,
      index_transaction,
                        written_row_keys);
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

TEST_F(DataSinkWithSearchTest, test_InsertFlatStrings) {
  std::vector<std::string> names = {"value", "description"};
  std::vector<velox::TypePtr> types = {velox::VARCHAR(), velox::VARCHAR()};

  std::vector<velox::VectorPtr> data = {
    makeFlatVector<velox::StringView>({"9001", "42", "1"}),
    makeFlatVector<velox::StringView>({"value1", "value2", "value3"})};

  sdb::ObjectId object_key;
  sdb::connector::primary_key::Keys written_row_keys{*pool_.get()};
  MakeRocksDBWrite(names, data, object_key, written_row_keys);
  auto reader = irs::DirectoryReader(_dir, _codec);
  ASSERT_EQ(1, reader.size());
  ASSERT_EQ(3, reader.docs_count());
  ASSERT_EQ(3, reader.live_docs_count());
  irs::And and_filter;
  {
    auto& term_filter = and_filter.add<irs::ByTerm>();
    *term_filter.mutable_field() =
      std::string{"\x00\x00\x00\x00\x00\x00\x00\x01\x03", 9};
    term_filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view{"value2", 6});
  }
  {
    auto& term_filter = and_filter.add<irs::ByTerm>();
    *term_filter.mutable_field() =
      std::string{"\x00\x00\x00\x00\x00\x00\x00\x00\x03", 9};
    term_filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view{"42", 2});
  }

  auto prepared = and_filter.prepare({.index = reader});
  auto docs = prepared->execute({.segment = reader[0], .scorers = {}});
  ASSERT_TRUE(docs->next());
  const auto* pk_column = reader[0].column(kPkColumn);
  ASSERT_NE(nullptr, pk_column);
  auto pk_values_itr = pk_column->iterator(irs::ColumnHint::Normal);
  ASSERT_NE(nullptr, pk_values_itr);
  auto* actual_pk_value = irs::get<irs::PayAttr>(*pk_values_itr);
  ASSERT_NE(nullptr, actual_pk_value);
  auto pk_seeked = pk_values_itr->seek(docs->value());
  ASSERT_EQ(docs->value(), pk_seeked);
  ASSERT_EQ(written_row_keys[1], irs::ViewCast<char>(actual_pk_value->value));
  ASSERT_FALSE(docs->next());
}


}  // namespace
