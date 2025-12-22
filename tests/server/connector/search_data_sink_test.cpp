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

#include <iresearch/store/memory_directory.hpp>

#include "connector/common.h"
#include "connector/primary_key.hpp"
#include "connector/search_data_sink.hpp"
#include "catalog/table_options.h"
#include "gtest/gtest.h"
#include "iresearch/utils/bytes_utils.hpp"

using namespace sdb::connector;

namespace {

constexpr sdb::ObjectId kObjectKey{123456};

class SearchDataSinkTest : public ::testing::Test,
                           public velox::test::VectorTestBase {
 public:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
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

    _data_writer = irs::IndexWriter::Make(_dir, irs::formats::Get("1_5"),
                                          irs::kOmCreate, options);
  }

  void TearDown() final { _data_writer.reset(); }

 protected:
  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _data_writer;
};

TEST_F(SearchDataSinkTest, InsertIntegerColumn) {
  auto trx = _data_writer->GetBatch();

  SearchDataSink sink{trx};
  const std::vector<sdb::catalog::Column::Id> col_id1{1};
  primary_key::Keys keys{*pool()};
  keys.push_back("pk1");

  auto data = makeRowVector({"c1"},
                            {makeFlatVector<int32_t>({10, 20, 30, 40, 50})});


  auto vector1 = makeFlatVector<int32_t>(
    num_rows, [](auto row) { return static_cast<int32_t>(row * 10); });
  auto vector2 = makeFlatVector<std::string>(
    num_rows, [](auto row) { return "str_" + std::to_string(row); });

  velox::RowVector input{
    velox::ROW(
      {velox::TypeKind::INTEGER, velox::TypeKind::VARCHAR}),
    velox::memory::getDefaultMemoryPool(),
    {vector1, vector2},
    num_rows};

  primary_key::Keys keys;
  for (size_t i = 0; i < num_rows; ++i) {
    keys.push_back("pk_" + std::to_string(i));
  }

  sink.AppendData(
    input, std::span<const key_utils::ColumnId>{col_id1, col_id2}, keys);
  EXPECT_TRUE(sink.Finish());

}  // namespace
