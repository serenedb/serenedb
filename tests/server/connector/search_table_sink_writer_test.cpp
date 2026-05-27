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

#include <gtest/gtest.h>

#include <duckdb.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/columnstore/format.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/search/scorers.hpp>
#include <iresearch/store/memory_directory.hpp>

#include "catalog/identifiers/object_id.h"
#include "connector/columnstore_materializer.h"
#include "connector/search_table_sink_writer.h"

namespace sdb::connector {
namespace {

duckdb::DatabaseInstance& TestDb() {
  static std::unique_ptr<duckdb::DuckDB> kDb = []() {
    duckdb::DBConfig cfg;
    cfg.options.access_mode = duckdb::AccessMode::AUTOMATIC;
    return std::make_unique<duckdb::DuckDB>(":memory:", &cfg);
  }();
  return *kDb->instance;
}

class SearchTableSinkWriterTest : public ::testing::Test {
 public:
  static void SetUpTestCase() {
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
    _writer = irs::IndexWriter::Make(_dir, _codec, irs::kOmCreate, options);
  }

  void TearDown() final { _writer.reset(); }

 protected:
  // Build a tiny INTEGER Vector of `count` rows with values 100, 101, ...
  static duckdb::Vector MakeIntVector(duckdb::idx_t count) {
    duckdb::Vector vec{duckdb::LogicalType::INTEGER, count};
    for (duckdb::idx_t i = 0; i < count; ++i) {
      vec.SetValue(i, duckdb::Value::INTEGER(static_cast<int32_t>(100 + i)));
    }
    return vec;
  }

  irs::Format::ptr _codec;
  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _writer;
};

TEST_F(SearchTableSinkWriterTest, InsertThreeRowsOneColumn) {
  auto trx = _writer->GetBatch();
  SearchTableSinkWriter sink{trx};

  sink.Init(/*batch_size=*/3);
  auto vec = MakeIntVector(3);
  sink.SwitchColumn(catalog::Column::Id{42}, duckdb::LogicalType::INTEGER, vec,
                    3);
  sink.Write("pk_a");
  sink.Write("pk_b");
  sink.Write("pk_c");
  sink.Finish();
  ASSERT_TRUE(trx.Commit());
  _writer->Commit();

  auto reader = irs::DirectoryReader(_dir, _codec, {.db = &TestDb()});
  ASSERT_EQ(reader.size(), 1u);
  EXPECT_EQ(reader.docs_count(), 3u);
  EXPECT_EQ(reader.live_docs_count(), 3u);

  // Materialize via ColumnstoreMaterializer -- exercises the same scan
  // path SereneDBSearchScan uses. If this works but the operator's
  // SELECT crashes on the same shape of data, the bug is operator-side
  // (e.g. wrong shard/reader/snapshot wiring). If this crashes here,
  // the bug is in the sink (columnstore writes never reached the
  // segment).
  auto& segment = reader[0];
  const auto* cs_reader = segment.CsReader();
  ASSERT_NE(cs_reader, nullptr);
  std::vector<irs::field_id> field_ids{
    static_cast<irs::field_id>(catalog::Column::Id{42})};
  std::vector<duckdb::idx_t> output_slots{0};
  ColumnstoreMaterializer materializer{*cs_reader, field_ids, output_slots};
  duckdb::DataChunk output;
  output.Initialize(duckdb::Allocator::DefaultAllocator(),
                    {duckdb::LogicalType::INTEGER});
  materializer.Scan(/*start_doc=*/0, /*count=*/3, output);
  output.SetCardinality(3);
  ASSERT_EQ(output.size(), 3u);
  EXPECT_EQ(output.data[0].GetValue(0).GetValue<int32_t>(), 100);
  EXPECT_EQ(output.data[0].GetValue(1).GetValue<int32_t>(), 101);
  EXPECT_EQ(output.data[0].GetValue(2).GetValue<int32_t>(), 102);
}

TEST_F(SearchTableSinkWriterTest, InsertThenDeleteOneRow) {
  // Insert 3 rows.
  {
    auto trx = _writer->GetBatch();
    SearchTableSinkWriter sink{trx};
    sink.Init(3);
    auto vec = MakeIntVector(3);
    sink.SwitchColumn(catalog::Column::Id{1}, duckdb::LogicalType::INTEGER, vec,
                      3);
    sink.Write("pk_x");
    sink.Write("pk_y");
    sink.Write("pk_z");
    sink.Finish();
    ASSERT_TRUE(trx.Commit());
  }
  _writer->Commit();

  // Delete pk_y via a second sink/batch (DeleteRow drains at Finish via
  // SearchRemoveFilter).
  {
    auto trx = _writer->GetBatch();
    SearchTableSinkWriter sink{trx};
    sink.Init(1);
    sink.DeleteRow("pk_y");
    sink.Finish();
    ASSERT_TRUE(trx.Commit());
  }
  _writer->Commit();

  auto reader = irs::DirectoryReader(_dir, _codec, {.db = &TestDb()});
  EXPECT_EQ(reader.docs_count(), 3u);
  EXPECT_EQ(reader.live_docs_count(), 2u);
}

TEST_F(SearchTableSinkWriterTest, TwoBatchesShareSink) {
  auto trx = _writer->GetBatch();
  SearchTableSinkWriter sink{trx};

  sink.Init(2);
  auto vec1 = MakeIntVector(2);
  sink.SwitchColumn(catalog::Column::Id{7}, duckdb::LogicalType::INTEGER, vec1,
                    2);
  sink.Write("pk_1");
  sink.Write("pk_2");
  sink.Finish();

  sink.Init(2);
  auto vec2 = MakeIntVector(2);
  sink.SwitchColumn(catalog::Column::Id{7}, duckdb::LogicalType::INTEGER, vec2,
                    2);
  sink.Write("pk_3");
  sink.Write("pk_4");
  sink.Finish();

  ASSERT_TRUE(trx.Commit());
  _writer->Commit();

  auto reader = irs::DirectoryReader(_dir, _codec, {.db = &TestDb()});
  EXPECT_EQ(reader.docs_count(), 4u);
  EXPECT_EQ(reader.live_docs_count(), 4u);
}

TEST_F(SearchTableSinkWriterTest, AbortReleasesDocumentCleanly) {
  auto trx = _writer->GetBatch();
  SearchTableSinkWriter sink{trx};

  sink.Init(2);
  auto vec = MakeIntVector(2);
  sink.SwitchColumn(catalog::Column::Id{1}, duckdb::LogicalType::INTEGER, vec,
                    2);
  sink.Write("pk_a");
  sink.Write("pk_b");
  sink.Abort();
  trx.Abort();
  _writer->Commit();

  // Aborted rows don't reach the reader.
  auto reader = irs::DirectoryReader(_dir, _codec, {.db = &TestDb()});
  EXPECT_EQ(reader.docs_count(), 0u);
}

}  // namespace
}  // namespace sdb::connector
