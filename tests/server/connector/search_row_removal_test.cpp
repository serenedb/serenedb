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

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <duckdb.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <iresearch/analysis/keyword_tokenizer.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/file_names.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <iterator>
#include <limits>
#include <map>
#include <mutex>
#include <numeric>
#include <random>
#include <set>
#include <thread>
#include <vector>

#include "connector/column_id.h"
#include "connector/row_position.h"
#include "connector/search_sink_writer.hpp"
#include "gtest/gtest.h"
#include "search/search_table_changes.h"

namespace {

using namespace sdb;
using namespace sdb::connector;

duckdb::DatabaseInstance& TestDb() {
  return ::irs::DuckDBEngine::Instance().instance();
}

duckdb::ClientContext& TestContext() {
  static auto* conn = new duckdb::Connection{TestDb()};
  return *conn->context;
}

catalog::ColumnTokenizer KeywordTokenizer(irs::field_id) {
  static auto gTokenizer = std::make_shared<catalog::Tokenizer>(
    search::Features{},
    irs::analysis::TokenizerConfig{.config = irs::KeywordTokenizer::Options{}});
  return {.analyzer = gTokenizer->Acquire(TestContext()),
          .features = irs::IndexFeatures::None};
}

constexpr ColumnId kValueColumn{1};

std::vector<irs::doc_id_t> ResolveDocs(
  const irs::DocRemoval& removal, std::span<const irs::SubReader* const> live,
  const irs::SubReader& segment) {
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  resolver.Prepare(removals, live);
  const auto docs = resolver.Docs(removal, segment);
  return {docs.begin(), docs.end()};
}

TEST(RowPositionTest, RemovedRowSplitsThePosition) {
  const auto placed = RemovedRow(42, MakeRowPosition(7, 9));
  EXPECT_EQ(7, placed.segment);
  EXPECT_EQ(9, placed.doc);
  EXPECT_EQ(42, placed.key);
  const auto unplaced = RemovedRow(-3, kNoRowPosition);
  EXPECT_EQ(irs::DocRemoval::kNoSegment, unplaced.segment);
  EXPECT_EQ(-3, unplaced.key);
}

const irs::CompactionPolicy& FullMerge() {
  static const auto kPolicy = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});
  return kPolicy;
}

class SearchRowRemovalTest : public ::testing::Test {
 protected:
  void SetUp() override { Open(); }

  void TearDown() override { _writer.reset(); }

  void Open() {
    irs::IndexWriterOptions options;
    options.db = &TestDb();
    options.reader_options.db = &TestDb();
    _writer = irs::IndexWriter::Make(_dir, irs::kOmCreate, std::move(options));
  }

  static void WriteRows(irs::IndexWriter::Transaction& trx,
                        std::span<const int64_t> rowids) {
    DuckDBSearchSinkInsertWriter sink{
      trx, KeywordTokenizer, std::array<ColumnId, 1>{kValueColumn},
      NoEntryInfoProvider(),
      PkPolicy{.index_term = false, .column = catalog::PkColumnKind::Has}};
    for (size_t first = 0; first < rowids.size();
         first += STANDARD_VECTOR_SIZE) {
      const auto n =
        std::min<size_t>(STANDARD_VECTOR_SIZE, rowids.size() - first);
      duckdb::Vector ids{duckdb::LogicalType::BIGINT, n};
      duckdb::Vector values{duckdb::LogicalType::BIGINT, n};
      auto* id_data = duckdb::FlatVector::GetDataMutable<int64_t>(ids);
      auto* value_data = duckdb::FlatVector::GetDataMutable<int64_t>(values);
      for (size_t i = 0; i < n; ++i) {
        id_data[i] = rowids[first + i];
        value_data[i] = rowids[first + i] * 10;
      }
      sink.Init(n, PkChunk{.column = &ids});
      sink.SwitchColumn(
        ColumnDescriptor{kValueColumn, duckdb::LogicalType::BIGINT}, values, n);
      sink.Finish();
    }
  }

  void Insert(std::span<const int64_t> rowids) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    WriteRows(trx, rowids);
    ASSERT_TRUE(trx.Commit());
    _writer->RefreshCommit();
    _model.insert(rowids.begin(), rowids.end());
  }

  void InsertRange(int64_t first, int64_t count) {
    std::vector<int64_t> rowids(count);
    std::iota(rowids.begin(), rowids.end(), first);
    Insert(rowids);
  }

  struct Located {
    int64_t rowid;
    uint64_t position;
  };

  static std::vector<int64_t> ReadRowids(const irs::SubReader& segment) {
    std::vector<int64_t> out;
    const auto* columns = segment.GetColReader();
    const auto* column = columns ? columns->Column(kGeneratedPKId) : nullptr;
    if (!column) {
      return out;
    }
    irs::ReadContext ctx{*columns};
    auto state = column->InitScan(ctx);
    irs::ColumnReader::VectorScratch scratch{column->Type()};
    uint64_t row = 0;
    while (row < column->RowCount()) {
      const auto take =
        std::min<uint64_t>(column->RowCount() - row, STANDARD_VECTOR_SIZE);
      auto& values = scratch.Reset();
      column->Scan(state, values, take);
      duckdb::UnifiedVectorFormat format;
      values.ToUnifiedFormat(take, format);
      const auto* data = duckdb::UnifiedVectorFormat::GetData<int64_t>(format);
      for (duckdb::idx_t i = 0; i < take; ++i) {
        out.push_back(data[format.sel->get_index(i)]);
      }
      row += take;
    }
    return out;
  }

  std::map<int64_t, uint64_t> LivePositions() const {
    std::map<int64_t, uint64_t> out;
    const auto reader = _writer->GetSnapshot();
    for (const auto& segment : reader) {
      const auto number = SegmentNumber(segment.Meta().name);
      EXPECT_TRUE(number.has_value());
      const auto rowids = ReadRowids(segment);
      auto mask = segment.MaskedDocs();
      for (size_t row = 0; row < rowids.size(); ++row) {
        const auto doc =
          static_cast<irs::doc_id_t>(row) + irs::doc_limits::min();
        if (mask.Contains(doc)) {
          continue;
        }
        EXPECT_TRUE(
          out.emplace(rowids[row], MakeRowPosition(*number, doc)).second)
          << "rowid " << rowids[row] << " is alive twice";
      }
    }
    return out;
  }

  std::set<int64_t> Live() const {
    std::set<int64_t> out;
    for (const auto& [rowid, position] : LivePositions()) {
      out.insert(rowid);
    }
    return out;
  }

  std::vector<uint64_t> PositionsOf(std::span<const int64_t> rowids) const {
    const auto positions = LivePositions();
    std::vector<uint64_t> out;
    for (const auto rowid : rowids) {
      const auto it = positions.find(rowid);
      out.push_back(it == positions.end() ? kNoRowPosition : it->second);
    }
    return out;
  }

  void Remove(std::span<const int64_t> rowids,
              std::span<const uint64_t> positions) {
    auto trx = _writer->GetBatch();
    if (auto removal = MakeRowRemoval(rowids, positions)) {
      trx.Remove(std::move(removal));
    }
    ASSERT_TRUE(trx.Commit());
    _writer->RefreshCommit();
    for (const auto rowid : rowids) {
      _model.erase(rowid);
    }
  }

  void RemoveDecoyed(std::span<const int64_t> keys,
                     std::span<const uint64_t> positions,
                     std::span<const int64_t> victims) {
    auto trx = _writer->GetBatch();
    trx.Remove(MakeRowRemoval(keys, positions));
    ASSERT_TRUE(trx.Commit());
    _writer->RefreshCommit();
    for (const auto rowid : victims) {
      _model.erase(rowid);
    }
  }

  void ExpectModel() const {
    const auto live = Live();
    EXPECT_EQ(_model.size(), live.size());
    EXPECT_TRUE(std::ranges::equal(_model, live));
  }

  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _writer;
  std::set<int64_t> _model;
};

TEST(RowPositionTest, SegmentNumberParsesWriterNames) {
  EXPECT_EQ(0, SegmentNumber("_0"));
  EXPECT_EQ(7, SegmentNumber("_7"));
  EXPECT_EQ(123456, SegmentNumber("_123456"));
  EXPECT_EQ(kRowPositionDocMask - 1,
            SegmentNumber(absl::StrCat("_", kRowPositionDocMask - 1)));
}

TEST(RowPositionTest, SegmentNumberRejectsEverythingElse) {
  for (const auto* name : {"", "_", "7", "a7", "_7a", "_-7", "_+7", "_ 7",
                           "__7", "_0x10", "_99999999999999999999999999"}) {
    EXPECT_FALSE(SegmentNumber(name).has_value()) << name;
  }
  EXPECT_FALSE(SegmentNumber(absl::StrCat("_", kRowPositionDocMask)));
  EXPECT_FALSE(SegmentNumber(absl::StrCat("_", kRowPositionDocMask + 1)));
}

TEST(RowPositionTest, PositionRoundTrips) {
  for (const uint64_t segment :
       {uint64_t{0}, uint64_t{1}, uint64_t{77}, kRowPositionDocMask - 1}) {
    for (const irs::doc_id_t doc :
         {irs::doc_limits::min(), irs::doc_id_t{2}, irs::doc_id_t{1 << 20},
          std::numeric_limits<irs::doc_id_t>::max() - 1}) {
      const auto position = MakeRowPosition(segment, doc);
      EXPECT_EQ(segment, RowPositionSegment(position));
      EXPECT_EQ(doc, RowPositionDoc(position));
      EXPECT_NE(kNoRowPosition, position);
    }
  }
}

TEST(RowPositionTest, PositionsOrderBySegmentThenDoc) {
  EXPECT_LT(MakeRowPosition(1, 5), MakeRowPosition(1, 6));
  EXPECT_LT(MakeRowPosition(1, std::numeric_limits<irs::doc_id_t>::max() - 1),
            MakeRowPosition(2, irs::doc_limits::min()));
}

TEST_F(SearchRowRemovalTest, EmptyRemovalRemovesNothing) {
  InsertRange(0, 10);
  Remove({}, {});
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, DuplicateRowidsInASegmentAreAllRemoved) {
  const std::vector<int64_t> rowids{1, 2, 2, 3, 2};
  Insert(rowids);
  const std::vector<int64_t> victims{2};
  Remove(victims, {});
  const auto snapshot = _writer->GetSnapshot();
  EXPECT_EQ(2, snapshot.live_docs_count());
  const auto live = Live();
  EXPECT_EQ((std::set<int64_t>{1, 3}), live);
}

TEST_F(SearchRowRemovalTest, PositionsRemoveExactlyTheirRows) {
  InsertRange(0, 1000);
  InsertRange(1000, 1000);
  InsertRange(2000, 1000);
  std::vector<int64_t> victims;
  for (int64_t r = 0; r < 3000; r += 7) {
    victims.push_back(r);
  }
  Remove(victims, PositionsOf(victims));
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, LivePositionsNeverConsultTheKeys) {
  InsertRange(0, 5000);
  InsertRange(5000, 5000);
  const std::vector<int64_t> victims{3, 4999, 5000, 9999};
  const std::vector<int64_t> decoys{4, 4998, 5001, 9998};
  RemoveDecoyed(decoys, PositionsOf(victims), victims);
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, RowidsWithoutPositionsAreResolvedByScan) {
  InsertRange(0, 3000);
  InsertRange(3000, 3000);
  const std::vector<int64_t> victims{0, 1, 2999, 3000, 5999, 4242};
  Remove(victims, {});
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, UnknownRowidsAreIgnored) {
  InsertRange(0, 100);
  const std::vector<int64_t> victims{-5, 100, 1000000,
                                     std::numeric_limits<int64_t>::max(),
                                     std::numeric_limits<int64_t>::min()};
  Remove(victims, {});
  ExpectModel();
  EXPECT_EQ(100, Live().size());
}

TEST_F(SearchRowRemovalTest, MixedPlacedAndUnplacedRows) {
  InsertRange(0, 2000);
  InsertRange(2000, 2000);
  const std::vector<int64_t> victims{10, 20, 2010, 2020, 3999};
  auto positions = PositionsOf(victims);
  positions[1] = kNoRowPosition;
  positions[3] = kNoRowPosition;
  Remove(victims, positions);
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, StalePositionOfACompactedSegmentFallsBack) {
  InsertRange(0, 2000);
  InsertRange(2000, 2000);
  InsertRange(4000, 2000);
  std::vector<int64_t> victims;
  for (int64_t r = 5; r < 6000; r += 11) {
    victims.push_back(r);
  }
  const auto positions = PositionsOf(victims);
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  _writer->RefreshCommit();
  ASSERT_EQ(1, _writer->GetSnapshot().size());
  Remove(victims, positions);
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, RemovalDuringAPendingCompactionIsRemapped) {
  InsertRange(0, 3000);
  InsertRange(3000, 3000);
  std::vector<int64_t> victims;
  for (int64_t r = 1; r < 6000; r += 13) {
    victims.push_back(r);
  }
  const auto positions = PositionsOf(victims);
  std::vector<int64_t> decoys;
  for (const auto rowid : victims) {
    decoys.emplace_back(rowid + 1);
  }
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  RemoveDecoyed(decoys, positions, victims);
  ASSERT_EQ(1, _writer->GetSnapshot().size());
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, RemovalInFlightDuringAMergeIsRemapped) {
  InsertRange(0, 3000);
  InsertRange(3000, 3000);
  std::vector<int64_t> victims;
  std::vector<int64_t> decoys;
  for (int64_t r = 2; r < 6000; r += 17) {
    victims.emplace_back(r);
    decoys.emplace_back(r + 1);
  }
  const auto positions = PositionsOf(victims);
  auto trx = _writer->GetBatch();
  trx.Remove(MakeRowRemoval(decoys, positions));
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  ASSERT_TRUE(trx.Commit());
  _writer->RefreshCommit();
  for (const auto rowid : victims) {
    _model.erase(rowid);
  }
  ASSERT_EQ(1, _writer->GetSnapshot().size());
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, CompactionAfterRemovalKeepsRowsRemoved) {
  InsertRange(0, 3000);
  InsertRange(3000, 3000);
  std::vector<int64_t> victims{0, 2999, 3000, 5999, 1234};
  Remove(victims, PositionsOf(victims));
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  _writer->RefreshCommit();
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, SegmentFullyRemovedDisappears) {
  InsertRange(0, 100);
  InsertRange(100, 100);
  std::vector<int64_t> victims(100);
  std::iota(victims.begin(), victims.end(), 0);
  Remove(victims, PositionsOf(victims));
  EXPECT_EQ(1, _writer->GetSnapshot().size());
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, AlreadyRemovedRowsAreNoOps) {
  InsertRange(0, 500);
  const std::vector<int64_t> victims{1, 2, 3};
  const auto positions = PositionsOf(victims);
  Remove(victims, positions);
  Remove(victims, positions);
  Remove(victims, {});
  ExpectModel();
  EXPECT_EQ(497, Live().size());
}

TEST_F(SearchRowRemovalTest, DuplicatesInOneRemovalAreHarmless) {
  InsertRange(0, 500);
  const std::vector<int64_t> victims{9, 9, 9, 10, 10};
  const auto positions = PositionsOf(victims);
  Remove(victims, positions);
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, PositionPastTheSegmentEndIsIgnored) {
  InsertRange(0, 50);
  const auto live = LivePositions();
  const auto segment = RowPositionSegment(live.begin()->second);
  const std::vector<int64_t> victims{999};
  const std::vector<uint64_t> positions{
    MakeRowPosition(segment, irs::doc_limits::min() + 50)};
  Remove(victims, positions);
  EXPECT_EQ(50, Live().size());
}

TEST_F(SearchRowRemovalTest, PositionOfAnUnknownSegmentUsesTheRowid) {
  InsertRange(0, 50);
  const std::vector<int64_t> victims{7};
  const std::vector<uint64_t> positions{MakeRowPosition(4000000, 1)};
  Remove(victims, positions);
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, InterleavedRowidsAcrossSegmentsAndBlocks) {
  std::mt19937_64 rng{42};
  std::vector<int64_t> all(300000);
  std::iota(all.begin(), all.end(), 0);
  std::shuffle(all.begin(), all.end(), rng);
  const std::span<const int64_t> view{all};
  Insert(view.subspan(0, 150000));
  Insert(view.subspan(150000));
  std::vector<int64_t> victims;
  for (int64_t r = 3; r < 300000; r += 97) {
    victims.push_back(r);
  }
  Remove(victims, {});
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, SequentialRowidsAcrossManyBlocks) {
  InsertRange(0, 400000);
  std::vector<int64_t> victims;
  for (int64_t r = 0; r < 400000; r += 1013) {
    victims.push_back(r);
  }
  victims.push_back(399999);
  Remove(victims, {});
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, ManyKeyedRemovalsInOneCommit) {
  InsertRange(0, 4000);
  InsertRange(4000, 4000);
  InsertRange(8000, 4000);
  auto trx = _writer->GetBatch();
  for (int64_t chunk = 0; chunk < 50; ++chunk) {
    const std::array<int64_t, 2> rowids{chunk * 7, 11999 - chunk * 5};
    trx.Remove(MakeRowRemoval(rowids, {}));
    _model.erase(rowids[0]);
    _model.erase(rowids[1]);
  }
  ASSERT_TRUE(trx.Commit());
  _writer->RefreshCommit();
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, ManyStalePositionsInOneCommit) {
  InsertRange(0, 4000);
  InsertRange(4000, 4000);
  const auto before = LivePositions();
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  _writer->RefreshCommit();
  auto trx = _writer->GetBatch();
  for (int64_t chunk = 0; chunk < 40; ++chunk) {
    const std::array<int64_t, 1> rowids{chunk * 199};
    const std::array<uint64_t, 1> positions{before.at(rowids[0])};
    trx.Remove(MakeRowRemoval(rowids, positions));
    _model.erase(rowids[0]);
  }
  ASSERT_TRUE(trx.Commit());
  _writer->RefreshCommit();
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, ResolverMergesPositionalAndKeyedDocs) {
  InsertRange(0, 1000);
  const auto reader = _writer->GetSnapshot();
  ASSERT_EQ(1, reader.size());
  const auto number = *SegmentNumber(reader[0].Meta().name);
  const std::array<int64_t, 5> rowids{900, 7, 5, 300, 300};
  const std::array<uint64_t, 5> positions{
    MakeRowPosition(number, irs::doc_limits::min() + 900),
    MakeRowPosition(number + 1, irs::doc_limits::min() + 7), kNoRowPosition,
    MakeRowPosition(number, irs::doc_limits::min() + 300), kNoRowPosition};
  const auto removal = MakeRowRemoval(rowids, positions);
  const std::array<const irs::SubReader*, 1> live{&reader[0]};
  EXPECT_EQ((std::vector<irs::doc_id_t>{
              irs::doc_limits::min() + 5, irs::doc_limits::min() + 7,
              irs::doc_limits::min() + 300, irs::doc_limits::min() + 900}),
            ResolveDocs(*removal, live, reader[0]));
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{removal.get()};
  resolver.Prepare(removals, live);
  const auto positional = resolver.PositionalDocs(*removal, reader[0]);
  EXPECT_EQ((std::vector<irs::doc_id_t>{irs::doc_limits::min() + 300,
                                        irs::doc_limits::min() + 900}),
            std::vector<irs::doc_id_t>(positional.begin(), positional.end()));
}

TEST_F(SearchRowRemovalTest, ResolverAnswersEachRemovalWithItsOwnKeys) {
  InsertRange(0, 1000);
  const auto reader = _writer->GetSnapshot();
  const std::array<int64_t, 2> first_keys{1, 5};
  const std::array<int64_t, 2> second_keys{3, 999};
  const auto first = MakeRowRemoval(first_keys, {});
  const auto second = MakeRowRemoval(second_keys, {});
  const auto unprepared = MakeRowRemoval(second_keys, {});
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 2> removals{first.get(),
                                                       second.get()};
  resolver.Prepare(removals, {});
  auto docs = resolver.Docs(*first, reader[0]);
  EXPECT_EQ((std::vector<irs::doc_id_t>{2, 6}),
            std::vector<irs::doc_id_t>(docs.begin(), docs.end()));
  docs = resolver.Docs(*second, reader[0]);
  EXPECT_EQ((std::vector<irs::doc_id_t>{4, 1000}),
            std::vector<irs::doc_id_t>(docs.begin(), docs.end()));
  docs = resolver.Docs(*first, reader[0]);
  EXPECT_EQ((std::vector<irs::doc_id_t>{2, 6}),
            std::vector<irs::doc_id_t>(docs.begin(), docs.end()));
  EXPECT_TRUE(resolver.Docs(*unprepared, reader[0]).empty());
}

TEST_F(SearchRowRemovalTest, ResolverKeepsKeyColumnsApart) {
  InsertRange(0, 100);
  const auto reader = _writer->GetSnapshot();
  const std::array<int64_t, 2> keys{4, 2000};
  const auto by_rowid = MakeRowRemoval(keys, {});
  std::vector<irs::DocRemoval::Row> rows{{irs::DocRemoval::kNoSegment, 0, 4}};
  const auto by_other = std::make_shared<const irs::DocRemoval>(
    irs::DocRemoval::Build(kGeneratedPKId + 1000, std::move(rows)));
  const std::array<int64_t, 1> missing_keys{2000};
  const auto missing = MakeRowRemoval(missing_keys, {});
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 3> removals{
    by_rowid.get(), by_other.get(), missing.get()};
  resolver.Prepare(removals, {});
  const auto docs = resolver.Docs(*by_rowid, reader[0]);
  EXPECT_EQ((std::vector<irs::doc_id_t>{irs::doc_limits::min() + 4}),
            std::vector<irs::doc_id_t>(docs.begin(), docs.end()));
  EXPECT_TRUE(resolver.Docs(*by_other, reader[0]).empty());
  EXPECT_TRUE(resolver.Docs(*missing, reader[0]).empty());
}

TEST_F(SearchRowRemovalTest, ResolverPrunesSegmentsOutsideTheRange) {
  InsertRange(0, 1000);
  InsertRange(1000000, 1000);
  const std::array<int64_t, 1> rowids{500};
  const auto removal = MakeRowRemoval(rowids, {});
  const auto reader = _writer->GetSnapshot();
  size_t hits = 0;
  for (const auto& segment : reader) {
    hits += ResolveDocs(*removal, {}, segment).size();
  }
  EXPECT_EQ(1, hits);
}

TEST_F(SearchRowRemovalTest, ResolverSkipsBlocksWithoutTargets) {
  InsertRange(0, 400000);
  const auto reader = _writer->GetSnapshot();
  ASSERT_EQ(1, reader.size());
  const auto* column = reader[0].GetColReader()->Column(kGeneratedPKId);
  ASSERT_NE(nullptr, column);
  ASSERT_GT(column->DataBlocks().size(), 2);
  const std::array<int64_t, 2> rowids{5, 7};
  EXPECT_EQ((std::vector<irs::doc_id_t>{irs::doc_limits::min() + 5,
                                        irs::doc_limits::min() + 7}),
            ResolveDocs(*MakeRowRemoval(rowids, {}), {}, reader[0]));
  const std::array<int64_t, 3> spread{5, 7, 399999};
  EXPECT_EQ((std::vector<irs::doc_id_t>{irs::doc_limits::min() + 5,
                                        irs::doc_limits::min() + 7,
                                        irs::doc_limits::min() + 399999}),
            ResolveDocs(*MakeRowRemoval(spread, {}), {}, reader[0]));
}

TEST_F(SearchRowRemovalTest, ResolverIgnoresSegmentsWithoutTheRowidColumn) {
  {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    DuckDBSearchSinkInsertWriter sink{
      trx, KeywordTokenizer, std::array<ColumnId, 1>{kValueColumn},
      NoEntryInfoProvider(),
      PkPolicy{.index_term = false, .column = catalog::PkColumnKind::None}};
    duckdb::Vector values{duckdb::LogicalType::BIGINT, 3};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(values);
    data[0] = 1;
    data[1] = 2;
    data[2] = 3;
    sink.Init(3, PkChunk{});
    sink.SwitchColumn(
      ColumnDescriptor{kValueColumn, duckdb::LogicalType::BIGINT}, values, 3);
    sink.Finish();
    ASSERT_TRUE(trx.Commit());
    _writer->RefreshCommit();
  }
  const auto reader = _writer->GetSnapshot();
  ASSERT_EQ(1, reader.size());
  ASSERT_EQ(nullptr, reader[0].GetColReader()->Column(kGeneratedPKId));
  const std::array<int64_t, 2> rowids{1, 2};
  EXPECT_TRUE(ResolveDocs(*MakeRowRemoval(rowids, {}), {}, reader[0]).empty());

  const std::vector<int64_t> victims{1, 2, 3};
  auto trx = _writer->GetBatch();
  trx.Remove(MakeRowRemoval(victims, {}));
  ASSERT_TRUE(trx.Commit());
  _writer->RefreshCommit();
  EXPECT_EQ(3, _writer->GetSnapshot().live_docs_count());
}

TEST_F(SearchRowRemovalTest, ResolverSkipsNullRowids) {
  auto write = [&](std::initializer_list<std::optional<int64_t>> rowids) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    DuckDBSearchSinkInsertWriter sink{
      trx, KeywordTokenizer, std::array<ColumnId, 1>{kValueColumn},
      NoEntryInfoProvider(),
      PkPolicy{.index_term = false, .column = catalog::PkColumnKind::Has}};
    const auto n = rowids.size();
    duckdb::Vector ids{duckdb::LogicalType::BIGINT, n};
    duckdb::Vector values{duckdb::LogicalType::BIGINT, n};
    auto* id_data = duckdb::FlatVector::GetDataMutable<int64_t>(ids);
    auto* value_data = duckdb::FlatVector::GetDataMutable<int64_t>(values);
    size_t i = 0;
    for (const auto& rowid : rowids) {
      id_data[i] = rowid.value_or(1000 + static_cast<int64_t>(i));
      value_data[i] = static_cast<int64_t>(i);
      if (!rowid) {
        duckdb::FlatVector::ValidityMutable(ids).SetInvalid(i);
      }
      ++i;
    }
    sink.Init(n, PkChunk{.column = &ids});
    sink.SwitchColumn(
      ColumnDescriptor{kValueColumn, duckdb::LogicalType::BIGINT}, values, n);
    sink.Finish();
    ASSERT_TRUE(trx.Commit());
    _writer->RefreshCommit();
  };
  write({10, std::nullopt, 12, 13});
  write({std::nullopt, std::nullopt});
  const auto reader = _writer->GetSnapshot();
  ASSERT_EQ(2, reader.size());
  const std::array<int64_t, 5> rowids{10, 12, 1000, 1001, 1002};
  const auto removal = MakeRowRemoval(rowids, {});
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{removal.get()};
  resolver.Prepare(removals, {});
  size_t hits = 0;
  for (const auto& segment : reader) {
    hits += resolver.Docs(*removal, segment).size();
  }
  EXPECT_EQ(2, hits);
}

TEST_F(SearchRowRemovalTest, MakeRowRemovalGroupsRowsBySegment) {
  EXPECT_EQ(nullptr, MakeRowRemoval({}, {}));
  const std::array<int64_t, 4> rowids{5, 6, 3, 3};
  const std::array<uint64_t, 4> positions{MakeRowPosition(2, 7),
                                          MakeRowPosition(1, 3), kNoRowPosition,
                                          kNoRowPosition};
  const auto removal = MakeRowRemoval(rowids, positions);
  ASSERT_NE(nullptr, removal);
  EXPECT_EQ(kGeneratedPKId, removal->key_column);
  EXPECT_EQ((std::vector<uint64_t>{1, 2, irs::DocRemoval::kNoSegment}),
            removal->segments);
  EXPECT_EQ((std::vector<irs::doc_id_t>{3, 7, 0}), removal->docs);
  EXPECT_EQ((std::vector<int64_t>{6, 5, 3}), removal->keys);
  const auto unplaced = MakeRowRemoval(rowids, {});
  ASSERT_NE(nullptr, unplaced);
  EXPECT_EQ((std::vector<uint64_t>{irs::DocRemoval::kNoSegment}),
            unplaced->segments);
  EXPECT_EQ((std::vector<int64_t>{3, 5, 6}), unplaced->keys);
}

class FakeSegment final : public irs::SubReader {
 public:
  FakeSegment(std::string name, uint32_t docs) {
    _info.name = std::move(name);
    _info.docs_count = docs;
    _info.live_docs_count = docs;
  }

  uint64_t CountMappedMemory() const final { return 0; }
  irs::NormReader::ptr norms(irs::field_id) const final { return nullptr; }
  const irs::SegmentInfo& Meta() const final { return _info; }
  const irs::DocumentMask* docs_mask() const final { return nullptr; }
  irs::lead::Node::ptr docs_iterator() const final { return nullptr; }
  std::span<const irs::field_id> field_ids() const final { return {}; }
  const irs::TermReader* field(irs::field_id) const final { return nullptr; }

 private:
  irs::SegmentInfo _info;
};

TEST(DocRemovalResolverTest, UnparseableSegmentGetsNoPositionalDocs) {
  const FakeSegment bogus{"bogus", 10};
  const FakeSegment numbered{"_3", 10};
  const std::array<int64_t, 2> rowids{1, 2};
  const std::array<uint64_t, 2> positions{MakeRowPosition(3, 2),
                                          MakeRowPosition(3, 5)};
  const auto removal = MakeRowRemoval(rowids, positions);
  const std::array<const irs::SubReader*, 2> live{&bogus, &numbered};
  EXPECT_TRUE(ResolveDocs(*removal, live, bogus).empty());
  EXPECT_EQ((std::vector<irs::doc_id_t>{2, 5}),
            ResolveDocs(*removal, live, numbered));
}

TEST(DocRemovalResolverTest,
     SegmentNumberedLikeTheSentinelGetsNoPositionalDocs) {
  const FakeSegment sentinel{irs::FileName(irs::DocRemoval::kNoSegment), 10};
  const std::array<int64_t, 1> rowids{1};
  const auto removal = MakeRowRemoval(rowids, {});
  ASSERT_EQ(irs::DocRemoval::kNoSegment, removal->segments.front());
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{removal.get()};
  const std::array<const irs::SubReader*, 1> live{&sentinel};
  resolver.Prepare(removals, live);
  EXPECT_TRUE(resolver.PositionalDocs(*removal, sentinel).empty());
  EXPECT_TRUE(resolver.Docs(*removal, sentinel).empty());
}

TEST(DocRemovalResolverTest, PositionsPastTheSegmentEndAreCut) {
  const FakeSegment segment{"_3", 4};
  const std::array<int64_t, 3> rowids{1, 2, 3};
  const std::array<uint64_t, 3> positions{
    MakeRowPosition(3, 4), MakeRowPosition(3, 5), MakeRowPosition(3, 9)};
  const auto removal = MakeRowRemoval(rowids, positions);
  const std::array<const irs::SubReader*, 1> live{&segment};
  EXPECT_EQ((std::vector<irs::doc_id_t>{4}),
            ResolveDocs(*removal, live, segment));
}

TEST(DocRemovalResolverTest, VanishedTargetsFallBackToASegmentWithoutRowids) {
  const FakeSegment other{"_9", 10};
  const std::array<int64_t, 1> rowids{1};
  const std::array<uint64_t, 1> positions{MakeRowPosition(3, 2)};
  const auto removal = MakeRowRemoval(rowids, positions);
  const std::array<const irs::SubReader*, 1> live{&other};
  EXPECT_TRUE(ResolveDocs(*removal, live, other).empty());
}

TEST(RowPositionTest, FillRowPositions) {
  duckdb::Vector out{duckdb::LogicalType::UBIGINT, 4};
  FillRowPositions(
    std::optional<uint64_t>{7}, 4,
    [](duckdb::idx_t i) { return static_cast<irs::doc_id_t>(10 + 2 * i); },
    out);
  const auto* data = duckdb::FlatVector::GetData<uint64_t>(out);
  for (duckdb::idx_t i = 0; i < 4; ++i) {
    EXPECT_TRUE(duckdb::FlatVector::Validity(out).RowIsValid(i));
    EXPECT_EQ(MakeRowPosition(7, static_cast<irs::doc_id_t>(10 + 2 * i)),
              data[i]);
  }
  duckdb::Vector null_out{duckdb::LogicalType::UBIGINT, 4};
  FillRowPositions(
    std::nullopt, 3, [](duckdb::idx_t) { return irs::doc_id_t{1}; }, null_out);
  for (duckdb::idx_t i = 0; i < 3; ++i) {
    EXPECT_FALSE(duckdb::FlatVector::Validity(null_out).RowIsValid(i));
  }
  EXPECT_TRUE(duckdb::FlatVector::Validity(null_out).RowIsValid(3));
}

TEST(LocalTableChangesTest, DeletesMergeOnlyWithinOneBand) {
  search::LocalTableChangesEntry entry;
  const std::array<int64_t, 2> rows{1, 2};
  const std::array<uint64_t, 2> positions{MakeRowPosition(1, 1),
                                          MakeRowPosition(1, 2)};
  entry.AppendDeletes(rows, positions);
  entry.AppendDeletes(rows, positions);
  ASSERT_EQ(1, entry.ops.size());
  EXPECT_EQ(4, entry.ops[0].delete_rows.size());
  EXPECT_EQ(4, entry.ops[0].delete_positions.size());

  entry.pk_segments.push_back({0, 10});
  entry.AppendDeletes(rows, positions);
  ASSERT_EQ(2, entry.ops.size());
  EXPECT_EQ(1, entry.ops[1].band_watermark);

  entry.applied_ops = entry.ops.size();
  entry.AppendDeletes(rows, positions);
  ASSERT_EQ(3, entry.ops.size());

  entry.AppendDeletes({}, {});
  ASSERT_EQ(3, entry.ops.size());

  entry.AppendTruncate(false);
  ASSERT_EQ(1, entry.ops.size());
  entry.AppendDeletes(rows, positions);
  ASSERT_EQ(2, entry.ops.size());
  EXPECT_TRUE(entry.ops[0].IsTruncate());
  EXPECT_TRUE(entry.ops[1].IsDelete());
}

TEST(CollectRemovedRowsTest, ReadsRowidsAndPositions) {
  duckdb::DataChunk chunk;
  chunk.InitializeEmpty(duckdb::vector<duckdb::LogicalType>{
    duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
    duckdb::LogicalType::UBIGINT});
  duckdb::Vector text{duckdb::LogicalType::VARCHAR, 3};
  duckdb::Vector rowid{duckdb::LogicalType::BIGINT, 3};
  duckdb::Vector position{duckdb::LogicalType::UBIGINT, 3};
  auto* rowids = duckdb::FlatVector::GetDataMutable<int64_t>(rowid);
  auto* positions = duckdb::FlatVector::GetDataMutable<uint64_t>(position);
  for (int i = 0; i < 3; ++i) {
    rowids[i] = 100 + i;
    positions[i] = MakeRowPosition(4, irs::doc_limits::min() + i);
  }
  duckdb::FlatVector::ValidityMutable(position).SetInvalid(1);
  chunk.data[0].Reference(text);
  chunk.data[1].Reference(rowid);
  chunk.data[2].Reference(position);
  chunk.SetCardinality(3);

  std::vector<int64_t> rows;
  std::vector<uint64_t> places;
  const std::vector<primary_key::PKColumn> both{
    {1, duckdb::LogicalType::BIGINT}, {2, duckdb::LogicalType::UBIGINT}};
  CollectRemovedRows(chunk, both, rows, places);
  EXPECT_EQ((std::vector<int64_t>{100, 101, 102}), rows);
  EXPECT_EQ((std::vector<uint64_t>{MakeRowPosition(4, 1), kNoRowPosition,
                                   MakeRowPosition(4, 3)}),
            places);
}

TEST_F(SearchRowRemovalTest, RemovalReachesReplacementSegments) {
  InsertRange(0, 1000);
  std::vector<std::string> sources;
  for (const auto& segment : _writer->GetSnapshot()) {
    sources.emplace_back(segment.Meta().name);
  }
  const std::vector<int64_t> victims{0, 500, 999};
  const auto positions = PositionsOf(victims);

  auto build = _writer->GetBatch(/*exclusive_segment=*/true);
  std::vector<int64_t> copy(1000);
  std::iota(copy.begin(), copy.end(), 0);
  WriteRows(build, copy);
  std::vector<std::string> metas;
  for (const auto& flushed : build.FlushAndFsync()) {
    metas.emplace_back(flushed.filename);
  }
  build.Abort();

  const auto removal = MakeRowRemoval(victims, positions);
  auto trx = _writer->GetBatch();
  trx.Remove(removal);
  ASSERT_TRUE(trx.Commit());

  std::vector<std::string_view> replaced{sources.begin(), sources.end()};
  std::vector<std::string_view> adopted{metas.begin(), metas.end()};
  ASSERT_TRUE(_writer->ReplaceSegments(
    replaced, adopted, irs::IndexWriter::QueryContext::RemovalPtr{}));
  _writer->RefreshCommit();
  for (const auto rowid : victims) {
    _model.erase(rowid);
  }
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, ReplaceSegmentsCarriesARowidRemoval) {
  InsertRange(0, 1000);
  std::vector<std::string> sources;
  for (const auto& segment : _writer->GetSnapshot()) {
    sources.emplace_back(segment.Meta().name);
  }
  auto build = _writer->GetBatch(/*exclusive_segment=*/true);
  std::vector<int64_t> copy(1000);
  std::iota(copy.begin(), copy.end(), 0);
  WriteRows(build, copy);
  std::vector<std::string> metas;
  for (const auto& flushed : build.FlushAndFsync()) {
    metas.emplace_back(flushed.filename);
  }
  build.Abort();

  const std::vector<int64_t> victims{1, 2, 998};
  const auto removal = MakeRowRemoval(victims, {});
  std::vector<std::string_view> replaced{sources.begin(), sources.end()};
  std::vector<std::string_view> adopted{metas.begin(), metas.end()};
  ASSERT_TRUE(_writer->ReplaceSegments(replaced, adopted, removal));
  _writer->RefreshCommit();
  for (const auto rowid : victims) {
    _model.erase(rowid);
  }
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, RemovalBeforeAnInsertInOneTransaction) {
  InsertRange(0, 100);
  auto trx = _writer->GetBatch();
  const std::vector<int64_t> victims{5, 200};
  const auto removal = MakeRowRemoval(victims, {});
  trx.Remove(removal);
  std::vector<int64_t> later{200, 201};
  WriteRows(trx, later);
  ASSERT_TRUE(trx.Commit());
  _writer->RefreshCommit();
  _model.erase(5);
  _model.insert(200);
  _model.insert(201);
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, InsertBeforeARemovalInOneTransaction) {
  InsertRange(0, 100);
  auto trx = _writer->GetBatch();
  std::vector<int64_t> earlier{300, 301};
  WriteRows(trx, earlier);
  const std::vector<int64_t> victims{6, 300};
  const auto removal = MakeRowRemoval(victims, {});
  trx.Remove(removal);
  ASSERT_TRUE(trx.Commit());
  _writer->RefreshCommit();
  _model.erase(6);
  _model.insert(301);
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, SeveralRemovalsInOneCommit) {
  InsertRange(0, 3000);
  InsertRange(3000, 3000);
  auto trx = _writer->GetBatch();
  for (int64_t k = 0; k < 6; ++k) {
    std::vector<int64_t> victims;
    for (int64_t r = k; r < 6000; r += 600) {
      victims.push_back(r);
    }
    const auto positions =
      k % 2 == 0 ? PositionsOf(victims) : std::vector<uint64_t>{};
    const auto removal = MakeRowRemoval(victims, positions);
    trx.Remove(removal);
    for (const auto rowid : victims) {
      _model.erase(rowid);
    }
  }
  ASSERT_TRUE(trx.Commit());
  _writer->RefreshCommit();
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, RandomizedAgainstAModel) {
  std::mt19937_64 rng{7};
  int64_t next = 0;
  for (int round = 0; round < 60; ++round) {
    const auto action = rng() % 10;
    if (action < 4 || _model.empty()) {
      const auto n = static_cast<int64_t>(1 + rng() % 3000);
      InsertRange(next, n);
      next += n;
      continue;
    }
    if (action < 6) {
      ASSERT_TRUE(_writer->Compact(FullMerge()));
      if (rng() % 2) {
        _writer->RefreshCommit();
      }
      continue;
    }
    const auto live = LivePositions();
    std::vector<int64_t> victims;
    std::vector<uint64_t> positions;
    for (const auto& [rowid, position] : live) {
      if (rng() % 9 == 0) {
        victims.push_back(rowid);
        positions.push_back(rng() % 5 == 0 ? kNoRowPosition : position);
      }
    }
    if (rng() % 3 == 0) {
      ASSERT_TRUE(_writer->Compact(FullMerge()));
    }
    if (rng() % 4 == 0) {
      _writer->RefreshCommit();
    }
    Remove(victims, positions);
    ExpectModel();
  }
  _writer->RefreshCommit();
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, ConcurrentInsertRemoveCompact) {
  constexpr int64_t kInserters = 3;
  constexpr int64_t kBatches = 40;
  constexpr int64_t kBatch = 1500;
  std::mutex mutex;
  std::set<int64_t> inserted;
  std::set<int64_t> removed;
  std::atomic_bool stop{false};

  std::vector<std::thread> threads;
  for (int64_t t = 0; t < kInserters; ++t) {
    threads.emplace_back([&, t] {
      for (int64_t b = 0; b < kBatches; ++b) {
        std::vector<int64_t> rowids(kBatch);
        std::iota(rowids.begin(), rowids.end(), (t * kBatches + b) * kBatch);
        auto trx = _writer->GetBatch(/*exclusive_segment=*/b % 2 == 0);
        WriteRows(trx, rowids);
        EXPECT_TRUE(trx.Commit());
        std::lock_guard lock{mutex};
        inserted.insert(rowids.begin(), rowids.end());
      }
    });
  }
  std::vector<std::thread> removers;
  for (int t = 0; t < 2; ++t) {
    removers.emplace_back([&, t] {
      std::mt19937_64 rng{static_cast<uint64_t>(100 + t)};
      while (!stop.load()) {
        const auto live = LivePositions();
        std::vector<int64_t> victims;
        std::vector<uint64_t> positions;
        for (const auto& [rowid, position] : live) {
          if (rng() % 50 == 0) {
            victims.push_back(rowid);
            positions.push_back(position);
          }
        }
        if (victims.empty()) {
          std::this_thread::yield();
          continue;
        }
        const auto removal = MakeRowRemoval(victims, positions);
        auto trx = _writer->GetBatch();
        trx.Remove(removal);
        EXPECT_TRUE(trx.Commit());
        std::lock_guard lock{mutex};
        removed.insert(victims.begin(), victims.end());
      }
    });
  }
  std::thread compactor{[&] {
    while (!stop.load()) {
      _writer->Compact(
        irs::index_utils::MakePolicy(irs::index_utils::CompactionCount{3}));
    }
  }};
  std::thread refresher{[&] {
    while (!stop.load()) {
      _writer->RefreshCommit();
    }
  }};
  for (auto& thread : threads) {
    thread.join();
  }
  stop.store(true);
  for (auto& thread : removers) {
    thread.join();
  }
  compactor.join();
  refresher.join();
  _writer->RefreshCommit();

  std::set<int64_t> expected;
  std::ranges::set_difference(inserted, removed,
                              std::inserter(expected, expected.end()));
  const auto live = Live();
  EXPECT_EQ(expected.size(), live.size());
  EXPECT_TRUE(std::ranges::equal(expected, live));
  EXPECT_FALSE(removed.empty());
}

}  // namespace
