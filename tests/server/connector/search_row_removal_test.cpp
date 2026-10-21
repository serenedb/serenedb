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
#include <absl/synchronization/mutex.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <duckdb.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/vector/constant_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/storage/wal_entry.hpp>
#include <iresearch/analysis/keyword_tokenizer.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
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

duckdb::WALDeleteRowRanges RunsOf(std::span<const int64_t> rowids,
                                  std::span<const uint64_t> positions) {
  duckdb::WALDeleteRowRanges runs;
  for (size_t i = 0; i < rowids.size(); ++i) {
    search::AppendRowRun(runs, rowids[i], positions[i]);
  }
  return runs;
}

std::shared_ptr<const irs::DocRemoval> MakeRowRemoval(
  std::span<const int64_t> rowids, std::span<const uint64_t> positions) {
  const auto runs = RunsOf(rowids, positions);
  return MakeRowRunRemoval(runs.starts, runs.counts, runs.positions);
}

std::shared_ptr<const irs::DocRemoval> MakeUnplacedRemoval(
  std::span<const int64_t> rowids) {
  const std::vector<uint64_t> positions(rowids.size(), kNoRowPosition);
  return MakeRowRemoval(rowids, positions);
}

std::vector<std::pair<int64_t, int64_t>> Pairs(
  std::span<const irs::KeyRange> ranges) {
  std::vector<std::pair<int64_t, int64_t>> out;
  for (const auto& range : ranges) {
    out.emplace_back(range.begin, range.end);
  }
  return out;
}

std::vector<irs::doc_id_t> DocsOf(const irs::DocRemovalResolver& resolver,
                                  const irs::DocRemoval& removal,
                                  const irs::SubReader& segment) {
  const auto positional = resolver.PositionalDocs(removal, segment);
  std::vector<irs::doc_id_t> docs{positional.begin(), positional.end()};
  if (resolver.ByKey(removal)) {
    for (const auto& range : resolver.ByKeyDocs(segment)) {
      for (auto doc = range.begin; doc != range.end; ++doc) {
        docs.emplace_back(doc);
      }
    }
  }
  std::ranges::sort(docs);
  const auto duplicates = std::ranges::unique(docs);
  docs.erase(duplicates.begin(), duplicates.end());
  return docs;
}

std::vector<irs::doc_id_t> ResolveDocs(
  const irs::DocRemoval& removal, std::span<const irs::SubReader* const> live,
  const irs::SubReader& segment) {
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  resolver.Prepare(removals, live);
  return DocsOf(resolver, removal, segment);
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
      const auto number = irs::SegmentNumber(segment.Meta().name);
      EXPECT_NE(irs::DocRemoval::kNoSegment, number);
      const auto rowids = ReadRowids(segment);
      auto mask = segment.MaskedDocs();
      for (size_t row = 0; row < rowids.size(); ++row) {
        const auto doc =
          static_cast<irs::doc_id_t>(row) + irs::doc_limits::min();
        if (mask.Contains(doc)) {
          continue;
        }
        EXPECT_TRUE(
          out.emplace(rowids[row], MakeRowPosition(number, doc)).second)
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
    if (auto removal = positions.empty() ? MakeUnplacedRemoval(rowids)
                                         : MakeRowRemoval(rowids, positions)) {
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

TEST(RowPositionTest, PositionRoundTrips) {
  for (const uint32_t segment : {uint32_t{0}, uint32_t{1}, uint32_t{77},
                                 irs::DocRemoval::kNoSegment - 1}) {
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
                                     std::numeric_limits<int64_t>::max() - 1,
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

TEST_F(SearchRowRemovalTest, ManyByKeyRemovalsInOneCommit) {
  InsertRange(0, 4000);
  InsertRange(4000, 4000);
  InsertRange(8000, 4000);
  auto trx = _writer->GetBatch();
  for (int64_t chunk = 0; chunk < 50; ++chunk) {
    const std::array<int64_t, 2> rowids{chunk * 7, 11999 - chunk * 5};
    trx.Remove(MakeUnplacedRemoval(rowids));
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

TEST_F(SearchRowRemovalTest, ResolverMergesPositionalAndByKeyDocs) {
  InsertRange(0, 1000);
  const auto reader = _writer->GetSnapshot();
  ASSERT_EQ(1, reader.size());
  const auto number = irs::SegmentNumber(reader[0].Meta().name);
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
  EXPECT_EQ((std::vector<irs::doc_id_t>{irs::doc_limits::min() + 900,
                                        irs::doc_limits::min() + 300}),
            std::vector<irs::doc_id_t>(positional.begin(), positional.end()));
}

TEST_F(SearchRowRemovalTest, ResolverAnswersEveryByKeyRemovalWithTheUnion) {
  InsertRange(0, 1000);
  const auto reader = _writer->GetSnapshot();
  const std::array<int64_t, 2> first_keys{1, 5};
  const std::array<int64_t, 2> second_keys{3, 999};
  const auto first = MakeUnplacedRemoval(first_keys);
  const auto second = MakeUnplacedRemoval(second_keys);
  const auto unprepared = MakeUnplacedRemoval(second_keys);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 2> removals{first.get(),
                                                       second.get()};
  resolver.Prepare(removals, {});
  const std::vector<irs::doc_id_t> all{2, 4, 6, 1000};
  EXPECT_EQ(all, DocsOf(resolver, *first, reader[0]));
  EXPECT_EQ(all, DocsOf(resolver, *second, reader[0]));
  EXPECT_EQ(all, DocsOf(resolver, *first, reader[0]));
  EXPECT_TRUE(DocsOf(resolver, *unprepared, reader[0]).empty());
}

TEST_F(SearchRowRemovalTest, ResolverPrunesSegmentsOutsideTheRange) {
  InsertRange(0, 1000);
  InsertRange(1000000, 1000);
  const std::array<int64_t, 1> rowids{500};
  const auto removal = MakeUnplacedRemoval(rowids);
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
            ResolveDocs(*MakeUnplacedRemoval(rowids), {}, reader[0]));
  const std::array<int64_t, 3> spread{5, 7, 399999};
  EXPECT_EQ((std::vector<irs::doc_id_t>{irs::doc_limits::min() + 5,
                                        irs::doc_limits::min() + 7,
                                        irs::doc_limits::min() + 399999}),
            ResolveDocs(*MakeUnplacedRemoval(spread), {}, reader[0]));
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
  EXPECT_TRUE(ResolveDocs(*MakeUnplacedRemoval(rowids), {}, reader[0]).empty());

  const std::vector<int64_t> victims{1, 2, 3};
  auto trx = _writer->GetBatch();
  trx.Remove(MakeUnplacedRemoval(victims));
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
  const auto removal = MakeUnplacedRemoval(rowids);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{removal.get()};
  resolver.Prepare(removals, {});
  size_t hits = 0;
  for (const auto& segment : reader) {
    hits += DocsOf(resolver, *removal, segment).size();
  }
  EXPECT_EQ(2, hits);
}

struct RunRow {
  int64_t rowid;
  uint64_t position;

  bool operator==(const RunRow&) const = default;
};

std::vector<RunRow> ExpandRuns(const duckdb::WALDeleteRowRanges& runs) {
  std::vector<RunRow> rows;
  for (size_t i = 0; i < runs.starts.size(); ++i) {
    for (uint64_t row = 0; row < runs.counts[i]; ++row) {
      rows.push_back({runs.starts[i] + static_cast<int64_t>(row),
                      runs.positions[i] == kNoRowPosition
                        ? kNoRowPosition
                        : runs.positions[i] + row});
    }
  }
  return rows;
}

TEST(RowRunsTest, JoinConsecutiveRowsAtConsecutivePositions) {
  const std::array<int64_t, 8> rowids{5, 6, 7, 9, 10, 20, 21, 23};
  const std::array<uint64_t, 8> positions{
    MakeRowPosition(1, 1), MakeRowPosition(1, 2), MakeRowPosition(1, 3),
    MakeRowPosition(1, 4), MakeRowPosition(2, 1), kNoRowPosition,
    kNoRowPosition,        kNoRowPosition};
  const auto runs = RunsOf(rowids, positions);
  EXPECT_EQ((std::vector<int64_t>{5, 9, 10, 20, 23}), runs.starts);
  EXPECT_EQ((std::vector<uint64_t>{3, 1, 1, 2, 1}), runs.counts);
  EXPECT_EQ((std::vector<uint64_t>{MakeRowPosition(1, 1), MakeRowPosition(1, 4),
                                   MakeRowPosition(2, 1), kNoRowPosition,
                                   kNoRowPosition}),
            runs.positions);
}

TEST(RowRunsTest, StartANewRunOnARowidGap) {
  const std::array<int64_t, 2> rowids{1, 3};
  const std::array<uint64_t, 2> positions{MakeRowPosition(1, 1),
                                          MakeRowPosition(1, 2)};
  const auto runs = RunsOf(rowids, positions);
  EXPECT_EQ((std::vector<int64_t>{1, 3}), runs.starts);
  EXPECT_EQ((std::vector<uint64_t>{1, 1}), runs.counts);
}

TEST(RowRunsTest, StartANewRunOnAPositionGap) {
  const std::array<int64_t, 2> rowids{1, 2};
  const std::array<uint64_t, 2> positions{MakeRowPosition(1, 1),
                                          MakeRowPosition(1, 3)};
  const auto runs = RunsOf(rowids, positions);
  EXPECT_EQ((std::vector<uint64_t>{1, 1}), runs.counts);
  EXPECT_EQ(
    (std::vector<uint64_t>{MakeRowPosition(1, 1), MakeRowPosition(1, 3)}),
    runs.positions);
}

TEST(RowRunsTest, StartANewRunInAnotherSegment) {
  const std::array<int64_t, 2> rowids{1, 2};
  const std::array<uint64_t, 2> positions{MakeRowPosition(1, 5),
                                          MakeRowPosition(2, 6)};
  const auto runs = RunsOf(rowids, positions);
  EXPECT_EQ((std::vector<uint64_t>{1, 1}), runs.counts);
}

TEST(RowRunsTest, KeepPlacedAndUnplacedRowsApart) {
  const std::array<int64_t, 4> rowids{1, 2, 3, 4};
  const std::array<uint64_t, 4> positions{MakeRowPosition(1, 1), kNoRowPosition,
                                          MakeRowPosition(1, 3),
                                          MakeRowPosition(1, 4)};
  const auto runs = RunsOf(rowids, positions);
  EXPECT_EQ((std::vector<int64_t>{1, 2, 3}), runs.starts);
  EXPECT_EQ((std::vector<uint64_t>{1, 1, 2}), runs.counts);
  EXPECT_EQ((std::vector<uint64_t>{MakeRowPosition(1, 1), kNoRowPosition,
                                   MakeRowPosition(1, 3)}),
            runs.positions);
}

TEST(RowRunsTest, JoinConsecutiveUnplacedRowids) {
  const std::array<int64_t, 3> rowids{7, 8, 9};
  const std::array<uint64_t, 3> positions{kNoRowPosition, kNoRowPosition,
                                          kNoRowPosition};
  const auto runs = RunsOf(rowids, positions);
  EXPECT_EQ((std::vector<int64_t>{7}), runs.starts);
  EXPECT_EQ((std::vector<uint64_t>{3}), runs.counts);
  EXPECT_EQ((std::vector<uint64_t>{kNoRowPosition}), runs.positions);
}

TEST(RowRunsTest, JoinALongRun) {
  std::vector<int64_t> rowids(10000);
  std::vector<uint64_t> positions(rowids.size());
  for (size_t i = 0; i < rowids.size(); ++i) {
    rowids[i] = 50000 + static_cast<int64_t>(i);
    positions[i] = MakeRowPosition(3, static_cast<irs::doc_id_t>(1 + i));
  }
  const auto runs = RunsOf(rowids, positions);
  EXPECT_EQ((std::vector<int64_t>{50000}), runs.starts);
  EXPECT_EQ((std::vector<uint64_t>{10000}), runs.counts);
  EXPECT_EQ((std::vector<uint64_t>{MakeRowPosition(3, 1)}), runs.positions);
}

TEST(RowRunsTest, KeepDuplicatesAndDescendingRowsApart) {
  const std::array<int64_t, 3> rowids{5, 5, 4};
  const std::array<uint64_t, 3> positions{
    MakeRowPosition(1, 5), MakeRowPosition(1, 5), MakeRowPosition(1, 4)};
  const auto runs = RunsOf(rowids, positions);
  EXPECT_EQ((std::vector<int64_t>{5, 5, 4}), runs.starts);
  EXPECT_EQ((std::vector<uint64_t>{1, 1, 1}), runs.counts);
}

TEST(RowRunsTest, ExpandBackToTheRowsInOrder) {
  std::mt19937_64 rng{11};
  for (int round = 0; round < 200; ++round) {
    std::vector<int64_t> rowids;
    std::vector<uint64_t> positions;
    std::vector<RunRow> expected;
    const auto streaks = rng() % 50;
    for (uint64_t streak = 0; streak < streaks; ++streak) {
      const auto rowid = static_cast<int64_t>(rng() % 100000);
      const bool placed = rng() % 3 != 0;
      const auto position =
        placed ? MakeRowPosition(static_cast<uint32_t>(rng() % 4),
                                 static_cast<irs::doc_id_t>(1 + rng() % 1000))
               : kNoRowPosition;
      const auto length = 1 + rng() % 20;
      for (uint64_t row = 0; row < length; ++row) {
        rowids.push_back(rowid + static_cast<int64_t>(row));
        positions.push_back(placed ? position + row : kNoRowPosition);
        expected.push_back({rowids.back(), positions.back()});
      }
    }
    const auto runs = RunsOf(rowids, positions);
    ASSERT_EQ(runs.starts.size(), runs.counts.size());
    ASSERT_EQ(runs.starts.size(), runs.positions.size());
    ASSERT_LE(runs.starts.size(), static_cast<size_t>(streaks));
    ASSERT_EQ(expected, ExpandRuns(runs)) << round;
  }
}

TEST(RowRunRemovalTest, RunsExpandToRowsOrStayRangesWithoutPositions) {
  EXPECT_EQ(nullptr, MakeRowRunRemoval({}, {}, {}));
  const std::array<int64_t, 2> starts{100, 200};
  const std::array<uint64_t, 2> counts{3, 2};
  const std::array<uint64_t, 2> positions{MakeRowPosition(4, 7),
                                          kNoRowPosition};
  const auto removal = MakeRowRunRemoval(starts, counts, positions);
  ASSERT_NE(nullptr, removal);
  EXPECT_EQ((std::vector<uint32_t>{4}), removal->segments);
  EXPECT_EQ((std::vector<irs::doc_id_t>{7, 8, 9}), removal->docs);
  EXPECT_EQ((std::vector<int64_t>{100, 101, 102}), removal->keys);
  EXPECT_EQ((std::vector<std::pair<int64_t, int64_t>>{{200, 202}}),
            Pairs(removal->ranges));
  const auto unpositioned = MakeRowRunRemoval(starts, counts, {});
  ASSERT_NE(nullptr, unpositioned);
  EXPECT_TRUE(unpositioned->segments.empty());
  EXPECT_EQ((std::vector<std::pair<int64_t, int64_t>>{{100, 103}, {200, 202}}),
            Pairs(unpositioned->ranges));
}

TEST(RowRunRemovalTest, RejectsMismatchedRuns) {
  const std::array<int64_t, 2> starts{1, 5};
  const std::array<uint64_t, 1> one_count{1};
  const std::array<uint64_t, 2> counts{1, 2};
  const std::array<uint64_t, 1> one_position{MakeRowPosition(1, 1)};
  EXPECT_DEATH(MakeRowRunRemoval(starts, one_count, {}),
               "2 removed row runs with 1 counts and 0 positions");
  EXPECT_DEATH(MakeRowRunRemoval(starts, counts, one_position),
               "2 removed row runs with 2 counts and 1 positions");
}

TEST(RowRunRemovalTest, UnplacedRunsOnlyHaveNoGroups) {
  const std::array<int64_t, 3> starts{30, 10, 20};
  const std::array<uint64_t, 3> counts{1, 5, 2};
  const std::array<uint64_t, 3> positions{kNoRowPosition, kNoRowPosition,
                                          kNoRowPosition};
  const auto removal = MakeRowRunRemoval(starts, counts, positions);
  ASSERT_NE(nullptr, removal);
  EXPECT_EQ(kGeneratedPKId, removal->key_column);
  EXPECT_TRUE(removal->segments.empty());
  EXPECT_TRUE(removal->docs.empty());
  EXPECT_EQ(
    (std::vector<std::pair<int64_t, int64_t>>{{30, 31}, {10, 15}, {20, 22}}),
    Pairs(removal->ranges));
}

TEST(RowRunRemovalTest, PlacedRunsGroupBySegment) {
  const std::array<int64_t, 3> starts{100, 10, 300};
  const std::array<uint64_t, 3> counts{2, 3, 1};
  const std::array<uint64_t, 3> positions{
    MakeRowPosition(2, 5), MakeRowPosition(1, 1), MakeRowPosition(2, 40)};
  const auto removal = MakeRowRunRemoval(starts, counts, positions);
  ASSERT_NE(nullptr, removal);
  EXPECT_EQ((std::vector<uint32_t>{1, 2}), removal->segments);
  EXPECT_EQ((std::vector<irs::doc_id_t>{1, 2, 3}),
            std::vector<irs::doc_id_t>(removal->Docs(0).begin(),
                                       removal->Docs(0).end()));
  EXPECT_EQ(
    (std::vector<int64_t>{10, 11, 12}),
    std::vector<int64_t>(removal->Keys(0).begin(), removal->Keys(0).end()));
  EXPECT_EQ((std::vector<irs::doc_id_t>{5, 6, 40}),
            std::vector<irs::doc_id_t>(removal->Docs(1).begin(),
                                       removal->Docs(1).end()));
  EXPECT_EQ(
    (std::vector<int64_t>{100, 101, 300}),
    std::vector<int64_t>(removal->Keys(1).begin(), removal->Keys(1).end()));
  EXPECT_TRUE(removal->ranges.empty());
}

TEST(RowRunRemovalTest, MatchesAModelOnRandomRows) {
  std::mt19937_64 rng{23};
  for (int round = 0; round < 100; ++round) {
    std::vector<int64_t> rowids(rng() % 3000);
    std::vector<uint64_t> positions(rowids.size());
    std::map<uint32_t, std::vector<std::pair<irs::doc_id_t, int64_t>>> placed;
    std::vector<int64_t> unplaced;
    for (size_t i = 0; i < rowids.size(); ++i) {
      rowids[i] = static_cast<int64_t>(i / 3 + rng() % 2);
      if (rng() % 4 == 0) {
        positions[i] = kNoRowPosition;
        unplaced.push_back(rowids[i]);
        continue;
      }
      const auto segment = static_cast<uint32_t>(rng() % 5);
      const auto doc = static_cast<irs::doc_id_t>(1 + rng() % 10000);
      positions[i] = MakeRowPosition(segment, doc);
      placed[segment].emplace_back(doc, rowids[i]);
    }
    const auto removal = MakeRowRemoval(rowids, positions);
    if (rowids.empty()) {
      ASSERT_EQ(nullptr, removal);
      continue;
    }
    ASSERT_NE(nullptr, removal);
    ASSERT_EQ(placed.size(), removal->segments.size());
    size_t group = 0;
    for (const auto& [segment, expected] : placed) {
      ASSERT_EQ(segment, removal->segments[group]);
      const auto docs = removal->Docs(group);
      const auto keys = removal->Keys(group);
      ASSERT_EQ(expected.size(), docs.size());
      for (size_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(expected[i].first, docs[i]);
        ASSERT_EQ(expected[i].second, keys[i]);
      }
      ++group;
    }
    std::vector<int64_t> covered;
    for (const auto& range : removal->ranges) {
      for (auto key = range.begin; key != range.end; ++key) {
        covered.push_back(key);
      }
    }
    ASSERT_EQ(unplaced, covered) << round;
  }
}

TEST(RowRangeRemovalTest, NoRangesMakeNoRemoval) {
  EXPECT_EQ(nullptr, MakeRowRangeRemoval({}));
}

TEST(RowRangeRemovalTest, KeepsTheRangesAsGiven) {
  const auto removal = MakeRowRangeRemoval({{5, 9}, {1, 2}, {5, 6}});
  ASSERT_NE(nullptr, removal);
  EXPECT_EQ(kGeneratedPKId, removal->key_column);
  EXPECT_TRUE(removal->segments.empty());
  EXPECT_EQ((std::vector<std::pair<int64_t, int64_t>>{{5, 9}, {1, 2}, {5, 6}}),
            Pairs(removal->ranges));
}

TEST(WalDeleteEntryTest, RoundTripsRunsWithPositions) {
  duckdb::WALDeleteRowRanges entry;
  entry.starts = {5, 100, 7};
  entry.counts = {3, 1, 2};
  entry.positions = {MakeRowPosition(4, 9), kNoRowPosition,
                     MakeRowPosition(1, 2)};
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer::Serialize(entry, stream);
  stream.Rewind();
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Begin();
  const auto back = duckdb::WALDeleteRowRanges::Deserialize(deserializer);
  deserializer.End();
  EXPECT_EQ(entry.starts, back.starts);
  EXPECT_EQ(entry.counts, back.counts);
  EXPECT_EQ(entry.positions, back.positions);
}

TEST(WalDeleteEntryTest, EntryWithoutPositionsReadsWithNone) {
  duckdb::WALDeleteRowRanges entry;
  entry.starts = {5};
  entry.counts = {3};
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer::Serialize(entry, stream);
  stream.Rewind();
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Begin();
  const auto back = duckdb::WALDeleteRowRanges::Deserialize(deserializer);
  deserializer.End();
  EXPECT_EQ(entry.starts, back.starts);
  EXPECT_EQ(entry.counts, back.counts);
  EXPECT_TRUE(back.positions.empty());
  const auto removal =
    MakeRowRunRemoval(back.starts, back.counts, back.positions);
  ASSERT_NE(nullptr, removal);
  EXPECT_TRUE(removal->segments.empty());
  EXPECT_EQ((std::vector<std::pair<int64_t, int64_t>>{{5, 8}}),
            Pairs(removal->ranges));
}

TEST_F(SearchRowRemovalTest, RowRunRemovalGroupsRowsBySegment) {
  EXPECT_EQ(nullptr, MakeRowRemoval({}, {}));
  const std::array<int64_t, 4> rowids{5, 6, 3, 3};
  const std::array<uint64_t, 4> positions{MakeRowPosition(2, 7),
                                          MakeRowPosition(1, 3), kNoRowPosition,
                                          kNoRowPosition};
  const auto removal = MakeRowRemoval(rowids, positions);
  ASSERT_NE(nullptr, removal);
  EXPECT_EQ(kGeneratedPKId, removal->key_column);
  EXPECT_EQ((std::vector<uint32_t>{1, 2}), removal->segments);
  EXPECT_EQ((std::vector<irs::doc_id_t>{3, 7}), removal->docs);
  EXPECT_EQ((std::vector<int64_t>{6, 5}), removal->keys);
  EXPECT_EQ((std::vector<std::pair<int64_t, int64_t>>{{3, 4}, {3, 4}}),
            Pairs(removal->ranges));
  const auto unplaced = MakeUnplacedRemoval(rowids);
  ASSERT_NE(nullptr, unplaced);
  EXPECT_TRUE(unplaced->segments.empty());
  EXPECT_TRUE(unplaced->keys.empty());
  EXPECT_EQ((std::vector<std::pair<int64_t, int64_t>>{{5, 7}, {3, 4}, {3, 4}}),
            Pairs(unplaced->ranges));
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
    7, 4,
    [](duckdb::idx_t i) { return static_cast<irs::doc_id_t>(10 + 2 * i); },
    out);
  EXPECT_EQ(4, out.size());
  const auto* data = duckdb::FlatVector::GetData<uint64_t>(out);
  for (duckdb::idx_t i = 0; i < 4; ++i) {
    EXPECT_TRUE(duckdb::FlatVector::Validity(out).RowIsValid(i));
    EXPECT_EQ(MakeRowPosition(7, static_cast<irs::doc_id_t>(10 + 2 * i)),
              data[i]);
  }
  duckdb::Vector null_out{duckdb::LogicalType::UBIGINT, 4};
  FillRowPositions(
    irs::DocRemoval::kNoSegment, 3,
    [](duckdb::idx_t) { return irs::doc_id_t{1}; }, null_out);
  EXPECT_EQ(duckdb::VectorType::CONSTANT_VECTOR, null_out.GetVectorType());
  EXPECT_TRUE(duckdb::ConstantVector::IsNull(null_out));
  EXPECT_EQ(3, null_out.size());
}

TEST(RowPositionTest, FillRowPositionsOfHighSegments) {
  for (const uint32_t segment :
       {uint32_t{0}, uint32_t{1} << 31, uint32_t{3000000000},
        irs::DocRemoval::kNoSegment - 1}) {
    duckdb::Vector out{duckdb::LogicalType::UBIGINT, 3};
    FillRowPositions(
      segment, 3,
      [](duckdb::idx_t i) {
        return std::numeric_limits<irs::doc_id_t>::max() - 2 +
               static_cast<irs::doc_id_t>(i);
      },
      out);
    for (duckdb::idx_t i = 0; i < 3; ++i) {
      const auto position = out.GetValue(i).GetValue<uint64_t>();
      EXPECT_EQ(segment, RowPositionSegment(position)) << segment << " " << i;
      EXPECT_EQ(std::numeric_limits<irs::doc_id_t>::max() - 2 + i,
                RowPositionDoc(position))
        << segment << " " << i;
      EXPECT_NE(kNoRowPosition, position);
    }
  }
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
  _writer->ReplaceSegments(
    replaced, adopted, [](irs::IndexWriter::QueryContext&) { return true; });
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
  const auto removal = MakeUnplacedRemoval(victims);
  std::vector<std::string_view> replaced{sources.begin(), sources.end()};
  std::vector<std::string_view> adopted{metas.begin(), metas.end()};
  _writer->ReplaceSegments(replaced, adopted,
                           [&](irs::IndexWriter::QueryContext& out) {
                             out = {removal, irs::writer_limits::kMinTick};
                             return true;
                           });
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
  const auto removal = MakeUnplacedRemoval(victims);
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
  const auto removal = MakeUnplacedRemoval(victims);
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
    const auto removal = k % 2 == 0
                           ? MakeRowRemoval(victims, PositionsOf(victims))
                           : MakeUnplacedRemoval(victims);
    trx.Remove(removal);
    for (const auto rowid : victims) {
      _model.erase(rowid);
    }
  }
  ASSERT_TRUE(trx.Commit());
  _writer->RefreshCommit();
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, PositionsInDescendingOrderRemoveTheirRows) {
  InsertRange(0, 3000);
  InsertRange(3000, 3000);
  std::vector<int64_t> victims;
  for (int64_t r = 5999; r >= 0; r -= 7) {
    victims.push_back(r);
  }
  Remove(victims, PositionsOf(victims));
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, PositionsInRandomOrderRemoveTheirRows) {
  InsertRange(0, 4000);
  InsertRange(4000, 4000);
  InsertRange(8000, 4000);
  std::vector<int64_t> victims;
  for (int64_t r = 0; r < 12000; r += 3) {
    victims.push_back(r);
  }
  std::mt19937_64 rng{5};
  std::shuffle(victims.begin(), victims.end(), rng);
  Remove(victims, PositionsOf(victims));
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, LongRunsOfPositionsRemoveTheirRows) {
  InsertRange(0, 10000);
  InsertRange(10000, 10000);
  std::vector<int64_t> victims(9000);
  std::iota(victims.begin(), victims.end(), 5500);
  const auto positions = PositionsOf(victims);
  EXPECT_EQ(2, RunsOf(victims, positions).starts.size());
  Remove(victims, positions);
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, ValidAndPastTheEndPositionsInOneRemoval) {
  InsertRange(0, 100);
  const auto live = LivePositions();
  const auto segment = RowPositionSegment(live.at(0));
  const std::vector<int64_t> keys{3, 99999, 50};
  const std::vector<uint64_t> positions{
    live.at(3), MakeRowPosition(segment, irs::doc_limits::min() + 1000),
    live.at(50)};
  Remove(keys, positions);
  ExpectModel();
  EXPECT_EQ(98, Live().size());
}

TEST_F(SearchRowRemovalTest,
       PositionPastTheEndDuringAPendingCompactionIsIgnored) {
  InsertRange(0, 2000);
  InsertRange(2000, 2000);
  const auto live = LivePositions();
  const auto segment = RowPositionSegment(live.at(0));
  const std::vector<int64_t> keys{10, 99999, 2010};
  const std::vector<uint64_t> positions{
    live.at(10), MakeRowPosition(segment, irs::doc_limits::min() + 5000),
    live.at(2010)};
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  Remove(keys, positions);
  ASSERT_EQ(1, _writer->GetSnapshot().size());
  ExpectModel();
  EXPECT_EQ(3998, Live().size());
}

TEST_F(SearchRowRemovalTest, RemovingEveryRowEmptiesTheIndex) {
  InsertRange(0, 3000);
  InsertRange(3000, 3000);
  std::vector<int64_t> victims(6000);
  std::iota(victims.begin(), victims.end(), 0);
  Remove(victims, PositionsOf(victims));
  EXPECT_EQ(0, _writer->GetSnapshot().size());
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, ByKeyDocsMatchAModel) {
  constexpr int64_t kRows = 70000;
  std::vector<int64_t> ascending(kRows);
  std::vector<int64_t> descending(kRows);
  std::vector<int64_t> shuffled(kRows);
  std::iota(ascending.begin(), ascending.end(), 0);
  for (int64_t i = 0; i < kRows; ++i) {
    descending[i] = 2 * kRows - 1 - i;
  }
  std::iota(shuffled.begin(), shuffled.end(), 2 * kRows);
  std::mt19937_64 rng{17};
  std::shuffle(shuffled.begin(), shuffled.end(), rng);
  Insert(ascending);
  Insert(descending);
  Insert(shuffled);
  const auto reader = _writer->GetSnapshot();
  ASSERT_EQ(3, reader.size());
  std::vector<std::vector<int64_t>> rowids;
  for (const auto& segment : reader) {
    rowids.push_back(ReadRowids(segment));
  }
  constexpr int64_t kOffset = 10000;
  constexpr int64_t kDomain = 3 * kRows + 2 * kOffset + 30000;
  for (int round = 0; round < 60; ++round) {
    std::vector<irs::KeyRange> ranges;
    const auto kind = round % 4;
    const auto count = 1 + rng() % 200;
    for (uint64_t i = 0; i < count; ++i) {
      const auto begin = static_cast<int64_t>(rng() % (3 * kRows)) - kOffset;
      const auto width = kind == 0   ? uint64_t{1}
                         : kind == 1 ? 1 + rng() % 50
                         : kind == 2 ? 1 + rng() % 20000
                                     : 1 + rng() % 3;
      ranges.push_back({begin, begin + static_cast<int64_t>(width)});
    }
    if (round == 0) {
      ranges = {{-5, 3 * kRows + 5}};
    } else if (round == 1) {
      ranges = {{5 * kRows, 5 * kRows + 1}};
    }
    std::vector<bool> marked(kDomain);
    for (const auto& range : ranges) {
      for (auto key = std::max(range.begin, -kOffset);
           key < std::min(range.end, kDomain - kOffset); ++key) {
        marked[key + kOffset] = true;
      }
    }
    const auto removal = MakeRowRangeRemoval(ranges);
    irs::DocRemovalResolver resolver;
    const std::array<const irs::DocRemoval*, 1> removals{removal.get()};
    resolver.Prepare(removals, {});
    ASSERT_TRUE(resolver.ByKey(*removal));
    for (size_t s = 0; s < reader.size(); ++s) {
      std::vector<irs::doc_id_t> expected;
      for (size_t row = 0; row < rowids[s].size(); ++row) {
        if (marked[rowids[s][row] + kOffset]) {
          expected.push_back(static_cast<irs::doc_id_t>(row) +
                             irs::doc_limits::min());
        }
      }
      std::vector<irs::doc_id_t> actual;
      for (const auto& range : resolver.ByKeyDocs(reader[s])) {
        ASSERT_LT(range.begin, range.end);
        for (auto doc = range.begin; doc != range.end; ++doc) {
          actual.push_back(doc);
        }
      }
      ASSERT_EQ(expected, actual) << "round " << round << " segment " << s;
    }
  }
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
  absl::Mutex mutex;
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
