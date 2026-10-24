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
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "connector/column_id.h"
#include "connector/row_position.h"
#include "connector/search_sink_writer.hpp"
#include "gtest/gtest.h"

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

std::optional<irs::DocRemoval> MakeRemoval(
  std::span<const uint64_t> positions,
  std::shared_ptr<const irs::DocMapList> maps = nullptr) {
  irs::DocRemovalBuilder builder;
  for (const auto position : positions) {
    builder.Add(RowPositionSegment(position), RowPositionDoc(position));
  }
  if (builder.Empty()) {
    return std::nullopt;
  }
  auto removal = std::move(builder).Finish();
  removal.maps = std::move(maps);
  return removal;
}

std::optional<irs::DocRemoval> ReplayedRemoval(
  std::span<const uint64_t> positions,
  std::shared_ptr<const irs::DocMapList> maps = nullptr) {
  const auto removal = MakeRemoval(positions);
  if (!removal) {
    return std::nullopt;
  }
  auto replayed = irs::ReadDocRemoval(irs::WriteDocRemoval(*removal));
  replayed.maps = std::move(maps);
  return replayed;
}

std::vector<uint64_t> RemovedPositions(const irs::DocRemoval& removal) {
  std::vector<uint64_t> positions;
  for (size_t group = 0; group < removal.segments.size(); ++group) {
    removal.Docs(group).ForEach([&](irs::doc_id_t doc) {
      positions.push_back(MakeRowPosition(removal.segments[group], doc));
    });
  }
  return positions;
}

const irs::CompactionPolicy& FullMerge() {
  static const auto kPolicy = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});
  return kPolicy;
}

class SearchRowRemovalTest : public ::testing::Test {
 protected:
  void SetUp() override { Open(irs::kOmCreate); }

  void TearDown() override { _writer.reset(); }

  void Open(irs::OpenMode mode) {
    irs::IndexWriterOptions options;
    options.db = &TestDb();
    options.reader_options.db = &TestDb();
    _writer = irs::IndexWriter::Make(_dir, mode, std::move(options));
  }

  void Reopen() {
    _writer.reset();
    Open(irs::kOmAppend);
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

  static std::map<int64_t, uint64_t> PositionsIn(
    const irs::DirectoryReader& reader) {
    std::map<int64_t, uint64_t> out;
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

  std::map<int64_t, uint64_t> LivePositions() const {
    return PositionsIn(_writer->GetSnapshot());
  }

  std::set<int64_t> Live() const {
    std::set<int64_t> out;
    for (const auto& [rowid, position] : LivePositions()) {
      out.insert(rowid);
    }
    return out;
  }

  static std::vector<uint64_t> PositionsOf(const irs::DirectoryReader& reader,
                                           std::span<const int64_t> rowids) {
    const auto positions = PositionsIn(reader);
    std::vector<uint64_t> out;
    for (const auto rowid : rowids) {
      const auto it = positions.find(rowid);
      EXPECT_NE(positions.end(), it) << "rowid " << rowid << " is not alive";
      if (it != positions.end()) {
        out.push_back(it->second);
      }
    }
    return out;
  }

  std::vector<uint64_t> PositionsOf(std::span<const int64_t> rowids) const {
    return PositionsOf(_writer->GetSnapshot(), rowids);
  }

  void Commit(std::optional<irs::DocRemoval> removal,
              std::span<const int64_t> victims) {
    auto trx = _writer->GetBatch();
    if (removal) {
      trx.Remove(std::move(*removal));
    }
    ASSERT_TRUE(trx.Commit());
    _writer->RefreshCommit();
    for (const auto rowid : victims) {
      _model.erase(rowid);
    }
  }

  void Remove(std::span<const uint64_t> positions,
              std::span<const int64_t> victims,
              std::shared_ptr<const irs::DocMapList> maps = nullptr) {
    Commit(MakeRemoval(positions, std::move(maps)), victims);
  }

  void RemoveReplayed(std::span<const uint64_t> positions,
                      std::span<const int64_t> victims,
                      std::shared_ptr<const irs::DocMapList> maps = nullptr) {
    Commit(ReplayedRemoval(positions, std::move(maps)), victims);
  }

  void Remove(std::span<const int64_t> victims) {
    Remove(PositionsOf(victims), victims);
  }

  std::vector<std::string> FlushCopy(std::span<const int64_t> rowids) {
    auto build = _writer->GetBatch(/*exclusive_segment=*/true);
    WriteRows(build, rowids);
    std::vector<std::string> metas;
    for (const auto& flushed : build.FlushAndFsync()) {
      metas.emplace_back(flushed.filename);
    }
    build.Abort();
    return metas;
  }

  void ReplaceAll(const irs::DirectoryReader& snapshot,
                  const std::vector<std::string>& metas) {
    std::vector<std::string_view> replaced;
    std::vector<irs::DocMap> absorbed;
    irs::doc_id_t base = irs::doc_limits::min();
    for (const auto& segment : snapshot.Meta().index_meta.segments) {
      const auto live =
        static_cast<irs::doc_id_t>(segment.meta.live_docs_count);
      replaced.emplace_back(segment.meta.name);
      absorbed.push_back({.source = irs::SegmentNumber(segment.meta.name),
                          .mask = segment.meta.docs_mask,
                          .rank_begin = 0,
                          .rank_end = live,
                          .base = base});
      base += live;
    }
    ASSERT_EQ(1, metas.size());
    std::array<irs::IndexWriter::Replacement, 1> replacements{
      {{.meta_file = metas.front(), .absorbed = std::move(absorbed)}}};
    _writer->ReplaceSegments(replaced, replacements);
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
    }
  }
}

TEST(RowPositionTest, PositionsOrderBySegmentThenDoc) {
  EXPECT_LT(MakeRowPosition(1, 5), MakeRowPosition(1, 6));
  EXPECT_LT(MakeRowPosition(1, std::numeric_limits<irs::doc_id_t>::max() - 1),
            MakeRowPosition(2, irs::doc_limits::min()));
}

TEST(RowPositionTest, FillRowPositions) {
  duckdb::Vector out{duckdb::LogicalType::ROW_TYPE, 4};
  FillRowPositions(
    7, 4,
    [](duckdb::idx_t i) { return static_cast<irs::doc_id_t>(10 + 2 * i); },
    out);
  EXPECT_EQ(4, out.size());
  const auto* data = duckdb::FlatVector::GetData<int64_t>(out);
  for (duckdb::idx_t i = 0; i < 4; ++i) {
    EXPECT_TRUE(duckdb::FlatVector::Validity(out).RowIsValid(i));
    EXPECT_EQ(MakeRowPosition(7, static_cast<irs::doc_id_t>(10 + 2 * i)),
              static_cast<uint64_t>(data[i]));
  }
  duckdb::Vector null_out{duckdb::LogicalType::ROW_TYPE, 4};
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
    duckdb::Vector out{duckdb::LogicalType::ROW_TYPE, 3};
    FillRowPositions(
      segment, 3,
      [](duckdb::idx_t i) {
        return std::numeric_limits<irs::doc_id_t>::max() - 2 +
               static_cast<irs::doc_id_t>(i);
      },
      out);
    for (duckdb::idx_t i = 0; i < 3; ++i) {
      const auto position =
        static_cast<uint64_t>(out.GetValue(i).GetValue<int64_t>());
      EXPECT_EQ(segment, RowPositionSegment(position)) << segment << " " << i;
      EXPECT_EQ(std::numeric_limits<irs::doc_id_t>::max() - 2 + i,
                RowPositionDoc(position))
        << segment << " " << i;
    }
  }
}

TEST(RowRemovalTest, PositionsGroupBySegmentSortedOnce) {
  const std::array<uint64_t, 7> positions{
    MakeRowPosition(2, 5), MakeRowPosition(1, 3), MakeRowPosition(2, 40),
    MakeRowPosition(1, 1), MakeRowPosition(2, 5), MakeRowPosition(1, 2),
    MakeRowPosition(2, 6)};
  const auto removal = MakeRemoval(positions);
  ASSERT_TRUE(removal);
  EXPECT_FALSE(removal->all);
  EXPECT_EQ((std::vector<uint32_t>{1, 2}), removal->segments);
  EXPECT_EQ(
    (std::vector<uint64_t>{MakeRowPosition(1, 1), MakeRowPosition(1, 2),
                           MakeRowPosition(1, 3), MakeRowPosition(2, 5),
                           MakeRowPosition(2, 6), MakeRowPosition(2, 40)}),
    RemovedPositions(*removal));
}

TEST(RowRemovalTest, NoPositionsMakeNoRemoval) {
  EXPECT_FALSE(MakeRemoval({}));
}

TEST(RowRemovalTest, TheLastDocOfASegmentAndTheFirstOfTheNextStayApart) {
  const auto last = irs::doc_limits::eof() - 1;
  const std::array<uint64_t, 2> positions{
    MakeRowPosition(1, last), MakeRowPosition(2, irs::doc_limits::min())};
  const auto removal = ReplayedRemoval(positions);
  ASSERT_TRUE(removal);
  EXPECT_EQ((std::vector<uint32_t>{1, 2}), removal->segments);
  EXPECT_EQ(std::vector<uint64_t>(positions.begin(), positions.end()),
            RemovedPositions(*removal));
}

TEST(RowRemovalTest, ARunOfAMillionRowsTakesAFewWalBytes) {
  std::vector<uint64_t> positions(1000000);
  for (size_t i = 0; i < positions.size(); ++i) {
    positions[i] = MakeRowPosition(3, static_cast<irs::doc_id_t>(1 + i));
  }
  const auto removal = MakeRemoval(positions);
  ASSERT_TRUE(removal);
  EXPECT_GT(1024, irs::WriteDocRemoval(*removal).size());
}

TEST(RowRemovalTest, ScatteredRowsTakeAtMostAboutTwoWalBytesEach) {
  std::mt19937_64 rng{3};
  std::vector<uint64_t> positions(100000);
  for (auto& position : positions) {
    position = MakeRowPosition(static_cast<uint32_t>(rng() % 4),
                               static_cast<irs::doc_id_t>(1 + rng() % 2000000));
  }
  const auto removal = MakeRemoval(positions);
  ASSERT_TRUE(removal);
  EXPECT_GT(2 * positions.size() + 4096, irs::WriteDocRemoval(*removal).size());
}

TEST(RowRemovalTest, WalBytesGiveBackTheSortedPositions) {
  std::mt19937_64 rng{11};
  for (int round = 0; round < 200; ++round) {
    std::vector<uint64_t> positions;
    const auto streaks = rng() % 50;
    for (uint64_t streak = 0; streak < streaks; ++streak) {
      const auto position =
        MakeRowPosition(static_cast<uint32_t>(rng() % 4),
                        static_cast<irs::doc_id_t>(1 + rng() % 100000));
      const auto length = 1 + rng() % 20;
      for (uint64_t row = 0; row < length; ++row) {
        positions.push_back(position + row);
      }
    }
    std::shuffle(positions.begin(), positions.end(), rng);
    const auto removal = ReplayedRemoval(positions);
    if (positions.empty()) {
      ASSERT_FALSE(removal);
      continue;
    }
    ASSERT_TRUE(removal);
    std::sort(positions.begin(), positions.end());
    positions.erase(std::unique(positions.begin(), positions.end()),
                    positions.end());
    ASSERT_EQ(positions, RemovedPositions(*removal)) << round;
  }
}

TEST(WalDeleteEntryTest, RoundTripsThePositions) {
  const std::array<uint64_t, 4> positions{
    MakeRowPosition(4, 9), MakeRowPosition(0, 1), MakeRowPosition(1, 2),
    MakeRowPosition(4, 10)};
  const duckdb::WALDeleteRowPositions entry{
    irs::WriteDocRemoval(*MakeRemoval(positions))};
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer::Serialize(entry, stream);
  stream.Rewind();
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Begin();
  const auto back = duckdb::WALDeleteRowPositions::Deserialize(deserializer);
  deserializer.End();
  EXPECT_EQ(entry.positions, back.positions);
  EXPECT_EQ(
    (std::vector<uint64_t>{MakeRowPosition(0, 1), MakeRowPosition(1, 2),
                           MakeRowPosition(4, 9), MakeRowPosition(4, 10)}),
    RemovedPositions(irs::ReadDocRemoval(back.positions)));
}

TEST(WalRowPositionMapsTest, RoundTripsTheMaps) {
  const duckdb::WALRowPositionMaps entry{
    irs::WriteDocMaps({std::make_shared<const irs::DocMap>(
      irs::DocMap{.source = 3, .target = 9, .rank_end = 5, .base = 2})})};
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer::Serialize(entry, stream);
  stream.Rewind();
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Begin();
  const auto back = duckdb::WALRowPositionMaps::Deserialize(deserializer);
  deserializer.End();
  EXPECT_EQ(entry.maps, back.maps);
  const auto maps = irs::ReadDocMaps(back.maps);
  ASSERT_EQ(1, maps.size());
  EXPECT_EQ(3, maps.front()->source);
  EXPECT_EQ(9, maps.front()->target);
}

TEST_F(SearchRowRemovalTest, EmptyRemovalRemovesNothing) {
  InsertRange(0, 10);
  Remove({}, {});
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, PositionsRemoveExactlyTheirRows) {
  InsertRange(0, 1000);
  InsertRange(1000, 1000);
  InsertRange(2000, 1000);
  std::vector<int64_t> victims;
  for (int64_t r = 0; r < 3000; r += 7) {
    victims.push_back(r);
  }
  Remove(victims);
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, StalePositionOfACompactedSegmentIsRemapped) {
  InsertRange(0, 2000);
  InsertRange(2000, 2000);
  InsertRange(4000, 2000);
  std::vector<int64_t> victims;
  for (int64_t r = 5; r < 6000; r += 11) {
    victims.push_back(r);
  }
  const auto snapshot = _writer->GetSnapshot();
  const auto positions = PositionsOf(snapshot, victims);
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  _writer->RefreshCommit();
  ASSERT_EQ(1, _writer->GetSnapshot().size());
  Remove(positions, victims);
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, StalePositionsAreRemappedThroughTwoMerges) {
  InsertRange(0, 2000);
  InsertRange(2000, 2000);
  std::vector<int64_t> victims;
  for (int64_t r = 3; r < 4000; r += 13) {
    victims.push_back(r);
  }
  const auto snapshot = _writer->GetSnapshot();
  const auto positions = PositionsOf(snapshot, victims);
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  _writer->RefreshCommit();
  const std::vector<int64_t> early{1, 2};
  Remove(early);
  InsertRange(4000, 2000);
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  _writer->RefreshCommit();
  ASSERT_EQ(1, _writer->GetSnapshot().size());
  Remove(positions, victims);
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, StalePositionsAfterAReopenNeedTheMapsTheyCarry) {
  InsertRange(0, 3000);
  InsertRange(3000, 3000);
  const std::vector<int64_t> early{10, 3011};
  Remove(early);
  std::vector<int64_t> victims;
  for (int64_t r = 1; r < 6000; r += 17) {
    victims.push_back(r);
  }
  std::vector<uint64_t> positions;
  std::string record;
  {
    const auto snapshot = _writer->GetSnapshot();
    positions = PositionsOf(snapshot, victims);
    ASSERT_TRUE(_writer->Compact(FullMerge()));
    _writer->RefreshCommit();
    std::vector<uint32_t> segments;
    for (const auto& segment : snapshot) {
      segments.push_back(irs::SegmentNumber(segment.Meta().name));
    }
    record = irs::WriteDocMaps(_writer->CopyDocMaps(segments));
  }
  ASSERT_FALSE(record.empty());
  Reopen();
  ASSERT_EQ(1, _writer->GetSnapshot().size());
  RemoveReplayed(positions, {});
  ExpectModel();
  RemoveReplayed(
    positions, victims,
    std::make_shared<const irs::DocMapList>(irs::ReadDocMaps(record)));
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, RemovalDuringAPendingCompactionIsRemapped) {
  InsertRange(0, 3000);
  InsertRange(3000, 3000);
  std::vector<int64_t> victims;
  for (int64_t r = 1; r < 6000; r += 13) {
    victims.push_back(r);
  }
  const auto snapshot = _writer->GetSnapshot();
  const auto positions = PositionsOf(snapshot, victims);
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  Remove(positions, victims);
  ASSERT_EQ(1, _writer->GetSnapshot().size());
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, RemovalInFlightDuringAMergeIsRemapped) {
  InsertRange(0, 3000);
  InsertRange(3000, 3000);
  std::vector<int64_t> victims;
  for (int64_t r = 2; r < 6000; r += 17) {
    victims.emplace_back(r);
  }
  const auto positions = PositionsOf(victims);
  auto trx = _writer->GetBatch();
  trx.Remove(*MakeRemoval(positions));
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
  const std::vector<int64_t> victims{0, 2999, 3000, 5999, 1234};
  Remove(victims);
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  _writer->RefreshCommit();
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, SegmentFullyRemovedDisappears) {
  InsertRange(0, 100);
  InsertRange(100, 100);
  std::vector<int64_t> victims(100);
  std::iota(victims.begin(), victims.end(), 0);
  Remove(victims);
  EXPECT_EQ(1, _writer->GetSnapshot().size());
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, AlreadyRemovedRowsAreNoOps) {
  InsertRange(0, 500);
  const std::vector<int64_t> victims{1, 2, 3};
  const auto positions = PositionsOf(victims);
  Remove(positions, victims);
  Remove(positions, victims);
  ExpectModel();
  EXPECT_EQ(497, Live().size());
}

TEST_F(SearchRowRemovalTest, DuplicatesInOneRemovalAreHarmless) {
  InsertRange(0, 500);
  const std::vector<int64_t> victims{9, 9, 9, 10, 10};
  Remove(victims);
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, PositionPastTheSegmentEndIsIgnored) {
  InsertRange(0, 50);
  const auto live = LivePositions();
  const auto segment = RowPositionSegment(live.begin()->second);
  const std::vector<uint64_t> positions{
    MakeRowPosition(segment, irs::doc_limits::min() + 50)};
  Remove(positions, {});
  EXPECT_EQ(50, Live().size());
}

TEST_F(SearchRowRemovalTest, PositionOfAnUnknownSegmentIsIgnored) {
  InsertRange(0, 50);
  const std::vector<uint64_t> positions{MakeRowPosition(4000000, 1)};
  Remove(positions, {});
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
  Remove(victims);
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, ManyRemovalsInOneCommit) {
  InsertRange(0, 4000);
  InsertRange(4000, 4000);
  InsertRange(8000, 4000);
  const auto positions = LivePositions();
  auto trx = _writer->GetBatch();
  for (int64_t chunk = 0; chunk < 50; ++chunk) {
    const std::array<uint64_t, 2> places{positions.at(chunk * 7),
                                         positions.at(11999 - chunk * 5)};
    trx.Remove(*MakeRemoval(places));
    _model.erase(chunk * 7);
    _model.erase(11999 - chunk * 5);
  }
  ASSERT_TRUE(trx.Commit());
  _writer->RefreshCommit();
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, ManyStalePositionsInOneCommit) {
  InsertRange(0, 4000);
  InsertRange(4000, 4000);
  const auto snapshot = _writer->GetSnapshot();
  const auto before = PositionsIn(snapshot);
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  _writer->RefreshCommit();
  auto trx = _writer->GetBatch();
  for (int64_t chunk = 0; chunk < 40; ++chunk) {
    const std::array<uint64_t, 1> positions{before.at(chunk * 199)};
    trx.Remove(*MakeRemoval(positions));
    _model.erase(chunk * 199);
  }
  ASSERT_TRUE(trx.Commit());
  _writer->RefreshCommit();
  ExpectModel();
}

TEST_F(SearchRowRemovalTest,
       SegmentsWithoutTheRowidColumnAreRemovedByPosition) {
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
  sink.SwitchColumn(ColumnDescriptor{kValueColumn, duckdb::LogicalType::BIGINT},
                    values, 3);
  sink.Finish();
  ASSERT_TRUE(trx.Commit());
  _writer->RefreshCommit();
  const auto reader = _writer->GetSnapshot();
  ASSERT_EQ(1, reader.size());
  ASSERT_EQ(nullptr, reader[0].GetColReader()->Column(kGeneratedPKId));
  const auto number = irs::SegmentNumber(reader[0].Meta().name);
  const std::vector<uint64_t> positions{MakeRowPosition(number, 2)};
  Remove(positions, {});
  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());
}

TEST_F(SearchRowRemovalTest, RemovalReachesReplacementSegments) {
  InsertRange(0, 1000);
  const auto snapshot = _writer->GetSnapshot();
  const std::vector<int64_t> victims{0, 500, 999};
  const auto positions = PositionsOf(snapshot, victims);

  std::vector<int64_t> copy(1000);
  std::iota(copy.begin(), copy.end(), 0);
  const auto metas = FlushCopy(copy);

  auto trx = _writer->GetBatch();
  trx.Remove(*MakeRemoval(positions));
  ASSERT_TRUE(trx.Commit());

  ReplaceAll(snapshot, metas);
  _writer->RefreshCommit();
  for (const auto rowid : victims) {
    _model.erase(rowid);
  }
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, ReplaceSegmentsCarriesARemovalAppliedBeforeIt) {
  InsertRange(0, 1000);
  InsertRange(1000, 1000);
  const std::vector<int64_t> early{7, 1007};
  Remove(early);
  const auto snapshot = _writer->GetSnapshot();
  std::vector<int64_t> copy;
  for (const auto& segment : snapshot) {
    auto mask = segment.MaskedDocs();
    const auto rowids = ReadRowids(segment);
    for (size_t row = 0; row < rowids.size(); ++row) {
      if (!mask.Contains(static_cast<irs::doc_id_t>(row) +
                         irs::doc_limits::min())) {
        copy.push_back(rowids[row]);
      }
    }
  }
  const auto metas = FlushCopy(copy);

  const std::vector<int64_t> victims{1, 2, 998, 1500};
  Remove(victims);
  ReplaceAll(snapshot, metas);
  _writer->RefreshCommit();
  ASSERT_EQ(1, _writer->GetSnapshot().size());
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, RemovalAndInsertInOneTransaction) {
  InsertRange(0, 100);
  const std::vector<int64_t> victims{5};
  auto trx = _writer->GetBatch();
  trx.Remove(*MakeRemoval(PositionsOf(victims)));
  const std::vector<int64_t> later{200, 201};
  WriteRows(trx, later);
  ASSERT_TRUE(trx.Commit());
  _writer->RefreshCommit();
  _model.erase(5);
  _model.insert(later.begin(), later.end());
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, InsertAndRemovalInOneTransaction) {
  InsertRange(0, 100);
  const std::vector<int64_t> victims{6};
  const auto positions = PositionsOf(victims);
  auto trx = _writer->GetBatch();
  const std::vector<int64_t> earlier{300, 301};
  WriteRows(trx, earlier);
  trx.Remove(*MakeRemoval(positions));
  ASSERT_TRUE(trx.Commit());
  _writer->RefreshCommit();
  _model.erase(6);
  _model.insert(earlier.begin(), earlier.end());
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, SeveralRemovalsInOneCommit) {
  InsertRange(0, 3000);
  InsertRange(3000, 3000);
  const auto positions = LivePositions();
  auto trx = _writer->GetBatch();
  for (int64_t k = 0; k < 6; ++k) {
    std::vector<uint64_t> places;
    for (int64_t r = k; r < 6000; r += 600) {
      places.push_back(positions.at(r));
      _model.erase(r);
    }
    trx.Remove(*MakeRemoval(places));
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
  Remove(victims);
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
  Remove(victims);
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, LongRunsOfPositionsRemoveTheirRows) {
  InsertRange(0, 10000);
  InsertRange(10000, 10000);
  std::vector<int64_t> victims(9000);
  std::iota(victims.begin(), victims.end(), 5500);
  const auto positions = PositionsOf(victims);
  const auto removal = MakeRemoval(positions);
  ASSERT_TRUE(removal);
  EXPECT_EQ(2, removal->segments.size());
  EXPECT_GT(128, irs::WriteDocRemoval(*removal).size());
  Remove(positions, victims);
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, ValidAndPastTheEndPositionsInOneRemoval) {
  InsertRange(0, 100);
  const auto live = LivePositions();
  const auto segment = RowPositionSegment(live.at(0));
  const std::vector<int64_t> victims{3, 50};
  const std::vector<uint64_t> positions{
    live.at(3), MakeRowPosition(segment, irs::doc_limits::min() + 1000),
    live.at(50)};
  Remove(positions, victims);
  ExpectModel();
  EXPECT_EQ(98, Live().size());
}

TEST_F(SearchRowRemovalTest,
       PositionPastTheEndDuringAPendingCompactionIsIgnored) {
  InsertRange(0, 2000);
  InsertRange(2000, 2000);
  const auto snapshot = _writer->GetSnapshot();
  const auto live = PositionsIn(snapshot);
  const auto segment = RowPositionSegment(live.at(0));
  const std::vector<int64_t> victims{10, 2010};
  const std::vector<uint64_t> positions{
    live.at(10), MakeRowPosition(segment, irs::doc_limits::min() + 5000),
    live.at(2010)};
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  Remove(positions, victims);
  ASSERT_EQ(1, _writer->GetSnapshot().size());
  ExpectModel();
  EXPECT_EQ(3998, Live().size());
}

TEST_F(SearchRowRemovalTest, RemovingEveryRowEmptiesTheIndex) {
  InsertRange(0, 3000);
  InsertRange(3000, 3000);
  std::vector<int64_t> victims(6000);
  std::iota(victims.begin(), victims.end(), 0);
  Remove(victims);
  EXPECT_EQ(0, _writer->GetSnapshot().size());
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, RemoveAllTakesEveryCommittedRow) {
  InsertRange(0, 3000);
  InsertRange(3000, 3000);
  auto trx = _writer->GetBatch();
  trx.Remove(irs::DocRemoval::All());
  const std::vector<int64_t> later{7000, 7001};
  WriteRows(trx, later);
  ASSERT_TRUE(trx.Commit());
  _writer->RefreshCommit();
  _model.clear();
  _model.insert(later.begin(), later.end());
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, RemoveAllSparesRowsWrittenAfterIt) {
  InsertRange(0, 100);
  auto trx = _writer->GetBatch();
  const std::vector<int64_t> earlier{500, 501};
  WriteRows(trx, earlier);
  trx.Remove(irs::DocRemoval::All());
  const std::vector<int64_t> later{600};
  WriteRows(trx, later);
  ASSERT_TRUE(trx.Commit());
  _writer->RefreshCommit();
  _model = {600};
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
    const auto snapshot = _writer->GetSnapshot();
    std::vector<int64_t> victims;
    std::vector<uint64_t> positions;
    for (const auto& [rowid, position] : PositionsIn(snapshot)) {
      if (rng() % 9 == 0) {
        victims.push_back(rowid);
        positions.push_back(position);
      }
    }
    if (rng() % 3 == 0) {
      ASSERT_TRUE(_writer->Compact(FullMerge()));
    }
    if (rng() % 4 == 0) {
      _writer->RefreshCommit();
    }
    if (round % 2 == 0) {
      Remove(positions, victims);
    } else {
      RemoveReplayed(positions, victims);
    }
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
        const auto snapshot = _writer->GetSnapshot();
        std::vector<int64_t> victims;
        std::vector<uint64_t> positions;
        for (const auto& [rowid, position] : PositionsIn(snapshot)) {
          if (rng() % 50 == 0) {
            victims.push_back(rowid);
            positions.push_back(position);
          }
        }
        if (victims.empty()) {
          std::this_thread::yield();
          continue;
        }
        auto trx = _writer->GetBatch();
        trx.Remove(*MakeRemoval(positions));
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
