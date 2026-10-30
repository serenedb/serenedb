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
#include <chrono>
#include <duckdb.hpp>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/enums/wal_type.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <future>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/segment_reader.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <iterator>
#include <limits>
#include <map>
#include <mutex>
#include <numeric>
#include <random>
#include <ranges>
#include <set>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "../search_test_utils.h"
#include "connector/primary_key.h"
#include "connector/row_position.h"
#include "connector/term_dict.h"
#include "gtest/gtest.h"
#include "search/inverted_index_storage.h"
#include "search/row_position_wal.h"

namespace {

using namespace sdb;
using namespace sdb::connector;
using test::EngineDb;
using test::IdOf;
using test::OneRowSegments;

irs::IndexWriterOptions WriterOptions() {
  irs::IndexWriterOptions options;
  options.db = &EngineDb();
  options.reader_options.db = &EngineDb();
  options.key_field = term_dict::kPKFieldId;
  return options;
}

void WriteRows(irs::IndexWriter::Transaction& trx,
               std::span<const int64_t> rowids, bool index_term = false) {
  std::vector<duckdb::string_t> keys;
  std::vector<int64_t> values;
  for (size_t first = 0; first < rowids.size(); first += STANDARD_VECTOR_SIZE) {
    const auto chunk = rowids.subspan(
      first, std::min<size_t>(STANDARD_VECTOR_SIZE, rowids.size() - first));
    duckdb::Vector ids{duckdb::LogicalType::ROW_TYPE, chunk.size()};
    std::ranges::copy(chunk, duckdb::FlatVector::GetDataMutable<int64_t>(ids));
    keys.clear();
    values.clear();
    for (const auto rowid : chunk) {
      values.emplace_back(rowid * 10);
      if (index_term) {
        keys.emplace_back(primary_key::SignedKeyTerm(rowid));
      }
    }
    test::WriteValues(
      trx, values, PkChunk{.key_terms = keys, .column = &ids},
      PkPolicy{.index_term = index_term, .column = catalog::PkColumnKind::Has});
  }
}

std::shared_ptr<const irs::DocRemoval> MakeRemoval(
  std::span<const uint64_t> positions, const irs::DirectoryReader& snapshot) {
  irs::DocRemovalBuilder builder;
  for (const auto position : positions) {
    builder.Add(RowPositionIndex(position), RowPositionDoc(position));
  }
  return std::move(builder).Finish(snapshot);
}

irs::DocRemoval AsLogged(const irs::DocRemoval& removal) {
  irs::DocRemoval logged;
  for (const auto& group : removal.groups) {
    irs::DocumentMaskBuilder docs;
    docs.Merge(group.docs);
    logged.groups.push_back(
      {.segment = group.segment, .docs = std::move(docs).Finish()});
  }
  return logged;
}

using MapsBySource =
  irs::containers::FlatHashMap<uint64_t, std::vector<irs::DocMap>>;

irs::DocRemoval Rows(
  std::initializer_list<std::pair<uint64_t, irs::doc_id_t>> rows) {
  std::map<uint64_t, irs::DocumentMaskBuilder> groups;
  for (const auto& [segment, doc] : rows) {
    groups[segment].Add(doc);
  }
  irs::DocRemoval removal;
  for (auto& [segment, docs] : groups) {
    removal.groups.push_back(
      {.segment = segment, .docs = std::move(docs).Finish()});
  }
  return removal;
}

MapsBySource Logged(const irs::IndexWriter& writer,
                    std::span<const irs::DocRemoval* const> removals) {
  MapsBySource doc_maps;
  writer.VisitDocMaps(removals, [&](const std::vector<irs::DocMap>& maps) {
    doc_maps.insert_or_assign(maps.front().source, maps);
  });
  return doc_maps;
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
    _writer = irs::IndexWriter::Make(_dir, mode, WriterOptions());
  }

  void Reopen() {
    _writer.reset();
    Open(irs::kOmAppend);
  }

  void Insert(std::span<const int64_t> rowids, bool index_term = false) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    WriteRows(trx, rowids, index_term);
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

  static std::vector<std::pair<int64_t, uint64_t>> LiveRows(
    const irs::DirectoryReader& reader) {
    std::vector<std::pair<int64_t, uint64_t>> out;
    for (uint32_t index = 0; index != reader.size(); ++index) {
      const auto& segment = reader[index];
      const auto rowids = ReadRowids(segment);
      const auto mask = segment.MaskedDocs();
      for (size_t row = 0; row < rowids.size(); ++row) {
        const auto doc =
          static_cast<irs::doc_id_t>(row) + irs::doc_limits::min();
        if (!mask.Contains(doc)) {
          out.emplace_back(rowids[row], MakeRowPosition(index, doc));
        }
      }
    }
    return out;
  }

  static std::map<int64_t, uint64_t> PositionsIn(
    const irs::DirectoryReader& reader) {
    std::map<int64_t, uint64_t> out;
    for (const auto& [rowid, position] : LiveRows(reader)) {
      EXPECT_TRUE(out.emplace(rowid, position).second)
        << "rowid " << rowid << " is alive twice";
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

  template<typename Removal>
  void Commit(Removal removal, std::span<const int64_t> victims) {
    auto trx = _writer->GetBatch();
    trx.Remove(std::move(removal));
    ASSERT_TRUE(trx.Commit());
    _writer->RefreshCommit();
    for (const auto rowid : victims) {
      _model.erase(rowid);
    }
  }

  void Remove(const irs::DirectoryReader& snapshot,
              std::span<const uint64_t> positions,
              std::span<const int64_t> victims) {
    Commit(MakeRemoval(positions, snapshot), victims);
  }

  void Remove(std::span<const uint64_t> positions,
              std::span<const int64_t> victims) {
    Remove(_writer->GetSnapshot(), positions, victims);
  }

  void RemoveReplayed(const irs::DirectoryReader& snapshot,
                      std::span<const uint64_t> positions,
                      std::span<const int64_t> victims) {
    const auto removal = MakeRemoval(positions, snapshot);
    const std::array<const irs::DocRemoval*, 1> removals{removal.get()};
    const auto maps = Logged(*_writer, removals);
    Commit(
      search::ReplayRemoval(AsLogged(*removal), _writer->GetSnapshot(), maps),
      victims);
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
    std::vector<const irs::SubReader*> replaced;
    std::vector<irs::DocMap> absorbed;
    irs::doc_id_t base = irs::doc_limits::min();
    for (const auto& sub : snapshot) {
      const auto& segment =
        *irs::utils::downCast<irs::SegmentReader>(sub).GetImpl();
      const auto live = segment.Meta().live_docs_count;
      replaced.emplace_back(&sub);
      absorbed.push_back({.source = IdOf(sub),
                          .mask = segment.SharedDocsMask(),
                          .rank_begin = 0,
                          .rank_end = live,
                          .base = base});
      base += live;
    }
    ASSERT_EQ(1, metas.size());
    std::vector<irs::IndexWriter::Replacement> replacements;
    replacements.push_back(
      {.meta_file = metas.front(), .absorbed = std::move(absorbed)});
    _writer->ReplaceSegments(replaced, std::move(replacements));
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
  for (const uint32_t index : {uint32_t{0}, uint32_t{1}, uint32_t{77},
                               std::numeric_limits<uint32_t>::max()}) {
    for (const irs::doc_id_t doc :
         {irs::doc_limits::min(), irs::doc_id_t{2}, irs::doc_id_t{1 << 20},
          std::numeric_limits<irs::doc_id_t>::max() - 1}) {
      const auto position = MakeRowPosition(index, doc);
      EXPECT_EQ(index, RowPositionIndex(position));
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
  const std::vector<irs::doc_id_t> docs{10, 12, 14, 16};
  duckdb::Vector from_docs{duckdb::LogicalType::ROW_TYPE, 4};
  FillRowPositions(7, docs, from_docs);
  duckdb::SelectionVector sel{4};
  for (duckdb::idx_t i = 0; i < 4; ++i) {
    sel.set_index(i, 2 * i);
  }
  duckdb::Vector from_sel{duckdb::LogicalType::ROW_TYPE, 4};
  FillRowPositions(7, 10, &sel, 4, from_sel);
  duckdb::Vector from_run{duckdb::LogicalType::ROW_TYPE, 4};
  FillRowPositions(7, 10, nullptr, 4, from_run);
  EXPECT_EQ(duckdb::VectorType::SEQUENCE_VECTOR, from_run.GetVectorType());
  for (const auto* out : {&from_docs, &from_sel, &from_run}) {
    EXPECT_EQ(4, out->size());
  }
  for (duckdb::idx_t i = 0; i < 4; ++i) {
    EXPECT_EQ(MakeRowPosition(7, docs[i]),
              static_cast<uint64_t>(from_docs.GetValue(i).GetValue<int64_t>()));
    EXPECT_EQ(MakeRowPosition(7, docs[i]),
              static_cast<uint64_t>(from_sel.GetValue(i).GetValue<int64_t>()));
    EXPECT_EQ(MakeRowPosition(7, static_cast<irs::doc_id_t>(10 + i)),
              static_cast<uint64_t>(from_run.GetValue(i).GetValue<int64_t>()));
  }
}

TEST(RowPositionTest, FillRowPositionsOfHighIndexes) {
  constexpr auto kLast = std::numeric_limits<irs::doc_id_t>::max();
  const std::vector<irs::doc_id_t> docs{kLast - 2, kLast - 1, kLast};
  for (const uint32_t index :
       {uint32_t{0}, uint32_t{1} << 31, uint32_t{3000000000},
        std::numeric_limits<uint32_t>::max()}) {
    duckdb::Vector from_docs{duckdb::LogicalType::ROW_TYPE, 3};
    FillRowPositions(index, docs, from_docs);
    duckdb::Vector from_run{duckdb::LogicalType::ROW_TYPE, 3};
    FillRowPositions(index, docs[0], nullptr, 3, from_run);
    for (const auto* out : {&from_docs, &from_run}) {
      for (duckdb::idx_t i = 0; i < 3; ++i) {
        const auto position =
          static_cast<uint64_t>(out->GetValue(i).GetValue<int64_t>());
        EXPECT_EQ(index, RowPositionIndex(position)) << index << " " << i;
        EXPECT_EQ(docs[i], RowPositionDoc(position)) << index << " " << i;
      }
    }
  }
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
  Remove(snapshot, positions, victims);
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
  irs::DocRemoval logged;
  MapsBySource maps;
  {
    const auto snapshot = _writer->GetSnapshot();
    const auto positions = PositionsOf(snapshot, victims);
    ASSERT_TRUE(_writer->Compact(FullMerge()));
    _writer->RefreshCommit();
    const auto removal = MakeRemoval(positions, snapshot);
    const std::array<const irs::DocRemoval*, 1> removals{removal.get()};
    maps = Logged(*_writer, removals);
    logged = AsLogged(*removal);
  }
  ASSERT_FALSE(maps.empty());
  Reopen();
  ASSERT_EQ(1, _writer->GetSnapshot().size());
  Commit(search::ReplayRemoval(AsLogged(logged), _writer->GetSnapshot(), {}),
         {});
  ExpectModel();
  Commit(search::ReplayRemoval(std::move(logged), _writer->GetSnapshot(), maps),
         victims);
  ExpectModel();
}

constexpr uint64_t kGone = 1000000;
constexpr uint64_t kAlsoGone = 1000001;

TEST(ReplayRemovalTest, ARecoveredSegmentKeepsItsDocsAndSlot) {
  const auto live = IdOf(OneRowSegments()[2]);
  const auto removal =
    search::ReplayRemoval(Rows({{live, 1}}), OneRowSegments(), {});
  ASSERT_EQ(1, removal->groups.size());
  EXPECT_EQ(live, removal->groups[0].segment);
  EXPECT_TRUE(removal->groups[0].docs.Contains(1));
  EXPECT_EQ(irs::utils::downCast<irs::SegmentReader>(OneRowSegments()[2])
              .GetImpl()
              ->DocMaps(),
            removal->groups[0].maps);
}

TEST(ReplayRemovalTest, AGoneSegmentFollowsItsMapsToARecoveredOne) {
  const auto live = IdOf(OneRowSegments()[3]);
  MapsBySource maps;
  maps.emplace(
    kGone,
    std::vector<irs::DocMap>{
      {.source = kGone, .target = kAlsoGone, .rank_end = 10, .base = 5}});
  maps.emplace(
    kAlsoGone,
    std::vector<irs::DocMap>{
      {.source = kAlsoGone, .target = live, .rank_end = 20, .base = 1}});
  const auto removal =
    search::ReplayRemoval(Rows({{kGone, 2}}), OneRowSegments(), maps);
  ASSERT_EQ(1, removal->groups.size());
  EXPECT_EQ(live, removal->groups[0].segment);
  EXPECT_EQ(1, removal->groups[0].docs.Count());
  EXPECT_TRUE(removal->groups[0].docs.Contains(6));
}

TEST(ReplayRemovalTest, AGoneSegmentWithoutMapsIsDropped) {
  EXPECT_TRUE(search::ReplayRemoval(Rows({{kGone, 1}}), OneRowSegments(), {})
                ->groups.empty());
}

TEST(ReplayRemovalTest, AMapOfARecoveredSegmentIsIgnored) {
  const auto live = IdOf(OneRowSegments()[2]);
  MapsBySource maps;
  maps.emplace(
    live,
    std::vector<irs::DocMap>{
      {.source = live, .target = IdOf(OneRowSegments()[3]), .rank_end = 1}});
  const auto removal =
    search::ReplayRemoval(Rows({{live, 1}}), OneRowSegments(), maps);
  ASSERT_EQ(1, removal->groups.size());
  EXPECT_EQ(live, removal->groups[0].segment);
}

TEST(ReplayRemovalTest, ASourceSplitAcrossTargetsKeepsEveryWindow) {
  const auto first = IdOf(OneRowSegments()[1]);
  const auto second = IdOf(OneRowSegments()[4]);
  MapsBySource maps;
  maps.emplace(
    kGone,
    std::vector<irs::DocMap>{
      {.source = kGone, .target = first, .rank_end = 5},
      {.source = kGone, .target = second, .rank_begin = 5, .rank_end = 10}});
  const auto removal = search::ReplayRemoval(
    Rows({{kGone, 1}, {kGone, 2}, {kGone, 6}, {kGone, 10}}), OneRowSegments(),
    maps);
  ASSERT_EQ(2, removal->groups.size());
  EXPECT_EQ(first, removal->groups[0].segment);
  EXPECT_EQ(2, removal->groups[0].docs.Count());
  EXPECT_TRUE(removal->groups[0].docs.Contains(1));
  EXPECT_TRUE(removal->groups[0].docs.Contains(2));
  EXPECT_EQ(second, removal->groups[1].segment);
  EXPECT_EQ(2, removal->groups[1].docs.Count());
  EXPECT_TRUE(removal->groups[1].docs.Contains(1));
  EXPECT_TRUE(removal->groups[1].docs.Contains(5));
}

TEST(ReplayRemovalTest, AWindowWithoutRemovedDocsGetsNoGroup) {
  const auto first = IdOf(OneRowSegments()[1]);
  const auto second = IdOf(OneRowSegments()[4]);
  MapsBySource maps;
  maps.emplace(
    kGone,
    std::vector<irs::DocMap>{
      {.source = kGone, .target = first, .rank_end = 5},
      {.source = kGone, .target = second, .rank_begin = 5, .rank_end = 10}});
  const auto removal =
    search::ReplayRemoval(Rows({{kGone, 7}}), OneRowSegments(), maps);
  ASSERT_EQ(1, removal->groups.size());
  EXPECT_EQ(second, removal->groups[0].segment);
  EXPECT_EQ(1, removal->groups[0].docs.Count());
  EXPECT_TRUE(removal->groups[0].docs.Contains(2));
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
  const auto snapshot = _writer->GetSnapshot();
  const auto positions = PositionsOf(snapshot, victims);
  Remove(snapshot, positions, victims);
  Remove(snapshot, positions, victims);
  ExpectModel();
  EXPECT_EQ(497, Live().size());
}

TEST_F(SearchRowRemovalTest, DuplicatesInOneRemovalAreHarmless) {
  InsertRange(0, 500);
  const std::vector<int64_t> victims{9, 9, 9, 10, 10};
  Remove(victims);
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
  const auto snapshot = _writer->GetSnapshot();
  const auto positions = PositionsIn(snapshot);
  auto trx = _writer->GetBatch();
  for (int64_t chunk = 0; chunk < 50; ++chunk) {
    const std::array<uint64_t, 2> places{positions.at(chunk * 7),
                                         positions.at(11999 - chunk * 5)};
    trx.Remove(MakeRemoval(places, snapshot));
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
    trx.Remove(MakeRemoval(positions, snapshot));
    _model.erase(chunk * 199);
  }
  ASSERT_TRUE(trx.Commit());
  _writer->RefreshCommit();
  ExpectModel();
}

TEST_F(SearchRowRemovalTest,
       SegmentsWithoutTheRowidColumnAreRemovedByPosition) {
  auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
  test::WriteValues(trx, std::array<int64_t, 3>{1, 2, 3});
  ASSERT_TRUE(trx.Commit());
  _writer->RefreshCommit();
  const auto reader = _writer->GetSnapshot();
  ASSERT_EQ(1, reader.size());
  ASSERT_EQ(nullptr, reader[0].GetColReader()->Column(kGeneratedPKId));
  const std::vector<uint64_t> positions{MakeRowPosition(0, 2)};
  Remove(reader, positions, {});
  EXPECT_EQ(2, _writer->GetSnapshot().live_docs_count());
}

TEST_F(SearchRowRemovalTest, ReplaceSegmentsCarriesARemovalAppliedBeforeIt) {
  InsertRange(0, 1000);
  InsertRange(1000, 1000);
  const std::vector<int64_t> early{7, 1007};
  Remove(early);
  const auto snapshot = _writer->GetSnapshot();
  const auto copy =
    LiveRows(snapshot) | std::views::keys | std::ranges::to<std::vector>();
  const auto metas = FlushCopy(copy);

  const std::vector<int64_t> victims{1, 2, 998, 1500};
  Remove(victims);
  ReplaceAll(snapshot, metas);
  _writer->RefreshCommit();
  ASSERT_EQ(1, _writer->GetSnapshot().size());
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, RemovalAndInsertInOneTransactionInEitherOrder) {
  InsertRange(0, 100);
  for (const bool remove_first : {true, false}) {
    const std::vector<int64_t> victims{remove_first ? 5 : 6};
    const std::vector<int64_t> rows{remove_first ? 200 : 300};
    const auto snapshot = _writer->GetSnapshot();
    const auto removal = MakeRemoval(PositionsOf(snapshot, victims), snapshot);
    auto trx = _writer->GetBatch();
    if (remove_first) {
      trx.Remove(removal);
      WriteRows(trx, rows);
    } else {
      WriteRows(trx, rows);
      trx.Remove(removal);
    }
    ASSERT_TRUE(trx.Commit());
    _writer->RefreshCommit();
    _model.erase(victims.front());
    _model.insert(rows.begin(), rows.end());
    ExpectModel();
  }
}

TEST_F(SearchRowRemovalTest, LongRunsOfPositionsRemoveTheirRows) {
  InsertRange(0, 10000);
  InsertRange(10000, 10000);
  std::vector<int64_t> victims(9000);
  std::iota(victims.begin(), victims.end(), 5500);
  const auto snapshot = _writer->GetSnapshot();
  const auto positions = PositionsOf(snapshot, victims);
  const auto removal = MakeRemoval(positions, snapshot);
  ASSERT_EQ(2, removal->groups.size());
  EXPECT_GT(128, removal->groups[0].docs.FrozenSize() +
                   removal->groups[1].docs.FrozenSize());
  Remove(snapshot, positions, victims);
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, SegmentIdsPast32BitsAreRemovedByPosition) {
  _writer.reset();
  {
    auto out = _dir.create("_4294967294");
    ASSERT_TRUE(out);
  }
  auto options = WriterOptions();
  options.cleanup_on_open = false;
  _writer = irs::IndexWriter::Make(_dir, irs::kOmCreate, std::move(options));
  InsertRange(0, 100);
  InsertRange(100, 100);
  const auto snapshot = _writer->GetSnapshot();
  ASSERT_EQ(2, snapshot.size());
  for (const auto& segment : snapshot) {
    EXPECT_LE(std::numeric_limits<uint32_t>::max(), IdOf(segment));
  }
  const std::vector<int64_t> victims{3, 150};
  const auto positions = PositionsOf(snapshot, victims);
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  _writer->RefreshCommit();
  ASSERT_EQ(1, _writer->GetSnapshot().size());
  Remove(snapshot, positions, victims);
  ExpectModel();
}

TEST(WalRowPositionMapsTest, EntryTypesRoundTripTheirNames) {
  for (const auto type : {duckdb::WALType::DELETE_ROW_POSITIONS,
                          duckdb::WALType::ROW_POSITION_MAPS}) {
    EXPECT_EQ(type, duckdb::EnumUtil::FromString<duckdb::WALType>(
                      duckdb::EnumUtil::ToChars(type)));
  }
}

TEST_F(SearchRowRemovalTest, RowIdTermsRemoveTheLiveCopyOfTheirKey) {
  Insert(std::vector<int64_t>{5, 6, 7, 8}, /*index_term=*/true);
  Commit(irs::TermRemoval{{8, 5}}, std::vector<int64_t>{5, 8});
  Insert(std::vector<int64_t>{5, 8, 9}, /*index_term=*/true);
  Commit(irs::TermRemoval{{8, -3, 6, 100, 5, 8}},
         std::vector<int64_t>{5, 6, 8});
  ASSERT_EQ(2, _writer->GetSnapshot().size());
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, RowIdTermsOfOneCommitRemoveTogether) {
  Insert(std::vector<int64_t>{1, 2, 3}, /*index_term=*/true);
  Insert(std::vector<int64_t>{4, 5, 6}, /*index_term=*/true);
  {
    auto trx = _writer->GetBatch();
    trx.Remove(irs::TermRemoval{{5, 2}});
    trx.Remove(irs::TermRemoval{{2, 6, 9}});
    ASSERT_TRUE(trx.Commit());
  }
  {
    auto trx = _writer->GetBatch();
    trx.Remove(irs::TermRemoval{{1}});
    ASSERT_TRUE(trx.Commit());
  }
  _writer->RefreshCommit();
  _model = {3, 4};
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, RowIdTermsReachRowsFlushedInTheSameCommit) {
  Insert(std::vector<int64_t>{1, 2}, /*index_term=*/true);
  const auto write = [&](std::vector<int64_t> rowids) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    WriteRows(trx, rowids, /*index_term=*/true);
    ASSERT_TRUE(trx.Commit());
  };
  write({3, 4});
  {
    auto trx = _writer->GetBatch();
    trx.Remove(irs::TermRemoval{{2, 4}});
    ASSERT_TRUE(trx.Commit());
  }
  write({4});
  _writer->RefreshCommit();
  _model = {1, 3, 4};
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, RowIdTermsReachAMergedInSegment) {
  std::vector<int64_t> rowids(4000);
  std::iota(rowids.begin(), rowids.end(), 0);
  Insert(std::span{rowids}.first(2000), /*index_term=*/true);
  Insert(std::span{rowids}.subspan(2000), /*index_term=*/true);
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  const std::vector<int64_t> victims{7, 1999, 2000, 3999};
  Commit(irs::TermRemoval{victims}, victims);
  ASSERT_EQ(1, _writer->GetSnapshot().size());
  ExpectModel();
}

TEST_F(SearchRowRemovalTest, RowIdTermsPassACopyTheirCommitCarriesOut) {
  for (int64_t round = 0; round != 8; ++round) {
    const int64_t base = round * 10;
    const std::vector<int64_t> keys{base, base + 1, base + 2, base + 3};
    Insert(std::vector<int64_t>{base, base + 1, base + 2, base + 3, base + 4},
           /*index_term=*/true);
    Insert(std::vector<int64_t>{base + 5}, /*index_term=*/true);
    std::promise<void> merging;
    std::promise<void> resume;
    std::atomic_bool parked{false};
    irs::CompactionResult merged;
    std::thread merge{[&, resumed = resume.get_future()] {
      merged = _writer->Compact(FullMerge(), nullptr, [&] {
        if (!parked.exchange(true)) {
          merging.set_value();
          resumed.wait();
        }
        return true;
      });
    }};
    if (merging.get_future().wait_for(std::chrono::seconds{60}) !=
        std::future_status::ready) {
      merge.join();
      FAIL() << "the merge never reached its progress callback";
    }
    Commit(irs::TermRemoval{keys}, keys);
    for (const auto key : keys) {
      Insert(std::vector<int64_t>{key}, /*index_term=*/true);
    }
    {
      auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
      WriteRows(trx, std::vector<int64_t>{base + 6}, /*index_term=*/true);
      EXPECT_TRUE(trx.Commit());
    }
    EXPECT_TRUE(_writer->RefreshBegin());
    resume.set_value();
    merge.join();
    EXPECT_EQ(irs::CompactionError::Pending, merged.error);
    _writer->RefreshFinish();
    _model.insert(base + 6);
    Commit(irs::TermRemoval{keys}, keys);
    ExpectModel();
  }
}

TEST_F(SearchRowRemovalTest, ValidAndPastTheEndPositionsInOneRemoval) {
  InsertRange(0, 100);
  const auto live = LivePositions();
  const auto index = RowPositionIndex(live.at(0));
  const std::vector<int64_t> victims{3, 50};
  const std::vector<uint64_t> positions{
    live.at(3), MakeRowPosition(index, irs::doc_limits::min() + 1000),
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
  const auto index = RowPositionIndex(live.at(0));
  const std::vector<int64_t> victims{10, 2010};
  const std::vector<uint64_t> positions{
    live.at(10), MakeRowPosition(index, irs::doc_limits::min() + 5000),
    live.at(2010)};
  ASSERT_TRUE(_writer->Compact(FullMerge()));
  Remove(snapshot, positions, victims);
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
  trx.RemoveAll();
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
  trx.RemoveAll();
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
      Remove(snapshot, positions, victims);
    } else {
      RemoveReplayed(snapshot, positions, victims);
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
        auto removal = MakeRemoval(positions, snapshot);
        const std::array<const irs::DocRemoval*, 1> removals{removal.get()};
        _writer->VisitDocMaps(removals,
                              [](const std::vector<irs::DocMap>& maps) {
                                for (const auto& map : maps) {
                                  EXPECT_EQ(maps.front().source, map.source);
                                  EXPECT_LT(map.source, map.target);
                                }
                              });
        auto trx = _writer->GetBatch();
        trx.Remove(std::move(removal));
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
  for (bool idle = true; idle;) {
    std::this_thread::yield();
    std::lock_guard lock{mutex};
    idle = removed.empty();
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
