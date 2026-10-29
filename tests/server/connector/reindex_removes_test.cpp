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

#include <roaring/roaring64.h>

#include <cstdint>
#include <duckdb.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <initializer_list>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/segment_reader.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <limits>
#include <roaring/roaring.hh>
#include <set>
#include <span>
#include <utility>
#include <vector>

#include "../search_test_utils.h"
#include "connector/reindex_removes.h"
#include "connector/view_fast_path.h"
#include "gtest/gtest.h"

namespace {

using namespace sdb;
using namespace sdb::connector;
using test::EngineDb;
using test::IdOf;
using test::IndexOf;

using Row = std::pair<uint64_t, int64_t>;
using Doc = std::pair<uint64_t, irs::doc_id_t>;
using DeletionVector = duckdb::unordered_map<int32_t, roaring::Roaring>;

constexpr uint32_t kRowGroupSize = 4;

duckdb::shared_ptr<const roaring::api::roaring64_bitmap_t> DeadRowsOf(
  const std::vector<uint64_t>& rows) {
  auto* bitmap = roaring::api::roaring64_bitmap_create();
  for (const auto row : rows) {
    roaring::api::roaring64_bitmap_add(bitmap, row);
  }
  return duckdb::shared_ptr<const roaring::api::roaring64_bitmap_t>{
    bitmap, [](const roaring::api::roaring64_bitmap_t* dead) {
      roaring::api::roaring64_bitmap_free(
        const_cast<roaring::api::roaring64_bitmap_t*>(dead));
    }};
}

duckdb::shared_ptr<const DeletionVector> DeletionVectorOf(
  DeletionVector buckets) {
  return duckdb::make_shared_ptr<const DeletionVector>(std::move(buckets));
}

std::vector<int64_t> Rows(int64_t first, int64_t last) {
  std::vector<int64_t> rows;
  for (auto row = first; row < last; ++row) {
    rows.push_back(row);
  }
  return rows;
}

class ReindexRemovesTest : public ::testing::Test {
 protected:
  void SetUp() override {
    irs::IndexWriterOptions options;
    options.db = &EngineDb();
    options.reader_options.db = &EngineDb();
    options.column_options = [](irs::field_id) { return irs::ColumnOptions{}; };
    options.row_group_size = kRowGroupSize;
    _writer = irs::IndexWriter::Make(_dir, irs::kOmCreate, std::move(options));
  }

  void TearDown() override { _writer.reset(); }

  uint64_t AddSegment(std::span<const Row> rows) {
    const auto before = Segments();
    duckdb::Vector key{FileIndexRowNumberStructType(), rows.size()};
    auto& halves = duckdb::StructVector::GetEntries(key);
    auto* files = duckdb::FlatVector::GetDataMutable<uint64_t>(halves[0]);
    auto* row_numbers = duckdb::FlatVector::GetDataMutable<int64_t>(halves[1]);
    std::vector<int64_t> values;
    for (size_t i = 0; i != rows.size(); ++i) {
      files[i] = rows[i].first;
      row_numbers[i] = rows[i].second;
      values.emplace_back(rows[i].second);
    }
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    test::WriteValues(
      trx, values, PkChunk{.column = &key},
      PkPolicy{.index_term = false, .column = catalog::PkColumnKind::Has});
    EXPECT_TRUE(trx.Commit());
    _writer->RefreshCommit();
    for (const auto segment : Segments()) {
      if (!before.contains(segment)) {
        return segment;
      }
    }
    ADD_FAILURE() << "no new segment";
    return 0;
  }

  uint64_t AddFile(uint64_t file, std::span<const int64_t> rows) {
    std::vector<Row> keyed;
    for (const auto row : rows) {
      keyed.emplace_back(file, row);
    }
    return AddSegment(keyed);
  }

  void Mask(std::span<const Doc> docs) {
    const auto snapshot = _writer->GetSnapshot();
    irs::DocRemovalBuilder builder;
    for (const auto [segment, doc] : docs) {
      builder.Add(IndexOf(snapshot, segment), doc);
    }
    auto trx = _writer->GetBatch();
    trx.Remove(std::move(builder).Finish(snapshot));
    ASSERT_TRUE(trx.Commit());
    _writer->RefreshCommit();
  }

  std::set<uint64_t> Segments() const {
    std::set<uint64_t> out;
    for (const auto& segment : _writer->GetSnapshot()) {
      out.insert(IdOf(segment));
    }
    return out;
  }

  std::vector<Doc> Resolve(
    const std::vector<uint64_t>& deleted_files,
    const std::vector<IcebergObserve::DeleteMask>& masks = {},
    std::vector<Row> eq_rows = {}) const {
    return test::RemovedDocs(*ResolveRemoves(
      _writer->GetSnapshot(), deleted_files, masks, std::move(eq_rows)));
  }

  static std::vector<Doc> DocsOf(uint64_t segment,
                                 std::initializer_list<size_t> positions) {
    std::vector<Doc> docs;
    for (const auto position : positions) {
      docs.emplace_back(segment, DocAt(position));
    }
    return docs;
  }

  std::set<Row> AddMergedFiles(uint64_t files, int64_t rows) {
    std::vector<Doc> masked;
    std::set<Row> gone;
    for (uint64_t file = 1; file <= files; ++file) {
      const auto segment = AddFile(file, Rows(0, rows));
      for (int64_t row = 0; row < rows; ++row) {
        if ((file * 31 + row * 17) % 5 == 0) {
          masked.emplace_back(segment, DocAt(row));
          gone.emplace(file, row);
        }
      }
    }
    Mask(masked);
    EXPECT_TRUE(_writer->Compact(irs::index_utils::MakePolicy(
      irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()})));
    _writer->RefreshCommit();
    EXPECT_EQ(1, Segments().size());
    return gone;
  }

  static irs::doc_id_t DocAt(size_t index) {
    return static_cast<irs::doc_id_t>(irs::doc_limits::min() + index);
  }

  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _writer;
};

TEST_F(ReindexRemovesTest, ARemovalNamesItsSegmentsAndHoldsTheirDocMaps) {
  const auto segment = AddFile(1, Rows(0, 4));
  const auto snapshot = _writer->GetSnapshot();
  const std::vector<uint64_t> files{1};
  const auto removal = ResolveRemoves(snapshot, files, {}, {});
  ASSERT_EQ(1, removal->groups.size());
  EXPECT_EQ(segment, removal->groups.front().segment);
  EXPECT_EQ(
    irs::utils::downCast<irs::SegmentReader>(snapshot[0]).GetImpl()->DocMaps(),
    removal->groups.front().maps);
}

TEST_F(ReindexRemovesTest, NothingToRemoveIsNoRemoval) {
  AddFile(1, Rows(0, 4));
  EXPECT_TRUE(
    ResolveRemoves(_writer->GetSnapshot(), {}, {}, {})->groups.empty());
  EXPECT_TRUE(Resolve({7}).empty());
  EXPECT_TRUE(Resolve({}, {}, {{7, 0}}).empty());
}

TEST_F(ReindexRemovesTest, DeletedFilesRemoveEveryRowOfTheFile) {
  const std::vector<Row> first_rows{{1, 0}, {1, 1}, {1, 2}, {2, 0}, {2, 1}};
  const auto first = AddSegment(first_rows);
  const std::vector<Row> second_rows{{0, 9}, {1, 3}, {3, 0}};
  const auto second = AddSegment(second_rows);
  Mask(std::vector<Doc>{{first, DocAt(1)}});
  EXPECT_EQ((std::vector<Doc>{{first, DocAt(0)},
                              {first, DocAt(1)},
                              {first, DocAt(2)},
                              {second, DocAt(1)}}),
            Resolve({1, 9}));
  EXPECT_EQ((std::vector<Doc>{{second, DocAt(2)}}), Resolve({3}));
  EXPECT_TRUE(Resolve({}, {{.file_id = 0, .rows = DeadRowsOf({8})}}).empty());
  EXPECT_EQ((std::vector<Doc>{{second, DocAt(0)}}),
            Resolve({}, {{.file_id = 0, .rows = DeadRowsOf({9})}}));
}

TEST_F(ReindexRemovesTest, DeadRowsRemoveOnlyTheRowsTheirFileHas) {
  const std::vector<int64_t> kept{0, 1, 2, 4, 5, 7};
  const auto segment = AddFile(1, kept);
  const auto other = AddFile(2, Rows(0, 2));
  EXPECT_EQ((std::vector<Doc>{
              {segment, DocAt(1)}, {segment, DocAt(2)}, {other, DocAt(1)}}),
            Resolve({}, {{.file_id = 1, .rows = DeadRowsOf({1, 2, 3, 6, 9})},
                         {.file_id = 2, .rows = DeadRowsOf({1, 40})}}));
}

TEST_F(ReindexRemovesTest, DeadRowsPastTheEndOfAFileFindNothing) {
  const auto segment = AddFile(5, Rows(0, 3));
  EXPECT_EQ((std::vector<Doc>{{segment, DocAt(2)}}),
            Resolve({}, {{.file_id = 5, .rows = DeadRowsOf({2, 3, 8})},
                         {.file_id = 6, .rows = DeadRowsOf({0})}}));
}

TEST_F(ReindexRemovesTest, DeletionVectorsNameTheirRowsBucketByBucket) {
  constexpr int64_t kHigh = int64_t{1} << 32;
  const std::vector<int64_t> rows{
    0, 1, 2, 3, 4, 5, kHigh | 2, kHigh | 3, kHigh | 5, 2 * kHigh};
  const auto segment = AddFile(1, rows);
  roaring::Roaring low;
  low.add(1);
  low.add(4);
  low.add(6);
  roaring::Roaring high;
  high.add(3);
  EXPECT_EQ((std::vector<Doc>{
              {segment, DocAt(1)}, {segment, DocAt(4)}, {segment, DocAt(7)}}),
            Resolve({}, {{.file_id = 1,
                          .dv = DeletionVectorOf({{1, high}, {0, low}})}}));
  EXPECT_EQ((std::vector<Doc>{{segment, DocAt(1)}, {segment, DocAt(4)}}),
            Resolve({}, {{.file_id = 1, .dv = DeletionVectorOf({{0, low}})}}));
}

TEST_F(ReindexRemovesTest, DeletedFilesAndChangedMasksResolveTogether) {
  const auto first = AddFile(1, Rows(0, 6));
  const auto second = AddFile(2, Rows(0, 3));
  const auto third = AddFile(3, Rows(0, 4));
  roaring::Roaring dead;
  dead.add(2);
  EXPECT_EQ(
    (std::vector<Doc>{{first, DocAt(1)},
                      {first, DocAt(4)},
                      {second, DocAt(0)},
                      {second, DocAt(1)},
                      {second, DocAt(2)},
                      {third, DocAt(2)}}),
    Resolve({2}, {{.file_id = 1, .rows = DeadRowsOf({1, 4})},
                  {.file_id = 3, .dv = DeletionVectorOf({{0, dead}})}}));
}

TEST_F(ReindexRemovesTest, RowGroupsOfAWhollyRemovedFileAreRemovedWhole) {
  std::vector<Row> rows;
  for (int64_t row = 0; row != 10; ++row) {
    rows.emplace_back(1, row);
  }
  for (int64_t row = 0; row != 6; ++row) {
    rows.emplace_back(2, row);
  }
  const auto segment = AddSegment(rows);
  EXPECT_EQ(DocsOf(segment, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}), Resolve({1}));
  EXPECT_EQ(DocsOf(segment, {10, 11, 12, 13, 14, 15}), Resolve({2}));
}

TEST_F(ReindexRemovesTest, DeadRowsFindTheirRowGroupsWhateverTheRowOrder) {
  std::vector<Row> rows;
  for (int64_t row = 11; row >= 0; --row) {
    rows.emplace_back(1, row);
  }
  for (int64_t row = 0; row != 8; ++row) {
    rows.emplace_back(2, row * 10);
  }
  const auto segment = AddSegment(rows);
  EXPECT_EQ(DocsOf(segment, {5, 6, 11}),
            Resolve({}, {{.file_id = 1, .rows = DeadRowsOf({0, 5, 6, 40})}}));
  EXPECT_EQ(DocsOf(segment, {13, 18}),
            Resolve({}, {{.file_id = 2, .rows = DeadRowsOf({1, 10, 41, 60})}}));
}

TEST_F(ReindexRemovesTest, RowGroupsMixingFilesRemoveEachFilesOwnRows) {
  std::vector<Row> rows;
  for (int64_t row = 0; row != 6; ++row) {
    rows.emplace_back(1 + row % 3, row);
  }
  const auto segment = AddSegment(rows);
  EXPECT_EQ(DocsOf(segment, {1, 4}), Resolve({2}));
  EXPECT_EQ(DocsOf(segment, {1, 3, 4}),
            Resolve({2}, {{.file_id = 1, .rows = DeadRowsOf({1, 3})},
                          {.file_id = 3, .rows = DeadRowsOf({0, 1})}}));
  EXPECT_EQ(DocsOf(segment, {2, 5}), Resolve({}, {}, {{3, 2}, {3, 5}, {1, 2}}));
  EXPECT_EQ(
    DocsOf(segment, {0, 3, 4}),
    Resolve({}, {{.file_id = 1, .rows = DeadRowsOf({0})}}, {{1, 3}, {2, 4}}));
}

TEST_F(ReindexRemovesTest, EveryKindOfRemoveOfOneFileUnites) {
  const auto segment = AddFile(1, Rows(0, 10));
  roaring::Roaring dead;
  dead.add(7);
  const std::vector<IcebergObserve::DeleteMask> masks{
    {.file_id = 1, .dv = DeletionVectorOf({{0, dead}})}};
  EXPECT_EQ(DocsOf(segment, {2, 3, 7}), Resolve({}, masks, {{1, 3}, {1, 2}}));
  EXPECT_EQ(DocsOf(segment, {3, 5}),
            Resolve({}, {{.file_id = 1, .rows = DeadRowsOf({5})}}, {{1, 3}}));
  EXPECT_EQ(DocsOf(segment, {8}), Resolve({}, {}, {{1, 8}}));
  EXPECT_EQ(Resolve({1}), Resolve({1}, masks, {{1, 3}}));
}

TEST_F(ReindexRemovesTest, DeadRowsOfMergedFilesAllFindTheirDocs) {
  constexpr uint64_t kFiles = 60;
  constexpr int64_t kRows = 175;
  const auto gone = AddMergedFiles(kFiles, kRows);
  std::vector<IcebergObserve::DeleteMask> masks;
  size_t expected = 0;
  for (uint64_t file = kFiles + 1; file >= 1; --file) {
    std::vector<uint64_t> dead;
    for (int64_t row = 0; row < kRows + 5; ++row) {
      if ((file * 7 + row * 3) % 4 == 0) {
        dead.push_back(row);
        if (file <= kFiles && row < kRows && !gone.contains({file, row})) {
          ++expected;
        }
      }
    }
    masks.push_back({.file_id = file, .rows = DeadRowsOf(dead)});
  }
  EXPECT_EQ(expected, Resolve({}, masks).size());
}

TEST_F(ReindexRemovesTest, EqualityRowsRemoveTheirDocsInEverySegment) {
  const std::vector<Row> first_rows{{1, 2}, {1, 3}};
  const auto first = AddSegment(first_rows);
  const std::vector<Row> second_rows{{1, 2}, {4, 4}};
  const auto second = AddSegment(second_rows);
  EXPECT_EQ((std::vector<Doc>{{first, DocAt(0)}, {second, DocAt(0)}}),
            Resolve({}, {}, {{1, 2}, {9, 9}}));
  EXPECT_EQ((std::vector<Doc>{
              {first, DocAt(0)}, {second, DocAt(0)}, {second, DocAt(1)}}),
            Resolve({}, {}, {{4, 4}, {1, 2}}));
}

}  // namespace
