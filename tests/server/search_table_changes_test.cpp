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

#include <gtest/gtest.h>

#include <cstdint>
#include <duckdb.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <span>
#include <utility>
#include <vector>

#include "connector/row_position.h"
#include "search/search_table_changes.h"
#include "search_test_utils.h"

namespace {

using sdb::connector::FillRowPositions;
using sdb::connector::MakeRowPosition;
using sdb::search::LocalTableChangesEntry;
using sdb::test::EngineDb;
using sdb::test::OneRowSegments;

uint32_t IndexOf(uint64_t id) {
  return sdb::test::IndexOf(OneRowSegments(), id);
}

class SearchTableChangesRows : public ::testing::Test {
 protected:
  void AppendRows(LocalTableChangesEntry& changes, duckdb::idx_t count) {
    duckdb::DataChunk chunk;
    chunk.Initialize(duckdb::Allocator::DefaultAllocator(), _types, count);
    auto* values = duckdb::FlatVector::GetDataMutable<int64_t>(chunk.data[0]);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      values[i] = _next_row++;
    }
    chunk.SetCardinality(count);
    changes.AppendInsertChunk(
      duckdb::BufferManager::GetBufferManager(EngineDb()), _types, _columns,
      duckdb::Catalog::GetSystemCatalog(EngineDb()), chunk);
  }

 private:
  duckdb::vector<duckdb::LogicalType> _types{duckdb::LogicalType::BIGINT};
  std::vector<sdb::connector::ColumnId> _columns{0};
  int64_t _next_row = 0;
};

duckdb::Vector MakePositions(std::span<const uint64_t> values) {
  duckdb::Vector vector{duckdb::LogicalType::ROW_TYPE};
  auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(vector);
  for (size_t i = 0; i < values.size(); ++i) {
    data[i] = static_cast<int64_t>(values[i]);
  }
  duckdb::FlatVector::SetSize(vector, values.size());
  return vector;
}

void AppendDeletes(LocalTableChangesEntry& changes,
                   std::span<const uint64_t> positions) {
  changes.AppendDeletes(MakePositions(positions));
}

std::vector<uint64_t> Positions(const irs::DocRemoval& removal) {
  std::vector<uint64_t> positions;
  for (const auto [segment, doc] : sdb::test::RemovedDocs(removal)) {
    positions.push_back(MakeRowPosition(IndexOf(segment), doc));
  }
  return positions;
}

std::vector<uint64_t> Pending(const LocalTableChangesEntry& changes) {
  auto positions = changes.positions;
  return Positions(*std::move(positions).Finish(OneRowSegments()));
}

TEST_F(SearchTableChangesRows, DeletesGatherUntilAFlushFinishesThem) {
  LocalTableChangesEntry changes;
  const std::vector<uint64_t> first{7, 8, 9};
  const std::vector<uint64_t> second{1, 2};
  AppendDeletes(changes, first);
  AppendDeletes(changes, second);
  AppendRows(changes, 2);
  const std::vector<uint64_t> third{3};
  AppendDeletes(changes, third);
  EXPECT_EQ(Pending(changes), (std::vector<uint64_t>{1, 2, 3, 7, 8, 9}));

  const auto removal = changes.FinishRemoval(OneRowSegments());
  EXPECT_TRUE(changes.positions.Empty());
  ASSERT_EQ(1, changes.removals.size());
  EXPECT_EQ(removal, changes.removals.front());
  EXPECT_EQ(Positions(*removal), (std::vector<uint64_t>{1, 2, 3, 7, 8, 9}));

  const std::vector<uint64_t> fourth{4};
  AppendDeletes(changes, fourth);
  EXPECT_EQ(Pending(changes), (std::vector<uint64_t>{4}));
  EXPECT_EQ(1, changes.removals.size());
}

TEST_F(SearchTableChangesRows, ClearingTheRowsKeepsTheRemovals) {
  LocalTableChangesEntry changes;
  AppendRows(changes, 3);
  const std::vector<uint64_t> first{100};
  AppendDeletes(changes, first);
  changes.FinishRemoval(OneRowSegments());
  const std::vector<uint64_t> second{200};
  AppendDeletes(changes, second);
  changes.ClearBufferedRows();
  EXPECT_FALSE(changes.HasBufferedRows());
  ASSERT_EQ(1, changes.removals.size());
  EXPECT_EQ(Positions(*changes.removals.front()), (std::vector<uint64_t>{100}));
  EXPECT_EQ(Pending(changes), (std::vector<uint64_t>{200}));
}

TEST_F(SearchTableChangesRows, TruncateDropsEverythingBufferedBeforeIt) {
  LocalTableChangesEntry changes;
  const std::vector<uint64_t> positions{11, 12};
  AppendRows(changes, 2);
  AppendDeletes(changes, positions);
  changes.FinishRemoval(OneRowSegments());
  AppendDeletes(changes, positions);
  changes.AppendSegments({"_1"});
  changes.AppendTruncate(false);
  EXPECT_TRUE(changes.truncate);
  EXPECT_FALSE(changes.ClearsShard());
  EXPECT_FALSE(changes.HasBufferedRows());
  EXPECT_TRUE(changes.positions.Empty());
  EXPECT_TRUE(changes.removals.empty());
  EXPECT_TRUE(changes.segments.empty());

  AppendDeletes(changes, positions);
  EXPECT_TRUE(changes.truncate);
  EXPECT_EQ(Pending(changes), (std::vector<uint64_t>{11, 12}));
}

TEST(SearchTableChanges, EmptyDeleteLeavesNothingPending) {
  LocalTableChangesEntry changes;
  AppendDeletes(changes, {});
  EXPECT_TRUE(changes.positions.Empty());
}

TEST(SearchTableChanges, DeleteChunksJoin) {
  LocalTableChangesEntry changes;
  const std::vector<uint64_t> first{11, 12, 20};
  const std::vector<uint64_t> second{13, 14, 12};
  AppendDeletes(changes, first);
  AppendDeletes(changes, second);
  EXPECT_EQ(Pending(changes), (std::vector<uint64_t>{11, 12, 13, 14, 20}));
}

TEST(SearchTableChanges, PositionsKeepTheirSegments) {
  LocalTableChangesEntry changes;
  const auto last = irs::doc_limits::eof() - 1;
  const std::vector<uint64_t> positions{
    MakeRowPosition(3, last - 1), MakeRowPosition(3, last),
    MakeRowPosition(4, irs::doc_limits::min()), MakeRowPosition(3, 2)};
  AppendDeletes(changes, positions);
  const auto& removal = *changes.FinishRemoval(OneRowSegments());
  std::vector<uint32_t> indexes;
  for (const auto& group : removal.groups) {
    indexes.emplace_back(IndexOf(group.segment));
  }
  EXPECT_EQ((std::vector<uint32_t>{3, 4}), indexes);
  EXPECT_EQ(
    Positions(removal),
    (std::vector<uint64_t>{MakeRowPosition(3, 2), MakeRowPosition(3, last - 1),
                           MakeRowPosition(3, last),
                           MakeRowPosition(4, irs::doc_limits::min())}));
}

TEST(SearchTableChanges, AMillionRowDeleteFreezesToAFewBytes) {
  LocalTableChangesEntry changes;
  std::vector<uint64_t> chunk(STANDARD_VECTOR_SIZE);
  uint64_t next = MakeRowPosition(5, irs::doc_limits::min());
  for (size_t rows = 0; rows < 1000000; rows += chunk.size()) {
    for (auto& position : chunk) {
      position = next++;
    }
    AppendDeletes(changes, chunk);
  }
  const auto& removal = *changes.FinishRemoval(OneRowSegments());
  ASSERT_EQ(1, removal.groups.size());
  EXPECT_LE(1000000, removal.groups[0].docs.Count());
  EXPECT_GT(1024, removal.groups[0].docs.FrozenSize());
}

TEST(SearchTableChanges, DeletesReadConstantVectors) {
  LocalTableChangesEntry changes;
  const auto position = MakeRowPosition(1, 5);
  const duckdb::Vector positions{
    duckdb::Value::BIGINT(static_cast<int64_t>(position)), duckdb::count_t(3)};
  changes.AppendDeletes(positions);
  EXPECT_EQ(Pending(changes), (std::vector<uint64_t>{position}));
}

TEST(SearchTableChanges, DeletesReadDictionaryVectors) {
  LocalTableChangesEntry changes;
  std::vector<uint64_t> places;
  for (uint32_t i = 0; i < 10; ++i) {
    places.emplace_back(MakeRowPosition(2, 1 + i));
  }
  auto positions = MakePositions(places);
  duckdb::SelectionVector sel{3};
  sel.set_index(0, 9);
  sel.set_index(1, 3);
  sel.set_index(2, 3);
  positions.Slice(sel, 3);
  changes.AppendDeletes(positions);
  EXPECT_EQ(Pending(changes), (std::vector<uint64_t>{MakeRowPosition(2, 4),
                                                     MakeRowPosition(2, 10)}));
}

TEST(SearchTableChanges, DeletesReadFilledPositions) {
  LocalTableChangesEntry changes;
  duckdb::Vector positions{duckdb::LogicalType::ROW_TYPE};
  FillRowPositions(6, 20, nullptr, 4, positions);
  changes.AppendDeletes(positions);
  EXPECT_EQ(
    Pending(changes),
    (std::vector<uint64_t>{MakeRowPosition(6, 20), MakeRowPosition(6, 21),
                           MakeRowPosition(6, 22), MakeRowPosition(6, 23)}));
}

}  // namespace
