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

#include <algorithm>
#include <cstdint>
#include <duckdb.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "connector/row_position.h"
#include "search/search_table_changes.h"

namespace {

using sdb::connector::FillRowPositions;
using sdb::connector::MakeRowPosition;
using sdb::search::LocalTableChangesEntry;

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
      duckdb::BufferManager::GetBufferManager(*_db.instance), _types, _columns,
      duckdb::Catalog::GetSystemCatalog(*_db.instance), chunk);
  }

  static std::vector<int64_t> Rows(const duckdb::DataChunk& chunk) {
    std::vector<int64_t> rows;
    for (duckdb::idx_t i = 0; i < chunk.size(); ++i) {
      rows.emplace_back(chunk.GetValue(0, i).GetValue<int64_t>());
    }
    return rows;
  }

 private:
  duckdb::DuckDB _db{nullptr};
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
  for (size_t group = 0; group < removal.segments.size(); ++group) {
    removal.Docs(group).ForEach([&](irs::doc_id_t doc) {
      positions.push_back(MakeRowPosition(removal.segments[group], doc));
    });
  }
  return positions;
}

std::vector<uint64_t> Pending(const LocalTableChangesEntry::Op& op) {
  auto positions = op.positions;
  return Positions(std::move(positions).Finish());
}

TEST_F(SearchTableChangesRows, DeletesShareAnOpUntilRowsOrAFlushComeBetween) {
  LocalTableChangesEntry changes;
  const std::vector<uint64_t> first{7, 8, 9};
  const std::vector<uint64_t> second{1, 2};
  AppendDeletes(changes, first);
  AppendDeletes(changes, second);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_EQ(Pending(changes.ops[0]), (std::vector<uint64_t>{1, 2, 7, 8, 9}));

  AppendRows(changes, 2);
  const std::vector<uint64_t> third{3};
  AppendDeletes(changes, third);
  ASSERT_EQ(changes.ops.size(), 2);
  EXPECT_EQ(changes.ops[1].row_watermark, 2);

  changes.applied_ops = changes.ops.size();
  const std::vector<uint64_t> fourth{4};
  AppendDeletes(changes, fourth);
  ASSERT_EQ(changes.ops.size(), 3);
  EXPECT_EQ(Pending(changes.ops[2]), (std::vector<uint64_t>{4}));
}

TEST(SearchTableChanges, EmptyDeleteAddsNoOp) {
  LocalTableChangesEntry changes;
  AppendDeletes(changes, {});
  EXPECT_TRUE(changes.ops.empty());
}

TEST(SearchTableChanges, DeleteChunksJoinInTheirOp) {
  LocalTableChangesEntry changes;
  const std::vector<uint64_t> first{11, 12, 20};
  const std::vector<uint64_t> second{13, 14, 12};
  AppendDeletes(changes, first);
  AppendDeletes(changes, second);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_EQ(Pending(changes.ops[0]),
            (std::vector<uint64_t>{11, 12, 13, 14, 20}));
}

TEST(SearchTableChanges, PositionsKeepTheirSegments) {
  LocalTableChangesEntry changes;
  const auto last = irs::doc_limits::eof() - 1;
  const std::vector<uint64_t> positions{
    MakeRowPosition(3, last - 1), MakeRowPosition(3, last),
    MakeRowPosition(4, irs::doc_limits::min()), MakeRowPosition(3, 2)};
  AppendDeletes(changes, positions);
  ASSERT_EQ(changes.ops.size(), 1);
  const auto& removal = changes.ops[0].Removal();
  EXPECT_EQ((std::vector<uint32_t>{3, 4}), removal.segments);
  EXPECT_EQ(
    Positions(removal),
    (std::vector<uint64_t>{MakeRowPosition(3, 2), MakeRowPosition(3, last - 1),
                           MakeRowPosition(3, last),
                           MakeRowPosition(4, irs::doc_limits::min())}));
}

TEST(SearchTableChanges, AnOpBuildsItsRemovalOnce) {
  LocalTableChangesEntry changes;
  const std::vector<uint64_t> positions{MakeRowPosition(2, 7),
                                        MakeRowPosition(1, 3)};
  AppendDeletes(changes, positions);
  auto& op = changes.ops[0];
  const auto* docs = &op.Removal().Docs(0);
  EXPECT_TRUE(op.positions.Empty());
  EXPECT_EQ(docs, &op.Removal().Docs(0));
  EXPECT_EQ(
    Positions(op.Removal()),
    (std::vector<uint64_t>{MakeRowPosition(1, 3), MakeRowPosition(2, 7)}));
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
  ASSERT_EQ(changes.ops.size(), 1);
  const auto& removal = changes.ops[0].Removal();
  ASSERT_EQ(1, removal.docs.size());
  EXPECT_LE(1000000, removal.Docs(0).Count());
  EXPECT_GT(1024, removal.Docs(0).FrozenSize());
}

TEST(SearchTableChanges, DeletedRowWithoutPositionFailsAndAddsNoOp) {
  LocalTableChangesEntry changes;
  const std::vector<uint64_t> places{
    MakeRowPosition(4, 1), MakeRowPosition(4, 2), MakeRowPosition(4, 3)};
  auto positions = MakePositions(places);
  duckdb::FlatVector::ValidityMutable(positions).SetInvalid(1);
  EXPECT_THROW(changes.AppendDeletes(positions), irs::SqlException);
  EXPECT_TRUE(changes.ops.empty());
}

TEST(SearchTableChanges, DeletesWithConstantNullPositionFail) {
  LocalTableChangesEntry changes;
  const duckdb::Vector positions{duckdb::Value(duckdb::LogicalType::ROW_TYPE),
                                 duckdb::count_t(3)};
  EXPECT_THROW(changes.AppendDeletes(positions), irs::SqlException);
  EXPECT_TRUE(changes.ops.empty());
}

TEST(SearchTableChanges, DeletesReadConstantVectors) {
  LocalTableChangesEntry changes;
  const auto position = MakeRowPosition(1, 5);
  const duckdb::Vector positions{
    duckdb::Value::BIGINT(static_cast<int64_t>(position)), duckdb::count_t(3)};
  changes.AppendDeletes(positions);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_EQ(Pending(changes.ops[0]), (std::vector<uint64_t>{position}));
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
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_EQ(
    Pending(changes.ops[0]),
    (std::vector<uint64_t>{MakeRowPosition(2, 4), MakeRowPosition(2, 10)}));
}

TEST(SearchTableChanges, DeletesReadFilledPositions) {
  LocalTableChangesEntry changes;
  duckdb::Vector positions{duckdb::LogicalType::ROW_TYPE};
  FillRowPositions(
    6, 4, [](duckdb::idx_t i) { return static_cast<irs::doc_id_t>(20 + i); },
    positions);
  changes.AppendDeletes(positions);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_EQ(
    Pending(changes.ops[0]),
    (std::vector<uint64_t>{MakeRowPosition(6, 20), MakeRowPosition(6, 21),
                           MakeRowPosition(6, 22), MakeRowPosition(6, 23)}));
}

TEST(SearchTableChanges, TruncateDropsTheDeletesBeforeIt) {
  LocalTableChangesEntry changes;
  const std::vector<uint64_t> positions{11, 12};
  AppendDeletes(changes, positions);
  changes.AppendTruncate(false);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_TRUE(changes.ops[0].IsTruncate());
  EXPECT_TRUE(changes.ops[0].positions.Empty());
  AppendDeletes(changes, positions);
  ASSERT_EQ(changes.ops.size(), 2);
  EXPECT_FALSE(changes.ops[1].IsTruncate());
  EXPECT_EQ(Pending(changes.ops[1]), (std::vector<uint64_t>{11, 12}));
}

TEST(SearchTableChanges, VisitAppliesEveryOpInOrderWithoutRows) {
  LocalTableChangesEntry changes;
  for (const uint64_t first : {100, 200, 300}) {
    const std::vector<uint64_t> positions{first, first + 1};
    AppendDeletes(changes, positions);
    changes.applied_ops = changes.ops.size();
  }
  ASSERT_EQ(changes.ops.size(), 3);
  std::vector<uint64_t> applied;
  changes.Visit(
    0, [](duckdb::DataChunk&) { FAIL(); },
    [&](LocalTableChangesEntry::Op& op) {
      const auto positions = Positions(op.Removal());
      ASSERT_EQ(2, positions.size());
      applied.push_back(positions.front());
    });
  EXPECT_EQ(applied, (std::vector<uint64_t>{100, 200, 300}));
}

TEST_F(SearchTableChangesRows, VisitInterleavesOpsWithTheRowsBeforeThem) {
  LocalTableChangesEntry changes;
  AppendRows(changes, 3);
  const std::vector<uint64_t> first{100};
  AppendDeletes(changes, first);
  AppendRows(changes, 2);
  const std::vector<uint64_t> second{200};
  AppendDeletes(changes, second);
  const std::vector<uint64_t> third{300};
  AppendRows(changes, 1);
  AppendDeletes(changes, third);
  ASSERT_EQ(changes.ops.size(), 3);
  std::vector<std::string> events;
  changes.Visit(
    0,
    [&](duckdb::DataChunk& chunk) {
      for (const auto row : Rows(chunk)) {
        events.emplace_back("row " + std::to_string(row));
      }
    },
    [&](LocalTableChangesEntry::Op& op) {
      events.emplace_back("op " +
                          std::to_string(Positions(op.Removal()).front()));
    });
  EXPECT_EQ(events, (std::vector<std::string>{"row 0", "row 1", "row 2",
                                              "op 100", "row 3", "row 4",
                                              "op 200", "row 5", "op 300"}));
}

TEST_F(SearchTableChangesRows, VisitEmitsTheRowsBetweenOpsTogether) {
  LocalTableChangesEntry changes;
  for (int i = 0; i < 5; ++i) {
    AppendRows(changes, 1);
  }
  std::vector<std::vector<int64_t>> emitted;
  changes.Visit(
    0, [&](duckdb::DataChunk& chunk) { emitted.emplace_back(Rows(chunk)); },
    [](LocalTableChangesEntry::Op&) { FAIL(); });
  EXPECT_EQ(emitted, (std::vector<std::vector<int64_t>>{{0, 1, 2, 3, 4}}));
}

TEST_F(SearchTableChangesRows, VisitStartsAtTheGivenOp) {
  LocalTableChangesEntry changes;
  const std::vector<uint64_t> first{100};
  AppendDeletes(changes, first);
  AppendRows(changes, 1);
  changes.applied_ops = changes.ops.size();
  changes.ClearBufferedRows();
  const std::vector<uint64_t> second{200};
  AppendDeletes(changes, second);
  AppendRows(changes, 1);
  std::vector<uint64_t> applied;
  std::vector<int64_t> rows;
  changes.Visit(
    changes.applied_ops,
    [&](duckdb::DataChunk& chunk) {
      for (const auto row : Rows(chunk)) {
        rows.emplace_back(row);
      }
    },
    [&](LocalTableChangesEntry::Op& op) {
      applied.push_back(Positions(op.Removal()).front());
    });
  EXPECT_EQ(applied, (std::vector<uint64_t>{200}));
  EXPECT_EQ(rows, (std::vector<int64_t>{1}));
}

}  // namespace
