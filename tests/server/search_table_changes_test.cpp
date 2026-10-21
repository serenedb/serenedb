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
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <span>
#include <utility>
#include <vector>

#include "connector/row_position.h"
#include "search/search_table_changes.h"

namespace {

using sdb::connector::FillRowPositions;
using sdb::connector::kNoRowPosition;
using sdb::connector::MakeRowPosition;
using sdb::search::LocalTableChangesEntry;
using sdb::search::RowRange;

template<typename T>
duckdb::Vector MakeFlat(duckdb::LogicalType type, std::span<const T> values) {
  duckdb::Vector vector{std::move(type)};
  std::copy(values.begin(), values.end(),
            duckdb::FlatVector::GetDataMutable<T>(vector));
  duckdb::FlatVector::SetSize(vector, values.size());
  return vector;
}

void AppendDeletes(LocalTableChangesEntry& changes,
                   std::span<const int64_t> rows,
                   std::span<const uint64_t> positions) {
  changes.AppendDeletes(MakeFlat(duckdb::LogicalType::BIGINT, rows),
                        MakeFlat(duckdb::LogicalType::UBIGINT, positions));
}

std::vector<std::pair<int64_t, int64_t>> Pairs(
  const std::vector<RowRange>& ranges) {
  std::vector<std::pair<int64_t, int64_t>> pairs;
  for (const auto& range : ranges) {
    pairs.emplace_back(range.begin, range.end);
  }
  return pairs;
}

TEST(SearchTableChanges, AppendRowRangesJoinsConsecutiveRowids) {
  std::vector<RowRange> ranges;
  const std::vector<int64_t> rows{5, 6, 7, 9, 10, 3, 4, 11};
  irs::AppendKeyRanges(ranges, rows);
  EXPECT_EQ(Pairs(ranges), (std::vector<std::pair<int64_t, int64_t>>{
                             {5, 8}, {9, 11}, {3, 5}, {11, 12}}));
}

TEST(SearchTableChanges, NormalizeRowRangesSortsAndMergesAdjacent) {
  std::vector<RowRange> ranges{{9, 11}, {5, 8}, {11, 12}, {3, 5}, {20, 21}};
  irs::NormalizeKeyRanges(ranges);
  EXPECT_EQ(Pairs(ranges), (std::vector<std::pair<int64_t, int64_t>>{
                             {3, 8}, {9, 12}, {20, 21}}));
  irs::NormalizeKeyRanges(ranges);
  EXPECT_EQ(Pairs(ranges), (std::vector<std::pair<int64_t, int64_t>>{
                             {3, 8}, {9, 12}, {20, 21}}));
}

TEST(SearchTableChanges, NormalizeRowRangesEmpty) {
  std::vector<RowRange> ranges;
  irs::NormalizeKeyRanges(ranges);
  EXPECT_TRUE(ranges.empty());
}

TEST(SearchTableChanges, DeletesShareAnOpUntilRowsOrAFlushComeBetween) {
  LocalTableChangesEntry changes;
  const std::vector<int64_t> first{10, 11, 12};
  const std::vector<uint64_t> first_positions{7, 8, 9};
  const std::vector<int64_t> second{0, 1};
  const std::vector<uint64_t> second_positions{1, 2};
  AppendDeletes(changes, first, first_positions);
  AppendDeletes(changes, second, second_positions);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_EQ(changes.ops[0].deletes.starts, (std::vector<int64_t>{10, 0}));
  EXPECT_EQ(changes.ops[0].deletes.counts, (std::vector<uint64_t>{3, 2}));
  EXPECT_EQ(changes.ops[0].deletes.positions, (std::vector<uint64_t>{7, 1}));

  changes.pk_segments.push_back({.base = 100, .count = 2});
  const std::vector<int64_t> third{20};
  const std::vector<uint64_t> third_positions{3};
  AppendDeletes(changes, third, third_positions);
  ASSERT_EQ(changes.ops.size(), 2);
  EXPECT_EQ(changes.ops[1].band_watermark, 1);

  changes.applied_ops = changes.ops.size();
  const std::vector<int64_t> fourth{21};
  const std::vector<uint64_t> fourth_positions{4};
  AppendDeletes(changes, fourth, fourth_positions);
  ASSERT_EQ(changes.ops.size(), 3);
  EXPECT_EQ(changes.ops[2].deletes.starts, (std::vector<int64_t>{21}));
  EXPECT_EQ(changes.ops[2].deletes.counts, (std::vector<uint64_t>{1}));
  EXPECT_EQ(changes.ops[2].deletes.positions, (std::vector<uint64_t>{4}));
}

TEST(SearchTableChanges, EmptyDeleteAddsNoOp) {
  LocalTableChangesEntry changes;
  AppendDeletes(changes, {}, {});
  EXPECT_TRUE(changes.ops.empty());
}

TEST(SearchTableChanges, DeletesWithMismatchedPositionsFail) {
  LocalTableChangesEntry changes;
  const std::vector<int64_t> rows{1, 2};
  const std::vector<uint64_t> positions{1};
  EXPECT_DEATH(AppendDeletes(changes, rows, positions),
               "1 row positions for 2 deleted rows");
}

TEST(SearchTableChanges, RunsContinueAcrossDeleteChunks) {
  LocalTableChangesEntry changes;
  const std::vector<int64_t> first{1, 2};
  const std::vector<uint64_t> first_positions{11, 12};
  const std::vector<int64_t> second{3, 4};
  const std::vector<uint64_t> second_positions{13, 14};
  AppendDeletes(changes, first, first_positions);
  AppendDeletes(changes, second, second_positions);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_EQ(changes.ops[0].deletes.starts, (std::vector<int64_t>{1}));
  EXPECT_EQ(changes.ops[0].deletes.counts, (std::vector<uint64_t>{4}));
  EXPECT_EQ(changes.ops[0].deletes.positions, (std::vector<uint64_t>{11}));
}

TEST(SearchTableChanges, RowsWithoutPositionsJoinIntoRuns) {
  LocalTableChangesEntry changes;
  const std::vector<int64_t> rows{5, 6, 7, 9};
  const duckdb::Vector positions{duckdb::Value(duckdb::LogicalType::UBIGINT),
                                 duckdb::count_t(rows.size())};
  changes.AppendDeletes(MakeFlat<int64_t>(duckdb::LogicalType::BIGINT, rows),
                        positions);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_EQ(changes.ops[0].deletes.starts, (std::vector<int64_t>{5, 9}));
  EXPECT_EQ(changes.ops[0].deletes.counts, (std::vector<uint64_t>{3, 1}));
  EXPECT_EQ(changes.ops[0].deletes.positions,
            (std::vector<uint64_t>{kNoRowPosition, kNoRowPosition}));
}

TEST(SearchTableChanges, DeletesReadNullPositionsOfAFlatVector) {
  LocalTableChangesEntry changes;
  const std::vector<int64_t> rows{100, 101, 102};
  const std::vector<uint64_t> places{
    MakeRowPosition(4, 1), MakeRowPosition(4, 2), MakeRowPosition(4, 3)};
  auto positions = MakeFlat<uint64_t>(duckdb::LogicalType::UBIGINT, places);
  duckdb::FlatVector::ValidityMutable(positions).SetInvalid(1);
  changes.AppendDeletes(MakeFlat<int64_t>(duckdb::LogicalType::BIGINT, rows),
                        positions);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_EQ(changes.ops[0].deletes.starts,
            (std::vector<int64_t>{100, 101, 102}));
  EXPECT_EQ(changes.ops[0].deletes.counts, (std::vector<uint64_t>{1, 1, 1}));
  EXPECT_EQ(changes.ops[0].deletes.positions,
            (std::vector<uint64_t>{MakeRowPosition(4, 1), kNoRowPosition,
                                   MakeRowPosition(4, 3)}));
}

TEST(SearchTableChanges, DeletesReadConstantVectors) {
  LocalTableChangesEntry changes;
  const duckdb::Vector rowids{duckdb::Value::BIGINT(42), duckdb::count_t(3)};
  const duckdb::Vector positions{duckdb::Value(duckdb::LogicalType::UBIGINT),
                                 duckdb::count_t(3)};
  changes.AppendDeletes(rowids, positions);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_EQ(changes.ops[0].deletes.starts, (std::vector<int64_t>{42, 42, 42}));
  EXPECT_EQ(changes.ops[0].deletes.counts, (std::vector<uint64_t>{1, 1, 1}));
  EXPECT_EQ(
    changes.ops[0].deletes.positions,
    (std::vector<uint64_t>{kNoRowPosition, kNoRowPosition, kNoRowPosition}));
}

TEST(SearchTableChanges, DeletesReadDictionaryVectors) {
  LocalTableChangesEntry changes;
  std::vector<int64_t> rows;
  std::vector<uint64_t> places;
  for (uint32_t i = 0; i < 10; ++i) {
    rows.emplace_back(1000 + i);
    places.emplace_back(MakeRowPosition(2, 1 + i));
  }
  auto rowids = MakeFlat<int64_t>(duckdb::LogicalType::BIGINT, rows);
  auto positions = MakeFlat<uint64_t>(duckdb::LogicalType::UBIGINT, places);
  duckdb::SelectionVector sel{3};
  sel.set_index(0, 9);
  sel.set_index(1, 3);
  sel.set_index(2, 3);
  rowids.Slice(sel, 3);
  positions.Slice(sel, 3);
  changes.AppendDeletes(rowids, positions);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_EQ(changes.ops[0].deletes.starts,
            (std::vector<int64_t>{1009, 1003, 1003}));
  EXPECT_EQ(changes.ops[0].deletes.counts, (std::vector<uint64_t>{1, 1, 1}));
  EXPECT_EQ(
    changes.ops[0].deletes.positions,
    (std::vector<uint64_t>{MakeRowPosition(2, 10), MakeRowPosition(2, 4),
                           MakeRowPosition(2, 4)}));
}

TEST(SearchTableChanges, DeletesReadSequencesAndFilledPositionsAsOneRun) {
  LocalTableChangesEntry changes;
  duckdb::Vector rowids{duckdb::LogicalType::BIGINT, nullptr};
  rowids.Sequence(500, 1, 4);
  duckdb::Vector positions{duckdb::LogicalType::UBIGINT};
  FillRowPositions(
    6, 4, [](duckdb::idx_t i) { return static_cast<irs::doc_id_t>(20 + i); },
    positions);
  changes.AppendDeletes(rowids, positions);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_EQ(changes.ops[0].deletes.starts, (std::vector<int64_t>{500}));
  EXPECT_EQ(changes.ops[0].deletes.counts, (std::vector<uint64_t>{4}));
  EXPECT_EQ(changes.ops[0].deletes.positions,
            (std::vector<uint64_t>{MakeRowPosition(6, 20)}));
}

TEST(SearchTableChanges, TruncateDropsTheDeletesBeforeIt) {
  LocalTableChangesEntry changes;
  const std::vector<int64_t> rows{1, 2};
  const std::vector<uint64_t> positions{11, 12};
  AppendDeletes(changes, rows, positions);
  changes.AppendTruncate(false);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_TRUE(changes.ops[0].IsTruncate());
  EXPECT_TRUE(changes.ops[0].deletes.starts.empty());
  AppendDeletes(changes, rows, positions);
  ASSERT_EQ(changes.ops.size(), 2);
  EXPECT_FALSE(changes.ops[1].IsTruncate());
  EXPECT_EQ(changes.ops[1].deletes.counts, (std::vector<uint64_t>{2}));
}

TEST(SearchTableChanges, VisitAppliesEveryOpInOrderWithoutRows) {
  LocalTableChangesEntry changes;
  for (const int64_t first : {100, 200, 300}) {
    const std::vector<int64_t> rows{first, first + 1};
    const std::vector<uint64_t> positions{1, 2};
    AppendDeletes(changes, rows, positions);
    changes.pk_segments.push_back({.base = 0, .count = 0});
  }
  ASSERT_EQ(changes.ops.size(), 3);
  std::vector<int64_t> applied;
  changes.Visit(
    0, [](duckdb::DataChunk&, uint64_t) { FAIL(); },
    [&](LocalTableChangesEntry::Op& op) {
      applied.push_back(op.deletes.starts.front());
      EXPECT_EQ(op.deletes.counts, (std::vector<uint64_t>{2}));
    });
  EXPECT_EQ(applied, (std::vector<int64_t>{100, 200, 300}));
}

TEST(SearchTableChanges, VisitStartsAtTheGivenOp) {
  LocalTableChangesEntry changes;
  for (const int64_t first : {100, 200}) {
    const std::vector<int64_t> rows{first};
    const std::vector<uint64_t> positions{1};
    AppendDeletes(changes, rows, positions);
    changes.pk_segments.push_back({.base = 0, .count = 0});
  }
  std::vector<int64_t> applied;
  changes.Visit(
    1, [](duckdb::DataChunk&, uint64_t) { FAIL(); },
    [&](LocalTableChangesEntry::Op& op) {
      applied.push_back(op.deletes.starts.front());
    });
  EXPECT_EQ(applied, (std::vector<int64_t>{200}));
}

}  // namespace
