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
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <span>
#include <utility>
#include <vector>

#include "connector/row_position.h"
#include "search/search_table_changes.h"

namespace {

using sdb::connector::FillRowPositions;
using sdb::connector::MakeRowPosition;
using sdb::search::LocalTableChangesEntry;

duckdb::Vector MakePositions(std::span<const uint64_t> values) {
  duckdb::Vector vector{duckdb::LogicalType::UBIGINT};
  std::copy(values.begin(), values.end(),
            duckdb::FlatVector::GetDataMutable<uint64_t>(vector));
  duckdb::FlatVector::SetSize(vector, values.size());
  return vector;
}

void AppendDeletes(LocalTableChangesEntry& changes,
                   std::span<const uint64_t> positions) {
  changes.AppendDeletes(MakePositions(positions));
}

TEST(SearchTableChanges, DeletesShareAnOpUntilRowsOrAFlushComeBetween) {
  LocalTableChangesEntry changes;
  const std::vector<uint64_t> first{7, 8, 9};
  const std::vector<uint64_t> second{1, 2};
  AppendDeletes(changes, first);
  AppendDeletes(changes, second);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_EQ(changes.ops[0].deletes.starts, (std::vector<uint64_t>{7, 1}));
  EXPECT_EQ(changes.ops[0].deletes.counts, (std::vector<uint64_t>{3, 2}));

  changes.pk_segments.push_back({.base = 100, .count = 2});
  const std::vector<uint64_t> third{3};
  AppendDeletes(changes, third);
  ASSERT_EQ(changes.ops.size(), 2);
  EXPECT_EQ(changes.ops[1].band_watermark, 1);

  changes.applied_ops = changes.ops.size();
  const std::vector<uint64_t> fourth{4};
  AppendDeletes(changes, fourth);
  ASSERT_EQ(changes.ops.size(), 3);
  EXPECT_EQ(changes.ops[2].deletes.starts, (std::vector<uint64_t>{4}));
  EXPECT_EQ(changes.ops[2].deletes.counts, (std::vector<uint64_t>{1}));
}

TEST(SearchTableChanges, EmptyDeleteAddsNoOp) {
  LocalTableChangesEntry changes;
  AppendDeletes(changes, {});
  EXPECT_TRUE(changes.ops.empty());
}

TEST(SearchTableChanges, RunsContinueAcrossDeleteChunks) {
  LocalTableChangesEntry changes;
  const std::vector<uint64_t> first{11, 12};
  const std::vector<uint64_t> second{13, 14};
  AppendDeletes(changes, first);
  AppendDeletes(changes, second);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_EQ(changes.ops[0].deletes.starts, (std::vector<uint64_t>{11}));
  EXPECT_EQ(changes.ops[0].deletes.counts, (std::vector<uint64_t>{4}));
}

TEST(SearchTableChanges, RunsStopAtTheEndOfASegment) {
  LocalTableChangesEntry changes;
  const auto last = irs::doc_limits::eof() - 1;
  const std::vector<uint64_t> positions{
    MakeRowPosition(3, last - 1), MakeRowPosition(3, last),
    MakeRowPosition(4, irs::doc_limits::min())};
  AppendDeletes(changes, positions);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_EQ(
    changes.ops[0].deletes.starts,
    (std::vector<uint64_t>{MakeRowPosition(3, last - 1),
                           MakeRowPosition(4, irs::doc_limits::min())}));
  EXPECT_EQ(changes.ops[0].deletes.counts, (std::vector<uint64_t>{2, 1}));
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
  const duckdb::Vector positions{duckdb::Value(duckdb::LogicalType::UBIGINT),
                                 duckdb::count_t(3)};
  EXPECT_THROW(changes.AppendDeletes(positions), irs::SqlException);
  EXPECT_TRUE(changes.ops.empty());
}

TEST(SearchTableChanges, DeletesReadConstantVectors) {
  LocalTableChangesEntry changes;
  const auto position = MakeRowPosition(1, 5);
  const duckdb::Vector positions{duckdb::Value::UBIGINT(position),
                                 duckdb::count_t(3)};
  changes.AppendDeletes(positions);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_EQ(changes.ops[0].deletes.starts,
            (std::vector<uint64_t>{position, position, position}));
  EXPECT_EQ(changes.ops[0].deletes.counts, (std::vector<uint64_t>{1, 1, 1}));
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
    changes.ops[0].deletes.starts,
    (std::vector<uint64_t>{MakeRowPosition(2, 10), MakeRowPosition(2, 4),
                           MakeRowPosition(2, 4)}));
  EXPECT_EQ(changes.ops[0].deletes.counts, (std::vector<uint64_t>{1, 1, 1}));
}

TEST(SearchTableChanges, DeletesReadFilledPositionsAsOneRun) {
  LocalTableChangesEntry changes;
  duckdb::Vector positions{duckdb::LogicalType::UBIGINT};
  FillRowPositions(
    6, 4, [](duckdb::idx_t i) { return static_cast<irs::doc_id_t>(20 + i); },
    positions);
  changes.AppendDeletes(positions);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_EQ(changes.ops[0].deletes.starts,
            (std::vector<uint64_t>{MakeRowPosition(6, 20)}));
  EXPECT_EQ(changes.ops[0].deletes.counts, (std::vector<uint64_t>{4}));
}

TEST(SearchTableChanges, TruncateDropsTheDeletesBeforeIt) {
  LocalTableChangesEntry changes;
  const std::vector<uint64_t> positions{11, 12};
  AppendDeletes(changes, positions);
  changes.AppendTruncate(false);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_TRUE(changes.ops[0].IsTruncate());
  EXPECT_TRUE(changes.ops[0].deletes.starts.empty());
  AppendDeletes(changes, positions);
  ASSERT_EQ(changes.ops.size(), 2);
  EXPECT_FALSE(changes.ops[1].IsTruncate());
  EXPECT_EQ(changes.ops[1].deletes.counts, (std::vector<uint64_t>{2}));
}

TEST(SearchTableChanges, VisitAppliesEveryOpInOrderWithoutRows) {
  LocalTableChangesEntry changes;
  for (const uint64_t first : {100, 200, 300}) {
    const std::vector<uint64_t> positions{first, first + 1};
    AppendDeletes(changes, positions);
    changes.pk_segments.push_back({.base = 0, .count = 0});
  }
  ASSERT_EQ(changes.ops.size(), 3);
  std::vector<uint64_t> applied;
  changes.Visit(
    0, [](duckdb::DataChunk&, uint64_t) { FAIL(); },
    [&](LocalTableChangesEntry::Op& op) {
      applied.push_back(op.deletes.starts.front());
      EXPECT_EQ(op.deletes.counts, (std::vector<uint64_t>{2}));
    });
  EXPECT_EQ(applied, (std::vector<uint64_t>{100, 200, 300}));
}

TEST(SearchTableChanges, VisitStartsAtTheGivenOp) {
  LocalTableChangesEntry changes;
  for (const uint64_t first : {100, 200}) {
    const std::vector<uint64_t> positions{first};
    AppendDeletes(changes, positions);
    changes.pk_segments.push_back({.base = 0, .count = 0});
  }
  std::vector<uint64_t> applied;
  changes.Visit(
    1, [](duckdb::DataChunk&, uint64_t) { FAIL(); },
    [&](LocalTableChangesEntry::Op& op) {
      applied.push_back(op.deletes.starts.front());
    });
  EXPECT_EQ(applied, (std::vector<uint64_t>{200}));
}

}  // namespace
