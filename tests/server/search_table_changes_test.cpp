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
#include <utility>
#include <vector>

#include "search/search_table_changes.h"

namespace {

using sdb::search::LocalTableChangesEntry;
using sdb::search::RowRange;

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
  sdb::search::AppendRowRanges(ranges, rows);
  EXPECT_EQ(Pairs(ranges), (std::vector<std::pair<int64_t, int64_t>>{
                             {5, 8}, {9, 11}, {3, 5}, {11, 12}}));
}

TEST(SearchTableChanges, NormalizeRowRangesSortsAndMergesAdjacent) {
  std::vector<RowRange> ranges{{9, 11}, {5, 8}, {11, 12}, {3, 5}, {20, 21}};
  sdb::search::NormalizeRowRanges(ranges);
  EXPECT_EQ(Pairs(ranges), (std::vector<std::pair<int64_t, int64_t>>{
                             {3, 8}, {9, 12}, {20, 21}}));
  sdb::search::NormalizeRowRanges(ranges);
  EXPECT_EQ(Pairs(ranges), (std::vector<std::pair<int64_t, int64_t>>{
                             {3, 8}, {9, 12}, {20, 21}}));
}

TEST(SearchTableChanges, NormalizeRowRangesEmpty) {
  std::vector<RowRange> ranges;
  sdb::search::NormalizeRowRanges(ranges);
  EXPECT_TRUE(ranges.empty());
}

TEST(SearchTableChanges, DeletesShareAnOpUntilRowsOrAFlushComeBetween) {
  LocalTableChangesEntry changes;
  const std::vector<int64_t> first{10, 11, 12};
  const std::vector<int64_t> second{0, 1};
  changes.AppendDeletes(first);
  changes.AppendDeletes(second);
  ASSERT_EQ(changes.ops.size(), 1);
  EXPECT_EQ(Pairs(changes.ops[0].delete_ranges),
            (std::vector<std::pair<int64_t, int64_t>>{{10, 13}, {0, 2}}));

  changes.pk_segments.push_back({.base = 100, .count = 2});
  const std::vector<int64_t> third{20};
  changes.AppendDeletes(third);
  ASSERT_EQ(changes.ops.size(), 2);
  EXPECT_EQ(changes.ops[1].band_watermark, 1);

  changes.applied_ops = changes.ops.size();
  const std::vector<int64_t> fourth{21};
  changes.AppendDeletes(fourth);
  ASSERT_EQ(changes.ops.size(), 3);
  EXPECT_EQ(Pairs(changes.ops[2].delete_ranges),
            (std::vector<std::pair<int64_t, int64_t>>{{21, 22}}));
}

}  // namespace
