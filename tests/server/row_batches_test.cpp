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
#include <iresearch/index/doc_removal.hpp>
#include <utility>
#include <vector>

#include "search/row_batches.h"

namespace {

using sdb::search::RowBatch;
using sdb::search::RowBatches;

struct Found {
  std::vector<std::pair<uint32_t, irs::doc_id_t>> docs;
  std::vector<int64_t> missing;
};

Found Find(const RowBatches& batches, std::vector<int64_t> rows) {
  irs::DocRemovalBuilder builder;
  Found found;
  batches.Find(rows, builder, found.missing);
  const auto removal = std::move(builder).Finish();
  for (size_t i = 0; i < removal.segments.size(); ++i) {
    for (const auto doc : removal.Docs(i).Docs()) {
      found.docs.emplace_back(removal.segments[i], doc);
    }
  }
  return found;
}

std::vector<RowBatch> Batch(int64_t begin, int64_t end, uint32_t segment,
                            irs::doc_id_t doc,
                            std::vector<uint32_t> offsets = {}) {
  std::vector<RowBatch> batches;
  batches.push_back({.begin = begin,
                     .end = end,
                     .segment = segment,
                     .doc = doc,
                     .offsets = std::move(offsets)});
  return batches;
}

using Docs = std::vector<std::pair<uint32_t, irs::doc_id_t>>;
using Rows = std::vector<int64_t>;

TEST(RowBatchesTest, AContiguousBatchNamesEachRowByItsOffset) {
  RowBatches batches;
  batches.Add(Batch(100, 110, 3, 1), 1);
  const auto found = Find(batches, {100, 105, 109});
  EXPECT_EQ((Docs{{3, 1}, {3, 6}, {3, 10}}), found.docs);
  EXPECT_TRUE(found.missing.empty());
}

TEST(RowBatchesTest, RowsOutsideEveryBatchAreMissing) {
  RowBatches batches;
  batches.Add(Batch(100, 110, 3, 1), 1);
  batches.Add(Batch(130, 140, 4, 1), 1);
  const auto found = Find(batches, {99, 110, 120, 140, 1000});
  EXPECT_TRUE(found.docs.empty());
  EXPECT_EQ((Rows{99, 110, 120, 140, 1000}), found.missing);
}

TEST(RowBatchesTest, ASparseBatchNamesOnlyTheRowsItWrote) {
  RowBatches batches;
  batches.Add(Batch(10, 20, 5, 7, {0, 2, 5}), 1);
  const auto found = Find(batches, {10, 11, 12, 14, 15, 18, 20});
  EXPECT_EQ((Docs{{5, 7}, {5, 8}, {5, 9}}), found.docs);
  EXPECT_EQ((Rows{11, 14, 18, 20}), found.missing);
}

TEST(RowBatchesTest, APruneDropsTheBatchesOfCommittedFlushesOnly) {
  RowBatches batches;
  batches.Add(Batch(10, 20, 1, 1), 2);
  batches.Add(Batch(20, 30, 2, 1), 4);
  batches.Prune(3);
  auto found = Find(batches, {15, 25});
  EXPECT_EQ((Docs{{2, 6}}), found.docs);
  EXPECT_EQ(Rows{15}, found.missing);
  batches.Prune(4);
  found = Find(batches, {25});
  EXPECT_TRUE(found.docs.empty());
}

TEST(RowBatchesTest, ABatchWrittenAgainOutlivesThePruneOfTheOldOne) {
  RowBatches batches;
  batches.Add(Batch(10, 20, 1, 1), 1);
  batches.Add(Batch(10, 20, 2, 1), 3);
  batches.Prune(1);
  auto found = Find(batches, {10});
  EXPECT_EQ((Docs{{2, 1}}), found.docs);
  batches.Prune(3);
  found = Find(batches, {10});
  EXPECT_EQ(Rows{10}, found.missing);
}

TEST(RowBatchesTest, APruneSkipsABatchAnEarlierEntryAlreadyDropped) {
  RowBatches batches;
  batches.Add(Batch(10, 20, 1, 1), 1);
  batches.Add(Batch(10, 20, 2, 1), 2);
  batches.Prune(2);
  const auto found = Find(batches, {10});
  EXPECT_TRUE(found.docs.empty());
  EXPECT_EQ(Rows{10}, found.missing);
}

}  // namespace
