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

#include <duckdb/common/allocator.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/value.hpp>
#include <vector>

#include "connector/primary_key.h"
#include "engine_context.h"
#include "gtest/gtest.h"
#include "search/wal_tail_keys.h"

namespace sdb::search {
namespace {

constexpr duckdb::idx_t kTable = 42;

connector::primary_key::KeySlot Slot() {
  return {.input_col_idx = 0,
          .name = "id",
          .type = duckdb::LogicalType::INTEGER,
          .column = connector::ColumnId{1},
          .layout = irs::DictLayout::Numeric};
}

void MakeChunk(duckdb::DataChunk& chunk, std::vector<int32_t> ids) {
  chunk.Initialize(
    duckdb::Allocator::DefaultAllocator(),
    duckdb::vector<duckdb::LogicalType>{duckdb::LogicalType::INTEGER});
  for (duckdb::idx_t row = 0; row != ids.size(); ++row) {
    chunk.SetValue(0, row, duckdb::Value::INTEGER(ids[row]));
  }
  chunk.SetCardinality(ids.size());
}

void Insert(WalTailKeyCollector& collector, std::vector<int32_t> ids) {
  duckdb::DataChunk chunk;
  MakeChunk(chunk, std::move(ids));
  collector.ReplayInsert(sdb::test::EngineContext(), chunk);
}

// A batch's entries are read before the reader knows whether its transaction
// committed, so a batch that never decided leaves nothing behind.
TEST(WalTailKeys, AnUncommittedBatchIsDropped) {
  const std::vector<connector::primary_key::KeySlot> slots{Slot()};
  WalTailKeyCollector collector{kTable, slots};

  Insert(collector, {1, 2});
  collector.ReplayAdoptSegments({"_1.0.sm"});
  collector.EndBatch(false);

  EXPECT_TRUE(collector.Keys().Empty());
}

TEST(WalTailKeys, ACommittedBatchIsKept) {
  const std::vector<connector::primary_key::KeySlot> slots{Slot()};
  WalTailKeyCollector collector{kTable, slots};

  Insert(collector, {1, 2});
  collector.ReplayAdoptSegments({"_1.0.sm"});
  collector.EndBatch(true);
  Insert(collector, {3});
  collector.EndBatch(true);

  const auto& keys = collector.Keys();
  EXPECT_EQ(3, keys.keys.size());
  EXPECT_EQ(1, keys.segments.size());
  EXPECT_FALSE(keys.truncated);
}

// A committed TRUNCATE kills what the tail said before it, and what the
// published reader holds with it, so the barrier travels with the result.
TEST(WalTailKeys, ACommittedTruncateClearsWhatCameBefore) {
  const std::vector<connector::primary_key::KeySlot> slots{Slot()};
  WalTailKeyCollector collector{kTable, slots};

  Insert(collector, {1, 2});
  collector.EndBatch(true);
  ASSERT_EQ(2, collector.Keys().keys.size());

  collector.ReplayTruncate();
  Insert(collector, {7});
  collector.EndBatch(true);

  const auto& keys = collector.Keys();
  EXPECT_TRUE(keys.truncated);
  // The truncate's own batch keeps the rows it wrote after it.
  EXPECT_EQ(1, keys.keys.size());
}

// The barrier is only a barrier once its transaction commits: a TRUNCATE that
// rolled back leaves every earlier batch standing.
TEST(WalTailKeys, AnUncommittedTruncateIsNotABarrier) {
  const std::vector<connector::primary_key::KeySlot> slots{Slot()};
  WalTailKeyCollector collector{kTable, slots};

  Insert(collector, {1, 2});
  collector.EndBatch(true);

  collector.ReplayTruncate();
  Insert(collector, {7});
  collector.EndBatch(false);

  const auto& keys = collector.Keys();
  EXPECT_FALSE(keys.truncated);
  EXPECT_EQ(2, keys.keys.size());
}

// Entries of a table the read passes over never reach this collector, which is
// what `Resolve` is for; it answers for one table and no other.
TEST(WalTailKeys, ItResolvesOnlyItsOwnTable) {
  const std::vector<connector::primary_key::KeySlot> slots{Slot()};
  WalTailKeyCollector collector{kTable, slots};

  EXPECT_EQ(&collector, collector.Resolve(kTable).get());
  EXPECT_EQ(nullptr, collector.Resolve(kTable + 1).get());
}

// Without key columns the collector still answers for what a certifier needs
// besides keys: the segments a committed transaction flushed, and the barrier.
TEST(WalTailKeys, WithoutKeyColumnsItStillCollectsSegments) {
  WalTailKeyCollector collector{kTable, {}};

  Insert(collector, {1, 2});
  collector.ReplayAdoptSegments({"_2.0.sm"});
  collector.EndBatch(true);

  const auto& keys = collector.Keys();
  EXPECT_TRUE(keys.keys.empty());
  EXPECT_EQ(1, keys.segments.size());
}

}  // namespace
}  // namespace sdb::search
