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

#include "search/search_shard_wal.h"

#include <absl/strings/str_format.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace sdb::search {
namespace {

duckdb::vector<duckdb::LogicalType> IntType() {
  return {duckdb::LogicalType::INTEGER};
}

void FillIntChunk(duckdb::DataChunk& chunk, duckdb::Allocator& alloc,
                  const std::vector<int32_t>& vals) {
  chunk.Initialize(alloc, IntType());
  for (duckdb::idx_t i = 0; i < vals.size(); ++i) {
    chunk.data[0].SetValue(i, duckdb::Value::INTEGER(vals[i]));
  }
  chunk.SetCardinality(vals.size());
}

std::unique_ptr<duckdb::ColumnDataCollection> MakeIntCdc(
  duckdb::Allocator& alloc, const std::vector<int32_t>& vals) {
  auto cdc = std::make_unique<duckdb::ColumnDataCollection>(alloc, IntType());
  duckdb::DataChunk chunk;
  FillIntChunk(chunk, alloc, vals);
  cdc->Append(chunk);
  return cdc;
}

std::string Hex16(uint64_t v) { return absl::StrFormat("%016x", v); }

// Accumulates everything Recover() replays.
struct Collected {
  // (tick, the int column's values) per replayed chunk, in replay order.
  std::vector<std::pair<uint64_t, std::vector<int32_t>>> chunks;
  std::vector<uint64_t> last_col_ids;
};

SearchShardWal::ReplayCallback MakeCollector(Collected& out) {
  return [&out](uint64_t tick, SearchShardWal::ColumnIds cols,
                duckdb::DataChunk& chunk) {
    std::vector<int32_t> vals;
    for (duckdb::idx_t i = 0; i < chunk.size(); ++i) {
      vals.push_back(chunk.GetValue(0, i).GetValue<int32_t>());
    }
    out.chunks.emplace_back(tick, std::move(vals));
    out.last_col_ids.assign(cols.begin(), cols.end());
  };
}

class SearchShardWalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    _fs = duckdb::FileSystem::CreateLocal();
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    _dir = std::filesystem::path(::testing::TempDir()) /
           absl::StrFormat("swal_%s_%s", info->test_suite_name(), info->name());
    std::filesystem::remove_all(_dir);
  }
  void TearDown() override { std::filesystem::remove_all(_dir); }

  duckdb::FileSystem& Fs() { return *_fs; }
  duckdb::Allocator& Alloc() { return duckdb::Allocator::DefaultAllocator(); }

  std::filesystem::path SegPath(uint64_t first_tick) const {
    return _dir / (Hex16(first_tick) + ".swal");
  }
  std::filesystem::path ChunkPath(uint64_t seg_id) const {
    return _dir / "chunks" / (Hex16(seg_id) + ".swchunk");
  }

  std::unique_ptr<duckdb::FileSystem> _fs;
  std::filesystem::path _dir;
};

TEST_F(SearchShardWalTest, InlineRoundTrip) {
  std::vector<uint64_t> cols{7, 8};
  {
    SearchShardWal wal(Fs(), _dir);
    auto cdc = MakeIntCdc(Alloc(), {10, 20, 30});
    EXPECT_EQ(wal.AppendCommit(SearchShardWal::InlineCommit{cols, *cdc}), 1u);
  }
  // Reopen (simulated restart) and replay everything (committed_tick = 0).
  Collected got;
  SearchShardWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(0, MakeCollector(got)), 1u);

  ASSERT_EQ(got.chunks.size(), 1u);
  EXPECT_EQ(got.chunks[0].first, 1u);
  EXPECT_EQ(got.chunks[0].second, (std::vector<int32_t>{10, 20, 30}));
  EXPECT_EQ(got.last_col_ids, cols);
}

TEST_F(SearchShardWalTest, ReferenceRoundTrip) {
  std::vector<uint64_t> cols{9};
  {
    SearchShardWal wal(Fs(), _dir);
    auto cw1 = wal.NewChunkWriter();
    auto cw2 = wal.NewChunkWriter();
    EXPECT_EQ(cw1.SegId(), 1u);
    EXPECT_EQ(cw2.SegId(), 2u);
    // Build materialized chunks (via CDC FetchChunk) -- the shape the real
    // pipeline produces, where per-vector size() matches the cardinality.
    auto cdc1 = MakeIntCdc(Alloc(), {1, 2});
    duckdb::DataChunk c1;
    c1.Initialize(Alloc(), IntType());
    cdc1->FetchChunk(0, c1);
    cw1.Append(c1);
    cw1.Finish();
    auto cdc2 = MakeIntCdc(Alloc(), {3, 4, 5});
    duckdb::DataChunk c2;
    c2.Initialize(Alloc(), IntType());
    cdc2->FetchChunk(0, c2);
    cw2.Append(c2);
    cw2.Finish();
    std::vector<uint64_t> segs{cw1.SegId(), cw2.SegId()};
    EXPECT_EQ(wal.AppendCommit(SearchShardWal::ReferenceCommit{cols, segs}), 1u);
  }
  Collected got;
  SearchShardWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(0, MakeCollector(got)), 1u);

  ASSERT_EQ(got.chunks.size(), 2u);
  EXPECT_EQ(got.chunks[0].first, 1u);
  EXPECT_EQ(got.chunks[0].second, (std::vector<int32_t>{1, 2}));
  EXPECT_EQ(got.chunks[1].second, (std::vector<int32_t>{3, 4, 5}));
  EXPECT_EQ(got.last_col_ids, cols);
}

TEST_F(SearchShardWalTest, RecoverySkipsConsumed) {
  std::vector<uint64_t> cols{1};
  {
    SearchShardWal wal(Fs(), _dir);
    auto a = MakeIntCdc(Alloc(), {10});
    auto b = MakeIntCdc(Alloc(), {20});
    EXPECT_EQ(wal.AppendCommit(SearchShardWal::InlineCommit{cols, *a}), 1u);
    EXPECT_EQ(wal.AppendCommit(SearchShardWal::InlineCommit{cols, *b}), 2u);
  }
  // committed_tick = 1 -> tick 1 already durable in iresearch, only 2 replays.
  Collected got;
  SearchShardWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(1, MakeCollector(got)), 2u);

  ASSERT_EQ(got.chunks.size(), 1u);
  EXPECT_EQ(got.chunks[0].first, 2u);
  EXPECT_EQ(got.chunks[0].second, (std::vector<int32_t>{20}));
}

TEST_F(SearchShardWalTest, CounterRecovery) {
  std::vector<uint64_t> cols{1};
  {
    SearchShardWal wal(Fs(), _dir);
    auto cw = wal.NewChunkWriter();  // seg_id 1, referenced below -> survives
    auto cdc = MakeIntCdc(Alloc(), {5});
    duckdb::DataChunk c;
    c.Initialize(Alloc(), IntType());
    cdc->FetchChunk(0, c);
    cw.Append(c);
    cw.Finish();
    std::vector<uint64_t> segs{cw.SegId()};
    EXPECT_EQ(wal.AppendCommit(SearchShardWal::ReferenceCommit{cols, segs}), 1u);
  }
  Collected got;
  SearchShardWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(0, MakeCollector(got)), 1u);

  // tick + seg_id counters continue past the recovered max.
  auto cdc = MakeIntCdc(Alloc(), {6});
  EXPECT_EQ(wal2.AppendCommit(SearchShardWal::InlineCommit{cols, *cdc}), 2u);
  EXPECT_EQ(wal2.NewChunkWriter().SegId(), 2u);
}

TEST_F(SearchShardWalTest, TornTailIgnored) {
  std::vector<uint64_t> cols{1};
  {
    SearchShardWal wal(Fs(), _dir);
    auto a = MakeIntCdc(Alloc(), {10});
    auto b = MakeIntCdc(Alloc(), {20});
    wal.AppendCommit(SearchShardWal::InlineCommit{cols, *a});  // tick 1
    wal.AppendCommit(SearchShardWal::InlineCommit{cols, *b});  // tick 2
  }
  // Simulate a torn write: append a partial (sub-header) tail to the segment
  // whose first record's tick is 1.
  {
    std::ofstream f(SegPath(1), std::ios::binary | std::ios::app);
    const char junk[7] = {};
    f.write(junk, sizeof(junk));
  }
  Collected got;
  SearchShardWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(0, MakeCollector(got)), 2u);

  ASSERT_EQ(got.chunks.size(), 2u);
  EXPECT_EQ(got.chunks[0].second, (std::vector<int32_t>{10}));
  EXPECT_EQ(got.chunks[1].second, (std::vector<int32_t>{20}));
}

TEST_F(SearchShardWalTest, RefreshCommitGcDeletesConsumed) {
  std::vector<uint64_t> cols{1};
  SearchShardWal wal(Fs(), _dir);
  auto cw = wal.NewChunkWriter();  // seg_id 1
  duckdb::DataChunk c;
  FillIntChunk(c, Alloc(), {1});
  cw.Append(c);
  cw.Finish();
  std::vector<uint64_t> segs{cw.SegId()};
  EXPECT_EQ(wal.AppendCommit(SearchShardWal::ReferenceCommit{cols, segs}), 1u);
  auto an_inline = MakeIntCdc(Alloc(), {2});
  EXPECT_EQ(wal.AppendCommit(SearchShardWal::InlineCommit{cols, *an_inline}), 2u);

  EXPECT_TRUE(std::filesystem::exists(SegPath(1)));
  EXPECT_TRUE(std::filesystem::exists(ChunkPath(1)));

  wal.OnRefreshCommit(2);  // ticks 1,2 durable in iresearch -> fully consumed

  EXPECT_FALSE(std::filesystem::exists(SegPath(1)));
  EXPECT_FALSE(std::filesystem::exists(ChunkPath(1)));

  // The next commit opens a fresh segment named by its tick (3).
  auto next = MakeIntCdc(Alloc(), {3});
  EXPECT_EQ(wal.AppendCommit(SearchShardWal::InlineCommit{cols, *next}), 3u);
  EXPECT_TRUE(std::filesystem::exists(SegPath(3)));
}

TEST_F(SearchShardWalTest, RefreshCommitKeepsLiveSegment) {
  std::vector<uint64_t> cols{1};
  SearchShardWal wal(Fs(), _dir);
  auto a = MakeIntCdc(Alloc(), {10});
  wal.AppendCommit(SearchShardWal::InlineCommit{cols, *a});  // tick 1

  // committed_tick = 0: nothing is durable in iresearch yet -> keep the segment.
  wal.OnRefreshCommit(0);
  EXPECT_TRUE(std::filesystem::exists(SegPath(1)));
}

TEST_F(SearchShardWalTest, OrphanChunkSweptOnRecover) {
  {
    SearchShardWal wal(Fs(), _dir);
    auto cw = wal.NewChunkWriter();  // seg_id 1, never referenced by a commit
    duckdb::DataChunk c;
    FillIntChunk(c, Alloc(), {1});
    cw.Append(c);
    cw.Finish();
  }
  EXPECT_TRUE(std::filesystem::exists(ChunkPath(1)));

  Collected got;
  SearchShardWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(0, MakeCollector(got)), 0u);

  EXPECT_TRUE(got.chunks.empty());
  EXPECT_FALSE(std::filesystem::exists(ChunkPath(1)));
  // seg_id counter resets (orphan gone) -> next chunk reuses id 1.
  EXPECT_EQ(wal2.NewChunkWriter().SegId(), 1u);
}

}  // namespace
}  // namespace sdb::search
