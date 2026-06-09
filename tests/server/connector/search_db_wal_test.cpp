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

#include <absl/strings/str_format.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <deque>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <filesystem>
#include <fstream>
#include <memory>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "search/search_db_wal.h"

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

// Materialised chunk (via CDC FetchChunk) -- the shape the real pipeline
// produces, where per-vector size() matches the cardinality.
void FetchMaterialized(duckdb::DataChunk& out, duckdb::Allocator& alloc,
                       duckdb::ColumnDataCollection& cdc) {
  out.Initialize(alloc, IntType());
  cdc.FetchChunk(0, out);
}

std::string Hex16(uint64_t v) { return absl::StrFormat("%016x", v); }

// Accumulates everything Recover() replays.
struct Collected {
  // (tick, schema_id, table_id, values, pk_base) per replayed chunk, in order.
  std::vector<
    std::tuple<uint64_t, uint64_t, uint64_t, std::vector<int32_t>, uint64_t>>
    chunks;
};

SearchDbWal::ReplayCallback MakeCollector(Collected& out) {
  return [&out](uint64_t tick, uint64_t schema_id, uint64_t table_id,
                uint64_t pk_base, duckdb::DataChunk& chunk) {
    std::vector<int32_t> vals;
    for (duckdb::idx_t i = 0; i < chunk.size(); ++i) {
      vals.push_back(chunk.GetValue(0, i).GetValue<int32_t>());
    }
    out.chunks.emplace_back(tick, schema_id, table_id, std::move(vals),
                            pk_base);
  };
}

// Replay hooks: every shard exists and nothing is durable yet (committed 0).
SearchDbWal::ShardExistsFn AllExist() {
  return [](uint64_t, uint64_t) { return true; };
}
SearchDbWal::ShardCommittedFn CommittedAll(uint64_t tick) {
  return [tick](uint64_t) { return tick; };
}

class SearchDbWalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    _fs = duckdb::FileSystem::CreateLocal();
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    _dir =
      std::filesystem::path(::testing::TempDir()) /
      absl::StrFormat("sdbwal_%s_%s", info->test_suite_name(), info->name());
    std::filesystem::remove_all(_dir);
  }
  void TearDown() override { std::filesystem::remove_all(_dir); }

  duckdb::FileSystem& Fs() { return *_fs; }
  duckdb::Allocator& Alloc() { return duckdb::Allocator::DefaultAllocator(); }

  std::filesystem::path SegPath(uint64_t first_tick) const {
    return _dir / (Hex16(first_tick) + ".swal");
  }
  std::filesystem::path ChunkPath(uint64_t schema_id, uint64_t table_id,
                                  uint64_t seg_id) const {
    return _dir / "chunks" / std::to_string(schema_id) /
           std::to_string(table_id) / (Hex16(seg_id) + ".swchunk");
  }

  // One section with a single INLINE op over `cdc` for (schema, table) and
  // optional per-Sink-chunk (base, count) segments. The op list is owned by
  // `_op_pools` so the section's `ops` span stays valid across AppendCommit.
  SearchDbWal::ShardSection InlineSection(
    uint64_t schema_id, uint64_t table_id,
    const duckdb::ColumnDataCollection& cdc,
    std::span<const SearchDbWal::InlinePk> pks = {}) {
    auto& ops = _op_pools.emplace_back();
    ops.push_back(SearchDbWal::Op{&cdc, pks, {}});
    return SearchDbWal::ShardSection{schema_id, table_id,
                                     std::span<const SearchDbWal::Op>{ops}};
  }
  // One section with a single REFERENCE op over chunk-file `segs`.
  SearchDbWal::ShardSection ReferenceSection(
    uint64_t schema_id, uint64_t table_id, const std::vector<uint64_t>& segs) {
    auto& ops = _op_pools.emplace_back();
    ops.push_back(
      SearchDbWal::Op{nullptr, {}, std::span<const uint64_t>{segs}});
    return SearchDbWal::ShardSection{schema_id, table_id,
                                     std::span<const SearchDbWal::Op>{ops}};
  }

  std::unique_ptr<duckdb::FileSystem> _fs;
  std::filesystem::path _dir;
  // Backing op lists for sections built by the helpers above. A deque so
  // references handed out stay stable as more sections are built in one test.
  std::deque<std::vector<SearchDbWal::Op>> _op_pools;
};

TEST_F(SearchDbWalTest, InlineRoundTrip) {
  {
    SearchDbWal wal(Fs(), _dir);
    auto cdc = MakeIntCdc(Alloc(), {10, 20, 30});
    auto sec = InlineSection(/*schema=*/3, /*table=*/5, *cdc);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got)), 1u);

  ASSERT_EQ(got.chunks.size(), 1u);
  EXPECT_EQ(std::get<0>(got.chunks[0]), 1u);  // tick
  EXPECT_EQ(std::get<1>(got.chunks[0]), 3u);  // schema_id
  EXPECT_EQ(std::get<2>(got.chunks[0]), 5u);  // table_id
  EXPECT_EQ(std::get<3>(got.chunks[0]), (std::vector<int32_t>{10, 20, 30}));
}

TEST_F(SearchDbWalTest, ReferenceRoundTrip) {
  {
    SearchDbWal wal(Fs(), _dir);
    auto cw1 = wal.NewChunkWriter(/*schema=*/1, /*table=*/2);
    auto cw2 = wal.NewChunkWriter(1, 2);
    EXPECT_EQ(cw1.SegId(), 1u);
    EXPECT_EQ(cw2.SegId(), 2u);
    auto cdc1 = MakeIntCdc(Alloc(), {1, 2});
    duckdb::DataChunk c1;
    FetchMaterialized(c1, Alloc(), *cdc1);
    cw1.Append(c1, 0);
    cw1.Finish();
    auto cdc2 = MakeIntCdc(Alloc(), {3, 4, 5});
    duckdb::DataChunk c2;
    FetchMaterialized(c2, Alloc(), *cdc2);
    cw2.Append(c2, 0);
    cw2.Finish();
    std::vector<uint64_t> segs{cw1.SegId(), cw2.SegId()};
    auto sec = ReferenceSection(1, 2, segs);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got)), 1u);

  ASSERT_EQ(got.chunks.size(), 2u);
  EXPECT_EQ(std::get<3>(got.chunks[0]), (std::vector<int32_t>{1, 2}));
  EXPECT_EQ(std::get<3>(got.chunks[1]), (std::vector<int32_t>{3, 4, 5}));
}

TEST_F(SearchDbWalTest, ReferenceRoundTripCompressed) {
  const std::vector<int32_t> vals(2048, 42);
  {
    SearchDbWal wal(Fs(), _dir);
    auto cw = wal.NewChunkWriter(1, 2);
    auto cdc = MakeIntCdc(Alloc(), vals);
    duckdb::DataChunk c;
    FetchMaterialized(c, Alloc(), *cdc);
    cw.Append(c, 0);
    cw.Finish();
    EXPECT_LT(std::filesystem::file_size(ChunkPath(1, 2, 1)), 1024u);
    std::vector<uint64_t> segs{cw.SegId()};
    auto sec = ReferenceSection(1, 2, segs);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got)), 1u);
  ASSERT_EQ(got.chunks.size(), 1u);
  EXPECT_EQ(std::get<3>(got.chunks[0]), vals);
}

TEST_F(SearchDbWalTest, MultiShardOneRecordIsAtomic) {
  // Two shards in ONE AppendCommit -> ONE central record at tick 1 (one fsync,
  // all-or-nothing). Recovery replays both sections.
  {
    SearchDbWal wal(Fs(), _dir);
    auto a = MakeIntCdc(Alloc(), {10});
    auto b = MakeIntCdc(Alloc(), {20});
    std::vector<SearchDbWal::ShardSection> secs{
      InlineSection(/*schema=*/1, /*table=*/100, *a),
      InlineSection(/*schema=*/1, /*table=*/200, *b)};
    EXPECT_EQ(wal.AppendCommit(secs), 1u);
  }
  // Exactly one segment (one record).
  EXPECT_TRUE(std::filesystem::exists(SegPath(1)));
  EXPECT_FALSE(std::filesystem::exists(SegPath(2)));

  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got)), 1u);
  ASSERT_EQ(got.chunks.size(), 2u);
  EXPECT_EQ(std::get<2>(got.chunks[0]), 100u);
  EXPECT_EQ(std::get<3>(got.chunks[0]), (std::vector<int32_t>{10}));
  EXPECT_EQ(std::get<2>(got.chunks[1]), 200u);
  EXPECT_EQ(std::get<3>(got.chunks[1]), (std::vector<int32_t>{20}));
}

TEST_F(SearchDbWalTest, RecoverySkipsConsumedPerShard) {
  // table 100 published up to tick 1; table 200 published nothing. One record
  // at tick 1 touches both -> table 100's section is skipped, table 200's
  // replays. (Per-shard skip, WAL_DESIGN.md §11.)
  {
    SearchDbWal wal(Fs(), _dir);
    auto a = MakeIntCdc(Alloc(), {10});
    auto b = MakeIntCdc(Alloc(), {20});
    std::vector<SearchDbWal::ShardSection> secs{InlineSection(1, 100, *a),
                                                InlineSection(1, 200, *b)};
    EXPECT_EQ(wal.AppendCommit(secs), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  auto committed_of = [](uint64_t table_id) -> uint64_t {
    return table_id == 100 ? 1 : 0;  // 100 already durable at tick 1
  };
  EXPECT_EQ(wal2.Recover(AllExist(), committed_of, MakeCollector(got)), 1u);
  ASSERT_EQ(got.chunks.size(), 1u);
  EXPECT_EQ(std::get<2>(got.chunks[0]), 200u);
  EXPECT_EQ(std::get<3>(got.chunks[0]), (std::vector<int32_t>{20}));
}

TEST_F(SearchDbWalTest, RecoverySkipsDroppedShard) {
  {
    SearchDbWal wal(Fs(), _dir);
    auto a = MakeIntCdc(Alloc(), {10});
    auto sec = InlineSection(1, 100, *a);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  auto none_exist = [](uint64_t, uint64_t) { return false; };  // table dropped
  EXPECT_EQ(wal2.Recover(none_exist, CommittedAll(0), MakeCollector(got)), 1u);
  EXPECT_TRUE(got.chunks.empty());
}

TEST_F(SearchDbWalTest, TornTailIgnored) {
  {
    SearchDbWal wal(Fs(), _dir);
    auto a = MakeIntCdc(Alloc(), {10});
    auto b = MakeIntCdc(Alloc(), {20});
    auto sa = InlineSection(1, 1, *a);
    wal.AppendCommit(std::span{&sa, 1});  // tick 1
    auto sb = InlineSection(1, 1, *b);
    wal.AppendCommit(std::span{&sb, 1});  // tick 2 (accumulates in seg 1)
  }
  {
    std::ofstream f(SegPath(1), std::ios::binary | std::ios::app);
    const char junk[7] = {};
    f.write(junk, sizeof(junk));
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got)), 2u);
  ASSERT_EQ(got.chunks.size(), 2u);
  EXPECT_EQ(std::get<3>(got.chunks[0]), (std::vector<int32_t>{10}));
  EXPECT_EQ(std::get<3>(got.chunks[1]), (std::vector<int32_t>{20}));
}

TEST_F(SearchDbWalTest, TickAndSegIdContinueOnReopen) {
  {
    SearchDbWal wal(Fs(), _dir);
    auto cw = wal.NewChunkWriter(1, 2);  // seg_id 1 (referenced -> survives)
    auto cdc = MakeIntCdc(Alloc(), {5});
    duckdb::DataChunk c;
    FetchMaterialized(c, Alloc(), *cdc);
    cw.Append(c, 0);
    cw.Finish();
    std::vector<uint64_t> segs{cw.SegId()};
    auto sec = ReferenceSection(1, 2, segs);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got)), 1u);
  // tick continues past the recovered max; per-(schema,table) seg_id too.
  auto cdc = MakeIntCdc(Alloc(), {6});
  auto sec = InlineSection(1, 2, *cdc);
  EXPECT_EQ(wal2.AppendCommit(std::span{&sec, 1}), 2u);
  EXPECT_EQ(wal2.NewChunkWriter(1, 2).SegId(), 2u);
}

TEST_F(SearchDbWalTest, CurrentTickReflectsAppends) {
  SearchDbWal wal(Fs(), _dir);
  EXPECT_EQ(wal.CurrentTick(), 0u);
  auto a = MakeIntCdc(Alloc(), {1});
  auto sec = InlineSection(1, 1, *a);
  wal.AppendCommit(std::span{&sec, 1});
  EXPECT_EQ(wal.CurrentTick(), 1u);
}

TEST_F(SearchDbWalTest, OrphanChunkSweptOnRecover) {
  {
    SearchDbWal wal(Fs(), _dir);
    auto cw = wal.NewChunkWriter(1, 2);  // never referenced by a commit
    duckdb::DataChunk c;
    FillIntChunk(c, Alloc(), {1});
    cw.Append(c, 0);
    cw.Finish();
  }
  EXPECT_TRUE(std::filesystem::exists(ChunkPath(1, 2, 1)));
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got)), 0u);
  EXPECT_TRUE(got.chunks.empty());
  EXPECT_FALSE(std::filesystem::exists(ChunkPath(1, 2, 1)));
}

TEST_F(SearchDbWalTest, MinTickGcDeletesConsumedSealedSegments) {
  // seal_threshold = 1 -> every commit rolls, so each record lands in its own
  // SEALED segment. After three commits: segments 1,2,3 (no active).
  SearchDbWal wal(Fs(), _dir, /*seal_threshold=*/1);
  for (int i = 1; i <= 3; ++i) {
    auto c = MakeIntCdc(Alloc(), {i});
    auto sec = InlineSection(1, 7, *c);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}), static_cast<uint64_t>(i));
  }
  EXPECT_TRUE(std::filesystem::exists(SegPath(1)));
  EXPECT_TRUE(std::filesystem::exists(SegPath(2)));
  EXPECT_TRUE(std::filesystem::exists(SegPath(3)));

  wal.RegisterShard(/*table=*/7, /*committed=*/0);
  wal.OnShardCommit(/*table=*/7, /*committed=*/2);  // min=2

  // Segments whose whole range <= 2 and that have a successor are deleted;
  // segment 3 (the last) is never deleted.
  EXPECT_FALSE(std::filesystem::exists(SegPath(1)));
  EXPECT_FALSE(std::filesystem::exists(SegPath(2)));
  EXPECT_TRUE(std::filesystem::exists(SegPath(3)));
}

TEST_F(SearchDbWalTest, MinTickGcReferenceChunksReclaimed) {
  SearchDbWal wal(Fs(), _dir, /*seal_threshold=*/1);
  auto cw = wal.NewChunkWriter(1, 7);  // seg_id 1
  duckdb::DataChunk c;
  FillIntChunk(c, Alloc(), {1});
  cw.Append(c, 0);
  cw.Finish();
  std::vector<uint64_t> segs{cw.SegId()};
  auto sec = ReferenceSection(1, 7, segs);
  EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}), 1u);  // sealed seg 1
  auto inl = MakeIntCdc(Alloc(), {2});
  auto sec2 = InlineSection(1, 7, *inl);
  EXPECT_EQ(wal.AppendCommit(std::span{&sec2, 1}),
            2u);  // sealed seg 2 (successor)

  EXPECT_TRUE(std::filesystem::exists(ChunkPath(1, 7, 1)));
  wal.RegisterShard(7, 0);
  wal.OnShardCommit(7,
                    1);  // min=1 -> seg 1 (next.first=2 <= 2) deleted + chunk
  EXPECT_FALSE(std::filesystem::exists(SegPath(1)));
  EXPECT_FALSE(std::filesystem::exists(ChunkPath(1, 7, 1)));
}

TEST_F(SearchDbWalTest, IdleShardPinsLogUntilDeregister) {
  // Two shards: 7 advances, 8 stays at committed 0 (idle, never VACUUM'd).
  // min = 0 -> nothing reclaimed, even segments shard 8 never wrote. Deregister
  // 8 -> min becomes shard 7's tick -> reclamation proceeds (WAL_DESIGN.md
  // §10.3).
  SearchDbWal wal(Fs(), _dir, /*seal_threshold=*/1);
  for (int i = 1; i <= 3; ++i) {
    auto c = MakeIntCdc(Alloc(), {i});
    auto sec = InlineSection(1, 7, *c);
    wal.AppendCommit(std::span{&sec, 1});
  }
  wal.RegisterShard(7, 0);
  wal.RegisterShard(8, 0);  // idle, pins min at 0
  wal.OnShardCommit(7, 3);  // min still 0 (shard 8 at 0) -> no GC
  EXPECT_TRUE(std::filesystem::exists(SegPath(1)));

  wal.DeregisterShard(
    8);  // min now 3 -> all 3 segments (ticks 1,2,3 <= 3) gone
  EXPECT_FALSE(std::filesystem::exists(SegPath(1)));
  EXPECT_FALSE(std::filesystem::exists(SegPath(2)));
  EXPECT_FALSE(std::filesystem::exists(SegPath(3)));
}

TEST_F(SearchDbWalTest, MinTickGcReclaimsLoneSealedSegment) {
  // A single bulk commit -> ONE sealed segment with NO successor (the case a
  // bulk CTAS hits). It must still be reclaimed once consumed -- regression
  // guard: an earlier filename-only rule never deleted the last segment, so a
  // load-then-query workload leaked the full chunk set on disk.
  SearchDbWal wal(Fs(), _dir, /*seal_threshold=*/1);
  auto cw = wal.NewChunkWriter(1, 7);
  duckdb::DataChunk c;
  FillIntChunk(c, Alloc(), {1});
  cw.Append(c, 0);
  cw.Finish();
  std::vector<uint64_t> segs{cw.SegId()};
  auto sec = ReferenceSection(1, 7, segs);
  EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}),
            1u);  // sealed seg 1, no successor
  EXPECT_TRUE(std::filesystem::exists(SegPath(1)));
  EXPECT_TRUE(std::filesystem::exists(ChunkPath(1, 7, 1)));

  wal.RegisterShard(7, 0);
  wal.OnShardCommit(
    7, 1);  // min=1: seg 1 (tick 1 <= 1) reclaimed despite being last
  EXPECT_FALSE(std::filesystem::exists(SegPath(1)));
  EXPECT_FALSE(std::filesystem::exists(ChunkPath(1, 7, 1)));
}

TEST_F(SearchDbWalTest, TickRestoredFromShardWhenWalEmpty) {
  // After GC empties the WAL, a fresh SearchDbWal scans no segments (tick 0);
  // RegisterShard with the shard's durable iresearch committed_tick must
  // restore the tick line so the next commit is strictly greater (iresearch
  // monotonicity).
  SearchDbWal wal(Fs(), _dir);  // empty dir -> no segments
  EXPECT_EQ(wal.CurrentTick(), 0u);
  wal.RegisterShard(/*table=*/7, /*committed=*/42);
  EXPECT_EQ(wal.CurrentTick(), 42u);
  auto cdc = MakeIntCdc(Alloc(), {1});
  auto sec = InlineSection(1, 7, *cdc);
  EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}),
            43u);  // strictly > committed 42
}

TEST_F(SearchDbWalTest, InlineInsertsAccumulateInOneSegment) {
  // Default (large) threshold: small INLINE inserts accumulate in one segment.
  SearchDbWal wal(Fs(), _dir);
  for (int i = 0; i < 5; ++i) {
    auto c = MakeIntCdc(Alloc(), {i});
    auto sec = InlineSection(1, 1, *c);
    wal.AppendCommit(std::span{&sec, 1});
  }
  EXPECT_TRUE(std::filesystem::exists(SegPath(1)));
  EXPECT_FALSE(std::filesystem::exists(SegPath(2)));
}

// Regression: the seal decision once summed ALL chunk files on disk, so a large
// bulk chunk still awaiting GC pushed every later small INLINE commit over the
// threshold -> a fresh segment per record (WAL bloat). Only the ACTIVE
// segment's own chunk bytes may count; a sealed segment's chunks are GC's
// problem. With a threshold the bulk chunk alone exceeds, the bulk commit seals
// its segment, and the small inserts after it must still accumulate in ONE
// fresh segment (WAL_DESIGN.md §10.2).
TEST_F(SearchDbWalTest, SealedSegmentChunksDoNotForceActiveRoll) {
  SearchDbWal wal(Fs(), _dir, /*seal_threshold=*/4096);
  // High-entropy values (xorshift) so the chunk can't compress below the
  // threshold -- the seal must trip on the chunk's real size.
  std::vector<int32_t> big(2048);
  uint32_t x = 0x9e3779b9u;
  for (int i = 0; i < 2048; ++i) {
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    big[i] = static_cast<int32_t>(x);
  }
  {
    auto cw = wal.NewChunkWriter(1, 7);
    auto cdc = MakeIntCdc(Alloc(), big);
    duckdb::DataChunk c;
    FetchMaterialized(c, Alloc(), *cdc);
    cw.Append(c, 0);
    cw.Finish();
    std::vector<uint64_t> segs{cw.SegId()};
    auto sec = ReferenceSection(1, 7, segs);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}),
              1u);  // tick 1 -> seals seg 1
  }
  // Precondition: the bulk chunk alone exceeds the seal threshold.
  ASSERT_GT(std::filesystem::file_size(ChunkPath(1, 7, 1)), 4096u);

  // Three tiny INLINE inserts. With the bug each rolls (the un-GC'd ~8 KB chunk
  // keeps the global sum over 4096); with the fix they share ONE fresh segment.
  for (int i = 0; i < 3; ++i) {
    auto c = MakeIntCdc(Alloc(), {i});
    auto sec = InlineSection(1, 7, *c);
    wal.AppendCommit(std::span{&sec, 1});  // ticks 2, 3, 4
  }
  EXPECT_TRUE(std::filesystem::exists(SegPath(1)));  // bulk (sealed)
  EXPECT_TRUE(std::filesystem::exists(SegPath(2)));  // inserts accumulate here
  EXPECT_FALSE(
    std::filesystem::exists(SegPath(3)));  // NOT a segment-per-record
  EXPECT_FALSE(std::filesystem::exists(SegPath(4)));
}

TEST_F(SearchDbWalTest, GeneratedPkBaseRoundTrip) {
  // INLINE: the per-chunk pk_base list is recorded in the body and recovered
  // per chunk (§5.6).
  {
    SearchDbWal wal(Fs(), _dir);
    auto cdc = MakeIntCdc(Alloc(), {7, 8, 9});
    std::vector<SearchDbWal::InlinePk> segs{{1000, 3}};  // base 1000, 3 rows
    auto sec = InlineSection(/*schema=*/1, /*table=*/5, *cdc,
                             std::span<const SearchDbWal::InlinePk>{segs});
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}), 1u);
  }
  {
    Collected got;
    SearchDbWal wal2(Fs(), _dir);
    EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got)),
              1u);
    ASSERT_EQ(got.chunks.size(), 1u);
    EXPECT_EQ(std::get<3>(got.chunks[0]), (std::vector<int32_t>{7, 8, 9}));
    EXPECT_EQ(std::get<4>(got.chunks[0]), 1000u);  // pk_base round-trips
  }

  // REFERENCE: pk_base rides the chunk frame.
  std::filesystem::remove_all(_dir);
  {
    SearchDbWal wal(Fs(), _dir);
    auto cw = wal.NewChunkWriter(1, 5);
    auto cdc = MakeIntCdc(Alloc(), {3, 4});
    duckdb::DataChunk c;
    FetchMaterialized(c, Alloc(), *cdc);
    cw.Append(c, 2000);
    cw.Finish();
    std::vector<uint64_t> segs{cw.SegId()};
    auto sec = ReferenceSection(1, 5, segs);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}), 1u);
  }
  {
    Collected got;
    SearchDbWal wal2(Fs(), _dir);
    EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got)),
              1u);
    ASSERT_EQ(got.chunks.size(), 1u);
    EXPECT_EQ(std::get<3>(got.chunks[0]), (std::vector<int32_t>{3, 4}));
    EXPECT_EQ(std::get<4>(got.chunks[0]), 2000u);
  }
}

// Two PARTIAL inline appends, each with its own (disjoint, non-contiguous)
// generated-PK base. ColumnDataCollection packs rows toward
// STANDARD_VECTOR_SIZE, so the two appends COALESCE into a single Chunks()
// entry. But pk_base is recorded per append-chunk (§5.6), so recovery must
// reproduce each append's rows with ITS base -- replaying by Chunks()-index
// tags the 2nd append's rows with the 1st base, minting wrong generated PKs (M6
// delete/dedup then breaks). The fix records per-append (base, count) segments
// and re-slices the CDC's rows by count on replay (§5.6); this pins that
// invariant.
TEST_F(SearchDbWalTest, InlinePkBaseAlignedToAppendsNotChunks) {
  auto cdc = std::make_unique<duckdb::ColumnDataCollection>(Alloc(), IntType());
  {
    duckdb::DataChunk c0;
    FillIntChunk(c0, Alloc(), {10, 11});
    cdc->Append(c0);
    duckdb::DataChunk c1;
    FillIntChunk(c1, Alloc(), {20, 21});
    cdc->Append(c1);
  }
  // Precondition: the partial appends coalesced (else the bug's premise is
  // gone).
  ASSERT_EQ(cdc->ChunkCount(), 1u);
  // One (base, count) segment per append; disjoint, non-contiguous bases.
  std::vector<SearchDbWal::InlinePk> segs{{1000, 2}, {2000, 2}};

  {
    SearchDbWal wal(Fs(), _dir);
    auto sec = InlineSection(/*schema=*/1, /*table=*/5, *cdc,
                             std::span<const SearchDbWal::InlinePk>{segs});
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}), 1u);
  }

  Collected got;
  {
    SearchDbWal wal2(Fs(), _dir);
    wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got));
  }
  // Each append's rows must come back tagged with its own base (pk = base +
  // row).
  ASSERT_EQ(got.chunks.size(), 2u);
  EXPECT_EQ(std::get<3>(got.chunks[0]), (std::vector<int32_t>{10, 11}));
  EXPECT_EQ(std::get<4>(got.chunks[0]), 1000u);
  EXPECT_EQ(std::get<3>(got.chunks[1]), (std::vector<int32_t>{20, 21}));
  EXPECT_EQ(std::get<4>(got.chunks[1]), 2000u);
}

// Two small INSERTs in one txn -> ONE shard section carrying TWO INLINE ops
// (no fold to a chunk file, no merge). Recovery walks the manifest in order,
// replaying each op's rows with its own generated-PK base (WAL_DESIGN.md §5.4).
TEST_F(SearchDbWalTest, MultipleInlineOpsOneSection) {
  auto a = MakeIntCdc(Alloc(), {10, 11});
  auto b = MakeIntCdc(Alloc(), {20});
  std::vector<SearchDbWal::InlinePk> segA{{1000, 2}};
  std::vector<SearchDbWal::InlinePk> segB{{2000, 1}};
  {
    SearchDbWal wal(Fs(), _dir);
    std::vector<SearchDbWal::Op> ops{
      SearchDbWal::Op{
        a.get(), std::span<const SearchDbWal::InlinePk>{segA}, {}},
      SearchDbWal::Op{
        b.get(), std::span<const SearchDbWal::InlinePk>{segB}, {}}};
    SearchDbWal::ShardSection sec{1, 5, std::span<const SearchDbWal::Op>{ops}};
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got)), 1u);
  ASSERT_EQ(got.chunks.size(), 2u);
  EXPECT_EQ(std::get<3>(got.chunks[0]), (std::vector<int32_t>{10, 11}));
  EXPECT_EQ(std::get<4>(got.chunks[0]), 1000u);
  EXPECT_EQ(std::get<3>(got.chunks[1]), (std::vector<int32_t>{20}));
  EXPECT_EQ(std::get<4>(got.chunks[1]), 2000u);
}

// A bulk+inline mix in one txn -> ONE shard section carrying an INLINE op AND a
// REFERENCE op (no fold: the inline rows ride the record, the bulk rows stay in
// their chunk file). Recovery replays both ops, in manifest order
// (WAL_DESIGN.md §5.4).
TEST_F(SearchDbWalTest, MixedInlineAndReferenceOps) {
  {
    SearchDbWal wal(Fs(), _dir);
    // Bulk chunk file (the REFERENCE op), pk_base rides the chunk frame.
    auto cw = wal.NewChunkWriter(1, 5);
    auto bulk = MakeIntCdc(Alloc(), {30, 31});
    duckdb::DataChunk bc;
    FetchMaterialized(bc, Alloc(), *bulk);
    cw.Append(bc, 3000);
    cw.Finish();
    std::vector<uint64_t> segids{cw.SegId()};
    // Inline rows (the INLINE op).
    auto inl = MakeIntCdc(Alloc(), {10, 11});
    std::vector<SearchDbWal::InlinePk> segA{{1000, 2}};
    std::vector<SearchDbWal::Op> ops{
      SearchDbWal::Op{
        inl.get(), std::span<const SearchDbWal::InlinePk>{segA}, {}},
      SearchDbWal::Op{nullptr, {}, std::span<const uint64_t>{segids}}};
    SearchDbWal::ShardSection sec{1, 5, std::span<const SearchDbWal::Op>{ops}};
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got)), 1u);
  ASSERT_EQ(got.chunks.size(), 2u);
  // INLINE op first (manifest order), then the REFERENCE op's chunk.
  EXPECT_EQ(std::get<3>(got.chunks[0]), (std::vector<int32_t>{10, 11}));
  EXPECT_EQ(std::get<4>(got.chunks[0]), 1000u);
  EXPECT_EQ(std::get<3>(got.chunks[1]), (std::vector<int32_t>{30, 31}));
  EXPECT_EQ(std::get<4>(got.chunks[1]), 3000u);
}

}  // namespace
}  // namespace sdb::search
