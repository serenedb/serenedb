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

// Materialised chunk (via CDC FetchChunk) where per-vector size() matches
// cardinality, as the real pipeline produces.
void FetchMaterialized(duckdb::DataChunk& out, duckdb::Allocator& alloc,
                       duckdb::ColumnDataCollection& cdc) {
  out.Initialize(alloc, IntType());
  cdc.FetchChunk(0, out);
}

std::string Hex16(uint64_t v) { return absl::StrFormat("%016x", v); }

// Accumulates everything Recover() replays.
struct Collected {
  // (tick, table_id, values, pk_base) per replayed chunk, in order.
  std::vector<std::tuple<uint64_t, uint64_t, std::vector<int32_t>, uint64_t>>
    chunks;
  // (tick, table_id, pks) per replayed DELETE op, in order.
  std::vector<std::tuple<uint64_t, uint64_t, std::vector<std::string>>> deletes;
  // (tick, table_id) per replayed TRUNCATE op, in order.
  std::vector<std::tuple<uint64_t, uint64_t>> truncates;
};

SearchDbWal::ReplayCallback MakeCollector(Collected& out) {
  return [&out](uint64_t tick, ObjectId table_id, uint64_t pk_base,
                duckdb::DataChunk& chunk) {
    std::vector<int32_t> vals;
    for (duckdb::idx_t i = 0; i < chunk.size(); ++i) {
      vals.push_back(chunk.GetValue(0, i).GetValue<int32_t>());
    }
    out.chunks.emplace_back(tick, table_id.id(), std::move(vals), pk_base);
  };
}

SearchDbWal::DeleteReplayCallback MakeDeleteCollector(Collected& out) {
  return [&out](uint64_t tick, ObjectId table_id,
                std::span<const std::string_view> pks) {
    std::vector<std::string> v;
    v.reserve(pks.size());
    for (auto pk : pks) {
      v.emplace_back(pk);
    }
    out.deletes.emplace_back(tick, table_id.id(), std::move(v));
  };
}

// No-op delete sink for the insert-only tests.
SearchDbWal::DeleteReplayCallback NoDeletes() {
  return [](uint64_t, ObjectId, std::span<const std::string_view>) {};
}

SearchDbWal::TruncateReplayCallback MakeTruncateCollector(Collected& out) {
  return [&out](uint64_t tick, ObjectId table_id) {
    out.truncates.emplace_back(tick, table_id.id());
  };
}

// No-op truncate sink for tests that don't exercise TRUNCATE.
SearchDbWal::TruncateReplayCallback NoTruncates() {
  return [](uint64_t, ObjectId) {};
}

// Replay hooks: every shard exists and nothing is durable yet (committed 0).
SearchDbWal::ShardExistsFn AllExist() {
  return [](ObjectId) { return true; };
}
SearchDbWal::ShardCommittedFn CommittedAll(uint64_t tick) {
  return [tick](ObjectId) { return tick; };
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
  std::filesystem::path ChunkPath(uint64_t table_id, uint64_t seg_id) const {
    return _dir / "chunks" / std::to_string(table_id) /
           (Hex16(seg_id) + ".swchunk");
  }

  // One section with a single INLINE op over `cdc` for `table_id` and optional
  // (base, count) segments. The op list is owned by `_op_pools` so the
  // section's `ops` span stays valid across AppendCommit.
  SearchDbWal::ShardSection InlineSection(
    uint64_t table_id, const duckdb::ColumnDataCollection& cdc,
    std::span<const SearchDbWal::InlinePk> pks = {}) {
    auto& ops = _op_pools.emplace_back();
    ops.push_back(SearchDbWal::Op{&cdc, pks, {}, {}});
    return SearchDbWal::ShardSection{ObjectId{table_id},
                                     std::span<const SearchDbWal::Op>{ops}};
  }
  // One section with a single REFERENCE op over bulk chunk files `chunks`.
  // AppendCommit derives the seg_ids from the chunks and marks them committed.
  SearchDbWal::ShardSection ReferenceSection(
    uint64_t table_id, std::span<SearchDbWal::PendingChunk> chunks) {
    auto& ops = _op_pools.emplace_back();
    ops.push_back(SearchDbWal::Op{nullptr, {}, chunks, {}});
    return SearchDbWal::ShardSection{ObjectId{table_id},
                                     std::span<const SearchDbWal::Op>{ops}};
  }
  // One section with a single DELETE op over encoded PK byte strings `pks`.
  SearchDbWal::ShardSection DeleteSection(uint64_t table_id,
                                          const std::vector<std::string>& pks) {
    auto& ops = _op_pools.emplace_back();
    ops.push_back(
      SearchDbWal::Op{nullptr, {}, {}, std::span<const std::string>{pks}});
    return SearchDbWal::ShardSection{ObjectId{table_id},
                                     std::span<const SearchDbWal::Op>{ops}};
  }
  // One section with a single (bodyless) TRUNCATE op.
  SearchDbWal::ShardSection TruncateSection(uint64_t table_id) {
    auto& ops = _op_pools.emplace_back();
    ops.push_back(SearchDbWal::Op{nullptr, {}, {}, {}, /*truncate=*/true});
    return SearchDbWal::ShardSection{ObjectId{table_id},
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
    auto sec = InlineSection(/*table=*/5, *cdc);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got),
                         NoDeletes(), NoTruncates()),
            1u);

  ASSERT_EQ(got.chunks.size(), 1u);
  EXPECT_EQ(std::get<0>(got.chunks[0]), 1u);  // tick
  EXPECT_EQ(std::get<1>(got.chunks[0]), 5u);  // table_id
  EXPECT_EQ(std::get<2>(got.chunks[0]), (std::vector<int32_t>{10, 20, 30}));
}

// A DELETE op round-trips its encoded PK byte strings (variable-width, incl.
// embedded NULs) through the central record.
TEST_F(SearchDbWalTest, DeleteRoundTrip) {
  const std::vector<std::string> pks{"pk-a", std::string("p\0k", 3), "ccc"};
  {
    SearchDbWal wal(Fs(), _dir);
    auto sec = DeleteSection(/*table=*/5, pks);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got),
                         MakeDeleteCollector(got), NoTruncates()),
            1u);
  EXPECT_TRUE(got.chunks.empty());
  ASSERT_EQ(got.deletes.size(), 1u);
  EXPECT_EQ(std::get<0>(got.deletes[0]), 1u);  // tick
  EXPECT_EQ(std::get<1>(got.deletes[0]), 5u);  // table_id
  EXPECT_EQ(std::get<2>(got.deletes[0]), pks);
}

// One section can carry an INSERT op and a DELETE op; both replay (in manifest
// order). The tick-band ordering that makes the delete mask the insert is a
// SearchTableTransaction concern -- here we only check the WAL round-trips
// both.
TEST_F(SearchDbWalTest, InsertAndDeleteInOneSection) {
  const std::vector<std::string> pks{"old1", "old2"};
  {
    SearchDbWal wal(Fs(), _dir);
    auto cdc = MakeIntCdc(Alloc(), {10, 20});
    auto& ops = _op_pools.emplace_back();
    ops.push_back(SearchDbWal::Op{cdc.get(), {}, {}, {}});  // INSERT (inline)
    ops.push_back(
      SearchDbWal::Op{nullptr, {}, {}, std::span<const std::string>{pks}});
    SearchDbWal::ShardSection sec{ObjectId{5},
                                  std::span<const SearchDbWal::Op>{ops}};
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got),
                         MakeDeleteCollector(got), NoTruncates()),
            1u);
  ASSERT_EQ(got.chunks.size(), 1u);
  EXPECT_EQ(std::get<2>(got.chunks[0]), (std::vector<int32_t>{10, 20}));
  ASSERT_EQ(got.deletes.size(), 1u);
  EXPECT_EQ(std::get<2>(got.deletes[0]), pks);
}

TEST_F(SearchDbWalTest, ReferenceRoundTrip) {
  {
    SearchDbWal wal(Fs(), _dir);
    auto cw1 = wal.NewChunkWriter(ObjectId{2});
    auto cw2 = wal.NewChunkWriter(ObjectId{2});
    EXPECT_EQ(cw1.SegId(), 1u);
    EXPECT_EQ(cw2.SegId(), 2u);
    auto cdc1 = MakeIntCdc(Alloc(), {1, 2});
    duckdb::DataChunk c1;
    FetchMaterialized(c1, Alloc(), *cdc1);
    cw1.Append(c1, 0);
    auto cdc2 = MakeIntCdc(Alloc(), {3, 4, 5});
    duckdb::DataChunk c2;
    FetchMaterialized(c2, Alloc(), *cdc2);
    cw2.Append(c2, 0);
    std::vector<SearchDbWal::PendingChunk> chunks;
    chunks.push_back(cw1.Finish());
    chunks.push_back(cw2.Finish());
    auto sec = ReferenceSection(2, chunks);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got),
                         NoDeletes(), NoTruncates()),
            1u);

  ASSERT_EQ(got.chunks.size(), 2u);
  EXPECT_EQ(std::get<2>(got.chunks[0]), (std::vector<int32_t>{1, 2}));
  EXPECT_EQ(std::get<2>(got.chunks[1]), (std::vector<int32_t>{3, 4, 5}));
}

TEST_F(SearchDbWalTest, ReferenceRoundTripCompressed) {
  const std::vector<int32_t> vals(2048, 42);
  {
    SearchDbWal wal(Fs(), _dir);
    auto cw = wal.NewChunkWriter(ObjectId{2});
    auto cdc = MakeIntCdc(Alloc(), vals);
    duckdb::DataChunk c;
    FetchMaterialized(c, Alloc(), *cdc);
    cw.Append(c, 0);
    std::vector<SearchDbWal::PendingChunk> chunks;
    chunks.push_back(cw.Finish());
    EXPECT_LT(std::filesystem::file_size(ChunkPath(2, 1)), 1024u);
    auto sec = ReferenceSection(2, chunks);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got),
                         NoDeletes(), NoTruncates()),
            1u);
  ASSERT_EQ(got.chunks.size(), 1u);
  EXPECT_EQ(std::get<2>(got.chunks[0]), vals);
}

TEST_F(SearchDbWalTest, MultiShardOneRecordIsAtomic) {
  // Two shards in ONE AppendCommit -> ONE record at tick 1; recovery replays
  // both sections.
  {
    SearchDbWal wal(Fs(), _dir);
    auto a = MakeIntCdc(Alloc(), {10});
    auto b = MakeIntCdc(Alloc(), {20});
    std::vector<SearchDbWal::ShardSection> secs{
      InlineSection(/*table=*/100, *a), InlineSection(/*table=*/200, *b)};
    EXPECT_EQ(wal.AppendCommit(secs, /*tick_span=*/1), 1u);
  }
  EXPECT_TRUE(std::filesystem::exists(SegPath(1)));
  EXPECT_FALSE(std::filesystem::exists(SegPath(2)));

  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got),
                         NoDeletes(), NoTruncates()),
            1u);
  ASSERT_EQ(got.chunks.size(), 2u);
  EXPECT_EQ(std::get<1>(got.chunks[0]), 100u);
  EXPECT_EQ(std::get<2>(got.chunks[0]), (std::vector<int32_t>{10}));
  EXPECT_EQ(std::get<1>(got.chunks[1]), 200u);
  EXPECT_EQ(std::get<2>(got.chunks[1]), (std::vector<int32_t>{20}));
}

TEST_F(SearchDbWalTest, RecoverySkipsConsumedPerShard) {
  // table 100 published up to tick 1; table 200 published nothing. One record
  // at tick 1 touches both -> table 100's section is skipped, table 200's
  // replays.
  {
    SearchDbWal wal(Fs(), _dir);
    auto a = MakeIntCdc(Alloc(), {10});
    auto b = MakeIntCdc(Alloc(), {20});
    std::vector<SearchDbWal::ShardSection> secs{InlineSection(100, *a),
                                                InlineSection(200, *b)};
    EXPECT_EQ(wal.AppendCommit(secs, /*tick_span=*/1), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  auto committed_of = [](ObjectId table_id) -> uint64_t {
    return table_id.id() == 100 ? 1 : 0;  // 100 already durable at tick 1
  };
  EXPECT_EQ(wal2.Recover(AllExist(), committed_of, MakeCollector(got),
                         NoDeletes(), NoTruncates()),
            1u);
  ASSERT_EQ(got.chunks.size(), 1u);
  EXPECT_EQ(std::get<1>(got.chunks[0]), 200u);
  EXPECT_EQ(std::get<2>(got.chunks[0]), (std::vector<int32_t>{20}));
}

TEST_F(SearchDbWalTest, RecoverySkipsDroppedShard) {
  {
    SearchDbWal wal(Fs(), _dir);
    auto a = MakeIntCdc(Alloc(), {10});
    auto sec = InlineSection(100, *a);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  auto none_exist = [](ObjectId) { return false; };  // table dropped
  EXPECT_EQ(wal2.Recover(none_exist, CommittedAll(0), MakeCollector(got),
                         NoDeletes(), NoTruncates()),
            1u);
  EXPECT_TRUE(got.chunks.empty());
}

TEST_F(SearchDbWalTest, TornTailIgnored) {
  {
    SearchDbWal wal(Fs(), _dir);
    auto a = MakeIntCdc(Alloc(), {10});
    auto b = MakeIntCdc(Alloc(), {20});
    auto sa = InlineSection(1, *a);
    wal.AppendCommit(std::span{&sa, 1}, /*tick_span=*/1);
    auto sb = InlineSection(1, *b);
    wal.AppendCommit(std::span{&sb, 1},
                     /*tick_span=*/1);  // accumulates in seg 1
  }
  {
    std::ofstream f(SegPath(1), std::ios::binary | std::ios::app);
    const char junk[7] = {};
    f.write(junk, sizeof(junk));
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got),
                         NoDeletes(), NoTruncates()),
            2u);
  ASSERT_EQ(got.chunks.size(), 2u);
  EXPECT_EQ(std::get<2>(got.chunks[0]), (std::vector<int32_t>{10}));
  EXPECT_EQ(std::get<2>(got.chunks[1]), (std::vector<int32_t>{20}));
}

TEST_F(SearchDbWalTest, TickAndSegIdContinueOnReopen) {
  {
    SearchDbWal wal(Fs(), _dir);
    auto cw =
      wal.NewChunkWriter(ObjectId{2});  // seg_id 1 (referenced -> survives)
    auto cdc = MakeIntCdc(Alloc(), {5});
    duckdb::DataChunk c;
    FetchMaterialized(c, Alloc(), *cdc);
    cw.Append(c, 0);
    std::vector<SearchDbWal::PendingChunk> chunks;
    chunks.push_back(cw.Finish());
    auto sec = ReferenceSection(2, chunks);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got),
                         NoDeletes(), NoTruncates()),
            1u);
  // tick and per-table seg_id both continue past the recovered max.
  auto cdc = MakeIntCdc(Alloc(), {6});
  auto sec = InlineSection(2, *cdc);
  EXPECT_EQ(wal2.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1), 2u);
  EXPECT_EQ(wal2.NewChunkWriter(ObjectId{2}).SegId(), 2u);
}

TEST_F(SearchDbWalTest, CurrentTickReflectsAppends) {
  SearchDbWal wal(Fs(), _dir);
  EXPECT_EQ(wal.CurrentTick(), 0u);
  auto a = MakeIntCdc(Alloc(), {1});
  auto sec = InlineSection(1, *a);
  wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1);
  EXPECT_EQ(wal.CurrentTick(), 1u);
}

// A record reserves a band of `tick_span` ticks and lands at the band top; the
// engine tick advances by the whole span (the per-shard iresearch bands the
// caller assigns live within (base, top]). Recovery replays each record at its
// band-top tick.
TEST_F(SearchDbWalTest, TickSpanReservesBand) {
  {
    SearchDbWal wal(Fs(), _dir);
    auto a = MakeIntCdc(Alloc(), {1});
    auto sec = InlineSection(7, *a);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/3), 3u);
    EXPECT_EQ(wal.CurrentTick(), 3u);
    auto b = MakeIntCdc(Alloc(), {2});
    auto sec2 = InlineSection(7, *b);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec2, 1}, /*tick_span=*/1), 4u);
    EXPECT_EQ(wal.CurrentTick(), 4u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got),
                         NoDeletes(), NoTruncates()),
            4u);
  ASSERT_EQ(got.chunks.size(), 2u);
  EXPECT_EQ(std::get<0>(got.chunks[0]), 3u);  // first record's band-top tick
  EXPECT_EQ(std::get<0>(got.chunks[1]), 4u);
}

TEST_F(SearchDbWalTest, OrphanChunkSweptOnRecover) {
  {
    SearchDbWal wal(Fs(), _dir);
    auto cw = wal.NewChunkWriter(ObjectId{2});  // never referenced by a commit
    duckdb::DataChunk c;
    FillIntChunk(c, Alloc(), {1});
    cw.Append(c, 0);
    cw.Finish().MarkCommitted();
  }
  EXPECT_TRUE(std::filesystem::exists(ChunkPath(2, 1)));
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got),
                         NoDeletes(), NoTruncates()),
            0u);
  EXPECT_TRUE(got.chunks.empty());
  EXPECT_FALSE(std::filesystem::exists(ChunkPath(2, 1)));
}

// An uncommitted PendingChunk reclaims its file when destroyed -- a rolled-back
// bulk insert leaves no orphan (the in-process counterpart of recovery's
// sweep).
TEST_F(SearchDbWalTest, UncommittedPendingChunkReclaimsFile) {
  SearchDbWal wal(Fs(), _dir);
  duckdb::DataChunk c;
  FillIntChunk(c, Alloc(), {1, 2, 3});
  std::filesystem::path path;
  {
    auto cw = wal.NewChunkWriter(ObjectId{7});
    cw.Append(c, 0);
    auto pending = cw.Finish();  // durable on disk, but never committed
    path = ChunkPath(7, pending.SegId());
    EXPECT_TRUE(std::filesystem::exists(path));
  }
  EXPECT_FALSE(std::filesystem::exists(path));
}

// MarkCommitted() suppresses the reclaim: a committed chunk survives its
// PendingChunk's destruction.
TEST_F(SearchDbWalTest, CommittedPendingChunkKeepsFile) {
  SearchDbWal wal(Fs(), _dir);
  duckdb::DataChunk c;
  FillIntChunk(c, Alloc(), {1, 2, 3});
  std::filesystem::path path;
  {
    auto cw = wal.NewChunkWriter(ObjectId{7});
    cw.Append(c, 0);
    auto pending = cw.Finish();
    pending.MarkCommitted();
    path = ChunkPath(7, pending.SegId());
    EXPECT_TRUE(std::filesystem::exists(path));
  }
  EXPECT_TRUE(std::filesystem::exists(path));
}

TEST_F(SearchDbWalTest, MinTickGcDeletesConsumedSealedSegments) {
  // seal_threshold = 1 -> every commit rolls, so each record lands in its own
  // sealed segment. After three commits: segments 1,2,3 (no active).
  SearchDbWal wal(Fs(), _dir, /*seal_threshold=*/1);
  for (int i = 1; i <= 3; ++i) {
    auto c = MakeIntCdc(Alloc(), {i});
    auto sec = InlineSection(7, *c);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1),
              static_cast<uint64_t>(i));
  }
  EXPECT_TRUE(std::filesystem::exists(SegPath(1)));
  EXPECT_TRUE(std::filesystem::exists(SegPath(2)));
  EXPECT_TRUE(std::filesystem::exists(SegPath(3)));

  wal.RegisterShard(ObjectId{7}, /*committed=*/0);
  wal.OnShardCommit(ObjectId{7}, /*committed=*/2);  // min=2

  // Segments whose whole range <= 2 and that have a successor are deleted;
  // segment 3 (the last) is never deleted.
  EXPECT_FALSE(std::filesystem::exists(SegPath(1)));
  EXPECT_FALSE(std::filesystem::exists(SegPath(2)));
  EXPECT_TRUE(std::filesystem::exists(SegPath(3)));
}

TEST_F(SearchDbWalTest, MinTickGcReferenceChunksReclaimed) {
  SearchDbWal wal(Fs(), _dir, /*seal_threshold=*/1);
  auto cw = wal.NewChunkWriter(ObjectId{7});
  duckdb::DataChunk c;
  FillIntChunk(c, Alloc(), {1});
  cw.Append(c, 0);
  std::vector<SearchDbWal::PendingChunk> chunks;
  chunks.push_back(cw.Finish());
  auto sec = ReferenceSection(7, chunks);
  EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1),
            1u);  // sealed seg 1
  auto inl = MakeIntCdc(Alloc(), {2});
  auto sec2 = InlineSection(7, *inl);
  EXPECT_EQ(wal.AppendCommit(std::span{&sec2, 1}, /*tick_span=*/1),
            2u);  // sealed seg 2 (successor)

  EXPECT_TRUE(std::filesystem::exists(ChunkPath(7, 1)));
  wal.RegisterShard(ObjectId{7}, 0);
  wal.OnShardCommit(ObjectId{7},
                    1);  // min=1 -> seg 1 deleted with its chunk
  EXPECT_FALSE(std::filesystem::exists(SegPath(1)));
  EXPECT_FALSE(std::filesystem::exists(ChunkPath(7, 1)));
}

TEST_F(SearchDbWalTest, IdleShardPinsLogUntilDeregister) {
  // Two shards: 7 advances, 8 stays at committed 0 (idle). min = 0 -> nothing
  // reclaimed, even segments shard 8 never wrote. Deregister 8 -> min becomes
  // shard 7's tick -> reclamation proceeds.
  SearchDbWal wal(Fs(), _dir, /*seal_threshold=*/1);
  for (int i = 1; i <= 3; ++i) {
    auto c = MakeIntCdc(Alloc(), {i});
    auto sec = InlineSection(7, *c);
    wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1);
  }
  wal.RegisterShard(ObjectId{7}, 0);
  wal.RegisterShard(ObjectId{8}, 0);  // idle, pins min at 0
  wal.OnShardCommit(ObjectId{7}, 3);  // min still 0 (shard 8 at 0) -> no GC
  EXPECT_TRUE(std::filesystem::exists(SegPath(1)));

  wal.DeregisterShard(ObjectId{8});  // min now 3 -> all 3 segments gone
  EXPECT_FALSE(std::filesystem::exists(SegPath(1)));
  EXPECT_FALSE(std::filesystem::exists(SegPath(2)));
  EXPECT_FALSE(std::filesystem::exists(SegPath(3)));
}

TEST_F(SearchDbWalTest, IdleShardAdvancedToCurrentTickUnpinsGc) {
  // The fix for the idle-shard GC stall: an idle shard (no records of its own,
  // so already caught up) advances its committed tick to the WAL's current
  // tick, lifting the shared WAL's GC floor without having to be dropped.
  SearchDbWal wal(Fs(), _dir, /*seal_threshold=*/1);
  for (int i = 1; i <= 3; ++i) {
    auto c = MakeIntCdc(Alloc(), {i});
    auto sec = InlineSection(7, *c);
    wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1);
  }
  wal.RegisterShard(ObjectId{7}, 0);
  wal.RegisterShard(ObjectId{8}, 0);  // idle, pins min at 0
  wal.OnShardCommit(ObjectId{7}, 2);  // min still 0 (shard 8 at 0) -> no GC
  EXPECT_TRUE(std::filesystem::exists(SegPath(1)));

  // Advance the idle shard to the current WAL tick (the fix). min now rises to
  // shard 7's tick (2), so consumed segments below it are reclaimed -- without
  // dropping shard 8.
  wal.OnShardCommit(ObjectId{8}, wal.CurrentTick());
  EXPECT_FALSE(std::filesystem::exists(SegPath(1)));
  EXPECT_FALSE(std::filesystem::exists(SegPath(2)));
  EXPECT_TRUE(
    std::filesystem::exists(SegPath(3)));  // shard 7 hasn't consumed 3
}

TEST_F(SearchDbWalTest, MinTickGcReclaimsLoneSealedSegment) {
  // A single bulk commit -> ONE sealed segment with NO successor. It must still
  // be reclaimed once consumed -- regression guard: an earlier filename-only
  // rule never deleted the last segment, leaking the chunk set on disk.
  SearchDbWal wal(Fs(), _dir, /*seal_threshold=*/1);
  auto cw = wal.NewChunkWriter(ObjectId{7});
  duckdb::DataChunk c;
  FillIntChunk(c, Alloc(), {1});
  cw.Append(c, 0);
  std::vector<SearchDbWal::PendingChunk> chunks;
  chunks.push_back(cw.Finish());
  auto sec = ReferenceSection(7, chunks);
  EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1),
            1u);  // sealed seg 1, no successor
  EXPECT_TRUE(std::filesystem::exists(SegPath(1)));
  EXPECT_TRUE(std::filesystem::exists(ChunkPath(7, 1)));

  wal.RegisterShard(ObjectId{7}, 0);
  wal.OnShardCommit(ObjectId{7},
                    1);  // min=1: seg 1 reclaimed despite being last
  EXPECT_FALSE(std::filesystem::exists(SegPath(1)));
  EXPECT_FALSE(std::filesystem::exists(ChunkPath(7, 1)));
}

TEST_F(SearchDbWalTest, TickRestoredFromShardWhenWalEmpty) {
  // After GC empties the WAL, a fresh SearchDbWal scans no segments (tick 0);
  // RegisterShard with the shard's durable committed_tick must restore the tick
  // line so the next commit is strictly greater (iresearch monotonicity).
  SearchDbWal wal(Fs(), _dir);  // empty dir -> no segments
  EXPECT_EQ(wal.CurrentTick(), 0u);
  wal.RegisterShard(ObjectId{7}, /*committed=*/42);
  EXPECT_EQ(wal.CurrentTick(), 42u);
  auto cdc = MakeIntCdc(Alloc(), {1});
  auto sec = InlineSection(7, *cdc);
  EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1),
            43u);  // strictly > committed 42
}

TEST_F(SearchDbWalTest, InlineInsertsAccumulateInOneSegment) {
  // Default (large) threshold: small INLINE inserts accumulate in one segment.
  SearchDbWal wal(Fs(), _dir);
  for (int i = 0; i < 5; ++i) {
    auto c = MakeIntCdc(Alloc(), {i});
    auto sec = InlineSection(1, *c);
    wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1);
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
// fresh segment.
TEST_F(SearchDbWalTest, SealedSegmentChunksDoNotForceActiveRoll) {
  SearchDbWal wal(Fs(), _dir, /*seal_threshold=*/4096);
  // High-entropy values so the chunk can't compress below the threshold.
  std::vector<int32_t> big(2048);
  uint32_t x = 0x9e3779b9u;
  for (int i = 0; i < 2048; ++i) {
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    big[i] = static_cast<int32_t>(x);
  }
  {
    auto cw = wal.NewChunkWriter(ObjectId{7});
    auto cdc = MakeIntCdc(Alloc(), big);
    duckdb::DataChunk c;
    FetchMaterialized(c, Alloc(), *cdc);
    cw.Append(c, 0);
    std::vector<SearchDbWal::PendingChunk> chunks;
    chunks.push_back(cw.Finish());
    auto sec = ReferenceSection(7, chunks);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1),
              1u);  // seals seg 1
  }
  // Precondition: the bulk chunk alone exceeds the seal threshold.
  ASSERT_GT(std::filesystem::file_size(ChunkPath(7, 1)), 4096u);

  // Three tiny INLINE inserts. With the bug each rolls (the un-GC'd chunk keeps
  // the global sum over threshold); with the fix they share ONE fresh segment.
  for (int i = 0; i < 3; ++i) {
    auto c = MakeIntCdc(Alloc(), {i});
    auto sec = InlineSection(7, *c);
    wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1);
  }
  EXPECT_TRUE(std::filesystem::exists(SegPath(1)));  // bulk (sealed)
  EXPECT_TRUE(std::filesystem::exists(SegPath(2)));  // inserts accumulate here
  EXPECT_FALSE(std::filesystem::exists(SegPath(3)));
  EXPECT_FALSE(std::filesystem::exists(SegPath(4)));
}

TEST_F(SearchDbWalTest, GeneratedPkBaseRoundTrip) {
  // INLINE: the per-chunk pk_base list is recorded in the body and recovered
  // per chunk.
  {
    SearchDbWal wal(Fs(), _dir);
    auto cdc = MakeIntCdc(Alloc(), {7, 8, 9});
    std::vector<SearchDbWal::InlinePk> segs{{1000, 3}};  // base 1000, 3 rows
    auto sec = InlineSection(/*table=*/5, *cdc,
                             std::span<const SearchDbWal::InlinePk>{segs});
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1), 1u);
  }
  {
    Collected got;
    SearchDbWal wal2(Fs(), _dir);
    EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got),
                           NoDeletes(), NoTruncates()),
              1u);
    ASSERT_EQ(got.chunks.size(), 1u);
    EXPECT_EQ(std::get<2>(got.chunks[0]), (std::vector<int32_t>{7, 8, 9}));
    EXPECT_EQ(std::get<3>(got.chunks[0]), 1000u);  // pk_base round-trips
  }

  // REFERENCE: pk_base rides the chunk frame.
  std::filesystem::remove_all(_dir);
  {
    SearchDbWal wal(Fs(), _dir);
    auto cw = wal.NewChunkWriter(ObjectId{5});
    auto cdc = MakeIntCdc(Alloc(), {3, 4});
    duckdb::DataChunk c;
    FetchMaterialized(c, Alloc(), *cdc);
    cw.Append(c, 2000);
    std::vector<SearchDbWal::PendingChunk> chunks;
    chunks.push_back(cw.Finish());
    auto sec = ReferenceSection(5, chunks);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1), 1u);
  }
  {
    Collected got;
    SearchDbWal wal2(Fs(), _dir);
    EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got),
                           NoDeletes(), NoTruncates()),
              1u);
    ASSERT_EQ(got.chunks.size(), 1u);
    EXPECT_EQ(std::get<2>(got.chunks[0]), (std::vector<int32_t>{3, 4}));
    EXPECT_EQ(std::get<3>(got.chunks[0]), 2000u);
  }
}

// Two PARTIAL inline appends, each with its own (disjoint, non-contiguous)
// generated-PK base. ColumnDataCollection packs rows toward
// STANDARD_VECTOR_SIZE, so the two appends COALESCE into a single Chunks()
// entry. But pk_base is recorded per append-chunk, so recovery must reproduce
// each append's rows with ITS base -- replaying by Chunks()-index tags the 2nd
// append's rows with the 1st base, minting wrong generated PKs. The fix records
// per-append (base, count) segments and re-slices the CDC's rows by count on
// replay; this pins that invariant.
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
    auto sec = InlineSection(/*table=*/5, *cdc,
                             std::span<const SearchDbWal::InlinePk>{segs});
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1), 1u);
  }

  Collected got;
  {
    SearchDbWal wal2(Fs(), _dir);
    wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got), NoDeletes(),
                 NoTruncates());
  }
  // Each append's rows must come back tagged with its own base (pk = base +
  // row).
  ASSERT_EQ(got.chunks.size(), 2u);
  EXPECT_EQ(std::get<2>(got.chunks[0]), (std::vector<int32_t>{10, 11}));
  EXPECT_EQ(std::get<3>(got.chunks[0]), 1000u);
  EXPECT_EQ(std::get<2>(got.chunks[1]), (std::vector<int32_t>{20, 21}));
  EXPECT_EQ(std::get<3>(got.chunks[1]), 2000u);
}

// Two small INSERTs in one txn -> ONE shard section carrying TWO INLINE ops
// (no fold to a chunk file, no merge). Recovery walks the manifest in order,
// replaying each op's rows with its own generated-PK base.
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
    SearchDbWal::ShardSection sec{ObjectId{5},
                                  std::span<const SearchDbWal::Op>{ops}};
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got),
                         NoDeletes(), NoTruncates()),
            1u);
  ASSERT_EQ(got.chunks.size(), 2u);
  EXPECT_EQ(std::get<2>(got.chunks[0]), (std::vector<int32_t>{10, 11}));
  EXPECT_EQ(std::get<3>(got.chunks[0]), 1000u);
  EXPECT_EQ(std::get<2>(got.chunks[1]), (std::vector<int32_t>{20}));
  EXPECT_EQ(std::get<3>(got.chunks[1]), 2000u);
}

// A bulk+inline mix in one txn -> ONE shard section carrying an INLINE op AND a
// REFERENCE op (no fold: the inline rows ride the record, the bulk rows stay in
// their chunk file). Recovery replays both ops, in manifest order.
TEST_F(SearchDbWalTest, MixedInlineAndReferenceOps) {
  {
    SearchDbWal wal(Fs(), _dir);
    // Bulk chunk file (the REFERENCE op), pk_base rides the chunk frame.
    auto cw = wal.NewChunkWriter(ObjectId{5});
    auto bulk = MakeIntCdc(Alloc(), {30, 31});
    duckdb::DataChunk bc;
    FetchMaterialized(bc, Alloc(), *bulk);
    cw.Append(bc, 3000);
    std::vector<SearchDbWal::PendingChunk> chunks;
    chunks.push_back(cw.Finish());
    // Inline rows (the INLINE op).
    auto inl = MakeIntCdc(Alloc(), {10, 11});
    std::vector<SearchDbWal::InlinePk> segA{{1000, 2}};
    std::vector<SearchDbWal::Op> ops{
      SearchDbWal::Op{
        inl.get(), std::span<const SearchDbWal::InlinePk>{segA}, {}},
      SearchDbWal::Op{
        nullptr, {}, std::span<SearchDbWal::PendingChunk>{chunks}}};
    SearchDbWal::ShardSection sec{ObjectId{5},
                                  std::span<const SearchDbWal::Op>{ops}};
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got),
                         NoDeletes(), NoTruncates()),
            1u);
  ASSERT_EQ(got.chunks.size(), 2u);
  // INLINE op first (manifest order), then the REFERENCE op's chunk.
  EXPECT_EQ(std::get<2>(got.chunks[0]), (std::vector<int32_t>{10, 11}));
  EXPECT_EQ(std::get<3>(got.chunks[0]), 1000u);
  EXPECT_EQ(std::get<2>(got.chunks[1]), (std::vector<int32_t>{30, 31}));
  EXPECT_EQ(std::get<3>(got.chunks[1]), 3000u);
}

// Interleaved INSERT/DELETE in ONE txn must replay in EXACT manifest order.
// Run-coalescing seals the current insert run at each DELETE, so the statement
// stream [INSERT, DELETE, INSERT, DELETE] yields four ops in that order -- NOT
// one batched DELETE floated to the end. Recovery walks the ops in order, so a
// single log fed by BOTH callbacks must observe I,D,I,D. (This is what lets the
// caller's descending tick band make each delete mask only the inserts before
// it; a batched-delete regression would collapse the two DELETEs and reorder
// them after both inserts -- which the separate-vector collectors can't catch,
// hence the combined ordered log here.) An end-to-end same-txn check waits on
// read-your-own-writes; until then this pins the WAL ordering contract.
TEST_F(SearchDbWalTest, InterleavedInsertDeleteReplayInManifestOrder) {
  auto a = MakeIntCdc(Alloc(), {10});
  auto b = MakeIntCdc(Alloc(), {20});
  const std::vector<std::string> del1{"d1"};
  const std::vector<std::string> del2{"d2"};
  {
    SearchDbWal wal(Fs(), _dir);
    std::vector<SearchDbWal::Op> ops{
      SearchDbWal::Op{a.get(), {}, {}, {}},  // INSERT 10
      SearchDbWal::Op{nullptr, {}, {}, std::span<const std::string>{del1}},
      SearchDbWal::Op{b.get(), {}, {}, {}},  // INSERT 20
      SearchDbWal::Op{nullptr, {}, {}, std::span<const std::string>{del2}}};
    SearchDbWal::ShardSection sec{ObjectId{5},
                                  std::span<const SearchDbWal::Op>{ops}};
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1), 1u);
  }

  // One ordered log across BOTH callbacks -- entries appear in replay order.
  std::vector<std::string> order;
  auto insert_cb = [&order](uint64_t, ObjectId, uint64_t,
                            duckdb::DataChunk& chunk) {
    order.push_back(
      absl::StrFormat("I%d", chunk.GetValue(0, 0).GetValue<int32_t>()));
  };
  auto delete_cb = [&order](uint64_t, ObjectId,
                            std::span<const std::string_view> pks) {
    order.push_back(absl::StrFormat("D%s", pks.front()));
  };
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), insert_cb, delete_cb,
                         NoTruncates()),
            1u);

  EXPECT_EQ(order, (std::vector<std::string>{"I10", "Dd1", "I20", "Dd2"}));
}

// A TRUNCATE op round-trips as a bodyless marker: replay fires only the
// truncate callback (no insert/delete) with the record tick + table id. The
// replayer turns that into an iresearch Clear.
TEST_F(SearchDbWalTest, TruncateRoundTrip) {
  {
    SearchDbWal wal(Fs(), _dir);
    auto sec = TruncateSection(/*table=*/5);
    EXPECT_EQ(wal.AppendCommit(std::span{&sec, 1}, /*tick_span=*/1), 1u);
  }
  Collected got;
  SearchDbWal wal2(Fs(), _dir);
  EXPECT_EQ(wal2.Recover(AllExist(), CommittedAll(0), MakeCollector(got),
                         MakeDeleteCollector(got), MakeTruncateCollector(got)),
            1u);
  EXPECT_TRUE(got.chunks.empty());
  EXPECT_TRUE(got.deletes.empty());
  ASSERT_EQ(got.truncates.size(), 1u);
  EXPECT_EQ(std::get<0>(got.truncates[0]), 1u);  // tick
  EXPECT_EQ(std::get<1>(got.truncates[0]), 5u);  // table_id
}

}  // namespace
}  // namespace sdb::search
