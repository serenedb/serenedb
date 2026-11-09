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

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <duckdb/main/connection.hpp>
#include <iresearch/index/merge/merge_profile.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/enum_name.hpp>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "search/compaction/merge_registry.h"
#include "server/utils/metrics.h"

namespace {

using sdb::search::CompactHistorySize;
using sdb::search::DescribeInputs;
using sdb::search::MergeOrigin;
using sdb::search::MergeOutcome;
using sdb::search::MergeRecord;
using sdb::search::MergeRegistry;

constexpr size_t kKept = MergeRegistry::kDefaultHistorySize;

std::vector<MergeRecord> HistoryOf(MergeRegistry& registry,
                                   uint64_t relation_id) {
  return registry.History(relation_id, CompactHistorySize());
}

bool IsRunning(const MergeRegistry& registry, uint64_t relation_id) {
  return !registry.Active(relation_id).empty();
}

void Exec(std::string_view sql) {
  const auto connection = irs::DuckDBEngine::Instance().CreateConnection();
  const auto result = connection->Query(sql);
  ASSERT_FALSE(result->HasError()) << sql << ": " << result->GetError();
}

irs::CompactionResult Result(irs::CompactionError error, size_t size) {
  irs::CompactionResult result;
  result.error = error;
  result.size = size;
  return result;
}

class SegmentStub final : public irs::SubReader {
 public:
  SegmentStub(std::string name, uint32_t docs)
    : SegmentStub{std::move(name), docs, docs, 0} {}

  SegmentStub(std::string name, uint32_t docs, uint32_t live_docs,
              uint64_t bytes)
    : _meta{.name = std::move(name),
            .docs_count = docs,
            .live_docs_count = live_docs,
            .byte_size = bytes} {}

  uint64_t CountMappedMemory() const final { return 0; }
  const irs::SegmentInfo& Meta() const final { return _meta; }
  const irs::DocumentMask* docs_mask() const final { return nullptr; }
  irs::lead::Node::ptr docs_iterator() const final { return nullptr; }
  std::span<const irs::field_id> field_ids() const final { return {}; }
  std::span<const irs::TermReader* const> fields(irs::field_id) const final {
    return {};
  }
  irs::NormReader::ptr norms(irs::field_id) const final { return {}; }

 private:
  irs::SegmentInfo _meta;
};

TEST(MergeRegistry, OutcomeOfMapsEveryCompactionError) {
  EXPECT_EQ(MergeOutcome::Failed,
            sdb::search::OutcomeOf(Result(irs::CompactionError::Fail, 3)));
  EXPECT_EQ(MergeOutcome::Busy,
            sdb::search::OutcomeOf(Result(irs::CompactionError::Busy, 0)));
  EXPECT_EQ(MergeOutcome::Deferred,
            sdb::search::OutcomeOf(Result(irs::CompactionError::Pending, 2)));
  EXPECT_EQ(MergeOutcome::Merged,
            sdb::search::OutcomeOf(Result(irs::CompactionError::Ok, 2)));
  EXPECT_EQ(MergeOutcome::Nothing,
            sdb::search::OutcomeOf(Result(irs::CompactionError::Ok, 0)));
  EXPECT_EQ(MergeOutcome::Cancelled,
            sdb::search::OutcomeOf(Result(irs::CompactionError::Cancelled, 2)));
}

TEST(MergeRegistry, OnlyMergedAndDeferredMergesProduceASegment) {
  EXPECT_TRUE(sdb::search::Produced(MergeOutcome::Merged));
  EXPECT_TRUE(sdb::search::Produced(MergeOutcome::Deferred));
  EXPECT_FALSE(sdb::search::Produced(MergeOutcome::Nothing));
  EXPECT_FALSE(sdb::search::Produced(MergeOutcome::Busy));
  EXPECT_FALSE(sdb::search::Produced(MergeOutcome::Failed));
  EXPECT_FALSE(sdb::search::Produced(MergeOutcome::Cancelled));
  EXPECT_FALSE(sdb::search::Produced(MergeOutcome::Running));
}

TEST(MergeRegistry, NamesAreStable) {
  EXPECT_EQ("background", irs::SnakeCaseName(MergeOrigin::Background));
  EXPECT_EQ("vacuum", irs::SnakeCaseName(MergeOrigin::Vacuum));
  EXPECT_EQ("sql", irs::SnakeCaseName(MergeOrigin::Sql));
  EXPECT_EQ("running", irs::SnakeCaseName(MergeOutcome::Running));
  EXPECT_EQ("merged", irs::SnakeCaseName(MergeOutcome::Merged));
  EXPECT_EQ("deferred", irs::SnakeCaseName(MergeOutcome::Deferred));
  EXPECT_EQ("nothing", irs::SnakeCaseName(MergeOutcome::Nothing));
  EXPECT_EQ("busy", irs::SnakeCaseName(MergeOutcome::Busy));
  EXPECT_EQ("failed", irs::SnakeCaseName(MergeOutcome::Failed));
  EXPECT_EQ("cancelled", irs::SnakeCaseName(MergeOutcome::Cancelled));
}

TEST(MergeRegistry, AQueuedMergeRunsAndFinishesUnderOneId) {
  MergeRegistry registry;
  const SegmentStub first{"_1", 10};
  const SegmentStub second{"_2", 30};
  const irs::SubReader* const segments[] = {&first, &second};
  const auto id =
    registry.Queue(7, MergeOrigin::Background, DescribeInputs(segments));
  auto active = registry.Active(7);
  ASSERT_EQ(1, active.size());
  EXPECT_EQ(7, active[0].relation_id);
  EXPECT_EQ(MergeOrigin::Background, active[0].origin);
  EXPECT_LT(0, active[0].queued_us);
  EXPECT_EQ(40, active[0].stats.input_docs);

  registry.Dispatch(id);
  active = registry.Active(7);
  ASSERT_EQ(1, active.size());
  EXPECT_LE(active[0].queued_us, active[0].started_us);

  registry.Finish(id, MergeOutcome::Cancelled, {}, {}, kKept);
  EXPECT_TRUE(registry.Active(7).empty());
  const auto history = registry.History(7, kKept);
  ASSERT_EQ(1, history.size());
  EXPECT_EQ(id, history[0].id);
  EXPECT_EQ(MergeOutcome::Cancelled, history[0].outcome);
  EXPECT_EQ(std::vector<std::string>({"_1", "_2"}), history[0].stats.inputs);
}

TEST(MergeRegistry, ADroppedQueuedMergeLeavesNoHistory) {
  MergeRegistry registry;
  const SegmentStub segment{"_1", 10};
  const auto id = registry.Queue(7, MergeOrigin::Background,
                                 DescribeInputs(irs::Compaction{&segment}));
  registry.Drop(id);
  registry.Dispatch(id);
  EXPECT_TRUE(registry.Active(7).empty());
  EXPECT_TRUE(registry.History(7, kKept).empty());
}

TEST(MergeRegistry, AForegroundMergeIsNeverQueued) {
  MergeRegistry registry;
  const SegmentStub segment{"_1", 10};
  const auto id = registry.Queue(7, MergeOrigin::Vacuum,
                                 DescribeInputs(irs::Compaction{&segment}));
  auto active = registry.Active(7);
  ASSERT_EQ(1, active.size());
  EXPECT_EQ(MergeOrigin::Vacuum, active[0].origin);
  EXPECT_EQ(0, active[0].queued_us);
  EXPECT_EQ(10, active[0].stats.input_docs);

  registry.Dispatch(id);
  active = registry.Active(7);
  ASSERT_EQ(1, active.size());
  EXPECT_EQ(0, active[0].queued_us);
  EXPECT_LT(0, active[0].started_us);
  registry.Finish(id, MergeOutcome::Busy, {}, {}, kKept);
}

TEST(MergeRegistry, DescribedInputsSumEachInputsLiveBytes) {
  const SegmentStub whole{"_1", 100, 100, 1000};
  const SegmentStub purged{"_2", 100, 1, 10};
  const irs::SubReader* const segments[] = {&whole, &purged};
  const auto inputs = DescribeInputs(segments);
  EXPECT_EQ(1010, inputs.input_bytes);
  EXPECT_EQ(1000, inputs.input_live_bytes);
}

TEST(MergeRegistry, MergesThatProduceASegmentCountTheirBytes) {
  MergeRegistry registry;
  const auto before =
    sdb::metrics::Get(sdb::metrics::Gauge::CompactionWrittenBytes);
  irs::MergeStats stats;
  stats.output_bytes = 70;
  registry.Finish(registry.Queue(7, MergeOrigin::Sql, {}), MergeOutcome::Merged,
                  stats, {}, kKept);
  registry.Finish(registry.Queue(7, MergeOrigin::Sql, {}), MergeOutcome::Failed,
                  stats, {}, kKept);
  EXPECT_EQ(before + 70,
            sdb::metrics::Get(sdb::metrics::Gauge::CompactionWrittenBytes));
}

TEST(MergeRegistry, FinishedMergeMovesToHistory) {
  constexpr uint64_t kRelation = 0x5eed'0001;
  MergeRegistry registry;
  const auto id = registry.Queue(kRelation, MergeOrigin::Sql, {});
  registry.Dispatch(id);
  EXPECT_TRUE(IsRunning(registry, kRelation));

  irs::MergeStats stats;
  stats.inputs = {"_1", "_2"};
  stats.input_docs = 10;
  stats.input_live_docs = 9;
  stats.input_bytes = 100;
  stats.output = "_3";
  stats.output_docs = 9;
  stats.output_bytes = 90;
  stats.profile.Add(irs::MergeStep::Columns, 1000);
  registry.Finish(id, MergeOutcome::Merged, stats, {}, kKept);
  EXPECT_FALSE(IsRunning(registry, kRelation));

  const auto history = HistoryOf(registry, kRelation);
  ASSERT_EQ(1, history.size());
  const auto& record = history.front();
  EXPECT_EQ(id, record.id);
  EXPECT_EQ(MergeOrigin::Sql, record.origin);
  EXPECT_EQ(MergeOutcome::Merged, record.outcome);
  EXPECT_LE(record.started_us, record.finished_us);
  EXPECT_EQ(stats.inputs, record.stats.inputs);
  EXPECT_EQ("_3", record.stats.output);
  EXPECT_EQ(9, record.stats.output_docs);
  EXPECT_EQ(1000, record.stats.profile.Nanos(irs::MergeStep::Columns));
  EXPECT_TRUE(record.error.empty());
}

TEST(MergeRegistry, NothingIsNotKept) {
  constexpr uint64_t kRelation = 0x5eed'0002;
  MergeRegistry registry;
  const auto id = registry.Queue(kRelation, MergeOrigin::Background, {});
  registry.Finish(id, MergeOutcome::Nothing, {}, {}, kKept);
  EXPECT_FALSE(IsRunning(registry, kRelation));
  EXPECT_TRUE(HistoryOf(registry, kRelation).empty());
}

TEST(MergeRegistry, FinishOfUnknownMergeIsIgnored) {
  constexpr uint64_t kRelation = 0x5eed'0003;
  MergeRegistry registry;
  registry.Finish(0, MergeOutcome::Merged, {}, {}, kKept);
  EXPECT_TRUE(HistoryOf(registry, kRelation).empty());
}

TEST(MergeRegistry, HistoryIsBounded) {
  constexpr uint64_t kRelation = 0x5eed'0004;
  constexpr size_t kSize = 3;
  absl::Cleanup reset = [] { Exec("RESET GLOBAL sdb_compact_history_size"); };
  Exec(absl::StrCat("SET GLOBAL sdb_compact_history_size = ", kSize));
  MergeRegistry registry;
  const auto total = kSize + 5;
  std::vector<uint64_t> ids;
  for (size_t i = 0; i < total; ++i) {
    ids.emplace_back(registry.Queue(kRelation, MergeOrigin::Vacuum, {}));
    registry.Finish(ids.back(), MergeOutcome::Busy, {}, {},
                    CompactHistorySize());
  }
  auto history = HistoryOf(registry, kRelation);
  ASSERT_EQ(kSize, history.size());
  EXPECT_EQ(ids[total - kSize], history.front().id);
  EXPECT_EQ(ids.back(), history.back().id);

  Exec("SET GLOBAL sdb_compact_history_size = 1");
  history = HistoryOf(registry, kRelation);
  ASSERT_EQ(1, history.size());
  EXPECT_EQ(ids.back(), history.front().id);

  Exec("SET GLOBAL sdb_compact_history_size = 0");
  EXPECT_TRUE(HistoryOf(registry, kRelation).empty());
  registry.Finish(registry.Queue(kRelation, MergeOrigin::Vacuum, {}),
                  MergeOutcome::Busy, {}, {}, CompactHistorySize());
  EXPECT_TRUE(HistoryOf(registry, kRelation).empty());
}

TEST(MergeRegistry, RunningMergesAreOrderedById) {
  constexpr uint64_t kRelation = 0x5eed'0008;
  MergeRegistry registry;
  const std::vector<uint64_t> started{
    registry.Queue(kRelation, MergeOrigin::Sql, {}),
    registry.Queue(kRelation, MergeOrigin::Sql, {}),
    registry.Queue(kRelation, MergeOrigin::Sql, {})};
  std::vector<uint64_t> running;
  for (const auto& record : registry.Active(kRelation)) {
    running.emplace_back(record.id);
  }
  EXPECT_EQ(started, running);
  for (const auto id : started) {
    registry.Finish(id, MergeOutcome::Busy, {}, {}, kKept);
  }
}

TEST(MergeRegistry, NothingWithAnErrorIsKept) {
  constexpr uint64_t kRelation = 0x5eed'000a;
  MergeRegistry registry;
  const auto id = registry.Queue(kRelation, MergeOrigin::Background, {});
  registry.Finish(id, MergeOutcome::Nothing, {}, "interrupted", kKept);
  const auto history = HistoryOf(registry, kRelation);
  ASSERT_EQ(1, history.size());
  EXPECT_EQ(MergeOutcome::Nothing, history.front().outcome);
  EXPECT_EQ("interrupted", history.front().error);
}

TEST(MergeRegistry, StepTimesWithoutInputsAreKept) {
  constexpr uint64_t kRelation = 0x5eed'000b;
  MergeRegistry registry;
  const auto id = registry.Queue(kRelation, MergeOrigin::Vacuum, {});
  irs::MergeStats stats;
  stats.profile.Add(irs::MergeStep::Prepare, 5);
  registry.Finish(id, MergeOutcome::Failed, stats, {}, kKept);
  const auto history = HistoryOf(registry, kRelation);
  ASSERT_EQ(1, history.size());
  EXPECT_EQ(5, history.front().stats.profile.Nanos(irs::MergeStep::Prepare));
}

}  // namespace
