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

#include <absl/time/clock.h>
#include <absl/time/time.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "search/compaction/merge_registry.h"
#include "server/utils/metrics.h"

namespace {

using sdb::search::MergeEstimate;
using sdb::search::MergeLane;
using sdb::search::MergeOrigin;
using sdb::search::MergeOutcome;
using sdb::search::MergeRecord;
using sdb::search::MergeRecorder;
using sdb::search::MergeRegistry;
using sdb::search::SegmentEvent;

std::vector<MergeRecord> HistoryOf(uint64_t relation_id) {
  auto records = MergeRegistry::Instance().History();
  std::erase_if(records, [&](const MergeRecord& record) {
    return record.relation_id != relation_id;
  });
  return records;
}

size_t CountOf(std::span<const MergeRecord> records, SegmentEvent event) {
  return static_cast<size_t>(std::count_if(
    records.begin(), records.end(),
    [&](const MergeRecord& record) { return record.event == event; }));
}

void LetAMicrosecondPass() {
  const auto start = absl::Now();
  while (absl::Now() - start < absl::Microseconds(2)) {
  }
}

bool IsRunning(uint64_t relation_id) {
  const auto running = MergeRegistry::Instance().Active();
  return std::any_of(running.begin(), running.end(),
                     [&](const MergeRecord& record) {
                       return record.relation_id == relation_id;
                     });
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
  const irs::TermReader* field(irs::field_id) const final { return nullptr; }
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
  EXPECT_EQ("background",
            sdb::search::MergeOriginName(MergeOrigin::Background));
  EXPECT_EQ("vacuum", sdb::search::MergeOriginName(MergeOrigin::Vacuum));
  EXPECT_EQ("sql", sdb::search::MergeOriginName(MergeOrigin::Sql));
  EXPECT_EQ("running", sdb::search::MergeOutcomeName(MergeOutcome::Running));
  EXPECT_EQ("merged", sdb::search::MergeOutcomeName(MergeOutcome::Merged));
  EXPECT_EQ("deferred", sdb::search::MergeOutcomeName(MergeOutcome::Deferred));
  EXPECT_EQ("nothing", sdb::search::MergeOutcomeName(MergeOutcome::Nothing));
  EXPECT_EQ("busy", sdb::search::MergeOutcomeName(MergeOutcome::Busy));
  EXPECT_EQ("failed", sdb::search::MergeOutcomeName(MergeOutcome::Failed));
  EXPECT_EQ("cancelled",
            sdb::search::MergeOutcomeName(MergeOutcome::Cancelled));
  EXPECT_EQ("small", sdb::search::MergeLaneName(MergeLane::Small));
  EXPECT_EQ("big", sdb::search::MergeLaneName(MergeLane::Big));
  EXPECT_EQ("flush", sdb::search::SegmentEventName(SegmentEvent::Flush));
  EXPECT_EQ("merge", sdb::search::SegmentEventName(SegmentEvent::Merge));
}

TEST(MergeRegistry, AFlushRecordsEverySegmentItWrote) {
  MergeRegistry registry;
  const SegmentStub first{"_1", 10, 8, 100};
  const SegmentStub second{"_2", 5, 5, 50};
  const irs::SubReader* const segments[] = {&first, &second};
  registry.RecordFlushes(7, segments);
  EXPECT_TRUE(registry.Active().empty());

  const auto history = registry.History();
  ASSERT_EQ(2, history.size());
  const auto& record = history[0];
  EXPECT_EQ(SegmentEvent::Flush, record.event);
  EXPECT_EQ(7, record.relation_id);
  EXPECT_EQ(0, record.started_us);
  EXPECT_LT(0, record.finished_us);
  EXPECT_TRUE(record.stats.inputs.empty());
  EXPECT_EQ("_1", record.stats.output);
  EXPECT_EQ(10, record.stats.output_docs);
  EXPECT_EQ(8, record.output_live_docs);
  EXPECT_EQ(100, record.stats.output_bytes);
  EXPECT_FALSE(record.rowid_min);
  EXPECT_FALSE(record.rowid_max);
  EXPECT_EQ("_2", history[1].stats.output);
  EXPECT_EQ(record.finished_us, history[1].finished_us);
  EXPECT_LT(record.id, history[1].id);
}

TEST(MergeRegistry, FlushesAndMergesShareOneTimeline) {
  MergeRegistry registry;
  const SegmentStub segment{"_1", 10};
  const irs::SubReader* const segments[] = {&segment};
  const auto merge = registry.Start(7, MergeOrigin::Sql);
  registry.RecordFlushes(7, segments);
  LetAMicrosecondPass();
  registry.Finish(merge, MergeOutcome::Merged, {}, {});

  const auto history = registry.History();
  ASSERT_EQ(2, history.size());
  EXPECT_EQ(SegmentEvent::Flush, history[0].event);
  EXPECT_LT(merge, history[0].id);
  EXPECT_EQ(SegmentEvent::Merge, history[1].event);
  EXPECT_EQ(merge, history[1].id);
}

TEST(MergeRegistry, FlushesKeepTheirOwnBoundedHistory) {
  MergeRegistry registry;
  const auto merge = registry.Start(7, MergeOrigin::Sql);
  registry.Finish(merge, MergeOutcome::Busy, {}, {});
  const SegmentStub segment{"_1", 1};
  const irs::SubReader* const segments[] = {&segment};
  const auto total = MergeRegistry::kFlushHistoryCapacity + 5;
  for (size_t i = 0; i < total; ++i) {
    registry.RecordFlushes(7, segments);
  }

  const auto history = registry.History();
  EXPECT_EQ(1, CountOf(history, SegmentEvent::Merge));
  ASSERT_EQ(MergeRegistry::kFlushHistoryCapacity,
            CountOf(history, SegmentEvent::Flush));
  uint64_t oldest = UINT64_MAX;
  for (const auto& record : history) {
    if (record.event == SegmentEvent::Flush) {
      oldest = std::min(oldest, record.id);
    }
  }
  EXPECT_EQ(merge + 1 + total - MergeRegistry::kFlushHistoryCapacity, oldest);
}

TEST(MergeRegistry, AQueuedMergeRunsAndFinishesUnderOneId) {
  MergeRegistry registry;
  const SegmentStub first{"_1", 10};
  const SegmentStub second{"_2", 30};
  const irs::SubReader* const segments[] = {&first, &second};
  const auto id = registry.Queue(
    7, segments, MergeEstimate{.score = 2.5, .lane = MergeLane::Small});
  auto active = registry.Active();
  ASSERT_EQ(1, active.size());
  EXPECT_TRUE(active[0].Queued());
  EXPECT_EQ(7, active[0].relation_id);
  EXPECT_EQ(MergeOrigin::Background, active[0].origin);
  EXPECT_LT(0, active[0].queued_us);
  EXPECT_EQ(MergeLane::Small, active[0].lane);
  EXPECT_DOUBLE_EQ(2.5, active[0].score);
  EXPECT_EQ(40, active[0].stats.input_docs);
  EXPECT_EQ(0, active[0].threads);

  registry.Dispatch(id, 3);
  active = registry.Active();
  ASSERT_EQ(1, active.size());
  EXPECT_FALSE(active[0].Queued());
  EXPECT_EQ(3, active[0].threads);
  EXPECT_LE(active[0].queued_us, active[0].started_us);

  registry.Finish(id, MergeOutcome::Cancelled, {}, {});
  EXPECT_TRUE(registry.Active().empty());
  const auto history = registry.History();
  ASSERT_EQ(1, history.size());
  EXPECT_EQ(id, history[0].id);
  EXPECT_EQ(MergeOutcome::Cancelled, history[0].outcome);
  EXPECT_EQ(std::vector<std::string>({"_1", "_2"}), history[0].stats.inputs);
}

TEST(MergeRegistry, ADroppedQueuedMergeLeavesNoHistory) {
  MergeRegistry registry;
  const SegmentStub segment{"_1", 10};
  const auto id = registry.Queue(7, irs::Compaction{&segment}, {});
  registry.Drop(id);
  registry.Dispatch(id, 1);
  EXPECT_TRUE(registry.Active().empty());
  EXPECT_TRUE(registry.History().empty());
}

TEST(MergeRegistry, EstimatedBytesFollowTheLiveShare) {
  MergeRecord record;
  EXPECT_EQ(0, record.EstimatedBytes());
  record.stats.input_docs = 10;
  record.stats.input_live_docs = 4;
  record.stats.input_bytes = 1000;
  EXPECT_EQ(400, record.EstimatedBytes());
}

TEST(MergeRegistry, MergesThatProduceASegmentCountTheirBytes) {
  MergeRegistry registry;
  const auto before =
    sdb::metrics::Get(sdb::metrics::Gauge::CompactionWrittenBytes);
  irs::MergeStats stats;
  stats.output_bytes = 70;
  registry.Finish(registry.Start(7, MergeOrigin::Sql), MergeOutcome::Merged,
                  stats, {});
  registry.Finish(registry.Start(7, MergeOrigin::Sql), MergeOutcome::Failed,
                  stats, {});
  EXPECT_EQ(before + 70,
            sdb::metrics::Get(sdb::metrics::Gauge::CompactionWrittenBytes));
}

TEST(MergeRegistry, FinishedMergeMovesToHistory) {
  constexpr uint64_t kRelation = 0x5eed'0001;
  auto& registry = MergeRegistry::Instance();
  const auto id = registry.Start(kRelation, MergeOrigin::Sql);
  EXPECT_TRUE(IsRunning(kRelation));

  irs::MergeStats stats;
  stats.inputs = {"_1", "_2"};
  stats.input_docs = 10;
  stats.input_live_docs = 9;
  stats.input_bytes = 100;
  stats.output = "_3";
  stats.output_docs = 9;
  stats.output_bytes = 90;
  stats.profile.Add(irs::MergeStep::Columns, 1000);
  registry.Finish(id, MergeOutcome::Merged, stats, {});
  EXPECT_FALSE(IsRunning(kRelation));

  const auto history = HistoryOf(kRelation);
  ASSERT_EQ(1, history.size());
  const auto& record = history.front();
  EXPECT_EQ(id, record.id);
  EXPECT_EQ(SegmentEvent::Merge, record.event);
  EXPECT_EQ(MergeOrigin::Sql, record.origin);
  EXPECT_EQ(MergeOutcome::Merged, record.outcome);
  EXPECT_LE(record.started_us, record.finished_us);
  EXPECT_EQ(stats.inputs, record.stats.inputs);
  EXPECT_EQ("_3", record.stats.output);
  EXPECT_EQ(9, record.output_live_docs);
  EXPECT_EQ(1000, record.stats.profile.Nanos(irs::MergeStep::Columns));
  EXPECT_TRUE(record.error.empty());
}

TEST(MergeRegistry, NothingIsNotKept) {
  constexpr uint64_t kRelation = 0x5eed'0002;
  auto& registry = MergeRegistry::Instance();
  const auto id = registry.Start(kRelation, MergeOrigin::Background);
  registry.Finish(id, MergeOutcome::Nothing, {}, {});
  EXPECT_FALSE(IsRunning(kRelation));
  EXPECT_TRUE(HistoryOf(kRelation).empty());
}

TEST(MergeRegistry, FinishOfUnknownMergeIsIgnored) {
  constexpr uint64_t kRelation = 0x5eed'0003;
  MergeRegistry::Instance().Finish(0, MergeOutcome::Merged, {}, {});
  EXPECT_TRUE(HistoryOf(kRelation).empty());
}

TEST(MergeRegistry, HistoryIsBounded) {
  constexpr uint64_t kRelation = 0x5eed'0004;
  auto& registry = MergeRegistry::Instance();
  const auto total = MergeRegistry::kHistoryCapacity + 5;
  std::vector<uint64_t> ids;
  for (size_t i = 0; i < total; ++i) {
    ids.emplace_back(registry.Start(kRelation, MergeOrigin::Vacuum));
    registry.Finish(ids.back(), MergeOutcome::Busy, {}, {});
  }
  const auto history = HistoryOf(kRelation);
  ASSERT_EQ(MergeRegistry::kHistoryCapacity, history.size());
  EXPECT_EQ(ids[total - MergeRegistry::kHistoryCapacity], history.front().id);
  EXPECT_EQ(ids.back(), history.back().id);
  EXPECT_EQ(MergeRegistry::kHistoryCapacity,
            CountOf(registry.History(), SegmentEvent::Merge));
}

TEST(MergeRegistry, RunningMergesAreOrderedById) {
  constexpr uint64_t kRelation = 0x5eed'0008;
  auto& registry = MergeRegistry::Instance();
  const std::vector<uint64_t> started{
    registry.Start(kRelation, MergeOrigin::Sql),
    registry.Start(kRelation, MergeOrigin::Sql),
    registry.Start(kRelation, MergeOrigin::Sql)};
  std::vector<uint64_t> running;
  for (const auto& record : registry.Active()) {
    if (record.relation_id == kRelation) {
      running.emplace_back(record.id);
    }
  }
  EXPECT_EQ(started, running);
  for (const auto id : started) {
    registry.Finish(id, MergeOutcome::Busy, {}, {});
  }
}

TEST(MergeRegistry, InputsOfAFinishedMergeAreIgnored) {
  constexpr uint64_t kRelation = 0x5eed'0009;
  auto& registry = MergeRegistry::Instance();
  const auto id = registry.Start(kRelation, MergeOrigin::Sql);
  registry.Finish(id, MergeOutcome::Busy, {}, {});
  const SegmentStub segment{"_1", 10};
  registry.SetInputs(id, irs::Compaction{&segment});
  const auto history = HistoryOf(kRelation);
  ASSERT_EQ(1, history.size());
  EXPECT_TRUE(history.front().stats.inputs.empty());
}

TEST(MergeRegistry, NothingWithAnErrorIsKept) {
  constexpr uint64_t kRelation = 0x5eed'000a;
  auto& registry = MergeRegistry::Instance();
  const auto id = registry.Start(kRelation, MergeOrigin::Background);
  registry.Finish(id, MergeOutcome::Nothing, {}, "interrupted");
  const auto history = HistoryOf(kRelation);
  ASSERT_EQ(1, history.size());
  EXPECT_EQ(MergeOutcome::Nothing, history.front().outcome);
  EXPECT_EQ("interrupted", history.front().error);
}

TEST(MergeRegistry, StepTimesWithoutInputsAreKept) {
  constexpr uint64_t kRelation = 0x5eed'000b;
  auto& registry = MergeRegistry::Instance();
  const auto id = registry.Start(kRelation, MergeOrigin::Vacuum);
  irs::MergeStats stats;
  stats.profile.Add(irs::MergeStep::Prepare, 5);
  registry.Finish(id, MergeOutcome::Failed, stats, {});
  const auto history = HistoryOf(kRelation);
  ASSERT_EQ(1, history.size());
  EXPECT_EQ(5, history.front().stats.profile.Nanos(irs::MergeStep::Prepare));
}

TEST(MergeRecorder, DroppedMidMergeRecordsAFailure) {
  constexpr uint64_t kRelation = 0x5eed'000c;
  const SegmentStub segment{"_1", 10};
  const irs::CompactionPolicy policy =
    [&](irs::Compaction& candidates, const irs::IndexReader&,
        const irs::CompactingSegments&) { candidates.emplace_back(&segment); };
  {
    MergeRecorder recorder{kRelation, MergeOrigin::Background};
    const auto wrapped = recorder.Wrap(policy);
    for (int round = 0; round < 2; ++round) {
      irs::Compaction candidates;
      wrapped(candidates, irs::SubReader::empty(), {});
    }
    EXPECT_TRUE(IsRunning(kRelation));
  }
  EXPECT_FALSE(IsRunning(kRelation));
  const auto history = HistoryOf(kRelation);
  ASSERT_EQ(1, history.size());
  EXPECT_EQ(MergeOutcome::Failed, history.front().outcome);
  EXPECT_EQ(std::vector<std::string>{"_1"}, history.front().stats.inputs);
  EXPECT_EQ(10, history.front().stats.input_docs);
}

TEST(MergeRecorder, AQueuedMergeKeepsItsId) {
  constexpr uint64_t kRelation = 0x5eed'000d;
  auto& registry = MergeRegistry::Instance();
  const SegmentStub segment{"_1", 10};
  const auto id = registry.Queue(kRelation, irs::Compaction{&segment}, {});
  registry.Dispatch(id, 1);
  {
    MergeRecorder recorder{kRelation, MergeOrigin::Background, id};
    recorder.Begin(irs::Compaction{&segment});
    EXPECT_TRUE(IsRunning(kRelation));
    recorder.Finish(Result(irs::CompactionError::Ok, 1));
  }
  const auto history = HistoryOf(kRelation);
  ASSERT_EQ(1, history.size());
  EXPECT_EQ(id, history[0].id);
  EXPECT_EQ(MergeOutcome::Merged, history[0].outcome);
  EXPECT_EQ(std::vector<std::string>{"_1"}, history[0].stats.inputs);
}

TEST(MergeRecorder, UnstartedRecorderLeavesNothing) {
  constexpr uint64_t kRelation = 0x5eed'0005;
  {
    MergeRecorder recorder{kRelation, MergeOrigin::Background};
    recorder.Finish(Result(irs::CompactionError::Ok, 0));
  }
  {
    MergeRecorder recorder{kRelation, MergeOrigin::Background};
  }
  EXPECT_FALSE(IsRunning(kRelation));
  EXPECT_TRUE(HistoryOf(kRelation).empty());
}

TEST(MergeRecorder, FailureIsRecordedWithItsError) {
  constexpr uint64_t kRelation = 0x5eed'0006;
  {
    MergeRecorder recorder{kRelation, MergeOrigin::Vacuum};
    recorder.Fail("disk full");
  }
  const auto history = HistoryOf(kRelation);
  ASSERT_EQ(1, history.size());
  EXPECT_EQ(MergeOutcome::Failed, history.front().outcome);
  EXPECT_EQ(MergeOrigin::Vacuum, history.front().origin);
  EXPECT_EQ("disk full", history.front().error);
}

TEST(MergeRecorder, WrappedPolicyThatPicksNothingStartsNothing) {
  constexpr uint64_t kRelation = 0x5eed'0007;
  bool called = false;
  const irs::CompactionPolicy policy =
    [&](irs::Compaction&, const irs::IndexReader&,
        const irs::CompactingSegments&) { called = true; };
  {
    MergeRecorder recorder{kRelation, MergeOrigin::Sql};
    const auto wrapped = recorder.Wrap(policy);
    irs::Compaction candidates;
    wrapped(candidates, irs::SubReader::empty(), {});
    EXPECT_TRUE(called);
    EXPECT_FALSE(IsRunning(kRelation));
    recorder.Finish(Result(irs::CompactionError::Ok, 0));
  }
  EXPECT_TRUE(HistoryOf(kRelation).empty());
}

}  // namespace
