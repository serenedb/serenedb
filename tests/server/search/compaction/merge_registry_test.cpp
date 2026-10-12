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
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "search/compaction/merge_registry.h"

namespace {

using sdb::search::MergeOrigin;
using sdb::search::MergeOutcome;
using sdb::search::MergeRecord;
using sdb::search::MergeRecorder;
using sdb::search::MergeRegistry;

std::vector<MergeRecord> HistoryOf(uint64_t relation_id) {
  auto records = MergeRegistry::Instance().History();
  std::erase_if(records, [&](const MergeRecord& record) {
    return record.relation_id != relation_id;
  });
  return records;
}

bool IsRunning(uint64_t relation_id) {
  const auto running = MergeRegistry::Instance().Running();
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
    : _meta{
        .name = std::move(name), .docs_count = docs, .live_docs_count = docs} {}

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
  EXPECT_EQ(MergeOrigin::Sql, record.origin);
  EXPECT_EQ(MergeOutcome::Merged, record.outcome);
  EXPECT_LE(record.started_us, record.finished_us);
  EXPECT_EQ(stats.inputs, record.stats.inputs);
  EXPECT_EQ("_3", record.stats.output);
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
  EXPECT_EQ(MergeRegistry::kHistoryCapacity, registry.History().size());
}

TEST(MergeRegistry, RunningMergesAreOrderedById) {
  constexpr uint64_t kRelation = 0x5eed'0008;
  auto& registry = MergeRegistry::Instance();
  const std::vector<uint64_t> started{
    registry.Start(kRelation, MergeOrigin::Sql),
    registry.Start(kRelation, MergeOrigin::Sql),
    registry.Start(kRelation, MergeOrigin::Sql)};
  std::vector<uint64_t> running;
  for (const auto& record : registry.Running()) {
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
