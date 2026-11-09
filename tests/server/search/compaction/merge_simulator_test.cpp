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

#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "merge_simulator.h"
#include "search/compaction/merge_planner.h"

namespace {

using sdb::search::MergePicker;
using sdb::search::MergeSimulation;
using sdb::search::SimulatedEvent;
using sdb::search::SimulatedEventType;
using sdb::search::SimulatedFlush;
using sdb::search::SimulatedSegment;
using sdb::search::SimulateMerges;

constexpr uint64_t kMiB = uint64_t{1} << 20;

absl::Time At(double seconds) {
  return absl::FromUnixSeconds(1'000'000) + absl::Seconds(seconds);
}

SimulatedFlush Flush(double seconds, uint64_t bytes, uint64_t relation = 1) {
  return {.time = At(seconds),
          .relation_id = relation,
          .docs = 100,
          .live_docs = 100,
          .bytes = bytes};
}

irs::MergeGroups PickUntaken(const irs::IndexReader& index,
                             const irs::CompactingSegments& taken,
                             size_t at_least) {
  irs::Compaction candidates;
  for (const auto& segment : index) {
    if (!taken.contains(segment.Meta().name)) {
      candidates.emplace_back(&segment);
    }
  }
  if (candidates.size() < at_least) {
    candidates.clear();
  }
  irs::MergeGroups groups;
  if (!candidates.empty()) {
    groups.emplace_back(std::move(candidates));
  }
  return groups;
}

MergePicker MergeAtLeast(size_t count) {
  return
    [count](const irs::IndexReader& index, const irs::CompactingSegments& taken,
            size_t) { return PickUntaken(index, taken, count); };
}

MergeSimulation Options(size_t slots, size_t at_least = 2) {
  MergeSimulation options;
  options.pick = MergeAtLeast(at_least);
  options.slots = slots;
  options.bytes_per_second = double{kMiB};
  return options;
}

std::vector<std::string> Names(std::initializer_list<const char*> names) {
  return {names.begin(), names.end()};
}

TEST(SimulatedSegment, ExposesOnlyItsMetadata) {
  const SimulatedSegment segment{
    irs::SegmentInfo{.name = "_1", .docs_count = 3, .live_docs_count = 2}};
  EXPECT_EQ(&segment, &*segment);
  EXPECT_EQ("_1", segment.Meta().name);
  EXPECT_EQ(2, segment.live_docs_count());
  EXPECT_EQ(0, segment.CountMappedMemory());
  EXPECT_EQ(nullptr, segment.docs_mask());
  EXPECT_EQ(nullptr, segment.docs_iterator());
  EXPECT_TRUE(segment.field_ids().empty());
  EXPECT_EQ(nullptr, segment.field(0));
  EXPECT_EQ(nullptr, segment.norms(0));
}

TEST(MergeSimulator, TheAdjacentPolicyMergesInFlushOrder) {
  auto options = Options(1);
  sdb::search::TasksSettings settings;
  settings.compaction_max_segments = 3;
  settings.compaction_max_segments_bytes = uint64_t{5} << 30;
  settings.compaction_floor_segment_bytes = 2 * kMiB;
  options.pick = [&settings](const irs::IndexReader& index,
                             const irs::CompactingSegments& taken,
                             size_t wanted) {
    return sdb::search::PickMerges(settings, std::numeric_limits<size_t>::max(),
                                   index, taken, {}, wanted);
  };
  const auto events =
    SimulateMerges({Flush(0, kMiB), Flush(0, kMiB), Flush(0, kMiB)}, options);
  ASSERT_EQ(4, events.size());
  const auto& merge = events[3];
  EXPECT_EQ(SimulatedEventType::Merge, merge.type);
  EXPECT_EQ(Names({"_1", "_2", "_3"}), merge.inputs);
}

TEST(MergeSimulator, AnEmptyTraceHasNoEvents) {
  EXPECT_TRUE(SimulateMerges({}, Options(1)).empty());
}

TEST(MergeSimulator, FlushesReplayInTimeOrder) {
  const auto events =
    SimulateMerges({Flush(2, 3 * kMiB), Flush(1, 2 * kMiB)}, Options(1, 3));
  ASSERT_EQ(2, events.size());
  EXPECT_EQ(SimulatedEventType::Flush, events[0].type);
  EXPECT_EQ(absl::ZeroDuration(), events[0].at);
  EXPECT_EQ(1, events[0].relation_id);
  EXPECT_EQ("_1", events[0].segment);
  EXPECT_EQ(2 * kMiB, events[0].bytes);
  EXPECT_EQ(1, events[0].segments);
  EXPECT_EQ(2 * kMiB, events[0].segment_bytes);
  EXPECT_EQ(absl::Seconds(1), events[1].at);
  EXPECT_EQ("_2", events[1].segment);
  EXPECT_EQ(2, events[1].segments);
  EXPECT_EQ(5 * kMiB, events[1].segment_bytes);
  EXPECT_EQ(0, events[1].merges_running);
}

TEST(MergeSimulator, AMergeTakesItsBytesOverTheThroughput) {
  const auto events =
    SimulateMerges({Flush(0, 2 * kMiB), Flush(1, 2 * kMiB)}, Options(1));
  ASSERT_EQ(3, events.size());
  EXPECT_EQ(1, events[1].merges_running);
  const auto& merge = events[2];
  EXPECT_EQ(SimulatedEventType::Merge, merge.type);
  EXPECT_EQ(absl::Seconds(5), merge.at);
  EXPECT_EQ("_3", merge.segment);
  EXPECT_EQ(Names({"_1", "_2"}), merge.inputs);
  EXPECT_EQ(200, merge.docs);
  EXPECT_EQ(200, merge.live_docs);
  EXPECT_EQ(4 * kMiB, merge.bytes);
  EXPECT_EQ(4 * kMiB, merge.input_bytes);
  EXPECT_EQ(absl::Seconds(1), merge.started);
  EXPECT_TRUE(merge.small);
  EXPECT_EQ(1, merge.segments);
  EXPECT_EQ(4 * kMiB, merge.segment_bytes);
  EXPECT_EQ(0, merge.merges_running);
}

TEST(MergeSimulator, AMergePurgesDeletedDocs) {
  auto first = Flush(0, 2 * kMiB);
  first.live_docs = 50;
  const auto events = SimulateMerges({first, Flush(0, 2 * kMiB)}, Options(1));
  ASSERT_EQ(3, events.size());
  EXPECT_EQ(50, events[0].live_docs);
  const auto& merge = events[2];
  EXPECT_EQ(absl::Seconds(3), merge.at);
  EXPECT_EQ(150, merge.docs);
  EXPECT_EQ(3 * kMiB, merge.bytes);
  EXPECT_EQ(4 * kMiB, merge.input_bytes);
}

TEST(MergeSimulator, BigMergesLeaveASlotForSmallOnes) {
  auto options = Options(2);
  options.bytes_per_second = double{32 * kMiB};
  const auto events = SimulateMerges(
    {Flush(0, 64 * kMiB, 1), Flush(0, 64 * kMiB, 1), Flush(0, 64 * kMiB, 2),
     Flush(0, 64 * kMiB, 2), Flush(1, kMiB, 3), Flush(1, kMiB, 3)},
    options);
  ASSERT_EQ(9, events.size());
  EXPECT_EQ(2, events[3].relation_id);
  EXPECT_EQ(1, events[3].merges_running);
  EXPECT_EQ(2, events[5].merges_running);
  const auto& small = events[6];
  EXPECT_EQ(3, small.relation_id);
  EXPECT_TRUE(small.small);
  EXPECT_EQ(absl::Seconds(1), small.started);
  EXPECT_EQ(absl::Seconds(1.0625), small.at);
  const auto& big = events[7];
  EXPECT_EQ(1, big.relation_id);
  EXPECT_FALSE(big.small);
  EXPECT_EQ(absl::ZeroDuration(), big.started);
  EXPECT_EQ(absl::Seconds(4), big.at);
  const auto& waited = events[8];
  EXPECT_EQ(2, waited.relation_id);
  EXPECT_FALSE(waited.small);
  EXPECT_EQ(absl::Seconds(4), waited.started);
  EXPECT_EQ(absl::Seconds(8), waited.at);
}

TEST(MergeSimulator, RelationsShareTheMergeSlots) {
  const auto events = SimulateMerges(
    {Flush(0, 2 * kMiB, 1), Flush(0, 2 * kMiB, 1), Flush(1, 2 * kMiB, 2),
     Flush(1, 2 * kMiB, 2), Flush(2, 2 * kMiB, 3), Flush(2, 2 * kMiB, 3)},
    Options(2));
  ASSERT_EQ(9, events.size());
  EXPECT_EQ(2, events[3].merges_running);
  EXPECT_EQ(3, events[5].relation_id);
  EXPECT_EQ(2, events[5].merges_running);
  EXPECT_EQ(1, events[6].relation_id);
  EXPECT_EQ(absl::Seconds(4), events[6].at);
  EXPECT_EQ(2, events[6].merges_running);
  EXPECT_EQ(2, events[7].relation_id);
  EXPECT_EQ(absl::Seconds(5), events[7].at);
  const auto& waited = events[8];
  EXPECT_EQ(3, waited.relation_id);
  EXPECT_EQ("_3", waited.segment);
  EXPECT_EQ(absl::Seconds(4), waited.started);
  EXPECT_EQ(absl::Seconds(8), waited.at);
}

TEST(MergeSimulator, AFlushAtAMergesEndComesFirst) {
  const auto events = SimulateMerges(
    {Flush(0, kMiB), Flush(0, kMiB), Flush(2, kMiB), Flush(3, kMiB)},
    Options(1, 2));
  ASSERT_EQ(7, events.size());
  EXPECT_EQ(SimulatedEventType::Flush, events[2].type);
  EXPECT_EQ(absl::Seconds(2), events[2].at);
  EXPECT_EQ(SimulatedEventType::Merge, events[3].type);
  EXPECT_EQ(absl::Seconds(2), events[3].at);
  EXPECT_EQ(Names({"_1", "_2"}), events[3].inputs);
  EXPECT_EQ(SimulatedEventType::Flush, events[4].type);
  EXPECT_EQ(absl::Seconds(3), events[4].at);
  EXPECT_EQ(absl::Seconds(5), events[5].at);
  EXPECT_EQ(Names({"_4", "_3"}), events[5].inputs);
  EXPECT_EQ(absl::Seconds(9), events[6].at);
  EXPECT_EQ(Names({"_6", "_5"}), events[6].inputs);
}

TEST(MergeSimulator, AFailingPolicyStopsTheReplay) {
  auto options = Options(1);
  options.pick = [](const irs::IndexReader& index,
                    const irs::CompactingSegments& taken, size_t) {
    if (index.size() > 2) {
      throw std::runtime_error{"policy failed"};
    }
    return PickUntaken(index, taken, 2);
  };
  EXPECT_THROW(
    SimulateMerges({Flush(0, 2 * kMiB), Flush(0, 2 * kMiB), Flush(1, kMiB)},
                   options),
    std::runtime_error);
}

}  // namespace
