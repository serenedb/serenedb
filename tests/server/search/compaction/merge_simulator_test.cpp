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

#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "search/compaction/merge_simulator.h"

namespace {

using sdb::search::MergeLane;
using sdb::search::MergeSimulation;
using sdb::search::SegmentEvent;
using sdb::search::SimulatedEvent;
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

SimulatedFlush Ranged(double seconds, int64_t first, int64_t last) {
  auto flush = Flush(seconds, kMiB);
  flush.rowid_min = first;
  flush.rowid_max = last;
  return flush;
}

void PickUntaken(irs::Compaction& candidates, const irs::IndexReader& index,
                 const irs::CompactingSegments& taken, size_t at_least) {
  for (const auto& segment : index) {
    if (!taken.contains(segment.Meta().name)) {
      candidates.emplace_back(&segment);
    }
  }
  if (candidates.size() < at_least) {
    candidates.clear();
  }
}

irs::CompactionPolicy MergeAtLeast(size_t count) {
  return [count](irs::Compaction& candidates, const irs::IndexReader& index,
                 const irs::CompactingSegments& taken) {
    PickUntaken(candidates, index, taken, count);
  };
}

MergeSimulation Options(size_t threads, size_t at_least = 2) {
  MergeSimulation options;
  options.policy = MergeAtLeast(at_least);
  options.threads = threads;
  options.helpers_per_merge = threads / 2;
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

TEST(MergeSimulator, AnEmptyTraceHasNoEvents) {
  EXPECT_TRUE(SimulateMerges({}, Options(1)).empty());
}

TEST(MergeSimulator, FlushesReplayInTimeOrder) {
  const auto events =
    SimulateMerges({Flush(2, 3 * kMiB), Flush(1, 2 * kMiB)}, Options(1, 3));
  ASSERT_EQ(2, events.size());
  EXPECT_EQ(SegmentEvent::Flush, events[0].type);
  EXPECT_EQ(absl::ZeroDuration(), events[0].at);
  EXPECT_EQ(1, events[0].relation_id);
  EXPECT_EQ("_1", events[0].segment);
  EXPECT_EQ(2 * kMiB, events[0].bytes);
  EXPECT_EQ(1, events[0].segments);
  EXPECT_EQ(2 * kMiB, events[0].segment_bytes);
  EXPECT_FALSE(events[0].overlap);
  EXPECT_FALSE(events[0].rowid_min);
  EXPECT_EQ(absl::Seconds(1), events[1].at);
  EXPECT_EQ("_2", events[1].segment);
  EXPECT_EQ(2, events[1].segments);
  EXPECT_EQ(5 * kMiB, events[1].segment_bytes);
  EXPECT_EQ(0, events[1].merges_queued);
  EXPECT_EQ(0, events[1].merges_running);
}

TEST(MergeSimulator, AMergeTakesItsBytesOverTheThroughput) {
  const auto events =
    SimulateMerges({Flush(0, 2 * kMiB), Flush(1, 2 * kMiB)}, Options(1));
  ASSERT_EQ(3, events.size());
  EXPECT_EQ(1, events[1].merges_running);
  const auto& merge = events[2];
  EXPECT_EQ(SegmentEvent::Merge, merge.type);
  EXPECT_EQ(absl::Seconds(5), merge.at);
  EXPECT_EQ("_3", merge.segment);
  EXPECT_EQ(Names({"_1", "_2"}), merge.inputs);
  EXPECT_EQ(200, merge.docs);
  EXPECT_EQ(200, merge.live_docs);
  EXPECT_EQ(4 * kMiB, merge.bytes);
  EXPECT_EQ(4 * kMiB, merge.input_bytes);
  EXPECT_EQ(absl::Seconds(1), merge.queued);
  EXPECT_EQ(absl::Seconds(1), merge.started);
  EXPECT_EQ(1, merge.threads);
  EXPECT_DOUBLE_EQ(double{6 * kMiB}, merge.score);
  EXPECT_EQ(MergeLane::Small, merge.lane);
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

TEST(MergeSimulator, BigMergesTakeHelperThreads) {
  auto options = Options(4);
  options.small_bytes = kMiB;
  const auto events =
    SimulateMerges({Flush(0, 2 * kMiB), Flush(0, 2 * kMiB)}, options);
  ASSERT_EQ(3, events.size());
  EXPECT_EQ(MergeLane::Big, events[2].lane);
  EXPECT_EQ(3, events[2].threads);
  EXPECT_EQ(absl::Seconds(4.0 / 3), events[2].at);
}

TEST(MergeSimulator, HelpersComeOnlyFromFreeThreads) {
  auto options = Options(2);
  options.small_bytes = kMiB;
  const auto events =
    SimulateMerges({Flush(0, 2 * kMiB), Flush(0, 2 * kMiB)}, options);
  ASSERT_EQ(3, events.size());
  EXPECT_EQ(MergeLane::Big, events[2].lane);
  EXPECT_EQ(1, events[2].threads);
  EXPECT_EQ(absl::Seconds(4), events[2].at);
}

TEST(MergeSimulator, RelationsShareTheMergeThreads) {
  const auto events =
    SimulateMerges({Flush(0, 2 * kMiB, 1), Flush(0, 2 * kMiB, 1),
                    Flush(1, 2 * kMiB, 2), Flush(1, 2 * kMiB, 2)},
                   Options(1));
  ASSERT_EQ(6, events.size());
  EXPECT_EQ(2, events[3].relation_id);
  EXPECT_EQ(1, events[3].merges_queued);
  EXPECT_EQ(1, events[3].merges_running);
  EXPECT_EQ(1, events[4].relation_id);
  EXPECT_EQ(absl::Seconds(4), events[4].at);
  EXPECT_EQ(0, events[4].merges_queued);
  EXPECT_EQ(1, events[4].merges_running);
  const auto& waited = events[5];
  EXPECT_EQ(2, waited.relation_id);
  EXPECT_EQ("_3", waited.segment);
  EXPECT_EQ(absl::Seconds(1), waited.queued);
  EXPECT_EQ(absl::Seconds(4), waited.started);
  EXPECT_EQ(absl::Seconds(8), waited.at);
}

TEST(MergeSimulator, AFlushAtAMergesEndComesFirst) {
  const auto events = SimulateMerges(
    {Flush(0, kMiB), Flush(0, kMiB), Flush(2, kMiB), Flush(3, kMiB)},
    Options(1, 2));
  ASSERT_EQ(7, events.size());
  EXPECT_EQ(SegmentEvent::Flush, events[2].type);
  EXPECT_EQ(absl::Seconds(2), events[2].at);
  EXPECT_EQ(SegmentEvent::Merge, events[3].type);
  EXPECT_EQ(absl::Seconds(2), events[3].at);
  EXPECT_EQ(Names({"_1", "_2"}), events[3].inputs);
  EXPECT_EQ(SegmentEvent::Flush, events[4].type);
  EXPECT_EQ(absl::Seconds(3), events[4].at);
  EXPECT_EQ(absl::Seconds(5), events[5].at);
  EXPECT_EQ(Names({"_3", "_4"}), events[5].inputs);
  EXPECT_EQ(absl::Seconds(9), events[6].at);
  EXPECT_EQ(Names({"_5", "_6"}), events[6].inputs);
}

TEST(MergeSimulator, RowIdRangesFollowTheSegments) {
  const auto events = SimulateMerges(
    {Ranged(0, 1, 100), Flush(0, kMiB), Ranged(0, 51, 150)}, Options(1, 3));
  ASSERT_EQ(4, events.size());
  EXPECT_EQ(1, events[0].rowid_min);
  EXPECT_EQ(100, events[0].rowid_max);
  EXPECT_DOUBLE_EQ(1.0, *events[0].overlap);
  EXPECT_DOUBLE_EQ(1.0, *events[1].overlap);
  EXPECT_DOUBLE_EQ(200.0 / 150, *events[2].overlap);
  const auto& merge = events[3];
  EXPECT_EQ(1, merge.rowid_min);
  EXPECT_EQ(150, merge.rowid_max);
  EXPECT_DOUBLE_EQ(1.0, *merge.overlap);
}

TEST(MergeSimulator, AMergeOfUnrangedSegmentsHasNoRange) {
  const auto events =
    SimulateMerges({Flush(0, kMiB), Flush(0, kMiB)}, Options(1));
  ASSERT_EQ(3, events.size());
  EXPECT_FALSE(events[2].rowid_min);
  EXPECT_FALSE(events[2].rowid_max);
  EXPECT_FALSE(events[2].overlap);
}

TEST(MergeSimulator, TheReplayChecksForInterruptsAtEveryEvent) {
  size_t checks = 0;
  const auto events = SimulateMerges({Flush(0, kMiB), Flush(0, kMiB)},
                                     Options(1), [&checks] { ++checks; });
  EXPECT_EQ(3, events.size());
  EXPECT_EQ(3, checks);
}

TEST(MergeSimulator, AnInterruptStopsTheReplay) {
  size_t checks = 0;
  EXPECT_THROW(SimulateMerges({Flush(0, kMiB), Flush(0, kMiB)}, Options(1),
                              [&checks] {
                                if (++checks == 3) {
                                  throw std::runtime_error{"interrupted"};
                                }
                              }),
               std::runtime_error);
}

TEST(MergeSimulator, AFailingPolicyStopsTheReplay) {
  auto options = Options(1);
  options.policy = [](irs::Compaction& candidates,
                      const irs::IndexReader& index,
                      const irs::CompactingSegments& taken) {
    if (index.size() > 2) {
      throw std::runtime_error{"policy failed"};
    }
    PickUntaken(candidates, index, taken, 2);
  };
  EXPECT_THROW(
    SimulateMerges({Flush(0, 2 * kMiB), Flush(0, 2 * kMiB), Flush(1, kMiB)},
                   options),
    std::runtime_error);
}

}  // namespace
