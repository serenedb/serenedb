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
#include <iresearch/index/composite_reader_impl.hpp>
#include <limits>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "search/compaction/merge_planner.h"
#include "search/maintenance.h"

namespace {

constexpr uint64_t kMiB = uint64_t{1} << 20;

class SegmentStub final : public irs::SubReader {
 public:
  explicit SegmentStub(irs::SegmentInfo meta) : _meta{std::move(meta)} {}

  const SegmentStub& operator*() const noexcept { return *this; }

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

struct Spec {
  std::string name;
  uint64_t bytes = 0;
  uint32_t docs = 100;
  uint32_t live_docs = 100;
};

class Index final : public irs::CompositeReaderImpl<std::vector<SegmentStub>> {
 public:
  explicit Index(const std::vector<Spec>& specs) : Index{Init{specs}} {}

 private:
  struct Init {
    explicit Init(const std::vector<Spec>& specs) {
      for (const auto& spec : specs) {
        readers.emplace_back(irs::SegmentInfo{.name = spec.name,
                                              .docs_count = spec.docs,
                                              .live_docs_count = spec.live_docs,
                                              .byte_size = spec.bytes});
        docs += spec.docs;
        live += spec.live_docs;
      }
    }

    std::vector<SegmentStub> readers;
    uint64_t docs = 0;
    uint64_t live = 0;
  };

  explicit Index(Init&& init)
    : irs::CompositeReaderImpl<std::vector<SegmentStub>>{
        std::move(init.readers), init.live, init.docs} {}
};

using Names = std::vector<std::string>;

class AdjacentRunPolicyTest : public ::testing::Test {
 protected:
  AdjacentRunPolicyTest() {
    settings.compaction_max_segments = 10;
    settings.compaction_max_segments_bytes = uint64_t{5} << 30;
    settings.compaction_floor_segment_bytes = 2 * kMiB;
  }

  std::vector<Names> PickAll(const std::vector<Spec>& specs, size_t wanted,
                             const irs::SegmentNames& taken = {},
                             std::span<const std::string> blocked = {}) {
    const Index index{specs};
    std::vector<Names> picks;
    for (const auto& group : sdb::search::PickMerges(
           settings, inputs_max, index, irs::CompactingSegments{taken}, blocked,
           wanted)) {
      auto& names = picks.emplace_back();
      for (const auto* candidate : group) {
        names.emplace_back(candidate->Meta().name);
      }
    }
    return picks;
  }

  Names Pick(const std::vector<Spec>& specs,
             const irs::SegmentNames& taken = {}) {
    auto picks = PickAll(specs, 1, taken);
    return picks.empty() ? Names{} : std::move(picks.front());
  }

  sdb::search::TasksSettings settings;
  size_t inputs_max = std::numeric_limits<size_t>::max();
};

std::vector<Spec> Equal(size_t count, uint64_t bytes) {
  std::vector<Spec> specs;
  for (size_t i = 0; i != count; ++i) {
    specs.emplace_back(Spec{.name = "s" + std::to_string(i), .bytes = bytes});
  }
  return specs;
}

TEST_F(AdjacentRunPolicyTest, TheDefaultBaseIsSix) {
  EXPECT_EQ(Names{}, Pick(Equal(5, 10 * kMiB)));
  EXPECT_EQ((Names{"s0", "s1", "s2", "s3", "s4", "s5"}),
            Pick(Equal(6, 10 * kMiB)));
}

TEST_F(AdjacentRunPolicyTest, PrefersFewerBytesPerRemovedSegment) {
  EXPECT_EQ(
    (Names{"s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10", "s11"}),
    Pick(Equal(12, 10 * kMiB)));
}

TEST_F(AdjacentRunPolicyTest, TakesAtMostMaxSegments) {
  settings.compaction_max_segments = 6;
  EXPECT_EQ((Names{"s3", "s4", "s5", "s6", "s7", "s8"}),
            Pick(Equal(9, 10 * kMiB)));
}

TEST_F(AdjacentRunPolicyTest, TakesAtMostTheInputsAMergeTakes) {
  inputs_max = 6;
  EXPECT_EQ((Names{"s3", "s4", "s5", "s6", "s7", "s8"}),
            Pick(Equal(9, 10 * kMiB)));
}

TEST_F(AdjacentRunPolicyTest, ANarrowMergeWidthLowersTheBase) {
  settings.compaction_max_segments = 2;
  EXPECT_EQ((Names{"s2", "s3"}), Pick(Equal(4, 10 * kMiB)));
  auto specs = Equal(2, 10 * kMiB);
  specs[0].bytes = 30 * kMiB;
  EXPECT_EQ(Names{}, Pick(specs));
}

TEST_F(AdjacentRunPolicyTest, KeepsABigSegmentOutOfSmallMerges) {
  auto specs = Equal(7, 10 * kMiB);
  specs[0].bytes = 1000 * kMiB;
  EXPECT_EQ((Names{"s1", "s2", "s3", "s4", "s5", "s6"}), Pick(specs));
}

TEST_F(AdjacentRunPolicyTest, MergesNeighboursInReaderOrder) {
  auto specs = Equal(7, 10 * kMiB);
  specs[3].bytes = 1000 * kMiB;
  EXPECT_EQ(Names{}, Pick(specs));
}

TEST_F(AdjacentRunPolicyTest, NeverMergesAcrossAReservedSegment) {
  const auto specs = Equal(13, 10 * kMiB);
  EXPECT_EQ((Names{"s7", "s8", "s9", "s10", "s11", "s12"}),
            Pick(specs, {"s6"}));
  EXPECT_EQ(Names{}, Pick(specs, {"s2", "s8"}));
}

TEST_F(AdjacentRunPolicyTest, NeverMergesAcrossABlockedSegment) {
  const std::string blocked[] = {"s6"};
  EXPECT_EQ((std::vector<Names>{
              {"s7", "s8", "s9", "s10", "s11", "s12"},
              {"s0", "s1", "s2", "s3", "s4", "s5"},
            }),
            PickAll(Equal(13, 10 * kMiB), 3, {}, blocked));
}

TEST_F(AdjacentRunPolicyTest, RespectsTheByteCap) {
  settings.compaction_max_segments_bytes = 55 * kMiB;
  EXPECT_EQ(Names{}, Pick(Equal(6, 10 * kMiB)));
  settings.compaction_max_segments_bytes = 60 * kMiB;
  EXPECT_EQ((Names{"s0", "s1", "s2", "s3", "s4", "s5"}),
            Pick(Equal(6, 10 * kMiB)));
}

TEST_F(AdjacentRunPolicyTest, NeverPassesTheMergeDocLimit) {
  auto specs = Equal(6, 10 * kMiB);
  for (auto& spec : specs) {
    spec.docs = 500'000'000;
    spec.live_docs = 500'000'000;
  }
  EXPECT_EQ(Names{}, Pick(specs));
}

TEST_F(AdjacentRunPolicyTest, LongRunsRelaxTheBalanceRule) {
  const auto alternating = [](size_t count) {
    auto specs = Equal(count, 10 * kMiB);
    for (size_t i = 1; i < specs.size(); i += 2) {
      specs[i].bytes = 20 * kMiB;
    }
    return specs;
  };
  settings.compaction_max_segments = 4;
  EXPECT_EQ(Names{}, Pick(alternating(39)));
  EXPECT_EQ((Names{"s36", "s37", "s38", "s39"}), Pick(alternating(40)));
}

TEST_F(AdjacentRunPolicyTest, TiesGoToTheNewestSegments) {
  settings.compaction_max_segments = 5;
  EXPECT_EQ((Names{"s5", "s6", "s7", "s8", "s9"}), Pick(Equal(10, 10 * kMiB)));
}

TEST_F(AdjacentRunPolicyTest, SmallSegmentsCountTheSegmentCost) {
  EXPECT_EQ(Names{}, Pick(Equal(5, kMiB / 10)));
  EXPECT_EQ((Names{"s0", "s1", "s2", "s3", "s4", "s5"}),
            Pick(Equal(6, kMiB / 10)));
}

TEST_F(AdjacentRunPolicyTest, SegmentsBelowTheFloorCountAsEqual) {
  auto specs = Equal(6, kMiB / 10);
  specs[1].bytes = kMiB / 8;
  specs[3].bytes = kMiB / 9;
  EXPECT_EQ((Names{"s0", "s1", "s2", "s3", "s4", "s5"}), Pick(specs));
}

TEST_F(AdjacentRunPolicyTest, LiveBytesDecideTheBalance) {
  auto specs = Equal(6, 10 * kMiB);
  specs[2].bytes = 20 * kMiB;
  EXPECT_EQ(Names{}, Pick(specs));
  specs[2].live_docs = 50;
  EXPECT_EQ((Names{"s0", "s1", "s2", "s3", "s4", "s5"}), Pick(specs));
}

TEST_F(AdjacentRunPolicyTest, PurgesTheSegmentWithTheMostDeletes) {
  auto specs = Equal(3, 10 * kMiB);
  specs[0].live_docs = 60;
  specs[1].live_docs = 40;
  specs[2].live_docs = 10;
  EXPECT_EQ(Names{"s2"}, Pick(specs));
  EXPECT_EQ(Names{"s1"}, Pick(specs, {"s2"}));
  specs[1].live_docs = 80;
  EXPECT_EQ(Names{"s0"}, Pick(specs, {"s2"}));
  specs[0].live_docs = 90;
  EXPECT_EQ(Names{}, Pick(specs, {"s2"}));
}

TEST_F(AdjacentRunPolicyTest, PurgesOnlySegmentsWithinTheByteCap) {
  settings.compaction_max_segments_bytes = 50 * kMiB;
  const std::vector<Spec> specs{
    {.name = "s0", .bytes = 1000 * kMiB, .live_docs = 20},
    {.name = "s1", .bytes = 100 * kMiB, .live_docs = 40},
  };
  EXPECT_EQ(Names{"s1"}, Pick(specs));
  EXPECT_EQ(Names{}, Pick(specs, {"s1"}));
}

TEST_F(AdjacentRunPolicyTest, SkipsSegmentsWithoutLiveDocs) {
  auto specs = Equal(7, 10 * kMiB);
  specs[2].live_docs = 0;
  EXPECT_EQ((Names{"s0", "s1", "s3", "s4", "s5", "s6"}), Pick(specs));
}

TEST_F(AdjacentRunPolicyTest, ARoundPicksDisjointRunsUntilNoneIsLeft) {
  const auto specs = Equal(20, 10 * kMiB);
  EXPECT_TRUE(PickAll(specs, 0).empty());
  EXPECT_EQ(
    (std::vector<Names>{
      {"s10", "s11", "s12", "s13", "s14", "s15", "s16", "s17", "s18", "s19"},
      {"s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9"},
    }),
    PickAll(specs, 3));
}

TEST_F(AdjacentRunPolicyTest, ARoundPurgesAfterItsRuns) {
  auto specs = Equal(7, 10 * kMiB);
  specs[0].bytes = 1000 * kMiB;
  specs[0].live_docs = 10;
  EXPECT_EQ((std::vector<Names>{
              {"s1", "s2", "s3", "s4", "s5", "s6"},
              {"s0"},
            }),
            PickAll(specs, 3));
}

}  // namespace
