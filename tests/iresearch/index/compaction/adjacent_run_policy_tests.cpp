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

#include <iresearch/index/compaction/adjacent_run_policy.hpp>
#include <iresearch/index/composite_reader_impl.hpp>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

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
  const irs::TermReader* field(irs::field_id) const final { return nullptr; }
  irs::NormReader::ptr norms(irs::field_id) const final { return {}; }

 private:
  irs::SegmentInfo _meta;
};

struct Spec {
  std::string name;
  uint64_t bytes = 0;
  uint32_t docs = 100;
  uint32_t live_docs = 100;
  std::optional<irs::SegmentRange> range;
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

class AdjacentRunPolicyTest : public ::testing::Test {
 protected:
  std::vector<std::string> Pick(const std::vector<Spec>& specs,
                                const irs::CompactingSegments& taken = {}) {
    std::map<std::string, std::optional<irs::SegmentRange>> ranges;
    for (const auto& spec : specs) {
      ranges.emplace(spec.name, spec.range);
    }
    const auto policy =
      irs::MakeAdjacentRunPolicy(options, [&](const irs::SubReader& segment) {
        return ranges.at(segment.Meta().name);
      });
    const Index index{specs};
    irs::Compaction candidates;
    policy(candidates, index, taken);
    std::vector<std::string> names;
    for (const auto* candidate : candidates) {
      names.emplace_back(candidate->Meta().name);
    }
    return names;
  }

  irs::AdjacentRunOptions options;
};

std::vector<Spec> Equal(size_t count, uint64_t bytes) {
  std::vector<Spec> specs;
  for (size_t i = 0; i != count; ++i) {
    specs.emplace_back(Spec{
      .name = "s" + std::to_string(i),
      .bytes = bytes,
      .range = irs::SegmentRange{.first = static_cast<int64_t>(i * 100),
                                 .last = static_cast<int64_t>(i * 100 + 99)}});
  }
  return specs;
}

using Names = std::vector<std::string>;

TEST_F(AdjacentRunPolicyTest, NeedsBaseSegmentsOfEqualSize) {
  EXPECT_EQ(Names{}, Pick(Equal(4, 10 * kMiB)));
  EXPECT_EQ((Names{"s0", "s1", "s2", "s3", "s4"}), Pick(Equal(5, 10 * kMiB)));
}

TEST_F(AdjacentRunPolicyTest, PrefersFewerBytesPerRemovedSegment) {
  EXPECT_EQ(
    (Names{"s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10", "s11"}),
    Pick(Equal(12, 10 * kMiB)));
}

TEST_F(AdjacentRunPolicyTest, TakesAtMostMaxSegments) {
  options.max_segments = 6;
  EXPECT_EQ((Names{"s3", "s4", "s5", "s6", "s7", "s8"}),
            Pick(Equal(9, 10 * kMiB)));
}

TEST_F(AdjacentRunPolicyTest, KeepsABigSegmentOutOfSmallMerges) {
  auto specs = Equal(6, 10 * kMiB);
  specs[0].bytes = 1000 * kMiB;
  EXPECT_EQ((Names{"s1", "s2", "s3", "s4", "s5"}), Pick(specs));
}

TEST_F(AdjacentRunPolicyTest, MergesNeighboursInRowIdOrder) {
  auto specs = Equal(6, 10 * kMiB);
  specs[0].bytes = 1000 * kMiB;
  std::swap(specs[0].range, specs[3].range);
  EXPECT_EQ(Names{}, Pick(specs));
}

TEST_F(AdjacentRunPolicyTest, SegmentsWithoutARangeComeLast) {
  auto specs = Equal(6, 10 * kMiB);
  specs[0].range.reset();
  EXPECT_EQ((Names{"s1", "s2", "s3", "s4", "s5", "s0"}), Pick(specs));
}

TEST_F(AdjacentRunPolicyTest, NeverMergesAcrossAReservedSegment) {
  const auto specs = Equal(11, 10 * kMiB);
  EXPECT_EQ((Names{"s6", "s7", "s8", "s9", "s10"}), Pick(specs, {"s5"}));
  EXPECT_EQ(Names{}, Pick(specs, {"s2", "s7"}));
}

TEST_F(AdjacentRunPolicyTest, RespectsTheByteCap) {
  options.max_bytes = 45 * kMiB;
  EXPECT_EQ(Names{}, Pick(Equal(5, 10 * kMiB)));
  options.max_bytes = 50 * kMiB;
  EXPECT_EQ((Names{"s0", "s1", "s2", "s3", "s4"}), Pick(Equal(5, 10 * kMiB)));
}

TEST_F(AdjacentRunPolicyTest, RespectsTheDocCap) {
  options.max_docs = 499;
  EXPECT_EQ(Names{}, Pick(Equal(5, 10 * kMiB)));
}

TEST_F(AdjacentRunPolicyTest, NeverPassesTheMergeDocLimit) {
  auto specs = Equal(5, 10 * kMiB);
  for (auto& spec : specs) {
    spec.docs = 500'000'000;
    spec.live_docs = 500'000'000;
  }
  EXPECT_EQ(Names{}, Pick(specs));
}

TEST_F(AdjacentRunPolicyTest, LongRunsRelaxTheBalanceRule) {
  auto specs = Equal(10, 10 * kMiB);
  specs[0].bytes = 30 * kMiB;
  options.max_segments = 2;
  EXPECT_EQ(Names{}, Pick(specs));
  options.relax_from = 2;
  options.relax_to = 10;
  EXPECT_EQ((Names{"s8", "s9"}), Pick(specs));
}

TEST_F(AdjacentRunPolicyTest, TiesGoToTheNewestSegments) {
  options.max_segments = 5;
  EXPECT_EQ((Names{"s5", "s6", "s7", "s8", "s9"}), Pick(Equal(10, 10 * kMiB)));
}

TEST_F(AdjacentRunPolicyTest, SmallSegmentsCountTheSegmentCost) {
  EXPECT_EQ(Names{}, Pick(Equal(4, kMiB / 10)));
  EXPECT_EQ((Names{"s0", "s1", "s2", "s3", "s4"}), Pick(Equal(5, kMiB / 10)));
}

TEST_F(AdjacentRunPolicyTest, LiveBytesDecideTheBalance) {
  auto specs = Equal(5, 10 * kMiB);
  specs[2].bytes = 20 * kMiB;
  EXPECT_EQ(Names{}, Pick(specs));
  specs[2].live_docs = 50;
  EXPECT_EQ((Names{"s0", "s1", "s2", "s3", "s4"}), Pick(specs));
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

TEST_F(AdjacentRunPolicyTest, SkipsSegmentsWithoutLiveDocs) {
  auto specs = Equal(6, 10 * kMiB);
  specs[2].live_docs = 0;
  EXPECT_EQ((Names{"s0", "s1", "s3", "s4", "s5"}), Pick(specs));
}

}  // namespace
