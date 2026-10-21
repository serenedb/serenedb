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

#include <array>
#include <cstdint>
#include <initializer_list>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/file_names.hpp>
#include <iresearch/index/index_writer.hpp>
#include <limits>
#include <map>
#include <random>
#include <set>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace {

using Ranges = std::vector<std::pair<int64_t, int64_t>>;

Ranges Pairs(std::span<const irs::KeyRange> ranges) {
  Ranges out;
  for (const auto& range : ranges) {
    out.emplace_back(range.begin, range.end);
  }
  return out;
}

template<typename T>
std::vector<T> ToVector(std::span<const T> values) {
  return {values.begin(), values.end()};
}

class FakeSegment final : public irs::SubReader {
 public:
  FakeSegment(std::string name, uint32_t docs) {
    _info.name = std::move(name);
    _info.docs_count = docs;
    _info.live_docs_count = docs;
  }

  uint64_t CountMappedMemory() const final { return 0; }
  irs::NormReader::ptr norms(irs::field_id) const final { return nullptr; }
  const irs::SegmentInfo& Meta() const final { return _info; }
  const irs::DocumentMask* docs_mask() const final { return nullptr; }
  irs::lead::Node::ptr docs_iterator() const final { return nullptr; }
  std::span<const irs::field_id> field_ids() const final { return {}; }
  const irs::TermReader* field(irs::field_id) const final { return nullptr; }

 private:
  irs::SegmentInfo _info;
};

}  // namespace

TEST(KeyRangesTest, AppendJoinsRunsOfConsecutiveKeys) {
  std::vector<irs::KeyRange> ranges;
  const std::array<int64_t, 8> keys{5, 6, 7, 9, 10, 3, 4, 11};
  irs::AppendKeyRanges(ranges, keys);
  EXPECT_EQ((Ranges{{5, 8}, {9, 11}, {3, 5}, {11, 12}}), Pairs(ranges));
}

TEST(KeyRangesTest, AppendKeepsDuplicatesAndDescendingKeysApart) {
  std::vector<irs::KeyRange> ranges;
  const std::array<int64_t, 4> keys{3, 3, 2, 1};
  irs::AppendKeyRanges(ranges, keys);
  EXPECT_EQ((Ranges{{3, 4}, {3, 4}, {2, 3}, {1, 2}}), Pairs(ranges));
}

TEST(KeyRangesTest, AppendContinuesTheLastRange) {
  std::vector<irs::KeyRange> ranges{{5, 7}};
  const std::array<int64_t, 2> keys{7, 8};
  irs::AppendKeyRanges(ranges, keys);
  EXPECT_EQ((Ranges{{5, 9}}), Pairs(ranges));
}

TEST(KeyRangesTest, AppendHandlesNegativeAndExtremeKeys) {
  constexpr auto kMin = std::numeric_limits<int64_t>::min();
  constexpr auto kMax = std::numeric_limits<int64_t>::max();
  std::vector<irs::KeyRange> ranges;
  const std::array<int64_t, 5> keys{kMin, kMin + 1, -1, 0, kMax - 1};
  irs::AppendKeyRanges(ranges, keys);
  EXPECT_EQ((Ranges{{kMin, kMin + 2}, {-1, 1}, {kMax - 1, kMax}}),
            Pairs(ranges));
}

TEST(KeyRangesTest, AppendOfNoKeysAddsNothing) {
  std::vector<irs::KeyRange> ranges{{1, 2}};
  irs::AppendKeyRanges(ranges, {});
  EXPECT_EQ((Ranges{{1, 2}}), Pairs(ranges));
}

TEST(KeyRangesTest, NormalizeMergesOverlappingNestedAndAdjacentRanges) {
  std::vector<irs::KeyRange> ranges{{20, 25}, {1, 10},  {2, 3},
                                    {10, 12}, {24, 30}, {30, 31}};
  irs::NormalizeKeyRanges(ranges);
  EXPECT_EQ((Ranges{{1, 12}, {20, 31}}), Pairs(ranges));
}

TEST(KeyRangesTest, NormalizeCollapsesIdenticalRanges) {
  std::vector<irs::KeyRange> ranges{{5, 6}, {5, 6}, {5, 6}};
  irs::NormalizeKeyRanges(ranges);
  EXPECT_EQ((Ranges{{5, 6}}), Pairs(ranges));
}

TEST(KeyRangesTest, NormalizeKeepsRangesAGapApart) {
  std::vector<irs::KeyRange> ranges{{3, 4}, {1, 2}};
  irs::NormalizeKeyRanges(ranges);
  EXPECT_EQ((Ranges{{1, 2}, {3, 4}}), Pairs(ranges));
}

TEST(KeyRangesTest, NormalizeOfNothingIsNothing) {
  std::vector<irs::KeyRange> ranges;
  irs::NormalizeKeyRanges(ranges);
  EXPECT_TRUE(ranges.empty());
}

TEST(KeyRangesTest, NormalizeMatchesAModelOnRandomKeys) {
  std::mt19937_64 rng{42};
  for (int round = 0; round < 300; ++round) {
    std::vector<int64_t> keys(rng() % 300);
    for (auto& key : keys) {
      key = static_cast<int64_t>(rng() % 500) - 250;
    }
    std::vector<irs::KeyRange> ranges;
    irs::AppendKeyRanges(ranges, keys);
    irs::NormalizeKeyRanges(ranges);
    Ranges expected;
    for (const auto key : std::set<int64_t>{keys.begin(), keys.end()}) {
      if (!expected.empty() && expected.back().second == key) {
        ++expected.back().second;
      } else {
        expected.emplace_back(key, key + 1);
      }
    }
    ASSERT_EQ(expected, Pairs(ranges)) << round;
    irs::NormalizeKeyRanges(ranges);
    ASSERT_EQ(expected, Pairs(ranges)) << round;
  }
}

TEST(SegmentNumberTest, InvertsFileName) {
  for (const uint32_t number :
       {uint32_t{0}, uint32_t{1}, uint32_t{42}, uint32_t{1} << 31,
        irs::DocRemoval::kNoSegment - 1}) {
    EXPECT_EQ(number, irs::SegmentNumber(irs::FileName(number)));
  }
}

TEST(SegmentNumberTest, ReadsTheSegmentOfAFileName) {
  EXPECT_EQ(4, irs::SegmentNumber("_4.col"));
  EXPECT_EQ(4, irs::SegmentNumber("_4.0.sm"));
  EXPECT_EQ(7, irs::SegmentNumber("_007"));
}

TEST(SegmentNumberTest, RejectsOtherNames) {
  for (const auto* name :
       {"", "_", "4", "_4x", "_-4", "_+4", "__4", "x_4", "_ 4", "_0x10",
        "_99999999999999999999", "segments_3", "_4_"}) {
    EXPECT_EQ(irs::DocRemoval::kNoSegment, irs::SegmentNumber(name)) << name;
  }
}

TEST(SegmentNumberTest, NumbersFromTheSentinelUpAreNoSegment) {
  for (const uint64_t number : {uint64_t{irs::DocRemoval::kNoSegment},
                                uint64_t{irs::DocRemoval::kNoSegment} + 1,
                                std::numeric_limits<uint64_t>::max()}) {
    EXPECT_EQ(irs::DocRemoval::kNoSegment,
              irs::SegmentNumber(irs::FileName(number)))
      << number;
  }
}

TEST(ParseSegmentIdTest, ParsesSegmentNamesAndTheirFiles) {
  for (const auto& [name, id] :
       std::initializer_list<std::pair<const char*, uint64_t>>{
         {"_0", 0},
         {"_5", 5},
         {"_5.sm", 5},
         {"_5.0.col", 5},
         {"_123456789", 123456789},
         {"_18446744073709551615", std::numeric_limits<uint64_t>::max()}}) {
    uint64_t parsed = 0;
    EXPECT_TRUE(irs::ParseSegmentId(name, parsed)) << name;
    EXPECT_EQ(id, parsed) << name;
  }
}

TEST(ParseSegmentIdTest, RejectsOtherNames) {
  for (const auto* name :
       {"", "_", "5", "x_5", "_x", "_5x", "_-5", "_+5", "_ 5", "__5", "_5_",
        "_0x10", "_18446744073709551616", "segments_3"}) {
    uint64_t parsed = 0;
    EXPECT_FALSE(irs::ParseSegmentId(name, parsed)) << name;
  }
}

TEST(DocRemovalTest, BuildGroupsBySegmentInArrivalOrder) {
  const std::vector<irs::DocRemoval::Row> rows{
    {5, 9, 90}, {2, 3, 30}, {5, 1, 10}, {2, 3, 30}, {5, 4, 40}};
  const auto removal = irs::DocRemoval::Build(7, rows);
  EXPECT_EQ(7, removal.key_column);
  EXPECT_EQ((std::vector<uint32_t>{2, 5}), removal.segments);
  EXPECT_EQ((std::vector<uint32_t>{0, 2, 5}), removal.offsets);
  EXPECT_EQ((std::vector<irs::doc_id_t>{3, 3, 9, 1, 4}), removal.docs);
  EXPECT_EQ((std::vector<int64_t>{30, 30, 90, 10, 40}), removal.keys);
  EXPECT_EQ((std::vector<irs::doc_id_t>{9, 1, 4}), ToVector(removal.Docs(1)));
  EXPECT_EQ((std::vector<int64_t>{90, 10, 40}), ToVector(removal.Keys(1)));
  EXPECT_TRUE(removal.ranges.empty());
}

TEST(DocRemovalTest, FindLooksUpSegmentNumbers) {
  const std::vector<irs::DocRemoval::Row> rows{
    {11, 1, 3}, {3, 1, 1}, {8, 1, 2}};
  const auto removal = irs::DocRemoval::Build(1, rows);
  EXPECT_EQ(0, removal.Find(3));
  EXPECT_EQ(1, removal.Find(8));
  EXPECT_EQ(2, removal.Find(11));
  for (const uint32_t missing : {0, 4, 9, 12}) {
    EXPECT_EQ(removal.segments.size(), removal.Find(missing)) << missing;
  }
  EXPECT_EQ(removal.segments.size(), removal.Find(irs::DocRemoval::kNoSegment));
}

TEST(DocRemovalTest, BuildOfNothingIsEmpty) {
  const auto removal = irs::DocRemoval::Build(1, {});
  EXPECT_TRUE(removal.segments.empty());
  EXPECT_EQ((std::vector<uint32_t>{0}), removal.offsets);
  EXPECT_TRUE(removal.docs.empty());
  EXPECT_TRUE(removal.keys.empty());
  EXPECT_TRUE(removal.ranges.empty());
  EXPECT_EQ(removal.segments.size(), removal.Find(0));
}

TEST(DocRemovalTest, BuildKeepsRangesAsGiven) {
  const auto removal = irs::DocRemoval::Build(3, {}, {{9, 10}, {1, 3}, {1, 3}});
  EXPECT_EQ(3, removal.key_column);
  EXPECT_TRUE(removal.segments.empty());
  EXPECT_EQ((std::vector<uint32_t>{0}), removal.offsets);
  EXPECT_EQ((Ranges{{9, 10}, {1, 3}, {1, 3}}), Pairs(removal.ranges));
}

TEST(DocRemovalTest, BuildKeepsRowsAndRangesTogether) {
  const std::vector<irs::DocRemoval::Row> rows{{4, 2, 20}, {4, 1, 10}};
  const auto removal = irs::DocRemoval::Build(3, rows, {{100, 105}});
  EXPECT_EQ((std::vector<uint32_t>{4}), removal.segments);
  EXPECT_EQ((std::vector<irs::doc_id_t>{2, 1}), removal.docs);
  EXPECT_EQ((Ranges{{100, 105}}), Pairs(removal.ranges));
}

TEST(DocRemovalTest, BuildOfOneSegmentKeepsArrivalOrder) {
  std::vector<irs::DocRemoval::Row> rows;
  for (irs::doc_id_t doc = 1000; doc > 0; --doc) {
    rows.push_back({7, doc, int64_t{doc} * 3});
  }
  const auto removal = irs::DocRemoval::Build(1, rows);
  ASSERT_EQ((std::vector<uint32_t>{7}), removal.segments);
  EXPECT_EQ((std::vector<uint32_t>{0, 1000}), removal.offsets);
  for (size_t i = 0; i < rows.size(); ++i) {
    EXPECT_EQ(rows[i].doc, removal.docs[i]) << i;
    EXPECT_EQ(rows[i].key, removal.keys[i]) << i;
  }
}

TEST(DocRemovalTest, BuildSeparatesInterleavedSegments) {
  std::vector<irs::DocRemoval::Row> rows;
  for (irs::doc_id_t doc = 1; doc <= 300; ++doc) {
    rows.push_back({doc % 3 == 0   ? 30u
                    : doc % 3 == 1 ? 10u
                                   : 20u,
                    doc, int64_t{doc}});
  }
  const auto removal = irs::DocRemoval::Build(1, rows);
  ASSERT_EQ((std::vector<uint32_t>{10, 20, 30}), removal.segments);
  EXPECT_EQ((std::vector<uint32_t>{0, 100, 200, 300}), removal.offsets);
  for (size_t group = 0; group < 3; ++group) {
    const auto docs = removal.Docs(group);
    for (size_t i = 0; i < docs.size(); ++i) {
      EXPECT_EQ(static_cast<irs::doc_id_t>(3 * i + group + 1), docs[i])
        << group << " " << i;
    }
  }
}

TEST(DocRemovalTest, BuildMatchesAModelOnRandomRows) {
  std::mt19937_64 rng{7};
  for (int round = 0; round < 50; ++round) {
    std::vector<irs::DocRemoval::Row> rows(rng() % 5000);
    std::map<uint32_t, std::vector<std::pair<irs::doc_id_t, int64_t>>> model;
    for (auto& row : rows) {
      row.segment = rng() % 4 == 0 ? static_cast<uint32_t>(rng() % 1000000)
                                   : static_cast<uint32_t>(rng() % 8);
      row.doc = static_cast<irs::doc_id_t>(1 + rng() % 100000);
      row.key = static_cast<int64_t>(rng());
      model[row.segment].emplace_back(row.doc, row.key);
    }
    const auto removal = irs::DocRemoval::Build(1, rows);
    ASSERT_EQ(model.size(), removal.segments.size());
    ASSERT_EQ(model.size() + 1, removal.offsets.size());
    ASSERT_EQ(rows.size(), removal.docs.size());
    ASSERT_EQ(rows.size(), removal.keys.size());
    size_t group = 0;
    for (const auto& [segment, expected] : model) {
      ASSERT_EQ(segment, removal.segments[group]);
      ASSERT_EQ(group, removal.Find(segment));
      const auto docs = removal.Docs(group);
      const auto keys = removal.Keys(group);
      ASSERT_EQ(expected.size(), docs.size());
      for (size_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(expected[i].first, docs[i]);
        ASSERT_EQ(expected[i].second, keys[i]);
      }
      ++group;
    }
    EXPECT_EQ(removal.segments.size(), removal.Find(1000001));
  }
}

TEST(DocRemovalTest, FindAmongManySegments) {
  std::vector<irs::DocRemoval::Row> rows;
  for (uint32_t segment = 4000; segment > 0; segment -= 2) {
    rows.push_back({segment, 1, segment});
  }
  const auto removal = irs::DocRemoval::Build(1, rows);
  ASSERT_EQ(2000, removal.segments.size());
  for (uint32_t segment = 0; segment <= 4001; ++segment) {
    if (segment % 2 == 0 && segment != 0) {
      EXPECT_EQ(segment / 2 - 1, removal.Find(segment)) << segment;
    } else {
      EXPECT_EQ(removal.segments.size(), removal.Find(segment)) << segment;
    }
  }
}

TEST(DocRemovalResolverTest, RemovalsOfLiveSegmentsArePositionalOnly) {
  const FakeSegment three{"_3", 10};
  const FakeSegment five{"_5", 10};
  const std::vector<irs::DocRemoval::Row> rows{
    {3, 2, 20}, {5, 7, 70}, {3, 1, 10}};
  const auto removal = irs::DocRemoval::Build(1, rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<const irs::SubReader*, 2> live{&three, &five};
  resolver.Prepare(removals, live);
  EXPECT_FALSE(resolver.ByKey(removal));
  EXPECT_EQ((std::vector<irs::doc_id_t>{2, 1}),
            ToVector(resolver.PositionalDocs(removal, three)));
  EXPECT_EQ((std::vector<irs::doc_id_t>{7}),
            ToVector(resolver.PositionalDocs(removal, five)));
}

TEST(DocRemovalResolverTest, GroupsOfSegmentsGoneFromTheSnapshotGoByKey) {
  const FakeSegment three{"_3", 10};
  const std::vector<irs::DocRemoval::Row> rows{{3, 2, 20}, {9, 7, 70}};
  const auto removal = irs::DocRemoval::Build(1, rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<const irs::SubReader*, 1> live{&three};
  resolver.Prepare(removals, live);
  EXPECT_TRUE(resolver.ByKey(removal));
  EXPECT_EQ((std::vector<irs::doc_id_t>{2}),
            ToVector(resolver.PositionalDocs(removal, three)));
  EXPECT_TRUE(resolver.ByKeyDocs(three).empty());
}

TEST(DocRemovalResolverTest, RangesMakeARemovalByKey) {
  const auto removal = irs::DocRemoval::Build(1, {}, {{1, 5}});
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  resolver.Prepare(removals, {});
  EXPECT_TRUE(resolver.ByKey(removal));
}

TEST(DocRemovalResolverTest, EmptyRemovalIsNeitherPositionalNorByKey) {
  const FakeSegment three{"_3", 10};
  const auto removal = irs::DocRemoval::Build(1, {});
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<const irs::SubReader*, 1> live{&three};
  resolver.Prepare(removals, live);
  EXPECT_FALSE(resolver.ByKey(removal));
  EXPECT_TRUE(resolver.PositionalDocs(removal, three).empty());
}

TEST(DocRemovalResolverTest, ByKeyRemovalsOfDifferentKeyColumnsFail) {
  const auto first = irs::DocRemoval::Build(1, {}, {{1, 2}});
  const auto second = irs::DocRemoval::Build(2, {}, {{3, 4}});
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 2> removals{&first, &second};
  EXPECT_DEATH(resolver.Prepare(removals, {}),
               "removals of one commit resolve keys in columns 1 and 2");
}

TEST(DocRemovalResolverTest, PositionalRemovalsMayUseDifferentKeyColumns) {
  const FakeSegment three{"_3", 10};
  const std::vector<irs::DocRemoval::Row> rows{{3, 2, 20}};
  const auto first = irs::DocRemoval::Build(1, rows);
  const auto second = irs::DocRemoval::Build(2, rows);
  const auto by_key = irs::DocRemoval::Build(3, {}, {{1, 2}});
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 3> removals{&first, &second,
                                                       &by_key};
  const std::array<const irs::SubReader*, 1> live{&three};
  EXPECT_NO_THROW(resolver.Prepare(removals, live));
  EXPECT_FALSE(resolver.ByKey(first));
  EXPECT_FALSE(resolver.ByKey(second));
  EXPECT_TRUE(resolver.ByKey(by_key));
}

TEST(DocRemovalResolverTest, RemovalLeftOutOfPrepareIsNotByKey) {
  const auto prepared = irs::DocRemoval::Build(1, {}, {{1, 2}});
  const auto other = irs::DocRemoval::Build(1, {}, {{1, 2}});
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&prepared};
  resolver.Prepare(removals, {});
  EXPECT_TRUE(resolver.ByKey(prepared));
  EXPECT_FALSE(resolver.ByKey(other));
}

TEST(DocRemovalResolverTest, SegmentsNamedLikeTheSentinelAreNeverPositional) {
  const FakeSegment sentinel{irs::FileName(irs::DocRemoval::kNoSegment), 10};
  const FakeSegment bogus{"bogus", 10};
  const std::vector<irs::DocRemoval::Row> rows{
    {irs::DocRemoval::kNoSegment - 1, 2, 20}};
  const auto removal = irs::DocRemoval::Build(1, rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<const irs::SubReader*, 2> live{&sentinel, &bogus};
  resolver.Prepare(removals, live);
  EXPECT_TRUE(resolver.ByKey(removal));
  EXPECT_TRUE(resolver.PositionalDocs(removal, sentinel).empty());
  EXPECT_TRUE(resolver.PositionalDocs(removal, bogus).empty());
}

TEST(DocRemovalResolverTest, PositionalDocsAreTheGroupAsBuilt) {
  const FakeSegment three{"_3", 4};
  const std::vector<irs::DocRemoval::Row> rows{
    {3, 9, 90}, {3, 2, 20}, {3, 2, 20}};
  const auto removal = irs::DocRemoval::Build(1, rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<const irs::SubReader*, 1> live{&three};
  resolver.Prepare(removals, live);
  EXPECT_EQ((std::vector<irs::doc_id_t>{9, 2, 2}),
            ToVector(resolver.PositionalDocs(removal, three)));
}

TEST(DocRemovalResolverTest, ByKeyDocsOfASegmentWithoutColumnsAreEmpty) {
  const FakeSegment three{"_3", 10};
  const auto removal = irs::DocRemoval::Build(1, {}, {{0, 100}});
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  resolver.Prepare(removals, {});
  EXPECT_TRUE(resolver.ByKeyDocs(three).empty());
}
