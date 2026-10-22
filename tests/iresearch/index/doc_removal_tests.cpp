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
#include <iresearch/index/document_mask.hpp>
#include <iresearch/index/file_names.hpp>
#include <iresearch/index/index_writer.hpp>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace {

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

std::shared_ptr<const irs::DocumentMask> MaskOf(
  std::initializer_list<irs::doc_id_t> docs) {
  irs::DocumentMaskBuilder builder;
  for (const auto doc : docs) {
    builder.Add(doc);
  }
  return std::make_shared<const irs::DocumentMask>(std::move(builder).Finish());
}

irs::SegmentMeta MetaOf(std::string name,
                        std::vector<irs::AbsorbedSegment> absorbed = {}) {
  irs::SegmentMeta meta;
  meta.name = std::move(name);
  meta.absorbed = std::move(absorbed);
  return meta;
}

irs::RemovalTarget TargetOf(const irs::SegmentMeta& meta) {
  return {.number = irs::SegmentNumber(meta.name), .meta = &meta};
}

std::vector<irs::doc_id_t> Resolve(const irs::DocRemoval& removal,
                                   std::span<const irs::RemovalTarget> targets,
                                   const irs::SubReader& segment) {
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  resolver.Prepare(removals, targets);
  return ToVector(resolver.PositionalDocs(removal, segment));
}

}  // namespace

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
    {5, 9}, {2, 3}, {5, 1}, {2, 3}, {5, 4}};
  const auto removal = irs::DocRemoval::Build(rows);
  EXPECT_FALSE(removal.all);
  EXPECT_EQ((std::vector<uint32_t>{2, 5}), removal.segments);
  EXPECT_EQ((std::vector<uint32_t>{0, 2, 5}), removal.offsets);
  EXPECT_EQ((std::vector<irs::doc_id_t>{3, 3, 9, 1, 4}), removal.docs);
  EXPECT_EQ((std::vector<irs::doc_id_t>{9, 1, 4}), ToVector(removal.Docs(1)));
}

TEST(DocRemovalTest, FindLooksUpSegmentNumbers) {
  const std::vector<irs::DocRemoval::Row> rows{{11, 1}, {3, 1}, {8, 1}};
  const auto removal = irs::DocRemoval::Build(rows);
  EXPECT_EQ(0, removal.Find(3));
  EXPECT_EQ(1, removal.Find(8));
  EXPECT_EQ(2, removal.Find(11));
  for (const uint32_t missing : {0, 4, 9, 12}) {
    EXPECT_EQ(removal.segments.size(), removal.Find(missing)) << missing;
  }
  EXPECT_EQ(removal.segments.size(), removal.Find(irs::DocRemoval::kNoSegment));
}

TEST(DocRemovalTest, BuildOfNothingIsEmpty) {
  const auto removal = irs::DocRemoval::Build({});
  EXPECT_FALSE(removal.all);
  EXPECT_TRUE(removal.segments.empty());
  EXPECT_EQ((std::vector<uint32_t>{0}), removal.offsets);
  EXPECT_TRUE(removal.docs.empty());
  EXPECT_EQ(removal.segments.size(), removal.Find(0));
}

TEST(DocRemovalTest, AllNamesNoSegment) {
  const auto removal = irs::DocRemoval::All();
  EXPECT_TRUE(removal.all);
  EXPECT_TRUE(removal.segments.empty());
  EXPECT_TRUE(removal.docs.empty());
  EXPECT_EQ(removal.segments.size(), removal.Find(0));
}

TEST(DocRemovalTest, BuildOfOneSegmentKeepsArrivalOrder) {
  std::vector<irs::DocRemoval::Row> rows;
  for (irs::doc_id_t doc = 1000; doc > 0; --doc) {
    rows.push_back({7, doc});
  }
  const auto removal = irs::DocRemoval::Build(rows);
  ASSERT_EQ((std::vector<uint32_t>{7}), removal.segments);
  EXPECT_EQ((std::vector<uint32_t>{0, 1000}), removal.offsets);
  for (size_t i = 0; i < rows.size(); ++i) {
    EXPECT_EQ(rows[i].doc, removal.docs[i]) << i;
  }
}

TEST(DocRemovalTest, BuildSeparatesInterleavedSegments) {
  std::vector<irs::DocRemoval::Row> rows;
  for (irs::doc_id_t doc = 1; doc <= 300; ++doc) {
    rows.push_back({doc % 3 == 0 ? 30u : doc % 3 == 1 ? 10u : 20u, doc});
  }
  const auto removal = irs::DocRemoval::Build(rows);
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
    std::map<uint32_t, std::vector<irs::doc_id_t>> model;
    for (auto& row : rows) {
      row.segment = rng() % 4 == 0 ? static_cast<uint32_t>(rng() % 1000000)
                                   : static_cast<uint32_t>(rng() % 8);
      row.doc = static_cast<irs::doc_id_t>(1 + rng() % 100000);
      model[row.segment].emplace_back(row.doc);
    }
    const auto removal = irs::DocRemoval::Build(rows);
    ASSERT_EQ(model.size(), removal.segments.size());
    ASSERT_EQ(model.size() + 1, removal.offsets.size());
    ASSERT_EQ(rows.size(), removal.docs.size());
    size_t group = 0;
    for (const auto& [segment, expected] : model) {
      ASSERT_EQ(segment, removal.segments[group]);
      ASSERT_EQ(group, removal.Find(segment));
      ASSERT_EQ(expected, ToVector(removal.Docs(group)));
      ++group;
    }
    EXPECT_EQ(removal.segments.size(), removal.Find(1000001));
  }
}

TEST(DocRemovalTest, FindAmongManySegments) {
  std::vector<irs::DocRemoval::Row> rows;
  for (uint32_t segment = 4000; segment > 0; segment -= 2) {
    rows.push_back({segment, 1});
  }
  const auto removal = irs::DocRemoval::Build(rows);
  ASSERT_EQ(2000, removal.segments.size());
  for (uint32_t segment = 0; segment <= 4001; ++segment) {
    if (segment % 2 == 0 && segment != 0) {
      EXPECT_EQ(segment / 2 - 1, removal.Find(segment)) << segment;
    } else {
      EXPECT_EQ(removal.segments.size(), removal.Find(segment)) << segment;
    }
  }
}

TEST(AbsorbedDocTest, NoStepsKeepTheDoc) {
  EXPECT_EQ(7, irs::AbsorbedDoc({}, 7));
}

TEST(AbsorbedDocTest, AStepWithoutAMaskShiftsTheDocToItsBase) {
  const std::array<irs::AbsorbedStep, 1> steps{
    {{.rank_begin = 0, .rank_end = 10, .base = 31}}};
  EXPECT_EQ(31, irs::AbsorbedDoc(steps, 1));
  EXPECT_EQ(35, irs::AbsorbedDoc(steps, 5));
  EXPECT_EQ(40, irs::AbsorbedDoc(steps, 10));
  EXPECT_EQ(std::nullopt, irs::AbsorbedDoc(steps, 11));
  EXPECT_EQ(std::nullopt, irs::AbsorbedDoc(steps, irs::doc_limits::invalid()));
}

TEST(AbsorbedDocTest, MaskedDocsAreGoneAndTheRestCloseUp) {
  const std::array<irs::AbsorbedStep, 1> steps{
    {{.mask = MaskOf({2, 3, 6}), .rank_begin = 0, .rank_end = 5, .base = 1}}};
  EXPECT_EQ(1, irs::AbsorbedDoc(steps, 1));
  EXPECT_EQ(std::nullopt, irs::AbsorbedDoc(steps, 2));
  EXPECT_EQ(std::nullopt, irs::AbsorbedDoc(steps, 3));
  EXPECT_EQ(2, irs::AbsorbedDoc(steps, 4));
  EXPECT_EQ(3, irs::AbsorbedDoc(steps, 5));
  EXPECT_EQ(std::nullopt, irs::AbsorbedDoc(steps, 6));
  EXPECT_EQ(4, irs::AbsorbedDoc(steps, 7));
  EXPECT_EQ(5, irs::AbsorbedDoc(steps, 8));
  EXPECT_EQ(std::nullopt, irs::AbsorbedDoc(steps, 9));
}

TEST(AbsorbedDocTest, AWindowTakesOnlyItsRanks) {
  const std::array<irs::AbsorbedStep, 1> steps{
    {{.mask = MaskOf({1}), .rank_begin = 3, .rank_end = 6, .base = 1}}};
  EXPECT_EQ(std::nullopt, irs::AbsorbedDoc(steps, 4));
  EXPECT_EQ(1, irs::AbsorbedDoc(steps, 5));
  EXPECT_EQ(3, irs::AbsorbedDoc(steps, 7));
  EXPECT_EQ(std::nullopt, irs::AbsorbedDoc(steps, 8));
}

TEST(AbsorbedDocTest, StepsChainInOrder) {
  const std::array<irs::AbsorbedStep, 2> steps{
    {{.mask = MaskOf({1}), .rank_begin = 0, .rank_end = 4, .base = 11},
     {.mask = MaskOf({12}), .rank_begin = 0, .rank_end = 20, .base = 101}}};
  EXPECT_EQ(std::nullopt, irs::AbsorbedDoc(steps, 1));
  EXPECT_EQ(std::nullopt, irs::AbsorbedDoc(steps, 3));
  EXPECT_EQ(111, irs::AbsorbedDoc(steps, 2));
  EXPECT_EQ(112, irs::AbsorbedDoc(steps, 4));
  EXPECT_EQ(113, irs::AbsorbedDoc(steps, 5));
  EXPECT_EQ(std::nullopt, irs::AbsorbedDoc(steps, 6));
}

TEST(DocRemovalResolverTest, RemovalsOfLiveSegmentsAreUsedAsBuilt) {
  const FakeSegment three{"_3", 10};
  const FakeSegment five{"_5", 10};
  const auto three_meta = MetaOf("_3");
  const auto five_meta = MetaOf("_5");
  const std::vector<irs::DocRemoval::Row> rows{{3, 2}, {5, 7}, {3, 1}};
  const auto removal = irs::DocRemoval::Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array targets{TargetOf(three_meta), TargetOf(five_meta)};
  resolver.Prepare(removals, targets);
  EXPECT_EQ((std::vector<irs::doc_id_t>{2, 1}),
            ToVector(resolver.PositionalDocs(removal, three)));
  EXPECT_EQ((std::vector<irs::doc_id_t>{7}),
            ToVector(resolver.PositionalDocs(removal, five)));
  EXPECT_FALSE(resolver.Used(3));
  EXPECT_FALSE(resolver.Used(5));
}

TEST(DocRemovalResolverTest, RemovalsOfAbsorbedSegmentsAreRemapped) {
  const FakeSegment nine{"_9", 30};
  const auto nine_meta = MetaOf(
    "_9",
    {{.number = 3, .steps = {{.rank_begin = 0, .rank_end = 10, .base = 1}}},
     {.number = 5,
      .steps = {
        {.mask = MaskOf({1}), .rank_begin = 0, .rank_end = 19, .base = 11}}}});
  const std::vector<irs::DocRemoval::Row> rows{{3, 2}, {5, 1}, {5, 4}, {3, 10}};
  const auto removal = irs::DocRemoval::Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array targets{TargetOf(nine_meta)};
  resolver.Prepare(removals, targets);
  EXPECT_EQ((std::vector<irs::doc_id_t>{2, 10, 13}),
            ToVector(resolver.PositionalDocs(removal, nine)));
  EXPECT_TRUE(resolver.Used(3));
  EXPECT_TRUE(resolver.Used(5));
  EXPECT_FALSE(resolver.Used(9));
}

TEST(DocRemovalResolverTest, ASourceSplitAcrossTargetsReachesEach) {
  const FakeSegment seven{"_7", 5};
  const FakeSegment eight{"_8", 5};
  const auto seven_meta = MetaOf(
    "_7",
    {{.number = 3, .steps = {{.rank_begin = 0, .rank_end = 5, .base = 1}}}});
  const auto eight_meta = MetaOf(
    "_8",
    {{.number = 3, .steps = {{.rank_begin = 5, .rank_end = 10, .base = 1}}}});
  const std::vector<irs::DocRemoval::Row> rows{{3, 1}, {3, 5}, {3, 6}, {3, 10}};
  const auto removal = irs::DocRemoval::Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array targets{TargetOf(seven_meta), TargetOf(eight_meta)};
  resolver.Prepare(removals, targets);
  EXPECT_EQ((std::vector<irs::doc_id_t>{1, 5}),
            ToVector(resolver.PositionalDocs(removal, seven)));
  EXPECT_EQ((std::vector<irs::doc_id_t>{1, 5}),
            ToVector(resolver.PositionalDocs(removal, eight)));
}

TEST(DocRemovalResolverTest, LiveAndAbsorbedGroupsResolveTogether) {
  const FakeSegment four{"_4", 10};
  const FakeSegment nine{"_9", 10};
  const auto four_meta = MetaOf("_4");
  const auto nine_meta = MetaOf(
    "_9",
    {{.number = 3, .steps = {{.rank_begin = 0, .rank_end = 10, .base = 1}}}});
  const std::vector<irs::DocRemoval::Row> rows{{4, 6}, {3, 2}, {4, 1}};
  const auto removal = irs::DocRemoval::Build(rows);
  const std::array targets{TargetOf(four_meta), TargetOf(nine_meta)};
  EXPECT_EQ((std::vector<irs::doc_id_t>{6, 1}),
            Resolve(removal, targets, four));
  EXPECT_EQ((std::vector<irs::doc_id_t>{2}), Resolve(removal, targets, nine));
}

TEST(DocRemovalResolverTest, GroupsOfSegmentsNeitherLiveNorAbsorbedAreGone) {
  const FakeSegment four{"_4", 10};
  const auto four_meta = MetaOf("_4");
  const std::vector<irs::DocRemoval::Row> rows{{4, 2}, {3, 7}};
  const auto removal = irs::DocRemoval::Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array targets{TargetOf(four_meta)};
  resolver.Prepare(removals, targets);
  EXPECT_EQ((std::vector<irs::doc_id_t>{2}),
            ToVector(resolver.PositionalDocs(removal, four)));
  EXPECT_FALSE(resolver.Used(3));
}

TEST(DocRemovalResolverTest, AbsorbedDocsMaskedBeforeTheMergeAreDropped) {
  const FakeSegment nine{"_9", 10};
  const auto nine_meta = MetaOf(
    "_9",
    {{.number = 3,
      .steps = {
        {.mask = MaskOf({2, 3}), .rank_begin = 0, .rank_end = 8, .base = 1}}}});
  const std::vector<irs::DocRemoval::Row> rows{{3, 2}, {3, 3}, {3, 4}};
  const auto removal = irs::DocRemoval::Build(rows);
  const std::array targets{TargetOf(nine_meta)};
  EXPECT_EQ((std::vector<irs::doc_id_t>{2}), Resolve(removal, targets, nine));
}

TEST(DocRemovalResolverTest, AllNeedsNoResolution) {
  const FakeSegment nine{"_9", 10};
  const auto nine_meta = MetaOf(
    "_9",
    {{.number = 3, .steps = {{.rank_begin = 0, .rank_end = 10, .base = 1}}}});
  const auto removal = irs::DocRemoval::All();
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array targets{TargetOf(nine_meta)};
  resolver.Prepare(removals, targets);
  EXPECT_TRUE(resolver.PositionalDocs(removal, nine).empty());
  EXPECT_FALSE(resolver.Used(3));
}

TEST(DocRemovalResolverTest, RemovalLeftOutOfPrepareUsesItsOwnGroups) {
  const FakeSegment three{"_3", 10};
  const auto nine_meta = MetaOf(
    "_9",
    {{.number = 3, .steps = {{.rank_begin = 0, .rank_end = 10, .base = 1}}}});
  const std::vector<irs::DocRemoval::Row> rows{{3, 2}};
  const auto prepared = irs::DocRemoval::Build(rows);
  const auto other = irs::DocRemoval::Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&prepared};
  const std::array targets{TargetOf(nine_meta)};
  resolver.Prepare(removals, targets);
  EXPECT_TRUE(resolver.PositionalDocs(prepared, three).empty());
  EXPECT_EQ((std::vector<irs::doc_id_t>{2}),
            ToVector(resolver.PositionalDocs(other, three)));
}

TEST(DocRemovalResolverTest, SegmentsNamedLikeTheSentinelAreNeverPositional) {
  const FakeSegment sentinel{irs::FileName(irs::DocRemoval::kNoSegment), 10};
  const FakeSegment bogus{"bogus", 10};
  const std::vector<irs::DocRemoval::Row> rows{
    {irs::DocRemoval::kNoSegment - 1, 2}};
  const auto removal = irs::DocRemoval::Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  resolver.Prepare(removals, {});
  EXPECT_TRUE(resolver.PositionalDocs(removal, sentinel).empty());
  EXPECT_TRUE(resolver.PositionalDocs(removal, bogus).empty());
}

TEST(DocRemovalResolverTest, PositionalDocsAreTheGroupAsBuilt) {
  const FakeSegment three{"_3", 4};
  const auto three_meta = MetaOf("_3");
  const std::vector<irs::DocRemoval::Row> rows{{3, 9}, {3, 2}, {3, 2}};
  const auto removal = irs::DocRemoval::Build(rows);
  const std::array targets{TargetOf(three_meta)};
  EXPECT_EQ((std::vector<irs::doc_id_t>{9, 2, 2}),
            Resolve(removal, targets, three));
}
