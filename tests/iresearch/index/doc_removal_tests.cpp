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

#include <absl/base/internal/endian.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <initializer_list>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/document_mask.hpp>
#include <iresearch/index/file_names.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "tests_shared.hpp"

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

irs::DocMap Direct(uint32_t source, uint32_t target, irs::AbsorbedStep step) {
  return {.source = source, .target = target, .steps = {std::move(step)}};
}

irs::AbsorbedStep Window(irs::doc_id_t rank_begin, irs::doc_id_t rank_end,
                         irs::doc_id_t base) {
  return {.rank_begin = rank_begin, .rank_end = rank_end, .base = base};
}

irs::DocMaps MapsOf(std::initializer_list<irs::DocMap> direct) {
  irs::DocMaps maps;
  const std::vector<irs::DocMap> batch{direct};
  maps.Absorb(batch);
  return maps;
}

std::vector<std::pair<uint32_t, uint32_t>> Edges(
  std::span<const std::shared_ptr<const irs::DocMap>> maps) {
  std::vector<std::pair<uint32_t, uint32_t>> edges;
  for (const auto& map : maps) {
    edges.emplace_back(map->source, map->target);
  }
  std::sort(edges.begin(), edges.end());
  return edges;
}

std::vector<irs::doc_id_t> Resolve(const irs::DocRemoval& removal,
                                   std::span<const uint32_t> targets,
                                   const irs::DocMaps& maps,
                                   const irs::SubReader& segment) {
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  resolver.Prepare(removals, targets, maps);
  return ToVector(resolver.PositionalDocs(removal, segment));
}

std::string Words(std::initializer_list<uint32_t> words) {
  std::string out;
  for (const auto word : words) {
    char buf[sizeof(uint32_t)];
    absl::little_endian::Store32(buf, word);
    out.append(buf, sizeof(buf));
  }
  return out;
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
  const std::vector<irs::DocRemoval::Row> rows{{3, 2}, {5, 7}, {3, 1}};
  const auto removal = irs::DocRemoval::Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<uint32_t, 2> targets{3, 5};
  resolver.Prepare(removals, targets, {});
  EXPECT_EQ((std::vector<irs::doc_id_t>{2, 1}),
            ToVector(resolver.PositionalDocs(removal, three)));
  EXPECT_EQ((std::vector<irs::doc_id_t>{7}),
            ToVector(resolver.PositionalDocs(removal, five)));
  EXPECT_FALSE(resolver.Used(3));
  EXPECT_FALSE(resolver.Used(5));
}

TEST(DocRemovalResolverTest, RemovalsOfAbsorbedSegmentsAreRemapped) {
  const FakeSegment nine{"_9", 30};
  const auto maps = MapsOf(
    {Direct(3, 9, Window(0, 10, 1)),
     Direct(
       5, 9,
       {.mask = MaskOf({1}), .rank_begin = 0, .rank_end = 19, .base = 11})});
  const std::vector<irs::DocRemoval::Row> rows{{3, 2}, {5, 1}, {5, 4}, {3, 10}};
  const auto removal = irs::DocRemoval::Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<uint32_t, 1> targets{9};
  resolver.Prepare(removals, targets, maps);
  EXPECT_EQ((std::vector<irs::doc_id_t>{2, 10, 13}),
            ToVector(resolver.PositionalDocs(removal, nine)));
  EXPECT_TRUE(resolver.Used(3));
  EXPECT_TRUE(resolver.Used(5));
  EXPECT_FALSE(resolver.Used(9));
}

TEST(DocRemovalResolverTest, ASourceSplitAcrossTargetsReachesEach) {
  const FakeSegment seven{"_7", 5};
  const FakeSegment eight{"_8", 5};
  const auto maps =
    MapsOf({Direct(3, 7, Window(0, 5, 1)), Direct(3, 8, Window(5, 10, 1))});
  const std::vector<irs::DocRemoval::Row> rows{{3, 1}, {3, 5}, {3, 6}, {3, 10}};
  const auto removal = irs::DocRemoval::Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<uint32_t, 2> targets{7, 8};
  resolver.Prepare(removals, targets, maps);
  EXPECT_EQ((std::vector<irs::doc_id_t>{1, 5}),
            ToVector(resolver.PositionalDocs(removal, seven)));
  EXPECT_EQ((std::vector<irs::doc_id_t>{1, 5}),
            ToVector(resolver.PositionalDocs(removal, eight)));
}

TEST(DocRemovalResolverTest, LiveAndAbsorbedGroupsResolveTogether) {
  const FakeSegment four{"_4", 10};
  const FakeSegment nine{"_9", 10};
  const auto maps = MapsOf({Direct(3, 9, Window(0, 10, 1))});
  const std::vector<irs::DocRemoval::Row> rows{{4, 6}, {3, 2}, {4, 1}};
  const auto removal = irs::DocRemoval::Build(rows);
  const std::array<uint32_t, 2> targets{4, 9};
  EXPECT_EQ((std::vector<irs::doc_id_t>{6, 1}),
            Resolve(removal, targets, maps, four));
  EXPECT_EQ((std::vector<irs::doc_id_t>{2}),
            Resolve(removal, targets, maps, nine));
}

TEST(DocRemovalResolverTest, GroupsOfSegmentsNeitherLiveNorAbsorbedAreGone) {
  const FakeSegment four{"_4", 10};
  const std::vector<irs::DocRemoval::Row> rows{{4, 2}, {3, 7}};
  const auto removal = irs::DocRemoval::Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<uint32_t, 1> targets{4};
  resolver.Prepare(removals, targets, {});
  EXPECT_EQ((std::vector<irs::doc_id_t>{2}),
            ToVector(resolver.PositionalDocs(removal, four)));
  EXPECT_FALSE(resolver.Used(3));
}

TEST(DocRemovalResolverTest, MapsIntoSegmentsThatAreNotLiveAreSkipped) {
  const FakeSegment four{"_4", 10};
  const auto maps = MapsOf({Direct(3, 9, Window(0, 10, 1))});
  const std::vector<irs::DocRemoval::Row> rows{{3, 2}, {4, 5}};
  const auto removal = irs::DocRemoval::Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<uint32_t, 1> targets{4};
  resolver.Prepare(removals, targets, maps);
  EXPECT_EQ((std::vector<irs::doc_id_t>{5}),
            ToVector(resolver.PositionalDocs(removal, four)));
  EXPECT_TRUE(resolver.Used(3));
}

TEST(DocRemovalResolverTest, AbsorbedDocsMaskedBeforeTheMergeAreDropped) {
  const FakeSegment nine{"_9", 10};
  const auto maps = MapsOf({Direct(
    3, 9,
    {.mask = MaskOf({2, 3}), .rank_begin = 0, .rank_end = 8, .base = 1})});
  const std::vector<irs::DocRemoval::Row> rows{{3, 2}, {3, 3}, {3, 4}};
  const auto removal = irs::DocRemoval::Build(rows);
  const std::array<uint32_t, 1> targets{9};
  EXPECT_EQ((std::vector<irs::doc_id_t>{2}),
            Resolve(removal, targets, maps, nine));
}

TEST(DocRemovalResolverTest, AllNeedsNoResolution) {
  const FakeSegment nine{"_9", 10};
  const auto maps = MapsOf({Direct(3, 9, Window(0, 10, 1))});
  const auto removal = irs::DocRemoval::All();
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<uint32_t, 1> targets{9};
  resolver.Prepare(removals, targets, maps);
  EXPECT_TRUE(resolver.PositionalDocs(removal, nine).empty());
  EXPECT_FALSE(resolver.Used(3));
}

TEST(DocRemovalResolverTest, RemovalLeftOutOfPrepareUsesItsOwnGroups) {
  const FakeSegment three{"_3", 10};
  const auto maps = MapsOf({Direct(3, 9, Window(0, 10, 1))});
  const std::vector<irs::DocRemoval::Row> rows{{3, 2}};
  const auto prepared = irs::DocRemoval::Build(rows);
  const auto other = irs::DocRemoval::Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&prepared};
  const std::array<uint32_t, 1> targets{9};
  resolver.Prepare(removals, targets, maps);
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
  resolver.Prepare(removals, {}, {});
  EXPECT_TRUE(resolver.PositionalDocs(removal, sentinel).empty());
  EXPECT_TRUE(resolver.PositionalDocs(removal, bogus).empty());
}

TEST(DocRemovalResolverTest, PositionalDocsAreTheGroupAsBuilt) {
  const FakeSegment three{"_3", 4};
  const std::vector<irs::DocRemoval::Row> rows{{3, 9}, {3, 2}, {3, 2}};
  const auto removal = irs::DocRemoval::Build(rows);
  const std::array<uint32_t, 1> targets{3};
  EXPECT_EQ((std::vector<irs::doc_id_t>{9, 2, 2}),
            Resolve(removal, targets, {}, three));
}

TEST(DocRemovalResolverTest, ARemovalResolvesThroughTheMapsItCarries) {
  const FakeSegment nine{"_9", 10};
  const std::vector<irs::DocRemoval::Row> rows{{3, 2}, {3, 4}, {5, 1}};
  auto removal = irs::DocRemoval::Build(rows);
  removal.maps = std::make_shared<const irs::DocMapList>(irs::DocMapList{
    std::make_shared<const irs::DocMap>(Direct(3, 9, Window(0, 10, 4))),
    std::make_shared<const irs::DocMap>(Direct(6, 9, Window(0, 10, 1)))});
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<uint32_t, 1> targets{9};
  resolver.Prepare(removals, targets, {});
  EXPECT_EQ((std::vector<irs::doc_id_t>{5, 7}),
            ToVector(resolver.PositionalDocs(removal, nine)));
  EXPECT_FALSE(resolver.Used(3));
}

TEST(DocRemovalResolverTest, CarriedMapsJoinTheWritersMaps) {
  const FakeSegment eight{"_8", 10};
  const FakeSegment nine{"_9", 10};
  const auto maps = MapsOf({Direct(3, 8, Window(0, 5, 1))});
  const std::vector<irs::DocRemoval::Row> rows{{3, 2}, {3, 7}};
  auto removal = irs::DocRemoval::Build(rows);
  removal.maps = std::make_shared<const irs::DocMapList>(irs::DocMapList{
    std::make_shared<const irs::DocMap>(Direct(3, 9, Window(5, 10, 1)))});
  const std::array<uint32_t, 2> targets{8, 9};
  EXPECT_EQ((std::vector<irs::doc_id_t>{2}),
            Resolve(removal, targets, maps, eight));
  EXPECT_EQ((std::vector<irs::doc_id_t>{2}),
            Resolve(removal, targets, maps, nine));
}

TEST(DocMapsTest, AbsorbingAMergedSegmentComposesTheMapsIntoIt) {
  irs::DocMaps maps;
  const std::vector<irs::DocMap> first{Direct(3, 9, Window(0, 10, 1)),
                                       Direct(5, 9, Window(0, 10, 11))};
  maps.Absorb(first);
  const std::vector<irs::DocMap> second{Direct(9, 12, Window(0, 20, 31))};
  maps.Absorb(second);
  EXPECT_EQ((std::vector<std::pair<uint32_t, uint32_t>>{{3, 9}, {3, 12}}),
            Edges(maps.Of(3)));
  EXPECT_EQ((std::vector<std::pair<uint32_t, uint32_t>>{{5, 9}, {5, 12}}),
            Edges(maps.Of(5)));
  EXPECT_EQ((std::vector<std::pair<uint32_t, uint32_t>>{{9, 12}}),
            Edges(maps.Of(9)));
  for (const auto& map : maps.Of(5)) {
    if (map->target == 12) {
      EXPECT_EQ(43, irs::AbsorbedDoc(map->steps, 3));
    }
  }
}

TEST(DocMapsTest, AbsorbingASourceAgainReplacesItsMaps) {
  irs::DocMaps maps;
  const std::vector<irs::DocMap> dropped{Direct(3, 9, Window(0, 10, 1))};
  maps.Absorb(dropped);
  const std::vector<irs::DocMap> taken{Direct(3, 11, Window(0, 10, 1))};
  maps.Absorb(taken);
  EXPECT_EQ((std::vector<std::pair<uint32_t, uint32_t>>{{3, 11}}),
            Edges(maps.Of(3)));
}

TEST(DocMapsTest, ASourceSplitInOneBatchKeepsEveryWindow) {
  const auto maps =
    MapsOf({Direct(3, 7, Window(0, 5, 1)), Direct(3, 8, Window(5, 10, 1))});
  EXPECT_EQ((std::vector<std::pair<uint32_t, uint32_t>>{{3, 7}, {3, 8}}),
            Edges(maps.Of(3)));
}

TEST(DocMapsTest, CopiesShareTheMapsOfTheNamedSources) {
  const auto maps =
    MapsOf({Direct(3, 9, Window(0, 10, 1)), Direct(5, 9, Window(0, 10, 11))});
  irs::DocMapList copied;
  const std::array<uint32_t, 3> sources{3, 4, 7};
  maps.CopyTo(sources, copied);
  ASSERT_EQ(1, copied.size());
  EXPECT_EQ(maps.Of(3).front().get(), copied.front().get());
  EXPECT_TRUE(maps.Of(4).empty());
}

TEST(DocMapsTest, MapsLastWhileAReaderOlderThanTheirCommitLives) {
  auto maps = MapsOf({Direct(3, 9, Window(0, 10, 1))});
  const irs::DocRemovalResolver unused;
  maps.Committed(3, 5);
  maps.Committed(3, 8);
  for (int i = 0; i < 3; ++i) {
    maps.Retire(4, unused);
    EXPECT_EQ(1, maps.Of(3).size());
  }
  maps.Retire(5, unused);
  EXPECT_EQ(1, maps.Of(3).size());
  maps.Retire(5, unused);
  EXPECT_TRUE(maps.Of(3).empty());
}

TEST(DocMapsTest, UncommittedMapsAreNeverRetired) {
  auto maps = MapsOf({Direct(3, 9, Window(0, 10, 1))});
  const irs::DocRemovalResolver unused;
  for (int i = 0; i < 3; ++i) {
    maps.Retire(std::numeric_limits<uint64_t>::max() - 1, unused);
  }
  EXPECT_EQ(1, maps.Of(3).size());
}

TEST(DocMapsTest, AUseRestartsTheRetirement) {
  auto maps = MapsOf({Direct(3, 9, Window(0, 10, 1))});
  maps.Committed(3, 1);
  const irs::DocRemovalResolver unused;
  maps.Retire(1, unused);
  const std::vector<irs::DocRemoval::Row> rows{{3, 2}};
  const auto removal = irs::DocRemoval::Build(rows);
  irs::DocRemovalResolver using_it;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<uint32_t, 1> targets{9};
  using_it.Prepare(removals, targets, maps);
  ASSERT_TRUE(using_it.Used(3));
  maps.Retire(1, using_it);
  maps.Retire(1, unused);
  EXPECT_EQ(1, maps.Of(3).size());
  maps.Retire(1, unused);
  EXPECT_TRUE(maps.Of(3).empty());
}

TEST(DocMapsCodecTest, MapsSurviveTheirBytes) {
  const irs::DocMapList maps{
    std::make_shared<const irs::DocMap>(irs::DocMap{
      .source = 3,
      .target = 12,
      .steps =
        {{.mask = MaskOf({2, 7}), .rank_begin = 0, .rank_end = 8, .base = 1},
         Window(0, 20, 31)}}),
    std::make_shared<const irs::DocMap>(Direct(5, 9, Window(4, 6, 2)))};
  const auto read = irs::ReadDocMaps(irs::WriteDocMaps(maps));
  ASSERT_EQ(maps.size(), read.size());
  for (size_t i = 0; i < maps.size(); ++i) {
    EXPECT_EQ(maps[i]->source, read[i]->source);
    EXPECT_EQ(maps[i]->target, read[i]->target);
    ASSERT_EQ(maps[i]->steps.size(), read[i]->steps.size());
    for (size_t j = 0; j < maps[i]->steps.size(); ++j) {
      const auto& written = maps[i]->steps[j];
      const auto& back = read[i]->steps[j];
      EXPECT_EQ(written.rank_begin, back.rank_begin);
      EXPECT_EQ(written.rank_end, back.rank_end);
      EXPECT_EQ(written.base, back.base);
      ASSERT_EQ(written.mask == nullptr, back.mask == nullptr);
      if (written.mask != nullptr) {
        EXPECT_EQ(*written.mask, *back.mask);
      }
    }
  }
}

TEST(DocMapsCodecTest, NoMapsAreNoBytes) {
  EXPECT_TRUE(irs::WriteDocMaps({}).empty());
  EXPECT_TRUE(irs::ReadDocMaps("").empty());
}

TEST(DocMapsCodecTest, MalformedBytesAreCorruptData) {
  const auto valid = Words({1, 3, 9, 1, 0, 10, 1, 0});
  ASSERT_EQ(1, irs::ReadDocMaps(valid).size());
  for (const auto& bytes :
       {valid.substr(0, valid.size() - 1), valid + "x",
        Words({2, 3, 9, 1, 0, 10, 1, 0}), Words({1, 3, 9, 0}),
        Words({1, 3, 9, 1, 5, 4, 1, 0}), Words({1, 3, 9, 1, 0, 10, 0, 0}),
        Words({1, 3, 9, 1, 0, 10, irs::doc_limits::eof() - 5, 0}),
        Words({1, irs::DocRemoval::kNoSegment, 9, 1, 0, 10, 1, 0}),
        Words({1, 3, irs::DocRemoval::kNoSegment, 1, 0, 10, 1, 0}),
        Words({1, 3, 9, 1, 0, 10, 1, 100})}) {
    ASSERT_SQL_ERROR(irs::ReadDocMaps(bytes), ERRCODE_DATA_CORRUPTED);
  }
  ASSERT_SQL_ERROR(irs::ReadDocMaps(Words({1, 3, 9, 1, 0, 10, 1, 4}) + "abcd"),
                   ERRCODE_INDEX_CORRUPTED);
}
