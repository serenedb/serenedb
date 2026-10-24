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

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
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
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "tests_shared.hpp"

namespace {

struct Row {
  uint32_t segment;
  irs::doc_id_t doc;
};

irs::DocRemoval Build(std::span<const Row> rows) {
  irs::DocRemovalBuilder builder;
  for (const auto& row : rows) {
    builder.Add(row.segment, row.doc);
  }
  return std::move(builder).Finish();
}

std::vector<irs::doc_id_t> DocsOf(const irs::DocumentMask* docs) {
  return docs == nullptr ? std::vector<irs::doc_id_t>{} : docs->Docs();
}

std::optional<irs::doc_id_t> Mapped(const irs::DocMap& map, irs::doc_id_t doc) {
  std::vector<irs::doc_id_t> docs{doc};
  irs::MapDocs(map, docs);
  if (docs.empty()) {
    return std::nullopt;
  }
  return docs.front();
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

irs::DocMap Direct(uint32_t source, uint32_t target, irs::DocMap window) {
  window.source = source;
  window.target = target;
  return window;
}

irs::DocMap Window(irs::doc_id_t rank_begin, irs::doc_id_t rank_end,
                   irs::doc_id_t base) {
  return {.rank_begin = rank_begin, .rank_end = rank_end, .base = base};
}

irs::DocMapList Shared(std::initializer_list<irs::DocMap> maps) {
  irs::DocMapList shared;
  for (const auto& map : maps) {
    shared.push_back(std::make_shared<const irs::DocMap>(map));
  }
  return shared;
}

irs::DocMaps MapsOf(std::initializer_list<irs::DocMap> direct) {
  irs::DocMaps maps;
  maps.Absorb(Shared(direct));
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
  return DocsOf(resolver.PositionalDocs(removal, segment));
}

std::string Bytes(const auto& write) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer out{stream};
  out.Begin();
  write(out);
  out.End();
  return std::string{reinterpret_cast<const char*>(stream.GetData()),
                     stream.GetPosition()};
}

void WriteBlob(duckdb::BinarySerializer& out, duckdb::field_id_t field,
               std::string_view blob) {
  out.WriteProperty(field, "blob",
                    reinterpret_cast<duckdb::const_data_ptr_t>(blob.data()),
                    blob.size());
}

std::string FrozenOf(const irs::DocumentMask& mask) {
  std::string frozen(mask.FrozenSize(), '\0');
  mask.WriteFrozen(frozen.data());
  return frozen;
}

struct RawGroup {
  uint32_t segment;
  std::string docs;
};

std::string RemovalBytes(std::initializer_list<RawGroup> groups) {
  return Bytes([&](duckdb::BinarySerializer& out) {
    out.WriteList(0, "groups", groups.size(),
                  [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
                    const auto& group = groups.begin()[i];
                    list.WriteObject([&](duckdb::BinarySerializer& obj) {
                      obj.WriteProperty<uint32_t>(0, "segment", group.segment);
                      WriteBlob(obj, 1, group.docs);
                    });
                  });
  });
}

struct RawMap {
  uint32_t source = 3;
  uint32_t target = 9;
  uint32_t rank_begin = 0;
  uint32_t rank_end = 10;
  uint32_t base = 1;
  std::optional<std::string> mask;
};

std::string MapBytes(const RawMap& map) {
  return Bytes([&](duckdb::BinarySerializer& out) {
    out.WriteList(
      0, "maps", 1, [&](duckdb::BinarySerializer::List& list, duckdb::idx_t) {
        list.WriteObject([&](duckdb::BinarySerializer& obj) {
          obj.WriteProperty<uint32_t>(0, "source", map.source);
          obj.WriteProperty<uint32_t>(1, "target", map.target);
          obj.WriteProperty<uint32_t>(2, "rank_begin", map.rank_begin);
          obj.WriteProperty<uint32_t>(3, "rank_end", map.rank_end);
          obj.WriteProperty<uint32_t>(4, "base", map.base);
          if (map.mask) {
            WriteBlob(obj, 5, *map.mask);
          }
        });
      });
  });
}

void ExpectMalformed(const auto& read, std::string_view bytes,
                     std::string_view what, std::string_view why) {
  try {
    read(bytes);
    ADD_FAILURE() << why;
  } catch (const irs::SqlException& e) {
    EXPECT_EQ(ERRCODE_DATA_CORRUPTED, e.error().errcode) << e.message();
    EXPECT_TRUE(
      e.message().starts_with(absl::StrCat("Malformed ", what, ": ", why)))
      << e.message();
  }
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

TEST(DocRemovalTest, BuildGroupsTheDocsOfEachSegment) {
  const std::vector<Row> rows{{5, 9}, {2, 3}, {5, 1}, {2, 3}, {5, 4}};
  const auto removal = Build(rows);
  EXPECT_FALSE(removal.all);
  EXPECT_EQ((std::vector<uint32_t>{2, 5}), removal.segments);
  ASSERT_EQ(2, removal.docs.size());
  EXPECT_EQ((std::vector<irs::doc_id_t>{3}), DocsOf(&removal.Docs(0)));
  EXPECT_EQ((std::vector<irs::doc_id_t>{1, 4, 9}), DocsOf(&removal.Docs(1)));
}

TEST(DocRemovalTest, FindLooksUpSegmentNumbers) {
  const std::vector<Row> rows{{11, 1}, {3, 1}, {8, 1}};
  const auto removal = Build(rows);
  EXPECT_EQ(0, removal.Find(3));
  EXPECT_EQ(1, removal.Find(8));
  EXPECT_EQ(2, removal.Find(11));
  for (const uint32_t missing : {0, 4, 9, 12}) {
    EXPECT_EQ(removal.segments.size(), removal.Find(missing)) << missing;
  }
  EXPECT_EQ(removal.segments.size(), removal.Find(irs::DocRemoval::kNoSegment));
}

TEST(DocRemovalTest, BuildOfNothingIsEmpty) {
  const auto removal = Build({});
  EXPECT_FALSE(removal.all);
  EXPECT_TRUE(removal.segments.empty());
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

TEST(DocRemovalTest, BuildOfOneSegmentSortsItsDocs) {
  std::vector<Row> rows;
  for (irs::doc_id_t doc = 1000; doc > 0; --doc) {
    rows.push_back({7, doc});
  }
  const auto removal = Build(rows);
  ASSERT_EQ((std::vector<uint32_t>{7}), removal.segments);
  const auto docs = DocsOf(&removal.Docs(0));
  ASSERT_EQ(rows.size(), docs.size());
  for (size_t i = 0; i < docs.size(); ++i) {
    EXPECT_EQ(static_cast<irs::doc_id_t>(i + 1), docs[i]) << i;
  }
}

TEST(DocRemovalTest, BuildSeparatesInterleavedSegments) {
  std::vector<Row> rows;
  for (irs::doc_id_t doc = 1; doc <= 300; ++doc) {
    rows.push_back({doc % 3 == 0 ? 30u : doc % 3 == 1 ? 10u : 20u, doc});
  }
  const auto removal = Build(rows);
  ASSERT_EQ((std::vector<uint32_t>{10, 20, 30}), removal.segments);
  for (size_t group = 0; group < 3; ++group) {
    const auto docs = DocsOf(&removal.Docs(group));
    ASSERT_EQ(100, docs.size()) << group;
    for (size_t i = 0; i < docs.size(); ++i) {
      EXPECT_EQ(static_cast<irs::doc_id_t>(3 * i + group + 1), docs[i])
        << group << " " << i;
    }
  }
}

TEST(DocRemovalTest, BuildMatchesAModelOnRandomRows) {
  std::mt19937_64 rng{7};
  for (int round = 0; round < 50; ++round) {
    std::vector<Row> rows(rng() % 5000);
    std::map<uint32_t, std::set<irs::doc_id_t>> model;
    for (auto& row : rows) {
      row.segment = rng() % 4 == 0 ? static_cast<uint32_t>(rng() % 1000000)
                                   : static_cast<uint32_t>(rng() % 8);
      row.doc = static_cast<irs::doc_id_t>(1 + rng() % 100000);
      model[row.segment].insert(row.doc);
    }
    const auto removal = Build(rows);
    ASSERT_EQ(model.size(), removal.segments.size());
    ASSERT_EQ(model.size(), removal.docs.size());
    size_t group = 0;
    for (const auto& [segment, expected] : model) {
      ASSERT_EQ(segment, removal.segments[group]);
      ASSERT_EQ(group, removal.Find(segment));
      ASSERT_EQ(std::vector<irs::doc_id_t>(expected.begin(), expected.end()),
                DocsOf(&removal.Docs(group)));
      ++group;
    }
    EXPECT_EQ(removal.segments.size(), removal.Find(1000001));
  }
}

TEST(DocRemovalBuilderTest, MergesDocsAndMasksIntoTheirSegments) {
  irs::DocRemovalBuilder builder;
  EXPECT_TRUE(builder.Empty());
  builder.Add(4, 7);
  builder.Add(4, *MaskOf({1, 7, 9}));
  builder.Add(2, *MaskOf({3}));
  builder.Add(4, 2);
  EXPECT_FALSE(builder.Empty());
  const auto removal = std::move(builder).Finish();
  EXPECT_EQ((std::vector<uint32_t>{2, 4}), removal.segments);
  EXPECT_EQ((std::vector<irs::doc_id_t>{3}), DocsOf(&removal.Docs(0)));
  EXPECT_EQ((std::vector<irs::doc_id_t>{1, 2, 7, 9}), DocsOf(&removal.Docs(1)));
}

TEST(DocRemovalBuilderTest, FinishLeavesItEmpty) {
  irs::DocRemovalBuilder builder;
  builder.Add(3, 1);
  const auto first = std::move(builder).Finish();
  ASSERT_EQ(1, first.segments.size());
  EXPECT_TRUE(builder.Empty());
  builder.Add(5, 2);
  const auto second = std::move(builder).Finish();
  EXPECT_EQ((std::vector<uint32_t>{5}), second.segments);
  EXPECT_EQ((std::vector<irs::doc_id_t>{2}), DocsOf(&second.Docs(0)));
}

TEST(DocRemovalBuilderTest, SegmentsLeftWithoutDocsAreDropped) {
  irs::DocRemovalBuilder builder;
  builder.AddRange(4, 5, 5);
  builder.Add(2, *MaskOf({}));
  builder.Add(6, 3);
  const auto removal = std::move(builder).Finish();
  EXPECT_EQ((std::vector<uint32_t>{6}), removal.segments);
  EXPECT_EQ((std::vector<irs::doc_id_t>{3}), DocsOf(&removal.Docs(0)));
}

TEST(DocRemovalBuilderTest, ARunOfRowsTakesAFewBytes) {
  irs::DocRemovalBuilder builder;
  for (irs::doc_id_t doc = 1; doc <= 1000000; ++doc) {
    builder.Add(3, doc);
  }
  const auto removal = std::move(builder).Finish();
  ASSERT_EQ(1000000, removal.Docs(0).Count());
  EXPECT_GT(1024, removal.Docs(0).FrozenSize());
  EXPECT_GT(1024, irs::WriteDocRemoval(removal).size());
}

TEST(DocRemovalBuilderTest, ScatteredDocsStayCompact) {
  irs::DocRemovalBuilder builder;
  for (irs::doc_id_t doc = 1; doc < 1000000; doc += 1000) {
    builder.Add(3, doc);
  }
  const auto removal = std::move(builder).Finish();
  ASSERT_EQ(1000, removal.Docs(0).Count());
  EXPECT_GT(4096, removal.Docs(0).FrozenSize());
}

TEST(DocRemovalTest, CopiesShareTheirDocs) {
  const std::vector<Row> rows{{3, 1}, {3, 2}};
  const auto removal = Build(rows);
  const auto copy = removal;
  EXPECT_EQ(&removal.Docs(0), &copy.Docs(0));
}

TEST(DocRemovalTest, FindAmongManySegments) {
  std::vector<Row> rows;
  for (uint32_t segment = 4000; segment > 0; segment -= 2) {
    rows.push_back({segment, 1});
  }
  const auto removal = Build(rows);
  ASSERT_EQ(2000, removal.segments.size());
  for (uint32_t segment = 0; segment <= 4001; ++segment) {
    if (segment % 2 == 0 && segment != 0) {
      EXPECT_EQ(segment / 2 - 1, removal.Find(segment)) << segment;
    } else {
      EXPECT_EQ(removal.segments.size(), removal.Find(segment)) << segment;
    }
  }
}

TEST(MapDocsTest, AWindowWithoutAMaskShiftsTheDocToItsBase) {
  const auto map = Window(0, 10, 31);
  EXPECT_EQ(31, Mapped(map, 1));
  EXPECT_EQ(35, Mapped(map, 5));
  EXPECT_EQ(40, Mapped(map, 10));
  EXPECT_EQ(std::nullopt, Mapped(map, 11));
  EXPECT_EQ(std::nullopt, Mapped(map, irs::doc_limits::invalid()));
}

TEST(MapDocsTest, MaskedDocsAreGoneAndTheRestCloseUp) {
  const irs::DocMap map{
    .mask = MaskOf({2, 3, 6}), .rank_begin = 0, .rank_end = 5, .base = 1};
  EXPECT_EQ(1, Mapped(map, 1));
  EXPECT_EQ(std::nullopt, Mapped(map, 2));
  EXPECT_EQ(std::nullopt, Mapped(map, 3));
  EXPECT_EQ(2, Mapped(map, 4));
  EXPECT_EQ(3, Mapped(map, 5));
  EXPECT_EQ(std::nullopt, Mapped(map, 6));
  EXPECT_EQ(4, Mapped(map, 7));
  EXPECT_EQ(5, Mapped(map, 8));
  EXPECT_EQ(std::nullopt, Mapped(map, 9));
}

TEST(MapDocsTest, AWindowTakesOnlyItsRanks) {
  const irs::DocMap map{
    .mask = MaskOf({1}), .rank_begin = 3, .rank_end = 6, .base = 1};
  EXPECT_EQ(std::nullopt, Mapped(map, 4));
  EXPECT_EQ(1, Mapped(map, 5));
  EXPECT_EQ(3, Mapped(map, 7));
  EXPECT_EQ(std::nullopt, Mapped(map, 8));
}

TEST(MapDocsTest, ManyDocsMapLikeOneAtATime) {
  std::mt19937 gen{29};
  const auto one_at_a_time =
    [](const std::vector<irs::doc_id_t>& masked, const irs::DocMap& map,
       irs::doc_id_t doc) -> std::optional<irs::doc_id_t> {
    const auto below = std::lower_bound(masked.begin(), masked.end(), doc);
    if (below != masked.end() && *below == doc) {
      return std::nullopt;
    }
    const auto rank = static_cast<irs::doc_id_t>(doc - irs::doc_limits::min() -
                                                 (below - masked.begin()));
    if (rank < map.rank_begin || rank >= map.rank_end) {
      return std::nullopt;
    }
    return map.base + (rank - map.rank_begin);
  };
  for (int round = 0; round < 30; ++round) {
    std::vector<irs::doc_id_t> masked;
    irs::DocumentMaskBuilder builder;
    const auto every = 2 + gen() % 40;
    for (irs::doc_id_t doc = 1; doc < 300000; ++doc) {
      if (gen() % every == 0) {
        builder.Add(doc);
        masked.push_back(doc);
      }
    }
    const irs::doc_id_t begin = gen() % 50000;
    const irs::DocMap map{
      .mask =
        std::make_shared<const irs::DocumentMask>(std::move(builder).Finish()),
      .rank_begin = begin,
      .rank_end = static_cast<irs::doc_id_t>(begin + gen() % 250000),
      .base = static_cast<irs::doc_id_t>(1 + gen() % 100000)};
    std::vector<irs::doc_id_t> docs;
    std::vector<irs::doc_id_t> expected;
    for (irs::doc_id_t doc = 1; doc < 400000; doc += 1 + gen() % 9) {
      docs.push_back(doc);
      if (const auto mapped = one_at_a_time(masked, map, doc)) {
        expected.push_back(*mapped);
      }
    }
    irs::MapDocs(map, docs);
    ASSERT_EQ(expected, docs) << round;
  }
}

TEST(DocRemovalResolverTest, RemovalsOfLiveSegmentsAreUsedAsBuilt) {
  const FakeSegment three{"_3", 10};
  const FakeSegment five{"_5", 10};
  const std::vector<Row> rows{{3, 2}, {5, 7}, {3, 1}};
  const auto removal = Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<uint32_t, 2> targets{3, 5};
  resolver.Prepare(removals, targets, {});
  EXPECT_EQ(&removal.Docs(0), resolver.PositionalDocs(removal, three));
  EXPECT_EQ(&removal.Docs(1), resolver.PositionalDocs(removal, five));
  EXPECT_EQ((std::vector<irs::doc_id_t>{1, 2}),
            DocsOf(resolver.PositionalDocs(removal, three)));
  EXPECT_EQ((std::vector<irs::doc_id_t>{7}),
            DocsOf(resolver.PositionalDocs(removal, five)));
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
  const std::vector<Row> rows{{3, 2}, {5, 1}, {5, 4}, {3, 10}};
  const auto removal = Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<uint32_t, 1> targets{9};
  resolver.Prepare(removals, targets, maps);
  EXPECT_EQ((std::vector<irs::doc_id_t>{2, 10, 13}),
            DocsOf(resolver.PositionalDocs(removal, nine)));
  EXPECT_TRUE(resolver.Used(3));
  EXPECT_TRUE(resolver.Used(5));
  EXPECT_FALSE(resolver.Used(9));
}

TEST(DocRemovalResolverTest, ASourceSplitAcrossTargetsReachesEach) {
  const FakeSegment seven{"_7", 5};
  const FakeSegment eight{"_8", 5};
  const auto maps =
    MapsOf({Direct(3, 7, Window(0, 5, 1)), Direct(3, 8, Window(5, 10, 1))});
  const std::vector<Row> rows{{3, 1}, {3, 5}, {3, 6}, {3, 10}};
  const auto removal = Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<uint32_t, 2> targets{7, 8};
  resolver.Prepare(removals, targets, maps);
  EXPECT_EQ((std::vector<irs::doc_id_t>{1, 5}),
            DocsOf(resolver.PositionalDocs(removal, seven)));
  EXPECT_EQ((std::vector<irs::doc_id_t>{1, 5}),
            DocsOf(resolver.PositionalDocs(removal, eight)));
}

TEST(DocRemovalResolverTest, LiveAndAbsorbedGroupsResolveTogether) {
  const FakeSegment four{"_4", 10};
  const FakeSegment nine{"_9", 10};
  const auto maps = MapsOf({Direct(3, 9, Window(0, 10, 1))});
  const std::vector<Row> rows{{4, 6}, {3, 2}, {4, 1}};
  const auto removal = Build(rows);
  const std::array<uint32_t, 2> targets{4, 9};
  EXPECT_EQ((std::vector<irs::doc_id_t>{1, 6}),
            Resolve(removal, targets, maps, four));
  EXPECT_EQ((std::vector<irs::doc_id_t>{2}),
            Resolve(removal, targets, maps, nine));
}

TEST(DocRemovalResolverTest, GroupsOfSegmentsNeitherLiveNorAbsorbedAreGone) {
  const FakeSegment four{"_4", 10};
  const std::vector<Row> rows{{4, 2}, {3, 7}};
  const auto removal = Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<uint32_t, 1> targets{4};
  resolver.Prepare(removals, targets, {});
  EXPECT_EQ((std::vector<irs::doc_id_t>{2}),
            DocsOf(resolver.PositionalDocs(removal, four)));
  EXPECT_FALSE(resolver.Used(3));
}

TEST(DocRemovalResolverTest, MapsIntoSegmentsThatAreNotLiveAreSkipped) {
  const FakeSegment four{"_4", 10};
  const auto maps = MapsOf({Direct(3, 9, Window(0, 10, 1))});
  const std::vector<Row> rows{{3, 2}, {4, 5}};
  const auto removal = Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<uint32_t, 1> targets{4};
  resolver.Prepare(removals, targets, maps);
  EXPECT_EQ((std::vector<irs::doc_id_t>{5}),
            DocsOf(resolver.PositionalDocs(removal, four)));
  EXPECT_TRUE(resolver.Used(3));
}

TEST(DocRemovalResolverTest, AbsorbedDocsMaskedBeforeTheMergeAreDropped) {
  const FakeSegment nine{"_9", 10};
  const auto maps = MapsOf({Direct(
    3, 9,
    {.mask = MaskOf({2, 3}), .rank_begin = 0, .rank_end = 8, .base = 1})});
  const std::vector<Row> rows{{3, 2}, {3, 3}, {3, 4}};
  const auto removal = Build(rows);
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
  EXPECT_EQ(nullptr, resolver.PositionalDocs(removal, nine));
  EXPECT_FALSE(resolver.Used(3));
}

TEST(DocRemovalResolverTest, RemovalLeftOutOfPrepareUsesItsOwnGroups) {
  const FakeSegment three{"_3", 10};
  const FakeSegment nine{"_9", 10};
  const auto maps = MapsOf({Direct(3, 9, Window(0, 10, 1))});
  const std::vector<Row> rows{{3, 2}};
  const auto prepared = Build(rows);
  const auto other = Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&prepared};
  const std::array<uint32_t, 1> targets{9};
  resolver.Prepare(removals, targets, maps);
  EXPECT_EQ((std::vector<irs::doc_id_t>{2}),
            DocsOf(resolver.PositionalDocs(prepared, nine)));
  EXPECT_EQ(nullptr, resolver.PositionalDocs(other, nine));
  EXPECT_EQ(&other.Docs(0), resolver.PositionalDocs(other, three));
}

TEST(DocRemovalResolverTest, ARemovalFollowsAChainOfMaps) {
  const FakeSegment twelve{irs::FileName(12), 30};
  const auto maps = MapsOf(
    {Direct(3, 9,
            {.mask = MaskOf({1}), .rank_begin = 0, .rank_end = 4, .base = 11}),
     Direct(
       9, 12,
       {.mask = MaskOf({12}), .rank_begin = 0, .rank_end = 20, .base = 101})});
  const std::vector<Row> rows{{3, 1}, {3, 2}, {3, 3}, {3, 4}, {3, 5}, {3, 6}};
  const auto removal = Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<uint32_t, 1> targets{12};
  resolver.Prepare(removals, targets, maps);
  EXPECT_EQ((std::vector<irs::doc_id_t>{111, 112, 113}),
            DocsOf(resolver.PositionalDocs(removal, twelve)));
  EXPECT_TRUE(resolver.Used(3));
  EXPECT_TRUE(resolver.Used(9));
}

TEST(DocRemovalResolverTest, AChainStopsAtTheFirstLiveTarget) {
  const FakeSegment nine{"_9", 10};
  const FakeSegment twelve{irs::FileName(12), 10};
  const auto maps =
    MapsOf({Direct(3, 9, Window(0, 10, 1)), Direct(9, 12, Window(0, 10, 1))});
  const std::vector<Row> rows{{3, 4}};
  const auto removal = Build(rows);
  const std::array<uint32_t, 2> targets{9, 12};
  EXPECT_EQ((std::vector<irs::doc_id_t>{4}),
            Resolve(removal, targets, maps, nine));
  EXPECT_TRUE(Resolve(removal, targets, maps, twelve).empty());
}

TEST(DocRemovalResolverTest, ACarriedChainResolvesWithoutTheWritersMaps) {
  const FakeSegment twelve{irs::FileName(12), 20};
  const std::vector<Row> rows{{3, 2}};
  auto removal = Build(rows);
  removal.maps = std::make_shared<const irs::DocMapList>(
    Shared({Direct(3, 9, Window(0, 10, 5)), Direct(9, 12, Window(0, 20, 1))}));
  const std::array<uint32_t, 1> targets{12};
  EXPECT_EQ((std::vector<irs::doc_id_t>{6}),
            Resolve(removal, targets, {}, twelve));
}

TEST(DocRemovalResolverTest, SegmentsNamedLikeTheSentinelAreNeverPositional) {
  const FakeSegment sentinel{irs::FileName(irs::DocRemoval::kNoSegment), 10};
  const FakeSegment bogus{"bogus", 10};
  const std::vector<Row> rows{{irs::DocRemoval::kNoSegment - 1, 2}};
  const auto removal = Build(rows);
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  resolver.Prepare(removals, {}, {});
  EXPECT_EQ(nullptr, resolver.PositionalDocs(removal, sentinel));
  EXPECT_EQ(nullptr, resolver.PositionalDocs(removal, bogus));
}

TEST(DocRemovalResolverTest, PositionalDocsAreTheGroupAsBuilt) {
  const FakeSegment three{"_3", 4};
  const std::vector<Row> rows{{3, 9}, {3, 2}, {3, 2}};
  const auto removal = Build(rows);
  const std::array<uint32_t, 1> targets{3};
  EXPECT_EQ((std::vector<irs::doc_id_t>{2, 9}),
            Resolve(removal, targets, {}, three));
}

TEST(DocRemovalResolverTest, DocsMappedFromSeveralSourcesMergeInTheirTarget) {
  const FakeSegment nine{"_9", 30};
  const auto maps =
    MapsOf({Direct(3, 9, Window(0, 10, 11)), Direct(5, 9, Window(0, 10, 1))});
  const std::vector<Row> rows{{9, 15}, {3, 5}, {5, 9}, {3, 1}, {5, 1}, {9, 2}};
  const auto removal = Build(rows);
  const std::array<uint32_t, 1> targets{9};
  EXPECT_EQ((std::vector<irs::doc_id_t>{1, 2, 9, 11, 15}),
            Resolve(removal, targets, maps, nine));
}

TEST(DocRemovalResolverTest, ARemovalResolvesThroughTheMapsItCarries) {
  const FakeSegment nine{"_9", 10};
  const std::vector<Row> rows{{3, 2}, {3, 4}, {5, 1}};
  auto removal = Build(rows);
  removal.maps = std::make_shared<const irs::DocMapList>(irs::DocMapList{
    std::make_shared<const irs::DocMap>(Direct(3, 9, Window(0, 10, 4))),
    std::make_shared<const irs::DocMap>(Direct(6, 9, Window(0, 10, 1)))});
  irs::DocRemovalResolver resolver;
  const std::array<const irs::DocRemoval*, 1> removals{&removal};
  const std::array<uint32_t, 1> targets{9};
  resolver.Prepare(removals, targets, {});
  EXPECT_EQ((std::vector<irs::doc_id_t>{5, 7}),
            DocsOf(resolver.PositionalDocs(removal, nine)));
  EXPECT_FALSE(resolver.Used(3));
}

TEST(DocRemovalResolverTest, CarriedMapsJoinTheWritersMaps) {
  const FakeSegment eight{"_8", 10};
  const FakeSegment nine{"_9", 10};
  const auto maps = MapsOf({Direct(3, 8, Window(0, 5, 1))});
  const std::vector<Row> rows{{3, 2}, {3, 7}};
  auto removal = Build(rows);
  removal.maps = std::make_shared<const irs::DocMapList>(irs::DocMapList{
    std::make_shared<const irs::DocMap>(Direct(3, 9, Window(5, 10, 1)))});
  const std::array<uint32_t, 2> targets{8, 9};
  EXPECT_EQ((std::vector<irs::doc_id_t>{2}),
            Resolve(removal, targets, maps, eight));
  EXPECT_EQ((std::vector<irs::doc_id_t>{2}),
            Resolve(removal, targets, maps, nine));
}

TEST(DocMapsTest, AbsorbingAMergedSegmentChainsItsMaps) {
  irs::DocMaps maps;
  maps.Absorb(
    Shared({Direct(3, 9, Window(0, 10, 1)), Direct(5, 9, Window(0, 10, 11))}));
  maps.Absorb(Shared({Direct(9, 12, Window(0, 20, 31))}));
  EXPECT_EQ((std::vector<std::pair<uint32_t, uint32_t>>{{3, 9}}),
            Edges(maps.Of(3)));
  EXPECT_EQ((std::vector<std::pair<uint32_t, uint32_t>>{{5, 9}}),
            Edges(maps.Of(5)));
  EXPECT_EQ((std::vector<std::pair<uint32_t, uint32_t>>{{9, 12}}),
            Edges(maps.Of(9)));
  const FakeSegment twelve{irs::FileName(12), 30};
  const std::vector<Row> rows{{5, 3}};
  const std::array<uint32_t, 1> targets{12};
  EXPECT_EQ((std::vector<irs::doc_id_t>{43}),
            Resolve(Build(rows), targets, maps, twelve));
}

TEST(DocMapsTest, AbsorbingASourceAgainReplacesItsMaps) {
  irs::DocMaps maps;
  maps.Absorb(Shared({Direct(3, 9, Window(0, 10, 1))}));
  maps.Absorb(Shared({Direct(3, 11, Window(0, 10, 1))}));
  EXPECT_EQ((std::vector<std::pair<uint32_t, uint32_t>>{{3, 11}}),
            Edges(maps.Of(3)));
}

TEST(DocMapsTest, AbsorbingTakesTheMapsWithoutCopies) {
  const auto taken = Shared({Direct(3, 9, Window(0, 10, 1))});
  irs::DocMaps maps;
  maps.Absorb(taken);
  ASSERT_EQ(1, maps.Of(3).size());
  EXPECT_EQ(taken.front().get(), maps.Of(3).front().get());
}

TEST(DocMapsTest, CopiesFollowTheChainOfTheNamedSources) {
  const auto maps =
    MapsOf({Direct(3, 9, Window(0, 10, 1)), Direct(9, 12, Window(0, 20, 1)),
            Direct(12, 15, Window(0, 30, 1)), Direct(5, 7, Window(0, 4, 1))});
  irs::DocMapList copied;
  const std::array<uint32_t, 2> sources{3, 9};
  maps.CopyTo(sources, copied);
  EXPECT_EQ(
    (std::vector<std::pair<uint32_t, uint32_t>>{{3, 9}, {9, 12}, {12, 15}}),
    Edges(copied));
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

TEST(DocMapsTest, CommittingASourceWithoutMapsChangesNothing) {
  auto maps = MapsOf({Direct(3, 9, Window(0, 10, 1))});
  const irs::DocRemovalResolver unused;
  maps.Committed(4, 1);
  for (int i = 0; i < 3; ++i) {
    maps.Retire(2, unused);
  }
  EXPECT_TRUE(maps.Of(4).empty());
  EXPECT_EQ(1, maps.Of(3).size());
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
  const std::vector<Row> rows{{3, 2}};
  const auto removal = Build(rows);
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
  const auto maps = Shared(
    {Direct(
       3, 12,
       {.mask = MaskOf({2, 7}), .rank_begin = 0, .rank_end = 8, .base = 1}),
     Direct(5, 9, Window(4, 6, 2))});
  const auto read = irs::ReadDocMaps(irs::WriteDocMaps(maps));
  ASSERT_EQ(maps.size(), read.size());
  for (size_t i = 0; i < maps.size(); ++i) {
    const auto& written = *maps[i];
    const auto& back = *read[i];
    EXPECT_EQ(written.source, back.source);
    EXPECT_EQ(written.target, back.target);
    EXPECT_EQ(written.rank_begin, back.rank_begin);
    EXPECT_EQ(written.rank_end, back.rank_end);
    EXPECT_EQ(written.base, back.base);
    ASSERT_EQ(written.mask == nullptr, back.mask == nullptr);
    if (written.mask != nullptr) {
      EXPECT_EQ(*written.mask, *back.mask);
    }
  }
}

TEST(DocMapsCodecTest, NoMapsAreNoBytes) {
  EXPECT_TRUE(irs::WriteDocMaps({}).empty());
  EXPECT_TRUE(irs::ReadDocMaps("").empty());
}

TEST(DocMapsCodecTest, MalformedBytesAreCorruptData) {
  const auto valid = MapBytes({});
  ASSERT_EQ(1, irs::ReadDocMaps(valid).size());
  const auto read = [](std::string_view bytes) { irs::ReadDocMaps(bytes); };
  for (const auto& [bytes, why] :
       std::initializer_list<std::pair<std::string, std::string_view>>{
         {"x", "Failed to deserialize"},
         {valid.substr(0, valid.size() - 1), "Failed to deserialize"},
         {valid + "x", "it has trailing bytes"},
         {MapBytes({.source = irs::DocRemoval::kNoSegment}),
          "a segment number is out of range"},
         {MapBytes({.target = irs::DocRemoval::kNoSegment}),
          "a segment number is out of range"},
         {MapBytes({.rank_begin = 5, .rank_end = 4}),
          "a window is out of range"},
         {MapBytes({.base = 0}), "a window is out of range"},
         {MapBytes({.base = irs::doc_limits::eof() - 5}),
          "a window is out of range"}}) {
    ExpectMalformed(read, bytes, "doc map", why);
  }
  ASSERT_SQL_ERROR(irs::ReadDocMaps(MapBytes({.mask = "abcd"})),
                   ERRCODE_INDEX_CORRUPTED);
}

TEST(DocRemovalCodecTest, RemovalsSurviveTheirBytes) {
  std::vector<Row> rows{{9, 70000}, {2, 1}, {9, 3}};
  for (irs::doc_id_t doc = 100; doc < 200000; ++doc) {
    rows.push_back({4, doc});
  }
  const auto removal = Build(rows);
  const auto read = irs::ReadDocRemoval(irs::WriteDocRemoval(removal));
  EXPECT_FALSE(read.all);
  EXPECT_EQ(nullptr, read.maps);
  ASSERT_EQ(removal.segments, read.segments);
  ASSERT_EQ(removal.docs.size(), read.docs.size());
  for (size_t group = 0; group < removal.segments.size(); ++group) {
    EXPECT_EQ(removal.Docs(group), read.Docs(group)) << group;
  }
}

TEST(DocRemovalCodecTest, MalformedBytesAreCorruptData) {
  const auto three = FrozenOf(*MaskOf({1, 2}));
  const auto five = FrozenOf(*MaskOf({4}));
  const auto valid = RemovalBytes({{3, three}, {5, five}});
  ASSERT_EQ((std::vector<uint32_t>{3, 5}), irs::ReadDocRemoval(valid).segments);
  const auto read = [](std::string_view bytes) { irs::ReadDocRemoval(bytes); };
  for (const auto& [bytes, why] :
       std::initializer_list<std::pair<std::string, std::string_view>>{
         {"", "Failed to deserialize"},
         {valid.substr(0, valid.size() - 1), "Failed to deserialize"},
         {valid + "x", "it has trailing bytes"},
         {RemovalBytes({}), "it names no segment"},
         {RemovalBytes({{irs::DocRemoval::kNoSegment, three}}),
          "a segment number is out of range"},
         {RemovalBytes({{5, five}, {3, three}}),
          "its segments are out of order"},
         {RemovalBytes({{3, three}, {3, three}}),
          "its segments are out of order"},
         {RemovalBytes({{3, ""}}), "a segment names no rows"}}) {
    ExpectMalformed(read, bytes, "doc removal", why);
  }
  ASSERT_SQL_ERROR(irs::ReadDocRemoval(RemovalBytes({{3, "abcd"}})),
                   ERRCODE_INDEX_CORRUPTED);
}
