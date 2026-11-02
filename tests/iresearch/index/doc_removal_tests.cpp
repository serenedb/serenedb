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
#include <cstdint>
#include <initializer_list>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/document_mask.hpp>
#include <iresearch/index/file_names.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/segment_reader.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <limits>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "index/doc_generator.hpp"
#include "index/index_tests.hpp"
#include "tests_shared.hpp"

namespace {

constexpr uint32_t kSegments = 8;

const irs::DirectoryReader& Segments() {
  static irs::MemoryDirectory dir;
  static const irs::DirectoryReader reader = [] {
    auto writer = irs::IndexWriter::Make(
      dir, irs::kOmCreate,
      tests::EnsureWriterDb(tests::CsDefaultWriterOptions()));
    for (uint32_t segment = 0; segment != kSegments; ++segment) {
      tests::StringField field{"name", "doc"};
      field.id = tests::FieldIdFor("name");
      auto trx = writer->GetBatch();
      EXPECT_TRUE(tests::InsertFields(trx.Insert(), &field, &field + 1));
      EXPECT_TRUE(trx.Commit());
      EXPECT_TRUE(writer->RefreshCommit());
    }
    return writer->GetSnapshot();
  }();
  return reader;
}

uint64_t IdOf(uint32_t index) {
  uint64_t id = 0;
  EXPECT_TRUE(irs::ParseSegmentId(Segments()[index].Meta().name, id));
  return id;
}

std::vector<uint64_t> IdsOf(std::initializer_list<uint32_t> indexes) {
  std::vector<uint64_t> ids;
  for (const auto index : indexes) {
    ids.emplace_back(IdOf(index));
  }
  return ids;
}

std::vector<uint64_t> SegmentsOf(
  const std::shared_ptr<const irs::DocRemoval>& removal) {
  std::vector<uint64_t> segments;
  for (const auto& group : removal->groups) {
    segments.emplace_back(group.segment);
  }
  return segments;
}

struct Row {
  uint32_t segment;
  irs::doc_id_t doc;
};

std::shared_ptr<const irs::DocRemoval> Build(std::span<const Row> rows) {
  irs::DocRemovalBuilder builder;
  for (const auto& row : rows) {
    builder.Add(row.segment, row.doc);
  }
  return std::move(builder).Finish(Segments());
}

std::vector<irs::doc_id_t> DocsOf(const irs::DocumentMask* docs) {
  std::vector<irs::doc_id_t> out;
  if (docs) {
    docs->ForEachRun(nullptr, [&](irs::doc_id_t first, irs::doc_id_t last) {
      for (auto doc = first; doc < last; ++doc) {
        out.emplace_back(doc);
      }
      return true;
    });
  }
  return out;
}

std::shared_ptr<const irs::DocumentMask> MaskOf(
  std::initializer_list<irs::doc_id_t> docs) {
  irs::DocumentMaskBuilder builder;
  for (const auto doc : docs) {
    builder.Add(doc);
  }
  return std::make_shared<const irs::DocumentMask>(std::move(builder).Finish());
}

std::vector<irs::doc_id_t> MappedDocs(const irs::DocMap& map,
                                      const irs::DocumentMask& docs) {
  irs::DocumentMaskBuilder out;
  irs::MapDocs(map, docs, out);
  const auto mapped = std::move(out).Finish();
  return DocsOf(&mapped);
}

irs::doc_id_t MappedAlone(const std::vector<irs::doc_id_t>& masked,
                          const irs::DocMap& map, irs::doc_id_t doc) {
  const auto below = std::lower_bound(masked.begin(), masked.end(), doc);
  if (below != masked.end() && *below == doc) {
    return irs::doc_limits::invalid();
  }
  const auto rank = static_cast<irs::doc_id_t>(doc - irs::doc_limits::min() -
                                               (below - masked.begin()));
  if (rank < map.rank_begin || rank >= map.rank_end) {
    return irs::doc_limits::invalid();
  }
  return map.base + (rank - map.rank_begin);
}

irs::doc_id_t Mapped(const irs::DocMap& map, irs::doc_id_t doc) {
  const auto docs = MappedDocs(map, *MaskOf({doc}));
  return docs.empty() ? irs::doc_limits::invalid() : docs.front();
}

irs::DocMap Window(irs::doc_id_t rank_begin, irs::doc_id_t rank_end,
                   irs::doc_id_t base) {
  return {.rank_begin = rank_begin, .rank_end = rank_end, .base = base};
}

}  // namespace

TEST(ParseSegmentIdTest, InvertsFileName) {
  for (const uint64_t id :
       {uint64_t{0}, uint64_t{1}, uint64_t{42}, uint64_t{1} << 32,
        uint64_t{1} << 63, std::numeric_limits<uint64_t>::max()}) {
    uint64_t parsed = 0;
    EXPECT_TRUE(irs::ParseSegmentId(irs::FileName(id), parsed)) << id;
    EXPECT_EQ(id, parsed);
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
  EXPECT_EQ(IdsOf({2, 5}), SegmentsOf(removal));
  ASSERT_EQ(2, removal->groups.size());
  EXPECT_EQ((std::vector<irs::doc_id_t>{3}), DocsOf(&removal->groups[0].docs));
  EXPECT_EQ((std::vector<irs::doc_id_t>{1, 4, 9}),
            DocsOf(&removal->groups[1].docs));
}

TEST(DocRemovalTest, BuildOfNothingIsEmpty) {
  const auto removal = Build({});
  EXPECT_TRUE(removal->groups.empty());
}

TEST(DocRemovalTest, BuildOfOneSegmentSortsItsDocs) {
  std::vector<Row> rows;
  for (irs::doc_id_t doc = 1000; doc > 0; --doc) {
    rows.push_back({7, doc});
  }
  const auto removal = Build(rows);
  ASSERT_EQ(IdsOf({7}), SegmentsOf(removal));
  const auto docs = DocsOf(&removal->groups[0].docs);
  ASSERT_EQ(rows.size(), docs.size());
  for (size_t i = 0; i < docs.size(); ++i) {
    EXPECT_EQ(static_cast<irs::doc_id_t>(i + 1), docs[i]) << i;
  }
}

TEST(DocRemovalTest, BuildSeparatesInterleavedSegments) {
  std::vector<Row> rows;
  for (irs::doc_id_t doc = 1; doc <= 300; ++doc) {
    rows.push_back({doc % 3 == 0 ? 6u : doc % 3 == 1 ? 2u : 4u, doc});
  }
  const auto removal = Build(rows);
  ASSERT_EQ(IdsOf({2, 4, 6}), SegmentsOf(removal));
  for (size_t group = 0; group < 3; ++group) {
    const auto docs = DocsOf(&removal->groups[group].docs);
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
      row.segment = static_cast<uint32_t>(rng() % kSegments);
      row.doc = static_cast<irs::doc_id_t>(1 + rng() % 100000);
      model[row.segment].insert(row.doc);
    }
    const auto removal = Build(rows);
    ASSERT_EQ(model.size(), removal->groups.size());
    size_t group = 0;
    for (const auto& [segment, expected] : model) {
      ASSERT_EQ(IdOf(segment), removal->groups[group].segment);
      ASSERT_EQ(std::vector<irs::doc_id_t>(expected.begin(), expected.end()),
                DocsOf(&removal->groups[group].docs));
      ++group;
    }
  }
}

TEST(DocRemovalBuilderTest, MergesDocsAndRangesIntoTheirSegments) {
  irs::DocRemovalBuilder builder;
  EXPECT_TRUE(builder.Empty());
  builder.Add(4, 7);
  builder.AddRange(4, 7, 10);
  builder.Add(2, 3);
  builder.Add(4, 2);
  EXPECT_FALSE(builder.Empty());
  const auto removal = std::move(builder).Finish(Segments());
  EXPECT_EQ(IdsOf({2, 4}), SegmentsOf(removal));
  EXPECT_EQ((std::vector<irs::doc_id_t>{3}), DocsOf(&removal->groups[0].docs));
  EXPECT_EQ((std::vector<irs::doc_id_t>{2, 7, 8, 9}),
            DocsOf(&removal->groups[1].docs));
}

TEST(DocRemovalBuilderTest, FinishLeavesItEmpty) {
  irs::DocRemovalBuilder builder;
  builder.Add(3, 1);
  const auto first = std::move(builder).Finish(Segments());
  ASSERT_EQ(1, first->groups.size());
  EXPECT_TRUE(builder.Empty());
  builder.Add(5, 2);
  const auto second = std::move(builder).Finish(Segments());
  EXPECT_EQ(IdsOf({5}), SegmentsOf(second));
  EXPECT_EQ((std::vector<irs::doc_id_t>{2}), DocsOf(&second->groups[0].docs));
}

TEST(DocRemovalBuilderTest, SegmentsLeftWithoutDocsAreDropped) {
  irs::DocRemovalBuilder builder;
  builder.AddRange(4, 5, 5);
  builder.Add(6, 3);
  const auto removal = std::move(builder).Finish(Segments());
  EXPECT_EQ(IdsOf({6}), SegmentsOf(removal));
  EXPECT_EQ((std::vector<irs::doc_id_t>{3}), DocsOf(&removal->groups[0].docs));
}

TEST(DocRemovalBuilderTest, AGroupHoldsTheDocMapsOfItsSegment) {
  irs::DocRemovalBuilder builder;
  builder.Add(1, 1);
  builder.Add(6, 1);
  const auto removal = std::move(builder).Finish(Segments());
  ASSERT_EQ(IdsOf({1, 6}), SegmentsOf(removal));
  for (size_t i = 0; const auto index : {1, 6}) {
    const auto& segment =
      irs::utils::downCast<irs::SegmentReader>(Segments()[index]);
    EXPECT_EQ(segment.GetImpl()->DocMaps(), removal->groups[i++].maps);
  }
}

TEST(DocRemovalBuilderTest, ARunOfRowsTakesAFewBytes) {
  irs::DocRemovalBuilder builder;
  for (irs::doc_id_t doc = 1; doc <= 1000000; ++doc) {
    builder.Add(3, doc);
  }
  const auto removal = std::move(builder).Finish(Segments());
  ASSERT_EQ(1000000, removal->groups[0].docs.Count());
  EXPECT_GT(1024, removal->groups[0].docs.FrozenSize());
}

TEST(DocRemovalBuilderTest, ScatteredDocsStayCompact) {
  irs::DocRemovalBuilder builder;
  for (irs::doc_id_t doc = 1; doc < 1000000; doc += 1000) {
    builder.Add(3, doc);
  }
  const auto removal = std::move(builder).Finish(Segments());
  ASSERT_EQ(1000, removal->groups[0].docs.Count());
  EXPECT_GT(4096, removal->groups[0].docs.FrozenSize());
}

TEST(MapDocsTest, AWindowWithoutAMaskShiftsTheDocToItsBase) {
  const auto map = Window(0, 10, 31);
  EXPECT_EQ(31, Mapped(map, 1));
  EXPECT_EQ(35, Mapped(map, 5));
  EXPECT_EQ(40, Mapped(map, 10));
  EXPECT_EQ(irs::doc_limits::invalid(), Mapped(map, 11));
}

TEST(MapDocsTest, MaskedDocsAreGoneAndTheRestCloseUp) {
  const irs::DocMap map{
    .mask = MaskOf({2, 3, 6}), .rank_begin = 0, .rank_end = 5, .base = 1};
  EXPECT_EQ(1, Mapped(map, 1));
  EXPECT_EQ(irs::doc_limits::invalid(), Mapped(map, 2));
  EXPECT_EQ(irs::doc_limits::invalid(), Mapped(map, 3));
  EXPECT_EQ(2, Mapped(map, 4));
  EXPECT_EQ(3, Mapped(map, 5));
  EXPECT_EQ(irs::doc_limits::invalid(), Mapped(map, 6));
  EXPECT_EQ(4, Mapped(map, 7));
  EXPECT_EQ(5, Mapped(map, 8));
  EXPECT_EQ(irs::doc_limits::invalid(), Mapped(map, 9));
}

TEST(MapDocsTest, AWindowTakesOnlyItsRanks) {
  const irs::DocMap map{
    .mask = MaskOf({1}), .rank_begin = 3, .rank_end = 6, .base = 1};
  EXPECT_EQ(irs::doc_limits::invalid(), Mapped(map, 4));
  EXPECT_EQ(1, Mapped(map, 5));
  EXPECT_EQ(3, Mapped(map, 7));
  EXPECT_EQ(irs::doc_limits::invalid(), Mapped(map, 8));
}

TEST(MapDocsTest, TellsWhetherItAddedADoc) {
  const auto map = Window(0, 10, 31);
  irs::DocumentMaskBuilder out;
  EXPECT_FALSE(irs::MapDocs(map, *MaskOf({11}), out));
  EXPECT_TRUE(irs::MapDocs(map, *MaskOf({1, 11}), out));
  EXPECT_FALSE(irs::MapDocs(map, *MaskOf({1}), out));
  EXPECT_EQ(1, out.Count());
}

TEST(MapDocsTest, ManyDocsMapLikeOneAtATime) {
  std::mt19937 gen{29};
  for (int round = 0; round < 30; ++round) {
    const bool unmasked = round % 5 == 0;
    std::vector<irs::doc_id_t> masked;
    irs::DocumentMaskBuilder builder;
    const auto every = 2 + gen() % 40;
    for (irs::doc_id_t doc = 1; !unmasked && doc < 300000; ++doc) {
      if (gen() % every == 0) {
        builder.Add(doc);
        masked.push_back(doc);
      }
    }
    const irs::doc_id_t begin = gen() % 50000;
    const irs::DocMap map{
      .mask = unmasked ? nullptr
                       : std::make_shared<const irs::DocumentMask>(
                           std::move(builder).Finish()),
      .rank_begin = begin,
      .rank_end = static_cast<irs::doc_id_t>(begin + gen() % 250000),
      .base = static_cast<irs::doc_id_t>(1 + gen() % 100000)};
    irs::DocumentMaskBuilder docs;
    std::vector<irs::doc_id_t> expected;
    for (irs::doc_id_t doc = 1; doc < 400000; doc += 1 + gen() % 9) {
      docs.Add(doc);
      if (const auto mapped = MappedAlone(masked, map, doc);
          irs::doc_limits::valid(mapped)) {
        expected.push_back(mapped);
      }
    }
    ASSERT_EQ(expected, MappedDocs(map, std::move(docs).Finish())) << round;
  }
}

TEST(MapDocsTest, RunsMapLikeOneAtATime) {
  std::mt19937 gen{31};
  for (int round = 0; round < 40; ++round) {
    std::vector<irs::doc_id_t> masked;
    irs::DocumentMaskBuilder mask;
    for (irs::doc_id_t doc = 1 + gen() % 50; round % 4 != 0 && doc < 300000;) {
      const auto run = 1 + gen() % (round % 2 == 0 ? 2 : 900);
      for (auto masked_doc = doc; masked_doc < doc + run; ++masked_doc) {
        mask.Add(masked_doc);
        masked.push_back(masked_doc);
      }
      doc += run + 1 + gen() % 2000;
    }
    const irs::doc_id_t begin = gen() % 40000;
    const irs::DocMap map{
      .mask = masked.empty() ? nullptr
                             : std::make_shared<const irs::DocumentMask>(
                                 std::move(mask).Finish()),
      .rank_begin = begin,
      .rank_end = static_cast<irs::doc_id_t>(begin + gen() % 300000),
      .base = static_cast<irs::doc_id_t>(1 + gen() % 100000)};
    irs::DocumentMaskBuilder docs;
    std::vector<irs::doc_id_t> expected;
    for (irs::doc_id_t doc = 1 + gen() % 9; doc < 400000;) {
      const auto run = 1 + gen() % (round % 3 == 0 ? 4 : 5000);
      docs.AddRange(doc, doc + run);
      for (auto removed = doc; removed < doc + run; ++removed) {
        if (const auto mapped = MappedAlone(masked, map, removed);
            irs::doc_limits::valid(mapped)) {
          expected.push_back(mapped);
        }
      }
      doc += run + gen() % 400;
    }
    ASSERT_EQ(expected, MappedDocs(map, std::move(docs).Finish())) << round;
  }
}
