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

#include <absl/algorithm/container.h>
#include <absl/random/random.h>

#include <iresearch/formats/empty_term_reader.hpp>
#include <iresearch/search/detail/lazy_bitset.hpp>
#include <iresearch/search/detail/sorted_docs.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/curve_box_filter.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/search/scorers/constant_score.hpp>
#include <limits>
#include <memory>
#include <pcg_random.hpp>
#include <span>
#include <string>
#include <vector>

#include "filter_test_case_base.hpp"
#include "tests_shared.hpp"

namespace {

using irs::curve::Box;
using irs::curve::Options;
using irs::curve::Point;

inline constexpr irs::field_id kPointFieldId = tests::FieldIdFor("point");
inline constexpr irs::field_id kTagFieldId = tests::FieldIdFor("tag");

class CodeField final : public tests::BinaryField {
 public:
  CodeField() {
    id = kPointFieldId;
    index_features = irs::IndexFeatures::Ranges;
  }

  irs::DictRef Dict() const noexcept final {
    return irs::NumericLayout::kNumbers;
  }

  bool InsertBlockInto(const irs::IndexWriter::Document& doc) const final {
    return doc.WithTokens(id, irs::NumericLayout::kNumbers, index_features,
                          nullptr,
                          [&](irs::FieldInverter& field, irs::TokenSink& sink) {
                            auto& tokens = GetTokens();
                            field.Configure(tokens.Traits());
                            tokens.Fill(tests::ToStringT(Value()), doc.DocId(),
                                        sink, {field.Layout()});
                          });
  }
};

struct Entry {
  Point point{};
  std::string tag;
};

using Segment = std::vector<Entry>;

bool InCell(const Point& point, const Box& box, const Options& options) {
  const auto shift = 64 - options.max_level;
  for (uint32_t axis = 0; axis < options.dimensions; ++axis) {
    const auto cell = point[axis] >> shift;
    if (cell < box.min[axis] >> shift || cell > box.max[axis] >> shift) {
      return false;
    }
  }
  return true;
}

irs::ByCurveBox MakeFilter(const Box& box, const Options& options) {
  irs::ByCurveBox filter;
  *filter.mutable_field_id() = kPointFieldId;
  filter.mutable_options()->box = box;
  filter.mutable_options()->curve = options;
  return filter;
}

std::unique_ptr<irs::ByCurveBox> MakeFilterPtr(const Box& box,
                                               const Options& options) {
  return std::make_unique<irs::ByCurveBox>(MakeFilter(box, options));
}

irs::TermClause TagClause(std::string_view tag) {
  return {.field = kTagFieldId,
          .dict = irs::StringLayout::kStrings,
          .term = irs::bstring{irs::ViewCast<irs::byte_type>(tag)}};
}

uint64_t Scaled(uint64_t value, uint32_t bits) { return value << (64 - bits); }

Point SmallPoint(pcg64& random, uint32_t dimensions, uint32_t cells,
                 uint32_t bits) {
  Point point{};
  for (uint32_t axis = 0; axis < dimensions; ++axis) {
    point[axis] = Scaled(absl::Uniform(random, uint32_t{0}, cells), bits);
  }
  return point;
}

Box SmallBox(pcg64& random, uint32_t dimensions, uint32_t cells,
             uint32_t bits) {
  Box box;
  const auto padding = (uint64_t{1} << (64 - bits)) - 1;
  for (uint32_t axis = 0; axis < dimensions; ++axis) {
    if (absl::Uniform(random, 0, 6) == 0) {
      box.max[axis] = std::numeric_limits<uint64_t>::max();
      continue;
    }
    const auto low = absl::Uniform(random, uint32_t{0}, cells);
    const auto width = absl::Uniform(random, uint32_t{0}, cells);
    box.min[axis] = Scaled(low, bits);
    box.max[axis] = Scaled(std::min(low + width, cells - 1), bits) | padding;
  }
  return box;
}

class CurveBoxFilterTestCase : public tests::FilterTestCaseBase {
 protected:
  void Write(const Options& options, std::span<const Segment> segments) {
    auto writer = open_writer(irs::kOmCreate);
    CodeField code;
    tests::StringField tag{"tag"};
    tag.id = kTagFieldId;
    irs::curve::Code bytes{};
    for (const auto& segment : segments) {
      auto batch = writer->GetBatch();
      for (const auto& entry : segment) {
        const auto size =
          irs::curve::LeafCode(entry.point, options, bytes.data());
        code.value(irs::bytes_view{bytes.data(), size});
        tag.value(entry.tag);
        auto doc = batch.Insert();
        ASSERT_TRUE(tests::InsertField(doc, code));
        ASSERT_TRUE(tests::InsertField(doc, tag));
      }
      batch.Commit();
      writer->RefreshCommit();
    }
  }

  template<typename Match>
  static Docs Expected(std::span<const Segment> segments, Match match) {
    Docs docs;
    for (const auto& segment : segments) {
      for (size_t i = 0; i < segment.size(); ++i) {
        if (match(segment[i])) {
          docs.push_back(static_cast<irs::doc_id_t>(i + 1));
        }
      }
    }
    return docs;
  }
};

TEST(by_curve_box_test, options) {
  irs::ByCurveBoxOptions options;
  ASSERT_EQ(Box{}, options.box);
  ASSERT_EQ(Options{}, options.curve);
}

TEST(by_curve_box_test, ctor) {
  irs::ByCurveBox filter;
  ASSERT_EQ(irs::Type<irs::ByCurveBox>::id(), filter.type());
  ASSERT_EQ(irs::ByCurveBoxOptions{}, filter.options());
  ASSERT_EQ(irs::kNoBoost, filter.GetBoost());
  ASSERT_EQ(irs::DictRef{irs::NumericLayout::kNumbers}, filter.dict());
}

TEST(by_curve_box_test, equal) {
  const Options options{.dimensions = 3, .max_level = 12};
  Box box;
  box.max[0] = 7;
  const auto filter = MakeFilter(box, options);
  ASSERT_EQ(filter, MakeFilter(box, options));
  auto other_box = box;
  other_box.min[2] = 1;
  ASSERT_NE(filter, MakeFilter(other_box, options));
  ASSERT_NE(filter, MakeFilter(box, Options{.dimensions = 3, .max_level = 13}));
  ASSERT_NE(filter,
            MakeFilter(
              box, Options{.dimensions = 3, .max_level = 12, .hilbert = true}));
  auto other_field = MakeFilter(box, options);
  *other_field.mutable_field_id() = kTagFieldId;
  ASSERT_NE(filter, other_field);
}

TEST(by_curve_box_test, reader_without_terms) {
  struct Everything final : irs::RangePruner {
    irs::RangeFit Fit(irs::bytes_view, irs::bytes_view) const final {
      return irs::RangeFit::Inside;
    }
    uint64_t Select(irs::bytes_view, const irs::byte_type*, uint32_t, uint32_t,
                    uint32_t) const final {
      return 1;
    }
  };
  const irs::EmptyTermReader reader{5};
  uint32_t visited = 0;
  reader.VisitPruned(
    Everything{}, [&](irs::bytes_view, const irs::PostingMeta&) { ++visited; });
  ASSERT_EQ(0U, visited);
}

TEST(by_curve_box_test, segment_without_the_field) {
  auto filter = MakeFilter(Box{}, Options{});
  filter.SetBoost(1.5f);
  tests::PreparedFilter prepared{filter, irs::SubReader::empty()};
  ASSERT_TRUE(irs::QueryBuilder::IsEmpty(*prepared.Query(0)));
}

TEST(sorted_docs_test, empty) {
  irs::detail::SortedDocs docs{{}};
  ASSERT_TRUE(irs::doc_limits::eof(docs.Next()));
  ASSERT_TRUE(irs::doc_limits::eof(docs.Next()));
  irs::detail::SortedDocs seeking{{}};
  ASSERT_TRUE(irs::doc_limits::eof(seeking.Seek(1)));
  ASSERT_TRUE(irs::doc_limits::eof(seeking.Seek(1)));
}

TEST(sorted_docs_test, next_reads_every_doc) {
  const std::vector<irs::doc_id_t> values{1, 2, 7, 100, 101};
  irs::detail::SortedDocs docs{values};
  for (const auto value : values) {
    ASSERT_EQ(value, docs.Next());
  }
  ASSERT_TRUE(irs::doc_limits::eof(docs.Next()));
}

TEST(sorted_docs_test, seek_stays_on_a_doc_behind_the_target) {
  const std::vector<irs::doc_id_t> values{5, 9, 12};
  irs::detail::SortedDocs docs{values};
  ASSERT_EQ(9, docs.Seek(6));
  ASSERT_EQ(9, docs.Seek(9));
  ASSERT_EQ(9, docs.Seek(3));
  ASSERT_EQ(12, docs.Next());
  ASSERT_TRUE(irs::doc_limits::eof(docs.Seek(13)));
  ASSERT_TRUE(irs::doc_limits::eof(docs.Seek(5)));
  ASSERT_TRUE(irs::doc_limits::eof(docs.Next()));
}

TEST(sorted_docs_test, seek_agrees_with_lower_bound) {
  pcg64 random{20261106};
  for (const auto size : {1U, 2U, 3U, 15U, 16U, 17U, 1000U}) {
    SCOPED_TRACE(size);
    std::vector<irs::doc_id_t> values(size);
    irs::doc_id_t doc = 0;
    for (auto& value : values) {
      doc += absl::Uniform(random, 1U, 20U);
      value = doc;
    }
    for (uint32_t round = 0; round < 500; ++round) {
      irs::detail::SortedDocs docs{values};
      irs::doc_id_t current = irs::doc_limits::invalid();
      while (!irs::doc_limits::eof(current)) {
        const auto target = current + absl::Uniform(random, 1U, 60U);
        const auto it = absl::c_lower_bound(values, target);
        const auto expected = it == values.end() ? irs::doc_limits::eof() : *it;
        ASSERT_EQ(expected, absl::Uniform(random, 0, 2) == 0
                              ? docs.Seek(target)
                              : docs.Probe(target));
        current = expected;
        if (!irs::doc_limits::eof(current) &&
            absl::Uniform(random, 0, 3) == 0) {
          const auto next = it + 1;
          ASSERT_EQ(next == values.end() ? irs::doc_limits::eof() : *next,
                    docs.Next());
          current = next == values.end() ? irs::doc_limits::eof() : *next;
        }
      }
    }
  }
}

TEST_P(CurveBoxFilterTestCase, matches_every_shape) {
  pcg64 random{1317};
  for (const uint32_t dimensions : {2U, 3U, 5U, 8U}) {
    SCOPED_TRACE(dimensions);
    for (const bool hilbert : {false, true}) {
      SCOPED_TRACE(hilbert);
      for (const uint32_t max_level :
           {1U, 3U, 16U, irs::curve::MaxLevel(dimensions)}) {
        SCOPED_TRACE(max_level);
        const Options options{
          .dimensions = dimensions, .max_level = max_level, .hilbert = hilbert};
        std::vector<Segment> segments(3);
        for (size_t i = 0; i < segments.size(); ++i) {
          segments[i].resize(size_t{1} << (i * 4));
          for (auto& entry : segments[i]) {
            entry.point = SmallPoint(random, dimensions, 16, 16);
          }
        }
        Write(options, segments);
        auto reader = open_reader();
        for (uint32_t sample = 0; sample < 10; ++sample) {
          const auto box = SmallBox(random, dimensions, 16, 16);
          CheckQuery(MakeFilter(box, options),
                     Expected(segments,
                              [&](const Entry& entry) {
                                return InCell(entry.point, box, options);
                              }),
                     reader);
        }
      }
    }
  }
}

TEST_P(CurveBoxFilterTestCase, points_shared_by_many_docs) {
  pcg64 random{1743};
  const Options options{.dimensions = 2, .max_level = 8};
  for (const uint32_t copies : {1U, 2U, 63U, 64U, 65U, 700U}) {
    SCOPED_TRACE(copies);
    std::vector<Segment> segments(1);
    auto& segment = segments.front();
    for (uint32_t i = 0; i < 3000; ++i) {
      const auto point =
        i % copies == 0 ? SmallPoint(random, 2, 64, 8) : segment.back().point;
      segment.push_back({.point = point});
    }
    Write(options, segments);
    auto reader = open_reader();
    for (uint32_t sample = 0; sample < 16; ++sample) {
      const auto box = SmallBox(random, 2, 64, 8);
      CheckQuery(MakeFilter(box, options),
                 Expected(segments,
                          [&](const Entry& entry) {
                            return InCell(entry.point, box, options);
                          }),
                 reader);
    }
  }
}

TEST_P(CurveBoxFilterTestCase, single_term_and_single_doc) {
  const Options options{.dimensions = 2, .max_level = 32};
  Point inside{};
  inside[0] = uint64_t{5} << 40;
  inside[1] = uint64_t{9} << 40;
  Box hit;
  hit.min = inside;
  hit.max = inside;
  Box miss;
  miss.min[0] = inside[0] + (uint64_t{1} << 33);
  miss.max[0] = std::numeric_limits<uint64_t>::max();
  miss.max[1] = std::numeric_limits<uint64_t>::max();

  for (const size_t docs : {size_t{1}, size_t{5000}}) {
    SCOPED_TRACE(docs);
    std::vector<Segment> segments{Segment(docs, Entry{.point = inside})};
    Write(options, segments);
    auto reader = open_reader();
    Docs all(docs);
    absl::c_iota(all, irs::doc_id_t{1});
    CheckQuery(MakeFilter(hit, options), all, reader);
    CheckQuery(MakeFilter(miss, options), Docs{}, reader);
  }
}

TEST_P(CurveBoxFilterTestCase, inverted_and_whole_boxes) {
  pcg64 random{1551};
  for (const bool hilbert : {false, true}) {
    SCOPED_TRACE(hilbert);
    const Options options{.dimensions = 3, .max_level = 20, .hilbert = hilbert};
    std::vector<Segment> segments(2);
    for (auto& segment : segments) {
      segment.resize(900);
      for (auto& entry : segment) {
        for (uint32_t axis = 0; axis < 3; ++axis) {
          entry.point[axis] = random();
        }
      }
    }
    Write(options, segments);
    auto reader = open_reader();
    Box whole;
    whole.max.fill(std::numeric_limits<uint64_t>::max());
    CheckQuery(MakeFilter(whole, options),
               Expected(segments, [](const Entry&) { return true; }), reader);
    auto inverted = whole;
    inverted.min[1] = 7;
    inverted.max[1] = 6;
    CheckQuery(MakeFilter(inverted, options), Docs{}, reader);
  }
}

TEST_P(CurveBoxFilterTestCase, inside_boolean_filters) {
  pcg64 random{1721};
  const Options options{.dimensions = 2, .max_level = 10};
  std::vector<Segment> segments(2);
  segments[0].resize(4000);
  segments[1].resize(300);
  for (auto& segment : segments) {
    for (auto& entry : segment) {
      entry.point = SmallPoint(random, 2, 256, 10);
      entry.tag = absl::Uniform(random, 0, 2) == 0 ? "a" : "b";
    }
  }
  Write(options, segments);
  auto reader = open_reader();
  for (uint32_t sample = 0; sample < 24; ++sample) {
    const auto box = SmallBox(random, 2, 256, 10);
    const auto other = SmallBox(random, 2, 256, 10);
    const auto in_box = [&](const Entry& entry) {
      return InCell(entry.point, box, options);
    };
    const auto in_other = [&](const Entry& entry) {
      return InCell(entry.point, other, options);
    };
    {
      irs::BooleanFilter root;
      root.Add(MakeFilterPtr(box, options), irs::Occur::Must);
      root.Add(TagClause("a"), irs::Occur::Must);
      CheckQuery(*tests::Optimized(std::move(root)),
                 Expected(segments,
                          [&](const Entry& entry) {
                            return in_box(entry) && entry.tag == "a";
                          }),
                 reader);
    }
    {
      irs::BooleanFilter root;
      root.Add(MakeFilterPtr(box, options), irs::Occur::Must);
      root.Add(TagClause("a"), irs::Occur::MustNot);
      CheckQuery(*tests::Optimized(std::move(root)),
                 Expected(segments,
                          [&](const Entry& entry) {
                            return in_box(entry) && entry.tag != "a";
                          }),
                 reader);
    }
    {
      irs::BooleanFilter root;
      root.Add(TagClause("a"), irs::Occur::Must);
      root.Add(MakeFilterPtr(box, options), irs::Occur::MustNot);
      CheckQuery(*tests::Optimized(std::move(root)),
                 Expected(segments,
                          [&](const Entry& entry) {
                            return !in_box(entry) && entry.tag == "a";
                          }),
                 reader);
    }
    {
      irs::BooleanFilter root;
      root.Add(MakeFilterPtr(box, options), irs::Occur::Should);
      root.Add(TagClause("a"), irs::Occur::Should);
      root.SetMinShouldMatch(1);
      CheckQuery(*tests::Optimized(std::move(root)),
                 Expected(segments,
                          [&](const Entry& entry) {
                            return in_box(entry) || entry.tag == "a";
                          }),
                 reader);
    }
    {
      irs::BooleanFilter root;
      root.Add(MakeFilterPtr(box, options), irs::Occur::Must);
      root.Add(MakeFilterPtr(other, options), irs::Occur::Must);
      CheckQuery(*tests::Optimized(std::move(root)),
                 Expected(segments,
                          [&](const Entry& entry) {
                            return in_box(entry) && in_other(entry);
                          }),
                 reader);
    }
    {
      irs::BooleanFilter root;
      root.Add(MakeFilterPtr(box, options), irs::Occur::Must);
      root.Add(MakeFilterPtr(other, options), irs::Occur::MustNot);
      CheckQuery(*tests::Optimized(std::move(root)),
                 Expected(segments,
                          [&](const Entry& entry) {
                            return in_box(entry) && !in_other(entry);
                          }),
                 reader);
    }
    {
      irs::BooleanFilter root;
      root.Add(MakeFilterPtr(box, options), irs::Occur::Should);
      root.Add(MakeFilterPtr(other, options), irs::Occur::Should);
      root.SetMinShouldMatch(1);
      CheckQuery(*tests::Optimized(std::move(root)),
                 Expected(segments,
                          [&](const Entry& entry) {
                            return in_box(entry) || in_other(entry);
                          }),
                 reader);
    }
  }
}

TEST_P(CurveBoxFilterTestCase, scored_docs_carry_the_boost) {
  pcg64 random{1320};
  const Options options{.dimensions = 2, .max_level = 12};
  std::vector<Segment> segments(1);
  segments[0].resize(5000);
  for (auto& entry : segments[0]) {
    entry.point = SmallPoint(random, 2, 1024, 12);
  }
  Write(options, segments);
  auto reader = open_reader();
  irs::Scorer::ptr scorer = irs::BM25::Make(irs::BM25::Options{});
  for (const uint32_t width : {1U, 8U, 64U, 1024U}) {
    SCOPED_TRACE(width);
    Box box;
    box.min[0] = Scaled(1024 - width, 12);
    box.max[0] = std::numeric_limits<uint64_t>::max();
    box.max[1] = std::numeric_limits<uint64_t>::max();
    auto filter = MakeFilter(box, options);
    filter.SetScorer(&irs::ForceConstScore());
    filter.SetBoost(1.5f);
    ScoredDocs expected;
    for (const auto doc : Expected(segments, [&](const Entry& entry) {
           return InCell(entry.point, box, options);
         })) {
      expected.emplace_back(doc, std::vector<irs::score_t>{1.5f});
    }
    CheckQuery(filter, std::span{&scorer, 1}, expected, reader);
  }
}

TEST_P(CurveBoxFilterTestCase, scores_add_up_inside_boolean_filters) {
  pcg64 random{1317};
  const Options options{.dimensions = 2, .max_level = 12};
  std::vector<Segment> segments(1);
  segments[0].resize(3000);
  for (auto& entry : segments[0]) {
    entry.point = SmallPoint(random, 2, 64, 12);
    entry.tag = absl::Uniform(random, 0, 2) == 0 ? "a" : "b";
  }
  Write(options, segments);
  auto reader = open_reader();
  irs::Scorer::ptr scorer = irs::BM25::Make(irs::BM25::Options{});
  const auto constant_box = [&](const Box& box, irs::score_t boost) {
    auto filter = MakeFilterPtr(box, options);
    filter->SetScorer(&irs::ForceConstScore());
    filter->SetBoost(boost);
    return filter;
  };
  const auto constant_tag = [] {
    auto clause = TagClause("a");
    clause.scorer = &irs::ForceConstScore();
    return clause;
  };
  for (const uint32_t width : {1U, 8U, 64U}) {
    SCOPED_TRACE(width);
    Box box;
    box.max[0] = Scaled(width, 12) - 1;
    box.max[1] = std::numeric_limits<uint64_t>::max();
    Box half;
    half.max[0] = std::numeric_limits<uint64_t>::max();
    half.max[1] = Scaled(32, 12) - 1;
    {
      irs::BooleanFilter root;
      root.Add(constant_box(box, 1.5f), irs::Occur::Should);
      root.Add(constant_tag(), irs::Occur::Should);
      root.SetMinShouldMatch(1);
      ScoredDocs expected;
      for (size_t i = 0; i < segments[0].size(); ++i) {
        const auto& entry = segments[0][i];
        const irs::score_t score =
          (InCell(entry.point, box, options) ? 1.5f : 0) +
          (entry.tag == "a" ? 1.f : 0);
        if (score != 0) {
          expected.emplace_back(static_cast<irs::doc_id_t>(i + 1),
                                std::vector<irs::score_t>{score});
        }
      }
      CheckQuery(*tests::Optimized(std::move(root), scorer.get()),
                 std::span{&scorer, 1}, expected, reader);
    }
    {
      auto nested = std::make_unique<irs::BooleanFilter>();
      nested->Add(constant_box(box, 1.5f), irs::Occur::Must);
      nested->Add(constant_tag(), irs::Occur::Must);
      nested->SetBoost(2.f);
      irs::BooleanFilter root;
      root.Add(std::move(nested), irs::Occur::Must);
      root.Add(constant_box(half, 1.f), irs::Occur::Must);
      ScoredDocs expected;
      for (const auto doc : Expected(segments, [&](const Entry& entry) {
             return entry.tag == "a" && InCell(entry.point, box, options) &&
                    InCell(entry.point, half, options);
           })) {
        expected.emplace_back(doc, std::vector<irs::score_t>{6.f});
      }
      CheckQuery(*tests::Optimized(std::move(root), scorer.get()),
                 std::span{&scorer, 1}, expected, reader);
    }
  }
}

TEST_P(CurveBoxFilterTestCase, sparse_sets_read_whole_posting_blocks) {
  const Options options{.dimensions = 2, .max_level = 20};
  constexpr uint32_t kDocs = 320000;
  constexpr uint32_t kShared = 300;
  std::vector<Segment> segments(1);
  auto& segment = segments.front();
  segment.resize(kDocs);
  for (uint32_t i = 0; i < kDocs; ++i) {
    segment[i].point[0] = Scaled(i >> 10, 20);
    segment[i].point[1] = Scaled(i & 1023, 20);
  }
  const auto place = [&](uint32_t doc, uint32_t x, uint32_t y) {
    segment[doc].point[0] = Scaled(x, 20);
    segment[doc].point[1] = Scaled(y, 20);
  };
  uint32_t gapped = 3000;
  for (uint32_t i = 0; i < kShared; ++i) {
    place(1000 + i, 5000, 7);
    place(gapped, 6000, 7);
    gapped += 1 + (i % 3 == 0 ? 1 : 0) + (i % 7 == 0 ? 1 : 0);
  }
  place(5000, 5001, 7);
  place(5001, 6001, 7);
  Write(options, segments);
  auto reader = open_reader();
  ASSERT_EQ(1, reader.size());
  for (const uint32_t x : {5000U, 6000U}) {
    SCOPED_TRACE(x);
    Box box;
    box.min[0] = Scaled(x, 20);
    box.max[0] = Scaled(x + 2, 20) - 1;
    box.min[1] = Scaled(7, 20);
    box.max[1] = Scaled(8, 20) - 1;
    const auto expected = Expected(segments, [&](const Entry& entry) {
      return InCell(entry.point, box, options);
    });
    ASSERT_EQ(kShared + 1, expected.size());
    CheckQuery(MakeFilter(box, options), expected, reader);
  }
}

TEST_P(CurveBoxFilterTestCase, doc_sets_stay_intact_between_uses) {
  pcg64 random{1551};
  const Options options{.dimensions = 2, .max_level = 12};
  std::vector<Segment> segments(1);
  segments[0].resize(20000);
  for (auto& entry : segments[0]) {
    entry.point = SmallPoint(random, 2, 256, 12);
  }
  Write(options, segments);
  auto reader = open_reader();
  ASSERT_EQ(1, reader.size());
  const auto& segment = reader[0];
  const auto docs_count = static_cast<irs::doc_id_t>(segment.docs_count());
  for (const uint32_t width : {1U, 4U, 16U, 128U, 256U}) {
    SCOPED_TRACE(width);
    Box box;
    box.max[0] = Scaled(width, 12) - 1;
    box.max[1] = Scaled(width, 12) - 1;
    const auto filter = MakeFilter(box, options);
    const auto query = filter.PrepareSegment(segment, {});
    const auto expected = Expected(segments, [&](const Entry& entry) {
      return InCell(entry.point, box, options);
    });
    if (expected.empty()) {
      ASSERT_TRUE(irs::QueryBuilder::IsEmpty(*query));
      continue;
    }
    const_cast<irs::QueryBuilder&>(*query).SetBoost(2.f);
    ASSERT_EQ(2.f, query->Boost());
    for (uint32_t round = 0; round < 3; ++round) {
      auto set = irs::detail::MakeDocSet(query.get(), docs_count);
      ASSERT_EQ(expected.size(), set.Count());
      for (const auto doc : expected) {
        ASSERT_TRUE(set.Contains(doc));
      }
    }
  }
}

inline constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(curve_box_filter_test, CurveBoxFilterTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs)),
                         CurveBoxFilterTestCase::to_string);

}  // namespace
