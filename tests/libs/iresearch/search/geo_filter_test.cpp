////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include <set>

#include "formats/column/test_cs_helpers.hpp"
#include "geo/geo_json.h"
#include "geo_test_helpers.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/geo_filter.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/store/store_utils.hpp"
#include "s2/s2point_region.h"
#include "s2/s2polygon.h"
#include "search_fields.hpp"
#include "tests_shared.hpp"

namespace {

using namespace sdb::geo;
using namespace irs;
using namespace irs::tests;

inline constexpr irs::field_id kName = 1;
inline constexpr irs::field_id kGeo = 2;

struct CustomSort final : public irs::ScorerBase<void> {
  static constexpr std::string_view type_name() noexcept {
    return "custom_sort";
  }

  struct Scorer : public irs::ScoreOperator {
    Scorer(const CustomSort& sort, const irs::ScoreContext& ctx)
      : document_attrs(ctx.doc_attrs),
        stats(ctx.stats),
        segment_reader(ctx.segment),
        sort(sort) {}

    template<irs::ScoreMergeType MergeType = irs::ScoreMergeType::Noop>
    void ScoreImpl(irs::score_t* res, irs::scores_size_t n) const noexcept {
      ASSERT_EQ(MergeType, irs::ScoreMergeType::Noop);
      if (sort.scorer_score) {
        sort.scorer_score(this, res, n);
      }
    }

    void Score(irs::score_t* res, irs::scores_size_t n) const noexcept final {
      ScoreImpl(res, n);
    }
    void ScoreSum(irs::score_t* res,
                  irs::scores_size_t n) const noexcept final {
      ScoreImpl<irs::ScoreMergeType::Sum>(res, n);
    }
    void ScoreMax(irs::score_t* res,
                  irs::scores_size_t n) const noexcept final {
      ScoreImpl<irs::ScoreMergeType::Max>(res, n);
    }

    const irs::AttributeProvider& document_attrs;
    const irs::byte_type* stats;
    const irs::NormProvider& segment_reader;
    const CustomSort& sort;
  };

  void collect(irs::byte_type* stats, const irs::FieldCollector* field,
               const irs::TermCollector* term) const final {
    if (collector_finish) {
      collector_finish(stats, field, term);
    }
  }

  irs::IndexFeatures GetIndexFeatures() const final {
    return irs::IndexFeatures::None;
  }

  irs::ScoreFunction PrepareScorer(const irs::ScoreContext& ctx) const final {
    if (_prepare_scorer) {
      _prepare_scorer(ctx);
    }

    return irs::ScoreFunction::Make<CustomSort::Scorer>(*this, ctx);
  }

  std::function<void(irs::byte_type*, const irs::FieldCollector*,
                     const irs::TermCollector*)>
    collector_finish;
  std::function<void(const irs::ScoreContext& ctx)> _prepare_scorer;  // NOLINT
  std::function<void(const irs::ScoreOperator*, irs::score_t*, size_t n)>
    scorer_score;

  CustomSort() = default;
};

}  // namespace

TEST(GeoFilterTest, options) {
  const S2RegionTermIndexer::Options s2opts;
  const GeoFilterOptions opts;
  ASSERT_TRUE(opts.prefix.empty());
  ASSERT_TRUE(opts.shape.empty());
  ASSERT_EQ(s2opts.level_mod(), opts.options.level_mod());
  ASSERT_EQ(s2opts.min_level(), opts.options.min_level());
  ASSERT_EQ(s2opts.max_level(), opts.options.max_level());
  ASSERT_EQ(s2opts.max_cells(), opts.options.max_cells());
  ASSERT_EQ(s2opts.marker(), opts.options.marker());
  ASSERT_EQ(s2opts.index_contains_points_only(),
            opts.options.index_contains_points_only());
  ASSERT_EQ(s2opts.optimize_for_space(), opts.options.optimize_for_space());
  ASSERT_EQ(GeoFilterType::Intersects, opts.type);
}

TEST(GeoFilterTest, ctor) {
  GeoFilter q;
  ASSERT_EQ(irs::Type<GeoFilter>::id(), q.type());
  ASSERT_EQ(irs::field_limits::invalid(), q.field_id());
  ASSERT_EQ(irs::kNoBoost, q.Boost());
#ifndef SDB_DEV
  ASSERT_EQ(GeoFilterOptions{}, q.options());
#endif
}

TEST(GeoFilterTest, equal) {
  GeoFilter q;
  q.mutable_options()->type = GeoFilterType::Intersects;
  q.mutable_options()->shape.reset(
    std::make_unique<S2PointRegion>(S2Point{1., 0., 0.}),
    ShapeContainer::Type::S2Point);
  *q.mutable_field_id() = 1;

  {
    GeoFilter q1;
    q1.mutable_options()->type = GeoFilterType::Intersects;
    q1.mutable_options()->shape.reset(
      std::make_unique<S2PointRegion>(S2Point{1., 0., 0.}),
      ShapeContainer::Type::S2Point);
    *q1.mutable_field_id() = 1;
    ASSERT_EQ(q, q1);
  }

  {
    GeoFilter q1;
    q1.boost(1.5);
    q1.mutable_options()->type = GeoFilterType::Intersects;
    q1.mutable_options()->shape.reset(
      std::make_unique<S2PointRegion>(S2Point{1., 0., 0.}),
      ShapeContainer::Type::S2Point);
    *q1.mutable_field_id() = 1;
    ASSERT_EQ(q, q1);
  }

  {
    GeoFilter q1;
    q1.mutable_options()->type = GeoFilterType::Intersects;
    q1.mutable_options()->shape.reset(
      std::make_unique<S2PointRegion>(S2Point{1., 0., 0.}),
      ShapeContainer::Type::S2Point);
    *q1.mutable_field_id() = 2;
    ASSERT_NE(q, q1);
  }

  {
    GeoFilter q1;
    q1.mutable_options()->type = GeoFilterType::Contains;
    q1.mutable_options()->shape.reset(
      std::make_unique<S2PointRegion>(S2Point{1., 0., 0.}),
      ShapeContainer::Type::S2Point);
    *q1.mutable_field_id() = 1;
    ASSERT_NE(q, q1);
  }

  {
    GeoFilter q1;
    q1.mutable_options()->type = GeoFilterType::Contains;
    q1.mutable_options()->shape.reset(std::make_unique<S2Polygon>(),
                                      ShapeContainer::Type::S2Polygon);
    *q1.mutable_field_id() = 1;
    ASSERT_NE(q, q1);
  }
}

TEST(GeoFilterTest, boost) {
  // no boost
  {
    GeoFilter q;
    q.mutable_options()->type = GeoFilterType::Intersects;
    q.mutable_options()->shape.reset(
      std::make_unique<S2PointRegion>(S2Point{1., 0., 0.}),
      ShapeContainer::Type::S2Point);
    *q.mutable_field_id() = 1;
    q.mutable_options()->store_field_id = kGeo;

    auto prepared = q.prepare({.index = irs::SubReader::empty()});
    ASSERT_EQ(irs::kNoBoost, prepared->Boost());
  }

  // with boost
  {
    irs::score_t boost = 1.5f;
    GeoFilter q;
    q.mutable_options()->type = GeoFilterType::Intersects;
    q.mutable_options()->shape.reset(
      std::make_unique<S2PointRegion>(S2Point{1., 0., 0.}),
      ShapeContainer::Type::S2Point);
    *q.mutable_field_id() = 1;
    q.mutable_options()->store_field_id = kGeo;
    q.boost(boost);

    auto prepared = q.prepare({.index = irs::SubReader::empty()});
    ASSERT_EQ(boost, prepared->Boost());
  }
}

TEST(GeoFilterTest, query) {
  auto docs = tests::ParseGeoDocs(R"([
    { "name": "A", "geometry": { "type": "Point", "coordinates": [ 37.615895, 55.7039   ] } },
    { "name": "B", "geometry": { "type": "Point", "coordinates": [ 37.615315, 55.703915 ] } },
    { "name": "C", "geometry": { "type": "Point", "coordinates": [ 37.61509, 55.703537  ] } },
    { "name": "D", "geometry": { "type": "Point", "coordinates": [ 37.614183, 55.703806 ] } },
    { "name": "E", "geometry": { "type": "Point", "coordinates": [ 37.613792, 55.704405 ] } },
    { "name": "F", "geometry": { "type": "Point", "coordinates": [ 37.614956, 55.704695 ] } },
    { "name": "G", "geometry": { "type": "Point", "coordinates": [ 37.616297, 55.704831 ] } },
    { "name": "H", "geometry": { "type": "Point", "coordinates": [ 37.617053, 55.70461  ] } },
    { "name": "I", "geometry": { "type": "Point", "coordinates": [ 37.61582, 55.704459  ] } },
    { "name": "J", "geometry": { "type": "Point", "coordinates": [ 37.614634, 55.704338 ] } },
    { "name": "K", "geometry": { "type": "Point", "coordinates": [ 37.613121, 55.704193 ] } },
    { "name": "L", "geometry": { "type": "Point", "coordinates": [ 37.614135, 55.703298 ] } },
    { "name": "M", "geometry": { "type": "Point", "coordinates": [ 37.613663, 55.704002 ] } },
    { "name": "N", "geometry": { "type": "Point", "coordinates": [ 37.616522, 55.704235 ] } },
    { "name": "O", "geometry": { "type": "Point", "coordinates": [ 37.615508, 55.704172 ] } },
    { "name": "P", "geometry": { "type": "Point", "coordinates": [ 37.614629, 55.704081 ] } },
    { "name": "Q", "geometry": { "type": "Point", "coordinates": [ 37.610235, 55.709754 ] } },
    { "name": "R", "geometry": { "type": "Point", "coordinates": [ 37.605,    55.707917 ] } },
    { "name": "S", "geometry": { "type": "Point", "coordinates": [ 37.545776, 55.722083 ] } },
    { "name": "T", "geometry": { "type": "Point", "coordinates": [ 37.559509, 55.715895 ] } },
    { "name": "U", "geometry": { "type": "Point", "coordinates": [ 37.701645, 55.832144 ] } },
    { "name": "V", "geometry": { "type": "Point", "coordinates": [ 37.73735,  55.816715 ] } },
    { "name": "W", "geometry": { "type": "Point", "coordinates": [ 37.75589,  55.798193 ] } },
    { "name": "X", "geometry": { "type": "Point", "coordinates": [ 37.659073, 55.843711 ] } },
    { "name": "Y", "geometry": { "type": "Point", "coordinates": [ 37.778549, 55.823659 ] } },
    { "name": "Z", "geometry": { "type": "Point", "coordinates": [ 37.729797, 55.853733 ] } },
    { "name": "1", "geometry": { "type": "Point", "coordinates": [ 37.608261, 55.784682 ] } },
    { "name": "2", "geometry": { "type": "Point", "coordinates": [ 37.525177, 55.802825 ] } }
  ])");

  irs::MemoryDirectory dir;
  irs::DirectoryReader reader;

  // index data
  {
    constexpr auto kFormatId = "1_5simd";
    auto codec = irs::formats::Get(kFormatId);
    ASSERT_NE(nullptr, codec);
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);
    GeoField geo_field;
    geo_field.field_name = "geometry";
    geo_field.id = kGeo;
    StringField name_field;
    name_field.field_name = "name";
    name_field.id = kName;
    {
      auto segment0 = writer->GetBatch();
      auto segment1 = writer->GetBatch();
      {
        size_t i = 0;
        for (const auto& doc_entry : docs) {
          geo_field.value = doc_entry.geometry;
          name_field.value = doc_entry.name;

          auto doc = (i++ % 2 ? segment0 : segment1).Insert();
          ASSERT_TRUE(doc.Insert(name_field));
          ASSERT_TRUE(doc.Insert(geo_field));
          irs::tests::StoreFieldAt(*doc.GetColWriter(), kName, doc.DocId(),
                                   name_field);
          irs::tests::StoreFieldAt(*doc.GetColWriter(), kGeo, doc.DocId(),
                                   geo_field);
        }
      }
      segment1.Commit();
      segment0.Commit();
    }
    writer->RefreshCommit();
    reader = writer->GetSnapshot();
  }

  ASSERT_NE(nullptr, reader);
  ASSERT_EQ(2U, reader->size());
  ASSERT_EQ(docs.size(), reader->docs_count());
  ASSERT_EQ(docs.size(), reader->live_docs_count());

  auto execute_query = [&reader](
                         const irs::Filter& q,
                         const std::vector<irs::CostAttr::Type>& costs) {
    std::set<std::string> actual_results;

    struct MaxMemoryCounter final : irs::IResourceManager {
      void Reset() noexcept {
        current = 0;
        max = 0;
      }

      void Increase(size_t value) final {
        current += value;
        max = std::max(max, current);
      }

      void Decrease(size_t value) noexcept final { current -= value; }

      size_t current{0};
      size_t max{0};
    };

    MaxMemoryCounter counter;
    auto prepared = q.prepare({
      .index = *reader,
      .memory = counter,
    });
    EXPECT_NE(nullptr, prepared);
    auto expected_cost = costs.begin();
    for (auto& segment : *reader) {
      const auto* column = segment.Column(kName);
      EXPECT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto it = prepared->execute({.segment = segment});
      EXPECT_NE(nullptr, it);
      auto seek_it = prepared->execute({.segment = segment});
      EXPECT_NE(nullptr, seek_it);
      auto* cost = irs::get<irs::CostAttr>(*it);
      EXPECT_NE(nullptr, cost);

      EXPECT_NE(expected_cost, costs.end());
      EXPECT_EQ(*expected_cost, cost->estimate());
      ++expected_cost;

      if (irs::doc_limits::eof(it->value())) {
        continue;
      }

      EXPECT_FALSE(irs::doc_limits::valid(it->value()));
      while (it->next()) {
        auto doc_id = it->value();
        EXPECT_EQ(doc_id, seek_it->seek(doc_id));
        EXPECT_EQ(doc_id, seek_it->seek(doc_id));
        EXPECT_FALSE(values.IsNull(doc_id));

        actual_results.emplace(
          irs::tests::ReadStoredStr<std::string>(values, doc_id));
      }
      EXPECT_TRUE(irs::doc_limits::eof(it->value()));
      EXPECT_TRUE(irs::doc_limits::eof(seek_it->seek(it->value())));

      {
        auto it = prepared->execute({.segment = segment});
        EXPECT_NE(nullptr, it);

        while (it->next()) {
          const auto doc_id = it->value();
          auto seek_it = prepared->execute({.segment = segment});
          EXPECT_NE(nullptr, seek_it);
          EXPECT_EQ(doc_id, seek_it->seek(doc_id));
          do {
            if (!values.IsNull(seek_it->value())) {
              EXPECT_NE(
                actual_results.end(),
                actual_results.find(irs::tests::ReadStoredStr<std::string>(
                  values, seek_it->value())));
            }
          } while (seek_it->next());
          EXPECT_TRUE(irs::doc_limits::eof(seek_it->value()));
        }
        EXPECT_TRUE(irs::doc_limits::eof(it->value()));
      }
    }
    EXPECT_EQ(expected_cost, costs.end());

    prepared.reset();
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);

    return actual_results;
  };

  {
    const std::set<std::string> expected{"Q"};

    auto json = tests::FromJson(R"({
      "type": "Point",
      "coordinates": [ 37.610235, 55.709754 ]
    })");

    GeoFilter q;
    q.mutable_options()->type = GeoFilterType::Intersects;
    ASSERT_TRUE(
      json::ParseRegion(json.value(), q.mutable_options()->shape).ok());
    ASSERT_EQ(ShapeContainer::Type::S2Point, q.mutable_options()->shape.type());
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->store_field_id = kGeo;

    ASSERT_EQ(expected, execute_query(q, {2, 0}));
  }

  {
    const std::set<std::string> expected{"Q", "R"};

    auto json = tests::FromJson(R"({
      "type": "Polygon",
      "coordinates": [
          [
              [37.602682, 55.706853],
              [37.613025, 55.706853],
              [37.613025, 55.711906],
              [37.602682, 55.711906],
              [37.602682, 55.706853]
          ]
      ]
    })");

    GeoFilter q;
    q.mutable_options()->type = GeoFilterType::Intersects;
    ASSERT_TRUE(
      json::ParseRegion(json.value(), q.mutable_options()->shape).ok());
    ASSERT_EQ(ShapeContainer::Type::S2Polygon,
              q.mutable_options()->shape.type());
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->store_field_id = kGeo;

    ASSERT_EQ(expected, execute_query(q, {2, 2}));
  }

  {
    const auto& origin = docs[7];
    std::set<std::string> expected{origin.name};

    GeoFilter q;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->store_field_id = kGeo;
    std::vector<S2LatLng> cache;
    ASSERT_TRUE(ParseShape<Parsing::OnlyPoint>(
      tests::FromJson(origin.geometry).value(), q.mutable_options()->shape,
      cache, coding::Options::Invalid, nullptr));
    q.mutable_options()->type = GeoFilterType::Intersects;
    q.mutable_options()->options.set_index_contains_points_only(true);

    ASSERT_EQ(expected, execute_query(q, {2, 4}));
  }

  {
    const auto& origin = docs[7];
    std::set<std::string> expected{origin.name};

    GeoFilter q;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->store_field_id = kGeo;
    std::vector<S2LatLng> cache;
    ASSERT_TRUE(ParseShape<Parsing::OnlyPoint>(
      tests::FromJson(origin.geometry).value(), q.mutable_options()->shape,
      cache, coding::Options::Invalid, nullptr));
    q.mutable_options()->type = GeoFilterType::Contains;
    q.mutable_options()->options.set_index_contains_points_only(true);

    ASSERT_EQ(expected, execute_query(q, {2, 4}));
  }

  {
    const auto& origin = docs[7];
    std::set<std::string> expected{origin.name};

    GeoFilter q;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->store_field_id = kGeo;
    std::vector<S2LatLng> cache;
    ASSERT_TRUE(ParseShape<Parsing::OnlyPoint>(
      tests::FromJson(origin.geometry).value(), q.mutable_options()->shape,
      cache, coding::Options::Invalid, nullptr));
    q.mutable_options()->type = GeoFilterType::IsContained;
    q.mutable_options()->options.set_index_contains_points_only(true);

    ASSERT_EQ(expected, execute_query(q, {2, 4}));
  }

  {
    auto shape_json = tests::FromJson(R"({
      "type": "Polygon",
        "coordinates": [
            [
                [37.590322, 55.695583],
                [37.626114, 55.695583],
                [37.626114, 55.71488],
                [37.590322, 55.71488],
                [37.590322, 55.695583]
            ]
      ]
    })");

    ShapeContainer shape;
    ShapeContainer point;
    std::vector<S2LatLng> cache;
    ASSERT_TRUE(ParseShape<Parsing::GeoJson>(
      shape_json.value(), shape, cache, coding::Options::Invalid, nullptr));
    std::set<std::string> expected;
    for (const auto& doc_entry : docs) {
      ASSERT_TRUE(ParseShape<Parsing::OnlyPoint>(
        tests::FromJson(doc_entry.geometry).value(), point, cache,
        coding::Options::Invalid, nullptr));
      if (!shape.contains(point)) {
        continue;
      }
      expected.emplace(doc_entry.name);
    }

    GeoFilter q;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->store_field_id = kGeo;
    ASSERT_TRUE(ParseShape<Parsing::GeoJson>(
      shape_json.value(), q.mutable_options()->shape, cache,
      coding::Options::Invalid, nullptr));
    q.mutable_options()->type = GeoFilterType::Contains;
    q.mutable_options()->options.set_index_contains_points_only(true);

    EXPECT_EQ(expected, execute_query(q, {18, 18}));
  }

  {
    auto shape_json = tests::FromJson(R"({
      "type": "Polygon",
        "coordinates": [
            [
                [37.590322, 55.695583],
                [37.626114, 55.695583],
                [37.626114, 55.71488],
                [37.590322, 55.71488],
                [37.590322, 55.695583]
            ]
      ]
    })");

    ShapeContainer shape;
    ShapeContainer point;
    std::vector<S2LatLng> cache;
    ASSERT_TRUE(ParseShape<Parsing::GeoJson>(
      shape_json.value(), shape, cache, coding::Options::Invalid, nullptr));
    std::set<std::string> expected;
    for (const auto& doc_entry : docs) {
      ASSERT_TRUE(ParseShape<Parsing::OnlyPoint>(
        tests::FromJson(doc_entry.geometry).value(), point, cache,
        coding::Options::Invalid, nullptr));
      if (!shape.contains(point)) {
        continue;
      }
      expected.emplace(doc_entry.name);
    }

    GeoFilter q;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->store_field_id = kGeo;
    ASSERT_TRUE(ParseShape<Parsing::GeoJson>(
      shape_json.value(), q.mutable_options()->shape, cache,
      coding::Options::Invalid, nullptr));
    q.mutable_options()->type = GeoFilterType::Intersects;

    EXPECT_EQ(expected, execute_query(q, {18, 18}));
  }

  {
    auto shape_json = tests::FromJson(R"({
      "type": "Polygon",
        "coordinates": [
            [
                [37.590322, 55.695583],
                [37.626114, 55.695583],
                [37.626114, 55.71488],
                [37.590322, 55.71488],
                [37.590322, 55.695583]
            ]
      ]
    })");

    ShapeContainer shape;
    ShapeContainer point;
    std::set<std::string> expected;
    std::vector<S2LatLng> cache;

    GeoFilter q;
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->store_field_id = kGeo;
    ASSERT_TRUE(ParseShape<Parsing::GeoJson>(
      shape_json.value(), q.mutable_options()->shape, cache,
      coding::Options::Invalid, nullptr));
    q.mutable_options()->type = GeoFilterType::IsContained;

    EXPECT_EQ(expected, execute_query(q, {18, 18}));
  }
}

TEST(GeoFilterTest, checkScorer) {
  auto docs = tests::ParseGeoDocs(R"([
    { "name": "A", "geometry": { "type": "Point", "coordinates": [ 37.615895, 55.7039   ] } },
    { "name": "B", "geometry": { "type": "Point", "coordinates": [ 37.615315, 55.703915 ] } },
    { "name": "C", "geometry": { "type": "Point", "coordinates": [ 37.61509, 55.703537  ] } },
    { "name": "D", "geometry": { "type": "Point", "coordinates": [ 37.614183, 55.703806 ] } },
    { "name": "E", "geometry": { "type": "Point", "coordinates": [ 37.613792, 55.704405 ] } },
    { "name": "F", "geometry": { "type": "Point", "coordinates": [ 37.614956, 55.704695 ] } },
    { "name": "G", "geometry": { "type": "Point", "coordinates": [ 37.616297, 55.704831 ] } },
    { "name": "H", "geometry": { "type": "Point", "coordinates": [ 37.617053, 55.70461  ] } },
    { "name": "I", "geometry": { "type": "Point", "coordinates": [ 37.61582, 55.704459  ] } },
    { "name": "J", "geometry": { "type": "Point", "coordinates": [ 37.614634, 55.704338 ] } },
    { "name": "K", "geometry": { "type": "Point", "coordinates": [ 37.613121, 55.704193 ] } },
    { "name": "L", "geometry": { "type": "Point", "coordinates": [ 37.614135, 55.703298 ] } },
    { "name": "M", "geometry": { "type": "Point", "coordinates": [ 37.613663, 55.704002 ] } },
    { "name": "N", "geometry": { "type": "Point", "coordinates": [ 37.616522, 55.704235 ] } },
    { "name": "O", "geometry": { "type": "Point", "coordinates": [ 37.615508, 55.704172 ] } },
    { "name": "P", "geometry": { "type": "Point", "coordinates": [ 37.614629, 55.704081 ] } },
    { "name": "Q", "geometry": { "type": "Point", "coordinates": [ 37.610235, 55.709754 ] } },
    { "name": "R", "geometry": { "type": "Point", "coordinates": [ 37.605,    55.707917 ] } },
    { "name": "S", "geometry": { "type": "Point", "coordinates": [ 37.545776, 55.722083 ] } },
    { "name": "T", "geometry": { "type": "Point", "coordinates": [ 37.559509, 55.715895 ] } },
    { "name": "U", "geometry": { "type": "Point", "coordinates": [ 37.701645, 55.832144 ] } },
    { "name": "V", "geometry": { "type": "Point", "coordinates": [ 37.73735,  55.816715 ] } },
    { "name": "W", "geometry": { "type": "Point", "coordinates": [ 37.75589,  55.798193 ] } },
    { "name": "X", "geometry": { "type": "Point", "coordinates": [ 37.659073, 55.843711 ] } },
    { "name": "Y", "geometry": { "type": "Point", "coordinates": [ 37.778549, 55.823659 ] } },
    { "name": "Z", "geometry": { "type": "Point", "coordinates": [ 37.729797, 55.853733 ] } },
    { "name": "1", "geometry": { "type": "Point", "coordinates": [ 37.608261, 55.784682 ] } },
    { "name": "2", "geometry": { "type": "Point", "coordinates": [ 37.525177, 55.802825 ] } }
  ])");

  irs::MemoryDirectory dir;
  irs::DirectoryReader reader;

  // index data
  {
    constexpr auto kFormatId = "1_5simd";
    auto codec = irs::formats::Get(kFormatId);
    ASSERT_NE(nullptr, codec);
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);
    GeoField geo_field;
    geo_field.field_name = "geometry";
    geo_field.id = kGeo;
    StringField name_field;
    name_field.field_name = "name";
    name_field.id = kName;
    {
      auto segment0 = writer->GetBatch();
      auto segment1 = writer->GetBatch();
      {
        size_t i = 0;
        for (const auto& doc_entry : docs) {
          geo_field.value = doc_entry.geometry;
          name_field.value = doc_entry.name;

          auto doc = (i++ % 2 ? segment0 : segment1).Insert();
          ASSERT_TRUE(doc.Insert(name_field));
          ASSERT_TRUE(doc.Insert(geo_field));
          irs::tests::StoreFieldAt(*doc.GetColWriter(), kName, doc.DocId(),
                                   name_field);
          irs::tests::StoreFieldAt(*doc.GetColWriter(), kGeo, doc.DocId(),
                                   geo_field);
        }
      }
      segment1.Commit();
      segment0.Commit();
    }
    writer->RefreshCommit();
    reader = writer->GetSnapshot();
  }

  ASSERT_NE(nullptr, reader);
  ASSERT_EQ(2, reader->size());
  ASSERT_EQ(docs.size(), reader->docs_count());
  ASSERT_EQ(docs.size(), reader->live_docs_count());

  irs::DocIterator* cur_it = nullptr;
  auto execute_query = [&](const irs::Filter& q, const irs::Scorer& ord) {
    std::map<std::string, score_t> actual_results;

    auto prepared = q.prepare({.index = *reader, .scorer = &ord});
    EXPECT_NE(nullptr, prepared);
    for (auto& segment : *reader) {
      const auto* column = segment.Column(kName);
      EXPECT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      auto it = prepared->execute({.segment = segment, .scorer = &ord});
      EXPECT_NE(nullptr, it);
      auto seek_it = prepared->execute({.segment = segment});
      EXPECT_NE(nullptr, seek_it);
      auto* cost = irs::get<irs::CostAttr>(*it);
      EXPECT_NE(nullptr, cost);

      if (irs::doc_limits::eof(it->value())) {
        continue;
      }

      const auto score = it->PrepareScore({
        .scorer = &ord,
        .segment = &segment,
      });
      EXPECT_FALSE(score.IsDefault());

      EXPECT_FALSE(irs::doc_limits::valid(it->value()));
      cur_it = it.get();
      while (it->next()) {
        const auto doc_id = it->value();
        EXPECT_EQ(doc_id, seek_it->seek(doc_id));
        EXPECT_EQ(doc_id, seek_it->seek(doc_id));
        EXPECT_FALSE(values.IsNull(doc_id));

        irs::score_t score_value;
        score.Score(&score_value, 1);

        actual_results.emplace(
          irs::tests::ReadStoredStr<std::string>(values, doc_id),
          std::move(score_value));
      }
      EXPECT_TRUE(irs::doc_limits::eof(it->value()));
      EXPECT_TRUE(irs::doc_limits::eof(seek_it->seek(it->value())));

      {
        auto it = prepared->execute({.segment = segment, .scorer = &ord});
        EXPECT_NE(nullptr, it);

        while (it->next()) {
          const auto doc_id = it->value();
          auto seek_it = prepared->execute({.segment = segment});
          EXPECT_NE(nullptr, seek_it);
          EXPECT_EQ(doc_id, seek_it->seek(doc_id));
          do {
            if (!values.IsNull(seek_it->value())) {
              EXPECT_NE(
                actual_results.end(),
                actual_results.find(irs::tests::ReadStoredStr<std::string>(
                  values, seek_it->value())));
            }
          } while (seek_it->next());
          EXPECT_TRUE(irs::doc_limits::eof(seek_it->value()));
        }
        EXPECT_TRUE(irs::doc_limits::eof(it->value()));
      }
    }

    return actual_results;
  };

  {
    auto json = tests::FromJson(R"({
      "type": "Polygon",
      "coordinates": [
          [
              [37.602682, 55.706853],
              [37.613025, 55.706853],
              [37.613025, 55.711906],
              [37.602682, 55.711906],
              [37.602682, 55.706853]
          ]
      ]
    })");

    GeoFilter q;
    q.mutable_options()->type = GeoFilterType::Intersects;
    ASSERT_TRUE(
      json::ParseRegion(json.value(), q.mutable_options()->shape).ok());
    ASSERT_EQ(ShapeContainer::Type::S2Polygon,
              q.mutable_options()->shape.type());
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->store_field_id = kGeo;

    size_t collector_finish_count = 0;
    uint64_t collector_field_docs = 0;
    size_t scorer_score_count = 0;
    size_t prepare_scorer_count = 0;

    ::CustomSort sort;

    sort.collector_finish = [&](irs::byte_type*,
                                const irs::FieldCollector* field,
                                const irs::TermCollector* term) -> void {
      ++collector_finish_count;
      // geo filter exercises field collector but not term collector
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(nullptr, term);
      collector_field_docs += field->docs_with_field;
    };
    sort._prepare_scorer = [&](const irs::ScoreContext& ctx) {
      EXPECT_EQ(q.Boost(), ctx.boost);
      ++prepare_scorer_count;
    };

    sort.scorer_score = [&](const irs::ScoreOperator* ctx, irs::score_t* res,
                            size_t n) -> void {
      ASSERT_TRUE(res);
      ASSERT_TRUE(cur_it);
      ASSERT_EQ(1, n);
      *res = cur_it->value();
      ++scorer_score_count;
    };

    const std::map<std::string, score_t> expected{{"Q", 9}, {"R", 9}};

    ASSERT_EQ(expected, execute_query(q, sort));
    ASSERT_EQ(1, collector_finish_count);
    ASSERT_GT(collector_field_docs, 0u);  // field collector ran on segments
    ASSERT_EQ(2, scorer_score_count);
  }

  {
    auto json = tests::FromJson(R"({
      "type": "Polygon",
      "coordinates": [
          [
              [37.602682, 55.706853],
              [37.613025, 55.706853],
              [37.613025, 55.711906],
              [37.602682, 55.711906],
              [37.602682, 55.706853]
          ]
      ]
    })");

    GeoFilter q;
    q.boost(1.5f);
    q.mutable_options()->type = GeoFilterType::Intersects;
    ASSERT_TRUE(
      json::ParseRegion(json.value(), q.mutable_options()->shape).ok());
    ASSERT_EQ(ShapeContainer::Type::S2Polygon,
              q.mutable_options()->shape.type());
    *q.mutable_field_id() = kGeo;
    q.mutable_options()->store_field_id = kGeo;

    size_t collector_finish_count = 0;
    uint64_t collector_field_docs = 0;
    size_t scorer_score_count = 0;
    size_t prepare_scorer_count = 0;

    ::CustomSort sort;

    sort.collector_finish = [&](irs::byte_type*,
                                const irs::FieldCollector* field,
                                const irs::TermCollector* term) -> void {
      ++collector_finish_count;
      // geo filter exercises field collector but not term collector
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(nullptr, term);
      collector_field_docs += field->docs_with_field;
    };
    sort._prepare_scorer = [&](const irs::ScoreContext& ctx) {
      EXPECT_EQ(q.Boost(), ctx.boost);
      ++prepare_scorer_count;
    };

    sort.scorer_score = [&](const irs::ScoreOperator* ctx, irs::score_t* res,
                            size_t n) -> void {
      ASSERT_TRUE(res != nullptr);
      ASSERT_TRUE(cur_it);
      ASSERT_EQ(1, n);
      *res = cur_it->value();
      ++scorer_score_count;
    };

    const std::map<std::string, irs::score_t> expected{{"Q", 9}, {"R", 9}};

    ASSERT_EQ(expected, execute_query(q, sort));
    ASSERT_EQ(1, collector_finish_count);
    ASSERT_GT(collector_field_docs, 0u);  // field collector ran on segments
    ASSERT_EQ(2, scorer_score_count);
  }
}
