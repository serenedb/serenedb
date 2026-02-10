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

#include <vpack/iterator.h>
#include <vpack/parser.h>

#include <set>

#include "geo/geo_json.h"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/geo_filter.h"
#include "iresearch/search/score.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "s2/s2point_region.h"
#include "s2/s2polygon.h"
#include "search_fields.h"
#include "tests_shared.hpp"

namespace {

using namespace sdb::geo;
using namespace irs;
using namespace irs::tests;

struct CustomSort final : public irs::ScorerBase<void> {
  static constexpr std::string_view type_name() noexcept {
    return "custom_sort";
  }

  class FieldCollector final : public irs::FieldCollector {
   public:
    FieldCollector(const CustomSort& sort) : _sort(sort) {}

    void collect(const irs::SubReader& segment,
                 const irs::TermReader& field) final {
      if (_sort.field_collector_collect) {
        _sort.field_collector_collect(segment, field);
      }
    }

    void collect(irs::bytes_view) final {}

    void reset() final {}

    void write(irs::DataOutput&) const final {}

   private:
    const CustomSort& _sort;
  };

  class TermCollector final : public irs::TermCollector {
   public:
    TermCollector(const CustomSort& sort) : _sort(sort) {}

    virtual void collect(const irs::SubReader& segment,
                         const irs::TermReader& field,
                         const irs::AttributeProvider& term_attrs) final {
      if (_sort.term_collector_collect) {
        _sort.term_collector_collect(segment, field, term_attrs);
      }
    }

    void collect(irs::bytes_view) final {}

    void reset() final {}

    void write(irs::DataOutput&) const final {}

   private:
    const CustomSort& _sort;
  };

  struct Scorer final : public irs::ScoreCtx {
    Scorer(const CustomSort& sort, const irs::ScoreContext& ctx)
      : document_attrs(ctx.doc_attrs),
        stats(ctx.stats),
        segment_reader(ctx.segment),
        sort(sort) {}

    const irs::AttributeProvider& document_attrs;
    const irs::byte_type* stats;
    const irs::ColumnProvider& segment_reader;
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

  irs::FieldCollector::ptr PrepareFieldCollector() const final {
    if (_prepare_field_collector) {
      return _prepare_field_collector();
    }

    return std::make_unique<CustomSort::FieldCollector>(*this);
  }

  irs::ScoreFunction PrepareScorer(const irs::ScoreContext& ctx) const final {
    if (_prepare_scorer) {
      _prepare_scorer(ctx);
    }

    return irs::ScoreFunction::Make<CustomSort::Scorer>(
      [](irs::ScoreCtx* ctx, irs::score_t* res, size_t n) noexcept {
        auto& ctx_impl = *reinterpret_cast<const CustomSort::Scorer*>(ctx);

        if (ctx_impl.sort.scorer_score) {
          ctx_impl.sort.scorer_score(ctx, res, n);
        }
      },
      irs::ScoreFunction::NoopMin, *this, ctx);
  }

  irs::TermCollector::ptr PrepareTermCollector() const final {
    if (_prepare_term_collector) {
      return _prepare_term_collector();
    }

    return std::make_unique<CustomSort::TermCollector>(*this);
  }

  std::function<void(const irs::SubReader&, const irs::TermReader&)>
    field_collector_collect;
  std::function<void(const irs::SubReader&, const irs::TermReader&,
                     const irs::AttributeProvider&)>
    term_collector_collect;
  std::function<void(irs::byte_type*, const irs::FieldCollector*,
                     const irs::TermCollector*)>
    collector_finish;
  std::function<irs::FieldCollector::ptr()> _prepare_field_collector;  // NOLINT
  std::function<void(const irs::ScoreContext& ctx)> _prepare_scorer;   // NOLINT
  std::function<irs::TermCollector::ptr()> _prepare_term_collector;    // NOLINT
  std::function<void(irs::ScoreCtx*, irs::score_t*, size_t n)> scorer_score;

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
  ASSERT_EQ("", q.field());
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
  *q.mutable_field() = "field";

  {
    GeoFilter q1;
    q1.mutable_options()->type = GeoFilterType::Intersects;
    q1.mutable_options()->shape.reset(
      std::make_unique<S2PointRegion>(S2Point{1., 0., 0.}),
      ShapeContainer::Type::S2Point);
    *q1.mutable_field() = "field";
    ASSERT_EQ(q, q1);
  }

  {
    GeoFilter q1;
    q1.boost(1.5);
    q1.mutable_options()->type = GeoFilterType::Intersects;
    q1.mutable_options()->shape.reset(
      std::make_unique<S2PointRegion>(S2Point{1., 0., 0.}),
      ShapeContainer::Type::S2Point);
    *q1.mutable_field() = "field";
    ASSERT_EQ(q, q1);
  }

  {
    GeoFilter q1;
    q1.mutable_options()->type = GeoFilterType::Intersects;
    q1.mutable_options()->shape.reset(
      std::make_unique<S2PointRegion>(S2Point{1., 0., 0.}),
      ShapeContainer::Type::S2Point);
    *q1.mutable_field() = "field1";
    ASSERT_NE(q, q1);
  }

  {
    GeoFilter q1;
    q1.mutable_options()->type = GeoFilterType::Contains;
    q1.mutable_options()->shape.reset(
      std::make_unique<S2PointRegion>(S2Point{1., 0., 0.}),
      ShapeContainer::Type::S2Point);
    *q1.mutable_field() = "field";
    ASSERT_NE(q, q1);
  }

  {
    GeoFilter q1;
    q1.mutable_options()->type = GeoFilterType::Contains;
    q1.mutable_options()->shape.reset(std::make_unique<S2Polygon>(),
                                      ShapeContainer::Type::S2Polygon);
    *q1.mutable_field() = "field";
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
    *q.mutable_field() = "field";

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
    *q.mutable_field() = "field";
    q.boost(boost);

    auto prepared = q.prepare({.index = irs::SubReader::empty()});
    ASSERT_EQ(boost, prepared->Boost());
  }
}

TEST(GeoFilterTest, query) {
  auto docs = vpack::Parser::fromJson(R"([
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
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);
    GeoField geo_field;
    geo_field.field_name = "geometry";
    StringField name_field;
    name_field.field_name = "name";
    {
      auto segment0 = writer->GetBatch();
      auto segment1 = writer->GetBatch();
      {
        size_t i = 0;
        for (auto doc_slice : vpack::ArrayIterator(docs->slice())) {
          geo_field.shape_slice = doc_slice.get("geometry");
          name_field.value = slice_to_string_view(doc_slice.get("name"));

          auto doc = (i++ % 2 ? segment0 : segment1).Insert();
          ASSERT_TRUE(
            doc.Insert<(irs::Action::INDEX | irs::Action::STORE)>(name_field));
          ASSERT_TRUE(
            doc.Insert<(irs::Action::INDEX | irs::Action::STORE)>(geo_field));
        }
      }
    }
    writer->Commit();
    reader = writer->GetSnapshot();
  }

  ASSERT_NE(nullptr, reader);
  ASSERT_EQ(2U, reader->size());
  ASSERT_EQ(docs->slice().length(), reader->docs_count());
  ASSERT_EQ(docs->slice().length(), reader->live_docs_count());

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
      auto column = segment.column("name");
      EXPECT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      EXPECT_NE(nullptr, values);
      auto* value = irs::get<irs::PayAttr>(*values);
      EXPECT_NE(nullptr, value);
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

      auto* doc = irs::get<irs::DocAttr>(*it);
      EXPECT_NE(nullptr, doc);
      EXPECT_FALSE(irs::doc_limits::valid(doc->value));
      EXPECT_FALSE(irs::doc_limits::valid(it->value()));
      while (it->next()) {
        auto doc_id = it->value();
        EXPECT_EQ(doc_id, seek_it->seek(doc_id));
        EXPECT_EQ(doc_id, seek_it->seek(doc_id));
        EXPECT_EQ(doc_id, doc->value);
        EXPECT_EQ(doc_id, values->seek(doc_id));
        EXPECT_FALSE(irs::IsNull(value->value));

        actual_results.emplace(irs::ToString<std::string>(value->value.data()));
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
          auto column_it = column->iterator(irs::ColumnHint::Normal);
          EXPECT_NE(nullptr, column_it);
          auto* payload = irs::get<irs::PayAttr>(*column_it);
          EXPECT_NE(nullptr, payload);
          EXPECT_EQ(doc_id, seek_it->seek(doc_id));
          do {
            EXPECT_EQ(seek_it->value(), column_it->seek(seek_it->value()));
            if (!irs::doc_limits::eof(column_it->value())) {
              EXPECT_NE(actual_results.end(),
                        actual_results.find(
                          irs::ToString<std::string>(payload->value.data())));
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

    auto json = vpack::Parser::fromJson(R"({
      "type": "Point",
      "coordinates": [ 37.610235, 55.709754 ]
    })");

    GeoFilter q;
    q.mutable_options()->type = GeoFilterType::Intersects;
    ASSERT_TRUE(
      json::ParseRegion(json->slice(), q.mutable_options()->shape).ok());
    ASSERT_EQ(ShapeContainer::Type::S2Point, q.mutable_options()->shape.type());
    *q.mutable_field() = "geometry";

    ASSERT_EQ(expected, execute_query(q, {2, 0}));
  }

  {
    const std::set<std::string> expected{"Q", "R"};

    auto json = vpack::Parser::fromJson(R"({
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
      json::ParseRegion(json->slice(), q.mutable_options()->shape).ok());
    ASSERT_EQ(ShapeContainer::Type::S2Polygon,
              q.mutable_options()->shape.type());
    *q.mutable_field() = "geometry";

    ASSERT_EQ(expected, execute_query(q, {2, 2}));
  }

  {
    const auto origin = docs->slice().at(7);
    std::set<std::string> expected{origin.get("name").copyString()};

    GeoFilter q;
    *q.mutable_field() = "geometry";
    std::vector<S2LatLng> cache;
    ASSERT_TRUE(ParseShape<Parsing::OnlyPoint>(
      origin.get("geometry"), q.mutable_options()->shape, cache,
      coding::Options::Invalid, nullptr));
    q.mutable_options()->type = GeoFilterType::Intersects;
    q.mutable_options()->options.set_index_contains_points_only(true);

    ASSERT_EQ(expected, execute_query(q, {2, 4}));
  }

  {
    const auto origin = docs->slice().at(7);
    std::set<std::string> expected{origin.get("name").copyString()};

    GeoFilter q;
    *q.mutable_field() = "geometry";
    std::vector<S2LatLng> cache;
    ASSERT_TRUE(ParseShape<Parsing::OnlyPoint>(
      origin.get("geometry"), q.mutable_options()->shape, cache,
      coding::Options::Invalid, nullptr));
    q.mutable_options()->type = GeoFilterType::Contains;
    q.mutable_options()->options.set_index_contains_points_only(true);

    ASSERT_EQ(expected, execute_query(q, {2, 4}));
  }

  {
    const auto origin = docs->slice().at(7);
    std::set<std::string> expected{origin.get("name").copyString()};

    GeoFilter q;
    *q.mutable_field() = "geometry";
    std::vector<S2LatLng> cache;
    ASSERT_TRUE(ParseShape<Parsing::OnlyPoint>(
      origin.get("geometry"), q.mutable_options()->shape, cache,
      coding::Options::Invalid, nullptr));
    q.mutable_options()->type = GeoFilterType::IsContained;
    q.mutable_options()->options.set_index_contains_points_only(true);

    ASSERT_EQ(expected, execute_query(q, {2, 4}));
  }

  {
    auto shape_json = vpack::Parser::fromJson(R"({
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
      shape_json->slice(), shape, cache, coding::Options::Invalid, nullptr));
    std::set<std::string> expected;
    for (auto doc : vpack::ArrayIterator(docs->slice())) {
      auto geo = doc.get("geometry");
      ASSERT_TRUE(geo.isObject());
      ASSERT_TRUE(ParseShape<Parsing::OnlyPoint>(
        geo, point, cache, coding::Options::Invalid, nullptr));
      if (!shape.contains(point)) {
        continue;
      }

      auto name = doc.get("name");
      ASSERT_TRUE(name.isString());
      expected.emplace(slice_to_string(name));
    }

    GeoFilter q;
    *q.mutable_field() = "geometry";
    ASSERT_TRUE(ParseShape<Parsing::GeoJson>(
      shape_json->slice(), q.mutable_options()->shape, cache,
      coding::Options::Invalid, nullptr));
    q.mutable_options()->type = GeoFilterType::Contains;
    q.mutable_options()->options.set_index_contains_points_only(true);

    EXPECT_EQ(expected, execute_query(q, {18, 18}));
  }

  {
    auto shape_json = vpack::Parser::fromJson(R"({
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
      shape_json->slice(), shape, cache, coding::Options::Invalid, nullptr));
    std::set<std::string> expected;
    for (auto doc : vpack::ArrayIterator(docs->slice())) {
      auto geo = doc.get("geometry");
      ASSERT_TRUE(geo.isObject());
      ASSERT_TRUE(ParseShape<Parsing::OnlyPoint>(
        geo, point, cache, coding::Options::Invalid, nullptr));
      if (!shape.contains(point)) {
        continue;
      }

      auto name = doc.get("name");
      ASSERT_TRUE(name.isString());
      expected.emplace(slice_to_string_view(name));
    }

    GeoFilter q;
    *q.mutable_field() = "geometry";
    ASSERT_TRUE(ParseShape<Parsing::GeoJson>(
      shape_json->slice(), q.mutable_options()->shape, cache,
      coding::Options::Invalid, nullptr));
    q.mutable_options()->type = GeoFilterType::Intersects;

    EXPECT_EQ(expected, execute_query(q, {18, 18}));
  }

  {
    auto shape_json = vpack::Parser::fromJson(R"({
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
    *q.mutable_field() = "geometry";
    ASSERT_TRUE(ParseShape<Parsing::GeoJson>(
      shape_json->slice(), q.mutable_options()->shape, cache,
      coding::Options::Invalid, nullptr));
    q.mutable_options()->type = GeoFilterType::IsContained;

    EXPECT_EQ(expected, execute_query(q, {18, 18}));
  }
}

TEST(GeoFilterTest, checkScorer) {
  auto docs = vpack::Parser::fromJson(R"([
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
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);
    GeoField geo_field;
    geo_field.field_name = "geometry";
    StringField name_field;
    name_field.field_name = "name";
    {
      auto segment0 = writer->GetBatch();
      auto segment1 = writer->GetBatch();
      {
        size_t i = 0;
        for (auto doc_slice : vpack::ArrayIterator(docs->slice())) {
          geo_field.shape_slice = doc_slice.get("geometry");
          name_field.value = slice_to_string_view(doc_slice.get("name"));

          auto doc = (i++ % 2 ? segment0 : segment1).Insert();
          ASSERT_TRUE(
            doc.Insert<(irs::Action::INDEX | irs::Action::STORE)>(name_field));
          ASSERT_TRUE(
            doc.Insert<(irs::Action::INDEX | irs::Action::STORE)>(geo_field));
        }
      }
    }
    writer->Commit();
    reader = writer->GetSnapshot();
  }

  ASSERT_NE(nullptr, reader);
  ASSERT_EQ(2, reader->size());
  ASSERT_EQ(docs->slice().length(), reader->docs_count());
  ASSERT_EQ(docs->slice().length(), reader->live_docs_count());

  irs::DocIterator* cur_it = nullptr;
  auto execute_query = [&](const irs::Filter& q, const irs::Scorers& ord) {
    std::map<std::string, score_t> actual_results;

    auto prepared = q.prepare({.index = *reader, .scorers = ord});
    EXPECT_NE(nullptr, prepared);
    for (auto& segment : *reader) {
      auto column = segment.column("name");
      EXPECT_NE(nullptr, column);
      auto column_it = column->iterator(irs::ColumnHint::Normal);
      EXPECT_NE(nullptr, column_it);
      auto* payload = irs::get<irs::PayAttr>(*column_it);
      EXPECT_NE(nullptr, payload);
      auto it = prepared->execute({.segment = segment, .scorers = ord});
      EXPECT_NE(nullptr, it);
      auto seek_it = prepared->execute({.segment = segment});
      EXPECT_NE(nullptr, seek_it);
      auto* cost = irs::get<irs::CostAttr>(*it);
      EXPECT_NE(nullptr, cost);

      if (irs::doc_limits::eof(it->value())) {
        continue;
      }

      const auto score = it->PrepareScore({
        .scorer = ord.buckets().front().bucket,
        .segment = &segment,
      });
      EXPECT_FALSE(score.IsDefault());

      auto* doc = irs::get<irs::DocAttr>(*it);
      EXPECT_NE(nullptr, doc);
      EXPECT_FALSE(irs::doc_limits::valid(doc->value));
      EXPECT_FALSE(irs::doc_limits::valid(it->value()));
      cur_it = it.get();
      while (it->next()) {
        const auto doc_id = it->value();
        EXPECT_EQ(doc_id, seek_it->seek(doc_id));
        EXPECT_EQ(doc_id, seek_it->seek(doc_id));
        EXPECT_EQ(doc_id, column_it->seek(doc_id));
        EXPECT_EQ(doc_id, doc->value);
        EXPECT_FALSE(irs::IsNull(payload->value));

        irs::score_t score_value;
        score.Score(&score_value, 1);

        actual_results.emplace(
          irs::ToString<std::string>(payload->value.data()),
          std::move(score_value));
      }
      EXPECT_TRUE(irs::doc_limits::eof(it->value()));
      EXPECT_TRUE(irs::doc_limits::eof(seek_it->seek(it->value())));

      {
        auto it = prepared->execute({.segment = segment, .scorers = ord});
        EXPECT_NE(nullptr, it);

        while (it->next()) {
          const auto doc_id = it->value();
          auto seek_it = prepared->execute({.segment = segment});
          EXPECT_NE(nullptr, seek_it);
          auto column_it = column->iterator(irs::ColumnHint::Normal);
          EXPECT_NE(nullptr, column_it);
          auto* payload = irs::get<irs::PayAttr>(*column_it);
          EXPECT_NE(nullptr, payload);
          EXPECT_EQ(doc_id, seek_it->seek(doc_id));
          do {
            EXPECT_EQ(seek_it->value(), column_it->seek(seek_it->value()));
            if (!irs::doc_limits::eof(column_it->value())) {
              EXPECT_NE(actual_results.end(),
                        actual_results.find(
                          irs::ToString<std::string>(payload->value.data())));
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
    auto json = vpack::Parser::fromJson(R"({
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
      json::ParseRegion(json->slice(), q.mutable_options()->shape).ok());
    ASSERT_EQ(ShapeContainer::Type::S2Polygon,
              q.mutable_options()->shape.type());
    *q.mutable_field() = "geometry";

    size_t collector_collect_field_count = 0;
    size_t collector_collect_term_count = 0;
    size_t collector_finish_count = 0;
    size_t scorer_score_count = 0;
    size_t prepare_scorer_count = 0;

    ::CustomSort sort;

    sort.field_collector_collect = [&collector_collect_field_count, &q](
                                     const irs::SubReader&,
                                     const irs::TermReader& field) -> void {
      collector_collect_field_count += (q.field() == field.meta().name);
    };
    sort.term_collector_collect = [&collector_collect_term_count, &q](
                                    const irs::SubReader&,
                                    const irs::TermReader& field,
                                    const irs::AttributeProvider&) -> void {
      collector_collect_term_count += (q.field() == field.meta().name);
    };
    sort.collector_finish = [&collector_finish_count](
                              irs::byte_type*, const irs::FieldCollector*,
                              const irs::TermCollector*) -> void {
      ++collector_finish_count;
    };
    sort._prepare_scorer = [&](const irs::ScoreContext& ctx) {
      EXPECT_EQ(q.Boost(), ctx.boost);
      ++prepare_scorer_count;
    };

    sort.scorer_score = [&](irs::ScoreCtx* ctx, irs::score_t* res,
                            size_t n) -> void {
      ASSERT_TRUE(res);
      ASSERT_TRUE(cur_it);
      ASSERT_EQ(1, n);
      *res = cur_it->value();
      ++scorer_score_count;
    };

    const std::map<std::string, score_t> expected{{"Q", 9}, {"R", 9}};

    ASSERT_EQ(expected, execute_query(q, irs::Scorers::Prepare(sort)));
    ASSERT_EQ(2, collector_collect_field_count);  // 2 segments
    ASSERT_EQ(0, collector_collect_term_count);
    ASSERT_EQ(1, collector_finish_count);
    ASSERT_EQ(2, scorer_score_count);
  }

  {
    auto json = vpack::Parser::fromJson(R"({
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
      json::ParseRegion(json->slice(), q.mutable_options()->shape).ok());
    ASSERT_EQ(ShapeContainer::Type::S2Polygon,
              q.mutable_options()->shape.type());
    *q.mutable_field() = "geometry";

    size_t collector_collect_field_count = 0;
    size_t collector_collect_term_count = 0;
    size_t collector_finish_count = 0;
    size_t scorer_score_count = 0;
    size_t prepare_scorer_count = 0;

    ::CustomSort sort;

    sort.field_collector_collect = [&collector_collect_field_count, &q](
                                     const irs::SubReader&,
                                     const irs::TermReader& field) -> void {
      collector_collect_field_count += (q.field() == field.meta().name);
    };
    sort.term_collector_collect = [&collector_collect_term_count, &q](
                                    const irs::SubReader&,
                                    const irs::TermReader& field,
                                    const irs::AttributeProvider&) -> void {
      collector_collect_term_count += (q.field() == field.meta().name);
    };
    sort.collector_finish = [&collector_finish_count](
                              irs::byte_type*, const irs::FieldCollector*,
                              const irs::TermCollector*) -> void {
      ++collector_finish_count;
    };
    sort._prepare_scorer = [&](const irs::ScoreContext& ctx) {
      EXPECT_EQ(q.Boost(), ctx.boost);
      ++prepare_scorer_count;
    };

    sort.scorer_score = [&](irs::ScoreCtx* ctx, irs::score_t* res,
                            size_t n) -> void {
      ASSERT_TRUE(res != nullptr);
      ASSERT_TRUE(cur_it);
      ASSERT_EQ(1, n);
      *res = cur_it->value();
      ++scorer_score_count;
    };

    const std::map<std::string, irs::score_t> expected{{"Q", 9}, {"R", 9}};

    ASSERT_EQ(expected, execute_query(q, irs::Scorers::Prepare(sort)));
    ASSERT_EQ(2, collector_collect_field_count);  // 2 segments
    ASSERT_EQ(0, collector_collect_term_count);
    ASSERT_EQ(1, collector_finish_count);
    ASSERT_EQ(2, scorer_score_count);
  }
}
