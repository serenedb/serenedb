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

#include "tests_shared.hpp"
#include <s2/s2point_region.h>
#include <vpack/parser.h>

#include "basics/down_cast.h"
#include "iresearch/analysis/geo_analyzer.hpp"
#include "iresearch/utils/vpack_utils.hpp"
#include "geo/geo_json.h"
#include "iresearch/search/geo_filter.h"

using namespace irs;
using namespace analysis;
using namespace sdb::geo;


TEST(GeoOptionsTest, constants) {
  static_assert(20 == GeoOptions::kDefaultMaxCells);
  static_assert(4 == GeoOptions::kDefaultMinLevel);
  static_assert(23 == GeoOptions::kDefaultMaxLevel);
}

TEST(GeoOptionsTest, options) {
  GeoOptions opts;
  ASSERT_EQ(GeoOptions::kDefaultMaxCells, opts.max_cells);
  ASSERT_EQ(GeoOptions::kDefaultMinLevel, opts.min_level);
  ASSERT_EQ(GeoOptions::kDefaultMaxLevel, opts.max_level);
}

TEST(GeoBench, sizes) {
  GTEST_SKIP() << "It's just for check sizes, not comment out to allow compile";
  
  auto vpack_analyzer = GeoJsonAnalyzer::make(slice_to_view<char>(vpack::Slice::emptyObjectSlice()));
  GeoJsonAnalyzer::Options opts;
  opts.coding = GeoJsonAnalyzer::Coding::S2LatLngU32;
  vpack::Builder opts_builder;
  ToVPack(opts_builder, opts);
  auto s2_analyzer = GeoJsonAnalyzer::make(irs::slice_to_view<char>(opts_builder.slice()));

  auto builder = vpack::Parser::fromJson(R"=([ 6.537, 50.332 ])=");
  vpack_analyzer->reset(irs::slice_to_view<char>(builder->slice()));
  s2_analyzer->reset(irs::slice_to_view<char>(builder->slice()));
  std::cerr << builder->toString() << std::endl;
  std::cerr << GeoJsonAnalyzer::store(vpack_analyzer.get(), builder->slice()).size()
            << std::endl;
  std::cerr << GeoJsonAnalyzer::store(s2_analyzer.get(), builder->slice()).size()
            << std::endl;

  builder = vpack::Parser::fromJson(R"=(
      { "type": "Point",
      "coordinates": [ 6.537, 50.332 ]
      })=");
  std::cerr << builder->toString() << std::endl;
  std::cerr << GeoJsonAnalyzer::store(vpack_analyzer.get(), builder->slice()).size()
            << std::endl;
  std::cerr << GeoJsonAnalyzer::store(s2_analyzer.get(), builder->slice()).size()
            << std::endl;

  builder = vpack::Parser::fromJson(R"=(
      { "type": "MultiPoint",
        "coordinates": [ [ 6.537, 50.332 ], [ 6.537, 50.376 ] ]
      })=");
  std::cerr << builder->toString() << std::endl;
  std::cerr << GeoJsonAnalyzer::store(vpack_analyzer.get(), builder->slice()).size()
            << std::endl;
  std::cerr << GeoJsonAnalyzer::store(s2_analyzer.get(), builder->slice()).size()
            << std::endl;

  builder = vpack::Parser::fromJson(R"=(
      { "type": "MultiPoint",
        "coordinates": [ [ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ], [ 6.537, 50.376 ] ]
      })=");
  std::cerr << builder->toString() << std::endl;
  std::cerr << GeoJsonAnalyzer::store(vpack_analyzer.get(), builder->slice()).size()
            << std::endl;
  std::cerr << GeoJsonAnalyzer::store(s2_analyzer.get(), builder->slice()).size()
            << std::endl;

  builder = vpack::Parser::fromJson(R"=(
      { "type": "LineString",
        "coordinates": [ [ 6.537, 50.332 ], [ 6.537, 50.376 ] ]
      })=");
  std::cerr << builder->toString() << std::endl;
  std::cerr << GeoJsonAnalyzer::store(vpack_analyzer.get(), builder->slice()).size()
            << std::endl;
  std::cerr << GeoJsonAnalyzer::store(s2_analyzer.get(), builder->slice()).size()
            << std::endl;

  builder = vpack::Parser::fromJson(R"=(
      { "type": "MultiLineString",
        "coordinates": [ [ [ 6.537, 50.332 ], [ 6.537, 50.376 ] ],
                         [ [ 6.621, 50.332 ], [ 6.621, 50.376 ] ] ]
      })=");
  std::cerr << builder->toString() << std::endl;
  std::cerr << GeoJsonAnalyzer::store(vpack_analyzer.get(), builder->slice()).size()
            << std::endl;
  std::cerr << GeoJsonAnalyzer::store(s2_analyzer.get(), builder->slice()).size()
            << std::endl;

  builder = vpack::Parser::fromJson(R"=(
      { "type": "Polygon",
        "coordinates": [ [ [6.1,50.1], [7.5,50.1], [7.5,52.1], [6.1,51.1], [6.1,50.1] ] ]
      })=");
  std::cerr << builder->toString() << std::endl;
  std::cerr << GeoJsonAnalyzer::store(vpack_analyzer.get(), builder->slice()).size()
            << std::endl;
  std::cerr << GeoJsonAnalyzer::store(s2_analyzer.get(), builder->slice()).size()
            << std::endl;

  builder = vpack::Parser::fromJson(R"=(
      { "type": "MultiPolygon",
        "coordinates": [ [ [ [6.501,50.1], [7.5,50.1], [7.5,51.1],
                             [6.501,51.1], [6.501,50.1] ] ],
                         [ [ [6.1,50.1], [6.5,50.1], [6.5,51.1], [6.1,51.1], [6.1,50.1] ] ] ]
      })=");
  std::cerr << builder->toString() << std::endl;
  std::cerr << GeoJsonAnalyzer::store(vpack_analyzer.get(), builder->slice()).size()
            << std::endl;
  std::cerr << GeoJsonAnalyzer::store(s2_analyzer.get(), builder->slice()).size()
            << std::endl;

  builder = vpack::Parser::fromJson(R"=(
      { "type": "Polygon",
        "coordinates": [ [ [6.1,50.1], [7.5,50.1], [7.5,51.1], [6.1,51.1], [6.1,50.1] ] ]
      })=");
  std::cerr << builder->toString() << std::endl;
  std::cerr << GeoJsonAnalyzer::store(vpack_analyzer.get(), builder->slice()).size()
            << std::endl;
  std::cerr << GeoJsonAnalyzer::store(s2_analyzer.get(), builder->slice()).size()
            << std::endl;

  builder = vpack::Parser::fromJson(R"=(
      { "type": "LineString",
        "coordinates": [ [ 5.437, 50.332 ], [ 7.537, 50.376 ] ]
      })=");
  std::cerr << builder->toString() << std::endl;
  std::cerr << GeoJsonAnalyzer::store(vpack_analyzer.get(), builder->slice()).size()
            << std::endl;
  std::cerr << GeoJsonAnalyzer::store(s2_analyzer.get(), builder->slice()).size()
            << std::endl;

  builder = vpack::Parser::fromJson(R"=(
      { "type": "Polygon",
        "coordinates": [ [ [1,1], [4,1], [4,4], [1,4], [1,1] ] ]
      })=");
  std::cerr << builder->toString() << std::endl;
  std::cerr << GeoJsonAnalyzer::store(vpack_analyzer.get(), builder->slice()).size()
            << std::endl;
  std::cerr << GeoJsonAnalyzer::store(s2_analyzer.get(), builder->slice()).size()
            << std::endl;

  builder = vpack::Parser::fromJson(R"=(
      { "type": "Polygon",
        "coordinates": [ [ [1.1,1.1], [4.1,1.1], [4.1,4.1], [1.1,4.1], [1.1,1.1] ] ]
      })=");
  std::cerr << builder->toString() << std::endl;
  std::cerr << GeoJsonAnalyzer::store(vpack_analyzer.get(), builder->slice()).size()
            << std::endl;
  std::cerr << GeoJsonAnalyzer::store(s2_analyzer.get(), builder->slice()).size()
            << std::endl;

  builder = vpack::Parser::fromJson(
    R"=({"type": "Polygon","coordinates": [[[100.318391,13.535502],[100.318391,14.214848],[101.407575,14.214848],[101.407575,13.535502],[100.318391,13.535502]]]})=");
  std::cerr << builder->toString() << std::endl;
  std::cerr << GeoJsonAnalyzer::store(vpack_analyzer.get(), builder->slice()).size()
            << std::endl;
  std::cerr << GeoJsonAnalyzer::store(s2_analyzer.get(), builder->slice()).size()
            << std::endl;
}

TEST(GeoPointAnalyzerTest, constants) {
  static_assert("geopoint" == GeoPointAnalyzer::type_name());
}

TEST(GeoPointAnalyzerTest, options) {
  GeoPointAnalyzer::Options opts;
  ASSERT_TRUE(opts.latitude.empty());
  ASSERT_TRUE(opts.longitude.empty());
  ASSERT_EQ(GeoOptions{}.max_cells, opts.options.max_cells);
  ASSERT_EQ(GeoOptions{}.min_level, opts.options.min_level);
  ASSERT_EQ(GeoOptions{}.max_level, opts.options.max_level);
}

TEST(GeoPointAnalyzerTest, prepareQuery) {
  {
    GeoPointAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 2;
    opts.options.max_level = 22;
    opts.latitude = {"foo"};
    opts.longitude = {"bar"};
    GeoPointAnalyzer a(opts);

    GeoFilterOptionsBase options;
    a.prepare(options);

    EXPECT_EQ(options.prefix, "");
    EXPECT_EQ(options.stored, StoredType::VPack);
    EXPECT_EQ(1, options.options.level_mod());
    EXPECT_FALSE(options.options.optimize_for_space());
    EXPECT_EQ("$", options.options.marker());
    EXPECT_EQ(opts.options.min_level, options.options.min_level());
    EXPECT_EQ(opts.options.max_level, options.options.max_level());
    EXPECT_EQ(opts.options.max_cells, options.options.max_cells());
    EXPECT_TRUE(options.options.index_contains_points_only());
  }

  {
    GeoPointAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 2;
    opts.options.max_level = 22;
    GeoPointAnalyzer a(opts);

    GeoFilterOptionsBase options;
    a.prepare(options);

    EXPECT_EQ(options.prefix, "");
    EXPECT_EQ(options.stored, StoredType::VPack);
    EXPECT_EQ(1, options.options.level_mod());
    EXPECT_FALSE(options.options.optimize_for_space());
    EXPECT_EQ("$", options.options.marker());
    EXPECT_EQ(opts.options.min_level, options.options.min_level());
    EXPECT_EQ(opts.options.max_level, options.options.max_level());
    EXPECT_EQ(opts.options.max_cells, options.options.max_cells());
    EXPECT_TRUE(options.options.index_contains_points_only());
  }
}

TEST(GeoPointAnalyzerTest, ctor) {
  {
    GeoPointAnalyzer::Options opts;
    GeoPointAnalyzer a(opts);
    ASSERT_TRUE(opts.latitude.empty());
    ASSERT_TRUE(opts.longitude.empty());
    {
      auto* inc = get<IncAttr>(a);
      ASSERT_NE(nullptr, inc);
      ASSERT_EQ(1U, inc->value);
    }
    {
      auto* term = get<TermAttr>(a);
      ASSERT_NE(nullptr, term);
      ASSERT_TRUE(IsNull(term->value));
    }
    ASSERT_EQ(Type<GeoPointAnalyzer>::id(), a.type());
    ASSERT_FALSE(a.next());
  }

  {
    GeoPointAnalyzer::Options opts;
    opts.latitude = {"foo"};
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoPointAnalyzer::make(slice_to_view<char>(builder.slice()));
    EXPECT_TRUE(a == nullptr);
  }

  {
    GeoPointAnalyzer::Options opts;
    opts.longitude = {"foo"};
vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoPointAnalyzer::make(slice_to_view<char>(builder.slice()));
    EXPECT_TRUE(a == nullptr);
  }

  {
    GeoPointAnalyzer::Options opts;
    opts.latitude = {"foo"};
    opts.longitude = {"bar"};
    GeoPointAnalyzer a(opts);
    ASSERT_EQ(std::vector<std::string>{"foo"}, a.latitude());
    ASSERT_EQ(std::vector<std::string>{"bar"}, a.longitude());
    {
      auto* inc = get<IncAttr>(a);
      ASSERT_NE(nullptr, inc);
      ASSERT_EQ(1, inc->value);
    }
    {
      auto* term = get<TermAttr>(a);
      ASSERT_NE(nullptr, term);
      ASSERT_TRUE(IsNull(term->value));
    }
    ASSERT_EQ(Type<GeoPointAnalyzer>::id(), a.type());
    ASSERT_FALSE(a.next());
  }
}

TEST(GeoPointAnalyzerTest, tokenizePointFromArray) {
  auto json =
    vpack::Parser::fromJson(R"([ 63.57789956676574, 53.72314453125 ])");

  ShapeContainer shape;
  ASSERT_TRUE(
    json::ParseCoordinates<true>(json->slice(), shape, false).ok());
  ASSERT_EQ(ShapeContainer::Type::S2Point, shape.type());

  // tokenize point
  {
    GeoPointAnalyzer::Options opts;
    GeoPointAnalyzer a(opts);
  ASSERT_TRUE(a.latitude().empty());
  ASSERT_TRUE(a.longitude().empty());
  ASSERT_EQ(1, a.options().level_mod());
  ASSERT_FALSE(a.options().optimize_for_space());
  ASSERT_EQ("$", a.options().marker());
  ASSERT_EQ(opts.options.min_level, a.options().min_level());
  ASSERT_EQ(opts.options.max_level, a.options().max_level());
  ASSERT_EQ(opts.options.max_cells, a.options().max_cells());
  ASSERT_TRUE(a.options().index_contains_points_only());

    auto* inc = get<IncAttr>(a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(a);
    ASSERT_NE(nullptr, term);
  ASSERT_TRUE(a.reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
  for (; a.next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize point, custom options
  {
    GeoPointAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    GeoPointAnalyzer a(opts);
  EXPECT_TRUE(a.latitude().empty());
  EXPECT_TRUE(a.longitude().empty());
  EXPECT_EQ(1, a.options().level_mod());
  EXPECT_FALSE(a.options().optimize_for_space());
  EXPECT_EQ("$", a.options().marker());
  EXPECT_EQ(opts.options.min_level, a.options().min_level());
  EXPECT_EQ(opts.options.max_level, a.options().max_level());
  EXPECT_EQ(opts.options.max_cells, a.options().max_cells());
  EXPECT_TRUE(a.options().index_contains_points_only());

    auto* inc = get<IncAttr>(a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(a);
    ASSERT_NE(nullptr, term);
  ASSERT_TRUE(a.reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
  for (; a.next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }
}

TEST(GeoPointAnalyzerTest, tokenizePointFromObject) {
  auto json =
    vpack::Parser::fromJson(R"([ 63.57789956676574, 53.72314453125 ])");
  auto json_object = vpack::Parser::fromJson(
    R"({ "lat": 63.57789956676574, "lon": 53.72314453125 })");

  ShapeContainer shape;
  ASSERT_TRUE(
    json::ParseCoordinates<true>(json->slice(), shape, false).ok());
  ASSERT_EQ(ShapeContainer::Type::S2Point, shape.type());

  // tokenize point
  {
    GeoPointAnalyzer::Options opts;
    opts.latitude = {"lat"};
    opts.longitude = {"lon"};
    GeoPointAnalyzer a(opts);
  EXPECT_EQ(std::vector<std::string>{"lat"}, a.latitude());
  EXPECT_EQ(std::vector<std::string>{"lon"}, a.longitude());
  EXPECT_EQ(1, a.options().level_mod());
  EXPECT_FALSE(a.options().optimize_for_space());
  EXPECT_EQ("$", a.options().marker());
  EXPECT_EQ(opts.options.min_level, a.options().min_level());
  EXPECT_EQ(opts.options.max_level, a.options().max_level());
  EXPECT_EQ(opts.options.max_cells, a.options().max_cells());
  EXPECT_TRUE(a.options().index_contains_points_only());

    auto* inc = get<IncAttr>(a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(a);
    ASSERT_NE(nullptr, term);
  ASSERT_TRUE(a.reset(slice_to_view<char>(json_object->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
  for (; a.next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize point, custom options
  {
    GeoPointAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    opts.latitude = {"lat"};
    opts.longitude = {"lon"};
    GeoPointAnalyzer a(opts);
  EXPECT_EQ(std::vector<std::string>{"lat"}, a.latitude());
  EXPECT_EQ(std::vector<std::string>{"lon"}, a.longitude());
  EXPECT_EQ(1, a.options().level_mod());
  EXPECT_FALSE(a.options().optimize_for_space());
  EXPECT_EQ("$", a.options().marker());
  EXPECT_EQ(opts.options.min_level, a.options().min_level());
  EXPECT_EQ(opts.options.max_level, a.options().max_level());
  EXPECT_EQ(opts.options.max_cells, a.options().max_cells());
  EXPECT_TRUE(a.options().index_contains_points_only());

    auto* inc = get<IncAttr>(a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(a);
    ASSERT_NE(nullptr, term);
  ASSERT_TRUE(a.reset(slice_to_view<char>(json_object->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
  for (; a.next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }
}

TEST(GeoPointAnalyzerTest, tokenizePointFromObjectComplexPath) {
  auto json =
    vpack::Parser::fromJson(R"([ 63.57789956676574, 53.72314453125 ])");
  auto json_object = vpack::Parser::fromJson(
    R"({ "subObj": { "lat": 63.57789956676574, "lon": 53.72314453125 } })");

  ShapeContainer shape;
  ASSERT_TRUE(
    json::ParseCoordinates<true>(json->slice(), shape, false).ok());
  ASSERT_EQ(ShapeContainer::Type::S2Point, shape.type());

  // tokenize point
  {
    GeoPointAnalyzer::Options opts;
    opts.latitude = {"subObj", "lat"};
    opts.longitude = {"subObj", "lon"};
    GeoPointAnalyzer a(opts);
  EXPECT_EQ((std::vector<std::string>{"subObj", "lat"}), a.latitude());
  EXPECT_EQ((std::vector<std::string>{"subObj", "lon"}), a.longitude());
  EXPECT_EQ(1, a.options().level_mod());
  EXPECT_FALSE(a.options().optimize_for_space());
  EXPECT_EQ("$", a.options().marker());
  EXPECT_EQ(opts.options.min_level, a.options().min_level());
  EXPECT_EQ(opts.options.max_level, a.options().max_level());
  EXPECT_EQ(opts.options.max_cells, a.options().max_cells());
  EXPECT_TRUE(a.options().index_contains_points_only());

    auto* inc = get<IncAttr>(a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(a);
    ASSERT_NE(nullptr, term);
  ASSERT_TRUE(a.reset(slice_to_view<char>(json_object->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
  for (; a.next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize point, custom options
  {
    GeoPointAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    opts.latitude = {"subObj", "lat"};
    opts.longitude = {"subObj", "lon"};
    GeoPointAnalyzer a(opts);
  EXPECT_EQ((std::vector<std::string>{"subObj", "lat"}), a.latitude());
  EXPECT_EQ((std::vector<std::string>{"subObj", "lon"}), a.longitude());
  EXPECT_EQ(1, a.options().level_mod());
  EXPECT_FALSE(a.options().optimize_for_space());
  EXPECT_EQ("$", a.options().marker());
  EXPECT_EQ(opts.options.min_level, a.options().min_level());
  EXPECT_EQ(opts.options.max_level, a.options().max_level());
  EXPECT_EQ(opts.options.max_cells, a.options().max_cells());
  EXPECT_TRUE(a.options().index_contains_points_only());

    auto* inc = get<IncAttr>(a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a.reset(slice_to_view<char>(json_object->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, true));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a.next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }
}

TEST(GeoPointAnalyzerTest, createFromSlice) {
  {
    auto json = vpack::Parser::fromJson(R"({})");
    auto a = GeoPointAnalyzer::make(slice_to_view<char>(json->slice()));
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoPointAnalyzer&>(*a);

    GeoPointAnalyzer::Options opts;
    EXPECT_TRUE(impl.longitude().empty());
    EXPECT_TRUE(impl.latitude().empty());
    EXPECT_EQ(1, impl.options().level_mod());
    EXPECT_FALSE(impl.options().optimize_for_space());
    EXPECT_EQ("$", impl.options().marker());
    EXPECT_EQ(opts.options.min_level, impl.options().min_level());
    EXPECT_EQ(opts.options.max_level, impl.options().max_level());
    EXPECT_EQ(opts.options.max_cells, impl.options().max_cells());
    EXPECT_TRUE(impl.options().index_contains_points_only());
  }

  {
    auto json = vpack::Parser::fromJson(R"({
      "options" : {
        "maxCells": 1000
      }
    })");
    auto a = GeoPointAnalyzer::make(slice_to_view<char>(json->slice()));
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoPointAnalyzer&>(*a);

    GeoPointAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    EXPECT_TRUE(impl.longitude().empty());
    EXPECT_TRUE(impl.latitude().empty());
    EXPECT_EQ(1, impl.options().level_mod());
    EXPECT_FALSE(impl.options().optimize_for_space());
    EXPECT_EQ("$", impl.options().marker());
    EXPECT_EQ(opts.options.min_level, impl.options().min_level());
    EXPECT_EQ(opts.options.max_level, impl.options().max_level());
    EXPECT_EQ(opts.options.max_cells, impl.options().max_cells());
    EXPECT_TRUE(impl.options().index_contains_points_only());
  }

  {
    auto json = vpack::Parser::fromJson(R"({
      "options" : {
        "maxCells": 1000,
        "minLevel": 2,
        "maxLevel": 22
      }
    })");
    auto a = GeoPointAnalyzer::make(slice_to_view<char>(json->slice()));
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoPointAnalyzer&>(*a);

    GeoPointAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 2;
    opts.options.max_level = 22;
    EXPECT_TRUE(impl.longitude().empty());
    EXPECT_TRUE(impl.latitude().empty());
    EXPECT_EQ(1, impl.options().level_mod());
    EXPECT_FALSE(impl.options().optimize_for_space());
    EXPECT_EQ("$", impl.options().marker());
    EXPECT_EQ(opts.options.min_level, impl.options().min_level());
    EXPECT_EQ(opts.options.max_level, impl.options().max_level());
    EXPECT_EQ(opts.options.max_cells, impl.options().max_cells());
    EXPECT_TRUE(impl.options().index_contains_points_only());
  }

  {
    auto json = vpack::Parser::fromJson(
      R"({ "latitude": ["foo"], "longitude":["bar"] })");
    auto a = GeoPointAnalyzer::make(slice_to_view<char>(json->slice()));
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoPointAnalyzer&>(*a);

    GeoPointAnalyzer::Options opts;
    EXPECT_EQ(std::vector<std::string>{"bar"}, impl.longitude());
    EXPECT_EQ(std::vector<std::string>{"foo"}, impl.latitude());
    EXPECT_EQ(1, impl.options().level_mod());
    EXPECT_FALSE(impl.options().optimize_for_space());
    EXPECT_EQ("$", impl.options().marker());
    EXPECT_EQ(opts.options.min_level, impl.options().min_level());
    EXPECT_EQ(opts.options.max_level, impl.options().max_level());
    EXPECT_EQ(opts.options.max_cells, impl.options().max_cells());
    EXPECT_TRUE(impl.options().index_contains_points_only());
  }

  {
    auto json = vpack::Parser::fromJson(
      R"({ "latitude": ["subObj", "foo"], "longitude":["subObj", "bar"] })");
    auto a = GeoPointAnalyzer::make(slice_to_view<char>(json->slice()));
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoPointAnalyzer&>(*a);

    GeoPointAnalyzer::Options opts;
    EXPECT_EQ((std::vector<std::string>{"subObj", "foo"}), impl.latitude());
    EXPECT_EQ((std::vector<std::string>{"subObj", "bar"}), impl.longitude());
    EXPECT_EQ(1, impl.options().level_mod());
    EXPECT_FALSE(impl.options().optimize_for_space());
    EXPECT_EQ("$", impl.options().marker());
    EXPECT_EQ(opts.options.min_level, impl.options().min_level());
    EXPECT_EQ(opts.options.max_level, impl.options().max_level());
    EXPECT_EQ(opts.options.max_cells, impl.options().max_cells());
    EXPECT_TRUE(impl.options().index_contains_points_only());
  }

  {
    auto json = vpack::Parser::fromJson(
      R"({ "unknownField": "anything", "latitude": ["subObj", "foo"], "longitude":["subObj", "bar"] })");
    auto a = GeoPointAnalyzer::make(slice_to_view<char>(json->slice()));
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoPointAnalyzer&>(*a);

    GeoPointAnalyzer::Options opts;
    EXPECT_EQ((std::vector<std::string>{"subObj", "foo"}), impl.latitude());
    EXPECT_EQ((std::vector<std::string>{"subObj", "bar"}), impl.longitude());
    EXPECT_EQ(1, impl.options().level_mod());
    EXPECT_FALSE(impl.options().optimize_for_space());
    EXPECT_EQ("$", impl.options().marker());
    EXPECT_EQ(opts.options.min_level, impl.options().min_level());
    EXPECT_EQ(opts.options.max_level, impl.options().max_level());
    EXPECT_EQ(opts.options.max_cells, impl.options().max_cells());
    EXPECT_TRUE(impl.options().index_contains_points_only());
  }

  // latitude field is not set
  {
    auto json = vpack::Parser::fromJson(R"({
      "longitude": ["foo"]
    })");
    ASSERT_EQ(nullptr, GeoPointAnalyzer::make(slice_to_view<char>(json->slice())));
  }

  // longitude is not set
  {
    auto json = vpack::Parser::fromJson(R"({
      "latitude": ["foo"]
    })");
    ASSERT_EQ(nullptr, GeoPointAnalyzer::make(slice_to_view<char>(json->slice())));
  }

  // minLevel > maxLevel
  {
    auto json = vpack::Parser::fromJson(R"({
      "options" : {
        "minLevel": 22,
        "maxLevel": 2
      }
    })");
    ASSERT_EQ(nullptr, GeoPointAnalyzer::make(slice_to_view<char>(json->slice())));
  }

  // negative value
  {
    auto json = vpack::Parser::fromJson(R"({
      "options" : {
        "minLevel": -2,
        "maxLevel": 22
      }
    })");
    ASSERT_EQ(nullptr, GeoPointAnalyzer::make(slice_to_view<char>(json->slice())));
  }

  // negative value
  {
    auto json = vpack::Parser::fromJson(R"({
      "options" : {
        "minLevel": -22,
        "maxLevel": -2
      }
    })");
    ASSERT_EQ(nullptr, GeoPointAnalyzer::make(slice_to_view<char>(json->slice())));
  }

  // negative value
  {
    auto json = vpack::Parser::fromJson(R"({
      "options" : {
        "maxCells": -2
      }
    })");
    ASSERT_EQ(nullptr, GeoPointAnalyzer::make(slice_to_view<char>(json->slice())));
  }

  // nan
  {
    auto json = vpack::Parser::fromJson(R"({
      "options" : {
        "maxCells": "2"
      }
    })");
    ASSERT_EQ(nullptr, GeoPointAnalyzer::make(slice_to_view<char>(json->slice())));
  }

  // higher than max GeoOptions::MAX_LEVEL
  {
    auto json = vpack::Parser::fromJson(R"({
      "options" : {
        "maxLevel": 31
      }
    })");
    ASSERT_EQ(nullptr, GeoPointAnalyzer::make(slice_to_view<char>(json->slice())));
  }

  // higher than max GeoOptions::MAX_LEVEL
  {
    auto json = vpack::Parser::fromJson(R"({
      "options" : {
        "minLevel": 31,
        "maxLevel": 31
      }
    })");
    ASSERT_EQ(nullptr, GeoPointAnalyzer::make(slice_to_view<char>(json->slice())));
  }
}

TEST(GeoJsonAnalyzerTest, constants) {
  static_assert("geojson" == GeoJsonAnalyzer::type_name());
}

TEST( GeoJsonAnalyzerVpackTest, options) {
  GeoJsonAnalyzer::Options opts;
  ASSERT_EQ(GeoJsonAnalyzer::Type::Shape, opts.type);
  ASSERT_EQ(GeoOptions{}.max_cells, opts.options.max_cells);
  ASSERT_EQ(GeoOptions{}.min_level, opts.options.min_level);
  ASSERT_EQ(GeoOptions{}.max_level, opts.options.max_level);
}

TEST( GeoJsonAnalyzerVpackTest, ctor) {
  auto a = GeoJsonAnalyzer::make(irs::slice_to_view<char>(vpack::Slice::emptyObjectSlice()));
  {
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    ASSERT_EQ(1, inc->value);
  }
  {
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(IsNull(term->value));
  }
  ASSERT_EQ(Type<GeoJsonAnalyzer>::id(), a->type());
  ASSERT_FALSE(a->next());
}

TEST( GeoJsonAnalyzerVpackTest, tokenizeLatLngRect) {
  auto json = vpack::Parser::fromJson(R"({
    "type": "Polygon",
    "coordinates": [
      [
        [
          50.361328125,
          61.501734289732326
        ],
        [
          51.2841796875,
          61.501734289732326
        ],
        [
          51.2841796875,
          61.907926072709756
        ],
        [
          50.361328125,
          61.907926072709756
        ],
        [
          50.361328125,
          61.501734289732326
        ]
      ]
    ]
  })");

  ShapeContainer shape;
  ASSERT_TRUE(json::ParseRegion(json->slice(), shape).ok());
  ASSERT_EQ(ShapeContainer::Type::S2Polygon, shape.type());

  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    auto a = GeoJsonAnalyzer::make(irs::slice_to_view<char>(vpack::Slice::emptyObjectSlice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(*shape.region(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize shape, custom options
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(*shape.region(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize centroid
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize centroid, custom options
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize point
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Point;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_FALSE(a->reset(slice_to_view<char>(json->slice())));
    ASSERT_FALSE(a->next());
  }
}

TEST( GeoJsonAnalyzerVpackTest, tokenizePolygon) {
  auto json = vpack::Parser::fromJson(R"({
    "type": "Polygon",
    "coordinates": [
      [
        [
          52.44873046875,
          64.33039136366138
        ],
        [
          50.73486328125,
          63.792191443824464
        ],
        [
          51.5478515625,
          63.104699747121074
        ],
        [
          52.6904296875,
          62.825055614564306
        ],
        [
          54.95361328125,
          63.203925767041305
        ],
        [
          55.37109374999999,
          63.82128765261384
        ],
        [
          54.7998046875,
          64.37794095121995
        ],
        [
          53.525390625,
          64.44437240555092
        ],
        [
          52.44873046875,
          64.33039136366138
        ]
      ]
    ]
  })");

  ShapeContainer shape;
  ASSERT_TRUE(json::ParseRegion(json->slice(), shape).ok());
  ASSERT_EQ(ShapeContainer::Type::S2Polygon, shape.type());

  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(*shape.region(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize shape, custom options
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(*shape.region(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize centroid
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize centroid, custom options
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize point
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Point;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_FALSE(a->reset(slice_to_view<char>(json->slice())));
    ASSERT_FALSE(a->next());
  }
}

TEST( GeoJsonAnalyzerVpackTest, tokenizeLineString) {
  auto json = vpack::Parser::fromJson(R"({
    "type": "LineString",
    "coordinates": [
      [
        37.615908086299896,
        55.704700721216476
      ],
      [
        37.61495590209961,
        55.70460097444075
      ],
      [
        37.614915668964386,
        55.704266972019845
      ],
      [
        37.61498004198074,
        55.70365336737268
      ],
      [
        37.61568009853363,
        55.7036518560193
      ],
      [
        37.61656254529953,
        55.7041400201247
      ],
      [
        37.61668860912323,
        55.70447251230901
      ],
      [
        37.615661323070526,
        55.704404502774175
      ],
      [
        37.61548697948456,
        55.70397830699434
      ],
      [
        37.61526703834534,
        55.70439090085301
      ]
    ]
  })");

  ShapeContainer shape;
  ASSERT_TRUE(json::ParseRegion(json->slice(), shape).ok());
  ASSERT_EQ(ShapeContainer::Type::S2Polyline, shape.type());

  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(*shape.region(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    auto end = terms.end();
    for (; a->next() && begin != end; ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, end);
    while (a->next()) {  // centroid terms
    }
  }

  // tokenize shape, custom options
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(*shape.region(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    auto end = terms.end();
    for (; a->next() && begin != end; ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, end);
    while (a->next()) {  // centroid terms
    }
  }

  // tokenize centroid
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize centroid, custom options
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize point
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Point;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_FALSE(a->reset(slice_to_view<char>(json->slice())));
    ASSERT_FALSE(a->next());
  }
}

TEST( GeoJsonAnalyzerVpackTest, tokenizeMultiPolygon) {
  auto json = vpack::Parser::fromJson(R"({
    "type": "MultiPolygon",
    "coordinates": [
        [
            [
                [
                    107,
                    7
                ],
                [
                    108,
                    7
                ],
                [
                    108,
                    8
                ],
                [
                    107,
                    8
                ],
                [
                    107,
                    7
                ]
            ]
        ],
        [
            [
                [
                    100,
                    0
                ],
                [
                    101,
                    0
                ],
                [
                    101,
                    1
                ],
                [
                    100,
                    1
                ],
                [
                    100,
                    0
                ]
            ]
        ]
    ]
  })");

  ShapeContainer shape;
  ASSERT_TRUE(json::ParseRegion(json->slice(), shape).ok());
  ASSERT_EQ(ShapeContainer::Type::S2Polygon, shape.type());

  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(*shape.region(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    auto end = terms.end();
    for (; a->next() && begin != end; ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, end);
    while (a->next()) {  // centroid terms
    }
  }

  // tokenize centroid
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize point
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Point;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_FALSE(a->reset(slice_to_view<char>(json->slice())));
    ASSERT_FALSE(a->next());
  }
}

TEST( GeoJsonAnalyzerVpackTest, tokenizeMultiPoint) {
  auto json = vpack::Parser::fromJson(R"({
    "type": "MultiPoint",
    "coordinates": [
        [
            -105.01621,
            39.57422
        ],
        [
            -80.666513,
            35.053994
        ]
    ]
  })");

  ShapeContainer shape;
  ASSERT_TRUE(json::ParseRegion(json->slice(), shape).ok());
  ASSERT_EQ(ShapeContainer::Type::S2Multipoint, shape.type());

  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(*shape.region(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    auto end = terms.end();
    for (; a->next() && begin != end; ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, end);
    while (a->next()) {  // centroid terms
    }
  }

  // tokenize shape, custom options
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(*shape.region(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    auto end = terms.end();
    for (; a->next() && begin != end; ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, end);
    while (a->next()) {  // centroid terms
    }
  }

  // tokenize centroid
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize centroid, custom options
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize point
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Point;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_FALSE(a->reset(slice_to_view<char>(json->slice())));
    ASSERT_FALSE(a->next());
  }
}

TEST( GeoJsonAnalyzerVpackTest, tokenizeMultiPolyLine) {
  auto json = vpack::Parser::fromJson(R"({
    "type": "MultiLineString",
    "coordinates": [
        [
            [
                -105.021443,
                39.578057
            ],
            [
                -105.021507,
                39.577809
            ],
            [
                -105.021572,
                39.577495
            ],
            [
                -105.021572,
                39.577164
            ],
            [
                -105.021572,
                39.577032
            ],
            [
                -105.021529,
                39.576784
            ]
        ],
        [
            [
                -105.019898,
                39.574997
            ],
            [
                -105.019598,
                39.574898
            ],
            [
                -105.019061,
                39.574782
            ]
        ],
        [
            [
                -105.017173,
                39.574402
            ],
            [
                -105.01698,
                39.574385
            ],
            [
                -105.016636,
                39.574385
            ],
            [
                -105.016508,
                39.574402
            ],
            [
                -105.01595,
                39.57427
            ]
        ],
        [
            [
                -105.014276,
                39.573972
            ],
            [
                -105.014126,
                39.574038
            ],
            [
                -105.013825,
                39.57417
            ],
            [
                -105.01331,
                39.574452
            ]
        ]
    ]
  })");

  ShapeContainer shape;
  ASSERT_TRUE(json::ParseRegion(json->slice(), shape).ok());
  ASSERT_EQ(ShapeContainer::Type::S2Multipolyline, shape.type());

  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(*shape.region(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    auto end = terms.end();
    for (; a->next() && begin != end; ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, end);
    while (a->next()) {  // centroid terms
    }
  }

  // tokenize shape, custom options
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(*shape.region(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    auto end = terms.end();
    for (; a->next() && begin != end; ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, end);
    while (a->next()) {  // centroid terms
    }
  }

  // tokenize centroid
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize centroid, custom options
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize point
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Point;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_FALSE(a->reset(slice_to_view<char>(json->slice())));
    ASSERT_FALSE(a->next());
  }
}

TEST( GeoJsonAnalyzerVpackTest, tokenizePoint) {
  auto json = vpack::Parser::fromJson(R"({
    "type": "Point",
    "coordinates": [
      53.72314453125,
      63.57789956676574
    ]
  })");

  ShapeContainer shape;
  ASSERT_TRUE(json::ParseRegion(json->slice(), shape).ok());
  ASSERT_EQ(ShapeContainer::Type::S2Point, shape.type());

  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto tokenizer = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto a = static_cast<GeoJsonAnalyzer*>(tokenizer.get());
    EXPECT_EQ(GeoJsonAnalyzer::Type::Shape, a->shapeType());
    EXPECT_EQ(1, a->options().level_mod());
    EXPECT_FALSE(a->options().optimize_for_space());
    EXPECT_EQ("$", a->options().marker());
    EXPECT_EQ(opts.options.min_level, a->options().min_level());
    EXPECT_EQ(opts.options.max_level, a->options().max_level());
    EXPECT_EQ(opts.options.max_cells, a->options().max_cells());
    EXPECT_FALSE(a->options().index_contains_points_only());

    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize shape, custom options
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto tokenizer = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto a = static_cast<GeoJsonAnalyzer*>(tokenizer.get());
    EXPECT_EQ(GeoJsonAnalyzer::Type::Shape, a->shapeType());
    EXPECT_EQ(1, a->options().level_mod());
    EXPECT_FALSE(a->options().optimize_for_space());
    EXPECT_EQ("$", a->options().marker());
    EXPECT_EQ(opts.options.min_level, a->options().min_level());
    EXPECT_EQ(opts.options.max_level, a->options().max_level());
    EXPECT_EQ(opts.options.max_cells, a->options().max_cells());
    EXPECT_FALSE(a->options().index_contains_points_only());

    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize centroid
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto tokenizer = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto a = static_cast<GeoJsonAnalyzer*>(tokenizer.get());
    EXPECT_EQ(GeoJsonAnalyzer::Type::Centroid, a->shapeType());
    EXPECT_EQ(1, a->options().level_mod());
    EXPECT_FALSE(a->options().optimize_for_space());
    EXPECT_EQ("$", a->options().marker());
    EXPECT_EQ(opts.options.min_level, a->options().min_level());
    EXPECT_EQ(opts.options.max_level, a->options().max_level());
    EXPECT_EQ(opts.options.max_cells, a->options().max_cells());
    EXPECT_TRUE(a->options().index_contains_points_only());

    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, true));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize centroid, custom options
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto tokenizer = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto a = static_cast<GeoJsonAnalyzer*>(tokenizer.get());
    EXPECT_EQ(GeoJsonAnalyzer::Type::Centroid, a->shapeType());
    EXPECT_EQ(1, a->options().level_mod());
    EXPECT_FALSE(a->options().optimize_for_space());
    EXPECT_EQ("$", a->options().marker());
    EXPECT_EQ(opts.options.min_level, a->options().min_level());
    EXPECT_EQ(opts.options.max_level, a->options().max_level());
    EXPECT_EQ(opts.options.max_cells, a->options().max_cells());
    EXPECT_TRUE(a->options().index_contains_points_only());

    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, true));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize point
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Point;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto tokenizer = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto a = static_cast<GeoJsonAnalyzer*>(tokenizer.get());
    EXPECT_EQ(GeoJsonAnalyzer::Type::Point, a->shapeType());
    EXPECT_EQ(1, a->options().level_mod());
    EXPECT_FALSE(a->options().optimize_for_space());
    EXPECT_EQ("$", a->options().marker());
    EXPECT_EQ(opts.options.min_level, a->options().min_level());
    EXPECT_EQ(opts.options.max_level, a->options().max_level());
    EXPECT_EQ(opts.options.max_cells, a->options().max_cells());
    EXPECT_TRUE(a->options().index_contains_points_only());

    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize point, custom options
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    opts.type = GeoJsonAnalyzer::Type::Point;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto tokenizer = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto a = static_cast<GeoJsonAnalyzer*>(tokenizer.get());
    EXPECT_EQ(GeoJsonAnalyzer::Type::Point, a->shapeType());
    EXPECT_EQ(1, a->options().level_mod());
    EXPECT_FALSE(a->options().optimize_for_space());
    EXPECT_EQ("$", a->options().marker());
    EXPECT_EQ(opts.options.min_level, a->options().min_level());
    EXPECT_EQ(opts.options.max_level, a->options().max_level());
    EXPECT_EQ(opts.options.max_cells, a->options().max_cells());
    EXPECT_TRUE(a->options().index_contains_points_only());

    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, true));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }
}

TEST( GeoJsonAnalyzerVpackTest, tokenizePointGeoJSONArray) {
  auto json =
    vpack::Parser::fromJson(R"([ 53.72314453125, 63.57789956676574 ])");

  ShapeContainer shape;
  std::vector<S2LatLng> cache;
  ASSERT_TRUE(ParseShape<Parsing::OnlyPoint>(
    json->slice(), shape, cache, coding::Options::Invalid, nullptr));
  ASSERT_EQ(ShapeContainer::Type::S2Point, shape.type());

  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto tokenizer = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto a = static_cast<GeoJsonAnalyzer*>(tokenizer.get());
    ASSERT_EQ(GeoJsonAnalyzer::Type::Shape, a->shapeType());
    ASSERT_EQ(1, a->options().level_mod());
    ASSERT_FALSE(a->options().optimize_for_space());
    ASSERT_EQ("$", a->options().marker());
    ASSERT_EQ(opts.options.min_level, a->options().min_level());
    ASSERT_EQ(opts.options.max_level, a->options().max_level());
    ASSERT_EQ(opts.options.max_cells, a->options().max_cells());
    ASSERT_FALSE(a->options().index_contains_points_only());

    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize shape, custom options
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto tokenizer = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto a = static_cast<GeoJsonAnalyzer*>(tokenizer.get());  
    ASSERT_EQ(GeoJsonAnalyzer::Type::Shape, a->shapeType());
    ASSERT_EQ(1, a->options().level_mod());
    ASSERT_FALSE(a->options().optimize_for_space());
    ASSERT_EQ("$", a->options().marker());
    ASSERT_EQ(opts.options.min_level, a->options().min_level());
    ASSERT_EQ(opts.options.max_level, a->options().max_level());
    ASSERT_EQ(opts.options.max_cells, a->options().max_cells());
    ASSERT_FALSE(a->options().index_contains_points_only());

    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize centroid
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto tokenizer = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto a = static_cast<GeoJsonAnalyzer*>(tokenizer.get());
    EXPECT_EQ(GeoJsonAnalyzer::Type::Centroid, a->shapeType());
    EXPECT_EQ(1, a->options().level_mod());
    EXPECT_FALSE(a->options().optimize_for_space());
    EXPECT_EQ("$", a->options().marker());
    EXPECT_EQ(opts.options.min_level, a->options().min_level());
    EXPECT_EQ(opts.options.max_level, a->options().max_level());
    EXPECT_EQ(opts.options.max_cells, a->options().max_cells());
    EXPECT_TRUE(a->options().index_contains_points_only());

    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize centroid, custom options
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto tokenizer = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto a = static_cast<GeoJsonAnalyzer*>(tokenizer.get());  
    EXPECT_EQ(GeoJsonAnalyzer::Type::Centroid, a->shapeType());
    EXPECT_EQ(1, a->options().level_mod());
    EXPECT_FALSE(a->options().optimize_for_space());
    EXPECT_EQ("$", a->options().marker());
    EXPECT_EQ(opts.options.min_level, a->options().min_level());
    EXPECT_EQ(opts.options.max_level, a->options().max_level());
    EXPECT_EQ(opts.options.max_cells, a->options().max_cells());
    EXPECT_TRUE(a->options().index_contains_points_only());

    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize point
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Point;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto tokenizer = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto a = static_cast<GeoJsonAnalyzer*>(tokenizer.get());
    EXPECT_EQ(GeoJsonAnalyzer::Type::Point, a->shapeType());
    EXPECT_EQ(1, a->options().level_mod());
    EXPECT_FALSE(a->options().optimize_for_space());
    EXPECT_EQ("$", a->options().marker());
    EXPECT_EQ(opts.options.min_level, a->options().min_level());
    EXPECT_EQ(opts.options.max_level, a->options().max_level());
    EXPECT_EQ(opts.options.max_cells, a->options().max_cells());
    EXPECT_TRUE(a->options().index_contains_points_only());

    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }

  // tokenize point, custom options
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 3;
    opts.options.max_level = 22;
    opts.type = GeoJsonAnalyzer::Type::Point;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto tokenizer = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto a = static_cast<GeoJsonAnalyzer*>(tokenizer.get());
    EXPECT_EQ(GeoJsonAnalyzer::Type::Point, a->shapeType());
    EXPECT_EQ(1, a->options().level_mod());
    EXPECT_FALSE(a->options().optimize_for_space());
    EXPECT_EQ("$", a->options().marker());
    EXPECT_EQ(opts.options.min_level, a->options().min_level());
    EXPECT_EQ(opts.options.max_level, a->options().max_level());
    EXPECT_EQ(opts.options.max_cells, a->options().max_cells());
    EXPECT_TRUE(a->options().index_contains_points_only());

    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(slice_to_view<char>(json->slice())));

    S2RegionTermIndexer indexer(S2Options(opts.options, false));
    auto terms = indexer.GetIndexTerms(shape.centroid(), {});
    ASSERT_FALSE(terms.empty());

    auto begin = terms.begin();
    for (; a->next(); ++begin) {
      ASSERT_EQ(1, inc->value);
      ASSERT_EQ(*begin, ViewCast<char>(term->value));
    }
    ASSERT_EQ(begin, terms.end());
  }
}

TEST( GeoJsonAnalyzerVpackTest, invalidGeoJson) {
  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_FALSE(
      a->reset(slice_to_view<char>(vpack::Slice::emptyObjectSlice())));
    ASSERT_FALSE(
      a->reset(slice_to_view<char>(vpack::Slice::emptyArraySlice())));
    ASSERT_FALSE(a->reset(slice_to_view<char>(vpack::Slice::noneSlice())));
    ASSERT_FALSE(a->reset(slice_to_view<char>(vpack::Slice::falseSlice())));
    ASSERT_FALSE(a->reset(slice_to_view<char>(vpack::Slice::trueSlice())));
    ASSERT_FALSE(a->reset(slice_to_view<char>(vpack::Slice::zeroSlice())));
    ASSERT_FALSE(a->reset(slice_to_view<char>(vpack::Slice::nullSlice())));
  }

  // tokenize centroid
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_FALSE(
      a->reset(slice_to_view<char>(vpack::Slice::emptyObjectSlice())));
    ASSERT_FALSE(
      a->reset(slice_to_view<char>(vpack::Slice::emptyArraySlice())));
    ASSERT_FALSE(a->reset(slice_to_view<char>(vpack::Slice::noneSlice())));
    ASSERT_FALSE(a->reset(slice_to_view<char>(vpack::Slice::falseSlice())));
    ASSERT_FALSE(a->reset(slice_to_view<char>(vpack::Slice::trueSlice())));
    ASSERT_FALSE(a->reset(slice_to_view<char>(vpack::Slice::zeroSlice())));
    ASSERT_FALSE(a->reset(slice_to_view<char>(vpack::Slice::nullSlice())));
  }

  // tokenize point
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Point;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_FALSE(
      a->reset(slice_to_view<char>(vpack::Slice::emptyObjectSlice())));
    ASSERT_FALSE(
      a->reset(slice_to_view<char>(vpack::Slice::emptyArraySlice())));
    ASSERT_FALSE(a->reset(slice_to_view<char>(vpack::Slice::noneSlice())));
    ASSERT_FALSE(a->reset(slice_to_view<char>(vpack::Slice::falseSlice())));
    ASSERT_FALSE(a->reset(slice_to_view<char>(vpack::Slice::trueSlice())));
    ASSERT_FALSE(a->reset(slice_to_view<char>(vpack::Slice::zeroSlice())));
    ASSERT_FALSE(a->reset(slice_to_view<char>(vpack::Slice::nullSlice())));
  }
}

TEST( GeoJsonAnalyzerVpackTest, prepareQuery) {
  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 2;
    opts.options.max_level = 22;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto tokenizer = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto a = static_cast<GeoJsonAnalyzer*>(tokenizer.get());

    GeoFilterOptionsBase options;
    a->prepare(options);

    EXPECT_EQ(options.prefix, "");
    EXPECT_EQ(options.stored, StoredType::VPack);
    EXPECT_EQ(1, options.options.level_mod());
    EXPECT_FALSE(options.options.optimize_for_space());
    EXPECT_EQ("$", options.options.marker());
    EXPECT_EQ(opts.options.min_level, options.options.min_level());
    EXPECT_EQ(opts.options.max_level, options.options.max_level());
    EXPECT_EQ(opts.options.max_cells, options.options.max_cells());
    EXPECT_FALSE(options.options.index_contains_points_only());
  }

  // tokenize centroid
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 2;
    opts.options.max_level = 22;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto tokenizer = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto a = static_cast<GeoJsonAnalyzer*>(tokenizer.get());

    GeoFilterOptionsBase options;
    a->prepare(options);

    EXPECT_EQ(options.prefix, "");
    EXPECT_EQ(options.stored, StoredType::VPack);
    EXPECT_EQ(1, options.options.level_mod());
    EXPECT_FALSE(options.options.optimize_for_space());
    EXPECT_EQ("$", options.options.marker());
    EXPECT_EQ(opts.options.min_level, options.options.min_level());
    EXPECT_EQ(opts.options.max_level, options.options.max_level());
    EXPECT_EQ(opts.options.max_cells, options.options.max_cells());
    EXPECT_TRUE(options.options.index_contains_points_only());
  }

  // tokenize point
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 2;
    opts.options.max_level = 22;
    opts.type = GeoJsonAnalyzer::Type::Point;
    vpack::Builder builder;
    ToVPack(builder, opts);
    auto tokenizer = GeoJsonAnalyzer::make(slice_to_view<char>(builder.slice()));
    auto a = static_cast<GeoJsonAnalyzer*>(tokenizer.get());

    GeoFilterOptionsBase options;
    a->prepare(options);

    EXPECT_EQ(options.prefix, "");
    EXPECT_EQ(options.stored, StoredType::VPack);
    EXPECT_EQ(1, options.options.level_mod());
    EXPECT_FALSE(options.options.optimize_for_space());
    EXPECT_EQ("$", options.options.marker());
    EXPECT_EQ(opts.options.min_level, options.options.min_level());
    EXPECT_EQ(opts.options.max_level, options.options.max_level());
    EXPECT_EQ(opts.options.max_cells, options.options.max_cells());
    EXPECT_TRUE(options.options.index_contains_points_only());
  }
}

TEST( GeoJsonAnalyzerVpackTest, createFromSlice) {
  // no type supplied
  {
    auto json = vpack::Parser::fromJson(R"({})");
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(json->slice()));
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoJsonAnalyzer&>(*a);

    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Shape;
    ASSERT_EQ(opts.type, impl.shapeType());
    ASSERT_EQ(1, impl.options().level_mod());
    ASSERT_FALSE(impl.options().optimize_for_space());
    ASSERT_EQ("$", impl.options().marker());
    ASSERT_EQ(opts.options.min_level, impl.options().min_level());
    ASSERT_EQ(opts.options.max_level, impl.options().max_level());
    ASSERT_EQ(opts.options.max_cells, impl.options().max_cells());
    ASSERT_FALSE(impl.options().index_contains_points_only());
  }

  {
    auto json = vpack::Parser::fromJson(R"({ "type": "shape" })");
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(json->slice()));
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoJsonAnalyzer&>(*a);

    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Shape;
    ASSERT_EQ(opts.type, impl.shapeType());
    ASSERT_EQ(1, impl.options().level_mod());
    ASSERT_FALSE(impl.options().optimize_for_space());
    ASSERT_EQ("$", impl.options().marker());
    ASSERT_EQ(opts.options.min_level, impl.options().min_level());
    ASSERT_EQ(opts.options.max_level, impl.options().max_level());
    ASSERT_EQ(opts.options.max_cells, impl.options().max_cells());
    ASSERT_FALSE(impl.options().index_contains_points_only());
  }

  {
    auto json = vpack::Parser::fromJson(R"({
      "type": "shape",
      "options" : {
        "maxCells": 1000
      }
    })");
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(json->slice()));
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoJsonAnalyzer&>(*a);

    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.type = GeoJsonAnalyzer::Type::Shape;
    ASSERT_EQ(opts.type, impl.shapeType());
    ASSERT_EQ(1, impl.options().level_mod());
    ASSERT_FALSE(impl.options().optimize_for_space());
    ASSERT_EQ("$", impl.options().marker());
    ASSERT_EQ(opts.options.min_level, impl.options().min_level());
    ASSERT_EQ(opts.options.max_level, impl.options().max_level());
    ASSERT_EQ(opts.options.max_cells, impl.options().max_cells());
    ASSERT_FALSE(impl.options().index_contains_points_only());
  }

  {
    auto json = vpack::Parser::fromJson(R"({
      "type": "shape",
      "options" : {
        "maxCells": 1000,
        "minLevel": 2,
        "maxLevel": 22
      }
    })");
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(json->slice()));
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoJsonAnalyzer&>(*a);

    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 2;
    opts.options.max_level = 22;
    opts.type = GeoJsonAnalyzer::Type::Shape;
    ASSERT_EQ(opts.type, impl.shapeType());
    ASSERT_EQ(1, impl.options().level_mod());
    ASSERT_FALSE(impl.options().optimize_for_space());
    ASSERT_EQ("$", impl.options().marker());
    ASSERT_EQ(opts.options.min_level, impl.options().min_level());
    ASSERT_EQ(opts.options.max_level, impl.options().max_level());
    ASSERT_EQ(opts.options.max_cells, impl.options().max_cells());
    ASSERT_FALSE(impl.options().index_contains_points_only());
  }

  {
    auto json = vpack::Parser::fromJson(R"({ "type": "centroid" })");
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(json->slice()));
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoJsonAnalyzer&>(*a);

    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    EXPECT_EQ(opts.type, impl.shapeType());
    EXPECT_EQ(1, impl.options().level_mod());
    EXPECT_FALSE(impl.options().optimize_for_space());
    EXPECT_EQ("$", impl.options().marker());
    EXPECT_EQ(opts.options.min_level, impl.options().min_level());
    EXPECT_EQ(opts.options.max_level, impl.options().max_level());
    EXPECT_EQ(opts.options.max_cells, impl.options().max_cells());
    EXPECT_TRUE(impl.options().index_contains_points_only());
  }

  {
    auto json = vpack::Parser::fromJson(R"({ "type": "point" })");
    auto a = GeoJsonAnalyzer::make(slice_to_view<char>(json->slice()));
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoJsonAnalyzer&>(*a);

    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Point;
    EXPECT_EQ(opts.type, impl.shapeType());
    EXPECT_EQ(1, impl.options().level_mod());
    EXPECT_FALSE(impl.options().optimize_for_space());
    EXPECT_EQ("$", impl.options().marker());
    EXPECT_EQ(opts.options.min_level, impl.options().min_level());
    EXPECT_EQ(opts.options.max_level, impl.options().max_level());
    EXPECT_EQ(opts.options.max_cells, impl.options().max_cells());
    EXPECT_TRUE(impl.options().index_contains_points_only());
  }

  {
    auto json = vpack::Parser::fromJson(
      R"({ "type": "point", "unknownField":"anything" })");
    auto a =  GeoJsonAnalyzer::make(slice_to_view<char>(json->slice()));
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoJsonAnalyzer&>(*a);

    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Point;
    EXPECT_EQ(opts.type, impl.shapeType());
    EXPECT_EQ(1, impl.options().level_mod());
    EXPECT_FALSE(impl.options().optimize_for_space());
    EXPECT_EQ("$", impl.options().marker());
    EXPECT_EQ(opts.options.min_level, impl.options().min_level());
    EXPECT_EQ(opts.options.max_level, impl.options().max_level());
    EXPECT_EQ(opts.options.max_cells, impl.options().max_cells());
    EXPECT_TRUE(impl.options().index_contains_points_only());
  }

  {
    auto json = vpack::Parser::fromJson(R"({
      "type": "Shape"
    })");
    ASSERT_EQ(nullptr,  GeoJsonAnalyzer::make(slice_to_view<char>(json->slice())));
  }

  {
    auto json = vpack::Parser::fromJson(R"({
      "type": "Centroid"
    })");
    ASSERT_EQ(nullptr,  GeoJsonAnalyzer::make(slice_to_view<char>(json->slice())));
  }

  {
    auto json = vpack::Parser::fromJson(R"({
      "type": "Point"
    })");
    ASSERT_EQ(nullptr,  GeoJsonAnalyzer::make(slice_to_view<char>(json->slice())));
  }

  // minLevel > maxLevel
  {
    auto json = vpack::Parser::fromJson(R"({
      "type": "shape",
      "options" : {
        "minLevel": 22,
        "maxLevel": 2
      }
    })");
    ASSERT_EQ(nullptr,  GeoJsonAnalyzer::make(slice_to_view<char>(json->slice())));
  }

  // negative value
  {
    auto json = vpack::Parser::fromJson(R"({
      "options" : {
        "minLevel": -2,
        "maxLevel": 22
      }
    })");
    ASSERT_EQ(nullptr,  GeoJsonAnalyzer::make(slice_to_view<char>(json->slice())));
  }

  // negative value
  {
    auto json = vpack::Parser::fromJson(R"({
      "options" : {
        "minLevel": -22,
        "maxLevel": -2
      }
    })");
    ASSERT_EQ(nullptr,  GeoJsonAnalyzer::make(slice_to_view<char>(json->slice())));
  }

  // negative value
  {
    auto json = vpack::Parser::fromJson(R"({
      "options" : {
        "maxCells": -2
      }
    })");
    ASSERT_EQ(nullptr,  GeoJsonAnalyzer::make(slice_to_view<char>(json->slice())));
  }

  // nan
  {
    auto json = vpack::Parser::fromJson(R"({
      "options" : {
        "maxCells": "2"
      }
    })");
    ASSERT_EQ(nullptr,  GeoJsonAnalyzer::make(slice_to_view<char>(json->slice())));
  }

  // higher than max GeoOptions::MAX_LEVEL
  {
    auto json = vpack::Parser::fromJson(R"({
      "type": "shape",
      "options" : {
        "maxLevel": 31
      }
    })");
    ASSERT_EQ(nullptr,  GeoJsonAnalyzer::make(slice_to_view<char>(json->slice())));
  }

  // higher than max GeoOptions::MAX_LEVEL
  {
    auto json = vpack::Parser::fromJson(R"({
      "type": "shape",
      "options" : {
        "minLevel": 31,
        "maxLevel": 31
      }
    })");
    ASSERT_EQ(nullptr,  GeoJsonAnalyzer::make(slice_to_view<char>(json->slice())));
  }
}
