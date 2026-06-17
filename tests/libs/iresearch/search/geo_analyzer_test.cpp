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

#include <s2/s2point_region.h>
#include <simdjson.h>

#include <bit>
#include <cstring>

#include "basics/down_cast.h"
#include "geo/geo_json.h"
#include "geo_test_helpers.hpp"
#include "iresearch/analysis/geo_analyzer.hpp"
#include "iresearch/search/geo_filter.hpp"
#include "tests_shared.hpp"

using namespace irs;
using namespace analysis;
using namespace sdb::geo;

namespace irs::tests {

template<typename Owner>
inline irs::analysis::Analyzer::ptr MakeAnalyzer(typename Owner::Options opts) {
  return Owner::Make(std::move(opts));
}

}  // namespace irs::tests
namespace {

// Little-endian WKB builder for the resetWKB analyzer tests. Mirrors the
// builder in tests/libs/iresearch/utils/wkb_parser_test.cpp; kept local so
// these tests stay self-contained.
class WkbBuilder {
 public:
  WkbBuilder& PutU8(uint8_t v) {
    _buf.push_back(static_cast<char>(v));
    return *this;
  }
  WkbBuilder& PutU32(uint32_t v) {
    if (std::endian::native != std::endian::little) {
      v = std::byteswap(v);
    }
    _buf.append(reinterpret_cast<const char*>(&v), sizeof(v));
    return *this;
  }
  WkbBuilder& PutDouble(double v) {
    uint64_t raw;
    std::memcpy(&raw, &v, sizeof(v));
    if (std::endian::native != std::endian::little) {
      raw = std::byteswap(raw);
    }
    _buf.append(reinterpret_cast<const char*>(&raw), sizeof(raw));
    return *this;
  }
  // OGC SFA: lng then lat.
  WkbBuilder& PutXY(double lng, double lat) {
    return PutDouble(lng).PutDouble(lat);
  }
  WkbBuilder& Header(uint32_t type) { return PutU8(1).PutU32(type); }
  irs::bytes_view View() const {
    return {reinterpret_cast<const irs::byte_type*>(_buf.data()), _buf.size()};
  }

 private:
  std::string _buf;
};

}  // namespace

TEST(GeoOptionsTest, default_options) {
  GeoOptions opts;
  ASSERT_EQ(20, opts.max_cells);
  ASSERT_EQ(4, opts.min_level);
  ASSERT_EQ(23, opts.max_level);
  ASSERT_EQ(1, opts.level_mod);
  ASSERT_FALSE(opts.optimize_for_space);
}

TEST(GeoBench, sizes) {
  GTEST_SKIP() << "It's just for check sizes, not comment out to allow compile";

  auto source_analyzer =
    tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>({});
  GeoJsonAnalyzer::Options opts;
  opts.coding = GeoJsonAnalyzer::Coding::S2LatLngU32;
  auto s2_analyzer = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);

  auto store_size = [](irs::analysis::Analyzer& a, std::string_view json) {
    a.reset(json);
    const auto* store = irs::get<irs::StoreAttr>(a);
    return store ? store->value.size() : size_t{0};
  };

  auto bench = [&](std::string_view json) {
    std::cerr << json << std::endl;
    std::cerr << store_size(*source_analyzer, json) << std::endl;
    std::cerr << store_size(*s2_analyzer, json) << std::endl;
  };

  bench(R"=([ 6.537, 50.332 ])=");
  bench(R"=({ "type": "Point", "coordinates": [ 6.537, 50.332 ] })=");
  bench(
    R"=({ "type": "MultiPoint", "coordinates": [ [ 6.537, 50.332 ], [ 6.537, 50.376 ] ] })=");
  bench(
    R"=({ "type": "MultiPoint", "coordinates": [ [ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ],[ 6.537, 50.332 ], [ 6.537, 50.376 ] ] })=");
  bench(
    R"=({ "type": "LineString", "coordinates": [ [ 6.537, 50.332 ], [ 6.537, 50.376 ] ] })=");
  bench(
    R"=({ "type": "MultiLineString", "coordinates": [ [ [ 6.537, 50.332 ], [ 6.537, 50.376 ] ], [ [ 6.621, 50.332 ], [ 6.621, 50.376 ] ] ] })=");
  bench(
    R"=({ "type": "Polygon", "coordinates": [ [ [6.1,50.1], [7.5,50.1], [7.5,52.1], [6.1,51.1], [6.1,50.1] ] ] })=");
  bench(
    R"=({ "type": "MultiPolygon", "coordinates": [ [ [ [6.501,50.1], [7.5,50.1], [7.5,51.1], [6.501,51.1], [6.501,50.1] ] ], [ [ [6.1,50.1], [6.5,50.1], [6.5,51.1], [6.1,51.1], [6.1,50.1] ] ] ] })=");
  bench(
    R"=({ "type": "Polygon", "coordinates": [ [ [6.1,50.1], [7.5,50.1], [7.5,51.1], [6.1,51.1], [6.1,50.1] ] ] })=");
  bench(
    R"=({ "type": "LineString", "coordinates": [ [ 5.437, 50.332 ], [ 7.537, 50.376 ] ] })=");
  bench(
    R"=({ "type": "Polygon", "coordinates": [ [ [1,1], [4,1], [4,4], [1,4], [1,1] ] ] })=");
  bench(
    R"=({ "type": "Polygon", "coordinates": [ [ [1.1,1.1], [4.1,1.1], [4.1,4.1], [1.1,4.1], [1.1,1.1] ] ] })=");
  bench(
    R"=({"type": "Polygon","coordinates": [[[100.318391,13.535502],[100.318391,14.214848],[101.407575,14.214848],[101.407575,13.535502],[100.318391,13.535502]]]})=");
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
    EXPECT_EQ(options.stored, StoredType::Source);
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
    EXPECT_EQ(options.stored, StoredType::Source);
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
  auto json = tests::FromJson(R"([ 63.57789956676574, 53.72314453125 ])");

  ShapeContainer shape;
  ASSERT_TRUE(json::ParseCoordinates<true>(json.value(), shape, false).ok());
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
    ASSERT_TRUE(a.reset(json.text()));

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
    ASSERT_TRUE(a.reset(json.text()));

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
  auto json = tests::FromJson(R"([ 63.57789956676574, 53.72314453125 ])");
  auto json_object =
    tests::FromJson(R"({ "lat": 63.57789956676574, "lon": 53.72314453125 })");

  ShapeContainer shape;
  ASSERT_TRUE(json::ParseCoordinates<true>(json.value(), shape, false).ok());
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
    ASSERT_TRUE(a.reset(json_object.text()));

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
    ASSERT_TRUE(a.reset(json_object.text()));

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
  auto json = tests::FromJson(R"([ 63.57789956676574, 53.72314453125 ])");
  auto json_object = tests::FromJson(
    R"({ "subObj": { "lat": 63.57789956676574, "lon": 53.72314453125 } })");

  ShapeContainer shape;
  ASSERT_TRUE(json::ParseCoordinates<true>(json.value(), shape, false).ok());
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
    ASSERT_TRUE(a.reset(json_object.text()));

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
    ASSERT_TRUE(a.reset(json_object.text()));

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

TEST(GeoPointAnalyzerTest, createFromOptions) {
  {
    GeoPointAnalyzer::Options opts;
    auto a = tests::MakeAnalyzer<irs::analysis::GeoPointAnalyzer>(opts);
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoPointAnalyzer&>(*a);

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
    GeoPointAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    auto a = tests::MakeAnalyzer<irs::analysis::GeoPointAnalyzer>(opts);
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoPointAnalyzer&>(*a);

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
    GeoPointAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 2;
    opts.options.max_level = 22;
    auto a = tests::MakeAnalyzer<irs::analysis::GeoPointAnalyzer>(opts);
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoPointAnalyzer&>(*a);

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
    GeoPointAnalyzer::Options opts;
    opts.latitude = {"foo"};
    opts.longitude = {"bar"};
    auto a = tests::MakeAnalyzer<irs::analysis::GeoPointAnalyzer>(opts);
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoPointAnalyzer&>(*a);

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
    GeoPointAnalyzer::Options opts;
    opts.latitude = {"subObj", "foo"};
    opts.longitude = {"subObj", "bar"};
    auto a = tests::MakeAnalyzer<irs::analysis::GeoPointAnalyzer>(opts);
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoPointAnalyzer&>(*a);

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
}

TEST(GeoJsonAnalyzerTest, constants) {
  static_assert("geojson" == GeoJsonAnalyzer::type_name());
}

TEST(GeoJsonAnalyzerSourceTest, options) {
  GeoJsonAnalyzer::Options opts;
  ASSERT_EQ(GeoJsonAnalyzer::Type::Shape, opts.type);
  ASSERT_EQ(GeoOptions{}.max_cells, opts.options.max_cells);
  ASSERT_EQ(GeoOptions{}.min_level, opts.options.min_level);
  ASSERT_EQ(GeoOptions{}.max_level, opts.options.max_level);
}

TEST(GeoJsonAnalyzerSourceTest, ctor) {
  auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>({});
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

TEST(GeoJsonAnalyzerSourceTest, tokenizeLatLngRect) {
  auto json = tests::FromJson(R"({
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
  ASSERT_TRUE(json::ParseRegion(json.value(), shape).ok());
  ASSERT_EQ(ShapeContainer::Type::S2Polygon, shape.type());

  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>({});
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_FALSE(a->reset(json.text()));
    ASSERT_FALSE(a->next());
  }
}

TEST(GeoJsonAnalyzerSourceTest, tokenizePolygon) {
  auto json = tests::FromJson(R"({
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
  ASSERT_TRUE(json::ParseRegion(json.value(), shape).ok());
  ASSERT_EQ(ShapeContainer::Type::S2Polygon, shape.type());

  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_FALSE(a->reset(json.text()));
    ASSERT_FALSE(a->next());
  }
}

TEST(GeoJsonAnalyzerSourceTest, tokenizeLineString) {
  auto json = tests::FromJson(R"({
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
  ASSERT_TRUE(json::ParseRegion(json.value(), shape).ok());
  ASSERT_EQ(ShapeContainer::Type::S2Polyline, shape.type());

  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_FALSE(a->reset(json.text()));
    ASSERT_FALSE(a->next());
  }
}

TEST(GeoJsonAnalyzerSourceTest, tokenizeMultiPolygon) {
  auto json = tests::FromJson(R"({
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
  ASSERT_TRUE(json::ParseRegion(json.value(), shape).ok());
  ASSERT_EQ(ShapeContainer::Type::S2Polygon, shape.type());

  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_FALSE(a->reset(json.text()));
    ASSERT_FALSE(a->next());
  }
}

TEST(GeoJsonAnalyzerSourceTest, tokenizeMultiPoint) {
  auto json = tests::FromJson(R"({
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
  ASSERT_TRUE(json::ParseRegion(json.value(), shape).ok());
  ASSERT_EQ(ShapeContainer::Type::S2Multipoint, shape.type());

  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_FALSE(a->reset(json.text()));
    ASSERT_FALSE(a->next());
  }
}

TEST(GeoJsonAnalyzerSourceTest, tokenizeMultiPolyLine) {
  auto json = tests::FromJson(R"({
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
  ASSERT_TRUE(json::ParseRegion(json.value(), shape).ok());
  ASSERT_EQ(ShapeContainer::Type::S2Multipolyline, shape.type());

  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_TRUE(a->reset(json.text()));

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
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_FALSE(a->reset(json.text()));
    ASSERT_FALSE(a->next());
  }
}

TEST(GeoJsonAnalyzerSourceTest, tokenizePoint) {
  auto json = tests::FromJson(R"({
    "type": "Point",
    "coordinates": [
      53.72314453125,
      63.57789956676574
    ]
  })");

  ShapeContainer shape;
  ASSERT_TRUE(json::ParseRegion(json.value(), shape).ok());
  ASSERT_EQ(ShapeContainer::Type::S2Point, shape.type());

  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    auto tokenizer = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
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
    ASSERT_TRUE(a->reset(json.text()));

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
    auto tokenizer = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
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
    ASSERT_TRUE(a->reset(json.text()));

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
    auto tokenizer = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
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
    ASSERT_TRUE(a->reset(json.text()));

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
    auto tokenizer = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
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
    ASSERT_TRUE(a->reset(json.text()));

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
    auto tokenizer = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
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
    ASSERT_TRUE(a->reset(json.text()));

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
    auto tokenizer = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
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
    ASSERT_TRUE(a->reset(json.text()));

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

TEST(GeoJsonAnalyzerSourceTest, tokenizePointGeoJSONArray) {
  auto json = tests::FromJson(R"([ 53.72314453125, 63.57789956676574 ])");

  ShapeContainer shape;
  std::vector<S2LatLng> cache;
  ASSERT_TRUE(ParseShape<Parsing::OnlyPoint>(
    json.value(), shape, cache, coding::Options::Invalid, nullptr));
  ASSERT_EQ(ShapeContainer::Type::S2Point, shape.type());

  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    auto tokenizer = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
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
    ASSERT_TRUE(a->reset(json.text()));

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
    auto tokenizer = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
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
    ASSERT_TRUE(a->reset(json.text()));

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
    auto tokenizer = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
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
    ASSERT_TRUE(a->reset(json.text()));

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
    auto tokenizer = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
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
    ASSERT_TRUE(a->reset(json.text()));

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
    auto tokenizer = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
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
    ASSERT_TRUE(a->reset(json.text()));

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
    auto tokenizer = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
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
    ASSERT_TRUE(a->reset(json.text()));

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

TEST(GeoJsonAnalyzerSourceTest, invalidGeoJson) {
  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_FALSE(a->reset(R"({})"));
    ASSERT_FALSE(a->reset(R"([])"));
    ASSERT_FALSE(a->reset(""));
    ASSERT_FALSE(a->reset("false"));
    ASSERT_FALSE(a->reset("true"));
    ASSERT_FALSE(a->reset("0"));
    ASSERT_FALSE(a->reset("null"));
  }

  // tokenize centroid
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_FALSE(a->reset(R"({})"));
    ASSERT_FALSE(a->reset(R"([])"));
    ASSERT_FALSE(a->reset(""));
    ASSERT_FALSE(a->reset("false"));
    ASSERT_FALSE(a->reset("true"));
    ASSERT_FALSE(a->reset("0"));
    ASSERT_FALSE(a->reset("null"));
  }

  // tokenize point
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Point;
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto* inc = get<IncAttr>(*a);
    ASSERT_NE(nullptr, inc);
    auto* term = get<TermAttr>(*a);
    ASSERT_NE(nullptr, term);
    ASSERT_FALSE(a->reset(R"({})"));
    ASSERT_FALSE(a->reset(R"([])"));
    ASSERT_FALSE(a->reset(""));
    ASSERT_FALSE(a->reset("false"));
    ASSERT_FALSE(a->reset("true"));
    ASSERT_FALSE(a->reset("0"));
    ASSERT_FALSE(a->reset("null"));
  }
}

TEST(GeoJsonAnalyzerSourceTest, prepareQuery) {
  // tokenize shape
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 2;
    opts.options.max_level = 22;
    auto tokenizer = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto a = static_cast<GeoJsonAnalyzer*>(tokenizer.get());

    GeoFilterOptionsBase options;
    a->prepare(options);

    EXPECT_EQ(options.prefix, "");
    EXPECT_EQ(options.stored, StoredType::Source);
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
    auto tokenizer = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto a = static_cast<GeoJsonAnalyzer*>(tokenizer.get());

    GeoFilterOptionsBase options;
    a->prepare(options);

    EXPECT_EQ(options.prefix, "");
    EXPECT_EQ(options.stored, StoredType::Source);
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
    auto tokenizer = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    auto a = static_cast<GeoJsonAnalyzer*>(tokenizer.get());

    GeoFilterOptionsBase options;
    a->prepare(options);

    EXPECT_EQ(options.prefix, "");
    EXPECT_EQ(options.stored, StoredType::Source);
    EXPECT_EQ(1, options.options.level_mod());
    EXPECT_FALSE(options.options.optimize_for_space());
    EXPECT_EQ("$", options.options.marker());
    EXPECT_EQ(opts.options.min_level, options.options.min_level());
    EXPECT_EQ(opts.options.max_level, options.options.max_level());
    EXPECT_EQ(opts.options.max_cells, options.options.max_cells());
    EXPECT_TRUE(options.options.index_contains_points_only());
  }
}

TEST(GeoJsonAnalyzerSourceTest, createFromOptions) {
  // Default Options.
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Shape;
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoJsonAnalyzer&>(*a);

    ASSERT_EQ(opts.type, impl.shapeType());
    ASSERT_EQ(1, impl.options().level_mod());
    ASSERT_FALSE(impl.options().optimize_for_space());
    ASSERT_EQ("$", impl.options().marker());
    ASSERT_EQ(opts.options.min_level, impl.options().min_level());
    ASSERT_EQ(opts.options.max_level, impl.options().max_level());
    ASSERT_EQ(opts.options.max_cells, impl.options().max_cells());
    ASSERT_FALSE(impl.options().index_contains_points_only());
  }

  // Shape with custom max_cells.
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.type = GeoJsonAnalyzer::Type::Shape;
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoJsonAnalyzer&>(*a);

    ASSERT_EQ(opts.type, impl.shapeType());
    ASSERT_EQ(1, impl.options().level_mod());
    ASSERT_FALSE(impl.options().optimize_for_space());
    ASSERT_EQ("$", impl.options().marker());
    ASSERT_EQ(opts.options.min_level, impl.options().min_level());
    ASSERT_EQ(opts.options.max_level, impl.options().max_level());
    ASSERT_EQ(opts.options.max_cells, impl.options().max_cells());
    ASSERT_FALSE(impl.options().index_contains_points_only());
  }

  // Shape with all custom geo numerics.
  {
    GeoJsonAnalyzer::Options opts;
    opts.options.max_cells = 1000;
    opts.options.min_level = 2;
    opts.options.max_level = 22;
    opts.options.optimize_for_space = true;
    opts.type = GeoJsonAnalyzer::Type::Shape;
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoJsonAnalyzer&>(*a);

    ASSERT_EQ(opts.type, impl.shapeType());
    ASSERT_EQ(1, impl.options().level_mod());
    ASSERT_TRUE(impl.options().optimize_for_space());
    ASSERT_EQ("$", impl.options().marker());
    ASSERT_EQ(opts.options.min_level, impl.options().min_level());
    ASSERT_EQ(opts.options.max_level, impl.options().max_level());
    ASSERT_EQ(opts.options.max_cells, impl.options().max_cells());
    ASSERT_FALSE(impl.options().index_contains_points_only());
  }

  // Centroid.
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Centroid;
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoJsonAnalyzer&>(*a);

    EXPECT_EQ(opts.type, impl.shapeType());
    EXPECT_EQ(1, impl.options().level_mod());
    EXPECT_FALSE(impl.options().optimize_for_space());
    EXPECT_EQ("$", impl.options().marker());
    EXPECT_EQ(opts.options.min_level, impl.options().min_level());
    EXPECT_EQ(opts.options.max_level, impl.options().max_level());
    EXPECT_EQ(opts.options.max_cells, impl.options().max_cells());
    EXPECT_TRUE(impl.options().index_contains_points_only());
  }

  // Point.
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Point;
    auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
    ASSERT_NE(nullptr, a);
    auto& impl = dynamic_cast<GeoJsonAnalyzer&>(*a);

    EXPECT_EQ(opts.type, impl.shapeType());
    EXPECT_EQ(1, impl.options().level_mod());
    EXPECT_FALSE(impl.options().optimize_for_space());
    EXPECT_EQ("$", impl.options().marker());
    EXPECT_EQ(opts.options.min_level, impl.options().min_level());
    EXPECT_EQ(opts.options.max_level, impl.options().max_level());
    EXPECT_EQ(opts.options.max_cells, impl.options().max_cells());
    EXPECT_TRUE(impl.options().index_contains_points_only());
  }

  // Coding variants: Make routes Options.coding to the right impl arm.
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Shape;
    opts.coding = GeoJsonAnalyzer::Coding::S2Point;
    ASSERT_NE(nullptr,
              tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts));
  }
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Shape;
    opts.coding = GeoJsonAnalyzer::Coding::S2LatLngU32;
    ASSERT_NE(nullptr,
              tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts));
  }
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Shape;
    opts.coding = GeoJsonAnalyzer::Coding::S2LatLngF64;
    ASSERT_NE(nullptr,
              tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts));
  }
  {
    GeoJsonAnalyzer::Options opts;
    opts.type = GeoJsonAnalyzer::Type::Shape;
    opts.coding = GeoJsonAnalyzer::Coding::Source;
    ASSERT_NE(nullptr,
              tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts));
  }
}

// resetWKB path: feed raw WKB bytes (simulates GEOMETRY column ingest where
// the analyzer parses internally) and verify the analyzer produces the same
// index terms as the JSON path would for an equivalent point.
TEST(GeoJsonAnalyzerShapeTest, tokenizePoint) {
  GeoJsonAnalyzer::Options opts;
  opts.type = GeoJsonAnalyzer::Type::Point;
  opts.coding = GeoJsonAnalyzer::Coding::S2Point;
  auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
  ASSERT_NE(nullptr, a);
  auto* geo = dynamic_cast<GeoJsonAnalyzer*>(a.get());
  ASSERT_NE(nullptr, geo);

  WkbBuilder b;
  b.Header(1).PutXY(6.5, 50.3);

  auto* term = irs::get<TermAttr>(*a);
  ASSERT_NE(nullptr, term);
  ASSERT_TRUE(geo->resetWKB(b.View()));
  size_t term_count = 0;
  while (a->next()) {
    ++term_count;
  }
  EXPECT_GT(term_count, 0U);

  // Store attr must carry encoded bytes (S2Point tag stripped for non-Shape).
  auto* store = irs::get<irs::StoreAttr>(*a);
  ASSERT_NE(nullptr, store);
  EXPECT_FALSE(irs::IsNull(store->value));
}

TEST(GeoJsonAnalyzerShapeTest, tokenizePolygon) {
  GeoJsonAnalyzer::Options opts;
  opts.type = GeoJsonAnalyzer::Type::Shape;
  opts.coding = GeoJsonAnalyzer::Coding::S2Point;
  auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
  ASSERT_NE(nullptr, a);
  auto* geo = dynamic_cast<GeoJsonAnalyzer*>(a.get());
  ASSERT_NE(nullptr, geo);

  // Square polygon (single CCW ring), closed per OGC.
  WkbBuilder b;
  b.Header(3)
    .PutU32(1)  // ring count
    .PutU32(5)  // vertex count (closed)
    .PutXY(0.0, 0.0)
    .PutXY(1.0, 0.0)
    .PutXY(1.0, 1.0)
    .PutXY(0.0, 1.0)
    .PutXY(0.0, 0.0);

  ASSERT_TRUE(geo->resetWKB(b.View()));
  size_t term_count = 0;
  while (a->next()) {
    ++term_count;
  }
  EXPECT_GT(term_count, 0U);

  auto* store = irs::get<irs::StoreAttr>(*a);
  ASSERT_NE(nullptr, store);
  EXPECT_FALSE(irs::IsNull(store->value));
}

// Point-only analyzer must reject non-point shapes.
TEST(GeoJsonAnalyzerShapeTest, rejectsShapeVsTypeMismatch) {
  GeoJsonAnalyzer::Options opts;
  opts.type = GeoJsonAnalyzer::Type::Point;
  opts.coding = GeoJsonAnalyzer::Coding::S2Point;
  auto a = tests::MakeAnalyzer<irs::analysis::GeoJsonAnalyzer>(opts);
  auto* geo = dynamic_cast<GeoJsonAnalyzer*>(a.get());
  ASSERT_NE(nullptr, geo);

  WkbBuilder b;
  b.Header(3)
    .PutU32(1)
    .PutU32(5)
    .PutXY(0.0, 0.0)
    .PutXY(1.0, 0.0)
    .PutXY(1.0, 1.0)
    .PutXY(0.0, 1.0)
    .PutXY(0.0, 0.0);
  EXPECT_FALSE(geo->resetWKB(b.View()));
}

TEST(GeoPointAnalyzerShapeTest, tokenizePoint) {
  GeoPointAnalyzer::Options opts;
  GeoPointAnalyzer a{opts};

  WkbBuilder b;
  b.Header(1).PutXY(6.5, 50.3);
  ASSERT_TRUE(a.resetWKB(b.View()));

  auto* term = irs::get<TermAttr>(a);
  ASSERT_NE(nullptr, term);
  size_t term_count = 0;
  while (a.next()) {
    ++term_count;
  }
  EXPECT_GT(term_count, 0U);

  // GeoPoint uses Source coding: the analyzer writes no derived StoreAttr; the
  // force-included source column is re-parsed at query time instead.
  auto* store = irs::get<irs::StoreAttr>(a);
  ASSERT_NE(nullptr, store);
  EXPECT_TRUE(irs::IsNull(store->value));
}

TEST(GeoPointAnalyzerShapeTest, rejectsNonPoint) {
  GeoPointAnalyzer::Options opts;
  GeoPointAnalyzer a{opts};

  WkbBuilder b;
  b.Header(3)
    .PutU32(1)
    .PutU32(5)
    .PutXY(0.0, 0.0)
    .PutXY(1.0, 0.0)
    .PutXY(1.0, 1.0)
    .PutXY(0.0, 1.0)
    .PutXY(0.0, 0.0);
  EXPECT_FALSE(a.resetWKB(b.View()));
}
