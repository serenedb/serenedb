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

#include <s2/s2latlng.h>
#include <s2/s2loop.h>
#include <s2/s2polygon.h>
#include <s2/s2region_term_indexer.h>

#include <memory>
#include <span>
#include <string>
#include <vector>

#include "geo/coding.h"
#include "gtest/gtest.h"
#include "iresearch/analysis/token_sinks.hpp"
#include "iresearch/analysis/geo_analyzer.hpp"
#include "token_sink_utils.hpp"

namespace {

using irs::analysis::GeoJsonAnalyzer;
using irs::analysis::GeoPointAnalyzer;

std::string PointWkb(double lng, double lat) {
  std::string wkb;
  const auto append = [&](const void* p, size_t n) {
    wkb.append(reinterpret_cast<const char*>(p), n);
  };
  const uint8_t little_endian = 1;
  const uint32_t point_type = 1;
  append(&little_endian, 1);
  append(&point_type, 4);
  append(&lng, 8);
  append(&lat, 8);
  return wkb;
}

std::string PolygonWkb(std::span<const std::pair<double, double>> lng_lat) {
  std::string wkb;
  const auto append = [&](const void* p, size_t n) {
    wkb.append(reinterpret_cast<const char*>(p), n);
  };
  const uint8_t little_endian = 1;
  const uint32_t polygon_type = 3;
  const uint32_t nrings = 1;
  const auto npoints = static_cast<uint32_t>(lng_lat.size());
  append(&little_endian, 1);
  append(&polygon_type, 4);
  append(&nrings, 4);
  append(&npoints, 4);
  for (const auto& [lng, lat] : lng_lat) {
    append(&lng, 8);
    append(&lat, 8);
  }
  return wkb;
}

irs::analysis::Tokenizer::ptr MakeGeoJson(GeoJsonAnalyzer::Type type,
                                          GeoJsonAnalyzer::Coding coding) {
  return GeoJsonAnalyzer::Make({.type = type, .coding = coding});
}

std::vector<std::string> CollectedTerms(const irs::TokenCollector& collector) {
  std::vector<std::string> out;
  out.reserve(collector.tokens.size());
  for (const auto& t : collector.tokens) {
    out.emplace_back(reinterpret_cast<const char*>(t.term.data()),
                     t.term.size());
  }
  return out;
}

struct GeoAnalysis {
  std::vector<std::string> terms;
  std::string store;
};

std::optional<GeoAnalysis> AnalyzeGeo(irs::analysis::Tokenizer& tokenizer,
                                      std::string_view value) {
  irs::TokenCollector collector{irs::TokenLayout::TermsPos};
  if (!irs::AnalyzeValue(
        tokenizer,
        duckdb::string_t{value.data(), static_cast<uint32_t>(value.size())},
        collector)) {
    return std::nullopt;
  }
  GeoAnalysis out;
  out.terms = CollectedTerms(collector);
  out.store.assign(reinterpret_cast<const char*>(collector.store.data()),
                   collector.store.size());
  return out;
}

}  // namespace

TEST(GeoAnalyzerTests, traits_reflect_configuration) {
  {
    auto a = GeoPointAnalyzer::Make({});
    const auto traits = a->Traits();
    EXPECT_FALSE(traits.explicit_pos);
    EXPECT_FALSE(traits.store);
    EXPECT_FALSE(traits.unique);
    EXPECT_FALSE(traits.offsets);
  }
  {
    auto a = MakeGeoJson(GeoJsonAnalyzer::Type::Shape,
                         GeoJsonAnalyzer::Coding::Source);
    const auto traits = a->Traits();
    EXPECT_FALSE(traits.explicit_pos);
    EXPECT_FALSE(traits.store);
  }
  for (const auto coding :
       {GeoJsonAnalyzer::Coding::S2Point, GeoJsonAnalyzer::Coding::S2LatLngF64,
        GeoJsonAnalyzer::Coding::S2LatLngU32}) {
    auto a = MakeGeoJson(GeoJsonAnalyzer::Type::Shape, coding);
    EXPECT_TRUE(a->Traits().store);
  }
}

TEST(GeoAnalyzerTests, geojson_point_terms_match_indexer_oracle) {
  auto stream =
    MakeGeoJson(GeoJsonAnalyzer::Type::Point, GeoJsonAnalyzer::Coding::S2Point);
  auto* geo = dynamic_cast<GeoJsonAnalyzer*>(stream.get());
  ASSERT_NE(nullptr, geo);

  const auto analysis =
    AnalyzeGeo(*stream, R"({"type":"Point","coordinates":[2.0,1.0]})");
  ASSERT_TRUE(analysis.has_value());
  ASSERT_FALSE(analysis->terms.empty());

  S2RegionTermIndexer oracle{geo->options()};
  const auto point = S2LatLng::FromDegrees(1.0, 2.0).Normalized().ToPoint();
  EXPECT_EQ(oracle.GetIndexTerms(point, {}), analysis->terms);

  Encoder encoder;
  encoder.Ensure(30);
  sdb::geo::EncodePoint(encoder, point);
  EXPECT_EQ(std::string_view(encoder.base(), encoder.length()),
            analysis->store);
}

TEST(GeoAnalyzerTests, geojson_json_and_wkb_paths_agree) {
  for (const auto type :
       {GeoJsonAnalyzer::Type::Shape, GeoJsonAnalyzer::Type::Centroid,
        GeoJsonAnalyzer::Type::Point}) {
    SCOPED_TRACE(testing::Message() << "type=" << int(type));
    auto json_stream = MakeGeoJson(type, GeoJsonAnalyzer::Coding::S2Point);
    const auto via_json =
      AnalyzeGeo(*json_stream, R"({"type":"Point","coordinates":[2.0,1.0]})");
    ASSERT_TRUE(via_json.has_value());

    auto wkb_stream = MakeGeoJson(type, GeoJsonAnalyzer::Coding::S2Point);
    dynamic_cast<GeoJsonAnalyzer*>(wkb_stream.get())->SetWkbInput(true);
    const auto via_wkb = AnalyzeGeo(*wkb_stream, PointWkb(2.0, 1.0));
    ASSERT_TRUE(via_wkb.has_value());

    EXPECT_EQ(via_json->terms, via_wkb->terms);
    EXPECT_EQ(via_json->store, via_wkb->store);
  }
}

TEST(GeoAnalyzerTests, geojson_terms_independent_of_coding) {
  constexpr std::string_view kPoint =
    R"({"type":"Point","coordinates":[2.0,1.0]})";
  auto source =
    MakeGeoJson(GeoJsonAnalyzer::Type::Point, GeoJsonAnalyzer::Coding::Source);
  auto s2point =
    MakeGeoJson(GeoJsonAnalyzer::Type::Point, GeoJsonAnalyzer::Coding::S2Point);

  const auto via_source = AnalyzeGeo(*source, kPoint);
  const auto via_s2 = AnalyzeGeo(*s2point, kPoint);
  ASSERT_TRUE(via_source.has_value());
  ASSERT_TRUE(via_s2.has_value());
  EXPECT_EQ(via_s2->terms, via_source->terms);
  EXPECT_TRUE(via_source->store.empty());
  EXPECT_FALSE(via_s2->store.empty());
}

TEST(GeoAnalyzerTests, geojson_centroid_of_point_stores_point_bytes) {
  constexpr std::string_view kPoint =
    R"({"type":"Point","coordinates":[2.0,1.0]})";
  auto point_stream =
    MakeGeoJson(GeoJsonAnalyzer::Type::Point, GeoJsonAnalyzer::Coding::S2Point);
  auto centroid_stream = MakeGeoJson(GeoJsonAnalyzer::Type::Centroid,
                                     GeoJsonAnalyzer::Coding::S2Point);

  const auto via_point = AnalyzeGeo(*point_stream, kPoint);
  const auto via_centroid = AnalyzeGeo(*centroid_stream, kPoint);
  ASSERT_TRUE(via_point.has_value());
  ASSERT_TRUE(via_centroid.has_value());
  EXPECT_EQ(via_point->store, via_centroid->store);
  EXPECT_EQ(via_point->terms, via_centroid->terms);
}

TEST(GeoAnalyzerTests, geojson_polygon_terms_match_region_oracle) {
  const std::vector<std::pair<double, double>> ring{
    {0.0, 0.0}, {2.0, 0.0}, {2.0, 2.0}, {0.0, 2.0}, {0.0, 0.0}};

  auto stream =
    MakeGeoJson(GeoJsonAnalyzer::Type::Shape, GeoJsonAnalyzer::Coding::Source);
  auto* geo = dynamic_cast<GeoJsonAnalyzer*>(stream.get());
  ASSERT_NE(nullptr, geo);

  const auto analysis = AnalyzeGeo(
    *stream,
    R"({"type":"Polygon","coordinates":[[[0.0,0.0],[2.0,0.0],[2.0,2.0],[0.0,2.0],[0.0,0.0]]]})");
  ASSERT_TRUE(analysis.has_value());
  ASSERT_FALSE(analysis->terms.empty());

  std::vector<S2Point> vertices;
  for (size_t i = 0; i + 1 < ring.size(); ++i) {
    vertices.push_back(
      S2LatLng::FromDegrees(ring[i].second, ring[i].first).ToPoint());
  }
  S2Polygon polygon{std::make_unique<S2Loop>(vertices)};
  S2RegionTermIndexer oracle{geo->options()};
  auto expected = oracle.GetIndexTerms(polygon, {});
  EXPECT_EQ(expected, analysis->terms);

  auto wkb_stream =
    MakeGeoJson(GeoJsonAnalyzer::Type::Shape, GeoJsonAnalyzer::Coding::Source);
  dynamic_cast<GeoJsonAnalyzer*>(wkb_stream.get())->SetWkbInput(true);
  const auto via_wkb = AnalyzeGeo(*wkb_stream, PolygonWkb(ring));
  ASSERT_TRUE(via_wkb.has_value());
  EXPECT_EQ(analysis->terms, via_wkb->terms);
}

TEST(GeoAnalyzerTests, geojson_point_type_rejects_non_points) {
  constexpr std::string_view kPolygon =
    R"({"type":"Polygon","coordinates":[[[0.0,0.0],[2.0,0.0],[2.0,2.0],[0.0,2.0],[0.0,0.0]]]})";
  const std::vector<std::pair<double, double>> ring{
    {0.0, 0.0}, {2.0, 0.0}, {2.0, 2.0}, {0.0, 2.0}, {0.0, 0.0}};

  auto stream =
    MakeGeoJson(GeoJsonAnalyzer::Type::Point, GeoJsonAnalyzer::Coding::S2Point);
  EXPECT_FALSE(AnalyzeGeo(*stream, kPolygon).has_value());

  auto wkb_stream =
    MakeGeoJson(GeoJsonAnalyzer::Type::Point, GeoJsonAnalyzer::Coding::S2Point);
  dynamic_cast<GeoJsonAnalyzer*>(wkb_stream.get())->SetWkbInput(true);
  EXPECT_FALSE(AnalyzeGeo(*wkb_stream, PolygonWkb(ring)).has_value());
}

TEST(GeoPointAnalyzerTests, array_and_object_forms_match_oracle) {
  auto array_stream = GeoPointAnalyzer::Make({});
  auto* geo = dynamic_cast<GeoPointAnalyzer*>(array_stream.get());
  ASSERT_NE(nullptr, geo);

  const auto via_array = AnalyzeGeo(*array_stream, "[1.0, 2.0]");
  ASSERT_TRUE(via_array.has_value());
  ASSERT_FALSE(via_array->terms.empty());
  EXPECT_TRUE(via_array->store.empty());

  S2RegionTermIndexer oracle{geo->options()};
  const auto point = S2LatLng::FromDegrees(1.0, 2.0).Normalized().ToPoint();
  EXPECT_EQ(oracle.GetIndexTerms(point, {}), via_array->terms);

  auto object_stream = GeoPointAnalyzer::Make(
    {.latitude = {"location", "lat"}, .longitude = {"location", "lng"}});
  const auto via_object =
    AnalyzeGeo(*object_stream, R"({"location":{"lat":1.0,"lng":2.0}})");
  ASSERT_TRUE(via_object.has_value());
  EXPECT_EQ(via_array->terms, via_object->terms);
}

TEST(GeoPointAnalyzerTests, wkb_point_matches_json_terms) {
  auto json_stream = GeoPointAnalyzer::Make({});
  const auto via_json = AnalyzeGeo(*json_stream, "[1.0, 2.0]");
  ASSERT_TRUE(via_json.has_value());

  auto wkb_stream = GeoPointAnalyzer::Make({});
  dynamic_cast<GeoPointAnalyzer*>(wkb_stream.get())->SetWkbInput(true);
  const auto via_wkb = AnalyzeGeo(*wkb_stream, PointWkb(2.0, 1.0));
  ASSERT_TRUE(via_wkb.has_value());
  EXPECT_EQ(via_json->terms, via_wkb->terms);
}

TEST(GeoPointAnalyzerTests, rejects_invalid_input) {
  const std::vector<std::pair<double, double>> ring{
    {0.0, 0.0}, {2.0, 0.0}, {2.0, 2.0}, {0.0, 2.0}, {0.0, 0.0}};

  auto stream = GeoPointAnalyzer::Make({});
  EXPECT_FALSE(AnalyzeGeo(*stream, "[1.0]").has_value());
  EXPECT_FALSE(AnalyzeGeo(*stream, "[1.0, 2.0, 3.0]").has_value());
  EXPECT_FALSE(AnalyzeGeo(*stream, "{}").has_value());

  auto wkb_stream = GeoPointAnalyzer::Make({});
  dynamic_cast<GeoPointAnalyzer*>(wkb_stream.get())->SetWkbInput(true);
  EXPECT_FALSE(AnalyzeGeo(*wkb_stream, PolygonWkb(ring)).has_value());

  auto mismatched = []() {
    return GeoPointAnalyzer::Make({.latitude = {"lat"}, .longitude = {}});
  };
  EXPECT_ANY_THROW(mismatched());
}
