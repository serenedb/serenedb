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

#include <s2/s2cell_union.h>
#include <s2/s2latlng.h>
#include <s2/s2loop.h>
#include <s2/s2polygon.h>
#include <s2/s2region_coverer.h>
#include <s2/s2region_term_indexer.h>

#include <bit>
#include <cstring>
#include <format>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "geo/coding.h"
#include "geo/geo_terms.h"
#include "gtest/gtest.h"
#include "iresearch/analysis/geo_analyzer.hpp"
#include "iresearch/analysis/token_sinks.hpp"
#include "search/geo_test_helpers.hpp"
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

std::vector<std::string> CollectedTerms(
  const irs::ValueTokens<irs::TokenLayout::TermsPos>& tokens) {
  std::vector<std::string> out;
  out.reserve(tokens.terms().size());
  for (const auto& t : tokens.terms()) {
    out.emplace_back(t.GetData(), t.GetSize());
  }
  return out;
}

struct GeoAnalysis {
  std::vector<std::string> terms;
  std::string store;
};

std::optional<GeoAnalysis> AnalyzeGeo(irs::analysis::Tokenizer& tokenizer,
                                      std::string_view value) {
  irs::ValueAnalyzer value_analyzer;
  irs::ValueTokens<irs::TokenLayout::TermsPos> tokens{tokenizer.Traits()};
  if (!value_analyzer.Analyze(
        tokenizer,
        duckdb::string_t{value.data(), static_cast<uint32_t>(value.size())},
        tokens)) {
    return std::nullopt;
  }
  GeoAnalysis out;
  out.terms = CollectedTerms(tokens);
  out.store.assign(reinterpret_cast<const char*>(tokens.store().data()),
                   tokens.store().size());
  return out;
}

std::vector<std::vector<std::string>> FillGeoColumn(
  irs::analysis::Tokenizer& tokenizer, std::span<const std::string> values,
  irs::doc_id_t first_doc) {
  std::vector<duckdb::string_t> vals;
  vals.reserve(values.size());
  for (const auto& v : values) {
    vals.push_back(tests::ToStringT(v));
  }
  std::vector<std::vector<std::string>> got(values.size());
  const auto drain = [&](irs::TokenBatch& batch, irs::DocRuns runs) {
    uint32_t tok = 0;
    for (size_t r = 0; r < runs.size(); ++r) {
      const auto& run = runs[r];
      auto& bucket = got[run.doc - first_doc];
      for (uint32_t j = 0; j < run.ntokens; ++j, ++tok) {
        const auto& t = batch.terms[tok];
        bucket.emplace_back(t.GetData(), t.GetSize());
      }
    }
    ASSERT_EQ(batch.count, tok);
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPos, drain};
  tests::FillColumn(tokenizer, vals, first_doc, sink.writer, sink.layout);
  sink.writer.Finish();
  return got;
}

void ExpectColumnFillMatchesPerValue(irs::analysis::Tokenizer& fill_stream,
                                     irs::analysis::Tokenizer& value_stream,
                                     std::span<const std::string> values) {
  std::vector<std::vector<std::string>> expected(values.size());
  size_t accepted = 0;
  size_t rejected = 0;
  for (size_t i = 0; i < values.size(); ++i) {
    const auto analysis = AnalyzeGeo(value_stream, values[i]);
    if (!analysis.has_value()) {
      ++rejected;
      continue;
    }
    expected[i] = analysis->terms;
    ++accepted;
  }
  ASSERT_GT(accepted, 0);
  ASSERT_GT(rejected, 0);
  const auto got = FillGeoColumn(fill_stream, values, 1);
  ASSERT_EQ(expected.size(), got.size());
  for (size_t i = 0; i < values.size(); ++i) {
    SCOPED_TRACE(testing::Message() << "doc=" << i + 1);
    EXPECT_EQ(expected[i], got[i]);
  }
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
  EXPECT_EQ(irs::tests::BinaryTerms(oracle.GetIndexTerms(point, {})),
            analysis->terms);

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
  const auto expected =
    irs::tests::BinaryTerms(oracle.GetIndexTerms(polygon, {}));
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

TEST(GeoAnalyzerTests, geojson_column_fill_matches_per_value) {
  const std::vector<std::string> values = {
    R"({"type":"Point","coordinates":[2.0,1.0]})",
    R"({"type":"Point","coordinates":[-73.9,40.7]})",
    R"({"type":"Point","coordinates":[139.7,35.7]})",
    R"({"type":"Polygon","coordinates":[[[0.0,0.0],[2.0,0.0],[2.0,2.0],[0.0,2.0],[0.0,0.0]]]})",
    R"({"type":"Point","coordinates":[13.4,52.5]})",
    "{oops",
    "",
    R"({"type":"Point","coordinates":[151.2,-33.9]})"};

  for (const auto type :
       {GeoJsonAnalyzer::Type::Point, GeoJsonAnalyzer::Type::Shape,
        GeoJsonAnalyzer::Type::Centroid}) {
    for (const auto coding :
         {GeoJsonAnalyzer::Coding::Source, GeoJsonAnalyzer::Coding::S2Point}) {
      SCOPED_TRACE(testing::Message()
                   << "type=" << int(type) << " coding=" << int(coding));
      auto fill_stream = MakeGeoJson(type, coding);
      auto value_stream = MakeGeoJson(type, coding);
      ExpectColumnFillMatchesPerValue(*fill_stream, *value_stream, values);
    }
  }
}

TEST(GeoAnalyzerTests, geojson_wkb_column_fill_matches_per_value) {
  std::vector<std::string> values;
  for (const double lat : {-45.0, 0.0, 45.0}) {
    for (const double lng : {-90.0, 90.0}) {
      values.push_back(PointWkb(lng, lat));
    }
  }
  values.emplace_back("not-wkb");

  for (const auto type :
       {GeoJsonAnalyzer::Type::Point, GeoJsonAnalyzer::Type::Shape,
        GeoJsonAnalyzer::Type::Centroid}) {
    for (const auto coding :
         {GeoJsonAnalyzer::Coding::Source, GeoJsonAnalyzer::Coding::S2Point}) {
      SCOPED_TRACE(testing::Message()
                   << "type=" << int(type) << " coding=" << int(coding));
      auto fill_stream = MakeGeoJson(type, coding);
      auto value_stream = MakeGeoJson(type, coding);
      dynamic_cast<GeoJsonAnalyzer*>(fill_stream.get())->SetWkbInput(true);
      dynamic_cast<GeoJsonAnalyzer*>(value_stream.get())->SetWkbInput(true);
      ExpectColumnFillMatchesPerValue(*fill_stream, *value_stream, values);
    }
  }
}

TEST(GeoPointAnalyzerTests, geopoint_column_fill_matches_per_value) {
  const auto object_opts = [] {
    return GeoPointAnalyzer::Options{.latitude = {"location", "lat"},
                                     .longitude = {"location", "lng"}};
  };
  const std::vector<std::string> wkb_values = {
    PointWkb(2.0, 1.0), PointWkb(-73.9, 40.7), "not-wkb", PointWkb(13.4, 52.5)};

  {
    SCOPED_TRACE("array json");
    auto fill_stream = GeoPointAnalyzer::Make({});
    auto value_stream = GeoPointAnalyzer::Make({});
    const std::vector<std::string> values = {
      "[1.0, 2.0]", "[40.7, -73.9]", "[1.0]", "[52.5, 13.4]", "{}", ""};
    ExpectColumnFillMatchesPerValue(*fill_stream, *value_stream, values);
  }
  {
    SCOPED_TRACE("array wkb");
    auto fill_stream = GeoPointAnalyzer::Make({});
    auto value_stream = GeoPointAnalyzer::Make({});
    dynamic_cast<GeoPointAnalyzer*>(fill_stream.get())->SetWkbInput(true);
    dynamic_cast<GeoPointAnalyzer*>(value_stream.get())->SetWkbInput(true);
    ExpectColumnFillMatchesPerValue(*fill_stream, *value_stream, wkb_values);
  }
  {
    SCOPED_TRACE("object json");
    auto fill_stream = GeoPointAnalyzer::Make(object_opts());
    auto value_stream = GeoPointAnalyzer::Make(object_opts());
    const std::vector<std::string> values = {
      R"({"location":{"lat":1.0,"lng":2.0}})",
      R"({"location":{"lat":40.7,"lng":-73.9}})", R"({"location":{"lat":1.0}})",
      R"({"location":{"lat":52.5,"lng":13.4}})", "{oops"};
    ExpectColumnFillMatchesPerValue(*fill_stream, *value_stream, values);
  }
  {
    SCOPED_TRACE("object wkb");
    auto fill_stream = GeoPointAnalyzer::Make(object_opts());
    auto value_stream = GeoPointAnalyzer::Make(object_opts());
    dynamic_cast<GeoPointAnalyzer*>(fill_stream.get())->SetWkbInput(true);
    dynamic_cast<GeoPointAnalyzer*>(value_stream.get())->SetWkbInput(true);
    ExpectColumnFillMatchesPerValue(*fill_stream, *value_stream, wkb_values);
  }
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
  EXPECT_EQ(irs::tests::BinaryTerms(oracle.GetIndexTerms(point, {})),
            via_array->terms);

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

  auto mismatched = [] {
    return GeoPointAnalyzer::Make({.latitude = {"lat"}, .longitude = {}});
  };
  EXPECT_ANY_THROW(mismatched());
}

namespace {

struct LatLngCase {
  double lat;
  double lng;
};

std::vector<LatLngCase> SampleLatLngs(size_t n) {
  std::mt19937_64 rng{7};
  std::uniform_real_distribution<double> lat{-95.0, 95.0};
  std::uniform_real_distribution<double> lng{-200.0, 200.0};
  std::vector<LatLngCase> out;
  out.reserve(n + 8);
  for (size_t i = 0; i < n; ++i) {
    out.push_back({lat(rng), lng(rng)});
  }
  for (const auto [a, b] :
       {std::pair{0.0, 0.0}, std::pair{-0.0, -0.0}, std::pair{90.0, 180.0},
        std::pair{-90.0, -180.0}, std::pair{90.0, -180.0},
        std::pair{45.0, 179.9999999}, std::pair{0.0, 360.0},
        std::pair{1e-300, -1e-300}}) {
    out.push_back({a, b});
  }
  return out;
}

S2Point OraclePoint(const LatLngCase& c) {
  return S2LatLng::FromDegrees(c.lat, c.lng).Normalized().ToPoint();
}

std::string PointStoreBytes(const S2Point& point) {
  Encoder encoder;
  encoder.Ensure(30);
  sdb::geo::EncodePoint(encoder, point);
  return std::string(encoder.base(), encoder.length());
}

}  // namespace

TEST(GeoAnalyzerTests, json_point_sample_matches_oracle) {
  const auto cases = SampleLatLngs(512);
  for (const auto coding :
       {GeoJsonAnalyzer::Coding::Source, GeoJsonAnalyzer::Coding::S2Point,
        GeoJsonAnalyzer::Coding::S2LatLngF64,
        GeoJsonAnalyzer::Coding::S2LatLngU32}) {
    auto stream = MakeGeoJson(GeoJsonAnalyzer::Type::Point, coding);
    auto* geo = dynamic_cast<GeoJsonAnalyzer*>(stream.get());
    ASSERT_NE(nullptr, geo);
    S2RegionTermIndexer oracle{geo->options()};
    for (size_t i = 0; i < cases.size(); ++i) {
      const auto x = std::format("{}", cases[i].lng);
      const auto y = std::format("{}", cases[i].lat);
      std::string json;
      switch (i % 3) {
        case 0:
          json =
            std::format(R"({{"type":"Point","coordinates":[{},{}]}})", x, y);
          break;
        case 1:
          json = std::format(
            "{{ \"type\" : \"point\" ,\n\t\"coordinates\" : [ {} , {} ] }}\n",
            x, y);
          break;
        default:
          json = std::format("[{},{}]", x, y);
          break;
      }
      SCOPED_TRACE(testing::Message()
                   << "coding=" << int(coding) << " json=" << json);
      const auto analysis = AnalyzeGeo(*stream, json);
      ASSERT_TRUE(analysis.has_value());
      const auto point = OraclePoint(cases[i]);
      EXPECT_EQ(irs::tests::BinaryTerms(oracle.GetIndexTerms(point, {})),
                analysis->terms);
      if (coding == GeoJsonAnalyzer::Coding::S2Point) {
        EXPECT_EQ(PointStoreBytes(point), analysis->store);
      }
    }
  }
}

TEST(GeoAnalyzerTests, rejects_malformed_points) {
  auto stream =
    MakeGeoJson(GeoJsonAnalyzer::Type::Point, GeoJsonAnalyzer::Coding::S2Point);
  for (const char* bad :
       {"[01,2]", "[1.,2]", "[.5,2]", "[+1,2]", "[1e,2]", "[NaN,2]", "[1,2",
        "[1,2,3]", "[1]", "[1 2]", R"({"type":"Point","coordinates":[1,2])",
        R"({"type":"Point","coordinates":[1,2,3]})",
        R"({"type":"Point","coordinates":[1e400,2]})"}) {
    SCOPED_TRACE(bad);
    EXPECT_FALSE(AnalyzeGeo(*stream, bad).has_value());
  }
}

TEST(GeoPointAnalyzerTests, json_array_sample_matches_oracle) {
  const auto cases = SampleLatLngs(512);
  auto stream = GeoPointAnalyzer::Make({});
  auto* geo = dynamic_cast<GeoPointAnalyzer*>(stream.get());
  ASSERT_NE(nullptr, geo);
  S2RegionTermIndexer oracle{geo->options()};
  for (size_t i = 0; i < cases.size(); ++i) {
    const auto a = std::format("{}", cases[i].lat);
    const auto b = std::format("{}", cases[i].lng);
    const auto json = i % 2 == 0 ? std::format("[{},{}]", a, b)
                                 : std::format(" [ {} ,\t{} ]\n", a, b);
    SCOPED_TRACE(json);
    const auto analysis = AnalyzeGeo(*stream, json);
    ASSERT_TRUE(analysis.has_value());
    EXPECT_EQ(
      irs::tests::BinaryTerms(oracle.GetIndexTerms(OraclePoint(cases[i]), {})),
      analysis->terms);
  }
}

TEST(GeoAnalyzerTests, term_encoding_is_marker_plus_big_endian_cell) {
  const S2CellId cell{S2LatLng::FromDegrees(1.0, 2.0).ToPoint()};
  const auto plain = sdb::geo::terms::Term({}, cell, false, '$');
  const auto covering = sdb::geo::terms::Term({}, cell, true, '$');
  ASSERT_EQ(8, plain.size());
  ASSERT_EQ(9, covering.size());
  EXPECT_EQ('$', covering.front());
  EXPECT_EQ(plain, covering.substr(1));
  uint64_t be = 0;
  std::memcpy(&be, plain.data(), sizeof be);
  EXPECT_EQ(cell.id(), std::byteswap(be));
  EXPECT_EQ(static_cast<unsigned char>(cell.id() >> 56),
            static_cast<unsigned char>(plain[0]));
  const auto prefixed = sdb::geo::terms::Term("ab", cell, true, '#');
  EXPECT_EQ("ab#" + plain, prefixed);
}

TEST(GeoAnalyzerTests, query_terms_match_s2_oracle) {
  const std::vector<std::pair<double, double>> ring{
    {0.0, 0.0}, {2.0, 0.0}, {2.0, 2.0}, {0.0, 2.0}, {0.0, 0.0}};
  std::vector<S2Point> vertices;
  for (size_t i = 0; i + 1 < ring.size(); ++i) {
    vertices.push_back(
      S2LatLng::FromDegrees(ring[i].second, ring[i].first).ToPoint());
  }
  const S2Polygon polygon{std::make_unique<S2Loop>(vertices)};
  const auto point = S2LatLng::FromDegrees(1.0, 2.0).ToPoint();

  for (const auto& opts : {sdb::geo::GeoOptions{},
                           sdb::geo::GeoOptions{.max_cells = 8,
                                                .min_level = 2,
                                                .max_level = 20,
                                                .level_mod = 2,
                                                .optimize_for_space = true},
                           sdb::geo::GeoOptions{.max_cells = 30,
                                                .min_level = 6,
                                                .max_level = 24,
                                                .level_mod = 3,
                                                .optimize_for_space = false}}) {
    for (const bool points_only : {false, true}) {
      SCOPED_TRACE(testing::Message() << "cells=" << opts.max_cells
                                      << " mod=" << int(opts.level_mod)
                                      << " space=" << opts.optimize_for_space
                                      << " points_only=" << points_only);
      const auto options = sdb::geo::S2Options(opts, points_only);
      S2RegionTermIndexer oracle{options};
      S2RegionCoverer coverer{options};
      const auto ring_union = coverer.GetCovering(polygon).Difference(
        coverer.GetInteriorCovering(polygon));

      EXPECT_EQ(irs::tests::BinaryTerms(oracle.GetQueryTerms(point, {})),
                sdb::geo::terms::QueryTerms(options, point, {}));
      EXPECT_EQ(irs::tests::BinaryTerms(oracle.GetQueryTerms(polygon, {})),
                sdb::geo::terms::QueryTerms(options, polygon, {}));
      EXPECT_EQ(irs::tests::BinaryTerms(oracle.GetQueryTerms(ring_union, {})),
                sdb::geo::terms::QueryTerms(options, ring_union, {}));
      EXPECT_EQ(
        irs::tests::BinaryTerms(oracle.GetQueryTerms(point, "p:"), '$', "p:"),
        sdb::geo::terms::QueryTerms(options, point, "p:"));
    }
  }
}

TEST(GeoAnalyzerTests, wkb_point_sample_matches_oracle) {
  const auto cases = SampleLatLngs(2048);
  auto stream =
    MakeGeoJson(GeoJsonAnalyzer::Type::Point, GeoJsonAnalyzer::Coding::S2Point);
  auto* geo = dynamic_cast<GeoJsonAnalyzer*>(stream.get());
  ASSERT_NE(nullptr, geo);
  geo->SetWkbInput(true);
  S2RegionTermIndexer oracle{geo->options()};
  for (const auto& c : cases) {
    SCOPED_TRACE(testing::Message() << c.lat << "," << c.lng);
    const auto analysis = AnalyzeGeo(*stream, PointWkb(c.lng, c.lat));
    ASSERT_TRUE(analysis.has_value());
    const auto point = OraclePoint(c);
    EXPECT_EQ(irs::tests::BinaryTerms(oracle.GetIndexTerms(point, {})),
              analysis->terms);
    EXPECT_EQ(PointStoreBytes(point), analysis->store);
  }
}

TEST(GeoAnalyzerTests, covering_terms_match_region_oracle_across_options) {
  constexpr std::string_view kPolygon =
    R"({"type":"Polygon","coordinates":[[[0.0,0.0],[2.0,0.0],[2.0,2.0],[0.0,2.0],[0.0,0.0]]]})";
  const std::vector<std::pair<double, double>> ring{
    {0.0, 0.0}, {2.0, 0.0}, {2.0, 2.0}, {0.0, 2.0}, {0.0, 0.0}};
  std::vector<S2Point> vertices;
  for (size_t i = 0; i + 1 < ring.size(); ++i) {
    vertices.push_back(
      S2LatLng::FromDegrees(ring[i].second, ring[i].first).ToPoint());
  }
  const S2Polygon region{std::make_unique<S2Loop>(vertices)};

  for (const auto& opts : {sdb::geo::GeoOptions{.max_cells = 20,
                                                .min_level = 4,
                                                .max_level = 23,
                                                .level_mod = 1,
                                                .optimize_for_space = false},
                           sdb::geo::GeoOptions{.max_cells = 8,
                                                .min_level = 2,
                                                .max_level = 20,
                                                .level_mod = 2,
                                                .optimize_for_space = true},
                           sdb::geo::GeoOptions{.max_cells = 30,
                                                .min_level = 6,
                                                .max_level = 24,
                                                .level_mod = 3,
                                                .optimize_for_space = false}}) {
    SCOPED_TRACE(testing::Message()
                 << "cells=" << opts.max_cells << " mod=" << int(opts.level_mod)
                 << " space=" << opts.optimize_for_space);
    auto stream =
      GeoJsonAnalyzer::Make({.options = opts,
                             .type = GeoJsonAnalyzer::Type::Shape,
                             .coding = GeoJsonAnalyzer::Coding::Source});
    auto* geo = dynamic_cast<GeoJsonAnalyzer*>(stream.get());
    ASSERT_NE(nullptr, geo);
    const auto analysis = AnalyzeGeo(*stream, kPolygon);
    ASSERT_TRUE(analysis.has_value());
    ASSERT_FALSE(analysis->terms.empty());
    S2RegionTermIndexer oracle{geo->options()};
    EXPECT_EQ(irs::tests::BinaryTerms(oracle.GetIndexTerms(region, {})),
              analysis->terms);
  }
}

TEST(GeoAnalyzerTests, memory_usage_accounts_scratch) {
  auto json_stream =
    MakeGeoJson(GeoJsonAnalyzer::Type::Shape, GeoJsonAnalyzer::Coding::S2Point);
  const auto before = json_stream->MemoryUsage();
  const auto polygon = AnalyzeGeo(
    *json_stream,
    R"({"type":"Polygon","coordinates":[[[0.0,0.0],[2.0,0.0],[2.0,2.0],[0.0,2.0],[0.0,0.0]]]})");
  ASSERT_TRUE(polygon.has_value());
  ASSERT_FALSE(polygon->terms.empty());
  EXPECT_GT(json_stream->MemoryUsage(), 0);
  EXPECT_GT(json_stream->MemoryUsage(), before);

  auto point_stream = GeoPointAnalyzer::Make({});
  EXPECT_EQ(0, point_stream->MemoryUsage());
  ASSERT_TRUE(AnalyzeGeo(*point_stream, "[1.0, 2.0]").has_value());
  EXPECT_GT(point_stream->MemoryUsage(), 0);
}
