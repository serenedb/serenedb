////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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

#pragma once

#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>
#include <s2/s2region_coverer.h>
#include <s2/s2region_term_indexer.h>
#include <s2/util/coding/coder.h>
#include <simdjson.h>

#include <string>
#include <tuple>

#include "basics/noncopyable.hpp"
#include "geo/coding.h"
#include "geo/shape_container.h"
#include "iresearch/analysis/tokenizer.hpp"

namespace irs {

struct GeoFilterOptionsBase;

}  // namespace irs
namespace irs::analysis {

class GeoAnalyzer : private util::Noncopyable {
 public:
  static bool IsGeoAnalyzer(const Tokenizer& tokens) noexcept;

  // Resolves either of the two registered geo analyzers from a type-erased
  // tokenizer; callers guarantee IsGeoAnalyzer via catalog/column type.
  static GeoAnalyzer& Cast(Tokenizer& tokens) noexcept;
  static const GeoAnalyzer& Cast(const Tokenizer& tokens) noexcept {
    return Cast(const_cast<Tokenizer&>(tokens));
  }

  static GeoAnalyzer* TryCast(Tokenizer& tokens) noexcept {
    return IsGeoAnalyzer(tokens) ? &Cast(tokens) : nullptr;
  }

  auto PrepareBatch() const { return std::tuple{_wkb_input}; }

  template<TokenLayout Layout, bool Wkb>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

  // Level-1 input binding: a GEOMETRY column feeds WKB bytes, everything
  // else GeoJSON text. Set once when the column binding is established
  // (sink tokenizer-cache fill), reset by the pool via Unbind on lease
  // return, never mutated during Fill.
  void SetWkbInput(bool wkb) noexcept { _wkb_input = wkb; }

  virtual ~GeoAnalyzer() = default;

  virtual bool reset(simdjson::ondemand::value json) = 0;

  virtual bool resetWKB(duckdb::string_t wkb) = 0;

  virtual void prepare(GeoFilterOptionsBase& options) const = 0;

#ifdef SDB_GTEST
  const auto& options() const noexcept { return _indexer.options(); }
#endif

 protected:
  explicit GeoAnalyzer(const S2RegionTermIndexer::Options& options);

  // Ingest terms are staged as cells, not strings: EmitTerms materializes
  // each token straight into the sink, so no per-value term vector exists.
  void ClearStaged() noexcept {
    _covering.clear();
    _point_id = S2CellId::None();
  }
  void StagePoint(const S2Point& point) noexcept {
    _point_id = S2CellId{point};
  }
  void StageCovering(const S2Region& region) {
    _coverer.GetCovering(region, &_covering);
  }

  // Per-value store hook, run by DoFill after the terms: an analyzer that
  // derives a store blob pushes it into the sink here.
  virtual void Store(TokenSink& /*sink*/) {}

  S2RegionTermIndexer _indexer;

 private:
  template<TokenLayout Layout>
  void EmitTerms(TokenSink& sink);

  S2RegionCoverer _coverer;
  std::vector<S2CellId> _covering;
  S2CellId _point_id = S2CellId::None();
  simdjson::ondemand::parser _json_parser;
  std::unique_ptr<char[]> _json_buf;
  size_t _json_cap = 0;
  bool _wkb_input = false;
};

/// The analyzer capable of breaking up a valid geo point input
/// into a set of tokens for further indexing.
class GeoPointAnalyzer final : public TypedTokenizer<GeoPointAnalyzer>,
                               public GeoAnalyzer {
 public:
  struct Options {
    using Owner = GeoPointAnalyzer;
    sdb::geo::GeoOptions options;
    std::vector<std::string> latitude;
    std::vector<std::string> longitude;
  };
  static analysis::Tokenizer::ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept { return "geopoint"; }

  explicit GeoPointAnalyzer(const Options& options);

  using GeoAnalyzer::PrepareBatch;

  // The source column itself serves the filter (prepare() points it at the
  // lat/lng paths), so no derived store blob is ever produced.
  TokenTraits Traits() const noexcept final { return {}; }

  void Unbind() noexcept final { SetWkbInput(false); }

  bool reset(simdjson::ondemand::value json) final;
  bool resetWKB(duckdb::string_t wkb) final;

  void prepare(GeoFilterOptionsBase& options) const final;

#ifdef SDB_GTEST
  const auto& latitude() const noexcept { return _latitude; }
  const auto& longitude() const noexcept { return _longitude; }
#endif

 private:
  bool ParsePoint(simdjson::ondemand::value json, S2LatLng& out) const;

  bool _from_array;
  std::vector<std::string> _latitude;
  std::vector<std::string> _longitude;
};

/// The analyzer capable of breaking up a valid GeoJson input
/// into a set of tokens for further indexing.
class GeoJsonAnalyzer final : public TypedTokenizer<GeoJsonAnalyzer>,
                              public GeoAnalyzer {
 public:
  enum class Type : uint8_t {
    // analyzer accepts any valid GeoJson input
    // and produces tokens denoting an approximation for a given shape
    Shape = 0,
    // analyzer accepts any valid GeoJson shape
    // but produces tokens denoting a centroid of a given shape
    Centroid,
    // analyzer accepts points only
    Point,
  };

  enum class Coding : uint8_t {
    S2Point = std::to_underlying(sdb::geo::coding::Options::S2Point),
    S2LatLngF64 = std::to_underlying(sdb::geo::coding::Options::S2LatLngF64),
    S2LatLngU32 = std::to_underlying(sdb::geo::coding::Options::S2LatLngU32),
    Source,
  };

  struct Options {
    using Owner = GeoJsonAnalyzer;
    sdb::geo::GeoOptions options;
    Type type{Type::Shape};
    Coding coding{Coding::Source};
  };
  static analysis::Tokenizer::ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept { return "geojson"; }

  explicit GeoJsonAnalyzer(const Options& options);

  using GeoAnalyzer::PrepareBatch;

  // Source coding force-includes the indexed source column and re-parses it
  // at query time, so the analyzer writes no derived store blob.
  TokenTraits Traits() const noexcept final {
    return {
      .store = _coding != Coding::Source,
    };
  }

  void Unbind() noexcept final { SetWkbInput(false); }

  // Effective coding this analyzer was configured with. Lets callers (e.g.
  // CREATE INDEX validation) decide whether the coding is compatible with a
  // given column type.
  Coding coding() const noexcept { return _coding; }

  bool reset(simdjson::ondemand::value json) final;
  bool resetWKB(duckdb::string_t wkb) final;

  void prepare(GeoFilterOptionsBase& options) const final;

#ifdef SDB_GTEST
  auto shapeType() const noexcept { return _type; }
#endif

 private:
  bool ResetImpl(simdjson::ondemand::value json,
                 sdb::geo::coding::Options options, Encoder* encoder);

  // Shared epilogue: given _shape already populated, stage the cells whose
  // tokens EmitTerms will emit.
  void StageTerms();

  void Store(TokenSink& sink) final;

  sdb::geo::ShapeContainer _shape;
  S2Point _centroid;
  std::vector<S2LatLng> _cache;
  Encoder _encoder;
  sdb::geo::coding::Options _s2_coding{sdb::geo::coding::Options::Invalid};
  Type _type;
  Coding _coding;
};

extern template class TypedTokenizer<GeoPointAnalyzer>;
extern template class TypedTokenizer<GeoJsonAnalyzer>;

}  // namespace irs::analysis
