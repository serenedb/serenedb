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

#include <s2/s2latlng.h>
#include <s2/s2region_term_indexer.h>
#include <s2/util/coding/coder.h>
#include <simdjson.h>

#include <string>

#include "basics/resource_manager.hpp"
#include "geo/coding.h"
#include "geo/shape_container.h"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs {

struct GeoFilterOptionsBase;

}  // namespace irs
namespace irs::analysis {

class GeoAnalyzer : private util::Noncopyable {
 public:
  static constexpr TokenTraits kTraits{
    .dense_pos = false, .offsets = false, .store = true};

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

  template<TokenLayout Layout>
  bool DoFill(std::string_view value, TokenEmitter& sink);

  // Level-1 input binding: a GEOMETRY column feeds WKB bytes, everything
  // else GeoJSON text. Set per column bind (instances are pooled).
  void SetWkbInput(bool wkb) noexcept { _wkb_input = wkb; }

  virtual ~GeoAnalyzer() = default;

  virtual bool reset(simdjson::ondemand::value json) = 0;

  virtual bool resetWKB(bytes_view wkb) = 0;

  virtual void prepare(GeoFilterOptionsBase& options) const = 0;

#ifdef SDB_GTEST
  const auto& options() const noexcept { return _indexer.options(); }
#endif

 protected:
  explicit GeoAnalyzer(const S2RegionTermIndexer::Options& options);
  void reset(std::vector<std::string>&& terms) noexcept;
  void reset() noexcept {
    _begin = _terms.data();
    _end = _begin;
  }

  S2RegionTermIndexer _indexer;
  bytes_view _store_value;

 private:
  template<TokenLayout Layout>
  void EmitTerms(TokenEmitter& sink);

  std::vector<std::string> _terms;
  const std::string* _begin{_terms.data()};
  const std::string* _end{_begin};
  simdjson::ondemand::parser _json_parser;
  std::string _json_buffer;
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

  TokenTraits Traits() const noexcept final { return kTraits; }

  using GeoAnalyzer::reset;
  bool reset(simdjson::ondemand::value json) final;
  bool resetWKB(bytes_view wkb) final;

  void prepare(GeoFilterOptionsBase& options) const final;

#ifdef SDB_GTEST
  const auto& latitude() const noexcept { return _latitude; }
  const auto& longitude() const noexcept { return _longitude; }
#endif

 private:
  bool ParsePoint(simdjson::ondemand::value json, S2LatLng& out) const;

  S2LatLng _point;
  bool _from_array;
  std::vector<std::string> _latitude;
  std::vector<std::string> _longitude;
};

/// The analyzer capable of breaking up a valid GeoJson input
/// into a set of tokens for further indexing.
class GeoJsonAnalyzer : public TypedTokenizer<GeoJsonAnalyzer>,
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

  TokenTraits Traits() const noexcept final { return kTraits; }

  // Effective coding this analyzer was configured with. Lets callers (e.g.
  // CREATE INDEX validation) decide whether the coding is compatible with a
  // given column type.
  Coding coding() const noexcept { return _coding; }

#ifdef SDB_GTEST
  auto shapeType() const noexcept { return _type; }
#endif

 protected:
  explicit GeoJsonAnalyzer(const Options& options);

  bool ResetImpl(simdjson::ondemand::value json,
                 sdb::geo::coding::Options options, Encoder* encoder);

  // Shared epilogue: given _shape already populated, compute geo terms and
  // publish them via GeoAnalyzer::reset(terms).
  void ComputeAndPublishTerms();

  virtual void StoreImpl() = 0;

  sdb::geo::ShapeContainer _shape;
  S2Point _centroid;
  std::vector<S2LatLng> _cache;
  Type _type;
  Coding _coding;
};

extern template class TypedTokenizer<GeoPointAnalyzer>;
extern template class TypedTokenizer<GeoJsonAnalyzer>;

}  // namespace irs::analysis
