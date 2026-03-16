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
#include <vpack/builder.h>
#include <vpack/slice.h>

#include "basics/resource_manager.hpp"
#include "geo/coding.h"
#include "geo/shape_container.h"
#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs {

struct GeoFilterOptionsBase;

}

namespace irs::analysis {

class GeoAnalyzer : public irs::analysis::Analyzer,
                    private irs::util::Noncopyable {
 public:
  bool next() noexcept final;
  using irs::analysis::Analyzer::reset;

  virtual void prepare(GeoFilterOptionsBase& options) const = 0;

  irs::Attribute* GetMutable(irs::TypeInfo::type_id id) noexcept final {
    return irs::GetMutable(_attrs, id);
  }

#ifdef SDB_GTEST
  const auto& options() const noexcept { return _indexer.options(); }
#endif
  static void init();  // for registration in a static build

 protected:
  explicit GeoAnalyzer(const S2RegionTermIndexer::Options& options);
  void reset(std::vector<std::string>&& terms) noexcept;
  void reset() noexcept {
    _begin = _terms.data();
    _end = _begin;
  }

  S2RegionTermIndexer _indexer;

 private:
  using attributes = std::tuple<irs::IncAttr, irs::TermAttr>;

  std::vector<std::string> _terms;
  const std::string* _begin{_terms.data()};
  const std::string* _end{_begin};
  irs::OffsAttr _offset;
  attributes _attrs;
};

/// The analyzer capable of breaking up a valid geo point input
/// into a set of tokens for further indexing. Stores vpack.
class GeoPointAnalyzer final : public GeoAnalyzer {
 public:
  struct Options {
    sdb::geo::GeoOptions options;
    std::vector<std::string> latitude;
    std::vector<std::string> longitude;
  };

  static constexpr std::string_view type_name() noexcept { return "geopoint"; }
  static bool normalize(std::string_view args, std::string& out);
  static irs::analysis::Analyzer::ptr make(std::string_view args);

  // store point as [lng, lat] array to be GeoJSON compliant
  static irs::bytes_view store(irs::Tokenizer* ctx, vpack::Slice slice);

  explicit GeoPointAnalyzer(const Options& options);

  irs::TypeInfo::type_id type() const noexcept final {
    return irs::Type<GeoPointAnalyzer>::id();
  }

  bool reset(std::string_view value) final;

  void prepare(GeoFilterOptionsBase& options) const final;

#ifdef SDB_GTEST
  const auto& latitude() const noexcept { return _latitude; }
  const auto& longitude() const noexcept { return _longitude; }
#endif

 private:
  bool ParsePoint(vpack::Slice slice, S2LatLng& out) const;

  S2LatLng _point;
  bool _from_array;
  std::vector<std::string> _latitude;
  std::vector<std::string> _longitude;
  vpack::Builder _builder;
};

/// The analyzer capable of breaking up a valid GeoJson input
/// into a set of tokens for further indexing.
class GeoJsonAnalyzer : public GeoAnalyzer {
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
    VPack,
  };

  struct Options {
    sdb::geo::GeoOptions options;
    Type type{Type::Shape};
    // TODO(mbkkt) adjust tests to change default to S2LatLngF64
    Coding coding{Coding::VPack};
  };

  static constexpr std::string_view type_name() noexcept { return "geojson"; }
  static bool normalize(std::string_view args, std::string& out);
  static irs::analysis::Analyzer::ptr make(std::string_view args);
  static irs::bytes_view store(irs::Tokenizer* ctx, vpack::Slice slice);

  irs::TypeInfo::type_id type() const noexcept final {
    return irs::Type<GeoJsonAnalyzer>::id();
  }

#ifdef SDB_GTEST
  auto shapeType() const noexcept { return _type; }
#endif

 protected:
  explicit GeoJsonAnalyzer(const Options& options);

  bool ResetImpl(std::string_view value, sdb::geo::coding::Options options,
                 Encoder* encoder);

  virtual irs::bytes_view StoreImpl(irs::Tokenizer* ctx,
                                    vpack::Slice slice) = 0;

  sdb::geo::ShapeContainer _shape;
  S2Point _centroid;
  std::vector<S2LatLng> _cache;
  Type _type;
};

}  // namespace irs::analysis
