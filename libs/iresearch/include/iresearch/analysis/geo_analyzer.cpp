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

#include "iresearch/analysis/geo_analyzer.hpp"

#include <absl/strings/str_cat.h>
#include <s2/s2latlng.h>
#include <s2/s2point_region.h>

#include <magic_enum/magic_enum.hpp>
#include <string>

#include "basics/down_cast.h"
#include "basics/exceptions.h"
#include "basics/log.h"
#include "basics/serializer.h"
#include "geo/geo_json.h"
#include "geo/geo_params.h"
#include "geo/wkb.h"
#include "iresearch/search/geo_filter.hpp"

namespace magic_enum {

template<>
constexpr customize::customize_t
customize::enum_name<irs::analysis::GeoJsonAnalyzer::Type>(
  irs::analysis::GeoJsonAnalyzer::Type value) noexcept {
  switch (value) {
    case irs::analysis::GeoJsonAnalyzer::Type::Shape:
      return "shape";
    case irs::analysis::GeoJsonAnalyzer::Type::Centroid:
      return "centroid";
    case irs::analysis::GeoJsonAnalyzer::Type::Point:
      return "point";
  }
  return invalid_tag;
}

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<irs::analysis::GeoJsonAnalyzer::Coding>(
  irs::analysis::GeoJsonAnalyzer::Coding value) noexcept {
  switch (value) {
    case irs::analysis::GeoJsonAnalyzer::Coding::S2Point:
      return "s2Point";
    case irs::analysis::GeoJsonAnalyzer::Coding::S2LatLngF64:
      return "s2LatLngF64";
    case irs::analysis::GeoJsonAnalyzer::Coding::S2LatLngU32:
      return "s2LatLngU32";
    case irs::analysis::GeoJsonAnalyzer::Coding::Source:
      return "source";
  }
  return invalid_tag;
}

}  // namespace magic_enum
namespace irs::analysis {
namespace {

using namespace sdb;
using namespace sdb::geo;

struct S2AnalyzerData {
  sdb::geo::coding::Options coding;
  Encoder encoder;
};

// Source coding force-includes the indexed source column and re-parses it at
// query time, so the analyzer writes no derived StoreAttr blob.
struct SourceAnalyzerData {};

template<typename Data>
class GeoJsonAnalyzerImpl final : public GeoJsonAnalyzer {
 public:
  explicit GeoJsonAnalyzerImpl(const Options& options)
    : GeoJsonAnalyzer{options} {
    if constexpr (kIsS2) {
      SDB_ASSERT(options.coding != Coding::Source);
      _data.coding =
        sdb::geo::coding::Options{std::to_underlying(options.coding)};
      // should be enough space for type + size or S2Point
      _data.encoder.Ensure(30);
    }
  }

  using GeoAnalyzer::reset;
  bool reset(simdjson::ondemand::value json) final {
    if constexpr (kIsS2) {
      _data.encoder.clear();
      if (!ResetImpl(json, _data.coding, &_data.encoder)) {
        return false;
      }
    } else {
      if (!ResetImpl(json, geo::coding::Options::Invalid, nullptr)) {
        return false;
      }
    }
    StoreImpl();
    return true;
  }

  bool resetWKB(bytes_view wkb) final {
    if constexpr (kIsS2) {
      // WKB-based ingest currently supports only S2 codings (S2Point /
      // S2PointShapeCompact / S2PointRegionCompact). LatLng codings
      // (S2LatLngF64, S2LatLngU32) need a per-shape-type walker that emits
      // LatLng-encoded bytes either fused with the WKB read or from the
      // already-built S2 objects; S2LatLngU32 additionally needs
      // pre-quantization. Callers must configure S2 coding before reaching
      // here.
      SDB_ASSERT(geo::coding::IsOptionsS2(_data.coding) &&
                 "LatLng coding is not supported by resetWKB; "
                 "use S2Point / S2PointShapeCompact / S2PointRegionCompact");
      _shape = {};
      const std::string_view bytes{reinterpret_cast<const char*>(wkb.data()),
                                   wkb.size()};
      if (!sdb::geo::ParseShapeWKB(bytes, _shape)) {
        return false;
      }
      if (_type == Type::Point &&
          _shape.type() != sdb::geo::ShapeContainer::Type::S2Point) {
        return false;
      }
      _data.encoder.clear();
      // Match what ResetImpl's ParseShape path writes into _data.encoder.
      // Centroid over a non-point shape skips serialization here; StoreImpl
      // then encodes the centroid into _data.encoder itself.
      const bool without_serialization =
        _type == Type::Centroid &&
        _shape.type() != sdb::geo::ShapeContainer::Type::S2Point;
      if (!without_serialization) {
        _shape.Encode(_data.encoder, _data.coding);
      }
      ComputeAndPublishTerms();
      StoreImpl();
      return true;
    } else {
      _shape = {};
      const std::string_view bytes{reinterpret_cast<const char*>(wkb.data()),
                                   wkb.size()};
      if (!sdb::geo::ParseShapeWKB(bytes, _shape)) {
        return false;
      }
      if (_type == Type::Point &&
          _shape.type() != sdb::geo::ShapeContainer::Type::S2Point) {
        return false;
      }
      ComputeAndPublishTerms();
      return true;
    }
  }

  void prepare(GeoFilterOptionsBase& options) const final {
    options.options = _indexer.options();
    if constexpr (kIsS2) {
      switch (_type) {
        case Type::Shape:
          options.stored = StoredType::S2Region;
          break;
        case Type::Point:
          options.stored = StoredType::S2Point;
          break;
        case Type::Centroid:
          options.stored = StoredType::S2Centroid;
          break;
      }
      options.coding = _data.coding;
    } else {
      options.stored = StoredType::Source;
    }
  }

  void StoreImpl() final;

 protected:
  static constexpr bool kIsS2 = std::is_same_v<Data, S2AnalyzerData>;
  Data _data;
};

}  // namespace

GeoAnalyzer::GeoAnalyzer(const S2RegionTermIndexer::Options& options)
  : _indexer{options} {}

bool GeoAnalyzer::next() noexcept {
  if (_begin >= _end) {
    return false;
  }
  auto& value = *_begin++;
  std::get<irs::TermAttr>(_attrs).value = {
    reinterpret_cast<const irs::byte_type*>(value.data()), value.size()};
  return true;
}

void GeoAnalyzer::reset(std::vector<std::string>&& terms) noexcept {
  _terms = std::move(terms);
  _begin = _terms.data();
  _end = _begin + _terms.size();
}

bool GeoAnalyzer::reset(std::string_view value) {
  _json_buffer.assign(value);
  _json_buffer.append(simdjson::SIMDJSON_PADDING, '\0');
  simdjson::padded_string_view padded_view{_json_buffer.data(), value.size(),
                                           _json_buffer.size()};
  simdjson::ondemand::document doc;
  if (_json_parser.iterate(padded_view).get(doc) != simdjson::SUCCESS) {
    return false;
  }
  simdjson::ondemand::value json;
  if (doc.get_value().get(json) != simdjson::SUCCESS) {
    return false;
  }
  return reset(json);
}

irs::analysis::Analyzer::ptr GeoPointAnalyzer::Make(Options opts) {
  opts.options.Validate("geo_point");
  if (opts.latitude.empty() != opts.longitude.empty()) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER,
              "geo_point: latitude and longitude must both be set or both "
              "empty");
  }
  return std::make_unique<GeoPointAnalyzer>(opts);
}

GeoPointAnalyzer::GeoPointAnalyzer(const Options& options)
  : GeoAnalyzer{S2Options(options.options, true)},
    _from_array{options.latitude.empty()},
    _latitude{options.latitude},
    _longitude{options.longitude} {
  SDB_ASSERT(_latitude.empty() == _longitude.empty());
}

bool GeoPointAnalyzer::reset(simdjson::ondemand::value json) {
  if (!ParsePoint(json, _point)) {
    return false;
  }
  GeoAnalyzer::reset(_indexer.GetIndexTerms(_point.ToPoint(), {}));
  return true;
}

bool GeoPointAnalyzer::resetWKB(bytes_view wkb) {
  // GeoPointAnalyzer accepts points only.
  sdb::geo::ShapeContainer shape;
  const std::string_view bytes{reinterpret_cast<const char*>(wkb.data()),
                               wkb.size()};
  if (!sdb::geo::ParseShapeWKB(bytes, shape)) {
    return false;
  }
  if (shape.type() != sdb::geo::ShapeContainer::Type::S2Point) {
    return false;
  }
  _point = S2LatLng{shape.centroid()};
  GeoAnalyzer::reset(_indexer.GetIndexTerms(_point.ToPoint(), {}));
  return true;
}

void GeoPointAnalyzer::prepare(GeoFilterOptionsBase& options) const {
  options.options = _indexer.options();
  options.stored = StoredType::Source;
  options.source_is_point = true;
  options.point_latitude = _latitude;
  options.point_longitude = _longitude;
}

bool GeoPointAnalyzer::ParsePoint(simdjson::ondemand::value json,
                                  S2LatLng& point) const {
  double lat, lng;
  if (_from_array) {
    simdjson::ondemand::array array;
    if (json.get_array().get(array) != simdjson::SUCCESS) {
      return false;
    }
    double values[2];
    size_t i = 0;
    for (auto element : array) {
      if (i == 2) [[unlikely]] {
        return false;
      }
      if (element.get_double().get(values[i]) != simdjson::SUCCESS)
        [[unlikely]] {
        return false;
      }
      ++i;
    }
    if (i != 2) [[unlikely]] {
      return false;
    }
    lat = values[0];
    lng = values[1];
  } else {
    simdjson::ondemand::object object;
    if (json.get_object().get(object) != simdjson::SUCCESS) {
      return false;
    }
    auto find = [&object](std::span<const std::string> path,
                          double& out) -> bool {
      if (path.size() == 1) {
        return object.find_field_unordered(path.front())
                 .get_double()
                 .get(out) == simdjson::SUCCESS;
      }
      simdjson::ondemand::value current;
      if (object.find_field_unordered(path.front()).get(current) !=
          simdjson::SUCCESS) {
        return false;
      }
      for (size_t i = 1; i + 1 < path.size(); ++i) {
        simdjson::ondemand::object inner;
        if (current.get_object().get(inner) != simdjson::SUCCESS) {
          return false;
        }
        if (inner.find_field_unordered(path[i]).get(current) !=
            simdjson::SUCCESS) {
          return false;
        }
      }
      simdjson::ondemand::object inner;
      if (current.get_object().get(inner) != simdjson::SUCCESS) {
        return false;
      }
      return inner.find_field_unordered(path.back()).get_double().get(out) ==
             simdjson::SUCCESS;
    };
    if (!find(_latitude, lat)) [[unlikely]] {
      return false;
    }
    if (!find(_longitude, lng)) [[unlikely]] {
      return false;
    }
  }
  point = S2LatLng::FromDegrees(lat, lng).Normalized();
  return true;
}

irs::analysis::Analyzer::ptr GeoJsonAnalyzer::Make(Options opts) {
  opts.options.Validate("geo_json");
  if (opts.coding == Coding::Source) {
    return std::make_unique<GeoJsonAnalyzerImpl<SourceAnalyzerData>>(opts);
  }
  return std::make_unique<GeoJsonAnalyzerImpl<S2AnalyzerData>>(opts);
}

GeoJsonAnalyzer::GeoJsonAnalyzer(const Options& options)
  : GeoAnalyzer{S2Options(options.options, options.type != Type::Shape)},
    _type{options.type},
    _coding{options.coding} {}

bool GeoJsonAnalyzer::ResetImpl(simdjson::ondemand::value json,
                                geo::coding::Options options,
                                Encoder* encoder) {
  // Centroid skips shape serialization: StoreImpl encodes the centroid point
  // instead. For a Point input the centroid equals the point, so the stored
  // bytes are identical to a full point serialization.
  const bool without_serialization = _type == Type::Centroid;
  if (_type != Type::Point) {
    if (!ParseShape<Parsing::GeoJson>(
          json, _shape, _cache,
          without_serialization ? geo::coding::Options::Invalid : options,
          without_serialization ? nullptr : encoder)) {
      return false;
    }
  } else if (!ParseShape<Parsing::OnlyPoint>(json, _shape, _cache, options,
                                             encoder)) {
    return false;
  }

  ComputeAndPublishTerms();
  return true;
}

void GeoJsonAnalyzer::ComputeAndPublishTerms() {
  // TODO(mbkkt) try to avoid allocations in append
  _centroid = _shape.centroid();
  std::vector<std::string> geo_terms;
  const auto type = _shape.type();
  if (_type == Type::Centroid || type == geo::ShapeContainer::Type::S2Point) {
    geo_terms = _indexer.GetIndexTerms(_centroid, {});
  } else {
    geo_terms = _indexer.GetIndexTerms(*_shape.region(), {});
    if (!_shape.contains(_centroid)) {
      auto terms = _indexer.GetIndexTerms(_centroid, {});
      geo_terms.insert(geo_terms.end(), std::make_move_iterator(terms.begin()),
                       std::make_move_iterator(terms.end()));
      // TODO(mbkkt) do we need terms deduplication?
    }
  }
  GeoAnalyzer::reset(std::move(geo_terms));
}

template<>
void GeoJsonAnalyzerImpl<SourceAnalyzerData>::StoreImpl() {}

template<>
void GeoJsonAnalyzerImpl<S2AnalyzerData>::StoreImpl() {
  if (_data.encoder.length() == 0) {
    SDB_ASSERT(_type == Type::Centroid);
    SDB_ASSERT(_data.coding != geo::coding::Options::Invalid);
    _data.encoder.put8(0);
    if (geo::coding::IsOptionsS2(_data.coding)) {
      geo::EncodePoint(_data.encoder, _centroid);
    } else {
      S2LatLng lat_lng{_centroid};
      geo::EncodeLatLng(_data.encoder, lat_lng, _data.coding);
    }
  }
  irs::bytes_view data{
    reinterpret_cast<const irs::byte_type*>(_data.encoder.base()),
    _data.encoder.length()};
  if (_type != Type::Shape) {
    // For points we do not need type
    data = data.substr(1);
  }
  auto* store = irs::GetMutable<StoreAttr>(this);
  SDB_ASSERT(store);
  store->value = data;
}

}  // namespace irs::analysis
