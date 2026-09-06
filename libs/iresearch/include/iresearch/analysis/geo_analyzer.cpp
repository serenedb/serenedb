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

#include <absl/base/internal/endian.h>
#include <s2/s2latlng.h>

#include <bit>
#include <cmath>
#include <cstring>
#include <memory>
#include <string>

#include "basics/down_cast.h"
#include "geo/geo_json.h"
#include "geo/geo_params.h"
#include "geo/geo_terms.h"
#include "geo/wkb.h"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/search/geo_filter.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

using namespace sdb;
using namespace sdb::geo;

namespace {

IRS_FORCE_INLINE S2LatLng NormalizedLatLng(double lat_deg,
                                           double lng_deg) noexcept {
  const auto lat_lng = S2LatLng::FromDegrees(lat_deg, lng_deg);
  if (std::fabs(lat_lng.lat().radians()) <= M_PI_2 &&
      std::fabs(lat_lng.lng().radians()) <= M_PI) [[likely]] {
    return lat_lng;
  }
  return lat_lng.Normalized();
}

bool ParseWkbPoint(duckdb::string_t wkb, S2LatLng& out) noexcept {
  constexpr size_t kWkbPointSize = 1 + 4 + 8 + 8;
  const char* const data = wkb.GetData();
  if (wkb.GetSize() != kWkbPointSize || static_cast<byte_type>(data[0]) != 1) {
    return false;
  }
  if (absl::little_endian::Load32(data + 1) != 1) {
    return false;
  }
  const double lng = absl::little_endian::Load<double>(data + 5);
  const double lat = absl::little_endian::Load<double>(data + 13);
  out = NormalizedLatLng(lat, lng);
  return true;
}

}  // namespace

GeoAnalyzer::GeoAnalyzer(const S2RegionTermIndexer::Options& options)
  : _options{options} {
  *_coverer.mutable_options() = options;
}

bool GeoAnalyzer::IsGeoAnalyzer(const Tokenizer& tokens) noexcept {
  return tokens.type() == irs::Type<GeoPointAnalyzer>::id() ||
         tokens.type() == irs::Type<GeoJsonAnalyzer>::id();
}

GeoAnalyzer& GeoAnalyzer::Cast(Tokenizer& tokens) noexcept {
  if (tokens.type() == irs::Type<GeoPointAnalyzer>::id()) {
    return sdb::basics::downCast<GeoPointAnalyzer>(tokens);
  }
  SDB_ASSERT(tokens.type() == irs::Type<GeoJsonAnalyzer>::id());
  return sdb::basics::downCast<GeoJsonAnalyzer>(tokens);
}

template<TokenLayout Layout>
void GeoAnalyzer::EmitTerms(TokenSink& sink) {
  const auto& opts = _options;
  const auto marker = static_cast<byte_type>(opts.marker_character());
  const auto emit = [&](S2CellId id, bool covering) IRS_FORCE_INLINE {
    const uint64_t be = std::byteswap(id.id());
    const uint32_t prefix = covering ? 1 : 0;
    sink.Emit<Layout>(sizeof be + prefix, [&](byte_type* mem) IRS_FORCE_INLINE {
      mem[0] = marker;
      std::memcpy(mem + prefix, &be, sizeof be);
      return sizeof be + prefix;
    });
  };
  if (!_covering.empty()) {
    SDB_ASSERT(!opts.index_contains_points_only());
    S2CellId prev_id = S2CellId::None();
    const int true_max_level = opts.true_max_level();
    for (const S2CellId id : _covering) {
      const int cell_level = id.level();
      if (cell_level < true_max_level) {
        emit(id, true);
      }
      if (cell_level == true_max_level || !opts.optimize_for_space()) {
        emit(id, false);
      }
      for (int level = cell_level - opts.level_mod(); level >= opts.min_level();
           level -= opts.level_mod()) {
        const S2CellId ancestor_id = id.parent(level);
        if (prev_id != S2CellId::None() && prev_id.level() > level &&
            prev_id.parent(level) == ancestor_id) {
          break;
        }
        emit(ancestor_id, false);
      }
      prev_id = id;
    }
  }
  if (_point_id != S2CellId::None()) {
    for (int level = opts.min_level(); level <= opts.max_level();
         level += opts.level_mod()) {
      emit(_point_id.parent(level), false);
    }
  }
}

template<TokenLayout Layout, bool Wkb>
bool GeoAnalyzer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  if constexpr (Wkb) {
    if (!resetWKB(raw)) {
      return false;
    }
  } else {
    const size_t size = raw.GetSize();
    const size_t needed = size + simdjson::SIMDJSON_PADDING;
    if (needed > _json_cap) {
      _json_buf = std::make_unique_for_overwrite<char[]>(needed);
      _json_cap = needed;
    }
    std::memcpy(_json_buf.get(), raw.GetData(), size);
    simdjson::padded_string_view padded_view{_json_buf.get(), size, _json_cap};
    simdjson::ondemand::document doc;
    if (_json_parser.iterate(padded_view).get(doc) != simdjson::SUCCESS) {
      return false;
    }
    simdjson::ondemand::value json;
    if (doc.get_value().get(json) != simdjson::SUCCESS) {
      return false;
    }
    if (!reset(json)) {
      return false;
    }
  }
  EmitTerms<Layout>(sink);
  Store(sink);
  return true;
}

template class TypedTokenizer<GeoPointAnalyzer>;
template class TypedTokenizer<GeoJsonAnalyzer>;

irs::analysis::Tokenizer::ptr GeoPointAnalyzer::Make(Options opts) {
  opts.options.Validate("geo_point");
  if (opts.latitude.empty() != opts.longitude.empty()) {
    THROW_SQL_ERROR(
      ERR_MSG("geo_point: latitude and longitude must both be set or both "
              "empty"));
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
  S2LatLng point;
  if (!ParsePoint(json, point)) {
    return false;
  }
  RestagePoint(point.ToPoint());
  return true;
}

bool GeoPointAnalyzer::resetWKB(duckdb::string_t wkb) {
  if (S2LatLng ll; ParseWkbPoint(wkb, ll)) {
    RestagePoint(ll.ToPoint());
    return true;
  }
  sdb::geo::ShapeContainer shape;
  if (!sdb::geo::ParseShapeWKB({wkb.GetData(), wkb.GetSize()}, shape)) {
    return false;
  }
  if (shape.type() != sdb::geo::ShapeContainer::Type::S2Point) {
    return false;
  }
  RestagePoint(S2LatLng{shape.centroid()}.ToPoint());
  return true;
}

void GeoPointAnalyzer::prepare(GeoFilterOptionsBase& options) const {
  options.options = _options;
  options.stored = StoredType::Source;
  options.source_is_point = true;
  options.point_latitude = _latitude;
  options.point_longitude = _longitude;
}

bool GeoPointAnalyzer::FindDouble(simdjson::ondemand::object& object,
                                  std::span<const std::string> path,
                                  double& out) {
  if (path.size() == 1) {
    return object.find_field_unordered(path.front()).get_double().get(out) ==
           simdjson::SUCCESS;
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
    if (inner.find_field_unordered(path[i]).get(current) != simdjson::SUCCESS) {
      return false;
    }
  }
  simdjson::ondemand::object inner;
  if (current.get_object().get(inner) != simdjson::SUCCESS) {
    return false;
  }
  return inner.find_field_unordered(path.back()).get_double().get(out) ==
         simdjson::SUCCESS;
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
    if (!FindDouble(object, _latitude, lat)) [[unlikely]] {
      return false;
    }
    if (!FindDouble(object, _longitude, lng)) [[unlikely]] {
      return false;
    }
  }
  point = NormalizedLatLng(lat, lng);
  return true;
}

irs::analysis::Tokenizer::ptr GeoJsonAnalyzer::Make(Options opts) {
  opts.options.Validate("geo_json");
  return std::make_unique<GeoJsonAnalyzer>(opts);
}

GeoJsonAnalyzer::GeoJsonAnalyzer(const Options& options)
  : GeoAnalyzer{S2Options(options.options, options.type != Type::Shape)},
    _type{options.type},
    _coding{options.coding} {
  if (_coding != Coding::Source) {
    _s2_coding = sdb::geo::coding::Options{std::to_underlying(_coding)};
    _encoder.Ensure(30);
  }
}

bool GeoJsonAnalyzer::reset(simdjson::ondemand::value json) {
  _encoder.clear();
  Encoder* encoder = SerializesShape() ? &_encoder : nullptr;
  const auto coding = encoder ? _s2_coding : geo::coding::Options::Invalid;
  const bool parsed =
    _type == Type::Point
      ? ParseShape<Parsing::OnlyPoint>(json, _shape, _cache, coding, encoder)
      : ParseShape<Parsing::GeoJson>(json, _shape, _cache, coding, encoder);
  if (!parsed) {
    return false;
  }
  StageTerms();
  return true;
}

bool GeoJsonAnalyzer::resetWKB(duckdb::string_t wkb) {
  SDB_ASSERT(_coding == Coding::Source || geo::coding::IsOptionsS2(_s2_coding),
             "LatLng coding is not supported by resetWKB; "
             "use S2Point / S2PointShapeCompact / S2PointRegionCompact");
  _encoder.clear();
  if (S2LatLng ll; ParseWkbPoint(wkb, ll)) {
    _centroid = ll.ToPoint();
    RestagePoint(_centroid);
    return true;
  }
  if (!sdb::geo::ParseShapeWKB({wkb.GetData(), wkb.GetSize()}, _shape)) {
    return false;
  }
  if (_type == Type::Point &&
      _shape.type() != sdb::geo::ShapeContainer::Type::S2Point) {
    return false;
  }
  if (SerializesShape()) {
    _shape.Encode(_encoder, _s2_coding);
  }
  StageTerms();
  return true;
}

void GeoJsonAnalyzer::prepare(GeoFilterOptionsBase& options) const {
  options.options = _options;
  if (_coding == Coding::Source) {
    options.stored = StoredType::Source;
    return;
  }
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
  options.coding = _s2_coding;
}

void GeoJsonAnalyzer::StageTerms() {
  ClearStaged();
  _centroid = _shape.centroid();
  if (_type == Type::Centroid ||
      _shape.type() == geo::ShapeContainer::Type::S2Point) {
    StagePoint(_centroid);
  } else {
    StageCovering(*_shape.region());
    if (!_shape.contains(_centroid)) {
      StagePoint(_centroid);
    }
  }
}

void GeoJsonAnalyzer::Store(TokenSink& sink) {
  if (_coding == Coding::Source) {
    return;
  }
  if (_encoder.length() == 0) {
    _encoder.put8(geo::coding::ToTag(geo::coding::Type::Point, _s2_coding));
    if (geo::coding::IsOptionsS2(_s2_coding)) {
      geo::EncodePoint(_encoder, _centroid);
    } else {
      S2LatLng lat_lng{_centroid};
      geo::EncodeLatLng(_encoder, lat_lng, _s2_coding);
    }
  }
  irs::bytes_view data{reinterpret_cast<const irs::byte_type*>(_encoder.base()),
                       _encoder.length()};
  if (_type != Type::Shape) {
    data = data.substr(1);
  }
  sink.Store(data);
}

}  // namespace irs::analysis
