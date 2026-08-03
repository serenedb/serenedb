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
#include <cstring>
#include <memory>
#include <string>

#include "basics/down_cast.h"
#include "geo/geo_json.h"
#include "geo/geo_params.h"
#include "geo/wkb.h"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/search/geo_filter.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

using namespace sdb;
using namespace sdb::geo;

namespace {

// A 2D little-endian WKB point, the dominant GEOMETRY ingest shape: decoded
// inline so point values skip ParseShapeWKB's ShapeContainer and its
// per-value S2PointRegion allocation. Anything else (big-endian, other
// geometry types, EWKB flags) falls through to the general parser.
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
  out = S2LatLng::FromDegrees(lat, lng).Normalized();
  return true;
}

}  // namespace

GeoAnalyzer::GeoAnalyzer(const S2RegionTermIndexer::Options& options)
  : _indexer{options} {
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

// Replicates S2RegionTermIndexer::GetIndexTerms term-by-term (the covering
// walk with its ancestor-dedup break, then the plain level loop for a point),
// emitting each token straight into the sink; equality with the indexer is
// pinned by the oracle tests.
template<TokenLayout Layout>
void GeoAnalyzer::EmitTerms(TokenSink& sink) {
  const auto& opts = _indexer.options();
  const auto emit = [&](S2CellId id, bool covering) {
    // S2CellId::ToToken without the std::string: hex of the id with trailing
    // zero nibbles stripped (valid cells are never 0, so no "X" case).
    const uint64_t v = id.id();
    SDB_ASSERT(v != 0);
    constexpr char kHexDigits[] = "0123456789abcdef";
    const auto len = 16 - static_cast<size_t>(std::countr_zero(v) >> 2);
    sink.Emit<Layout>(
      len + 1,
      [&](byte_type* mem) IRS_FORCE_INLINE {
        size_t n = 0;
        if (covering) {
          mem[n++] = static_cast<byte_type>(opts.marker_character());
        }
        for (size_t j = 0; j < len; ++j) {
          mem[n + j] = static_cast<byte_type>(kHexDigits[(v >> (60 - 4 * j)) & 0xF]);
        }
        return static_cast<uint32_t>(n + len);
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
    // simdjson requires SIMDJSON_PADDING readable (not zeroed) bytes past the
    // value; a grow-only scratch keeps the copy to one memcpy per value.
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
  ClearStaged();
  StagePoint(point.ToPoint());
  return true;
}

bool GeoPointAnalyzer::resetWKB(duckdb::string_t wkb) {
  if (S2LatLng point; ParseWkbPoint(wkb, point)) {
    ClearStaged();
    StagePoint(point.ToPoint());
    return true;
  }
  // GeoPointAnalyzer accepts points only.
  sdb::geo::ShapeContainer shape;
  if (!sdb::geo::ParseShapeWKB({wkb.GetData(), wkb.GetSize()}, shape)) {
    return false;
  }
  if (shape.type() != sdb::geo::ShapeContainer::Type::S2Point) {
    return false;
  }
  const S2LatLng point{shape.centroid()};
  ClearStaged();
  StagePoint(point.ToPoint());
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
    // should be enough space for type + size or S2Point
    _encoder.Ensure(30);
  }
}

bool GeoJsonAnalyzer::reset(simdjson::ondemand::value json) {
  Encoder* encoder = nullptr;
  auto coding = geo::coding::Options::Invalid;
  if (_coding != Coding::Source) {
    _encoder.clear();
    encoder = &_encoder;
    coding = _s2_coding;
  }
  return ResetImpl(json, coding, encoder);
}

bool GeoJsonAnalyzer::resetWKB(duckdb::string_t wkb) {
  // WKB-based ingest currently supports only S2 codings (S2Point /
  // S2PointShapeCompact / S2PointRegionCompact). LatLng codings
  // (S2LatLngF64, S2LatLngU32) need a per-shape-type walker that emits
  // LatLng-encoded bytes either fused with the WKB read or from the
  // already-built S2 objects; S2LatLngU32 additionally needs
  // pre-quantization. Callers must configure S2 coding before reaching
  // here.
  SDB_ASSERT(_coding == Coding::Source || geo::coding::IsOptionsS2(_s2_coding),
             "LatLng coding is not supported by resetWKB; "
             "use S2Point / S2PointShapeCompact / S2PointRegionCompact");
  if (S2LatLng ll; ParseWkbPoint(wkb, ll)) {
    const auto point = ll.ToPoint();
    if (_coding != Coding::Source) {
      _encoder.clear();
      _encoder.Ensure(sizeof(uint8_t) + geo::coding::ToSize(_s2_coding));
      _encoder.put8(geo::coding::ToTag(geo::coding::Type::Point, _s2_coding));
      geo::EncodePoint(_encoder, point);
    }
    ClearStaged();
    _centroid = point;
    StagePoint(point);
    return true;
  }
  _shape = {};
  if (!sdb::geo::ParseShapeWKB({wkb.GetData(), wkb.GetSize()}, _shape)) {
    return false;
  }
  if (_type == Type::Point &&
      _shape.type() != sdb::geo::ShapeContainer::Type::S2Point) {
    return false;
  }
  if (_coding != Coding::Source) {
    _encoder.clear();
    // Match what ResetImpl's ParseShape path writes into _encoder.
    // Centroid over a non-point shape skips serialization here; StoreImpl
    // then encodes the centroid into _encoder itself.
    const bool without_serialization =
      _type == Type::Centroid &&
      _shape.type() != sdb::geo::ShapeContainer::Type::S2Point;
    if (!without_serialization) {
      _shape.Encode(_encoder, _s2_coding);
    }
  }
  StageTerms();
  return true;
}

void GeoJsonAnalyzer::prepare(GeoFilterOptionsBase& options) const {
  options.options = _indexer.options();
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

  StageTerms();
  return true;
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
      // The centroid's ancestor terms can duplicate covering ancestors;
      // duplicates collapse to one (term, doc) posting downstream (geo
      // fields carry no freq), so no dedup pass is needed.
      StagePoint(_centroid);
    }
  }
}

void GeoJsonAnalyzer::Store(TokenSink& sink) {
  if (_coding == Coding::Source) {
    // Source coding force-includes the indexed source column and re-parses
    // it at query time, so the analyzer writes no derived store blob.
    return;
  }
  if (_encoder.length() == 0) {
    SDB_ASSERT(_type == Type::Centroid);
    SDB_ASSERT(_s2_coding != geo::coding::Options::Invalid);
    _encoder.put8(0);
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
    // For points we do not need type
    data = data.substr(1);
  }
  sink.Store(data);
}

}  // namespace irs::analysis
