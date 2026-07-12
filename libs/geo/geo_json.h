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

#include <s2/s2point.h>
#include <simdjson.h>

#include <cstdint>
#include <string_view>
#include <vector>

#include "basics/exceptions.h"
#include "geo/coding.h"

class S2LatLng;
class S2Loop;
class S2Polyline;
class S2Polygon;
class Encoder;

namespace sdb::geo {

class S2MultiPointRegion;
class S2MultiPolylineRegion;

class ShapeContainer;

/// Simple GeoJson parser should be more or less forgiving
/// and complies with most of https://tools.ietf.org/html/rfc7946
namespace json {
namespace fields {

inline constexpr std::string_view kType = "type";
inline constexpr std::string_view kCoordinates = "coordinates";

}  // namespace fields

enum class Type : uint8_t {
  Unknown = 0,
  Point,
  Linestring,
  Polygon,
  MultiPoint,
  MultiLinestring,
  MultiPolygon,
  // TODO(mbkkt) we don't support GeometryCollection so it's a having it in Type
  //  a very little slowdown parsing, but provide better error.
  //
  //  Also I think we can support it, because for all operations with geometry
  //  we use S2ShapeIndex and it support construction from collection of
  //  S2Region so I think it's nice feature.
  GeometryCollection,
};

/// About without validation, it should be used when we parse data from our
/// indexes. Because this checks very expensive and we don't want to make it in
/// every query. Instead of this we store in our indexes only valid data.
/// @note
/// In other words I make assumption if Validation is false all parsing
/// is successful.
/// @note
/// In Maintainer mode under x86 linux Validation is false,
/// use same code as Validation is true. But make SDB_ASSERT that result is ok.
/// @note
/// For all functions without Validation parameter it's implicitly equal true.
/// @note
/// For all functions without geoJson parameter it's implicitly equal true.

/// Expects an GeoJson type:
///
/// {
///   "type": "..."
/// }
/// type is case-insensitive
Type ParseType(simdjson::ondemand::object& object) noexcept;

/// Expects an GeoJson Point:
///
/// https://www.rfc-editor.org/rfc/rfc7946#section-3.1.2
/// {
///   "type": "Point",
///   "coordinates": [lon, lat]
void ParsePoint(simdjson::ondemand::value json, S2LatLng& region);

/// Convenience function to build a region from a GeoJson type.
void ParseRegion(simdjson::ondemand::value json, ShapeContainer& region);

template<bool Valid = true>
void ParseRegion(simdjson::ondemand::value json, ShapeContainer& region,
                 std::vector<S2LatLng>& cache,
                 coding::Options options = coding::Options::Invalid,
                 Encoder* encoder = nullptr);

template<bool Valid = true>
void ParseCoordinates(simdjson::ondemand::value json, ShapeContainer& region,
                      bool geo_json,
                      coding::Options options = coding::Options::Invalid,
                      Encoder* encoder = nullptr);

}  // namespace json
}  // namespace sdb::geo
