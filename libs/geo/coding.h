////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2022 ArangoDB GmbH, Cologne, Germany
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

#include <s2/s2region_term_indexer.h>
#include <s2/s2shape.h>

#include <cstdint>
#include <span>
#include <vector>

#include "vpack/builder.h"
#include "vpack/slice.h"

class S2Polyline;
class S2Polygon;

namespace sdb::geo {

class ShapeContainer;

namespace coding {
// Numbers persistent to store! Do not change!

enum class Type : uint8_t {
  Point = 0,
  Polyline = 1,
  Polygon = 2,
  MultiPoint = 3,
  MultiPolyline = 4,
};

enum class Options : uint8_t {
  // Use S2Point representation, it's completely lossless
  S2Point = 0,
  S2PointShapeCompact = 1,
  S2PointRegionCompact = 2,
  // Use S2LatLng representation, lossy
  S2LatLngF64 = 3,
  S2LatLngU32 = 4,
  // To use as invalid value in template
  Invalid = 0xFF,
};

constexpr bool IsOptionsS2(Options options) noexcept {
  return std::to_underlying(options) < 3;
}

constexpr bool IsSameLoss(Options lhs, Options rhs) noexcept {
  const auto l = std::to_underlying(lhs);
  const auto r = std::to_underlying(rhs);
  return (l != 4) == (r != 4);
}

constexpr auto ToTag(Type t, Options o) noexcept {
  return static_cast<uint8_t>(static_cast<uint32_t>(t) << 5U |
                              static_cast<uint32_t>(o));
}

template<Type T, Options O = Options::S2Point>
consteval auto ToTag() noexcept {
  static_assert(
    std::to_underlying(T) < 8,
    "less because we want to use 0xE0 as special value to extend tag format");
  static_assert(
    std::to_underlying(O) < 31,
    "less because we want to use 0x1F as special value to extend tag format");
  return ToTag(T, O);
}

constexpr auto ToType(uint8_t tag) noexcept {
  return static_cast<uint8_t>(static_cast<uint32_t>(tag) & 0xE0);
}

constexpr auto ToPoint(uint8_t tag) noexcept {
  return static_cast<uint32_t>(tag) & 0x1F;
}

constexpr size_t ToSize(Options options) noexcept {
  switch (options) {
    case Options::S2LatLngU32:
      return 2 * sizeof(uint32_t);
    case Options::S2LatLngF64:
      return 2 * sizeof(double);
    case Options::S2Point:
      return 3 * sizeof(double);
    default:
      return 0;
  }
}

}  // namespace coding

void ToLatLngU32(S2LatLng& lat_lng) noexcept;
void EncodeLatLng(Encoder& encoder, S2LatLng& lat_lng,
                  coding::Options options) noexcept;
void EncodePoint(Encoder& encoder, const S2Point& point) noexcept;

void ToLatLngU32(std::span<S2LatLng> vertices) noexcept;
void EncodeVertices(Encoder& encode, std::span<const S2Point> vertices);
void EncodeVertices(Encoder& encode, std::span<S2LatLng> vertices,
                    coding::Options options);
bool DecodeVertices(Decoder& decoder, std::span<S2Point> vertices, uint8_t tag);

bool DecodePoint(Decoder& decoder, S2Point& point, uint8_t* tag);
bool DecodePoint(Decoder& decoder, S2Point& point, uint8_t tag);

void EncodePolyline(Encoder& encoder, const S2Polyline& polyline,
                    coding::Options options);
bool DecodePolyline(Decoder& decoder, S2Polyline& polyline, uint8_t tag,
                    std::vector<S2Point>& cache);

void EncodePolygon(Encoder& encoder, const S2Polygon& polygon,
                   coding::Options options);
bool DecodePolygon(Decoder& decoder, S2Polygon& polygon, uint8_t tag,
                   std::vector<S2Point>& cache);

struct GeoOptions {
  // TODO(mbkkt) different maxCells can be set on every insertion/querying
  int32_t max_cells{20};
  int32_t min_level{4};
  int32_t max_level{23};  // ~1m
  int8_t level_mod{1};
  bool optimize_for_space{false};

  sdb::Result Validate() const noexcept;
};

inline S2RegionTermIndexer::Options S2Options(const GeoOptions& opts,
                                              bool points_only) {
  S2RegionTermIndexer::Options s2opts;
  s2opts.set_max_cells(opts.max_cells);
  s2opts.set_min_level(opts.min_level);
  s2opts.set_max_level(opts.max_level);
  s2opts.set_level_mod(opts.level_mod);
  s2opts.set_optimize_for_space(opts.optimize_for_space);
  s2opts.set_index_contains_points_only(points_only);

  return s2opts;
}

enum class Parsing : uint8_t {
  FromIndex = 0,
  OnlyPoint,
  GeoJson,
};

template<Parsing P>
bool ParseShape(vpack::Slice vpack, ShapeContainer& region,
                std::vector<S2LatLng>& cache, coding::Options options,
                Encoder* encoder);

void PointToVPack(vpack::Builder& builder, S2LatLng point);

}  // namespace sdb::geo
