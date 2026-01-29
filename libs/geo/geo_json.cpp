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

#include "geo/geo_json.h"

#include <absl/strings/match.h>
#include <s2/s2loop.h>
#include <s2/s2point_region.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>
#include <s2/util/coding/coder.h>
#include <vpack/iterator.h>

#include <algorithm>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "basics/debugging.h"
#include "basics/logger/logger.h"
#include "basics/string_utils.h"
#include "geo/geo_params.h"
#include "geo/s2/multi_point_region.h"
#include "geo/s2/multi_polyline_region.h"
#include "geo/shape_container.h"
#include "vpack/vpack_helper.h"

namespace sdb::geo::json {
namespace {

template<bool Validation>
S2Point EncodePointImpl(S2LatLng lat_lng, coding::Options options,
                        Encoder* encoder) noexcept {
  if constexpr (Validation) {
    if (encoder != nullptr) {
      SDB_ASSERT(options != coding::Options::Invalid);
      SDB_ASSERT(encoder->avail() >= sizeof(uint8_t));
      encoder->put8(coding::ToTag(coding::Type::Point, options));
      if (coding::IsOptionsS2(options)) {
        auto point = lat_lng.ToPoint();
        EncodePoint(*encoder, point);
        return point;
      }
      EncodeLatLng(*encoder, lat_lng, options);
    } else if (options == coding::Options::S2LatLngU32) {
      ToLatLngU32(lat_lng);
    }
  } else {
    SDB_ASSERT(encoder == nullptr);
    SDB_ASSERT(options == coding::Options::Invalid);
  }
  return lat_lng.ToPoint();
}

void EncodeImpl(std::span<S2LatLng> cache, coding::Type type,
                coding::Options options, Encoder* encoder) {
  if (encoder != nullptr) {
    SDB_ASSERT(options != coding::Options::Invalid);
    SDB_ASSERT(!coding::IsOptionsS2(options));
    SDB_ASSERT(encoder->avail() >= sizeof(uint8_t) + Varint::kMax64);
    encoder->put8(coding::ToTag(type, options));
    encoder->put_varint64(cache.size());
    EncodeVertices(*encoder, cache, options);
  } else if (options == coding::Options::S2LatLngU32) {
    ToLatLngU32(cache);
  }
}

void FillPoints(std::vector<S2Point>& points, std::span<const S2LatLng> cache) {
  points.clear();
  points.reserve(cache.size());
  for (const auto& point : cache) {
    points.emplace_back(point.ToPoint());
  }
}

template<bool Validation>
size_t EncodeCount(size_t count, coding::Type type, coding::Options options,
                   Encoder* encoder) {
  SDB_ASSERT(count != 0);
  if (Validation && encoder != nullptr) {
    SDB_ASSERT(options != coding::Options::Invalid);
    SDB_ASSERT(!coding::IsOptionsS2(options));
    encoder->Ensure(sizeof(uint8_t) + (1 + count) * Varint::kMax64 +
                    (2 + static_cast<size_t>(type == coding::Type::Polygon)) *
                      coding::ToSize(options));
    encoder->put8(coding::ToTag(type, options));
    if (count != 1) {
      encoder->put_varint64(count * 2 + 1);
    } else {
      return 2;
    }
  }
  return 1;
}

// The main idea is check in our CI 3 compile time branches:
// 1. Validation true
// 2. Validation false kIsMaintainer true
// 3. Validation false kIsMaintainer false
#if defined(SDB_DEV) && defined(__linux__) && !defined(__aarch64__)
constexpr bool kIsMaintainer = true;
#else
constexpr bool kIsMaintainer = false;
#endif

constexpr std::string_view kTypeStringPoint = "point";
constexpr std::string_view kTypeStringPolygon = "polygon";
constexpr std::string_view kTypeStringLineString = "linestring";
constexpr std::string_view kTypeStringMultiPoint = "multipoint";
constexpr std::string_view kTypeStringMultiPolygon = "multipolygon";
constexpr std::string_view kTypeStringMultiLineString = "multilinestring";
constexpr std::string_view kTypeStringGeometryCollection = "geometrycollection";

// TODO(mbkkt) t as parameter looks better, but compiler produce strange error
//  t isn't compile time for line toString(t) where t from template
template<Type T>
consteval std::string_view toString() noexcept {
  switch (T) {
    case Type::Unknown:
      return {};
    case Type::Point:
      return kTypeStringPoint;
    case Type::Linestring:
      return kTypeStringLineString;
    case Type::Polygon:
      return kTypeStringPolygon;
    case Type::MultiPoint:
      return kTypeStringMultiPoint;
    case Type::MultiLinestring:
      return kTypeStringMultiLineString;
    case Type::MultiPolygon:
      return kTypeStringMultiPolygon;
    case Type::GeometryCollection:
      return kTypeStringGeometryCollection;
  }
}

consteval ShapeContainer::Type ToShapeType(Type t) noexcept {
  switch (t) {
    case Type::Unknown:
    case Type::GeometryCollection:
      return ShapeContainer::Type::Empty;
    case Type::Point:
      return ShapeContainer::Type::S2Point;
    case Type::Linestring:
      return ShapeContainer::Type::S2Polyline;
    case Type::Polygon:
    case Type::MultiPolygon:
      return ShapeContainer::Type::S2Polygon;
    case Type::MultiPoint:
      return ShapeContainer::Type::S2Multipoint;
    case Type::MultiLinestring:
      return ShapeContainer::Type::S2Multipolyline;
  }
  return ShapeContainer::Type::Empty;
}

template<typename Vertices>
void RemoveAdjacentDuplicates(Vertices& vertices) noexcept {
  // TODO We don't remove antipodal vertices
  auto it = std::unique(vertices.begin(), vertices.end());
  vertices.erase(it, vertices.end());
}

bool GetCoordinates(vpack::Slice& vpack) {
  SDB_ASSERT(vpack.isObject());
  vpack = vpack.get(fields::kCoordinates);
  return vpack.isArray();
}

template<bool Validation, bool GeoJson>
bool ParseImpl(vpack::Slice vpack, S2LatLng& vertex) {
  SDB_ASSERT(vpack.isArray());
  vpack::ArrayIterator jt{vpack};
  if (Validation && SDB_UNLIKELY(jt.size() != 2)) {
    return false;
  }
  const auto first = *jt;
  if (Validation && SDB_UNLIKELY(!first.isNumber<double>())) {
    return false;
  }
  jt.next();
  const auto second = *jt;
  if (Validation && SDB_UNLIKELY(!second.isNumber<double>())) {
    return false;
  }
  double lat, lon;
  if constexpr (GeoJson) {
    lon = first.getNumber<double>();
    lat = second.getNumber<double>();
  } else {
    lat = first.getNumber<double>();
    lon = second.getNumber<double>();
  }
  // We should Normalize all S2LatLng
  // because otherwise their converting to S2Point is invalid!
  vertex = S2LatLng::FromDegrees(lat, lon).Normalized();
  return true;
}

template<bool Validation, bool GeoJson>
Result ParseImpl(vpack::Slice vpack, std::vector<S2LatLng>& vertices) {
  SDB_ASSERT(vpack.isArray());
  vpack::ArrayIterator it{vpack};
  vertices.clear();
  vertices.reserve(it.size());

  S2LatLng vertex;
  for (; it.valid(); it.next()) {
    auto slice = *it;
    if (Validation && SDB_UNLIKELY(!slice.isArray())) {
      return {ERROR_BAD_PARAMETER,
              "Bad coordinates, should be array " + slice.toJson()};
    }
    auto ok = ParseImpl<Validation, GeoJson>(slice, vertex);
    if (Validation && SDB_UNLIKELY(!ok)) {
      return {ERROR_BAD_PARAMETER, "Bad coordinates values " + slice.toJson()};
    }
    vertices.emplace_back(vertex);
  }
  return {};
}

template<Type T>
Result Validate(vpack::Slice& vpack) {
  if (ParseType(vpack) != T) [[unlikely]] {
    // TODO(mbkkt) Unnecessary memcpy,
    //  this string can be constructed at compile time with compile time new
    return {ERROR_BAD_PARAMETER,
            absl::StrCat("Require type: '", toString<T>(), "'.")};
  }
  if (!GetCoordinates(vpack)) [[unlikely]] {
    return {ERROR_BAD_PARAMETER, "Coordinates missing."};
  }
  return {};
}

template<bool Validation, bool GeoJson>
Result ParsePointImpl(vpack::Slice vpack, S2LatLng& region) {
  auto ok = ParseImpl<Validation, GeoJson>(vpack, region);
  if (Validation && SDB_UNLIKELY(!ok)) {
    return {ERROR_BAD_PARAMETER, "Bad coordinates " + vpack.toJson()};
  }
  return {};
}

template<bool Validation>
Result ParsePointsImpl(vpack::Slice vpack, std::vector<S2LatLng>& vertices) {
  auto r = ParseImpl<Validation, true>(vpack, vertices);
  if (Validation && SDB_UNLIKELY(!r.ok())) {
    return r;
  }
  if (Validation && SDB_UNLIKELY(vertices.empty())) {
    return {ERROR_BAD_PARAMETER,
            "Invalid MultiPoint, it must contains at least one point."};
  }
  return {};
}

template<bool Validation>
Result ParseLineImpl(vpack::Slice vpack, std::vector<S2LatLng>& vertices) {
  auto r = ParseImpl<Validation, true>(vpack, vertices);
  if (Validation && SDB_UNLIKELY(!r.ok())) {
    return r;
  }
  RemoveAdjacentDuplicates(vertices);
  if (Validation && SDB_UNLIKELY(vertices.size() < 2)) {
    return {ERROR_BAD_PARAMETER,
            "Invalid LineString,"
            " adjacent vertices must not be identical or antipodal."};
  }
  return {};
}

template<bool Validation>
Result ParseLinesImpl(vpack::Slice vpack, std::vector<S2Polyline>& lines,
                      std::vector<S2LatLng>& vertices, coding::Options options,
                      Encoder* encoder) {
  SDB_ASSERT(vpack.isArray());
  vpack::ArrayIterator it{vpack};
  const auto n = it.size();
  if (Validation && SDB_UNLIKELY(n == 0)) {
    return {
      ERROR_BAD_PARAMETER,
      "Invalid MultiLinestring, it must contains at least one Linestring."};
  }
  auto multiplier =
    EncodeCount<Validation>(n, coding::Type::MultiPolyline, options, encoder);
  lines.clear();
  lines.reserve(n);
  for (; it.valid(); it.next()) {
    auto slice = *it;
    if (Validation && SDB_UNLIKELY(!slice.isArray())) {
      return {ERROR_BAD_PARAMETER, "Missing coordinates."};
    }
    [[maybe_unused]] auto r = ParseLineImpl<Validation>(slice, vertices);
    if constexpr (Validation) {
      if (!r.ok()) [[unlikely]] {
        return r;
      }
      if (encoder != nullptr) {
        encoder->put_varint64(n * multiplier);
        multiplier = 1;
        EncodeVertices(*encoder, vertices, options);
      } else if (options == coding::Options::S2LatLngU32) {
        ToLatLngU32(vertices);
      }
      auto& back = lines.emplace_back(vertices, S2Debug::DISABLE);
      if (S2Error error; SDB_UNLIKELY(back.FindValidationError(&error))) {
        return {ERROR_BAD_PARAMETER,
                absl::StrCat("Invalid Polyline: ", error.message())};
      }
    } else {
      lines.emplace_back(vertices, S2Debug::DISABLE);
    }
  }
  return {};
}

template<bool Validation>
Result MakeLoopValid(std::vector<S2LatLng>& vertices) noexcept(!Validation) {
  if (Validation && SDB_UNLIKELY(vertices.size() < 4)) {
    return {ERROR_BAD_PARAMETER,
            "Invalid GeoJson Loop, must have at least 4 vertices"};
  }
  // S2Loop doesn't like duplicates
  RemoveAdjacentDuplicates(vertices);
  if (Validation && SDB_UNLIKELY(vertices.front() != vertices.back())) {
    return {ERROR_BAD_PARAMETER, "Loop not closed"};
  }
  // S2Loop automatically add last edge
  if (vertices.size() > 1) [[likely]] {
    SDB_ASSERT(vertices.size() >= 3);
    // 3 is incorrect but it will be handled by S2Loop::FindValidationError
    vertices.pop_back();
  }
  return {};
}

template<bool Validation>
Result ParseLoopImpl(vpack::Slice vpack,
                     std::vector<std::unique_ptr<S2Loop>>& loops,
                     std::vector<S2LatLng>& vertices, S2Loop*& first,
                     coding::Options options, Encoder* encoder,
                     size_t multiplier) {
  if (Validation && SDB_UNLIKELY(!vpack.isArray())) {
    return {ERROR_BAD_PARAMETER, "Missing coordinates."};
  }
  // Coordinates of a Polygon are an array of LinearRing coordinate arrays.
  // The first element in the array represents the exterior ring.
  // Any subsequent elements represent interior rings (or holes).
  // - A linear ring is a closed LineString with four or more positions.
  // - The first and last positions are equivalent, and they MUST contain
  //   identical values; their representation SHOULD also be identical.
  // - A linear ring is the boundary of a surface or the boundary of a
  //   hole in a surface.
  // - A linear ring MUST follow the right-hand rule with respect to the
  //   area it bounds, i.e., exterior rings are counterclockwise (CCW), and
  //   holes are clockwise (CW).
  auto r = ParsePointsImpl<Validation>(vpack, vertices);
  if (Validation && SDB_UNLIKELY(!r.ok())) {
    return r;
  }
  r = MakeLoopValid<Validation>(vertices);
  if (Validation && SDB_UNLIKELY(!r.ok())) {
    return r;
  }
  auto points = s2internal::MakeS2PointArrayForOverwrite(vertices.size());
  if (Validation && options == coding::Options::S2LatLngU32) {
    absl::c_transform(vertices, points.get(), [](S2LatLng copy) {
      ToLatLngU32(copy);
      return copy.ToPoint();
    });
  } else {
    absl::c_transform(vertices, points.get(), [](const S2LatLng& lat_lng) {
      return lat_lng.ToPoint();
    });
  }
  auto& last = *loops.emplace_back(std::make_unique<S2Loop>(
    static_cast<int>(vertices.size()), std::move(points), S2Debug::DISABLE));
  if constexpr (Validation) {
    if (S2Error error; SDB_UNLIKELY(last.FindValidationError(&error))) {
      return {ERROR_BAD_PARAMETER,
              absl::StrCat("Invalid Loop in Polygon: ", error.message())};
    }
  }
  // Note that we are using InitNested for S2Polygon below.
  // Therefore, we are supposed to deliver all our loops in CCW
  // convention (aka right hand rule, interior is to the left of
  // the polyline).
  // Since we want to allow for loops, whose interior covers
  // more than half of the earth, we must not blindly "Normalize"
  // the loops, as we did in earlier versions, although RFC7946
  // says "parsers SHOULD NOT reject Polygons that do not follow
  // the right-hand rule". Since we cannot detect if the outer loop
  // respects the rule, we cannot reject it. However, for the other
  // loops, we can be a bit more tolerant: If a subsequent loop is
  // not contained in the first one (following the right hand rule),
  // then we can invert it silently and if it is then contained
  // in the first, we have proper nesting and leave the rest to
  // InitNested. This is, why we proceed like this:
  if (first == nullptr) {
    first = &last;
  } else if (first->Contains(last)) [[likely]] {
    return {};
  } else {
    // TODO(mbkkt) Maybe we want more strict rules about CCW?
    // We don't need to make Contains check in parsing stage in such case
    last.Invert();
    if (Validation && encoder != nullptr) {
      std::reverse(vertices.begin(), vertices.end());
    }
  }
  if constexpr (Validation) {
    if (first != &last && !first->Contains(last)) [[unlikely]] {
      return {ERROR_BAD_PARAMETER,
              "Subsequent loop is not a hole in a polygon."};
    }
    if (encoder != nullptr) {
      SDB_ASSERT(encoder->avail() >= Varint::kMax64);
      encoder->put_varint64(vertices.size() * multiplier);
      EncodeVertices(*encoder, vertices, options);
    }
  }
  return {};
}

S2Polygon CreatePolygon(std::vector<std::unique_ptr<S2Loop>>&& loops,
                        coding::Options options, Encoder* encoder) {
  if (loops.size() != 1 || !loops[0]->is_empty()) {
    return S2Polygon{std::move(loops), S2Debug::DISABLE};
  }
  // Handle creation of empty polygon otherwise will be error in validation
  S2Polygon polygon;
  polygon.set_s2debug_override(S2Debug::DISABLE);
  if (encoder != nullptr) {
    encoder->clear();
    encoder->put8(coding::ToTag(coding::Type::Polygon, options));
    encoder->put_varint64(0);
  }
  return polygon;
}

template<bool Validation>
Result ParsePolygonImpl(vpack::ArrayIterator it, S2Polygon& region,
                        std::vector<S2LatLng>& vertices,
                        coding::Options options, Encoder* encoder) {
  const auto n = it.size();
  SDB_ASSERT(n >= 1);
  std::vector<std::unique_ptr<S2Loop>> loops;
  loops.reserve(n);
  auto multiplier =
    EncodeCount<Validation>(n, coding::Type::Polygon, options, encoder);
  for (S2Loop* first = nullptr; it.valid(); it.next()) {
    auto r = ParseLoopImpl<Validation>(*it, loops, vertices, first, options,
                                       encoder, multiplier);
    if (Validation && SDB_UNLIKELY(!r.ok())) {
      return r;
    }
    multiplier = 1;
  }
  region = CreatePolygon(std::move(loops), options, encoder);
  if constexpr (Validation) {
    S2Error error;
    if (region.FindValidationError(&error)) [[unlikely]] {
      return {ERROR_BAD_PARAMETER,
              absl::StrCat("Invalid Polygon: ", error.message())};
    }
  }
  return {};
}

template<bool Validation>
Result ParsePolygonImpl(vpack::Slice vpack, ShapeContainer& region,
                        std::vector<S2LatLng>& vertices,
                        coding::Options options, Encoder* encoder) {
  SDB_ASSERT(vpack.isArray());
  vpack::ArrayIterator it{vpack};
  const auto n = it.size();
  if (Validation && SDB_UNLIKELY(n == 0)) {
    return {ERROR_BAD_PARAMETER, "Invalid GeoJSON Geometry Object."};
  }
  std::unique_ptr<S2Polygon> d;
  d = std::make_unique<S2Polygon>();
  auto r = ParsePolygonImpl<Validation>(it, *d, vertices, options, encoder);
  if (Validation && SDB_UNLIKELY(!r.ok())) {
    return r;
  }
  region.reset(std::move(d), ToShapeType(Type::Polygon), options);
  return {};
}

template<bool Validation>
Result ParseMultiPolygonImpl(vpack::Slice vpack, S2Polygon& region,
                             std::vector<S2LatLng>& vertices,
                             coding::Options options, Encoder* encoder) {
  SDB_ASSERT(vpack.isArray());
  vpack::ArrayIterator it{vpack};
  const auto n = it.size();
  if (Validation && SDB_UNLIKELY(n == 0)) {
    return {ERROR_BAD_PARAMETER,
            "MultiPolygon should contains at least one Polygon."};
  }
  std::vector<std::unique_ptr<S2Loop>> loops;
  loops.reserve(n);
  auto multiplier =
    EncodeCount<Validation>(n, coding::Type::Polygon, options, encoder);
  for (S2Loop* first = nullptr; it.valid(); it.next()) {
    if (Validation && SDB_UNLIKELY(!(*it).isArray())) {
      return {ERROR_BAD_PARAMETER,
              "Polygon should contains at least one coordinates array."};
    }
    vpack::ArrayIterator jt{*it};
    if (Validation && SDB_UNLIKELY(jt.size() == 0)) {
      return {ERROR_BAD_PARAMETER,
              "Polygon should contains at least one Loop."};
    }
    if (encoder != nullptr) {
      encoder->Ensure(jt.size() * Varint::kMax64);
    }
    for (; jt.valid(); jt.next()) {
      auto r = ParseLoopImpl<Validation>(*jt, loops, vertices, first, options,
                                         encoder, multiplier);
      if (Validation && SDB_UNLIKELY(!r.ok())) {
        return r;
      }
      multiplier = 1;
    }
    first = nullptr;
  }
  region = CreatePolygon(std::move(loops), options, encoder);
  if constexpr (Validation) {
    S2Error error;
    if (region.FindValidationError(&error)) [[unlikely]] {
      return {ERROR_BAD_PARAMETER,
              absl::StrCat("Invalid Loop in MultiPolygon: ", error.message())};
    }
  }
  return {};
}

template<bool Validation>
Result ParseRegionImpl(vpack::Slice vpack, ShapeContainer& region,
                       std::vector<S2LatLng>& cache, coding::Options options,
                       Encoder* encoder) {
  const auto t = ParseType(vpack);
  if constexpr (Validation) {
    if (t == Type::Unknown) [[unlikely]] {
      return {ERROR_BAD_PARAMETER, "Invalid GeoJSON Geometry Object."};
    }
    if (!GetCoordinates(vpack)) [[unlikely]] {
      return {ERROR_BAD_PARAMETER, "Coordinates missing."};
    }
  } else {
    vpack = vpack.get(fields::kCoordinates);
  }
  const bool is_s2 = coding::IsOptionsS2(options);
  switch (t) {
    case Type::Point: {
      S2LatLng lat_lng;
      auto r = ParsePointImpl<Validation, true>(vpack, lat_lng);
      if (Validation && SDB_UNLIKELY(!r.ok())) {
        return r;
      }
      region.reset(EncodePointImpl<Validation>(lat_lng, options, encoder),
                   options);
      return {};
    }
    case Type::Linestring: {
      auto r = ParseLineImpl<Validation>(vpack, cache);
      if (Validation && SDB_UNLIKELY(!r.ok())) {
        return r;
      }
      if (Validation && !is_s2) {
        EncodeImpl(cache, coding::Type::Polyline, options, encoder);
      }
      auto d = std::make_unique<S2Polyline>(cache, S2Debug::DISABLE);
      if (S2Error error; Validation && d->FindValidationError(&error)) {
        return {ERROR_BAD_PARAMETER,
                absl::StrCat("Invalid Polyline: ", error.message())};
      }
      region.reset(std::move(d), ToShapeType(Type::Linestring), options);
    } break;
    case Type::Polygon: {
      auto r = ParsePolygonImpl<Validation>(vpack, region, cache, options,
                                            is_s2 ? nullptr : encoder);
      if (Validation && SDB_UNLIKELY(!r.ok())) {
        return r;
      }
    } break;
    case Type::MultiPoint: {
      auto r = ParsePointsImpl<Validation>(vpack, cache);
      if (Validation && SDB_UNLIKELY(!r.ok())) {
        return r;
      }
      if (Validation && !is_s2) {
        EncodeImpl(cache, coding::Type::MultiPoint, options, encoder);
      }
      auto d = std::make_unique<S2MultiPointRegion>();
      FillPoints(d->Impl(), cache);
      region.reset(std::move(d), ToShapeType(Type::MultiPoint), options);
    } break;
    case Type::MultiLinestring: {
      std::vector<S2Polyline> lines;
      auto r = ParseLinesImpl<Validation>(vpack, lines, cache, options,
                                          is_s2 ? nullptr : encoder);
      if (Validation && SDB_UNLIKELY(!r.ok())) {
        return r;
      }
      auto d = std::make_unique<S2MultiPolylineRegion>();
      d->Impl() = std::move(lines);
      region.reset(std::move(d), ToShapeType(Type::MultiLinestring), options);
    } break;
    case Type::MultiPolygon: {
      auto d = std::make_unique<S2Polygon>();
      auto r = ParseMultiPolygonImpl<Validation>(vpack, *d, cache, options,
                                                 is_s2 ? nullptr : encoder);
      if (Validation && SDB_UNLIKELY(!r.ok())) {
        return r;
      }
      region.reset(std::move(d), ToShapeType(Type::MultiPolygon), options);
    } break;
    default:
      return {ERROR_NOT_IMPLEMENTED,
              "GeoJSON type GeometryCollection is not supported"};
  }
  if (Validation && encoder != nullptr && is_s2) {
    SDB_ASSERT(encoder->length() == 0);
    encoder->clear();
    region.Encode(*encoder, options);
  }
  return {};
}

}  // namespace

#define CASE(value)                                        \
  case toString<value>().size():                           \
    if (absl::EqualsIgnoreCase(toString<value>(), type)) { \
      return value;                                        \
    }                                                      \
    break

/// parse GeoJSON Type
Type ParseType(vpack::Slice vpack) noexcept {
  if (!vpack.isObject()) [[unlikely]] {
    return Type::Unknown;
  }
  const auto field = vpack.get(fields::kType);
  if (!field.isString()) [[unlikely]] {
    return Type::Unknown;
  }
  const auto type = field.stringView();
  switch (type.size()) {
    CASE(Type::Point);
    CASE(Type::Polygon);
    case toString<Type::Linestring>().size():
      static_assert(toString<Type::Linestring>().size() ==
                    toString<Type::MultiPoint>().size());
      if (absl::EqualsIgnoreCase(toString<Type::Linestring>(), type)) {
        return Type::Linestring;
      }
      if (absl::EqualsIgnoreCase(toString<Type::MultiPoint>(), type)) {
        return Type::MultiPoint;
      }
      break;
      CASE(Type::MultiPolygon);
      CASE(Type::MultiLinestring);
      CASE(Type::GeometryCollection);
    default:
      break;
  }
  return Type::Unknown;
}

#undef CASE

Result ParsePoint(vpack::Slice vpack, S2LatLng& region) {
  auto r = Validate<Type::Point>(vpack);
  if (!r.ok()) {
    return r;
  }
  return ParsePointImpl<true, true>(vpack, region);
}

Result ParseMultiPoint(vpack::Slice vpack, S2MultiPointRegion& region) {
  auto r = Validate<Type::MultiPoint>(vpack);
  if (!r.ok()) [[unlikely]] {
    return r;
  }
  std::vector<S2LatLng> vertices;
  r = ParsePointsImpl<true>(vpack, vertices);
  if (!r.ok()) [[unlikely]] {
    return r;
  }
  FillPoints(region.Impl(), vertices);
  return {};
}

Result ParseLinestring(vpack::Slice vpack, S2Polyline& region) {
  auto r = Validate<Type::Linestring>(vpack);
  if (!r.ok()) [[unlikely]] {
    return r;
  }
  std::vector<S2LatLng> vertices;
  r = ParseLineImpl<true>(vpack, vertices);
  if (!r.ok()) [[unlikely]] {
    return r;
  }
  region = S2Polyline{vertices, S2Debug::DISABLE};
  if (S2Error error; region.FindValidationError(&error)) {
    return {ERROR_BAD_PARAMETER,
            absl::StrCat("Invalid Polyline: ", error.message())};
  }
  return {};
}

Result ParseMultiLinestring(vpack::Slice vpack, S2MultiPolylineRegion& region) {
  auto r = Validate<Type::MultiLinestring>(vpack);
  if (!r.ok()) [[unlikely]] {
    return r;
  }
  auto& lines = region.Impl();
  std::vector<S2LatLng> vertices;
  return ParseLinesImpl<true>(vpack, lines, vertices, coding::Options::Invalid,
                              nullptr);
}

Result ParsePolygon(vpack::Slice vpack, S2Polygon& region) {
  auto r = Validate<Type::Polygon>(vpack);
  if (!r.ok()) [[unlikely]] {
    return r;
  }
  vpack::ArrayIterator it{vpack};
  if (it.size() == 0) [[unlikely]] {
    return {ERROR_BAD_PARAMETER, "Polygon should contains at least one loop."};
  }
  std::vector<S2LatLng> vertices;
  return ParsePolygonImpl<true>(it, region, vertices, coding::Options::Invalid,
                                nullptr);
}

Result ParseMultiPolygon(vpack::Slice vpack, S2Polygon& region) {
  auto r = Validate<Type::MultiPolygon>(vpack);
  if (!r.ok()) [[unlikely]] {
    return r;
  }
  std::vector<S2LatLng> vertices;
  return ParseMultiPolygonImpl<true>(vpack, region, vertices,
                                     coding::Options::Invalid, nullptr);
}

Result ParseRegion(vpack::Slice vpack, ShapeContainer& region) {
  std::vector<S2LatLng> cache;
  return ParseRegionImpl<true>(vpack, region, cache, coding::Options::Invalid,
                               nullptr);
}

template<bool Valid>
Result ParseRegion(vpack::Slice vpack, ShapeContainer& region,
                   std::vector<S2LatLng>& cache, coding::Options options,
                   Encoder* encoder) {
  auto r = ParseRegionImpl<(Valid || kIsMaintainer)>(vpack, region, cache,
                                                     options, encoder);
  SDB_ASSERT(Valid || r.ok(), r.errorMessage());
  return r;
}

template Result ParseRegion<true>(vpack::Slice vpack, ShapeContainer& region,
                                  std::vector<S2LatLng>& cache,
                                  coding::Options options, Encoder* encoder);
template Result ParseRegion<false>(vpack::Slice vpack, ShapeContainer& region,
                                   std::vector<S2LatLng>& cache,
                                   coding::Options options, Encoder* encoder);

template<bool Valid>
Result ParseCoordinates(vpack::Slice vpack, ShapeContainer& region,
                        bool geo_json, coding::Options options,
                        Encoder* encoder) {
  auto r = [&]() -> Result {
    static constexpr bool kValidation = Valid || kIsMaintainer;
    if (kValidation && SDB_UNLIKELY(!vpack.isArray())) {
      return {ERROR_BAD_PARAMETER, "Invalid coordinate pair."};
    }
    S2LatLng lat_lng;
    auto res = geo_json ? ParsePointImpl<kValidation, true>(vpack, lat_lng)
                        : ParsePointImpl<kValidation, false>(vpack, lat_lng);
    if (kValidation && SDB_UNLIKELY(!res.ok())) {
      return res;
    }
    region.reset(EncodePointImpl<kValidation>(lat_lng, options, encoder),
                 options);
    return {};
  }();
  SDB_ASSERT(Valid || r.ok(), r.errorMessage());
  return r;
}

template Result ParseCoordinates<true>(vpack::Slice vpack,
                                       ShapeContainer& region, bool geoJson,
                                       coding::Options options,
                                       Encoder* encoder);
template Result ParseCoordinates<false>(vpack::Slice vpack,
                                        ShapeContainer& region, bool geoJson,
                                        coding::Options options,
                                        Encoder* encoder);

}  // namespace sdb::geo::json
