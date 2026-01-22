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

#include "geo/shape_container.h"

#include <geodesic.h>
#include <s2/s1angle.h>
#include <s2/s2latlng.h>
#include <s2/s2point_region.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>

#include <bit>
#include <cmath>

#include "basics/application-exit.h"
#include "basics/down_cast.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "geo/ellipsoid.h"
#include "geo/geo_params.h"
#include "geo/s2/multi_point_region.h"
#include "geo/s2/multi_polyline_region.h"
#include "geo/utils.h"

namespace sdb::geo {
namespace {

bool IsExcessiveLngLat(S1Angle lngsmall, S1Angle lngbig, S1Angle latsmall,
                       S1Angle latbig) {
  return std::fabs(lngbig.radians() - lngsmall.radians()) +
           std::fabs(latbig.radians() - latsmall.radians()) >=
         M_PI;
}

constexpr uint8_t BinOpCase(ShapeContainer::Type lhs,
                            ShapeContainer::Type rhs) noexcept {
  // any >= 7 && <= 41 is ok, but compiler use 8, so why not?
  return (std::to_underlying(lhs) * 8) + std::to_underlying(rhs);
}

template<typename T>
bool ContainsPoint(const S2Region& region, const S2Region& point) {
  const auto& lhs = basics::downCast<T>(region);
  const auto& rhs = basics::downCast<S2PointRegion>(point);
  return lhs.Contains(rhs.point());
}

template<typename T>
bool ContainsPoints(const S2Region& region, const S2Region& points) {
  const auto& lhs = basics::downCast<T>(region);
  const auto& rhs = basics::downCast<S2MultiPointRegion>(points);
  for (const auto& point : rhs.Impl()) {
    if (!lhs.Contains(point)) {
      return false;
    }
  }
  return true;
}

template<typename T>
bool ContainsPolyline(const T& lhs, const S2Polyline& rhs) {
  static constexpr auto kMaxError = S1Angle::Radians(1e-6);
  if constexpr (std::is_same_v<T, S2Polyline>) {
    return lhs.ApproxEquals(rhs, kMaxError);
  } else if constexpr (std::is_same_v<T, S2Polygon>) {
    return lhs.Contains(rhs);
  } else {
    static_assert(std::is_same_v<T, S2MultiPolylineRegion>);
    for (const auto& polyline : lhs.Impl()) {
      if (polyline.ApproxEquals(rhs, kMaxError)) {
        return true;
      }
    }
    return false;
  }
}

template<typename T>
bool ContainsPolyline(const S2Region& region, const S2Region& polyline) {
  const auto& lhs = basics::downCast<T>(region);
  const auto& rhs = basics::downCast<S2Polyline>(polyline);
  return ContainsPolyline<T>(lhs, rhs);
}

template<typename T>
bool ContainsPolylines(const S2Region& region, const S2Region& polylines) {
  const auto& lhs = basics::downCast<T>(region);
  const auto& rhs = basics::downCast<S2MultiPolylineRegion>(polylines);
  for (const auto& polyline : rhs.Impl()) {
    if (!ContainsPolyline<T>(lhs, polyline)) {
      return false;
    }
  }
  return true;
}

template<typename R1, typename R2>
bool IntersectsHelper(const S2Region& r1, const S2Region& r2) {
  const auto& lhs = basics::downCast<R1>(r1);
  const auto& rhs = basics::downCast<R2>(r2);
  if constexpr (std::is_same_v<R1, S2PointRegion>) {
    return rhs.Contains(lhs.point());
  } else {
    return rhs.Intersects(lhs);
  }
}

}  // namespace

void ShapeContainer::updateBounds(QueryParams& qp) const noexcept {
  SDB_ASSERT(!empty());

  S2LatLng ll{centroid()};
  qp.origin = ll;

  auto rect = _data->GetRectBound();
  if (rect.is_empty() || rect.is_point()) {
    qp.max_distance = 0.0;
    return;
  }
  double rad_max = 0.0;
  // The following computation deserves an explanation:
  // We want to derive from the bounding LatLng box an upper bound for
  // the maximal distance. The centroid of the shape is contained in the
  // bounding box and the main idea is to take the maximum distance to
  // any of the corners and take this as upper bound for the distance.
  // The hope is then that the complete bounding box is contained in the
  // circle with radius this maximal distance.
  // However, this is not correct in all cases. A prominent counterexample
  // is the bounding box {lat:[-90, 90], lng:[-180, 180]} which is used
  // for very large polygons. Its "four" corners are twice the north pole
  // and twice the south pole. Most points on earth have a maximal distance
  // to north and south pole of less than half the diameter of the earth,
  // and yet, the smallest circle to contain the whole bounding box has
  // radius half of the diameter of the earth.
  // So we need to adjust our bound here. What we do is the following:
  // If the sum of the added difference in latitude and longitude
  // is less than 180 degrees, then the actual shortest geodesic to a
  // corner runs as expected (for example, with increasing lat and lng
  // towards the upper right corner of the bounding box). In this case
  // the estimate of the maximal distance is correct, otherwise we simply
  // take M_PI or 180 degrees or half the diameter of the earth as estimate.
  if (IsExcessiveLngLat(rect.lng_lo(), ll.lng(), rect.lat_lo(), ll.lat())) {
    rad_max = M_PI;
  } else {
    S1Angle a1{ll, rect.lo()};
    rad_max = a1.radians();
  }
  if (IsExcessiveLngLat(ll.lng(), rect.lng_hi(), rect.lat_lo(), ll.lat())) {
    rad_max = M_PI;
  } else {
    S1Angle a2{ll, S2LatLng{rect.lat_lo(), rect.lng_hi()}};
    rad_max = std::max(rad_max, a2.radians());
  }
  if (IsExcessiveLngLat(rect.lng_lo(), ll.lng(), ll.lat(), rect.lat_hi())) {
    rad_max = M_PI;
  } else {
    S1Angle a3{ll, S2LatLng{rect.lat_hi(), rect.lng_lo()}};
    rad_max = std::max(rad_max, a3.radians());
  }
  if (IsExcessiveLngLat(ll.lng(), rect.lng_hi(), ll.lat(), rect.lat_hi())) {
    rad_max = M_PI;
  } else {
    S1Angle a4{ll, rect.hi()};
    rad_max = std::max(rad_max, a4.radians());
  }
  const auto rad = kRadEps + rad_max;
  qp.max_distance = rad * kEarthRadiusInMeters;
}

S2Point ShapeContainer::centroid() const noexcept {
  switch (_type) {
    case Type::S2Point:
      // S2PointRegion should be constructed from unit length Point
      return basics::downCast<S2PointRegion>(*_data).point();
    case Type::S2Polyline:
      // S2Polyline::GetCentroid() result isn't unit length
      return basics::downCast<S2Polyline>(*_data).GetCentroid().Normalize();
    case Type::S2Polygon:
      // S2Polygon::GetCentroid() result isn't unit length
      return basics::downCast<S2Polygon>(*_data).GetCentroid().Normalize();
    case Type::S2Multipoint:
      // S2MultiPointRegion::GetCentroid() result isn't unit length
      return basics::downCast<S2MultiPointRegion>(*_data)
        .GetCentroid()
        .Normalize();
    case Type::S2Multipolyline:
      // S2MultiPolylineRegion::GetCentroid() result isn't unit length
      return basics::downCast<S2MultiPolylineRegion>(*_data)
        .GetCentroid()
        .Normalize();
    case Type::Empty:
      SDB_ASSERT(false);
  }
  return {};
}

bool ShapeContainer::contains(const S2Point& other) const {
  SDB_ASSERT(!empty());
  return _data->Contains(other);
}

bool ShapeContainer::contains(const ShapeContainer& other) const {
  SDB_ASSERT(coding::IsSameLoss(_options, other._options));
  // In this implementation we force compiler to generate single jmp table.
  const auto sum = BinOpCase(_type, other._type);
  switch (sum) {
    case BinOpCase(Type::S2Point, Type::S2Point):
      return ContainsPoint<S2PointRegion>(*_data, *other._data);
    case BinOpCase(Type::S2Polygon, Type::S2Point):
      return ContainsPoint<S2Polygon>(*_data, *other._data);
    case BinOpCase(Type::S2Multipoint, Type::S2Point):
      return ContainsPoint<S2MultiPointRegion>(*_data, *other._data);

    case BinOpCase(Type::S2Point, Type::S2Multipoint):
      return ContainsPoints<S2PointRegion>(*_data, *other._data);
    case BinOpCase(Type::S2Polygon, Type::S2Multipoint):
      return ContainsPoints<S2Polygon>(*_data, *other._data);
    case BinOpCase(Type::S2Multipoint, Type::S2Multipoint):
      return ContainsPoints<S2MultiPointRegion>(*_data, *other._data);

    case BinOpCase(Type::S2Polyline, Type::S2Polyline):
      return ContainsPolyline<S2Polyline>(*_data, *other._data);
    case BinOpCase(Type::S2Polygon, Type::S2Polyline):
      return ContainsPolyline<S2Polygon>(*_data, *other._data);
    case BinOpCase(Type::S2Multipolyline, Type::S2Polyline):
      return ContainsPolyline<S2MultiPolylineRegion>(*_data, *other._data);

    case BinOpCase(Type::S2Polyline, Type::S2Multipolyline):
      return ContainsPolylines<S2Polyline>(*_data, *other._data);
    case BinOpCase(Type::S2Polygon, Type::S2Multipolyline):
      return ContainsPolylines<S2Polygon>(*_data, *other._data);
    case BinOpCase(Type::S2Multipolyline, Type::S2Multipolyline):
      return ContainsPolylines<S2MultiPolylineRegion>(*_data, *other._data);

    case BinOpCase(Type::S2Polygon, Type::S2Polygon): {
      const auto& lhs = basics::downCast<S2Polygon>(*_data);
      const auto& rhs = basics::downCast<S2Polygon>(*other._data);
      return lhs.Contains(rhs);
    }

    case BinOpCase(Type::S2Polyline, Type::S2Point):
    case BinOpCase(Type::S2Multipolyline, Type::S2Point):
    case BinOpCase(Type::S2Point, Type::S2Polyline):
    case BinOpCase(Type::S2Multipoint, Type::S2Polyline):
    case BinOpCase(Type::S2Point, Type::S2Polygon):
    case BinOpCase(Type::S2Polyline, Type::S2Polygon):
    case BinOpCase(Type::S2Multipoint, Type::S2Polygon):
    case BinOpCase(Type::S2Multipolyline, Type::S2Polygon):
    case BinOpCase(Type::S2Polyline, Type::S2Multipoint):
    case BinOpCase(Type::S2Multipolyline, Type::S2Multipoint):
    case BinOpCase(Type::S2Point, Type::S2Multipolyline):
    case BinOpCase(Type::S2Multipoint, Type::S2Multipolyline):
      // is numerically unstable and thus always false
      return false;
    default:
      SDB_ASSERT(false);
  }
  return false;
}

bool ShapeContainer::intersects(const ShapeContainer& other) const {
  SDB_ASSERT(coding::IsSameLoss(_options, other._options));
  // In this implementation we manually decrease count of switch cases,
  // and force compiler to generate single jmp table.
  // We can because user expect intersects(a, b) == intersects(b, a)
  const auto* d1 = _data.get();
  const auto* d2 = other._data.get();
  auto t1 = _type;
  auto t2 = other._type;
  if (t1 > t2) {
    std::swap(d1, d2);
    std::swap(t1, t2);
  }
  const auto sum = BinOpCase(t1, t2);
  switch (sum) {
    case BinOpCase(Type::S2Point, Type::S2Point):
      return IntersectsHelper<S2PointRegion, S2PointRegion>(*d1, *d2);
    case BinOpCase(Type::S2Point, Type::S2Polygon):
      return IntersectsHelper<S2PointRegion, S2Polygon>(*d1, *d2);
    case BinOpCase(Type::S2Point, Type::S2Multipoint):
      return IntersectsHelper<S2PointRegion, S2MultiPointRegion>(*d1, *d2);

    case BinOpCase(Type::S2Polyline, Type::S2Polyline):
      return IntersectsHelper<S2Polyline, S2Polyline>(*d1, *d2);
    case BinOpCase(Type::S2Polyline, Type::S2Polygon):
      return IntersectsHelper<S2Polyline, S2Polygon>(*d1, *d2);
    case BinOpCase(Type::S2Polyline, Type::S2Multipolyline):
      return IntersectsHelper<S2Polyline, S2MultiPolylineRegion>(*d1, *d2);

    case BinOpCase(Type::S2Polygon, Type::S2Polygon):
      return IntersectsHelper<S2Polygon, S2Polygon>(*d1, *d2);
    case BinOpCase(Type::S2Polygon, Type::S2Multipoint):
      return IntersectsHelper<S2Polygon, S2MultiPointRegion>(*d1, *d2);
    case BinOpCase(Type::S2Polygon, Type::S2Multipolyline):
      return IntersectsHelper<S2Polygon, S2MultiPolylineRegion>(*d1, *d2);

    case BinOpCase(Type::S2Multipoint, Type::S2Multipoint):
      return IntersectsHelper<S2MultiPointRegion, S2MultiPointRegion>(*d1, *d2);

    case BinOpCase(Type::S2Multipolyline, Type::S2Multipolyline):
      return IntersectsHelper<S2MultiPolylineRegion, S2MultiPolylineRegion>(
        *d1, *d2);

    case BinOpCase(Type::S2Point, Type::S2Polyline):
    case BinOpCase(Type::S2Point, Type::S2Multipolyline):
    case BinOpCase(Type::S2Polyline, Type::S2Multipoint):
    case BinOpCase(Type::S2Multipoint, Type::S2Multipolyline): {
      SDB_THROW(ERROR_NOT_IMPLEMENTED,
                "The case GEO_INTERSECTS(<some points>, <some polylines>)"
                " is numerically unstable and thus not supported.");
    }
    default:
      SDB_ASSERT(false);
  }
  return false;
}

void ShapeContainer::reset(std::unique_ptr<S2Region> data, Type type,
                           coding::Options options) noexcept {
  SDB_ASSERT((data == nullptr) == (type == Type::Empty));
  SDB_ASSERT(data == nullptr || type != Type::S2Point ||
             S2::IsUnitLength(basics::downCast<S2PointRegion>(*data).point()));
  _data = std::move(data);
  _type = type;
  _options = options;
}

void ShapeContainer::reset(S2Point point, coding::Options options) {
  // TODO(mbkkt) enable s2 checks in maintainer mode
  // assert from S2PointRegion ctor
  SDB_ASSERT(S2::IsUnitLength(point));
  if (_type == Type::S2Point) [[likely]] {
    SDB_ASSERT(_data);
    auto& region = basics::downCast<S2PointRegion>(*_data);
    region = S2PointRegion{point};
  } else {
    _data = std::make_unique<S2PointRegion>(point);
    _type = Type::S2Point;
  }
  _options = options;
}

bool ShapeContainer::equals(const ShapeContainer& other) const {
  if (_type != other._type || !coding::IsSameLoss(_options, other._options)) {
    return false;
  }
  switch (_type) {
    case Type::Empty:
      return true;
    case Type::S2Point: {
      const auto& lhs = basics::downCast<S2PointRegion>(*_data);
      const auto& rhs = basics::downCast<S2PointRegion>(*other._data);
      return lhs.Contains(rhs.point());
    }
    case Type::S2Polyline: {
      const auto& lhs = basics::downCast<S2Polyline>(*_data);
      const auto& rhs = basics::downCast<S2Polyline>(*other._data);
      return lhs.Equals(rhs);
    }
    case Type::S2Polygon: {
      const auto& lhs = basics::downCast<S2Polygon>(*_data);
      const auto& rhs = basics::downCast<S2Polygon>(*other._data);
      return lhs.Equals(rhs);
    }
    case Type::S2Multipoint: {
      const auto& lhs = basics::downCast<S2MultiPointRegion>(*_data);
      const auto& rhs = basics::downCast<S2MultiPointRegion>(*other._data);
      const auto& lhs_points = lhs.Impl();
      const auto& rhs_points = rhs.Impl();
      const auto size = lhs_points.size();
      if (size != rhs_points.size()) {
        return false;
      }
      for (size_t i = 0; i != size; ++i) {
        if (lhs_points[i] != rhs_points[i]) {
          return false;
        }
      }
      return true;
    }
    case Type::S2Multipolyline: {
      const auto& lhs = basics::downCast<S2MultiPolylineRegion>(*_data);
      const auto& rhs = basics::downCast<S2MultiPolylineRegion>(*other._data);
      const auto& lhs_lines = lhs.Impl();
      const auto& rhs_lines = rhs.Impl();
      const auto size = lhs_lines.size();
      if (size != rhs_lines.size()) {
        return false;
      }
      for (size_t i = 0; i != size; ++i) {
        if (!lhs_lines[i].ApproxEquals(rhs_lines[i])) {
          return false;
        }
      }
      return true;
    }
  }
  return false;
}

double ShapeContainer::distanceFromCentroid(const S2Point& other,
                                            const Ellipsoid& e) const noexcept {
  return utils::GeodesicDistance(S2LatLng{centroid()}, S2LatLng{other}, e);
}

double ShapeContainer::distanceFromCentroid(
  const S2Point& other) const noexcept {
  return centroid().Angle(other) * geo::kEarthRadiusInMeters;
}

double ShapeContainer::area(const Ellipsoid& e) const {
  if (!isAreaType()) {
    return 0.0;
  }

  // TODO: perhaps remove in favor of one code-path below ?
  if (e.flattening() == 0.0) {
    switch (_type) {
      case Type::S2Polygon: {
        auto& data = basics::downCast<S2Polygon>(*_data);
        return data.GetArea() * kEarthRadiusInMeters * kEarthRadiusInMeters;
      }
      default:
        SDB_ASSERT(false);
        return 0.0;
    }
  }

  double area = 0.0;
  geod_geodesic g{};
  geod_init(&g, e.equator_radius(), e.flattening());
  double a = 0.0;
  double p = 0.0;

  switch (_type) {
    case Type::S2Polygon: {
      geod_polygon polygon{};
      auto& data = basics::downCast<S2Polygon>(*_data);
      for (int k = 0; k < data.num_loops(); ++k) {
        geod_polygon_init(&polygon, 0);

        const auto* loop = data.loop(k);
        for (const auto& vertex : loop->vertices_span()) {
          S2LatLng lat_lng{vertex};
          geod_polygon_addpoint(&g, &polygon, lat_lng.lat().degrees(),
                                lat_lng.lng().degrees());
        }

        geod_polygon_compute(&g, &polygon, /*reverse=*/false, /*sign=*/true, &a,
                             &p);
        area += a;
      }
    } break;
    default:
      SDB_ASSERT(false);
      return 0.0;
  }
  return area;
}

void ShapeContainer::Encode(Encoder& encoder, coding::Options options) const {
  SDB_ASSERT(coding::IsOptionsS2(options));
  SDB_ASSERT(encoder.avail() >= sizeof(uint8_t));
  switch (_type) {
    case Type::S2Polyline: {
      const auto& data = basics::downCast<S2Polyline>(*_data);
      EncodePolyline(encoder, data, options);
    } break;
    case Type::S2Polygon: {
      const auto& data = basics::downCast<S2Polygon>(*_data);
      EncodePolygon(encoder, data, options);
    } break;
    case Type::S2Multipoint: {
      const auto& data = basics::downCast<S2MultiPointRegion>(*_data);
      data.Encode(encoder, options);
    } break;
    case Type::S2Multipolyline: {
      const auto& data = basics::downCast<S2MultiPolylineRegion>(*_data);
      data.Encode(encoder, options);
    } break;
    case Type::S2Point:  // encoded in other path
    case Type::Empty:
      SDB_ASSERT(false);
  }
}

template<ShapeContainer::Type TypeT, typename T>
void ShapeContainer::decodeImpl(Decoder& decoder) {
  if (_type != TypeT) [[unlikely]] {
    if constexpr (std::is_same_v<T, S2PointRegion>) {
      // Any S2Point, but should be unit length
      _data = std::make_unique<T>(S2Point{1, 0, 0});
    } else {
      auto data = std::make_unique<T>();
      if constexpr (std::is_same_v<T, S2Polyline> ||
                    std::is_same_v<T, S2Polygon>) {
        data->set_s2debug_override(S2Debug::DISABLE);
      }
      _data = std::move(data);
    }
    _type = TypeT;
  }
}

bool ShapeContainer::Decode(Decoder& decoder, std::vector<S2Point>& cache) {
  if (decoder.avail() < sizeof(uint8_t)) {
    return false;
  }
  const auto tag = decoder.get8();
  switch (coding::ToType(tag)) {
    case coding::ToTag<coding::Type::Point>(): {
      decodeImpl<Type::S2Point, S2PointRegion>(decoder);
      auto& data = basics::downCast<S2PointRegion>(*_data);
      if (S2Point point; DecodePoint(decoder, point, tag)) {
        data = S2PointRegion{point};
        _options = static_cast<coding::Options>(coding::ToPoint(tag));
        return true;
      }
    } break;
    case coding::ToTag<coding::Type::Polyline>(): {
      decodeImpl<Type::S2Polyline, S2Polyline>(decoder);
      auto& polyline = basics::downCast<S2Polyline>(*_data);
      if (DecodePolyline(decoder, polyline, tag, cache)) {
        _options = static_cast<coding::Options>(coding::ToPoint(tag));
        return true;
      }
    } break;
    case coding::ToTag<coding::Type::Polygon>(): {
      decodeImpl<Type::S2Polygon, S2Polygon>(decoder);
      auto& polygon = basics::downCast<S2Polygon>(*_data);
      if (DecodePolygon(decoder, polygon, tag, cache)) {
        _options = static_cast<coding::Options>(coding::ToPoint(tag));
        return true;
      }
    } break;
    case coding::ToTag<coding::Type::MultiPoint>(): {
      decodeImpl<Type::S2Multipoint, S2MultiPointRegion>(decoder);
      auto& points = basics::downCast<S2MultiPointRegion>(*_data);
      if (points.Decode(decoder, tag)) {
        _options = static_cast<coding::Options>(coding::ToPoint(tag));
        return true;
      }
    } break;
    case coding::ToTag<coding::Type::MultiPolyline>(): {
      decodeImpl<Type::S2Multipolyline, S2MultiPolylineRegion>(decoder);
      auto& polylines = basics::downCast<S2MultiPolylineRegion>(*_data);
      if (polylines.Decode(decoder, tag, cache)) {
        _options = static_cast<coding::Options>(coding::ToPoint(tag));
        return true;
      }
    } break;
    default:
      SDB_ASSERT(false);
  }
  return false;
}

}  // namespace sdb::geo
