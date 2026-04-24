////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "geo/wkb.h"

#include <s2/s2latlng.h>
#include <s2/s2loop.h>
#include <s2/s2point.h>
#include <s2/s2point_region.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>

#include <bit>
#include <cstring>
#include <memory>

#include "basics/errors.h"
#include "geo/s2/multi_point_region.h"
#include "geo/s2/multi_polyline_region.h"
#include "geo/shape_container.h"

namespace sdb::geo {
namespace {

// EWKB type-flag bits (PostGIS extension).
constexpr uint32_t kEwkbSridFlag = 0x20000000;
constexpr uint32_t kEwkbZFlag = 0x80000000;
constexpr uint32_t kEwkbMFlag = 0x40000000;
constexpr int32_t kCRS84Srid = 4326;

// OGC SFA geometry type codes, masked to the low bits.
enum class WkbType : uint32_t {
  Point = 1,
  LineString = 2,
  Polygon = 3,
  MultiPoint = 4,
  MultiLineString = 5,
  MultiPolygon = 6,
  GeometryCollection = 7,
};

// Byte-oriented WKB reader. Tracks cursor + expected endianness so coordinate
// doubles are byteswapped as needed. All reads bounds-check.
class WkbReader {
 public:
  explicit WkbReader(std::string_view bytes) noexcept
    : _ptr{bytes.data()}, _end{bytes.data() + bytes.size()} {}

  bool ReadByteOrder() noexcept {
    uint8_t b;
    if (!ReadByte(b)) {
      return false;
    }
    // WKB: 0 = big-endian, 1 = little-endian (NDR). We normalize by tracking
    // whether a byteswap is needed to produce native order.
    if (b == 0) {
      _byteswap = std::endian::native != std::endian::big;
    } else if (b == 1) {
      _byteswap = std::endian::native != std::endian::little;
    } else {
      return false;
    }
    return true;
  }

  bool ReadU32(uint32_t& out) noexcept {
    if (_end - _ptr < 4) {
      return false;
    }
    std::memcpy(&out, _ptr, 4);
    _ptr += 4;
    if (_byteswap) {
      out = std::byteswap(out);
    }
    return true;
  }

  bool ReadDouble(double& out) noexcept {
    if (_end - _ptr < 8) {
      return false;
    }
    uint64_t raw;
    std::memcpy(&raw, _ptr, 8);
    _ptr += 8;
    if (_byteswap) {
      raw = std::byteswap(raw);
    }
    std::memcpy(&out, &raw, 8);
    return true;
  }

  // WKB coordinate order is (lng, lat).
  bool ReadLatLng(S2LatLng& out) noexcept {
    double lng, lat;
    if (!ReadDouble(lng) || !ReadDouble(lat)) {
      return false;
    }
    out = S2LatLng::FromDegrees(lat, lng).Normalized();
    return true;
  }

  bool ReadByte(uint8_t& out) noexcept {
    if (_ptr >= _end) {
      return false;
    }
    out = static_cast<uint8_t>(*_ptr);
    ++_ptr;
    return true;
  }

  bool Empty() const noexcept { return _ptr >= _end; }

 private:
  const char* _ptr;
  const char* _end;
  bool _byteswap{false};
};

// Read the endian byte, type word, optional SRID, and return the bare
// geometry type. Rejects Z/M dimensions and non-CRS84 SRIDs.
Result ReadHeader(WkbReader& r, WkbType& type) {
  if (!r.ReadByteOrder()) {
    return {ERROR_BAD_PARAMETER, "WKB: truncated or bad byte-order byte"};
  }
  uint32_t raw_type;
  if (!r.ReadU32(raw_type)) {
    return {ERROR_BAD_PARAMETER, "WKB: truncated type word"};
  }
  if ((raw_type & (kEwkbZFlag | kEwkbMFlag)) != 0) {
    return {ERROR_BAD_PARAMETER,
            "WKB: Z/M dimensions not supported (CRS84 is 2D)"};
  }
  const bool has_srid = (raw_type & kEwkbSridFlag) != 0;
  const uint32_t bare = raw_type & 0x000000FF;  // ISO WKB PointZ=1001 also
                                                // lands here after masking,
                                                // but Z/M check above rejects.
  if (bare < 1 || bare > 7) {
    return {ERROR_BAD_PARAMETER, "WKB: unsupported geometry type"};
  }
  if (has_srid) {
    uint32_t srid;
    if (!r.ReadU32(srid)) {
      return {ERROR_BAD_PARAMETER, "WKB: truncated SRID"};
    }
    if (static_cast<int32_t>(srid) != kCRS84Srid) {
      return {ERROR_BAD_PARAMETER,
              "WKB: only SRID 4326 (CRS84) is supported; got ", srid};
    }
  }
  type = static_cast<WkbType>(bare);
  return {};
}

Result ReadPoint(WkbReader& r, S2LatLng& out) {
  if (!r.ReadLatLng(out)) {
    return {ERROR_BAD_PARAMETER, "WKB: truncated Point coordinates"};
  }
  return {};
}

Result ReadLineStringVertices(WkbReader& r, std::vector<S2LatLng>& cache) {
  uint32_t count;
  if (!r.ReadU32(count)) {
    return {ERROR_BAD_PARAMETER, "WKB: truncated LineString vertex count"};
  }
  cache.clear();
  cache.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    S2LatLng ll;
    if (!r.ReadLatLng(ll)) {
      return {ERROR_BAD_PARAMETER, "WKB: truncated LineString vertex ", i};
    }
    cache.push_back(ll);
  }
  return {};
}

// Polygon = ring count + (ring = vertex count + vertices). First ring is
// outer; remaining rings are holes. S2Loop expects open rings (no duplicate
// closing vertex) -- WKB always includes it, so we drop the last vertex.
//
// Orientation strategy mirrors libs/geo/geo_json.cpp ParseLoopImpl so WKB and
// GeoJSON ingest produce the same S2Polygon for equivalent coordinates:
// the outer loop is left as-given (so polygons whose intended interior covers
// more than half the earth survive), and subsequent loops are inverted only
// when they aren't already contained in the outer.
Result ReadPolygonLoops(WkbReader& r, std::vector<std::unique_ptr<S2Loop>>& out,
                        std::vector<S2LatLng>& cache) {
  uint32_t ring_count;
  if (!r.ReadU32(ring_count)) {
    return {ERROR_BAD_PARAMETER, "WKB: truncated Polygon ring count"};
  }
  out.clear();
  out.reserve(ring_count);
  S2Loop* first = nullptr;
  for (uint32_t i = 0; i < ring_count; ++i) {
    uint32_t vcount;
    if (!r.ReadU32(vcount)) {
      return {ERROR_BAD_PARAMETER, "WKB: truncated Polygon ring-", i,
              " vertex count"};
    }
    if (vcount < 4) {
      return {ERROR_BAD_PARAMETER,
              "WKB: Polygon ring must have >= 4 vertices (closed); ring ", i,
              " has ", vcount};
    }
    std::vector<S2Point> pts;
    pts.reserve(vcount - 1);
    S2LatLng ll;
    for (uint32_t v = 0; v < vcount; ++v) {
      if (!r.ReadLatLng(ll)) {
        return {ERROR_BAD_PARAMETER, "WKB: truncated Polygon ring-", i,
                " vertex ", v};
      }
      // Skip the last vertex: WKB closes the ring, S2Loop must be open.
      if (v + 1 < vcount) {
        pts.push_back(ll.ToPoint());
      }
    }
    auto loop = std::make_unique<S2Loop>(pts, S2Debug::DISABLE);
    if (!loop->IsValid()) {
      return {ERROR_BAD_PARAMETER, "WKB: Polygon ring-", i, " is not a valid ",
              "S2 loop"};
    }
    auto* current = loop.get();
    out.push_back(std::move(loop));
    if (first == nullptr) {
      first = current;
      continue;
    }
    if (!first->Contains(*current)) {
      current->Invert();
      if (!first->Contains(*current)) {
        return {ERROR_BAD_PARAMETER, "WKB: Polygon ring-", i,
                " is not a hole in the outer ring"};
      }
    }
  }
  return {};
}

Result ParseGeometry(WkbReader& r, ShapeContainer& region,
                     std::vector<S2LatLng>& cache);

Result ParsePoint(WkbReader& r, ShapeContainer& region) {
  S2LatLng ll;
  if (auto res = ReadPoint(r, ll); res.fail()) {
    return res;
  }
  region.reset(ll.ToPoint());
  return {};
}

Result ParseLineString(WkbReader& r, ShapeContainer& region,
                       std::vector<S2LatLng>& cache) {
  if (auto res = ReadLineStringVertices(r, cache); res.fail()) {
    return res;
  }
  std::vector<S2Point> pts;
  pts.reserve(cache.size());
  for (const auto& ll : cache) {
    pts.push_back(ll.ToPoint());
  }
  auto line = std::make_unique<S2Polyline>(pts, S2Debug::DISABLE);
  if (!line->IsValid()) {
    return {ERROR_BAD_PARAMETER, "WKB: LineString is not a valid S2Polyline"};
  }
  region.reset(std::move(line), ShapeContainer::Type::S2Polyline);
  return {};
}

Result ParsePolygon(WkbReader& r, ShapeContainer& region,
                    std::vector<S2LatLng>& cache) {
  std::vector<std::unique_ptr<S2Loop>> loops;
  if (auto res = ReadPolygonLoops(r, loops, cache); res.fail()) {
    return res;
  }
  auto poly = std::make_unique<S2Polygon>();
  poly->InitNested(std::move(loops));
  region.reset(std::move(poly), ShapeContainer::Type::S2Polygon);
  return {};
}

Result ParseMultiPoint(WkbReader& r, ShapeContainer& region) {
  uint32_t count;
  if (!r.ReadU32(count)) {
    return {ERROR_BAD_PARAMETER, "WKB: truncated MultiPoint count"};
  }
  auto multi = std::make_unique<S2MultiPointRegion>();
  multi->Impl().reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    // Each sub-geometry is a full WKB Point (header + coords).
    WkbType sub_type;
    if (auto res = ReadHeader(r, sub_type); res.fail()) {
      return res;
    }
    if (sub_type != WkbType::Point) {
      return {ERROR_BAD_PARAMETER, "WKB: MultiPoint member ", i,
              " is not a Point"};
    }
    S2LatLng ll;
    if (auto res = ReadPoint(r, ll); res.fail()) {
      return res;
    }
    multi->Impl().push_back(ll.ToPoint());
  }
  region.reset(std::move(multi), ShapeContainer::Type::S2Multipoint);
  return {};
}

Result ParseMultiLineString(WkbReader& r, ShapeContainer& region,
                            std::vector<S2LatLng>& cache) {
  uint32_t count;
  if (!r.ReadU32(count)) {
    return {ERROR_BAD_PARAMETER, "WKB: truncated MultiLineString count"};
  }
  auto multi = std::make_unique<S2MultiPolylineRegion>();
  multi->Impl().reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    WkbType sub_type;
    if (auto res = ReadHeader(r, sub_type); res.fail()) {
      return res;
    }
    if (sub_type != WkbType::LineString) {
      return {ERROR_BAD_PARAMETER, "WKB: MultiLineString member ", i,
              " is not a LineString"};
    }
    if (auto res = ReadLineStringVertices(r, cache); res.fail()) {
      return res;
    }
    std::vector<S2Point> pts;
    pts.reserve(cache.size());
    for (const auto& ll : cache) {
      pts.push_back(ll.ToPoint());
    }
    S2Polyline line{pts, S2Debug::DISABLE};
    if (!line.IsValid()) {
      return {ERROR_BAD_PARAMETER, "WKB: MultiLineString member ", i,
              " is not a valid S2Polyline"};
    }
    multi->Impl().push_back(std::move(line));
  }
  region.reset(std::move(multi), ShapeContainer::Type::S2Multipolyline);
  return {};
}

Result ParseMultiPolygon(WkbReader& r, ShapeContainer& region,
                         std::vector<S2LatLng>& cache) {
  uint32_t count;
  if (!r.ReadU32(count)) {
    return {ERROR_BAD_PARAMETER, "WKB: truncated MultiPolygon count"};
  }
  // Flatten all member loops into one S2Polygon -- S2 handles disjoint
  // polygons natively through S2Polygon::InitNested.
  std::vector<std::unique_ptr<S2Loop>> all_loops;
  for (uint32_t i = 0; i < count; ++i) {
    WkbType sub_type;
    if (auto res = ReadHeader(r, sub_type); res.fail()) {
      return res;
    }
    if (sub_type != WkbType::Polygon) {
      return {ERROR_BAD_PARAMETER, "WKB: MultiPolygon member ", i,
              " is not a Polygon"};
    }
    std::vector<std::unique_ptr<S2Loop>> loops;
    if (auto res = ReadPolygonLoops(r, loops, cache); res.fail()) {
      return res;
    }
    for (auto& loop : loops) {
      all_loops.push_back(std::move(loop));
    }
  }
  auto poly = std::make_unique<S2Polygon>();
  poly->InitNested(std::move(all_loops));
  region.reset(std::move(poly), ShapeContainer::Type::S2Polygon);
  return {};
}

Result ParseGeometry(WkbReader& r, ShapeContainer& region,
                     std::vector<S2LatLng>& cache) {
  WkbType type;
  if (auto res = ReadHeader(r, type); res.fail()) {
    return res;
  }
  switch (type) {
    case WkbType::Point:
      return ParsePoint(r, region);
    case WkbType::LineString:
      return ParseLineString(r, region, cache);
    case WkbType::Polygon:
      return ParsePolygon(r, region, cache);
    case WkbType::MultiPoint:
      return ParseMultiPoint(r, region);
    case WkbType::MultiLineString:
      return ParseMultiLineString(r, region, cache);
    case WkbType::MultiPolygon:
      return ParseMultiPolygon(r, region, cache);
    case WkbType::GeometryCollection:
      return {ERROR_BAD_PARAMETER, "WKB: GeometryCollection is not supported"};
  }
  return {ERROR_BAD_PARAMETER, "WKB: unreachable geometry type"};
}

}  // namespace

Result ParseShapeWKB(std::string_view bytes, ShapeContainer& region,
                     std::vector<S2LatLng>& cache) {
  WkbReader r{bytes};
  return ParseGeometry(r, region, cache);
}

}  // namespace sdb::geo
