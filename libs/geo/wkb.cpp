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

#include <absl/base/internal/endian.h>
#include <s2/s2latlng.h>
#include <s2/s2loop.h>
#include <s2/s2point.h>
#include <s2/s2point_region.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>

#include <cstring>
#include <memory>
#include <utility>

#include "basics/errors.h"
#include "basics/misc.hpp"
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

  Min = Point,
  Max = GeometryCollection,
};

// Byte-oriented cursor with typed little-/big-endian fixed-size readers.
// The cursor is the only stateful piece; templated readers are stateless
// lenses that share it by reference, so a sub-geometry can be parsed with
// a different LittleEndian value than the parent and the cursor advance
// flows back through the parent reader transparently.
class WkbCursor {
 public:
  explicit WkbCursor(std::string_view bytes) noexcept
    : _ptr{bytes.data()}, _end{bytes.data() + bytes.size()} {}

  bool ReadByte(uint8_t& out) noexcept {
    if (_ptr >= _end) {
      return false;
    }
    out = static_cast<uint8_t>(*_ptr);
    ++_ptr;
    return true;
  }

  bool ReadU32LE(uint32_t& out) noexcept {
    if (static_cast<size_t>(_end - _ptr) < sizeof(uint32_t)) {
      return false;
    }
    out = absl::little_endian::Load32(_ptr);
    _ptr += sizeof(uint32_t);
    return true;
  }

  bool ReadU32BE(uint32_t& out) noexcept {
    if (static_cast<size_t>(_end - _ptr) < sizeof(uint32_t)) {
      return false;
    }
    out = absl::big_endian::Load32(_ptr);
    _ptr += sizeof(uint32_t);
    return true;
  }

  bool ReadDoubleLE(double& out) noexcept {
    if (static_cast<size_t>(_end - _ptr) < sizeof(double)) {
      return false;
    }
    out = absl::little_endian::Load<double>(_ptr);
    _ptr += sizeof(double);
    return true;
  }

  bool ReadDoubleBE(double& out) noexcept {
    if (static_cast<size_t>(_end - _ptr) < sizeof(double)) {
      return false;
    }
    out = absl::big_endian::Load<double>(_ptr);
    _ptr += sizeof(double);
    return true;
  }

  bool Empty() const noexcept { return _ptr >= _end; }

 private:
  const char* _ptr;
  const char* _end;
};

// Stateless typed lens over a WkbCursor. `LittleEndian` is set at
// construction from the byte-order byte of the geometry currently being
// parsed (1 = LE, 0 = BE per OGC); the dispatch in ReadU32/ReadDouble
// collapses to an `if constexpr`, so the hot vertex loop is branch-free.
// Sub-geometries get their own instantiation via WithGeometryReader.
template<bool LittleEndian>
class WkbReader {
 public:
  explicit WkbReader(WkbCursor& cursor) noexcept : _cursor{&cursor} {}

  bool ReadU32(uint32_t& out) noexcept {
    if constexpr (LittleEndian) {
      return _cursor->ReadU32LE(out);
    } else {
      return _cursor->ReadU32BE(out);
    }
  }

  bool ReadDouble(double& out) noexcept {
    if constexpr (LittleEndian) {
      return _cursor->ReadDoubleLE(out);
    } else {
      return _cursor->ReadDoubleBE(out);
    }
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

  WkbCursor& cursor() noexcept { return *_cursor; }

 private:
  WkbCursor* _cursor;
};

// Reads the byte-order byte from the cursor (1 = LE, 0 = BE per OGC),
// resolves LittleEndian at compile time via irs::ResolveBool, constructs
// a WkbReader<LittleEndian> over the same cursor, and invokes `func`
// with that reader. Used both at the top level (ParseShapeWKB) and at
// every Multi* sub-geometry header so each sub-geometry can carry its
// own byte order per OGC.
template<typename Func>
Result WithGeometryReader(WkbCursor& cursor, Func&& func) {
  uint8_t b;
  if (!cursor.ReadByte(b)) {
    return {ERROR_BAD_PARAMETER, "WKB: truncated byte-order byte"};
  }
  if (b > 1) {
    return {ERROR_BAD_PARAMETER, "WKB: bad byte-order byte"};
  }
  return irs::ResolveBool(b == 1, [&]<bool LittleEndian> {
    WkbReader<LittleEndian> r{cursor};
    return std::forward<Func>(func)
      .template operator()<WkbReader<LittleEndian>>(r);
  });
}

// Read the type word + optional SRID and return the bare geometry type.
// The byte-order byte is consumed by WithGeometryReader before this is
// called -- the templated reader's Byteswap value already reflects it.
// Rejects Z/M dimensions and non-CRS84 SRIDs.
template<class R>
Result ReadHeader(R& r, WkbType& type) {
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
  if (bare < std::to_underlying(WkbType::Min) ||
      bare > std::to_underlying(WkbType::Max)) {
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

template<class R>
Result ReadPoint(R& r, S2LatLng& out) {
  if (!r.ReadLatLng(out)) {
    return {ERROR_BAD_PARAMETER, "WKB: truncated Point coordinates"};
  }
  return {};
}

template<class R>
Result ReadLineStringVertices(R& r, std::vector<S2LatLng>& cache) {
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
template<class R>
Result ReadPolygonLoops(R& r, std::vector<std::unique_ptr<S2Loop>>& out) {
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

template<class R>
Result ParseGeometry(R& r, ShapeContainer& region);

template<class R>
Result ParsePoint(R& r, ShapeContainer& region) {
  S2LatLng ll;
  if (auto res = ReadPoint(r, ll); res.fail()) {
    return res;
  }
  region.reset(ll.ToPoint());
  return {};
}

template<class R>
Result ParseLineString(R& r, ShapeContainer& region) {
  std::vector<S2LatLng> verts;
  if (auto res = ReadLineStringVertices(r, verts); res.fail()) {
    return res;
  }
  std::vector<S2Point> pts;
  pts.reserve(verts.size());
  for (const auto& ll : verts) {
    pts.push_back(ll.ToPoint());
  }
  auto line = std::make_unique<S2Polyline>(pts, S2Debug::DISABLE);
  if (!line->IsValid()) {
    return {ERROR_BAD_PARAMETER, "WKB: LineString is not a valid S2Polyline"};
  }
  region.reset(std::move(line), ShapeContainer::Type::S2Polyline);
  return {};
}

template<class R>
Result ParsePolygon(R& r, ShapeContainer& region) {
  std::vector<std::unique_ptr<S2Loop>> loops;
  if (auto res = ReadPolygonLoops(r, loops); res.fail()) {
    return res;
  }
  auto poly = std::make_unique<S2Polygon>();
  poly->InitNested(std::move(loops));
  region.reset(std::move(poly), ShapeContainer::Type::S2Polygon);
  return {};
}

// Each Multi* member is a complete WKB sub-geometry with its own
// byte-order byte. WithGeometryReader consumes that BOM, resolves
// Byteswap at compile time, and constructs a fresh WkbReader<Sub>
// over the parent's cursor for the duration of the sub-parse. The
// outer reader (`r`) is unused inside the lambda but its cursor flows
// through transparently.
template<class R>
Result ParseMultiPoint(R& r, ShapeContainer& region) {
  uint32_t count;
  if (!r.ReadU32(count)) {
    return {ERROR_BAD_PARAMETER, "WKB: truncated MultiPoint count"};
  }
  auto multi = std::make_unique<S2MultiPointRegion>();
  multi->Impl().reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    auto res =
      WithGeometryReader(r.cursor(), [&]<class Sub>(Sub& sub) -> Result {
        WkbType sub_type;
        if (auto e = ReadHeader(sub, sub_type); e.fail()) {
          return e;
        }
        if (sub_type != WkbType::Point) {
          return {ERROR_BAD_PARAMETER, "WKB: MultiPoint member ", i,
                  " is not a Point"};
        }
        S2LatLng ll;
        if (auto e = ReadPoint(sub, ll); e.fail()) {
          return e;
        }
        multi->Impl().push_back(ll.ToPoint());
        return {};
      });
    if (res.fail()) {
      return res;
    }
  }
  region.reset(std::move(multi), ShapeContainer::Type::S2Multipoint);
  return {};
}

template<class R>
Result ParseMultiLineString(R& r, ShapeContainer& region) {
  uint32_t count;
  if (!r.ReadU32(count)) {
    return {ERROR_BAD_PARAMETER, "WKB: truncated MultiLineString count"};
  }
  auto multi = std::make_unique<S2MultiPolylineRegion>();
  multi->Impl().reserve(count);
  // Reused across sub-iterations so per-line scratch allocations don't
  // hit the allocator inside the loop.
  std::vector<S2LatLng> verts;
  for (uint32_t i = 0; i < count; ++i) {
    auto res =
      WithGeometryReader(r.cursor(), [&]<class Sub>(Sub& sub) -> Result {
        WkbType sub_type;
        if (auto e = ReadHeader(sub, sub_type); e.fail()) {
          return e;
        }
        if (sub_type != WkbType::LineString) {
          return {ERROR_BAD_PARAMETER, "WKB: MultiLineString member ", i,
                  " is not a LineString"};
        }
        if (auto e = ReadLineStringVertices(sub, verts); e.fail()) {
          return e;
        }
        std::vector<S2Point> pts;
        pts.reserve(verts.size());
        for (const auto& ll : verts) {
          pts.push_back(ll.ToPoint());
        }
        S2Polyline line{pts, S2Debug::DISABLE};
        if (!line.IsValid()) {
          return {ERROR_BAD_PARAMETER, "WKB: MultiLineString member ", i,
                  " is not a valid S2Polyline"};
        }
        multi->Impl().push_back(std::move(line));
        return {};
      });
    if (res.fail()) {
      return res;
    }
  }
  region.reset(std::move(multi), ShapeContainer::Type::S2Multipolyline);
  return {};
}

template<class R>
Result ParseMultiPolygon(R& r, ShapeContainer& region) {
  uint32_t count;
  if (!r.ReadU32(count)) {
    return {ERROR_BAD_PARAMETER, "WKB: truncated MultiPolygon count"};
  }
  // Flatten all member loops into one S2Polygon -- S2 handles disjoint
  // polygons natively through S2Polygon::InitNested.
  std::vector<std::unique_ptr<S2Loop>> all_loops;
  for (uint32_t i = 0; i < count; ++i) {
    auto res =
      WithGeometryReader(r.cursor(), [&]<class Sub>(Sub& sub) -> Result {
        WkbType sub_type;
        if (auto e = ReadHeader(sub, sub_type); e.fail()) {
          return e;
        }
        if (sub_type != WkbType::Polygon) {
          return {ERROR_BAD_PARAMETER, "WKB: MultiPolygon member ", i,
                  " is not a Polygon"};
        }
        std::vector<std::unique_ptr<S2Loop>> loops;
        if (auto e = ReadPolygonLoops(sub, loops); e.fail()) {
          return e;
        }
        for (auto& loop : loops) {
          all_loops.push_back(std::move(loop));
        }
        return {};
      });
    if (res.fail()) {
      return res;
    }
  }
  auto poly = std::make_unique<S2Polygon>();
  poly->InitNested(std::move(all_loops));
  region.reset(std::move(poly), ShapeContainer::Type::S2Polygon);
  return {};
}

template<class R>
Result ParseGeometry(R& r, ShapeContainer& region) {
  WkbType type;
  if (auto res = ReadHeader(r, type); res.fail()) {
    return res;
  }
  switch (type) {
    case WkbType::Point:
      return ParsePoint(r, region);
    case WkbType::LineString:
      return ParseLineString(r, region);
    case WkbType::Polygon:
      return ParsePolygon(r, region);
    case WkbType::MultiPoint:
      return ParseMultiPoint(r, region);
    case WkbType::MultiLineString:
      return ParseMultiLineString(r, region);
    case WkbType::MultiPolygon:
      return ParseMultiPolygon(r, region);
    case WkbType::GeometryCollection:
      return {ERROR_BAD_PARAMETER, "WKB: GeometryCollection is not supported"};
  }
  return {ERROR_BAD_PARAMETER, "WKB: unreachable geometry type"};
}

}  // namespace

Result ParseShapeWKB(std::string_view bytes, ShapeContainer& region) {
  WkbCursor cursor{bytes};
  return WithGeometryReader(
    cursor, [&]<class R>(R& r) -> Result { return ParseGeometry(r, region); });
}

}  // namespace sdb::geo
