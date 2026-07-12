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
#include "basics/exceptions.h"
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
void WithGeometryReader(WkbCursor& cursor, Func&& func) {
  uint8_t b;
  if (!cursor.ReadByte(b)) {
    SDB_THROW(ERROR_BAD_PARAMETER, "WKB: truncated byte-order byte");
  }
  if (b > 1) {
    SDB_THROW(ERROR_BAD_PARAMETER, "WKB: bad byte-order byte");
  }
  irs::ResolveBool(b == 1, [&]<bool LittleEndian> {
    WkbReader<LittleEndian> r{cursor};
    std::forward<Func>(func).template operator()<WkbReader<LittleEndian>>(r);
  });
}

// Read the type word + optional SRID and return the bare geometry type.
// The byte-order byte is consumed by WithGeometryReader before this is
// called -- the templated reader's Byteswap value already reflects it.
// Rejects Z/M dimensions and non-CRS84 SRIDs.
template<class R>
void ReadHeader(R& r, WkbType& type) {
  uint32_t raw_type;
  if (!r.ReadU32(raw_type)) {
    SDB_THROW(ERROR_BAD_PARAMETER, "WKB: truncated type word");
  }
  if ((raw_type & (kEwkbZFlag | kEwkbMFlag)) != 0) {
    SDB_THROW(ERROR_BAD_PARAMETER,
              "WKB: Z/M dimensions not supported (CRS84 is 2D)");
  }
  const bool has_srid = (raw_type & kEwkbSridFlag) != 0;
  const uint32_t bare = raw_type & 0x000000FF;  // ISO WKB PointZ=1001 also
                                                // lands here after masking,
                                                // but Z/M check above rejects.
  if (bare < std::to_underlying(WkbType::Min) ||
      bare > std::to_underlying(WkbType::Max)) {
    SDB_THROW(ERROR_BAD_PARAMETER, "WKB: unsupported geometry type");
  }
  if (has_srid) {
    uint32_t srid;
    if (!r.ReadU32(srid)) {
      SDB_THROW(ERROR_BAD_PARAMETER, "WKB: truncated SRID");
    }
    if (static_cast<int32_t>(srid) != kCRS84Srid) {
      SDB_THROW(ERROR_BAD_PARAMETER,
                "WKB: only SRID 4326 (CRS84) is supported; got ", srid);
    }
  }
  type = static_cast<WkbType>(bare);
}

template<class R>
void ReadPoint(R& r, S2LatLng& out) {
  if (!r.ReadLatLng(out)) {
    SDB_THROW(ERROR_BAD_PARAMETER, "WKB: truncated Point coordinates");
  }
}

template<class R>
void ReadLineStringVertices(R& r, std::vector<S2LatLng>& cache) {
  uint32_t count;
  if (!r.ReadU32(count)) {
    SDB_THROW(ERROR_BAD_PARAMETER, "WKB: truncated LineString vertex count");
  }
  cache.clear();
  cache.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    S2LatLng ll;
    if (!r.ReadLatLng(ll)) {
      SDB_THROW(ERROR_BAD_PARAMETER, "WKB: truncated LineString vertex ", i);
    }
    cache.push_back(ll);
  }
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
void ReadPolygonLoops(R& r, std::vector<std::unique_ptr<S2Loop>>& out) {
  uint32_t ring_count;
  if (!r.ReadU32(ring_count)) {
    SDB_THROW(ERROR_BAD_PARAMETER, "WKB: truncated Polygon ring count");
  }
  out.clear();
  out.reserve(ring_count);
  S2Loop* first = nullptr;
  for (uint32_t i = 0; i < ring_count; ++i) {
    uint32_t vcount;
    if (!r.ReadU32(vcount)) {
      SDB_THROW(ERROR_BAD_PARAMETER, "WKB: truncated Polygon ring-", i,
                " vertex count");
    }
    if (vcount < 4) {
      SDB_THROW(ERROR_BAD_PARAMETER,
                "WKB: Polygon ring must have >= 4 vertices (closed); ring ", i,
                " has ", vcount);
    }
    std::vector<S2Point> pts;
    pts.reserve(vcount - 1);
    S2LatLng ll;
    for (uint32_t v = 0; v < vcount; ++v) {
      if (!r.ReadLatLng(ll)) {
        SDB_THROW(ERROR_BAD_PARAMETER, "WKB: truncated Polygon ring-", i,
                  " vertex ", v);
      }
      // Skip the last vertex: WKB closes the ring, S2Loop must be open.
      if (v + 1 < vcount) {
        pts.push_back(ll.ToPoint());
      }
    }
    auto loop = std::make_unique<S2Loop>(pts, S2Debug::DISABLE);
    if (!loop->IsValid()) {
      SDB_THROW(ERROR_BAD_PARAMETER, "WKB: Polygon ring-", i,
                " is not a valid ", "S2 loop");
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
        SDB_THROW(ERROR_BAD_PARAMETER, "WKB: Polygon ring-", i,
                  " is not a hole in the outer ring");
      }
    }
  }
}

template<class R>
void ParseGeometry(R& r, ShapeContainer& region);

template<class R>
void ParsePoint(R& r, ShapeContainer& region) {
  S2LatLng ll;
  ReadPoint(r, ll);
  region.reset(ll.ToPoint());
}

template<class R>
void ParseLineString(R& r, ShapeContainer& region) {
  std::vector<S2LatLng> verts;
  ReadLineStringVertices(r, verts);
  std::vector<S2Point> pts;
  pts.reserve(verts.size());
  for (const auto& ll : verts) {
    pts.push_back(ll.ToPoint());
  }
  auto line = std::make_unique<S2Polyline>(pts, S2Debug::DISABLE);
  if (!line->IsValid()) {
    SDB_THROW(ERROR_BAD_PARAMETER, "WKB: LineString is not a valid S2Polyline");
  }
  region.reset(std::move(line), ShapeContainer::Type::S2Polyline);
}

template<class R>
void ParsePolygon(R& r, ShapeContainer& region) {
  std::vector<std::unique_ptr<S2Loop>> loops;
  ReadPolygonLoops(r, loops);
  auto poly = std::make_unique<S2Polygon>();
  poly->InitNested(std::move(loops));
  region.reset(std::move(poly), ShapeContainer::Type::S2Polygon);
}

// Each Multi* member is a complete WKB sub-geometry with its own
// byte-order byte. WithGeometryReader consumes that BOM, resolves
// Byteswap at compile time, and constructs a fresh WkbReader<Sub>
// over the parent's cursor for the duration of the sub-parse. The
// outer reader (`r`) is unused inside the lambda but its cursor flows
// through transparently.
template<class R>
void ParseMultiPoint(R& r, ShapeContainer& region) {
  uint32_t count;
  if (!r.ReadU32(count)) {
    SDB_THROW(ERROR_BAD_PARAMETER, "WKB: truncated MultiPoint count");
  }
  auto multi = std::make_unique<S2MultiPointRegion>();
  multi->Impl().reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    WithGeometryReader(r.cursor(), [&]<class Sub>(Sub& sub) {
      WkbType sub_type;
      ReadHeader(sub, sub_type);
      if (sub_type != WkbType::Point) {
        SDB_THROW(ERROR_BAD_PARAMETER, "WKB: MultiPoint member ", i,
                  " is not a Point");
      }
      S2LatLng ll;
      ReadPoint(sub, ll);
      multi->Impl().push_back(ll.ToPoint());
    });
  }
  region.reset(std::move(multi), ShapeContainer::Type::S2Multipoint);
}

template<class R>
void ParseMultiLineString(R& r, ShapeContainer& region) {
  uint32_t count;
  if (!r.ReadU32(count)) {
    SDB_THROW(ERROR_BAD_PARAMETER, "WKB: truncated MultiLineString count");
  }
  auto multi = std::make_unique<S2MultiPolylineRegion>();
  multi->Impl().reserve(count);
  // Reused across sub-iterations so per-line scratch allocations don't
  // hit the allocator inside the loop.
  std::vector<S2LatLng> verts;
  for (uint32_t i = 0; i < count; ++i) {
    WithGeometryReader(r.cursor(), [&]<class Sub>(Sub& sub) {
      WkbType sub_type;
      ReadHeader(sub, sub_type);
      if (sub_type != WkbType::LineString) {
        SDB_THROW(ERROR_BAD_PARAMETER, "WKB: MultiLineString member ", i,
                  " is not a LineString");
      }
      ReadLineStringVertices(sub, verts);
      std::vector<S2Point> pts;
      pts.reserve(verts.size());
      for (const auto& ll : verts) {
        pts.push_back(ll.ToPoint());
      }
      S2Polyline line{pts, S2Debug::DISABLE};
      if (!line.IsValid()) {
        SDB_THROW(ERROR_BAD_PARAMETER, "WKB: MultiLineString member ", i,
                  " is not a valid S2Polyline");
      }
      multi->Impl().push_back(std::move(line));
    });
  }
  region.reset(std::move(multi), ShapeContainer::Type::S2Multipolyline);
}

template<class R>
void ParseMultiPolygon(R& r, ShapeContainer& region) {
  uint32_t count;
  if (!r.ReadU32(count)) {
    SDB_THROW(ERROR_BAD_PARAMETER, "WKB: truncated MultiPolygon count");
  }
  // Flatten all member loops into one S2Polygon -- S2 handles disjoint
  // polygons natively through S2Polygon::InitNested.
  std::vector<std::unique_ptr<S2Loop>> all_loops;
  for (uint32_t i = 0; i < count; ++i) {
    WithGeometryReader(r.cursor(), [&]<class Sub>(Sub& sub) {
      WkbType sub_type;
      ReadHeader(sub, sub_type);
      if (sub_type != WkbType::Polygon) {
        SDB_THROW(ERROR_BAD_PARAMETER, "WKB: MultiPolygon member ", i,
                  " is not a Polygon");
      }
      std::vector<std::unique_ptr<S2Loop>> loops;
      ReadPolygonLoops(sub, loops);
      for (auto& loop : loops) {
        all_loops.push_back(std::move(loop));
      }
    });
  }
  auto poly = std::make_unique<S2Polygon>();
  poly->InitNested(std::move(all_loops));
  region.reset(std::move(poly), ShapeContainer::Type::S2Polygon);
}

template<class R>
void ParseGeometry(R& r, ShapeContainer& region) {
  WkbType type;
  ReadHeader(r, type);
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
      SDB_THROW(ERROR_BAD_PARAMETER,
                "WKB: GeometryCollection is not supported");
  }
  SDB_THROW(ERROR_BAD_PARAMETER, "WKB: unreachable geometry type");
}

}  // namespace

// The WKB subsystem boundary: deep readers throw, this converts to a value.
bool ParseShapeWKB(std::string_view bytes, ShapeContainer& region) {
  try {
    WkbCursor cursor{bytes};
    WithGeometryReader(cursor,
                       [&]<class R>(R& r) { ParseGeometry(r, region); });
    return true;
  } catch (const sdb::basics::Exception&) {
    return false;
  }
}

}  // namespace sdb::geo
