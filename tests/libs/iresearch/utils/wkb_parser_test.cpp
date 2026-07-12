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

#include <s2/s2latlng.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>

#include <bit>
#include <cstring>
#include <string>

#include "basics/errors.h"
#include "geo/s2/multi_point_region.h"
#include "geo/s2/multi_polyline_region.h"
#include "geo/shape_container.h"
#include "geo/wkb.h"
#include "gtest/gtest.h"

namespace {

using sdb::geo::ParseShapeWKB;
using sdb::geo::ShapeContainer;

// Little-endian WKB builder. Matches what native x86/arm emit by default.
class WkbBuilder {
 public:
  WkbBuilder& PutU8(uint8_t v) {
    _buf.push_back(static_cast<char>(v));
    return *this;
  }
  WkbBuilder& PutU32(uint32_t v) {
    if (std::endian::native != std::endian::little) {
      v = std::byteswap(v);
    }
    _buf.append(reinterpret_cast<const char*>(&v), sizeof(v));
    return *this;
  }
  WkbBuilder& PutDouble(double v) {
    uint64_t raw;
    std::memcpy(&raw, &v, sizeof(v));
    if (std::endian::native != std::endian::little) {
      raw = std::byteswap(raw);
    }
    _buf.append(reinterpret_cast<const char*>(&raw), sizeof(raw));
    return *this;
  }
  // lng then lat per OGC SFA.
  WkbBuilder& PutXY(double lng, double lat) {
    return PutDouble(lng).PutDouble(lat);
  }
  // Header: little-endian + type word.
  WkbBuilder& Header(uint32_t type) { return PutU8(1).PutU32(type); }
  // Extended header with SRID prefix.
  WkbBuilder& EwkbHeader(uint32_t type, uint32_t srid) {
    return PutU8(1).PutU32(type | 0x20000000).PutU32(srid);
  }
  std::string_view View() const { return _buf; }

 private:
  std::string _buf;
};

TEST(WkbParser, Point) {
  WkbBuilder b;
  b.Header(1).PutXY(6.5, 50.3);
  ShapeContainer shape;
  ASSERT_TRUE(ParseShapeWKB(b.View(), shape));
  EXPECT_EQ(ShapeContainer::Type::S2Point, shape.type());
}

TEST(WkbParser, LineString) {
  WkbBuilder b;
  b.Header(2).PutU32(3).PutXY(0.0, 0.0).PutXY(1.0, 1.0).PutXY(2.0, 1.5);
  ShapeContainer shape;
  ASSERT_TRUE(ParseShapeWKB(b.View(), shape));
  EXPECT_EQ(ShapeContainer::Type::S2Polyline, shape.type());
  ASSERT_NE(shape.region(), nullptr);
}

TEST(WkbParser, Polygon_SingleRing) {
  // CCW quad around (0,0)..(1,1); WKB must repeat first vertex at end.
  WkbBuilder b;
  b.Header(3)
    .PutU32(1)   // one ring
    .PutU32(5);  // 5 vertices (closed)
  b.PutXY(0.0, 0.0).PutXY(1.0, 0.0).PutXY(1.0, 1.0).PutXY(0.0, 1.0).PutXY(0.0,
                                                                          0.0);
  ShapeContainer shape;
  ASSERT_TRUE(ParseShapeWKB(b.View(), shape));
  EXPECT_EQ(ShapeContainer::Type::S2Polygon, shape.type());
}

TEST(WkbParser, Polygon_WithHole) {
  // Outer 10x10 square (CCW) enclosing an inner 2x2 hole (CW per OGC).
  WkbBuilder b;
  b.Header(3).PutU32(2);  // two rings: outer + hole
  // Outer ring -- CCW, 5 vertices.
  b.PutU32(5);
  b.PutXY(0.0, 0.0)
    .PutXY(10.0, 0.0)
    .PutXY(10.0, 10.0)
    .PutXY(0.0, 10.0)
    .PutXY(0.0, 0.0);
  // Hole ring -- CW, 5 vertices, inside outer.
  b.PutU32(5);
  b.PutXY(4.0, 4.0).PutXY(4.0, 6.0).PutXY(6.0, 6.0).PutXY(6.0, 4.0).PutXY(4.0,
                                                                          4.0);

  ShapeContainer shape;
  ASSERT_TRUE(ParseShapeWKB(b.View(), shape));
  ASSERT_EQ(ShapeContainer::Type::S2Polygon, shape.type());

  // Inside outer, outside hole -- contained.
  EXPECT_TRUE(shape.contains(S2LatLng::FromDegrees(1.0, 1.0).ToPoint()));
  EXPECT_TRUE(shape.contains(S2LatLng::FromDegrees(8.0, 8.0).ToPoint()));
  // Inside hole -- not contained.
  EXPECT_FALSE(shape.contains(S2LatLng::FromDegrees(5.0, 5.0).ToPoint()));
  // Outside outer -- not contained.
  EXPECT_FALSE(shape.contains(S2LatLng::FromDegrees(20.0, 20.0).ToPoint()));
}

TEST(WkbParser, Polygon_RingTooShort) {
  WkbBuilder b;
  b.Header(3).PutU32(1).PutU32(3);  // only 3 vertices, need 4
  b.PutXY(0.0, 0.0).PutXY(1.0, 0.0).PutXY(0.0, 0.0);
  ShapeContainer shape;
  EXPECT_FALSE(ParseShapeWKB(b.View(), shape));
}

TEST(WkbParser, MultiPoint) {
  WkbBuilder b;
  b.Header(4).PutU32(2);
  b.Header(1).PutXY(6.5, 50.3);
  b.Header(1).PutXY(7.0, 51.0);
  ShapeContainer shape;
  ASSERT_TRUE(ParseShapeWKB(b.View(), shape));
  EXPECT_EQ(ShapeContainer::Type::S2Multipoint, shape.type());
}

TEST(WkbParser, MultiLineString) {
  WkbBuilder b;
  b.Header(5).PutU32(2);
  b.Header(2).PutU32(2).PutXY(0.0, 0.0).PutXY(1.0, 1.0);
  b.Header(2).PutU32(2).PutXY(2.0, 2.0).PutXY(3.0, 3.0);
  ShapeContainer shape;
  ASSERT_TRUE(ParseShapeWKB(b.View(), shape));
  EXPECT_EQ(ShapeContainer::Type::S2Multipolyline, shape.type());
}

TEST(WkbParser, MultiPolygon_TwoDisjoint) {
  WkbBuilder b;
  b.Header(6).PutU32(2);
  // Polygon 1
  b.Header(3).PutU32(1).PutU32(5);
  b.PutXY(0.0, 0.0).PutXY(1.0, 0.0).PutXY(1.0, 1.0).PutXY(0.0, 1.0).PutXY(0.0,
                                                                          0.0);
  // Polygon 2
  b.Header(3).PutU32(1).PutU32(5);
  b.PutXY(5.0, 5.0).PutXY(6.0, 5.0).PutXY(6.0, 6.0).PutXY(5.0, 6.0).PutXY(5.0,
                                                                          5.0);
  ShapeContainer shape;
  ASSERT_TRUE(ParseShapeWKB(b.View(), shape));
  EXPECT_EQ(ShapeContainer::Type::S2Polygon, shape.type());
}

TEST(WkbParser, EwkbPoint_AcceptsCRS84) {
  WkbBuilder b;
  b.EwkbHeader(1, 4326).PutXY(6.5, 50.3);
  ShapeContainer shape;
  ASSERT_TRUE(ParseShapeWKB(b.View(), shape));
  EXPECT_EQ(ShapeContainer::Type::S2Point, shape.type());
}

TEST(WkbParser, EwkbPoint_RejectsNonCRS84) {
  WkbBuilder b;
  b.EwkbHeader(1, 3857).PutXY(6.5, 50.3);  // Web Mercator
  ShapeContainer shape;
  EXPECT_FALSE(ParseShapeWKB(b.View(), shape));
}

TEST(WkbParser, RejectsZM) {
  WkbBuilder b;
  b.PutU8(1).PutU32(1 | 0x80000000);  // PointZ via EWKB flag
  b.PutXY(0.0, 0.0).PutDouble(0.0);
  ShapeContainer shape;
  EXPECT_FALSE(ParseShapeWKB(b.View(), shape));
}

TEST(WkbParser, RejectsGeometryCollection) {
  WkbBuilder b;
  b.Header(7).PutU32(0);
  ShapeContainer shape;
  EXPECT_FALSE(ParseShapeWKB(b.View(), shape));
}

TEST(WkbParser, RejectsEmpty) {
  ShapeContainer shape;
  EXPECT_FALSE(ParseShapeWKB({}, shape));
}

TEST(WkbParser, RejectsTruncatedPoint) {
  WkbBuilder b;
  b.Header(1).PutDouble(6.5);  // missing latitude
  ShapeContainer shape;
  EXPECT_FALSE(ParseShapeWKB(b.View(), shape));
}

TEST(WkbParser, RejectsBadByteOrder) {
  WkbBuilder b;
  b.PutU8(2).PutU32(1).PutXY(0.0, 0.0);  // byte-order must be 0 or 1
  ShapeContainer shape;
  EXPECT_FALSE(ParseShapeWKB(b.View(), shape));
}

// Big-endian WKB encoding (byte-order byte = 0). Reuses the little-endian
// builder but explicitly byteswaps the header fields.
TEST(WkbParser, BigEndian_Point) {
  std::string buf;
  buf.push_back(static_cast<char>(0));  // big-endian
  uint32_t type = 1;
  if (std::endian::native == std::endian::little) {
    type = std::byteswap(type);
  }
  buf.append(reinterpret_cast<const char*>(&type), 4);
  double lng = 6.5, lat = 50.3;
  auto push_be_double = [&](double d) {
    uint64_t raw;
    std::memcpy(&raw, &d, 8);
    if (std::endian::native == std::endian::little) {
      raw = std::byteswap(raw);
    }
    buf.append(reinterpret_cast<const char*>(&raw), 8);
  };
  push_be_double(lng);
  push_be_double(lat);
  ShapeContainer shape;
  ASSERT_TRUE(ParseShapeWKB(buf, shape));
  EXPECT_EQ(ShapeContainer::Type::S2Point, shape.type());
}

}  // namespace
