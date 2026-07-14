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

#pragma once

#include <string>
#include <string_view>

namespace sdb::geo {

class ShapeContainer;

// Parse a WKB (Well-Known Binary) byte string into a ShapeContainer.
//
// Supports 2D Point, LineString, Polygon, MultiPoint, MultiLineString,
// MultiPolygon (OGC SFA types 1-6). Handles both plain WKB and EWKB with
// an optional SRID prefix; if SRID is present it must equal 4326 (CRS84).
// Z/M dimensions are not supported (the column-level CRS84 contract
// established at CREATE INDEX time implies 2D).
//
// WKB coordinate order is (longitude, latitude) per OGC SFA.
bool ParseShapeWKB(std::string_view bytes, ShapeContainer& region);

}  // namespace sdb::geo
