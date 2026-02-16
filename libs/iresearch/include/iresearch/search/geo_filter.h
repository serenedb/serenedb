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

#include <s2/s2region_term_indexer.h>

#include "basics/assert.h"
#include "geo/coding.h"
#include "geo/shape_container.h"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/search_range.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

enum class StoredType : uint8_t {
  // Valid GeoJson as VPack or coordinates array of two S2LatLng
  VPack = 0,
  // Valid ShapeContainer serialized as S2Region
  S2Region,
  // Same as S2Region, but contains only S2Point
  S2Point,
  // Store centroid
  S2Centroid,
};

struct GeoFilterOptionsBase {
  std::string prefix;
  S2RegionTermIndexer::Options options;
  StoredType stored{StoredType::VPack};
  sdb::geo::coding::Options coding{sdb::geo::coding::Options::Invalid};
};

enum class GeoFilterType : uint8_t {
  // Shape intersects indexed data
  Intersects = 0,
  // Shape fully contains indexed data
  Contains,
  // Shape is fully contained within indexed data
  IsContained,
};

class GeoFilter;

struct GeoFilterOptions : GeoFilterOptionsBase {
  using FilterType = GeoFilter;

  bool operator==(const GeoFilterOptions& rhs) const noexcept {
    return type == rhs.type && shape.equals(rhs.shape);
  }

  GeoFilterType type{GeoFilterType::Intersects};
  sdb::geo::ShapeContainer shape;
};

class GeoFilter final : public FilterWithField<GeoFilterOptions> {
 public:
  Query::ptr prepare(const PrepareContext& ctx) const final;
};

class GeoDistanceFilter;

struct GeoDistanceFilterOptions : GeoFilterOptionsBase {
  using FilterType = GeoDistanceFilter;

  bool operator==(const GeoDistanceFilterOptions& rhs) const noexcept {
    return origin == rhs.origin && range == rhs.range;
  }

  S2Point origin;
  SearchRange<double> range;
};

class GeoDistanceFilter final
  : public FilterWithField<GeoDistanceFilterOptions> {
 public:
  Query::ptr prepare(const PrepareContext& ctx) const final;
};

}  // namespace irs
