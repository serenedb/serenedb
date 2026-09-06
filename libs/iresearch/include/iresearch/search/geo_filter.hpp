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

#include <s2/s2cap.h>
#include <s2/s2region_term_indexer.h>

#include "basics/assert.h"
#include "geo/coding.h"
#include "geo/shape_container.h"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/search_range.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

enum class StoredType : uint8_t {
  Source = 0,
  S2Region,
  S2Point,
  S2Centroid,
};

struct GeoFilterOptionsBase {
  std::string prefix;
  S2RegionTermIndexer::Options options;
  StoredType stored{StoredType::Source};
  sdb::geo::coding::Options coding{sdb::geo::coding::Options::Invalid};
  field_id store_field_id{irs::field_limits::invalid()};
  bool source_is_wkb{false};
  bool source_is_point{false};
  std::vector<std::string> point_latitude;
  std::vector<std::string> point_longitude;
};

enum class GeoFilterType : uint8_t {
  Intersects = 0,
  Contains,
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
  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

  PrepareCollector::ptr MakeCollectorImpl(const Scorer* scorer,
                                          StatsArena& stats,
                                          uint32_t threads) const final;
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
  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

  PrepareCollector::ptr MakeCollectorImpl(const Scorer* scorer,
                                          StatsArena& stats,
                                          uint32_t threads) const final;
};

struct GeoIntersectsAcceptor {
  const sdb::geo::ShapeContainer* filter_shape;

  bool operator()(const sdb::geo::ShapeContainer& indexed_shape) const {
    return filter_shape->intersects(indexed_shape);
  }
};

struct GeoContainsAcceptor {
  const sdb::geo::ShapeContainer* filter_shape;

  bool operator()(const sdb::geo::ShapeContainer& indexed_shape) const {
    return filter_shape->contains(indexed_shape);
  }
};

struct GeoIsContainedAcceptor {
  const sdb::geo::ShapeContainer* filter_shape;

  bool operator()(const sdb::geo::ShapeContainer& indexed_shape) const {
    return indexed_shape.contains(*filter_shape);
  }
};

template<bool MinIncl, bool MaxIncl>
struct GeoDistanceRangeAcceptor {
  S2Cap min;
  S2Cap max;

  bool operator()(const sdb::geo::ShapeContainer& shape) const {
    const auto point = shape.centroid();

    return !(MinIncl ? min.InteriorContains(point) : min.Contains(point)) &&
           (MaxIncl ? max.Contains(point) : max.InteriorContains(point));
  }
};

template<bool Incl>
struct GeoDistanceAcceptor {
  S2Cap filter;

  bool operator()(const sdb::geo::ShapeContainer& shape) const {
    const auto point = shape.centroid();

    return Incl ? filter.Contains(point) : filter.InteriorContains(point);
  }
};

}  // namespace irs
