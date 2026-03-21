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

#include "utils.h"

#include <geodesic.h>
#include <s2/s2latlng.h>
#include <s2/s2region_coverer.h>
#include <vpack/vpack_helper.h>

#include <vector>

#include "basics/debugging.h"
#include "geo/ellipsoid.h"
#include "geo/shape_container.h"

namespace sdb::geo::utils {

Result IndexCellsLatLng(vpack::Slice data, bool geo_json,
                        std::vector<S2CellId>& cells, S2Point& centroid) {
  if (!data.isArray()) [[unlikely]] {
    return {ERROR_BAD_PARAMETER};
  }
  vpack::ArrayIterator it{data};
  if (it.size() != 2) [[unlikely]] {
    return {ERROR_BAD_PARAMETER};
  }
  const auto first = *it;
  if (!first.isNumber<double>()) [[unlikely]] {
    return {ERROR_BAD_PARAMETER};
  }
  ++it;
  const auto second = *it;
  if (!second.isNumber<double>()) [[unlikely]] {
    return {ERROR_BAD_PARAMETER};
  }
  double lat, lon;
  if (geo_json) {
    lon = first.getNumber<double>();
    lat = second.getNumber<double>();
  } else {
    lat = first.getNumber<double>();
    lon = second.getNumber<double>();
  }
  auto ll = S2LatLng::FromDegrees(lat, lon).Normalized();
  centroid = ll.ToPoint();
  cells.emplace_back(centroid);
  return {};
}

/// will return all the intervals including the cells containing them
/// in the less detailed levels. Should allow us to scan all intervals
/// which may contain intersecting geometries
void ScanIntervals(const QueryParams& params,
                   const std::vector<S2CellId>& cover,
                   std::vector<Interval>& sorted_intervals) {
  SDB_ASSERT(params.cover.worst_indexed_level > 0);
  if (cover.empty()) {
    return;
  }
  // reserve some space
  const auto pl = static_cast<size_t>(
    std::max(cover[0].level() - params.cover.worst_indexed_level, 0));
  sorted_intervals.reserve(cover.size() + pl * cover.size());

  // prefix matches
  for (const auto& prefix : cover) {
    if (prefix.is_leaf()) {
      sorted_intervals.emplace_back(prefix, prefix);
    } else {
      sorted_intervals.emplace_back(prefix.range_min(), prefix.range_max());
    }
  }

  if (!params.points_only || params.filter_type == FilterType::Intersects) {
    // we need to find larger cells that may still contain (parts of) the cover,
    // these are parent cells, up to the minimum allowed cell level allowed in
    // the index. In that case we do not need to look at all sub-cells only
    // at the exact parent cell id. E.g. we got cover cell id [47|11|50]; we do
    // not need
    // to look at [47|1|40] or [47|11|60] because these cells don't intersect,
    // but polygons indexed with exact cell id [47|11] still might.
    containers::FlatHashSet<uint64_t> parent_set;
    for (const S2CellId& interval : cover) {
      S2CellId cell = interval;

      // add all parent cells of our "exact" cover
      while (params.cover.worst_indexed_level < cell.level()) {  // don't use
        // level < 0
        cell = cell.parent();
        parent_set.insert(cell.id());
      }
    }
    // just add them, sort them later
    for (uint64_t exact : parent_set) {
      sorted_intervals.emplace_back(S2CellId(exact), S2CellId(exact));
    }
  }

  // sort these disjunctive intervals
  std::sort(sorted_intervals.begin(), sorted_intervals.end(),
            Interval::compare);

#ifdef SDB_DEV
  //  constexpr size_t diff = 64;
  for (size_t i = 0; i < sorted_intervals.size() - 1; i++) {
    SDB_ASSERT(sorted_intervals[i].range_min <= sorted_intervals[i].range_max);
    SDB_ASSERT(sorted_intervals[i].range_max <
               sorted_intervals[i + 1].range_min);
    /*
    if (std::abs((sortedIntervals[i].max.id() -
                  sortedIntervals[i + 1].min.id())) < diff) {
      sortedIntervals[i].max = sortedIntervals.min
    }
    */
  }
  SDB_ASSERT(!sorted_intervals.empty());
  SDB_ASSERT(sorted_intervals[0].range_min < sorted_intervals.back().range_max);
#endif
}

double GeodesicDistance(const S2LatLng& p1, const S2LatLng& p2,
                        const geo::Ellipsoid& e) {
  // Use Karney's algorithm
  geod_geodesic g{};
  geod_init(&g, e.equator_radius(), e.flattening());

  double dist;
  geod_inverse(&g, p1.lat().degrees(), p1.lng().degrees(), p2.lat().degrees(),
               p2.lng().degrees(), &dist, nullptr, nullptr);

  return dist;
}

}  // namespace sdb::geo::utils
