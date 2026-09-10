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

#include <s2/s2cell_id.h>
#include <s2/s2cell_union.h>
#include <s2/s2region.h>
#include <s2/s2region_coverer.h>
#include <s2/s2region_term_indexer.h>

#include <bit>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace irs::geo_terms {

inline std::string Term(std::string_view prefix, S2CellId id, bool covering,
                        char marker) {
  std::string out{prefix};
  if (covering) {
    out.push_back(marker);
  }
  const uint64_t be = std::byteswap(id.id());
  out.append(reinterpret_cast<const char*>(&be), sizeof be);
  return out;
}

inline std::vector<std::string> QueryTerms(
  const S2RegionTermIndexer::Options& options, const S2Region& region,
  std::string_view prefix) {
  S2RegionCoverer coverer{options};
  const S2CellUnion covering = coverer.GetCovering(region);
  std::vector<std::string> out;
  out.reserve(2 * covering.size());
  const char marker = options.marker_character();
  const int true_max_level = options.true_max_level();
  S2CellId prev_id = S2CellId::None();
  for (const S2CellId id : covering) {
    int level = id.level();
    out.push_back(Term(prefix, id, false, marker));
    if (options.index_contains_points_only()) {
      continue;
    }
    if (options.optimize_for_space() && level < true_max_level) {
      out.push_back(Term(prefix, id, true, marker));
    }
    while ((level -= options.level_mod()) >= options.min_level()) {
      const S2CellId ancestor_id = id.parent(level);
      if (prev_id != S2CellId::None() && prev_id.level() > level &&
          prev_id.parent(level) == ancestor_id) {
        break;
      }
      out.push_back(Term(prefix, ancestor_id, true, marker));
    }
    prev_id = id;
  }
  return out;
}

inline std::vector<std::string> QueryTerms(
  const S2RegionTermIndexer::Options& options, const S2Point& point,
  std::string_view prefix) {
  const S2CellId id{point};
  const char marker = options.marker_character();
  std::vector<std::string> out;
  int level = options.true_max_level();
  out.push_back(Term(prefix, id.parent(level), false, marker));
  if (options.index_contains_points_only()) {
    return out;
  }
  for (; level >= options.min_level(); level -= options.level_mod()) {
    out.push_back(Term(prefix, id.parent(level), true, marker));
  }
  return out;
}

}  // namespace irs::geo_terms
