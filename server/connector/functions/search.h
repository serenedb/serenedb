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

#include <duckdb/main/database.hpp>
#include <string>

namespace sdb::connector {

// TODO(codeworse): add constexpr prefix + function name
inline constexpr std::string_view kPhrase = "phrase";
inline constexpr std::string_view kTermEq = "term_eq";
inline constexpr std::string_view kTermLt = "term_lt";
inline constexpr std::string_view kTermLe = "term_lte";
inline constexpr std::string_view kTermGe = "term_gte";
inline constexpr std::string_view kTermGt = "term_gt";
inline constexpr std::string_view kTermIn = "term_in";
inline constexpr std::string_view kTermLike = "term_like";
inline constexpr std::string_view kNgramMatch = "ngram_match";
inline constexpr std::string_view kLevenshteinMatch = "levenshtein_match";
inline constexpr std::string_view kBoost = "boost";

// Geo range search:
//   geo_in_range(field, centroid_geojson, min_distance, max_distance,
//                [include_min, [include_max]]) -> BOOLEAN
// Matches rows whose indexed geo value lies between min_distance and
// max_distance metres of the centroid (parsed as GeoJSON). The bracket
// (inclusive vs exclusive) defaults to inclusive on both sides.
inline constexpr std::string_view kGeoInRange = "geo_in_range";

// Geo distance pseudo-function:
//   geo_distance(field, centroid) -> DOUBLE
// Returns the geodesic distance (metres) between the indexed value and the
// centroid. Cannot be evaluated outside an inverted-index scan; the filter
// builder recognizes `geo_distance(field, centroid) OP constant` patterns and
// rewrites them into iresearch GeoDistanceFilter range queries.
inline constexpr std::string_view kGeoDistance = "geo_distance";

// Geo set-relation predicates (build into iresearch GeoFilter):
//   geo_intersects(field, shape)  -> BOOLEAN  -- shape ∩ indexed != ∅
//   geo_intersects(shape, field)  -> BOOLEAN  -- commutative
//   geo_contains(field, shape)    -> BOOLEAN  -- indexed ⊇ shape
//   geo_contains(shape, field)    -> BOOLEAN  -- shape ⊇ indexed
// `field` is a column reference (VARCHAR with GeoJSON, or GEOMETRY).
// `shape` is a constant (VARCHAR holding GeoJSON text, or GEOMETRY).
inline constexpr std::string_view kGeoIntersects = "geo_intersects";
inline constexpr std::string_view kGeoContains = "geo_contains";

// Pseudo-functions that are claimed by the iresearch_plan rule and
// turn into projected columns on the SearchScan rather than running
// per-row at execution time. Scorer parameters are constants; the
// rule extracts them at compile time and threads them into bind_data
// so the runtime executor doesn't re-parse per row.
//
//   bm25(tableoid [, k1 DOUBLE, b DOUBLE])    -> FLOAT
//   tfidf(tableoid [, with_norms BOOLEAN])    -> FLOAT
//   sdb_offsets(col)                          -> BIGINT[]
//
// bm25 / tfidf need a scan anchor; the convention is `tableoid` so
// the binding survives projection pushdown. sdb_offsets takes the
// indexed column directly (the column ref's own binding.table_index
// is enough -- no separate anchor needed).
inline constexpr std::string_view kBm25 = "bm25";
inline constexpr std::string_view kTfidf = "tfidf";
inline constexpr std::string_view kOffsets = "offsets";

void RegisterSearchFunctions(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector
