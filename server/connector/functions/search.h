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
inline constexpr std::string_view kPhrase = "sdb_phrase";
inline constexpr std::string_view kTermEq = "sdb_term_eq";
inline constexpr std::string_view kTermLt = "sdb_term_lt";
inline constexpr std::string_view kTermLe = "sdb_term_lte";
inline constexpr std::string_view kTermGe = "sdb_term_gte";
inline constexpr std::string_view kTermGt = "sdb_term_gt";
inline constexpr std::string_view kTermIn = "sdb_term_in";
inline constexpr std::string_view kTermLike = "sdb_term_like";
inline constexpr std::string_view kNgramMatch = "sdb_ngram_match";
inline constexpr std::string_view kLevenshteinMatch = "sdb_levenshtein_match";
inline constexpr std::string_view kBoost = "sdb_boost";

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
inline constexpr std::string_view kOffsets = "sdb_offsets";

void RegisterSearchFunctions(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector
