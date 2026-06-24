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

#include <duckdb/common/types.hpp>
#include <duckdb/main/database.hpp>
#include <iresearch/analysis/analyzer.hpp>

#include "catalog/tokenizer.h"

namespace sdb::connector {

inline constexpr std::string_view kTSQueryTypeName = "TSQUERY";
inline constexpr std::string_view kTokenizerTypeName = "tokenize";
inline constexpr std::string_view kTokenizedTSQueryTypeName =
  "TOKENIZED_TSQUERY";
inline constexpr std::string_view kBoostTypeName = "boost";
inline constexpr std::string_view kBoostedTSQueryTypeName = "BOOSTED_TSQUERY";

// TSQUERY leaf constructors (unprefixed). Produce a TSQUERY value;
// stubs throw at runtime -- the filter builder claims them at bind.
inline constexpr std::string_view kTSQPhrase = "ts_phrase";
inline constexpr std::string_view kTSQNgram = "ts_ngram";
inline constexpr std::string_view kTSQLike = "ts_like";
inline constexpr std::string_view kTSQPrefix = "ts_starts_with";
inline constexpr std::string_view kTSQLevenshtein = "ts_levenshtein";
inline constexpr std::string_view kTSQAnyOf = "ts_any";
inline constexpr std::string_view kTSQAllOf = "ts_all";
inline constexpr std::string_view kTSQTokenize = "ts_tokenize";
inline constexpr std::string_view kTSQBetween = "ts_between";
inline constexpr std::string_view kTSQRegexp = "ts_regexp";
// Elasticsearch-style bool query: ts_compound(must, must_not, should
// [, min_should_match]). Each of the first three args is TSQUERY,
// TSQUERY[], or NULL.
inline constexpr std::string_view kTSQCompound = "ts_compound";

// Existence predicates: `col @@ ts_is_not_null()` matches rows that have
// a value for `col` in the cs; `col @@ ts_is_null()` matches the
// complement. Both emit `irs::ByColumnExistence` (negated for IS NULL).
// Useful pushdown shape for INCLUDE-only columns -- the implicit SQL
// `IS NULL` / `IS NOT NULL` operator goes through `ByTerm` against a
// null-mangled posting list, which only exists for indexed columns.
inline constexpr std::string_view kTSQIsNull = "ts_is_null";
inline constexpr std::string_view kTSQIsNotNull = "ts_is_not_null";

// Single-bound range constructors. Each takes one value (VARCHAR /
// numeric / BOOLEAN) and emits irs::ByRange (VARCHAR / BOOLEAN
// columns) or irs::ByGranularRange (numeric columns) with the bound
// on the appropriate side; the other side stays unbounded.
//
// VARCHAR bounds are tokenised through the ambient analyzer
// (identity column -> raw input; segmenting column -> lowercased /
// stemmed token). Multi-token tokenisation is rejected.
//
// For unbounded-on-one-side semantics on a single call, use
// RANGE(NULL, max, ...) or RANGE(min, NULL, ...) instead.
inline constexpr std::string_view kTSQLess = "ts_lt";
inline constexpr std::string_view kTSQLessEq = "ts_le";
inline constexpr std::string_view kTSQGreater = "ts_gt";
inline constexpr std::string_view kTSQGreaterEq = "ts_ge";

// PG-compat tsquery constructor family (input-string driven, all use
// the ambient column analyzer).
inline constexpr std::string_view kToTsquery = "to_tsquery";
inline constexpr std::string_view kPlainToTsquery = "plainto_tsquery";
inline constexpr std::string_view kPhraseToTsquery = "phraseto_tsquery";
inline constexpr std::string_view kWebsearchToTsquery = "websearch_to_tsquery";
inline constexpr std::string_view kTsqueryPhrase = "tsquery_phrase";

// TSQUERY combinators -- PG-style doubled glyphs.
inline constexpr std::string_view kTSQueryOr = "||";
inline constexpr std::string_view kTSQueryAnd = "&&";
inline constexpr std::string_view kTSQueryNot = "!!";
inline constexpr std::string_view kTSQueryBoost = "^";
inline constexpr std::string_view kTSQueryPhraseSeq = "##";

// @@ match: commutative (ANY, TSQUERY) -> BOOLEAN. Stub; filter builder
// claims the call at bind time and extracts the column from whichever
// side resolves to a column reference.
inline constexpr std::string_view kTSQueryMatch = "@@";

// Sugar predicates -- rewritten to `col @@ ts_*(...)` at filter-build.
inline constexpr std::string_view kPhraseMatches = "phrase_matches";
inline constexpr std::string_view kNgramMatches = "ngram_matches";
inline constexpr std::string_view kLevenshteinMatches = "levenshtein_matches";
inline constexpr std::string_view kHasAllTokens = "has_all_tokens";
inline constexpr std::string_view kHasAnyTokens = "has_any_tokens";

// Highlighting + position projections.
inline constexpr std::string_view kTsHighlight = "ts_highlight";
inline constexpr std::string_view kOffsets = "ts_offsets";

// Geo -- ST_Distance_Centroid is only usable inside an index scan
// (the filter builder turns `expr OP const` into a GeoDistanceFilter).
inline constexpr std::string_view kGeoInRange = "ST_Distance_Between";
inline constexpr std::string_view kGeoDistance = "ST_Distance_Centroid";
inline constexpr std::string_view kGeoIntersects = "ST_Intersects";
inline constexpr std::string_view kGeoContains = "ST_Contains";

duckdb::LogicalType MakeTSQueryType();

catalog::Tokenizer::TokenizerWrapper AcquireTokenizer(
  duckdb::ClientContext& context, std::string_view name);

std::shared_ptr<catalog::Tokenizer> ResolveCatalogTokenizer(
  duckdb::ClientContext& context, std::string_view name);

void RegisterSearchFunctions(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector
