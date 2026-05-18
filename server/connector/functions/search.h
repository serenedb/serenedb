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

// Match + combinators (commutative; PG-style doubled glyphs).
inline constexpr std::string_view kTSQueryMatch = "@@";
inline constexpr std::string_view kTSQueryOr = "||";
inline constexpr std::string_view kTSQueryAnd = "&&";
inline constexpr std::string_view kTSQueryNot = "!!";
inline constexpr std::string_view kTSQueryBoost = "^";
inline constexpr std::string_view kTSQueryPhraseSeq = "##";

// TSQUERY leaf constructors.
inline constexpr std::string_view kTSQPhrase = "ts_phrase";
inline constexpr std::string_view kTSQTokenize = "ts_tokenize";
inline constexpr std::string_view kTSQLike = "ts_like";
inline constexpr std::string_view kTSQPrefix = "ts_starts_with";
inline constexpr std::string_view kTSQRegexp = "ts_regexp";
inline constexpr std::string_view kTSQLevenshtein = "ts_levenshtein";
inline constexpr std::string_view kTSQNgram = "ts_ngram";
inline constexpr std::string_view kTSQAnyOf = "ts_any";
inline constexpr std::string_view kTSQAllOf = "ts_all";
inline constexpr std::string_view kTSQCompound = "ts_compound";
inline constexpr std::string_view kTSQRange = "ts_between";
// Single-bound shortcuts (ts_lt(x) == ts_between(NULL, x, ...)).
inline constexpr std::string_view kTSQLess = "ts_lt";
inline constexpr std::string_view kTSQLessEq = "ts_le";
inline constexpr std::string_view kTSQGreater = "ts_gt";
inline constexpr std::string_view kTSQGreaterEq = "ts_ge";

// PG-compat tsquery builders.
inline constexpr std::string_view kToTsquery = "to_tsquery";
inline constexpr std::string_view kPlainToTsquery = "plainto_tsquery";
inline constexpr std::string_view kPhraseToTsquery = "phraseto_tsquery";
inline constexpr std::string_view kWebsearchToTsquery = "websearch_to_tsquery";
inline constexpr std::string_view kTsqueryPhrase = "tsquery_phrase";

// Sugar predicates -- rewritten to `col @@ ts_*(...)` at filter-build.
inline constexpr std::string_view kPhraseMatches = "phrase_matches";
inline constexpr std::string_view kNgramMatches = "ngram_matches";
inline constexpr std::string_view kLevenshteinMatches = "levenshtein_matches";
inline constexpr std::string_view kHasAllTokens = "has_all_tokens";
inline constexpr std::string_view kHasAnyTokens = "has_any_tokens";

// Highlighting + position projections.
inline constexpr std::string_view kTsHeadline = "ts_headline";
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
