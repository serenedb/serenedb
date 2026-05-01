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

// Postgres-style FTS surface. All stubs; the filter builder claims
// them at bind time and builds the iresearch filter.
inline constexpr std::string_view kTSQueryTypeName = "TSQUERY";
// Distinct alias used for the result of `EXPR::tokenize(...)`. We
// can't reuse `TSQUERY` here because DuckDB short-circuits same-alias
// casts as no-ops (compares LogicalTypeId + alias, ignoring
// ExtensionTypeInfo modifiers), so a `TSQUERY -> TSQUERY-with-modifier`
// cast would never reach the filter builder. Giving the modified type
// its own alias keeps the BoundCastExpression alive in the bound tree
// so the modifier is observable downstream. Implicit casts wire it
// to flow freely wherever TSQUERY is expected.
inline constexpr std::string_view kTokenizedTSQueryTypeName =
  "TOKENIZED_TSQUERY";
inline constexpr std::string_view kTokenizerTypeName = "tokenize";
// Distinct alias for `EXPR::boost(K)` -- different from
// TOKENIZED_TSQUERY so the two casts compose in either order on a
// compound expression (each cast wrapper survives because its alias
// differs from its source).
inline constexpr std::string_view kBoostedTSQueryTypeName = "BOOSTED_TSQUERY";
inline constexpr std::string_view kBoostTypeName = "boost";

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
inline constexpr std::string_view kTSQRange = "ts_between";
inline constexpr std::string_view kTSQRegexp = "ts_regexp";
// Elasticsearch-style bool query: ts_compound(must, must_not, should
// [, min_should_match]). Each of the first three args is TSQUERY,
// TSQUERY[], or NULL.
inline constexpr std::string_view kTSQCompound = "ts_compound";

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

// Opaque logical type backing TSQUERY. Represented as VARCHAR+alias so
// storage/IO paths stay standard; the stubs never run so the byte slot is
// unused in practice. Mirrors the JSON type convention in DuckDB.
duckdb::LogicalType MakeTSQueryType();

// Looks up the named catalog tokenizer in the current transaction's
// snapshot and returns an owned AnalyzerWrapper. The caller controls
// the wrapper's lifetime: when destroyed, the underlying analyzer
// goes back to the Tokenizer's pool. Returns a null wrapper if the
// name doesn't resolve.
catalog::Tokenizer::TokenizerWrapper ResolveTokenizerAnalyzer(
  duckdb::ClientContext& context, std::string_view name);

// Pseudo-functions that are claimed by the iresearch_plan rule and
// turn into projected columns on the SearchScan rather than running
// per-row at execution time. Scorer parameters are constants; the
// rule extracts them at compile time and threads them into bind_data
// so the runtime executor doesn't re-parse per row.
//
//   bm25(tableoid [, k1 DOUBLE, b DOUBLE])    -> FLOAT
//   tfidf(tableoid [, with_norms BOOLEAN])    -> FLOAT
//   offsets(col)                          -> BIGINT[]
//
// bm25 / tfidf need a scan anchor; the convention is `tableoid` so
// the binding survives projection pushdown. offsets takes the
// indexed column directly (the column ref's own binding.table_index
// is enough -- no separate anchor needed).
inline constexpr std::string_view kBm25 = "bm25";
inline constexpr std::string_view kTfidf = "tfidf";
inline constexpr std::string_view kRawTf = "raw_tf";
inline constexpr std::string_view kLmJm = "lm_jm";
inline constexpr std::string_view kLmDirichlet = "lm_dirichlet";
inline constexpr std::string_view kIndriDirichlet = "indri_dirichlet";
inline constexpr std::string_view kDfi = "dfi";
inline constexpr std::string_view kOffsets = "offsets";

void RegisterSearchFunctions(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector
