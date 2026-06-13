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
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "network/http/response_writer.h"

namespace sdb::network::http::es {

// Field name -> ES type, from the index mapping. Translation is type-aware:
// text fields query through the inverted index, everything else through
// plain predicates, unmapped fields match nothing (like ES). "_id" is
// implicitly a keyword.
using FieldTypes = containers::FlatHashMap<std::string, std::string>;

// Parses es_mapping() output ({"properties":{"f":{"type":"text"},...}}).
bool ParseFieldTypes(std::string_view mappings_json, FieldTypes& out);

// One aggregation request; sub-aggregations are not supported.
struct Aggregation {
  enum class Kind {
    kTerms,
    kDateHistogram,
    kMin,
    kMax,
    kAvg,
    kSum,
    kValueCount,
    kCardinality,
  };
  std::string name;
  Kind kind;
  std::string field;
  // date_histogram: the date_trunc unit resolved from calendar_interval.
  std::string interval;
  // terms only.
  int64_t size = 10;
};

// A parsed _search/_count request body, translated to SQL fragments. The
// query subset: match_all, match (operator and/or), match_phrase, term,
// range, bool (must/filter/should/must_not, minimum_should_match 0/1); plus
// size, from, sort, boolean _source and flat aggs (terms, date_histogram,
// min/max/avg/sum/value_count/cardinality).
//
// match clauses become @@ ts_tokenize/plainto_tsquery predicates, which only
// plan on a relation with the inverted index -- the caller must target
// "<index>$text" when uses_match is set. A should group mixing match and
// filter clauses is rejected: the planner claims OR groups all-or-nothing,
// and an unclaimed @@ throws at runtime.
struct SearchRequest {
  std::string where;     // translated query, empty = match_all
  std::string order_by;  // full ORDER BY clause, empty = none
  // The raw "query" JSON, byte-exact; a scroll id carries it so every page
  // re-translates instead of round-tripping SQL through the client.
  std::string query_raw;
  // Sort columns, in order; also appended to the SELECT list so the handler
  // can emit per-hit "sort" arrays.
  std::vector<std::string> sort_fields;
  std::vector<Aggregation> aggs;
  int64_t size = 10;
  int64_t from = 0;
  bool include_source = true;
  bool uses_match = false;
};

// Parses a _search body ('' = match_all). On failure writes the ES error
// envelope and returns false.
bool ParseSearchBody(std::string_view body, const FieldTypes& fields,
                     SearchRequest& out, HttpResponseWriter& writer);

// Translates a query container captured in query_raw (scroll continuation);
// '' = match_all. Fills where/uses_match only.
bool TranslateStoredQuery(std::string_view query_json, const FieldTypes& fields,
                          SearchRequest& out, HttpResponseWriter& writer);

// Parses a _count body: only {"query": {...}} (or nothing) is legal; fills
// where/uses_match. On failure writes the error envelope and returns false.
bool ParseCountBody(std::string_view body, const FieldTypes& fields,
                    SearchRequest& out, HttpResponseWriter& writer);

}  // namespace sdb::network::http::es
