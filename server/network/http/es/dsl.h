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

#include "network/http/response_writer.h"

namespace sdb::network::http::es {

// A parsed _search/_count request body, translated to SQL fragments. The
// query subset: match_all, match (operator and/or), term, range, bool
// (must/filter/should/must_not, minimum_should_match 0/1); plus size, from,
// sort and boolean _source.
//
// match clauses become @@ ts_tokenize/plainto_tsquery predicates, which only
// plan on a relation with the inverted index — the caller must target
// "<index>$text" when uses_match is set. A should group mixing match and
// filter clauses is rejected: the planner claims OR groups all-or-nothing,
// and an unclaimed @@ throws at runtime.
struct SearchRequest {
  std::string where;     // translated query, empty = match_all
  std::string order_by;  // full ORDER BY clause, empty = none
  // Sort columns, in order; also appended to the SELECT list so the handler
  // can emit per-hit "sort" arrays.
  std::vector<std::string> sort_fields;
  int64_t size = 10;
  int64_t from = 0;
  bool include_source = true;
  bool uses_match = false;
};

// Parses a _search body ('' = match_all). On failure writes the ES error
// envelope and returns false.
bool ParseSearchBody(std::string_view body, SearchRequest& out,
                     HttpResponseWriter& writer);

// Parses a _count body: only {"query": {...}} (or nothing) is legal; fills
// where/uses_match. On failure writes the error envelope and returns false.
bool ParseCountBody(std::string_view body, SearchRequest& out,
                    HttpResponseWriter& writer);

}  // namespace sdb::network::http::es
