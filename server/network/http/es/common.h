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

#include "basics/message_sequence_view.h"
#include "network/http/response_writer.h"

namespace duckdb {
class ErrorData;
}

namespace sdb::network::http::es {

// Official ES clients (and esrally) refuse to talk to a server whose
// responses lack the product check header.
inline constexpr std::string_view kProductHeader =
  "X-Elastic-Product: Elasticsearch\r\n";

inline void WriteJson(HttpResponseWriter& writer, int status,
                      std::string_view body) {
  writer.Fixed(status, "application/json", body, kProductHeader);
}

inline void WriteText(HttpResponseWriter& writer, int status,
                      std::string_view body) {
  writer.Fixed(status, "text/plain", body, kProductHeader);
}

// ES error envelope: {"error":{"type":...,"reason":...},"status":N}.
void WriteError(HttpResponseWriter& writer, int status, std::string_view type,
                std::string_view reason);

void WriteIndexNotFound(HttpResponseWriter& writer, std::string_view index);

// Maps a failed es_*() call to the ES error envelope. Rethrows the ErrorData
// (which preserves the original exception) to recover the typed SqlException
// and its sqlstate; raw DuckDB errors go through the wire-frame
// ExceptionType mapping. A missing table (42P01) becomes the canonical
// index_not_found reason when the handler passes its index name.
void WriteSqlError(HttpResponseWriter& writer, const duckdb::ErrorData& error,
                   std::string_view index = {});

// Single-quoted SQL string literal with '' doubling; how the thin handlers
// pass request strings into es_*() calls (table function arguments cannot be
// prepared-statement parameters).
std::string SqlLiteral(std::string_view text);

// Double-quoted SQL identifier with "" doubling: injection-safe regardless
// of what the URL path contained (name validity itself is the functions'
// job).
std::string SqlIdentifier(std::string_view name);

// Flattens a request-body view (chunks pinned in the recv channel) into one
// string for parsers that need contiguous bytes (simdjson padded input).
std::string FlattenBody(const message::SequenceView& body);

}  // namespace sdb::network::http::es
