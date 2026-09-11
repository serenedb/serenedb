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

namespace sdb::network::http {

enum class HttpStatus : int {
  None = 0,
  Continue = 100,
  Ok = 200,
  Created = 201,
  Accepted = 202,
  NoContent = 204,
  MovedPermanently = 301,
  NotModified = 304,
  BadRequest = 400,
  Unauthorized = 401,
  Forbidden = 403,
  NotFound = 404,
  MethodNotAllowed = 405,
  RequestTimeout = 408,
  Conflict = 409,
  ContentTooLarge = 413,
  ExpectationFailed = 417,
  TooManyRequests = 429,
  RequestHeaderFieldsTooLarge = 431,
  InternalError = 500,
  NotImplemented = 501,
  ServiceUnavailable = 503,
};

inline constexpr std::string_view kJsonContentType = "application/json";

// Single-quoted SQL string literal with '' doubling; how the thin handlers
// pass request strings into SQL (table function arguments cannot be
// prepared-statement parameters).
std::string SqlLiteral(std::string_view text);

// Double-quoted SQL identifier with "" doubling: injection-safe regardless
// of what the URL path contained (name validity itself is the functions'
// job).
std::string SqlIdentifier(std::string_view name);

// Flattens a request-body view (chunks pinned in the recv channel) into one
// string for parsers that need contiguous bytes (simdjson padded input).
std::string FlattenBody(const message::SequenceView& body);

}  // namespace sdb::network::http
