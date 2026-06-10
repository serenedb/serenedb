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

#include <absl/strings/match.h>

#include <cstdint>
#include <iterator>
#include <string_view>

namespace sdb::network {

enum class HttpHeader : uint8_t {
  Unknown,
  Host,
  ContentType,
  ContentLength,
  Connection,
  TransferEncoding,
  ContentEncoding,
  AcceptEncoding,
  Accept,
  Expect,
  UserAgent,
  Authorization,
  Location,
  Date,
  Server,
  CacheControl,
  ETag,
};

// Indexed by HttpHeader value (Unknown -> ""); single source of truth for both
// directions -- HeaderName() indexes it, InternHeader() scans it. Order MUST
// match the enum above.
inline constexpr std::string_view kHttpHeaderName[] = {
  "",                   // Unknown
  "host",               // Host
  "content-type",       // ContentType
  "content-length",     // ContentLength
  "connection",         // Connection
  "transfer-encoding",  // TransferEncoding
  "content-encoding",   // ContentEncoding
  "accept-encoding",    // AcceptEncoding
  "accept",             // Accept
  "expect",             // Expect
  "user-agent",         // UserAgent
  "authorization",      // Authorization
  "location",           // Location
  "date",               // Date
  "server",             // Server
  "cache-control",      // CacheControl
  "etag",               // ETag
};

inline HttpHeader InternHeader(std::string_view name) noexcept {
  for (std::size_t i = 1; i < std::size(kHttpHeaderName); ++i) {
    if (absl::EqualsIgnoreCase(name, kHttpHeaderName[i])) {
      return static_cast<HttpHeader>(i);
    }
  }
  return HttpHeader::Unknown;
}

inline std::string_view HeaderName(HttpHeader header) noexcept {
  return kHttpHeaderName[static_cast<std::uint8_t>(header)];
}

}  // namespace sdb::network
