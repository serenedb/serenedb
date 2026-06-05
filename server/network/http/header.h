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

#include <cstdint>
#include <string_view>

#include "basics/containers/trivial_map.h"

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

inline constexpr containers::TrivialBiMap kHttpHeaderNames = [](auto selector) {
  return selector()
    .Case("host", HttpHeader::Host)
    .Case("content-type", HttpHeader::ContentType)
    .Case("content-length", HttpHeader::ContentLength)
    .Case("connection", HttpHeader::Connection)
    .Case("transfer-encoding", HttpHeader::TransferEncoding)
    .Case("content-encoding", HttpHeader::ContentEncoding)
    .Case("accept-encoding", HttpHeader::AcceptEncoding)
    .Case("accept", HttpHeader::Accept)
    .Case("expect", HttpHeader::Expect)
    .Case("user-agent", HttpHeader::UserAgent)
    .Case("authorization", HttpHeader::Authorization)
    .Case("location", HttpHeader::Location)
    .Case("date", HttpHeader::Date)
    .Case("server", HttpHeader::Server)
    .Case("cache-control", HttpHeader::CacheControl)
    .Case("etag", HttpHeader::ETag);
};

inline HttpHeader InternHeader(std::string_view name) noexcept {
  if (const auto found = kHttpHeaderNames.TryFindICaseByFirst(name)) {
    return *found;
  }
  return HttpHeader::Unknown;
}

inline std::string_view HeaderName(HttpHeader header) noexcept {
  if (const auto found = kHttpHeaderNames.TryFindBySecond(header)) {
    return *found;
  }
  return {};
}

}  // namespace sdb::network
