////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <string_view>

#include "endpoint/connection_info.h"
#include "rest/general_request.h"

namespace vpack {

class Builder;
struct Options;

}  // namespace vpack
namespace sdb {

class HttpRequest final : public GeneralRequest {
  HttpRequest(HttpRequest&&) = delete;

 public:
  HttpRequest(const ConnectionInfo&, uint64_t mid);

  ~HttpRequest() final;

  const std::string& cookieValue(const std::string& key) const;
  const std::string& cookieValue(const std::string& key, bool& found) const;
  containers::FlatHashMap<std::string, std::string> cookieValues() const {
    return _cookies;
  }

  void setDefaultContentType() noexcept override {
    _content_type = rest::ContentType::Json;
  }
  /// the body content length
  size_t contentLength() const noexcept override { return _payload.size(); }
  // Payload
  std::string_view rawPayload() const override;
  vpack::Slice payload(bool strict_validation) override;

  const vpack::BufferUInt8& body() { return _payload; }
  void appendBody(const char* data, size_t size);
  // append a NUL byte to the request body
  void appendNullTerminator();
  void clearBody() noexcept;

  /// parse an existing path
  void parseUrl(const char* start, size_t len);
  void setHeader(std::string key, std::string value);

 private:
  void setCookie(std::string key, std::string value);
  void parseCookies(const char* buffer, size_t length);
  void setValues(char* buffer, char* end);
  EncodingType parseAcceptEncoding(std::string_view value) const;

  containers::FlatHashMap<std::string, std::string> _cookies;

  /// was VPack payload validated
  bool _validated_payload = false;
};

}  // namespace sdb
