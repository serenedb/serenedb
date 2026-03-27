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

#include <vpack/options.h>

#include <string_view>

#include "basics/common.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/errors.h"
#include "basics/logger/logger.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "endpoint/endpoint.h"
#include "general_request.h"
#include "rest/common_defines.h"

namespace vpack {

class Slice;

}  // namespace vpack
namespace sdb {

using rest::ContentType;
using rest::EncodingType;
using rest::ResponseCode;

class GeneralRequest;

class GeneralResponse {
  GeneralResponse() = delete;
  GeneralResponse(const GeneralResponse&) = delete;
  GeneralResponse& operator=(const GeneralResponse&) = delete;

 public:
  // converts the response code to a string for delivering to a http client.
  static std::string responseString(ResponseCode);

  // returns true if code will become a valid http response.
  static bool isValidResponseCode(uint64_t code);

  // converts the response code string to the internal code
  static ResponseCode responseCode(const std::string& str);

  // response code from integer error code
  static ResponseCode responseCode(ErrorCode code);

  /// set content-type this sets the contnt type like you expect it
  void setContentType(ContentType type) { _content_type = type; }
  ContentType contentType() const { return _content_type; }

  /// set content-type from a string. this should only be used in
  /// cases when the content-type is user-defined
  /// this is a functionality so that user can set a type like application/zip
  /// from java script code the ContentType will be CUSTOM!!
  void setContentType(const std::string& content_type) {
    _headers[sdb::StaticStrings::kContentTypeHeader] = content_type;
    _content_type = ContentType::Custom;
  }

  void setContentType(std::string&& content_type) noexcept {
    _headers[sdb::StaticStrings::kContentTypeHeader] = std::move(content_type);
    _content_type = ContentType::Custom;
  }

  virtual void setAllowCompression(
    rest::ResponseCompressionType rct) noexcept = 0;
  virtual rest::ResponseCompressionType compressionAllowed() const noexcept = 0;

  void setContentTypeRequested(ContentType type) noexcept {
    _content_type_requested = type;
  }
  ContentType contentTypeRequested() const noexcept {
    return _content_type_requested;
  }

  virtual sdb::Endpoint::TransportType transportType() = 0;

 protected:
  explicit GeneralResponse(ResponseCode, uint64_t mid);

 public:
  virtual ~GeneralResponse() = default;

  // response codes are http response codes, but they are used in other
  // protocols as well
  ResponseCode responseCode() const { return _response_code; }
  void setResponseCode(ResponseCode response_code) {
    _response_code = response_code;
  }

  void setHeaders(containers::FlatHashMap<std::string, std::string> headers) {
    _headers = std::move(headers);
  }

  const auto& headers() const { return _headers; }

  // adds a header. the header field name will be lower-cased
  void setHeader(std::string_view key, const std::string& value) {
    _headers.insert_or_assign(absl::AsciiStrToLower(key), value);
  }

  // adds a header. the header field name must be lower-cased
  void setHeaderNC(std::string_view key, std::string value) {
    _headers.insert_or_assign(key, std::move(value));
  }

  // adds a header if not set. the header field name must be lower-cased
  void setHeaderNCIfNotSet(std::string_view key, std::string_view value) {
    _headers.emplace(key, value);
  }

  virtual bool isResponseEmpty() const noexcept = 0;

  uint64_t messageId() const { return _message_id; }
  void setMessageId(uint64_t msg_id) { _message_id = msg_id; }

  virtual void reset(ResponseCode) = 0;

  // Payload needs to be of type: vpack::Slice
  // or vpack::BufferUInt8&&
  template<typename Payload>
  void setPayload(Payload&& payload,
                  const vpack::Options& options = vpack::Options::gDefaults) {
    SDB_ASSERT(isResponseEmpty());
    addPayload(std::forward<Payload>(payload), &options);
  }

  virtual void addPayload(vpack::Slice slice,
                          const vpack::Options* = nullptr) = 0;
  virtual void addPayload(vpack::BufferUInt8&&,
                          const vpack::Options* = nullptr) = 0;
  virtual void addRawPayload(std::string_view payload) = 0;
  virtual ErrorCode reservePayload(size_t size) { return ERROR_OK; }

  virtual size_t bodySize() const = 0;

  virtual void clearBody() noexcept = 0;

  /// used for head
  bool generateBody() const noexcept { return _generate_body; }

  /// used for head-responses
  bool setGenerateBody(bool generate_body) noexcept {
    return _generate_body = generate_body;
  }

  virtual ErrorCode ZLibDeflate(bool only_if_smaller) = 0;
  virtual ErrorCode GZipCompress(bool only_if_smaller) = 0;
  virtual ErrorCode Lz4Compress(bool only_if_smaller) = 0;

 protected:
  containers::FlatHashMap<std::string, std::string>
    _headers;                   // headers/metadata map
  uint64_t _message_id;         // message ID
  ResponseCode _response_code;  // http response code
  ContentType _content_type;
  ContentType _content_type_requested;
  bool _generate_body;
};

}  // namespace sdb
