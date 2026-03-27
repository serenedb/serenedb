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

#include <vpack/builder.h>

#include <memory>
#include <string>

#include "basics/common.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/string_buffer.h"
#include "rest/common_defines.h"

/// class for storing a request result
namespace sdb::httpclient {

class HttpResult final {
  HttpResult(const HttpResult&) = delete;
  HttpResult& operator=(const HttpResult&) = delete;

 public:
  enum class ResultType : uint8_t {
    Complete = 0,
    CouldNotConnect,
    WriteError,
    ReadError,
    Unknown,
  };

  HttpResult();

  ~HttpResult();

  /// clear result values
  void clear();

  /// returns whether the response contains an HTTP error
  bool wasHttpError() const noexcept { return (_return_code >= 400); }

  /// returns the http return code
  int getHttpReturnCode() const noexcept { return _return_code; }

  /// sets the http return code
  void setHttpReturnCode(int return_code) noexcept {
    _return_code = return_code;
  }

  /// returns the http return message
  std::string getHttpReturnMessage() const { return _return_message; }

  /// sets the http return message
  void setHttpReturnMessage(const std::string& message) {
    _return_message = message;
  }

  void setHttpReturnMessage(std::string&& message) {
    _return_message = std::move(message);
  }

  /// whether or not the response contained a content length header
  bool hasContentLength() const noexcept { return _has_content_length; }

  /// returns the content length
  size_t getContentLength() const noexcept { return _content_length; }

  /// sets the content length
  void setContentLength(size_t len) noexcept {
    _content_length = len;
    _has_content_length = true;
  }

  /// returns the http body
  sdb::basics::StringBuffer& getBody();

  /// returns the http body
  const sdb::basics::StringBuffer& getBody() const;

  /// returns the http body as vpack
  std::shared_ptr<vpack::Builder> getBodyVPack() const;

  rest::EncodingType getEncodingType() const noexcept { return _encoding_type; }

  /// returns the request result type
  ResultType getResultType() const noexcept { return _request_result_type; }

  /// returns true if result type == OK
  bool isComplete() const noexcept {
    return _request_result_type == ResultType::Complete;
  }

  /// returns true if "transfer-encoding: chunked"
  bool isChunked() const noexcept { return _chunked; }

  /// sets the request result type
  void setResultType(ResultType request_result_type) noexcept {
    _request_result_type = request_result_type;
  }

  /// add header field
  void addHeaderField(const char* line, size_t length);

  /// return the value of a single header
  std::string getHeaderField(std::string_view name, bool& found) const;

  /// check if a header is present
  bool hasHeaderField(std::string_view name) const;

  /// get all header fields
  const auto& getHeaderFields() const { return _header_fields; }

 private:
  /// add header field
  void addHeaderField(std::string_view key, std::string_view value);

  // header information
  std::string _return_message;
  size_t _content_length;
  int _return_code;
  rest::EncodingType _encoding_type;
  bool _found_header;
  bool _has_content_length;
  bool _chunked;

  // request result type
  ResultType _request_result_type;

  // body content
  sdb::basics::StringBuffer _result_body;

  // header fields
  containers::FlatHashMap<std::string, std::string> _header_fields;
};

}  // namespace sdb::httpclient
